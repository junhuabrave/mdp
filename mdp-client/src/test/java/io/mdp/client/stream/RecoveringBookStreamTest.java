package io.mdp.client.stream;

import java.util.ArrayList;
import java.util.List;

import io.mdp.client.book.BookListener;
import io.mdp.client.book.PriceLevelBook;
import io.mdp.sbe.BookType;
import io.mdp.sbe.BookUpdateEncoder;
import io.mdp.sbe.FeedEventEncoder;
import io.mdp.sbe.FeedEventType;
import io.mdp.sbe.MessageHeaderEncoder;
import io.mdp.sbe.Side;
import io.mdp.sbe.SnapshotBeginEncoder;
import io.mdp.sbe.SnapshotEndEncoder;
import io.mdp.sbe.UpdateAction;
import org.junit.jupiter.api.Test;
import org.agrona.concurrent.UnsafeBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecoveringBookStreamTest
{
    private static final int CHANNEL = 310;
    private static final long X = 7;
    private static final long Y = 8;

    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
    private final MessageHeaderEncoder header = new MessageHeaderEncoder();
    private int length;

    /** A recorded message: incremental-stream sequence and bytes. */
    private record Msg(long seq, byte[] bytes)
    {
    }

    /** The publisher side: produces incremental messages and keeps the true books for images. */
    private final class Feed
    {
        final List<Msg> messages = new ArrayList<>();
        final PriceLevelBook truthX = new PriceLevelBook(X, CHANNEL, 10);
        final PriceLevelBook truthY = new PriceLevelBook(Y, CHANNEL, 10);
        boolean channelStale = true;
        long seq;
        long snapshotSeq;

        Msg update(final long instrumentId, final Side side, final UpdateAction action, final int level, final long price,
            final long size)
        {
            (instrumentId == X ? truthX : truthY).apply(side, action, level, price, size, 1);
            encodeUpdate(++seq, instrumentId, side, action, level, price, size, false);
            return record(seq);
        }

        Msg event(final FeedEventType type)
        {
            channelStale = type == FeedEventType.BOOK_STALE ? true :
                type == FeedEventType.RECOVERY_COMPLETE ? false : channelStale;
            final FeedEventEncoder event = new FeedEventEncoder().wrapAndApplyHeader(buffer, 0, header);
            event.venueId(1).eventType(type).lineId((short)0).channelId(CHANNEL).instrumentId(0)
                .fromSeq(0).toSeq(0).timeNs(0).streamSeq(++seq);
            length = MessageHeaderEncoder.ENCODED_LENGTH + event.encodedLength();
            return record(seq);
        }

        private Msg record(final long seq)
        {
            final byte[] bytes = new byte[length];
            buffer.getBytes(0, bytes);
            final Msg msg = new Msg(seq, bytes);
            messages.add(msg);
            return msg;
        }

        /** Image of a book as of the current incremental position. */
        List<byte[]> image(final long instrumentId)
        {
            final PriceLevelBook book = instrumentId == X ? truthX : truthY;
            final List<byte[]> out = new ArrayList<>();
            final int entries = book.levels(Side.BUY) + book.levels(Side.SELL);
            final SnapshotBeginEncoder begin = new SnapshotBeginEncoder().wrapAndApplyHeader(buffer, 0, header);
            begin.venueId(1).bookType(BookType.MBP).priceExponent((byte)-2).sizeExponent((byte)0)
                .bookStale((short)0).channelStale((short)(channelStale ? 1 : 0)).entryCount(entries)
                .channelId(CHANNEL).instrumentId(instrumentId).incrementalSeq(seq).streamSeq(++snapshotSeq).timeNs(0);
            out.add(copy(MessageHeaderEncoder.ENCODED_LENGTH + begin.encodedLength()));
            for (final Side side : new Side[]{Side.BUY, Side.SELL})
            {
                for (int level = 1; level <= book.levels(side); level++)
                {
                    encodeUpdate(++snapshotSeq, instrumentId, side, UpdateAction.NEW, level, book.price(side, level),
                        book.size(side, level), true);
                    out.add(copy(length));
                }
            }
            final SnapshotEndEncoder end = new SnapshotEndEncoder().wrapAndApplyHeader(buffer, 0, header);
            end.venueId(1).bookType(BookType.MBP).entryCount(entries).channelId(CHANNEL).instrumentId(instrumentId)
                .incrementalSeq(seq).streamSeq(++snapshotSeq).timeNs(0);
            out.add(copy(MessageHeaderEncoder.ENCODED_LENGTH + end.encodedLength()));
            return out;
        }

        private byte[] copy(final int len)
        {
            final byte[] bytes = new byte[len];
            buffer.getBytes(0, bytes);
            return bytes;
        }
    }

    private void encodeUpdate(final long streamSeq, final long instrumentId, final Side side, final UpdateAction action,
        final int level, final long price, final long size, final boolean snapshot)
    {
        final BookUpdateEncoder update = new BookUpdateEncoder().wrapAndApplyHeader(buffer, 0, header);
        update.venueId(1).bookType(BookType.MBP).side(side).action(action).level((short)level)
            .priceExponent((byte)-2).channelId(CHANNEL).instrumentId(instrumentId).priceMantissa(price).size(size)
            .numOrders(1).orderId(0).instrumentSeq(0).packetSeq(0).exchangeTimeNs(0).receiveTimeNs(0)
            .streamSeq(streamSeq).sizeExponent((byte)0);
        update.flags().clear().snapshot(snapshot).lastInEvent(true);
        length = MessageHeaderEncoder.ENCODED_LENGTH + update.encodedLength();
    }

    private static void deliver(final RecoveringBookStream stream, final Msg msg)
    {
        stream.onIncremental(new UnsafeBuffer(msg.bytes()), 0, msg.bytes().length);
    }

    private static void deliverImage(final RecoveringBookStream stream, final List<byte[]> image)
    {
        for (final byte[] bytes : image)
        {
            stream.onSnapshot(new UnsafeBuffer(bytes), 0, bytes.length);
        }
    }

    private static void assertSameBook(final PriceLevelBook expected, final PriceLevelBook actual)
    {
        for (final Side side : new Side[]{Side.BUY, Side.SELL})
        {
            assertEquals(expected.levels(side), actual.levels(side), side + " levels");
            for (int level = 1; level <= expected.levels(side); level++)
            {
                assertEquals(expected.price(side, level), actual.price(side, level), side + " price " + level);
                assertEquals(expected.size(side, level), actual.size(side, level), side + " size " + level);
            }
        }
    }

    private static RecoveringBookStream newStream()
    {
        return new RecoveringBookStream(10, new BookListener() { }, 1 << 20);
    }

    /** Builds a feed whose X and Y books have a few levels, with the channel live. */
    private Feed warmFeed()
    {
        final Feed feed = new Feed();
        feed.event(FeedEventType.RECOVERY_COMPLETE);
        feed.update(X, Side.BUY, UpdateAction.NEW, 1, 1000, 5);
        feed.update(X, Side.SELL, UpdateAction.NEW, 1, 1010, 7);
        feed.update(Y, Side.BUY, UpdateAction.NEW, 1, 2000, 1);
        feed.update(X, Side.BUY, UpdateAction.NEW, 2, 990, 3);
        return feed;
    }

    @Test
    void lateJoinerRecoversFromImageAndReplaysNewerUpdates()
    {
        final Feed feed = warmFeed();
        final RecoveringBookStream stream = newStream();

        // joins here: has not seen seq 1..5
        deliver(stream, feed.update(X, Side.BUY, UpdateAction.CHANGE, 1, 1000, 6));
        final List<byte[]> imageX = feed.image(X); // as of seq 6
        deliver(stream, feed.update(X, Side.SELL, UpdateAction.NEW, 1, 1005, 2)); // seq 7: after the image
        deliver(stream, feed.update(Y, Side.SELL, UpdateAction.NEW, 1, 2010, 4)); // seq 8
        assertNull(stream.books().book(X), "no book before an image");

        deliverImage(stream, imageX);
        assertTrue(stream.isSynced(X, BookType.MBP));
        assertSameBook(feed.truthX, stream.books().book(X));
        assertFalse(stream.books().book(X).isStale());

        // live updates now apply directly
        deliver(stream, feed.update(X, Side.BUY, UpdateAction.DELETE, 2, 990, 0));
        assertSameBook(feed.truthX, stream.books().book(X));

        // Y still needs its image
        assertTrue(stream.isRecovering());
        deliverImage(stream, feed.image(Y));
        assertSameBook(feed.truthY, stream.books().book(Y));
        assertFalse(stream.isRecovering());
    }

    @Test
    void imageAheadOfTheConsumerIsRejectedUntilCaughtUp()
    {
        final Feed feed = warmFeed();
        final RecoveringBookStream stream = newStream();
        final Msg m6 = feed.update(X, Side.BUY, UpdateAction.CHANGE, 1, 1000, 6);
        final Msg m7 = feed.update(X, Side.BUY, UpdateAction.CHANGE, 1, 1000, 8);
        final List<byte[]> imageAt7 = feed.image(X);

        deliver(stream, m6);
        deliverImage(stream, imageAt7); // consumer is only at 6
        assertFalse(stream.isSynced(X, BookType.MBP));
        assertEquals(1, stream.imagesRejected());

        deliver(stream, m7);
        deliverImage(stream, feed.image(X));
        assertTrue(stream.isSynced(X, BookType.MBP));
        assertSameBook(feed.truthX, stream.books().book(X));
    }

    @Test
    void streamGapMakesBooksStaleUntilNewImages()
    {
        final Feed feed = warmFeed();
        final RecoveringBookStream stream = newStream();
        deliver(stream, feed.update(X, Side.BUY, UpdateAction.CHANGE, 1, 1000, 6));
        deliverImage(stream, feed.image(X));
        assertFalse(stream.books().book(X).isStale());

        feed.update(X, Side.BUY, UpdateAction.CHANGE, 1, 1000, 9); // lost by this consumer
        deliver(stream, feed.update(Y, Side.BUY, UpdateAction.CHANGE, 1, 2000, 2));
        assertEquals(1, stream.gaps());
        assertTrue(stream.books().book(X).isStale());
        assertFalse(stream.isSynced(X, BookType.MBP));

        deliver(stream, feed.event(FeedEventType.RECOVERY_COMPLETE));
        assertTrue(stream.books().book(X).isStale(), "a feed recovery must not make an unsynced book live");

        deliverImage(stream, feed.image(X));
        assertSameBook(feed.truthX, stream.books().book(X));
        assertFalse(stream.books().book(X).isStale());
    }

    @Test
    void feedEventsAfterTheImageAreReplayed()
    {
        final Feed feed = warmFeed();
        final RecoveringBookStream stream = newStream();
        deliver(stream, feed.update(X, Side.BUY, UpdateAction.CHANGE, 1, 1000, 6));
        final List<byte[]> liveImage = feed.image(X); // channel live in the image
        deliver(stream, feed.event(FeedEventType.BOOK_STALE)); // ... but went stale after it
        deliverImage(stream, liveImage);
        assertTrue(stream.isSynced(X, BookType.MBP));
        assertTrue(stream.books().book(X).isStale(), "stale event after the image must win");

        deliver(stream, feed.event(FeedEventType.RECOVERY_COMPLETE));
        assertFalse(stream.books().book(X).isStale());
    }

    @Test
    void imageMissingEntriesIsDiscarded()
    {
        final Feed feed = warmFeed();
        final RecoveringBookStream stream = newStream();
        deliver(stream, feed.update(X, Side.BUY, UpdateAction.CHANGE, 1, 1000, 6));
        final List<byte[]> image = feed.image(X);
        image.remove(1); // lose one entry on the snapshot stream
        deliverImage(stream, image);
        assertFalse(stream.isSynced(X, BookType.MBP));
        assertTrue(stream.books().book(X).isStale());

        deliverImage(stream, feed.image(X));
        assertTrue(stream.isSynced(X, BookType.MBP));
        assertSameBook(feed.truthX, stream.books().book(X));
    }

    @Test
    void imageOlderThanRetainedMessagesIsRejected()
    {
        final Feed feed = warmFeed();
        final RecoveringBookStream stream = new RecoveringBookStream(10, new BookListener() { }, 4096);
        deliver(stream, feed.update(X, Side.BUY, UpdateAction.CHANGE, 1, 1000, 6));
        final List<byte[]> oldImage = feed.image(X);
        for (int i = 0; i < 200; i++) // overflow the small replay buffer
        {
            deliver(stream, feed.update(X, Side.BUY, UpdateAction.CHANGE, 1, 1000, 10 + i));
        }
        assertTrue(stream.bufferOverflows() > 0);
        deliverImage(stream, oldImage);
        assertFalse(stream.isSynced(X, BookType.MBP), "updates after the image were dropped from the buffer");

        deliverImage(stream, feed.image(X));
        assertSameBook(feed.truthX, stream.books().book(X));
    }
}
