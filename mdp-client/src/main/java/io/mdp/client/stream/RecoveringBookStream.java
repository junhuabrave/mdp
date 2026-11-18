package io.mdp.client.stream;

import io.mdp.client.book.BookListener;
import io.mdp.client.book.BookManager;
import io.mdp.client.book.PriceLevelBook;
import io.mdp.client.consumer.NormalizedDispatcher;
import io.mdp.sbe.BookType;
import io.mdp.sbe.BookUpdateDecoder;
import io.mdp.sbe.FeedEventDecoder;
import io.mdp.sbe.MessageHeaderDecoder;
import io.mdp.sbe.SnapshotBeginDecoder;
import io.mdp.sbe.SnapshotEndDecoder;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableRingBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.LongHashSet;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Builds books from the normalized incremental stream, and recovers them from the normalized snapshot
 * loop when it joins late or loses messages. It never asks the feed for anything.
 * <p>
 * Rules (design: docs/DESIGN-API-CRYPTO.md §3.2):
 * <ul>
 *   <li>Every incremental message carries {@code streamSeq}. A jump (loss, or a new publisher session)
 *       makes every book stale and unsynced.</li>
 *   <li>A book is <em>synced</em> once an image has been applied. Updates for synced books are applied;
 *       updates for unsynced books, and all feed events, are buffered while any book is unsynced.</li>
 *   <li>An image valid as of {@code incrementalSeq = S} is accepted for an unsynced book if this
 *       consumer has already processed the incremental stream up to S, and holds every message after S
 *       that could affect the book (in the buffer). After applying it, buffered messages with
 *       {@code streamSeq > S} for that book are replayed.</li>
 *   <li>Images for synced books are ignored.</li>
 * </ul>
 * Single threaded: call {@link #onIncremental} and {@link #onSnapshot} from one thread.
 */
public final class RecoveringBookStream
{
    private static final int INITIAL_BUFFER = 64 * 1024;

    private final BookManager books;
    private final BookListener listener;
    private final NormalizedDispatcher dispatcher;
    private final StreamSeqReader seqReader = new StreamSeqReader();
    private final StreamSeqReader snapshotSeqReader = new StreamSeqReader();
    private final MessageHeaderDecoder header = new MessageHeaderDecoder();
    private final BookUpdateDecoder bookUpdate = new BookUpdateDecoder();
    private final FeedEventDecoder feedEvent = new FeedEventDecoder();
    private final SnapshotBeginDecoder snapshotBegin = new SnapshotBeginDecoder();
    private final SnapshotEndDecoder snapshotEnd = new SnapshotEndDecoder();
    private final BookUpdateDecoder replayUpdate = new BookUpdateDecoder();
    private final FeedEventDecoder replayEvent = new FeedEventDecoder();
    private final MessageHeaderDecoder replayHeader = new MessageHeaderDecoder();

    private final LongHashSet synced = new LongHashSet();
    private final LongHashSet unsynced = new LongHashSet();
    private final ExpandableRingBuffer buffer;
    private final int maxBufferBytes;
    private final int initialBufferBytes;
    private final ExpandableRingBuffer.MessageConsumer replayer = this::replayOne;
    private final UnsafeBuffer entry = new UnsafeBuffer(new byte[Long.BYTES + 1024]);

    private boolean started;
    private long lastSeq;
    private long firstSeq;
    private boolean buffering = true;
    /** Every message from this streamSeq on that could affect an unsynced book has been applied or buffered. */
    private long coverageStartSeq;

    // snapshot image in progress
    private long lastSnapshotSeq;
    private boolean imageActive;
    private long imageKey;
    private long imageInstrumentId;
    private BookType imageType;
    private int imageChannelId;
    private long imageIncrementalSeq;
    private int imageEntriesExpected;
    private int imageEntries;
    private boolean imageStale;
    private PriceLevelBook imageBook;

    // replay context
    private PriceLevelBook replayBook;
    private BookType replayType;
    private long replayFromSeq;
    private boolean replayStale;

    private long gaps;
    private long imagesApplied;
    private long imagesRejected;
    private long bufferOverflows;

    /**
     * @param depth          book depth
     * @param listener       receives book changes, trades, status and feed events as for {@link BookManager}
     * @param maxBufferBytes bound on buffered incremental messages while recovering
     */
    public RecoveringBookStream(final int depth, final BookListener listener, final int maxBufferBytes)
    {
        this.listener = listener;
        this.books = new BookManager(depth, listener);
        this.books.liveGate(this::mayGoLive);
        this.dispatcher = new NormalizedDispatcher(books);
        this.maxBufferBytes = maxBufferBytes;
        // the ring only enforces its maximum when it grows, so never start above it
        this.initialBufferBytes = Math.min(INITIAL_BUFFER, maxBufferBytes);
        this.buffer = new ExpandableRingBuffer(initialBufferBytes, maxBufferBytes, true);
    }

    public BookManager books()
    {
        return books;
    }

    /** One message from the incremental stream. */
    public void onIncremental(final DirectBuffer msg, final int offset, final int length)
    {
        final long seq = seqReader.read(msg, offset);
        if (seq == StreamSeqReader.NO_SEQ)
        {
            return; // not a v2 normalized message
        }
        if (!started)
        {
            started = true;
            firstSeq = seq;
            coverageStartSeq = seq;
        }
        else if (seq != lastSeq + 1)
        {
            onStreamGap(seq);
        }
        lastSeq = seq;

        header.wrap(msg, offset);
        final int body = offset + MessageHeaderDecoder.ENCODED_LENGTH;
        switch (header.templateId())
        {
            case BookUpdateDecoder.TEMPLATE_ID ->
            {
                bookUpdate.wrap(msg, body, header.blockLength(), header.version());
                final BookType type = bookUpdate.bookType();
                if (type == BookType.MBO)
                {
                    dispatcher.onMessage(msg, offset, length);
                    return;
                }
                final long key = key(bookUpdate.instrumentId(), type);
                if (synced.contains(key))
                {
                    dispatcher.onMessage(msg, offset, length);
                }
                else
                {
                    if (unsynced.add(key) && !buffering)
                    {
                        buffering = true;
                    }
                    bufferMessage(msg, offset, length, seq);
                }
            }
            case FeedEventDecoder.TEMPLATE_ID ->
            {
                dispatcher.onMessage(msg, offset, length);
                if (buffering)
                {
                    bufferMessage(msg, offset, length, seq);
                }
                else
                {
                    // not buffered: an image older than this event could miss it
                    coverageStartSeq = seq + 1;
                }
            }
            default -> dispatcher.onMessage(msg, offset, length); // trades, status, definitions
        }
    }

    /** One message from the snapshot stream. */
    public void onSnapshot(final DirectBuffer msg, final int offset, final int length)
    {
        final long seq = snapshotSeqReader.read(msg, offset);
        if (seq == StreamSeqReader.NO_SEQ)
        {
            return;
        }
        if (lastSnapshotSeq != 0 && seq != lastSnapshotSeq + 1)
        {
            imageActive = false; // lost part of an image: wait for the next one
        }
        lastSnapshotSeq = seq;

        header.wrap(msg, offset);
        final int body = offset + MessageHeaderDecoder.ENCODED_LENGTH;
        switch (header.templateId())
        {
            case SnapshotBeginDecoder.TEMPLATE_ID ->
                onImageBegin(snapshotBegin.wrap(msg, body, header.blockLength(), header.version()));
            case BookUpdateDecoder.TEMPLATE_ID ->
            {
                if (imageActive)
                {
                    bookUpdate.wrap(msg, body, header.blockLength(), header.version());
                    if (bookUpdate.instrumentId() != imageInstrumentId || bookUpdate.bookType() != imageType)
                    {
                        abandonImage();
                        return;
                    }
                    imageBook.apply(bookUpdate.side(), bookUpdate.action(), bookUpdate.level(),
                        bookUpdate.priceMantissa(), bookUpdate.size(), bookUpdate.numOrders());
                    imageBook.lastUpdateNs(bookUpdate.receiveTimeNs());
                    imageEntries++;
                }
            }
            case SnapshotEndDecoder.TEMPLATE_ID ->
                onImageEnd(snapshotEnd.wrap(msg, body, header.blockLength(), header.version()));
            default ->
            {
            }
        }
    }

    /** True while any book still needs an image, or before the stream has started. */
    public boolean isRecovering()
    {
        return !started || !unsynced.isEmpty();
    }

    public boolean isSynced(final long instrumentId, final BookType type)
    {
        return synced.contains(key(instrumentId, type));
    }

    public int syncedCount()
    {
        return synced.size();
    }

    /** The first incremental {@code streamSeq} this consumer saw: above 1 if it joined mid-stream. */
    public long firstStreamSeq()
    {
        return firstSeq;
    }

    public long lastStreamSeq()
    {
        return lastSeq;
    }

    public long gaps()
    {
        return gaps;
    }

    public long imagesApplied()
    {
        return imagesApplied;
    }

    public long imagesRejected()
    {
        return imagesRejected;
    }

    public long bufferOverflows()
    {
        return bufferOverflows;
    }

    // ------------------------------------------------------------------ images

    private void onImageBegin(final SnapshotBeginDecoder begin)
    {
        imageActive = false;
        final BookType type = begin.bookType();
        final long key = key(begin.instrumentId(), type);
        if (!started || synced.contains(key))
        {
            return;
        }
        final long s = begin.incrementalSeq();
        if (s > lastSeq || s + 1 < coverageStartSeq)
        {
            // Ahead of this consumer (wait until the incremental stream catches up), or older than the
            // messages this consumer still holds: either way, use a later image.
            imagesRejected++;
            return;
        }
        imageActive = true;
        imageKey = key;
        imageInstrumentId = begin.instrumentId();
        imageType = type;
        imageChannelId = (int)begin.channelId();
        imageIncrementalSeq = s;
        imageEntriesExpected = begin.entryCount();
        imageEntries = 0;
        imageStale = begin.bookStale() != 0 || begin.channelStale() != 0;
        imageBook = books.getOrCreateBook(type, begin.instrumentId(), imageChannelId, begin.priceExponent(),
            begin.sizeExponent());
        imageBook.clear();
        if (!imageBook.isStale())
        {
            books.setBookStale(imageBook, true); // until the image is complete
        }
    }

    private void onImageEnd(final SnapshotEndDecoder end)
    {
        if (!imageActive)
        {
            return;
        }
        imageActive = false;
        if (end.instrumentId() != imageInstrumentId || end.bookType() != imageType ||
            end.incrementalSeq() != imageIncrementalSeq || imageEntries != imageEntriesExpected ||
            imageIncrementalSeq + 1 < coverageStartSeq || !unsyncedOrUnknown(imageKey))
        {
            abandonImage();
            return;
        }

        // Bring the image forward from S to the current position with the buffered messages.
        replayBook = imageBook;
        replayType = imageType;
        replayFromSeq = imageIncrementalSeq;
        replayStale = imageStale;
        buffer.forEach(replayer, Integer.MAX_VALUE);
        replayBook = null;

        synced.add(imageKey);
        unsynced.remove(imageKey);
        imagesApplied++;
        books.setBookStale(imageBook, replayStale);
        listener.onBookChanged(imageBook, true, imageBook.lastUpdateNs());
        imageBook = null;

        if (unsynced.isEmpty())
        {
            buffering = false;
            buffer.reset(initialBufferBytes);
        }
    }

    private boolean unsyncedOrUnknown(final long key)
    {
        return !synced.contains(key);
    }

    /** Feed events may bring a channel live, but a book this consumer has not synced must stay stale. */
    private boolean mayGoLive(final PriceLevelBook book, final BookType type)
    {
        return synced.contains(key(book.instrumentId(), type)) || book == replayBook;
    }

    private void abandonImage()
    {
        imageActive = false;
        imagesRejected++;
        if (null != imageBook)
        {
            imageBook.clear();
            books.setBookStale(imageBook, true);
            imageBook = null;
        }
    }

    private boolean replayOne(
        final MutableDirectBuffer msg, final int offset, final int length, final int headOffset)
    {
        // each buffered entry: [streamSeq:8][message]
        final long seq = msg.getLong(offset);
        if (seq <= replayFromSeq)
        {
            return true;
        }
        final int m = offset + Long.BYTES;
        replayHeader.wrap(msg, m);
        final int body = m + MessageHeaderDecoder.ENCODED_LENGTH;
        if (replayHeader.templateId() == BookUpdateDecoder.TEMPLATE_ID)
        {
            replayUpdate.wrap(msg, body, replayHeader.blockLength(), replayHeader.version());
            if (replayUpdate.instrumentId() == replayBook.instrumentId() && replayUpdate.bookType() == replayType)
            {
                replayBook.apply(replayUpdate.side(), replayUpdate.action(), replayUpdate.level(),
                    replayUpdate.priceMantissa(), replayUpdate.size(), replayUpdate.numOrders());
                replayBook.lastUpdateNs(replayUpdate.receiveTimeNs());
            }
        }
        else if (replayHeader.templateId() == FeedEventDecoder.TEMPLATE_ID)
        {
            replayEvent.wrap(msg, body, replayHeader.blockLength(), replayHeader.version());
            final boolean affects = replayEvent.channelId() == replayBook.channelId() &&
                (replayEvent.instrumentId() == 0 || replayEvent.instrumentId() == replayBook.instrumentId());
            if (affects)
            {
                switch (replayEvent.eventType())
                {
                    case BOOK_STALE -> replayStale = true;
                    case RECOVERY_COMPLETE -> replayStale = false;
                    case CHANNEL_RESET -> replayBook.clear();
                    default ->
                    {
                    }
                }
            }
        }
        return true;
    }

    // ------------------------------------------------------------------ incremental helpers

    private void onStreamGap(final long seq)
    {
        gaps++;
        imageActive = false;
        imageBook = null;
        for (final long key : synced)
        {
            unsynced.add(key);
        }
        synced.clear();
        books.markAllStale();
        buffer.reset(initialBufferBytes);
        buffering = true;
        coverageStartSeq = seq;
    }

    private void bufferMessage(final DirectBuffer msg, final int offset, final int length, final long seq)
    {
        if (!append(msg, offset, length, seq))
        {
            // Too far behind to replay: start the coverage window again from this message.
            bufferOverflows++;
            buffer.reset(initialBufferBytes);
            coverageStartSeq = seq;
            append(msg, offset, length, seq);
        }
    }

    private boolean append(final DirectBuffer msg, final int offset, final int length, final long seq)
    {
        entry.putLong(0, seq);
        entry.putBytes(Long.BYTES, msg, offset, length);
        return buffer.append(entry, 0, Long.BYTES + length);
    }

    private static long key(final long instrumentId, final BookType type)
    {
        return (instrumentId << 2) | type.value();
    }

    public int maxBufferBytes()
    {
        return maxBufferBytes;
    }
}
