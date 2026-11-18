package io.mdp.core.snapshot;

import java.util.concurrent.TimeUnit;

import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.mdp.client.book.BookListener;
import io.mdp.client.book.BookManager;
import io.mdp.client.book.PriceLevelBook;
import io.mdp.client.consumer.NormalizedDispatcher;
import io.mdp.client.stream.StreamSeqReader;
import io.mdp.core.metrics.MetricsRegistry;
import io.mdp.core.sink.NormalizedSink;
import io.mdp.sbe.BookType;
import io.mdp.sbe.BookUpdateEncoder;
import io.mdp.sbe.Side;
import io.mdp.sbe.UpdateAction;
import org.agrona.collections.LongArrayList;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.status.AtomicCounter;

/**
 * Publishes the normalized snapshot loop: a full image of every book, cycling, on its own stream.
 * <p>
 * It subscribes to the normalizer's own incremental stream (in-process, tethered, so it never misses
 * a message) and rebuilds books from it, off the normalizer's thread. Each image is written in one
 * {@link #doWork()} without polling in between, so it reflects exactly the incremental stream up to
 * {@code incrementalSeq}. Consumers that join late or lose messages recover from these images without
 * any request to the feed (design: docs/DESIGN-API-CRYPTO.md §3).
 */
public final class SnapshotPublisherAgent implements Agent
{
    private static final System.Logger LOG = System.getLogger(SnapshotPublisherAgent.class.getName());

    private final Subscription incremental;
    private final NormalizedSink sink;
    private final int venueId;
    private final long intervalNs;
    private final int imagesPerWork;
    private final EpochNanoClock clock;
    private final BookManager books;
    private final NormalizedDispatcher dispatcher;
    private final StreamSeqReader seqReader = new StreamSeqReader();
    private final FragmentHandler handler;
    private final LongArrayList cycleIds = new LongArrayList();
    private final LongArrayList cycleTypes = new LongArrayList();
    private final AtomicCounter images;
    private final AtomicCounter cycles;
    private final AtomicCounter gaps;

    private long lastIncrementalSeq;
    private boolean broken;
    private int cycleIndex = -1;
    private long nextCycleNs;

    public SnapshotPublisherAgent(
        final Subscription incremental,
        final NormalizedSink sink,
        final int venueId,
        final int depth,
        final long intervalMillis,
        final int imagesPerWork,
        final EpochNanoClock clock,
        final MetricsRegistry metrics)
    {
        this.incremental = incremental;
        this.sink = sink;
        this.venueId = venueId;
        this.intervalNs = TimeUnit.MILLISECONDS.toNanos(intervalMillis);
        this.imagesPerWork = imagesPerWork;
        this.clock = clock;
        this.books = new BookManager(depth, new BookListener() { });
        this.dispatcher = new NormalizedDispatcher(books);
        this.handler = (buffer, offset, length, header) -> onIncremental(buffer, offset, length);
        this.images = metrics.counter("mdp.snapshot.images", "venue", String.valueOf(venueId));
        this.cycles = metrics.counter("mdp.snapshot.cycles", "venue", String.valueOf(venueId));
        this.gaps = metrics.counter("mdp.snapshot.input_gaps", "venue", String.valueOf(venueId));
    }

    public int doWork()
    {
        int work = incremental.poll(handler, 256);
        if (broken)
        {
            return work;
        }
        if (cycleIndex < 0)
        {
            final long now = System.nanoTime();
            if (now - nextCycleNs >= 0 && lastIncrementalSeq > 0)
            {
                startCycle();
                nextCycleNs = now + intervalNs;
            }
        }
        if (cycleIndex >= 0)
        {
            final int end = Math.min(cycleIndex + imagesPerWork, cycleIds.size());
            for (; cycleIndex < end; cycleIndex++)
            {
                final BookType type = BookType.get((short)cycleTypes.getLong(cycleIndex));
                publishImage(books.book(type, cycleIds.getLong(cycleIndex)), type);
                work++;
            }
            if (cycleIndex == cycleIds.size())
            {
                cycleIndex = -1;
                MetricsRegistry.increment(cycles);
            }
        }
        return work;
    }

    private void onIncremental(final org.agrona.DirectBuffer buffer, final int offset, final int length)
    {
        final long seq = seqReader.read(buffer, offset);
        if (seq == StreamSeqReader.NO_SEQ)
        {
            return;
        }
        if (lastIncrementalSeq != 0 && seq != lastIncrementalSeq + 1 && !broken)
        {
            // Tethered in-process subscription: this should not happen. Without a source of truth the
            // publisher cannot repair its books, so it stops publishing rather than serve wrong images.
            broken = true;
            cycleIndex = -1;
            MetricsRegistry.increment(gaps);
            LOG.log(System.Logger.Level.ERROR, "snapshot publisher lost incremental messages (expected " +
                (lastIncrementalSeq + 1) + ", got " + seq + "); images stopped until restart");
        }
        lastIncrementalSeq = seq;
        dispatcher.onMessage(buffer, offset, length);
    }

    private void startCycle()
    {
        cycleIds.clear();
        cycleTypes.clear();
        for (final PriceLevelBook book : books.books())
        {
            cycleIds.addLong(book.instrumentId());
            cycleTypes.addLong(BookType.MBP.value());
        }
        for (final PriceLevelBook book : books.impliedBooks())
        {
            cycleIds.addLong(book.instrumentId());
            cycleTypes.addLong(BookType.IMPLIED.value());
        }
        cycleIndex = 0;
    }

    private void publishImage(final PriceLevelBook book, final BookType type)
    {
        final int bids = book.levels(Side.BUY);
        final int asks = book.levels(Side.SELL);
        final long now = clock.nanoTime();
        final boolean channelStale = !books.isChannelLive(book.channelId());

        sink.snapshotBegin()
            .venueId(venueId)
            .bookType(type)
            .priceExponent((byte)book.priceExponent())
            .sizeExponent((byte)book.sizeExponent())
            .bookStale((short)(book.isStale() ? 1 : 0))
            .channelStale((short)(channelStale ? 1 : 0))
            .entryCount(bids + asks)
            .channelId(book.channelId())
            .instrumentId(book.instrumentId())
            .incrementalSeq(lastIncrementalSeq)
            .timeNs(now);
        sink.commit();

        publishSide(book, type, Side.BUY, bids);
        publishSide(book, type, Side.SELL, asks);

        sink.snapshotEnd()
            .venueId(venueId)
            .bookType(type)
            .entryCount(bids + asks)
            .channelId(book.channelId())
            .instrumentId(book.instrumentId())
            .incrementalSeq(lastIncrementalSeq)
            .timeNs(now);
        sink.commit();
        MetricsRegistry.increment(images);
    }

    private void publishSide(final PriceLevelBook book, final BookType type, final Side side, final int levels)
    {
        for (int level = 1; level <= levels; level++)
        {
            final BookUpdateEncoder entry = sink.bookUpdate()
                .venueId(venueId)
                .bookType(type)
                .side(side)
                .action(UpdateAction.NEW)
                .level((short)level)
                .priceExponent((byte)book.priceExponent())
                .sizeExponent((byte)book.sizeExponent())
                .channelId(book.channelId())
                .instrumentId(book.instrumentId())
                .priceMantissa(book.price(side, level))
                .size(book.size(side, level))
                .numOrders(book.orders(side, level))
                .orderId(0)
                .instrumentSeq(0)
                .packetSeq(0)
                .exchangeTimeNs(0)
                .receiveTimeNs(book.lastUpdateNs());
            entry.flags().clear().snapshot(true).lastInEvent(level == levels && side == Side.SELL);
            sink.commit();
        }
    }

    public long lastIncrementalSeq()
    {
        return lastIncrementalSeq;
    }

    public String roleName()
    {
        return "snapshot-publisher-" + venueId;
    }
}
