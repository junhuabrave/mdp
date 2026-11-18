package io.mdp.cme;

import java.util.ArrayList;
import java.util.List;

import io.mdp.cme.CmePacketWriter.BookEntry;
import io.mdp.cme.CmePacketWriter.SnapshotEntry;
import io.mdp.cme.CmePacketWriter.TradeEntry;
import io.mdp.cme.sbe.AggressorSide;
import io.mdp.cme.sbe.MDUpdateAction;
import io.mdp.core.api.DecoderContext;
import io.mdp.core.api.FeedDecoder;
import io.mdp.core.arbitration.LineArbitrator;
import io.mdp.core.book.BookListener;
import io.mdp.core.book.BookManager;
import io.mdp.core.book.PriceLevelBook;
import io.mdp.core.channel.ChannelHandler;
import io.mdp.core.config.MdpConfig;
import io.mdp.core.consumer.MarketDataListener;
import io.mdp.core.consumer.NormalizedDispatcher;
import io.mdp.core.metrics.MetricsRegistry;
import io.mdp.core.sink.DirectDispatchSink;
import io.mdp.core.transport.InMemoryPacketSource;
import io.mdp.core.transport.PacketSource;
import io.mdp.sbe.FeedEventDecoder;
import io.mdp.sbe.FeedEventType;
import io.mdp.sbe.Side;
import io.mdp.sbe.TradeDecoder;
import org.agrona.concurrent.SystemEpochNanoClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CmePipelineTest
{
    private static final int CHANNEL = 310;
    private static final int ES = 1001;
    private static final long GAP_TIMEOUT_NS = 1_000;

    private final SystemEpochNanoClock clock = new SystemEpochNanoClock();
    private final List<FeedEventType> events = new ArrayList<>();
    private final List<Long> trades = new ArrayList<>();
    private final CmePacketWriter writer = new CmePacketWriter(1500);
    private InMemoryPacketSource lineA;
    private InMemoryPacketSource lineB;
    private InMemoryPacketSource snapA;
    private ChannelHandler channel;
    private CmeFeedDecoder decoder;
    private BookManager books;
    private long now = 1_000_000;
    private long incSeq = 1;
    private long rptSeq = 1;

    @BeforeEach
    void setUp()
    {
        final MetricsRegistry metrics = MetricsRegistry.local();
        books = new BookManager(10, new BookListener()
        {
            public void onTrade(final TradeDecoder trade)
            {
                trades.add(trade.priceMantissa());
            }

            public void onFeedEvent(final FeedEventDecoder event)
            {
                events.add(event.eventType());
            }
        });
        final DirectDispatchSink sink = new DirectDispatchSink(
            new NormalizedDispatcher(MarketDataListener.composite(books)), metrics, "test");

        final FeedDecoder feedDecoder = new CmeMdp3Protocol().newDecoder(
            new DecoderContext(CHANNEL, sink, metrics, MdpConfig.of("mdp.protocol", "cme-mdp3")));
        decoder = (CmeFeedDecoder)feedDecoder;

        lineA = new InMemoryPacketSource("A", clock, 64, 1500);
        lineB = new InMemoryPacketSource("B", clock, 64, 1500);
        snapA = new InMemoryPacketSource("S", clock, 64, 1500);
        channel = new ChannelHandler(CHANNEL, CmeMdp3Protocol.VENUE_ID, CmePacketHeader.INSTANCE, feedDecoder, sink,
            new PacketSource[]{ lineA, lineB }, new PacketSource[]{ snapA }, new PacketSource[0],
            new LineArbitrator.Config(GAP_TIMEOUT_NS, 64, 1500, Long.MAX_VALUE / 4, 1),
            metrics, clock, 16);
    }

    private void tick()
    {
        now += 10;
        channel.doWork(now);
    }

    /** Builds the next incremental packet: a new bid at level 1. */
    private CmePacketWriter nextBid(final long price, final int size)
    {
        return writer.begin(incSeq++, 0)
            .book(1, true, new BookEntry(ES, rptSeq++, '0', MDUpdateAction.New, 1, price, size, 1));
    }

    private void sendBoth(final CmePacketWriter packet)
    {
        lineA.offer(packet.buffer(), 0, packet.length());
        lineB.offer(packet.buffer(), 0, packet.length());
        tick();
    }

    private void sendSnapshot(final long snapshotRptSeq, final SnapshotEntry... entries)
    {
        writer.begin(1, 0).snapshot(ES, snapshotRptSeq, incSeq - 1, 1, 1, entries);
        snapA.offer(writer.buffer(), 0, writer.length());
        tick();
    }

    private void startAndRecover()
    {
        tick(); // activates snapshot feed
        assertTrue(decoder.wantsSnapshot());
        sendBoth(nextBid(100, 5));   // RptSeq 1: arrives before any snapshot, instrument is stale
        sendSnapshot(1, new SnapshotEntry('0', 1, 100, 5, 1), new SnapshotEntry('1', 1, 101, 7, 2));
        tick();
        assertFalse(decoder.wantsSnapshot());
    }

    @Test
    void startupRecoversFromSnapshotThenAppliesIncrementals()
    {
        startAndRecover();
        final PriceLevelBook book = books.book(ES);
        assertFalse(book.isStale());
        assertEquals(100, book.bestBid());
        assertEquals(101, book.bestAsk());

        sendBoth(nextBid(102, 3));
        assertEquals(102, book.bestBid());
        assertEquals(100, book.price(Side.BUY, 2));
        assertTrue(events.contains(FeedEventType.RECOVERY_COMPLETE));
    }

    @Test
    void lineBFillsLossOnLineAWithoutGap()
    {
        startAndRecover();
        final CmePacketWriter p1 = nextBid(102, 1);
        lineB.offer(p1.buffer(), 0, p1.length());             // A lost this one
        final byte[] p2 = nextBid(103, 1).toBytes();
        tick();
        lineA.offer(new org.agrona.concurrent.UnsafeBuffer(p2), 0, p2.length);
        tick();
        assertEquals(103, books.book(ES).bestBid());
        assertFalse(events.contains(FeedEventType.GAP_DETECTED));
    }

    @Test
    void gapOnBothLinesMarksStaleAndSnapshotRecovers()
    {
        startAndRecover();
        nextBid(102, 1);                    // lost on both lines
        sendBoth(nextBid(103, 1));          // buffered while waiting for the hole
        now += GAP_TIMEOUT_NS;
        tick();                             // gap timer fires

        assertTrue(events.contains(FeedEventType.GAP_DETECTED));
        assertTrue(books.book(ES).isStale());
        assertTrue(decoder.wantsSnapshot());

        // snapshot older than the highest incremental RptSeq seen (3): ignored
        sendSnapshot(2, new SnapshotEntry('0', 1, 102, 1, 1));
        assertTrue(books.book(ES).isStale());

        sendSnapshot(3, new SnapshotEntry('0', 1, 103, 1, 1), new SnapshotEntry('0', 2, 102, 1, 1));
        tick();
        final PriceLevelBook book = books.book(ES);
        assertFalse(book.isStale());
        assertEquals(103, book.bestBid());
        assertFalse(decoder.wantsSnapshot());

        sendBoth(nextBid(104, 1));          // RptSeq 4 continues live
        assertEquals(104, books.book(ES).bestBid());
    }

    @Test
    void rptSeqJumpMarksOnlyInstrumentStale()
    {
        startAndRecover();
        rptSeq++;                           // instrument update skipped by exchange view (RptSeq gap)
        sendBoth(nextBid(105, 1));
        assertTrue(books.book(ES).isStale());
        assertTrue(decoder.wantsSnapshot());
        assertFalse(events.contains(FeedEventType.GAP_DETECTED), "packet sequence was contiguous");
    }

    @Test
    void tradesAreNormalized()
    {
        startAndRecover();
        writer.begin(incSeq++, 0).trade(5, true,
            new TradeEntry(ES, rptSeq++, 101, 2, 2, AggressorSide.Buy, 77));
        sendBoth(writer);
        assertEquals(List.of(101L), trades);
    }
}
