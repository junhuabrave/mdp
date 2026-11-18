package io.mdp.euronext;

import java.util.ArrayList;
import java.util.List;

import io.mdp.core.api.DecoderContext;
import io.mdp.core.api.FeedDecoder;
import io.mdp.core.arbitration.LineArbitrator;
import io.mdp.core.book.BookListener;
import io.mdp.core.book.BookManager;
import io.mdp.core.channel.ChannelHandler;
import io.mdp.core.config.MdpConfig;
import io.mdp.core.consumer.NormalizedDispatcher;
import io.mdp.core.metrics.MetricsRegistry;
import io.mdp.core.sink.DirectDispatchSink;
import io.mdp.core.transport.InMemoryPacketSource;
import io.mdp.core.transport.PacketSource;
import io.mdp.euronext.OptiqPacketWriter.Level;
import io.mdp.euronext.sbe.MarketDataUpdateType;
import io.mdp.sbe.FeedEventDecoder;
import io.mdp.sbe.FeedEventType;
import org.agrona.concurrent.SystemEpochNanoClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OptiqPipelineTest
{
    private static final int CHANNEL = 7;
    private static final int SYMBOL = 4242;
    private static final long GAP_TIMEOUT_NS = 1_000;

    private final SystemEpochNanoClock clock = new SystemEpochNanoClock();
    private final OptiqPacketWriter writer = new OptiqPacketWriter(1500);
    private final List<FeedEventType> events = new ArrayList<>();
    private InMemoryPacketSource lineA;
    private InMemoryPacketSource lineB;
    private InMemoryPacketSource snap;
    private ChannelHandler channel;
    private OptiqFeedDecoder decoder;
    private BookManager books;
    private long now = 1_000_000;
    private long seq = 1;

    @BeforeEach
    void setUp()
    {
        final MetricsRegistry metrics = MetricsRegistry.local();
        books = new BookManager(10, new BookListener()
        {
            public void onFeedEvent(final FeedEventDecoder event)
            {
                events.add(event.eventType());
            }
        });
        final DirectDispatchSink sink = new DirectDispatchSink(new NormalizedDispatcher(books), metrics, "test");
        final FeedDecoder feedDecoder = new OptiqProtocol().newDecoder(
            new DecoderContext(CHANNEL, sink, metrics, MdpConfig.of("mdp.protocol", "euronext-optiq")));
        decoder = (OptiqFeedDecoder)feedDecoder;
        lineA = new InMemoryPacketSource("A", clock, 64, 1500);
        lineB = new InMemoryPacketSource("B", clock, 64, 1500);
        snap = new InMemoryPacketSource("S", clock, 64, 1500);
        channel = new ChannelHandler(CHANNEL, OptiqProtocol.VENUE_ID, OptiqPacketHeader.INSTANCE, feedDecoder, sink,
            new PacketSource[]{ lineA, lineB }, new PacketSource[]{ snap }, new PacketSource[0],
            new LineArbitrator.Config(GAP_TIMEOUT_NS, 64, 1500, Long.MAX_VALUE / 4, 1), metrics, clock, 16);
    }

    private void tick()
    {
        now += 10;
        channel.doWork(now);
    }

    private OptiqPacketWriter bidLevel(final long price, final long qty)
    {
        return writer.begin(seq++, 0, CHANNEL)
            .marketUpdate(SYMBOL, 1, new Level(MarketDataUpdateType.BidLevel, 1, price, qty, 1));
    }

    private void sendBoth(final OptiqPacketWriter packet)
    {
        lineA.offer(packet.buffer(), 0, packet.length());
        lineB.offer(packet.buffer(), 0, packet.length());
        tick();
    }

    private void snapshotCycle(final long lastSeq, final long bid)
    {
        long snapSeq = 1;
        writer.begin(snapSeq++, 0, CHANNEL).startOfSnapshot(lastSeq)
            .standingData(SYMBOL, 2, "FR0000000001", "TESTCO", 1);
        snap.offer(writer.buffer(), 0, writer.length());
        writer.begin(snapSeq++, 0, CHANNEL)
            .marketUpdate(SYMBOL, 1, new Level(MarketDataUpdateType.BidLevel, 1, bid, 10, 1),
                new Level(MarketDataUpdateType.OfferLevel, 1, bid + 5, 10, 1));
        snap.offer(writer.buffer(), 0, writer.length());
        writer.begin(snapSeq, 0, CHANNEL).endOfSnapshot(lastSeq);
        snap.offer(writer.buffer(), 0, writer.length());
        tick();
    }

    @Test
    void startupBuffersIncrementalsAndReplaysAfterSnapshot()
    {
        tick();
        assertTrue(decoder.wantsSnapshot());
        sendBoth(bidLevel(1000, 1));   // seq 1, covered by snapshot (L=1)
        sendBoth(bidLevel(1001, 1));   // seq 2, must be replayed after the snapshot
        snapshotCycle(1, 1000);
        tick();

        assertFalse(decoder.isRecovering());
        assertEquals(1001, books.book(SYMBOL).bestBid());
        assertEquals(1005, books.book(SYMBOL).bestAsk());
        assertEquals(-2, books.book(SYMBOL).priceExponent(), "exponent from StandingData price decimals");
        assertFalse(books.book(SYMBOL).isStale());
    }

    @Test
    void gapTriggersRecoveryAndOldSnapshotIsRejected()
    {
        tick();
        snapshotCycle(0, 1000);
        tick();
        assertFalse(decoder.isRecovering());

        sendBoth(bidLevel(1001, 1));   // seq 1
        bidLevel(1002, 1);             // seq 2 lost on both lines
        sendBoth(bidLevel(1003, 1));   // seq 3 buffered in the arbitrator
        now += GAP_TIMEOUT_NS;
        tick();
        assertTrue(events.contains(FeedEventType.GAP_DETECTED));
        assertTrue(decoder.isRecovering());
        assertTrue(books.book(SYMBOL).isStale());

        snapshotCycle(1, 1001);        // does not cover the gap (needs L >= 2)
        tick();
        assertTrue(decoder.isRecovering());

        sendBoth(bidLevel(1004, 1));   // seq 4 buffered for replay
        snapshotCycle(3, 1003);        // covers the gap; seq 4 replayed
        tick();
        assertFalse(decoder.isRecovering());
        assertFalse(books.book(SYMBOL).isStale());
        assertEquals(1004, books.book(SYMBOL).bestBid());
    }
}
