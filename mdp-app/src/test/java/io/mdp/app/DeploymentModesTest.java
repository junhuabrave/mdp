package io.mdp.app;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.mdp.app.sim.FeedSimulator;
import io.mdp.client.book.BookListener;
import io.mdp.client.book.BookManager;
import io.mdp.client.book.PriceLevelBook;
import io.mdp.core.config.MdpConfig;
import io.mdp.sbe.FeedEventDecoder;
import io.mdp.sbe.FeedEventType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end over real UDP sockets on localhost, with the simulator injecting loss.
 */
class DeploymentModesTest
{
    private static final int INSTRUMENTS = 20;

    private static final class EventCounter implements BookListener
    {
        final AtomicInteger gaps = new AtomicInteger();
        final AtomicInteger channelRecoveries = new AtomicInteger();
        final AtomicLong lastActivityNs = new AtomicLong(System.nanoTime());

        public void onBookChanged(final PriceLevelBook book, final boolean endOfEvent, final long receiveTimeNs)
        {
            lastActivityNs.set(System.nanoTime());
        }

        public void onFeedEvent(final FeedEventDecoder event)
        {
            lastActivityNs.set(System.nanoTime());
            if (event.eventType() == FeedEventType.GAP_DETECTED)
            {
                gaps.incrementAndGet();
            }
            if (event.eventType() == FeedEventType.RECOVERY_COMPLETE && event.instrumentId() == 0)
            {
                channelRecoveries.incrementAndGet();
            }
        }
    }

    private static MdpConfig simulator(final String protocol, final int basePort, final double lossBoth)
    {
        return MdpConfig.of(
            "sim.protocol", protocol,
            "sim.channel", "310",
            "sim.incremental.A", "udp://127.0.0.1:" + basePort,
            "sim.incremental.B", "udp://127.0.0.1:" + (basePort + 1),
            "sim.snapshot.A", "udp://127.0.0.1:" + (basePort + 10),
            "sim.rate", "5000",
            "sim.instruments", String.valueOf(INSTRUMENTS),
            "sim.loss.a", "0.05",
            "sim.loss.b", "0.05",
            "sim.loss.both", String.valueOf(lossBoth),
            "sim.snapshot.intervalMillis", "200",
            "sim.durationSeconds", "3",
            "sim.lossStopsAfterSeconds", "2");
    }

    private static MdpConfig handler(final String mode, final String protocol, final int basePort)
    {
        return MdpConfig.of(
            "mdp.mode", mode,
            "mdp.protocol", protocol,
            "mdp.channels", "310",
            "mdp.channel.310.incremental.A", "udp://127.0.0.1:" + basePort,
            "mdp.channel.310.incremental.B", "udp://127.0.0.1:" + (basePort + 1),
            "mdp.channel.310.snapshot.A", "udp://127.0.0.1:" + (basePort + 10),
            "mdp.idle.receiver", "backoff",
            "mdp.idle.consumer", "backoff",
            "mdp.optiq.defaultPriceDecimals", "2");
    }

    private static void runSimulator(final MdpConfig config) throws Exception
    {
        final Thread sim = new Thread(() ->
        {
            try
            {
                new FeedSimulator(config).run();
            }
            catch (final Exception ex)
            {
                throw new IllegalStateException(ex);
            }
        });
        sim.start();
        sim.join();
        Thread.sleep(500); // let the last snapshot loop drain
    }

    private static void assertAllBooksLive(final BookManager books)
    {
        assertEquals(INSTRUMENTS, books.bookCount());
        for (int i = 1; i <= INSTRUMENTS; i++)
        {
            final PriceLevelBook book = books.book(i);
            assertNotNull(book, "book " + i);
            assertFalse(book.isStale(), "book " + i + " stale");
            assertTrue(book.bestBid() < book.bestAsk(), "book " + i + " crossed: " + book);
        }
    }

    @Test
    void allInOneCmeSurvivesSingleLineLossAndRecoversFromDoubleLoss() throws Exception
    {
        final EventCounter events = new EventCounter();
        final FeedHandlerApp app = FeedHandlerApp.start(handler("all-in-one", "cme-mdp3", 41000), events);
        try
        {
            runSimulator(simulator("cme-mdp3", 41000, 0.002));
        }
        finally
        {
            app.close();
        }
        assertAllBooksLive(app.bookManager());
        assertTrue(events.gaps.get() > 0, "loss on both lines should produce gaps");
        assertTrue(events.channelRecoveries.get() >= 2, "startup recovery plus gap recoveries");
    }

    @Test
    void allInOneOptiqInlinePipeline() throws Exception
    {
        final EventCounter events = new EventCounter();
        final FeedHandlerApp app = FeedHandlerApp.start(
            handler("all-in-one", "euronext-optiq", 41100).with("mdp.pipeline", "inline"), events);
        try
        {
            runSimulator(simulator("euronext-optiq", 41100, 0.001));
        }
        finally
        {
            app.close();
        }
        assertAllBooksLive(app.bookManager());
        assertTrue(events.channelRecoveries.get() >= 1);
    }

    @Test
    void replaysRecordedCaptureDeterministically(@TempDir final Path tmp) throws Exception
    {
        final Path capture = tmp.resolve("cme-310.pcap");
        runSimulator(simulator("cme-mdp3", 41300, 0.002)
            .with("sim.pcap.out", capture.toString())
            .with("sim.pcap.only", "true"));

        final String base = "pcap:" + capture + "?speed=max&dst=127.0.0.1:";
        final MdpConfig config = MdpConfig.of(
            "mdp.mode", "all-in-one",
            "mdp.protocol", "cme-mdp3",
            "mdp.channels", "310",
            "mdp.channel.310.incremental.A", base + 41300,
            "mdp.channel.310.incremental.B", base + 41301,
            "mdp.channel.310.snapshot.A", base + 41310,
            "mdp.idle.receiver", "backoff",
            "mdp.idle.consumer", "backoff");

        final String[] finalBooks = new String[2];
        for (int run = 0; run < 2; run++)
        {
            final EventCounter events = new EventCounter();
            final FeedHandlerApp app = FeedHandlerApp.start(config, events);
            try
            {
                awaitQuiet(events, 300, 20_000);
            }
            finally
            {
                app.close();
            }
            assertAllBooksLive(app.bookManager());
            assertTrue(events.gaps.get() > 0, "loss on both lines is in the capture, so replay must see gaps");
            assertTrue(events.channelRecoveries.get() >= 2, "startup recovery plus gap recoveries");
            final StringBuilder books = new StringBuilder();
            for (int i = 1; i <= INSTRUMENTS; i++)
            {
                books.append(app.bookManager().book(i)).append('\n');
            }
            finalBooks[run] = books.toString();
        }
        assertEquals(finalBooks[0], finalBooks[1], "replaying the same capture must give the same books");
    }

    private static void awaitQuiet(final EventCounter events, final long quietMillis, final long timeoutMillis)
        throws InterruptedException
    {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        Thread.sleep(quietMillis);
        while (System.nanoTime() - events.lastActivityNs.get() < TimeUnit.MILLISECONDS.toNanos(quietMillis))
        {
            if (System.nanoTime() > deadline)
            {
                throw new AssertionError("replay still producing events after " + timeoutMillis + " ms");
            }
            Thread.sleep(50);
        }
    }

    @Test
    void lateConsumerRecoversFromNormalizedSnapshotLoop(@TempDir final Path tmp) throws Exception
    {
        final Path capture = tmp.resolve("cme-310.pcap");
        runSimulator(simulator("cme-mdp3", 41400, 0.002)
            .with("sim.pcap.out", capture.toString())
            .with("sim.pcap.only", "true"));

        final String aeronDir = Files.createDirectories(tmp.resolve("aeron")).toString();
        final MediaDriver driver = MediaDriver.launch(new MediaDriver.Context()
            .aeronDirectoryName(aeronDir)
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true));

        final String base = "pcap:" + capture + "?speed=1&dst=127.0.0.1:";
        final MdpConfig normalizerConfig = MdpConfig.of(
            "mdp.mode", "normalizer",
            "mdp.protocol", "cme-mdp3",
            "mdp.channels", "310",
            "mdp.aeron.dir", aeronDir,
            "mdp.channel.310.incremental.A", base + 41400,
            "mdp.channel.310.incremental.B", base + 41401,
            "mdp.channel.310.snapshot.A", base + 41410,
            "mdp.output.aeron.streamId", "1001",
            "mdp.output.snapshot.streamId", "1002",
            "mdp.output.snapshot.intervalMillis", "100",
            "mdp.idle.receiver", "backoff");
        final MdpConfig consumerConfig = MdpConfig.of(
            "mdp.mode", "consumer",
            "mdp.aeron.dir", aeronDir,
            "mdp.input.aeron.channel", "aeron:ipc?tether=false",
            "mdp.input.aeron.streamId", "1001",
            "mdp.input.snapshot.streamId", "1002",
            "mdp.idle.consumer", "backoff");

        final EventCounter earlyEvents = new EventCounter();
        final EventCounter lateEvents = new EventCounter();
        final FeedHandlerApp early = FeedHandlerApp.start(consumerConfig, earlyEvents);
        final FeedHandlerApp normalizer = FeedHandlerApp.start(normalizerConfig, new BookListener() {});
        FeedHandlerApp late = null;
        try
        {
            Thread.sleep(1200); // the capture is ~3 s long at speed 1: join in the middle
            late = FeedHandlerApp.start(consumerConfig, lateEvents);
            awaitQuiet(lateEvents, 500, 20_000);
        }
        finally
        {
            normalizer.close();
            early.close();
            if (null != late)
            {
                late.close();
            }
            driver.close();
        }

        assertAllBooksLive(early.bookManager());
        assertAllBooksLive(late.bookManager());
        assertTrue(late.recoveringStream().firstStreamSeq() > 1000, "late consumer must join mid-stream, joined at " +
            late.recoveringStream().firstStreamSeq());
        assertTrue(late.recoveringStream().imagesApplied() >= INSTRUMENTS, "late consumer synced from images");
        assertEquals(0, late.recoveringStream().gaps());
        for (int i = 1; i <= INSTRUMENTS; i++)
        {
            assertEquals(early.bookManager().book(i).toString(), late.bookManager().book(i).toString(),
                "instrument " + i);
        }
    }

    @Test
    void splitGatewayNormalizerConsumerOverAeron(@TempDir final Path tmp) throws Exception
    {
        final String aeronDir = Files.createDirectories(tmp.resolve("aeron")).toString();
        final MediaDriver driver = MediaDriver.launch(new MediaDriver.Context()
            .aeronDirectoryName(aeronDir)
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true));

        final MdpConfig gatewayConfig = handler("gateway", "cme-mdp3", 41200)
            .with("mdp.aeron.dir", aeronDir)
            .with("mdp.channel.310.relay.incremental.streamId", "2310")
            .with("mdp.channel.310.relay.snapshot.streamId", "3310");
        final MdpConfig normalizerConfig = MdpConfig.of(
            "mdp.mode", "normalizer",
            "mdp.protocol", "cme-mdp3",
            "mdp.channels", "310",
            "mdp.aeron.dir", aeronDir,
            "mdp.channel.310.incremental.A", "aeron:ipc",
            "mdp.channel.310.incremental.A.streamId", "2310",
            "mdp.channel.310.snapshot.A", "aeron:ipc",
            "mdp.channel.310.snapshot.A.streamId", "3310",
            "mdp.output.aeron.streamId", "1001",
            "mdp.idle.receiver", "backoff");
        final MdpConfig consumerConfig = MdpConfig.of(
            "mdp.mode", "consumer",
            "mdp.aeron.dir", aeronDir,
            "mdp.input.aeron.streamId", "1001",
            "mdp.idle.consumer", "backoff");

        final EventCounter events = new EventCounter();
        final FeedHandlerApp consumer = FeedHandlerApp.start(consumerConfig, events);
        final FeedHandlerApp normalizer = FeedHandlerApp.start(normalizerConfig, new BookListener() {});
        final FeedHandlerApp gateway = FeedHandlerApp.start(gatewayConfig, new BookListener() {});
        try
        {
            Thread.sleep(300);
            runSimulator(simulator("cme-mdp3", 41200, 0.001));
        }
        finally
        {
            gateway.close();
            normalizer.close();
            consumer.close();
            driver.close();
        }
        assertAllBooksLive(consumer.bookManager());
        assertTrue(events.channelRecoveries.get() >= 1);
    }
}
