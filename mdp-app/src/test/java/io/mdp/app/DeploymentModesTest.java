package io.mdp.app;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.mdp.app.sim.FeedSimulator;
import io.mdp.core.book.BookListener;
import io.mdp.core.book.BookManager;
import io.mdp.core.book.PriceLevelBook;
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

        public void onFeedEvent(final FeedEventDecoder event)
        {
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
