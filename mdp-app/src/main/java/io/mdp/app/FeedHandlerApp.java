package io.mdp.app;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.mdp.core.api.DecoderContext;
import io.mdp.core.api.FeedDecoder;
import io.mdp.core.api.FeedProtocol;
import io.mdp.core.api.FeedType;
import io.mdp.core.book.BookListener;
import io.mdp.core.book.BookManager;
import io.mdp.core.channel.ChannelFactory;
import io.mdp.core.channel.ChannelHandler;
import io.mdp.core.channel.FeedHandlerAgent;
import io.mdp.core.config.MdpConfig;
import io.mdp.core.consumer.AeronConsumerAgent;
import io.mdp.core.consumer.LatencyProbe;
import io.mdp.core.consumer.MarketDataListener;
import io.mdp.core.consumer.NormalizedDispatcher;
import io.mdp.core.consumer.NormalizedMessageHandler;
import io.mdp.core.consumer.RingBufferConsumerAgent;
import io.mdp.core.metrics.AeronCounterFactory;
import io.mdp.core.metrics.LocalCounterFactory;
import io.mdp.core.metrics.MetricsRegistry;
import io.mdp.core.sink.AeronSink;
import io.mdp.core.sink.BackPressurePolicy;
import io.mdp.core.sink.DirectDispatchSink;
import io.mdp.core.sink.NormalizedSink;
import io.mdp.core.sink.RingBufferSink;
import io.mdp.core.transport.AeronPacketRelay;
import io.mdp.core.transport.PacketSource;
import io.mdp.core.transport.pcap.PcapReplays;
import io.mdp.otel.OtelCountersExporter;
import org.agrona.BufferUtil;
import org.agrona.CloseHelper;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.OffsetEpochNanoClock;
import org.agrona.concurrent.SystemEpochNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;

/**
 * Wires the components for one deployment role (DESIGN §7). The same building blocks are
 * connected differently:
 * <pre>
 * all-in-one : UDP lines -> arbitrate -> decode -> ring buffer (or inline) -> books + listeners
 * gateway    : UDP lines -> arbitrate -> Aeron raw packet streams
 * normalizer : Aeron raw streams -> arbitrate -> decode -> Aeron normalized stream
 * consumer   : Aeron normalized stream -> books + listeners
 * observer   : Aeron CnC counters -> OpenTelemetry (no hot-path code at all)
 * </pre>
 */
public final class FeedHandlerApp implements AutoCloseable
{
    private static final System.Logger LOG = System.getLogger("mdp");

    private final MdpConfig config;
    private final List<AutoCloseable> closeables = new ArrayList<>();
    private final EpochNanoClock clock;
    private MediaDriver mediaDriver;
    private Aeron aeron;
    private MetricsRegistry metrics;
    private BookManager bookManager;

    private FeedHandlerApp(final MdpConfig config)
    {
        this.config = config;
        this.clock = clock(config);
    }

    /**
     * {@code mdp.clock=system} reads the host clock directly (comparable across processes, ns resolution
     * on Linux). {@code offset} is nanoTime plus a per-process epoch offset (ns resolution everywhere,
     * but calibrations can differ by microseconds between processes). The default is system on Linux,
     * offset elsewhere, because macOS's realtime clock has only microsecond resolution.
     */
    static EpochNanoClock clock(final MdpConfig config)
    {
        final boolean linux = System.getProperty("os.name", "").toLowerCase().contains("linux");
        final String kind = config.get("mdp.clock", linux ? "system" : "offset");
        return "system".equals(kind) ? new SystemEpochNanoClock() : new OffsetEpochNanoClock();
    }

    public static FeedHandlerApp start(final MdpConfig config, final BookListener listener)
    {
        final FeedHandlerApp app = new FeedHandlerApp(config);
        try
        {
            app.startRole(listener);
            return app;
        }
        catch (final RuntimeException ex)
        {
            app.close();
            throw ex;
        }
    }

    public BookManager bookManager()
    {
        return bookManager;
    }

    public MetricsRegistry metrics()
    {
        return metrics;
    }

    private void startRole(final BookListener listener)
    {
        final String mode = config.mode();
        LOG.log(System.Logger.Level.INFO, "starting role " + mode);
        switch (mode)
        {
            case "all-in-one" -> startAllInOne(listener);
            case "gateway" -> startGateway();
            case "normalizer" -> startNormalizer();
            case "consumer" -> startConsumer(listener);
            case "observer" -> startObserver();
            case "media-driver" -> startMediaDriver(true);
            default -> throw new IllegalArgumentException("unknown mdp.mode: " + mode);
        }
    }

    // ------------------------------------------------------------------ roles

    private void startAllInOne(final BookListener listener)
    {
        if (needsAeron())
        {
            connectAeron();
        }
        initMetrics();
        final FeedProtocol protocol = FeedProtocol.load(config.protocol());
        final NormalizedMessageHandler consumerHandler = consumerHandler(listener);

        final String pipeline = config.get("mdp.pipeline", "ringbuffer");
        final List<ChannelHandler> channels = new ArrayList<>();
        if ("inline".equals(pipeline))
        {
            for (final int channelId : config.channels())
            {
                channels.add(buildChannel(protocol, channelId,
                    new DirectDispatchSink(consumerHandler, metrics, "inline-" + channelId)));
            }
        }
        else
        {
            final int capacity = config.getInt("mdp.ringbuffer.capacity", 8 * 1024 * 1024);
            final OneToOneRingBuffer ringBuffer = new OneToOneRingBuffer(new UnsafeBuffer(
                BufferUtil.allocateDirectAligned(capacity + RingBufferDescriptor.TRAILER_LENGTH, 64)));
            final BackPressurePolicy policy = BackPressurePolicy.valueOf(
                config.get("mdp.backpressure", "DROP_AND_MARK_STALE"));
            for (final int channelId : config.channels())
            {
                channels.add(buildChannel(protocol, channelId,
                    new RingBufferSink(ringBuffer, policy, metrics, "ring")));
            }
            startAgent(new RingBufferConsumerAgent(ringBuffer, consumerHandler, 256, metrics, "ring"),
                config.get("mdp.idle.consumer", "busy-spin"));
        }
        startAgent(new FeedHandlerAgent("main", channels.toArray(new ChannelHandler[0]), metrics),
            config.get("mdp.idle.receiver", "busy-spin"));
        startExporter();
    }

    private void startGateway()
    {
        connectAeron();
        initMetrics();
        final FeedProtocol protocol = FeedProtocol.load(config.protocol());
        final List<ChannelHandler> channels = new ArrayList<>();
        for (final int channelId : config.channels())
        {
            final String prefix = MdpConfig.channelPrefix(channelId);
            final String relayChannel = config.get(prefix + "relay.channel", config.get("mdp.relay.channel", "aeron:ipc"));
            final ExclusivePublication incremental = aeron.addExclusivePublication(
                relayChannel, config.getInt(prefix + "relay.incremental.streamId", 2000 + channelId));
            final ExclusivePublication snapshot = aeron.addExclusivePublication(
                relayChannel, config.getInt(prefix + "relay.snapshot.streamId", 3000 + channelId));
            closeables.add(incremental);
            closeables.add(snapshot);
            final FeedDecoder relay = new AeronPacketRelay(incremental, snapshot, metrics, channelId);
            channels.add(newChannelHandler(protocol, channelId, relay, null));
        }
        startAgent(new FeedHandlerAgent("gateway", channels.toArray(new ChannelHandler[0]), metrics),
            config.get("mdp.idle.receiver", "busy-spin"));
        startExporter();
    }

    private void startNormalizer()
    {
        connectAeron();
        initMetrics();
        final FeedProtocol protocol = FeedProtocol.load(config.protocol());
        final ExclusivePublication output = aeron.addExclusivePublication(
            config.get("mdp.output.aeron.channel", "aeron:ipc"), config.getInt("mdp.output.aeron.streamId", 1001));
        closeables.add(output);
        final NormalizedSink sink = new AeronSink(output,
            BackPressurePolicy.valueOf(config.get("mdp.backpressure", "DROP_AND_MARK_STALE")), metrics, "normalized");
        final List<ChannelHandler> channels = new ArrayList<>();
        for (final int channelId : config.channels())
        {
            channels.add(buildChannel(protocol, channelId, sink));
        }
        startAgent(new FeedHandlerAgent("normalizer", channels.toArray(new ChannelHandler[0]), metrics),
            config.get("mdp.idle.receiver", "busy-spin"));
        startExporter();
    }

    private void startConsumer(final BookListener listener)
    {
        connectAeron();
        initMetrics();
        final var subscription = aeron.addSubscription(
            config.get("mdp.input.aeron.channel", config.get("mdp.output.aeron.channel", "aeron:ipc")),
            config.getInt("mdp.input.aeron.streamId", config.getInt("mdp.output.aeron.streamId", 1001)));
        closeables.add(subscription);
        startAgent(new AeronConsumerAgent(subscription, consumerHandler(listener), 256),
            config.get("mdp.idle.consumer", "busy-spin"));
        startExporter();
    }

    private void startObserver()
    {
        connectAeron();
        final OtelCountersExporter exporter = OtelCountersExporter.otlp(
            aeron.countersReader(),
            config.get("mdp.metrics.otlp.endpoint", "http://localhost:4317"),
            Duration.ofMillis(config.getLong("mdp.metrics.intervalMillis", 1000)),
            resourceAttributes(),
            true,
            config.getBoolean("mdp.metrics.jvm", true));
        closeables.add(exporter);
    }

    // ------------------------------------------------------------------ building blocks

    private ChannelHandler buildChannel(final FeedProtocol protocol, final int channelId, final NormalizedSink sink)
    {
        final FeedDecoder decoder = protocol.newDecoder(new DecoderContext(channelId, sink, metrics, config));
        return newChannelHandler(protocol, channelId, decoder, sink);
    }

    private ChannelHandler newChannelHandler(
        final FeedProtocol protocol, final int channelId, final FeedDecoder decoder, final NormalizedSink sink)
    {
        final PcapReplays replays = new PcapReplays(clock); // a channel's lines share one replay per capture file
        final PacketSource[] incremental =
            ChannelFactory.sources(config, channelId, FeedType.INCREMENTAL, aeron, clock, replays);
        final PacketSource[] snapshot = ChannelFactory.sources(config, channelId, FeedType.SNAPSHOT, aeron, clock, replays);
        final PacketSource[] definition =
            ChannelFactory.sources(config, channelId, FeedType.DEFINITION, aeron, clock, replays);
        LOG.log(System.Logger.Level.INFO, String.format("channel %d: %d incremental, %d snapshot, %d definition lines",
            channelId, incremental.length, snapshot.length, definition.length));
        return new ChannelHandler(channelId, protocol.venueId(), protocol.packetHeader(), decoder, sink,
            incremental, snapshot, definition, ChannelFactory.arbitration(config), metrics, clock,
            config.getInt("mdp.pollLimit", 16));
    }

    private NormalizedMessageHandler consumerHandler(final BookListener listener)
    {
        bookManager = new BookManager(config.getInt("mdp.book.depth", 10), listener);
        final LatencyProbe probe = new LatencyProbe(clock,
            metrics.latency("mdp.latency.e2e", "role", config.mode()),
            metrics.latency("mdp.latency.wire", "role", config.mode()));
        return new NormalizedDispatcher(MarketDataListener.composite(bookManager, probe));
    }

    private boolean needsAeron()
    {
        if (config.getBoolean("mdp.aeron.enabled", false))
        {
            return true;
        }
        for (final int channelId : config.channels())
        {
            for (final FeedType feed : FeedType.values())
            {
                for (final String line : config.lines(channelId, feed))
                {
                    if (config.lineUri(channelId, feed, line).startsWith("aeron:"))
                    {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private void connectAeron()
    {
        if (config.getBoolean("mdp.aeron.embeddedDriver", false))
        {
            startMediaDriver(false);
        }
        final Aeron.Context ctx = new Aeron.Context();
        final String dir = config.get("mdp.aeron.dir");
        if (null != dir)
        {
            ctx.aeronDirectoryName(dir);
        }
        aeron = Aeron.connect(ctx);
        closeables.add(aeron);
    }

    private void startMediaDriver(final boolean standalone)
    {
        final MediaDriver.Context ctx = new MediaDriver.Context()
            .threadingMode(ThreadingMode.valueOf(config.get("mdp.aeron.threadingMode", "SHARED")))
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)
            .termBufferSparseFile(false)
            .spiesSimulateConnection(true);
        final String dir = config.get("mdp.aeron.dir");
        if (null != dir)
        {
            ctx.aeronDirectoryName(dir);
        }
        mediaDriver = MediaDriver.launch(ctx);
        LOG.log(System.Logger.Level.INFO, "media driver started at " + mediaDriver.aeronDirectoryName() +
            (standalone ? " (standalone)" : " (embedded)"));
    }

    private void initMetrics()
    {
        metrics = null != aeron && config.getBoolean("mdp.metrics.aeronCounters", true) ?
            new MetricsRegistry(new AeronCounterFactory(aeron)) :
            new MetricsRegistry(new LocalCounterFactory(8192));
        metrics.startHistogramSnapshots(config.getLong("mdp.metrics.intervalMillis", 1000));
        closeables.add(metrics);
    }

    private void startExporter()
    {
        final String endpoint = config.get("mdp.metrics.otlp.endpoint");
        if (null != endpoint && config.getBoolean("mdp.metrics.inProcessExporter", true))
        {
            closeables.add(OtelCountersExporter.otlp(
                metrics.countersReader(),
                endpoint,
                Duration.ofMillis(config.getLong("mdp.metrics.intervalMillis", 1000)),
                resourceAttributes(),
                false,
                config.getBoolean("mdp.metrics.jvm", true)));
        }
    }

    private Map<String, String> resourceAttributes()
    {
        return Map.of(
            "service.name", "mdp-" + config.mode(),
            "service.instance.id", config.get("mdp.instance", "local"),
            "mdp.protocol", config.get("mdp.protocol", "none"));
    }

    private void startAgent(final org.agrona.concurrent.Agent agent, final String idle)
    {
        final AgentRunner runner = new AgentRunner(Idle.parse(idle),
            (t) -> LOG.log(System.Logger.Level.ERROR, "agent error", t), null, agent);
        AgentRunner.startOnThread(runner);
        closeables.add(runner);
    }

    public void close()
    {
        for (int i = closeables.size() - 1; i >= 0; i--)
        {
            CloseHelper.quietClose(closeables.get(i));
        }
        CloseHelper.quietClose(mediaDriver);
    }
}
