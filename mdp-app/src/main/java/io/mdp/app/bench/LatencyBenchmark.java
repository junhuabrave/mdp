package io.mdp.app.bench;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardProtocolFamily;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.DatagramChannel;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.mdp.cme.CmeFeedDecoder;
import io.mdp.cme.CmePacketHeader;
import io.mdp.cme.CmePacketWriter;
import io.mdp.cme.sbe.MDEntryTypeBook;
import io.mdp.cme.sbe.MDUpdateAction;
import io.mdp.core.arbitration.LineArbitrator;
import io.mdp.core.book.BookListener;
import io.mdp.core.book.BookManager;
import io.mdp.core.channel.ChannelHandler;
import io.mdp.core.channel.FeedHandlerAgent;
import io.mdp.core.consumer.AeronConsumerAgent;
import io.mdp.core.consumer.MarketDataListener;
import io.mdp.core.consumer.NormalizedDispatcher;
import io.mdp.core.consumer.NormalizedMessageHandler;
import io.mdp.core.consumer.RingBufferConsumerAgent;
import io.mdp.core.metrics.MetricsRegistry;
import io.mdp.core.sink.AeronSink;
import io.mdp.core.sink.BackPressurePolicy;
import io.mdp.core.sink.DirectDispatchSink;
import io.mdp.core.sink.NormalizedSink;
import io.mdp.core.sink.RingBufferSink;
import io.mdp.core.transport.PacketHandler;
import io.mdp.core.transport.PacketSource;
import io.mdp.core.transport.UdpPacketSource;
import io.mdp.sbe.BookUpdateDecoder;
import org.HdrHistogram.Histogram;
import org.agrona.BufferUtil;
import org.agrona.CloseHelper;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.OffsetEpochNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;

/**
 * Latency benchmark for the feed handler code paths (DESIGN §12).
 * <p>
 * Every scenario runs real components: line arbitrator, CME SBE decoder, normalized SBE encoding,
 * the hand-off transport, and the book manager. Packets are CME MDP3 {@code MDIncrementalRefreshBook46}
 * messages with one entry (a level change), spread over many instruments with continuous RptSeq.
 * <ul>
 *   <li>{@code inline}     : receive, decode, book update and listener on one thread</li>
 *   <li>{@code ringbuffer} : decoder thread, Agrona ring buffer, consumer thread (default all-in-one)</li>
 *   <li>{@code aeron-ipc}  : decoder thread, Aeron IPC publication, consumer thread (split mode hop)</li>
 *   <li>{@code udp}        : sender thread, kernel UDP loopback, receiver, ring buffer, consumer</li>
 * </ul>
 * In the in-process scenarios the receive timestamp is taken when the packet is handed to the
 * arbitrator, exactly where a socket read would return. The UDP scenario also measures send to receive
 * through the kernel.
 * <p>
 * Usage: {@code benchmark [--rate 100000] [--seconds 10] [--warmup 5] [--instruments 50]
 * [--scenarios inline,ringbuffer,aeron-ipc,udp] [--throughput true]}
 */
public final class LatencyBenchmark
{
    private static final int CHANNEL = 1;
    private static final int UDP_PORT = 39_001;

    private final long rate;
    private final long seconds;
    private final long warmupSeconds;
    private final int instruments;
    // ns resolution on every OS; one calibration shared by all threads of this process
    private final EpochNanoClock clock = new OffsetEpochNanoClock();

    LatencyBenchmark(final long rate, final long seconds, final long warmupSeconds, final int instruments)
    {
        this.rate = rate;
        this.seconds = seconds;
        this.warmupSeconds = warmupSeconds;
        this.instruments = instruments;
    }

    public static void main(final String[] args) throws Exception
    {
        final Map<String, String> opts = new LinkedHashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2)
        {
            opts.put(args[i].replaceFirst("^--", ""), args[i + 1]);
        }
        final LatencyBenchmark bench = new LatencyBenchmark(
            Long.parseLong(opts.getOrDefault("rate", "100000")),
            Long.parseLong(opts.getOrDefault("seconds", "10")),
            Long.parseLong(opts.getOrDefault("warmup", "5")),
            Integer.parseInt(opts.getOrDefault("instruments", "50")));

        System.out.printf("%nMDP latency benchmark: rate=%,d pkt/s, measure=%ds, warmup=%ds, instruments=%d%n",
            bench.rate, bench.seconds, bench.warmupSeconds, bench.instruments);
        System.out.printf("JVM %s on %s %s, %d cpus%n%n", Runtime.version(), System.getProperty("os.name"),
            System.getProperty("os.arch"), Runtime.getRuntime().availableProcessors());

        final List<Result> results = new ArrayList<>();
        for (final String scenario : opts.getOrDefault("scenarios", "inline,ringbuffer,aeron-ipc,udp").split(","))
        {
            results.addAll(bench.run(scenario.trim()));
        }

        System.out.printf("%n%-34s %10s %9s %9s %9s %9s %9s %9s%n",
            "path (ns)", "samples", "p50", "p90", "p99", "p99.9", "p99.99", "max");
        for (final Result r : results)
        {
            final Histogram h = r.histogram();
            System.out.printf("%-34s %,10d %9d %9d %9d %9d %9d %9d%n", r.name(), h.getTotalCount(),
                h.getValueAtPercentile(50), h.getValueAtPercentile(90), h.getValueAtPercentile(99),
                h.getValueAtPercentile(99.9), h.getValueAtPercentile(99.99), h.getMaxValue());
        }

        if (Boolean.parseBoolean(opts.getOrDefault("throughput", "true")))
        {
            bench.throughput();
        }
        System.exit(0);
    }

    record Result(String name, Histogram histogram)
    {
    }

    // ------------------------------------------------------------------ scenarios

    List<Result> run(final String scenario) throws Exception
    {
        System.out.printf("running %s ...%n", scenario);
        final long warmupPackets = rate * warmupSeconds;
        final long totalPackets = rate * (warmupSeconds + seconds);
        final MetricsRegistry metrics = MetricsRegistry.local();
        final Probe probe = new Probe(clock, warmupPackets, totalPackets);
        final NormalizedMessageHandler consumer = new NormalizedDispatcher(
            MarketDataListener.composite(new BookManager(10, new BookListener() {}), probe));

        final List<AutoCloseable> closeables = new ArrayList<>();
        try
        {
            final NormalizedSink sink;
            final List<Agent> consumerAgents = new ArrayList<>();
            switch (scenario)
            {
                case "inline" -> sink = new DirectDispatchSink(consumer, metrics, "inline");
                case "ringbuffer", "udp" ->
                {
                    final OneToOneRingBuffer ring = new OneToOneRingBuffer(new UnsafeBuffer(
                        BufferUtil.allocateDirectAligned((8 << 20) + RingBufferDescriptor.TRAILER_LENGTH, 64)));
                    sink = new RingBufferSink(ring, BackPressurePolicy.SPIN, metrics, "ring");
                    consumerAgents.add(new RingBufferConsumerAgent(ring, consumer, 64, metrics, "ring"));
                }
                case "aeron-ipc" ->
                {
                    final MediaDriver driver = MediaDriver.launchEmbedded(new MediaDriver.Context()
                        .threadingMode(ThreadingMode.SHARED)
                        .sharedIdleStrategy(new BusySpinIdleStrategy())
                        .termBufferSparseFile(false)
                        .dirDeleteOnStart(true)
                        .dirDeleteOnShutdown(true));
                    closeables.add(driver);
                    final Aeron aeron = Aeron.connect(
                        new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
                    closeables.add(0, aeron);
                    final ExclusivePublication publication = aeron.addExclusivePublication("aeron:ipc", 1001);
                    final Subscription subscription = aeron.addSubscription("aeron:ipc", 1001);
                    while (!publication.isConnected())
                    {
                        Thread.onSpinWait();
                    }
                    sink = new AeronSink(publication, BackPressurePolicy.SPIN, metrics, "aeron");
                    consumerAgents.add(new AeronConsumerAgent(subscription, consumer, 64));
                }
                default -> throw new IllegalArgumentException("unknown scenario " + scenario);
            }

            final CmeFeedDecoder decoder = new CmeFeedDecoder(
                CHANNEL, sink, metrics, CmeFeedDecoder.GapPolicy.CHANNEL_STALE, true);
            final PacketSource source;
            final UdpSender sender;
            if ("udp".equals(scenario))
            {
                source = new UdpPacketSource("A", "udp://127.0.0.1:" + UDP_PORT + "?rcvbuf=8388608", clock);
                sender = new UdpSender(new PacketGenerator(instruments, rate, totalPackets, clock), clock);
            }
            else
            {
                source = new GeneratorSource(new PacketGenerator(instruments, rate, totalPackets, clock), clock);
                sender = null;
            }
            final ChannelHandler channel = new ChannelHandler(CHANNEL, 1, CmePacketHeader.INSTANCE, decoder, sink,
                new PacketSource[]{ source }, new PacketSource[0], new PacketSource[0],
                LineArbitrator.Config.defaults(), metrics, clock, 16);

            final List<AgentRunner> hotRunners = new ArrayList<>();
            for (final Agent agent : consumerAgents)
            {
                final AgentRunner runner = start(agent);
                hotRunners.add(runner);
                closeables.add(0, runner);
            }
            final AgentRunner handlerRunner = start(new FeedHandlerAgent("bench", new ChannelHandler[]{ channel }, metrics));
            hotRunners.add(handlerRunner);
            closeables.add(0, handlerRunner);
            if (null != sender)
            {
                Thread.sleep(100);
                closeables.add(0, start(sender));
            }

            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(warmupSeconds + seconds + 30);
            while (probe.seen <= warmupPackets && System.nanoTime() < deadline)
            {
                Thread.sleep(5);
            }
            final long allocatedAtWarmup = allocatedBytes(hotRunners);
            final long seenAtWarmup = probe.seen;
            while (!probe.done.get() && System.nanoTime() < deadline)
            {
                Thread.sleep(50);
            }
            final long allocated = allocatedBytes(hotRunners) - allocatedAtWarmup;
            System.out.printf("  hot-thread allocation after warm-up: %,d bytes over %,d packets (%.3f bytes/packet)%n",
                allocated, probe.seen - seenAtWarmup, (double)allocated / Math.max(1, probe.seen - seenAtWarmup));
            metrics.snapshotHistograms();
            if (!probe.done.get())
            {
                System.out.printf("  WARNING: %s did not complete (%d/%d events)%n",
                    scenario, probe.seen, totalPackets);
            }

            final List<Result> results = new ArrayList<>();
            results.add(new Result(scenario + ": receive -> listener", probe.e2e));
            if ("udp".equals(scenario))
            {
                results.add(new Result("udp: send -> receive (kernel)", probe.wire));
                results.add(new Result("udp: send -> listener", probe.total));
            }
            if ("inline".equals(scenario))
            {
                // inline: the decode recorder includes the listener, same as e2e
                return results;
            }
            final Histogram decode = metrics.latencyRecorders().get(0).cumulativeCopy();
            results.add(new Result(scenario + ": receive -> committed*", decode));
            return results;
        }
        finally
        {
            for (final AutoCloseable c : closeables)
            {
                CloseHelper.quietClose(c);
            }
        }
    }

    /** Unpaced: how many packets per second the ring-buffer pipeline sustains on this host. */
    void throughput() throws Exception
    {
        final long packets = 5_000_000;
        final MetricsRegistry metrics = MetricsRegistry.local();
        final Probe probe = new Probe(clock, 0, packets);
        final OneToOneRingBuffer ring = new OneToOneRingBuffer(new UnsafeBuffer(
            BufferUtil.allocateDirectAligned((8 << 20) + RingBufferDescriptor.TRAILER_LENGTH, 64)));
        final NormalizedSink sink = new RingBufferSink(ring, BackPressurePolicy.SPIN, metrics, "ring");
        final CmeFeedDecoder decoder = new CmeFeedDecoder(CHANNEL, sink, metrics,
            CmeFeedDecoder.GapPolicy.CHANNEL_STALE, true);
        final GeneratorSource source = new GeneratorSource(
            new PacketGenerator(instruments, Long.MAX_VALUE, packets, clock), clock);
        final ChannelHandler channel = new ChannelHandler(CHANNEL, 1, CmePacketHeader.INSTANCE, decoder, sink,
            new PacketSource[]{ source }, new PacketSource[0], new PacketSource[0],
            LineArbitrator.Config.defaults(), metrics, clock, 64);
        final NormalizedMessageHandler consumer = new NormalizedDispatcher(
            MarketDataListener.composite(new BookManager(10, new BookListener() {}), probe));

        final AgentRunner consumerRunner = start(new RingBufferConsumerAgent(ring, consumer, 256, metrics, "ring"));
        final long start = System.nanoTime();
        final AgentRunner producer = start(new FeedHandlerAgent("tp", new ChannelHandler[]{ channel }, metrics));
        while (!probe.done.get())
        {
            Thread.sleep(10);
        }
        final double secs = (System.nanoTime() - start) / 1e9;
        CloseHelper.quietCloseAll(producer, consumerRunner);
        System.out.printf("%nthroughput (ring buffer pipeline, unpaced): %,.0f packets/s (%,d packets in %.2fs)%n",
            packets / secs, packets, secs);
    }

    private static long allocatedBytes(final List<AgentRunner> runners)
    {
        final com.sun.management.ThreadMXBean threads =
            (com.sun.management.ThreadMXBean)java.lang.management.ManagementFactory.getThreadMXBean();
        long total = 0;
        for (final AgentRunner runner : runners)
        {
            final Thread thread = runner.thread();
            if (null != thread)
            {
                total += threads.getThreadAllocatedBytes(thread.threadId());
            }
        }
        return total;
    }

    private static AgentRunner start(final Agent agent)
    {
        final AgentRunner runner = new AgentRunner(new BusySpinIdleStrategy(), Throwable::printStackTrace, null, agent);
        AgentRunner.startOnThread(runner);
        return runner;
    }

    // ------------------------------------------------------------------ helpers

    /** Records latency at the consumer for each live book update after warm-up. */
    static final class Probe implements MarketDataListener
    {
        final Histogram e2e = new Histogram(TimeUnit.SECONDS.toNanos(10), 3);
        final Histogram wire = new Histogram(TimeUnit.SECONDS.toNanos(10), 3);
        final Histogram total = new Histogram(TimeUnit.SECONDS.toNanos(10), 3);
        final AtomicBoolean done = new AtomicBoolean();
        private final EpochNanoClock clock;
        private final long warmupPackets;
        private final long totalPackets;
        volatile long seen;

        Probe(final EpochNanoClock clock, final long warmupPackets, final long totalPackets)
        {
            this.clock = clock;
            this.warmupPackets = warmupPackets;
            this.totalPackets = totalPackets;
        }

        public void onBookUpdate(final BookUpdateDecoder update)
        {
            final long now = clock.nanoTime();
            final long seq = update.packetSeq();
            if (seq > warmupPackets)
            {
                final long receive = update.receiveTimeNs();
                final long send = update.exchangeTimeNs();
                e2e.recordValue(Math.max(0, now - receive));
                wire.recordValue(Math.max(0, receive - send));
                total.recordValue(Math.max(0, now - send));
            }
            seen = seq;
            if (seq >= totalPackets)
            {
                done.set(true);
            }
        }
    }

    /** Builds CME book packets on demand with continuous sequences, allocation free. */
    static final class PacketGenerator
    {
        private final CmePacketWriter writer = new CmePacketWriter(256);
        private final long[] rptSeq;
        private final int instruments;
        private final long intervalNs;
        private final long totalPackets;
        private final EpochNanoClock clock;
        private long seq;
        private long startNs;
        private int next;

        PacketGenerator(final int instruments, final long rate, final long totalPackets, final EpochNanoClock clock)
        {
            this.instruments = instruments;
            this.rptSeq = new long[instruments];
            this.intervalNs = rate == Long.MAX_VALUE ? 0 : 1_000_000_000L / rate;
            this.totalPackets = totalPackets;
            this.clock = clock;
        }

        boolean due(final long nowNs)
        {
            if (startNs == 0)
            {
                startNs = nowNs;
            }
            return seq < totalPackets && nowNs - startNs >= seq * intervalNs;
        }

        boolean exhausted()
        {
            return seq >= totalPackets;
        }

        /** Build the next packet; SendingTime and TransactTime are stamped by the caller via {@link #stamp}. */
        CmePacketWriter build()
        {
            seq++;
            final int i = next;
            next = next + 1 == instruments ? 0 : next + 1;
            final int level = 1 + (int)(seq % 5);
            writer.begin(seq, 0)
                .beginBook(0, true, 1)
                .addBookEntry(i + 1, ++rptSeq[i], (seq & 1) == 0 ? MDEntryTypeBook.Bid : MDEntryTypeBook.Offer,
                    MDUpdateAction.Change, level, (4000L + i) * 1_000_000_000L, 1 + (int)(seq % 97), 1)
                .endBook();
            return writer;
        }

        /** Write the send time into SendingTime (packet header) and TransactTime (first message body). */
        static void stamp(final org.agrona.MutableDirectBuffer packet, final int offset, final long timeNs)
        {
            packet.putLong(offset + CmePacketHeader.SENDING_TIME_OFFSET, timeNs, ByteOrder.LITTLE_ENDIAN);
            packet.putLong(offset + CmePacketHeader.LENGTH + CmePacketHeader.MESSAGE_SIZE_LENGTH + 8, timeNs,
                ByteOrder.LITTLE_ENDIAN);
        }

        EpochNanoClock clock()
        {
            return clock;
        }
    }

    /** In-process source: hands a freshly built packet to the arbitrator when it is due. */
    static final class GeneratorSource implements PacketSource
    {
        private final PacketGenerator generator;
        private final EpochNanoClock clock;
        private final UnsafeBuffer packet = new UnsafeBuffer(new byte[256]);

        GeneratorSource(final PacketGenerator generator, final EpochNanoClock clock)
        {
            this.generator = generator;
            this.clock = clock;
        }

        public int poll(final PacketHandler handler, final int limit)
        {
            int count = 0;
            while (count < limit && generator.due(System.nanoTime()))
            {
                final CmePacketWriter writer = generator.build();
                final int length = writer.length();
                packet.putBytes(0, writer.buffer(), 0, length);
                final long receiveTime = clock.nanoTime();
                PacketGenerator.stamp(packet, 0, receiveTime);
                handler.onPacket(packet, 0, length, receiveTime);
                count++;
            }
            return count;
        }

        public String name()
        {
            return "A";
        }

        public void close()
        {
        }
    }

    /** Sends generated packets to the UDP loopback at the configured rate. */
    static final class UdpSender implements Agent
    {
        private final PacketGenerator generator;
        private final EpochNanoClock clock;
        private final DatagramChannel channel;
        private final ByteBuffer out = ByteBuffer.allocateDirect(256);
        private final UnsafeBuffer outBuffer = new UnsafeBuffer(out);
        private final InetSocketAddress target = new InetSocketAddress("127.0.0.1", UDP_PORT);

        UdpSender(final PacketGenerator generator, final EpochNanoClock clock) throws IOException
        {
            this.generator = generator;
            this.clock = clock;
            this.channel = DatagramChannel.open(StandardProtocolFamily.INET);
            this.channel.connect(target);
        }

        public int doWork() throws IOException
        {
            if (!generator.due(System.nanoTime()))
            {
                return 0;
            }
            final CmePacketWriter writer = generator.build();
            final int length = writer.length();
            outBuffer.putBytes(0, writer.buffer(), 0, length);
            PacketGenerator.stamp(outBuffer, 0, clock.nanoTime());
            out.clear().limit(length);
            channel.write(out);
            return 1;
        }

        public String roleName()
        {
            return "udp-sender";
        }

        public void onClose()
        {
            CloseHelper.quietClose(channel);
        }
    }
}
