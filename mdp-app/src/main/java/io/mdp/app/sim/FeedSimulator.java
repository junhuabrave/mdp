package io.mdp.app.sim;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import io.mdp.cme.CmePacketWriter;
import io.mdp.cme.sbe.AggressorSide;
import io.mdp.cme.sbe.MDUpdateAction;
import io.mdp.core.config.MdpConfig;
import io.mdp.core.transport.pcap.PcapWriter;
import io.mdp.core.transport.pcap.UdpFrame;
import io.mdp.euronext.OptiqPacketWriter;
import io.mdp.euronext.sbe.MarketDataUpdateType;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.SystemEpochNanoClock;

/**
 * Synthetic exchange feed for demos and soak tests. It publishes the same packet to lines A and B
 * (with independent and correlated loss), and a snapshot loop on the snapshot lines.
 * <p>
 * Settings (all {@code sim.*}):
 * <pre>
 * sim.protocol=cme-mdp3 | euronext-optiq
 * sim.channel=310
 * sim.incremental.A=udp://127.0.0.1:20001     sim.incremental.B=udp://127.0.0.1:20002
 * sim.snapshot.A=udp://127.0.0.1:20011        sim.snapshot.B=udp://127.0.0.1:20012
 * sim.rate=20000            packets per second
 * sim.instruments=50
 * sim.loss.a=0.01           probability a packet is dropped on A only
 * sim.loss.b=0.01           ... on B only
 * sim.loss.both=0.0001      ... on both (causes a real gap and a snapshot recovery)
 * sim.snapshot.intervalMillis=1000
 * sim.durationSeconds=0     0 = run forever
 * sim.lossStopsAfterSeconds=0  stop injecting loss after N seconds (0 = never), for a clean tail
 * sim.pcap.out=feed.pcap    also record every packet sent (after loss) to a capture file
 * sim.pcap.only=false       true: record only, do not send on the network
 * </pre>
 * SendingTime carries the epoch-nanosecond send time, so the handler's wire-latency metric measures
 * the network path (and loopback on a single host).
 */
public final class FeedSimulator implements Runnable
{
    private static final int LEVELS = 5;
    private static final long TICK = 25_000_000L; // 0.25 in PRICE9 units

    private final MdpConfig config;
    private final boolean cme;
    private final int channelId;
    private final int instruments;
    private final double lossA;
    private final double lossB;
    private final double lossBoth;
    private final long rate;
    private final long snapshotIntervalNs;
    private final long durationNs;
    private final long lossStopsAfterNs;
    private long startNs;
    private final DatagramChannel socket;
    private final PcapWriter pcap;
    private final boolean pcapOnly;
    private final int pcapSourceIp = UdpFrame.ipv4("10.0.0.1");
    private final List<InetSocketAddress> incrementalTargets = new ArrayList<>();
    private final List<InetSocketAddress> snapshotTargets = new ArrayList<>();
    private final ByteBuffer sendBuffer = ByteBuffer.allocateDirect(2048);
    private final SplittableRandom random = new SplittableRandom(42);
    private final SystemEpochNanoClock clock = new SystemEpochNanoClock();
    private final CmePacketWriter cmeWriter = new CmePacketWriter(1500);
    private final OptiqPacketWriter optiqWriter = new OptiqPacketWriter(1500);

    private final long[] rptSeq;
    private final long[][] bidQty;
    private final long[][] askQty;
    private final long[] mid;
    private long incrementalSeq = 1;
    private long sent;
    private long dropped;

    public FeedSimulator(final MdpConfig config) throws IOException
    {
        this.config = config;
        this.cme = config.get("sim.protocol", config.get("mdp.protocol", "cme-mdp3")).startsWith("cme");
        this.channelId = config.getInt("sim.channel", 310);
        this.instruments = config.getInt("sim.instruments", 50);
        this.lossA = Double.parseDouble(config.get("sim.loss.a", "0.0"));
        this.lossB = Double.parseDouble(config.get("sim.loss.b", "0.0"));
        this.lossBoth = Double.parseDouble(config.get("sim.loss.both", "0.0"));
        this.rate = config.getLong("sim.rate", 20_000);
        this.snapshotIntervalNs = TimeUnit.MILLISECONDS.toNanos(config.getLong("sim.snapshot.intervalMillis", 1000));
        this.durationNs = TimeUnit.SECONDS.toNanos(config.getLong("sim.durationSeconds", 0));
        this.lossStopsAfterNs = TimeUnit.SECONDS.toNanos(config.getLong("sim.lossStopsAfterSeconds", 0));
        for (final String line : List.of("A", "B", "C", "D"))
        {
            addTarget(incrementalTargets, config.get("sim.incremental." + line));
            addTarget(snapshotTargets, config.get("sim.snapshot." + line));
        }
        socket = DatagramChannel.open();
        final String pcapOut = config.get("sim.pcap.out", "");
        pcap = pcapOut.isBlank() ? null : new PcapWriter(Path.of(pcapOut), true);
        pcapOnly = null != pcap && config.getBoolean("sim.pcap.only", false);
        if (null != pcap)
        {
            Runtime.getRuntime().addShutdownHook(new Thread(pcap::close, "pcap-close"));
        }
        socket.setOption(java.net.StandardSocketOptions.IP_MULTICAST_LOOP, true);

        rptSeq = new long[instruments];
        bidQty = new long[instruments][LEVELS];
        askQty = new long[instruments][LEVELS];
        mid = new long[instruments];
        for (int i = 0; i < instruments; i++)
        {
            mid[i] = (4000L + i) * 1_000_000_000L;
            for (int l = 0; l < LEVELS; l++)
            {
                bidQty[i][l] = 1 + random.nextInt(100);
                askQty[i][l] = 1 + random.nextInt(100);
            }
        }
    }

    public static void main(final String[] args) throws Exception
    {
        new FeedSimulator(MdpConfig.load(Path.of(args[0]))).run();
    }

    public void run()
    {
        System.out.printf("simulator: %s channel=%d rate=%d pkt/s instruments=%d loss A=%.4f B=%.4f both=%.5f%n",
            cme ? "cme-mdp3" : "euronext-optiq", channelId, rate, instruments, lossA, lossB, lossBoth);
        final long start = System.nanoTime();
        startNs = start;
        final long intervalNs = Math.max(1, 1_000_000_000L / rate);
        long nextSend = start;
        long nextSnapshot = start;
        long nextReport = start + TimeUnit.SECONDS.toNanos(5);
        while (durationNs == 0 || System.nanoTime() - start < durationNs)
        {
            final long now = System.nanoTime();
            if (now >= nextSnapshot && !snapshotTargets.isEmpty())
            {
                sendSnapshotLoop();
                nextSnapshot = now + snapshotIntervalNs;
            }
            if (now >= nextSend)
            {
                sendIncremental();
                nextSend += intervalNs;
                if (now - nextSend > TimeUnit.MILLISECONDS.toNanos(100))
                {
                    nextSend = now; // fell behind: do not burst
                }
            }
            else if (nextSend - now > 50_000)
            {
                LockSupport.parkNanos(20_000);
            }
            if (now >= nextReport)
            {
                System.out.printf("simulator: sent=%d dropped(both)=%d seq=%d%n", sent, dropped, incrementalSeq);
                nextReport = now + TimeUnit.SECONDS.toNanos(5);
            }
        }
        if (null != pcap)
        {
            pcap.close();
            System.out.printf("simulator: recorded %d packets to %s%n", pcap.records(), pcap.path());
        }
    }

    private void sendIncremental()
    {
        final int i = random.nextInt(instruments);
        final boolean trade = random.nextInt(10) == 0;
        final DirectBuffer packet;
        final int length;
        final long sendTime = clock.nanoTime();
        if (cme)
        {
            cmeWriter.begin(incrementalSeq, sendTime);
            if (trade)
            {
                cmeWriter.trade(sendTime, true, new CmePacketWriter.TradeEntry(
                    i + 1, ++rptSeq[i], mid[i], 1 + random.nextInt(10), 2, AggressorSide.Buy, incrementalSeq));
            }
            else
            {
                final boolean bid = random.nextBoolean();
                final int level = 1 + random.nextInt(LEVELS);
                final long qty = 1 + random.nextInt(100);
                (bid ? bidQty : askQty)[i][level - 1] = qty;
                cmeWriter.book(sendTime, true, new CmePacketWriter.BookEntry(
                    i + 1, ++rptSeq[i], bid ? '0' : '1', MDUpdateAction.Change, level,
                    price(i, bid, level), (int)qty, 1 + random.nextInt(5)));
            }
            packet = cmeWriter.buffer();
            length = cmeWriter.length();
        }
        else
        {
            final boolean bid = random.nextBoolean();
            final int level = 1 + random.nextInt(LEVELS);
            final long qty = 1 + random.nextInt(100);
            (bid ? bidQty : askQty)[i][level - 1] = qty;
            optiqWriter.begin(incrementalSeq, sendTime, channelId).marketUpdate(i + 1, sendTime,
                new OptiqPacketWriter.Level(bid ? MarketDataUpdateType.BidLevel : MarketDataUpdateType.OfferLevel,
                    level, price(i, bid, level) / 10_000_000L, qty, 1));
            packet = optiqWriter.buffer();
            length = optiqWriter.length();
        }
        incrementalSeq++;

        final boolean lossActive = lossStopsAfterNs == 0 || System.nanoTime() - startNs < lossStopsAfterNs;
        final boolean dropBoth = lossActive && random.nextDouble() < lossBoth;
        if (dropBoth)
        {
            dropped++;
            return;
        }
        for (int line = 0; line < incrementalTargets.size(); line++)
        {
            final double loss = !lossActive ? 0.0 : line == 0 ? lossA : line == 1 ? lossB : 0.0;
            if (random.nextDouble() >= loss)
            {
                send(packet, length, incrementalTargets.get(line));
            }
        }
        sent++;
    }

    private void sendSnapshotLoop()
    {
        long snapSeq = 1;
        final long lastSeq = incrementalSeq - 1;
        if (cme)
        {
            for (int i = 0; i < instruments; i++)
            {
                final CmePacketWriter.SnapshotEntry[] entries = new CmePacketWriter.SnapshotEntry[LEVELS * 2];
                for (int l = 1; l <= LEVELS; l++)
                {
                    entries[l - 1] = new CmePacketWriter.SnapshotEntry('0', l, price(i, true, l), (int)bidQty[i][l - 1], 1);
                    entries[LEVELS + l - 1] =
                        new CmePacketWriter.SnapshotEntry('1', l, price(i, false, l), (int)askQty[i][l - 1], 1);
                }
                cmeWriter.begin(snapSeq++, clock.nanoTime())
                    .snapshot(i + 1, rptSeq[i], lastSeq, instruments, clock.nanoTime(), entries);
                sendToAll(cmeWriter.buffer(), cmeWriter.length(), snapshotTargets);
            }
        }
        else
        {
            optiqWriter.begin(snapSeq++, clock.nanoTime(), channelId).startOfSnapshot(lastSeq);
            sendToAll(optiqWriter.buffer(), optiqWriter.length(), snapshotTargets);
            for (int i = 0; i < instruments; i++)
            {
                final OptiqPacketWriter.Level[] levels = new OptiqPacketWriter.Level[LEVELS * 2];
                for (int l = 1; l <= LEVELS; l++)
                {
                    levels[l - 1] = new OptiqPacketWriter.Level(MarketDataUpdateType.BidLevel, l,
                        price(i, true, l) / 10_000_000L, bidQty[i][l - 1], 1);
                    levels[LEVELS + l - 1] = new OptiqPacketWriter.Level(MarketDataUpdateType.OfferLevel, l,
                        price(i, false, l) / 10_000_000L, askQty[i][l - 1], 1);
                }
                optiqWriter.begin(snapSeq++, clock.nanoTime(), channelId)
                    .standingData(i + 1, 2, "FR00000000" + String.format("%02d", i % 100), "SIM" + (i + 1), 1)
                    .marketUpdate(i + 1, clock.nanoTime(), levels);
                sendToAll(optiqWriter.buffer(), optiqWriter.length(), snapshotTargets);
            }
            optiqWriter.begin(snapSeq, clock.nanoTime(), channelId).endOfSnapshot(lastSeq);
            sendToAll(optiqWriter.buffer(), optiqWriter.length(), snapshotTargets);
        }
    }

    private long price(final int instrument, final boolean bid, final int level)
    {
        return bid ? mid[instrument] - level * TICK : mid[instrument] + level * TICK;
    }

    private void sendToAll(final DirectBuffer packet, final int length, final List<InetSocketAddress> targets)
    {
        for (final InetSocketAddress target : targets)
        {
            send(packet, length, target);
        }
    }

    private void send(final DirectBuffer packet, final int length, final InetSocketAddress target)
    {
        if (null != pcap)
        {
            pcap.writeUdp(clock.nanoTime(), pcapSourceIp, 30000,
                UdpFrame.ipv4(target.getAddress().getHostAddress()), target.getPort(), packet, 0, length);
            if (pcapOnly)
            {
                return;
            }
        }
        sendBuffer.clear();
        packet.getBytes(0, sendBuffer, length);
        sendBuffer.flip();
        try
        {
            socket.send(sendBuffer, target);
        }
        catch (final IOException ex)
        {
            throw new IllegalStateException(ex);
        }
    }

    private static void addTarget(final List<InetSocketAddress> targets, final String uri)
    {
        if (null != uri && !uri.isBlank())
        {
            final URI parsed = URI.create(uri);
            targets.add(new InetSocketAddress(parsed.getHost(), parsed.getPort()));
        }
    }

    public MdpConfig config()
    {
        return config;
    }
}
