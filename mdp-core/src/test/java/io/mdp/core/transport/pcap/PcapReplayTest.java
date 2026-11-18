package io.mdp.core.transport.pcap;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import io.mdp.core.transport.PacketHandler;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.SystemEpochNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PcapReplayTest
{
    private static final int SRC = UdpFrame.ipv4("10.0.0.1");
    private static final int GROUP_A = UdpFrame.ipv4("224.0.31.1");
    private static final int GROUP_B = UdpFrame.ipv4("224.0.32.1");
    private static final int GROUP_SNAP = UdpFrame.ipv4("224.0.31.22");
    private static final long T0 = 1_763_000_000_123_456_789L;

    @TempDir
    Path tmp;

    /** Records (line, marker) pairs; the payload's first int is the marker. */
    private static final class Recorder
    {
        final List<String> events = new ArrayList<>();
        final List<Integer> lengths = new ArrayList<>();

        PacketHandler line(final String name)
        {
            return (buffer, offset, length, receiveTimeNs) ->
            {
                events.add(name + buffer.getInt(offset, ByteOrder.LITTLE_ENDIAN));
                lengths.add(length);
            };
        }
    }

    private static UnsafeBuffer payload(final int marker, final int length)
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[length]);
        buffer.putInt(0, marker, ByteOrder.LITTLE_ENDIAN);
        for (int i = 4; i < length; i++)
        {
            buffer.putByte(i, (byte)i);
        }
        return buffer;
    }

    private static void write(final PcapWriter writer, final long ts, final int group, final int port, final int marker,
        final int length)
    {
        writer.writeUdp(ts, SRC, 30000, group, port, payload(marker, length), 0, length);
    }

    /** Poll the lines round-robin, as a channel does, until the replay finishes. */
    private static void drain(final PcapReplays replays, final PcapPacketSource[] sources, final PacketHandler[] handlers)
    {
        for (int spins = 0; spins < 1_000_000; spins++)
        {
            for (int i = 0; i < sources.length; i++)
            {
                sources[i].poll(handlers[i], 4);
            }
            if (sources[0].replay().isFinished())
            {
                return;
            }
        }
        throw new AssertionError("replay did not finish");
    }

    @Test
    void demultiplexesLinesInCaptureOrder()
    {
        for (final boolean nanos : new boolean[]{false, true})
        {
            final Path file = tmp.resolve("ab-" + nanos + ".pcap");
            try (PcapWriter writer = new PcapWriter(file, nanos))
            {
                write(writer, T0, GROUP_A, 14310, 1, 100);
                write(writer, T0 + 1_000, GROUP_B, 15310, 1, 100);
                write(writer, T0 + 2_000, GROUP_B, 15310, 2, 100);
                writer.vlan(42);
                write(writer, T0 + 3_000, GROUP_A, 14310, 2, 4); // padded to 60 bytes on the wire
                writer.vlan(0);
                write(writer, T0 + 4_000, GROUP_A, 9999, 99, 10); // no line for this port
                write(writer, T0 + 5_000, GROUP_A, 14310, 3, 1400);
            }

            final PcapReplays replays = new PcapReplays(new SystemEpochNanoClock());
            final String base = "pcap:" + file + "?speed=max&dst=";
            final PcapPacketSource a = replays.source("A", base + "224.0.31.1:14310");
            final PcapPacketSource b = replays.source("B", base + "224.0.32.1:15310");
            final Recorder recorder = new Recorder();
            // B is polled first, but A's first packet comes first in the file
            drain(replays, new PcapPacketSource[]{b, a}, new PacketHandler[]{recorder.line("B"), recorder.line("A")});

            assertEquals(List.of("A1", "B1", "B2", "A2", "A3"), recorder.events);
            assertEquals(List.of(100, 100, 100, 4, 1400), recorder.lengths, "padding must not leak into payload");
            assertEquals(1, a.replay().skippedNoMatch());
            a.close();
            b.close();
        }
    }

    @Test
    void readsTimestampsAtBothResolutions()
    {
        for (final boolean nanos : new boolean[]{false, true})
        {
            final Path file = tmp.resolve("ts-" + nanos + ".pcap");
            try (PcapWriter writer = new PcapWriter(file, nanos))
            {
                write(writer, T0, GROUP_A, 14310, 1, 20);
            }
            try (PcapReader reader = new PcapReader(file))
            {
                assertTrue(reader.next());
                assertEquals(nanos ? T0 : T0 / 1000 * 1000, reader.timestampNs());
                assertEquals(UdpFrame.LINKTYPE_ETHERNET, reader.linkType());
                assertFalse(reader.next());
            }
        }
    }

    @Test
    void skipsFragmentsAndNonUdpTraffic()
    {
        final Path file = tmp.resolve("junk.pcap");
        try (PcapWriter writer = new PcapWriter(file, true))
        {
            MutableDirectBuffer frame = writer.buildUdpFrame(SRC, 30000, GROUP_A, 14310, payload(7, 50), 0, 50);
            frame.putShort(14 + 6, (short)0x2000, ByteOrder.BIG_ENDIAN); // more-fragments
            writer.writeFrame(T0, frame, 0, writer.builtLength());

            frame = writer.buildUdpFrame(SRC, 30000, GROUP_A, 14310, payload(8, 50), 0, 50);
            frame.putByte(14 + 9, (byte)6); // TCP
            writer.writeFrame(T0 + 1, frame, 0, writer.builtLength());

            frame = writer.buildUdpFrame(SRC, 30000, GROUP_A, 14310, payload(9, 50), 0, 50);
            frame.putShort(12, (short)0x86DD, ByteOrder.BIG_ENDIAN); // IPv6 ethertype
            writer.writeFrame(T0 + 2, frame, 0, writer.builtLength());

            frame = writer.buildUdpFrame(SRC, 30000, GROUP_A, 14310, payload(10, 50), 0, 50);
            writer.writeFrame(T0 + 3, frame, 0, 40); // cut by snaplen
            write(writer, T0 + 4, GROUP_A, 14310, 11, 50);
        }
        final PcapReplays replays = new PcapReplays(new SystemEpochNanoClock());
        final PcapPacketSource a = replays.source("A", "pcap:" + file + "?speed=max&dst=224.0.31.1:14310");
        final Recorder recorder = new Recorder();
        drain(replays, new PcapPacketSource[]{a}, new PacketHandler[]{recorder.line("A")});
        assertEquals(List.of("A11"), recorder.events);
        assertEquals(1, a.replay().skippedFragments());
        assertEquals(2, a.replay().skippedNotUdp());
        assertEquals(1, a.replay().skippedTruncated());
    }

    @Test
    void inactiveLineDropsItsPacketsAndWildcardTakesTheRest()
    {
        final Path file = tmp.resolve("snap.pcap");
        try (PcapWriter writer = new PcapWriter(file, true))
        {
            write(writer, T0, GROUP_SNAP, 14322, 1, 20);
            write(writer, T0 + 1, GROUP_A, 14310, 1, 20);
            write(writer, T0 + 2, GROUP_SNAP, 14322, 2, 20);
            write(writer, T0 + 3, GROUP_B, 15310, 1, 20);
            write(writer, T0 + 4, GROUP_SNAP, 14322, 3, 20);
        }
        final PcapReplays replays = new PcapReplays(new SystemEpochNanoClock());
        final PcapPacketSource a = replays.source("A", "pcap:" + file + "?speed=max&dst=224.0.31.1:14310");
        final PcapPacketSource snap = replays.source("S", "pcap:" + file + "?speed=max&dst=224.0.31.22:14322");
        final PcapPacketSource any = replays.source("W", "pcap:" + file + "?speed=max");
        final Recorder recorder = new Recorder();
        final PacketHandler[] handlers = {recorder.line("A"), recorder.line("S"), recorder.line("W")};

        snap.deactivate();
        a.poll(handlers[0], 10); // discards S1, delivers A1, stops at S2 (discarded on the next poll)
        assertEquals(List.of("A1"), recorder.events);
        snap.activate();
        drain(replays, new PcapPacketSource[]{a, snap, any}, handlers);
        assertEquals(List.of("A1", "W1", "S3"), recorder.events);
        assertEquals(2, snap.replay().discarded(1));
    }

    @Test
    void pacesAtCapturedTiming() throws Exception
    {
        final Path file = tmp.resolve("paced.pcap");
        try (PcapWriter writer = new PcapWriter(file, true))
        {
            write(writer, T0, GROUP_A, 14310, 1, 20);
            write(writer, T0 + 80_000_000L, GROUP_A, 14310, 2, 20); // 80 ms later
        }
        final PcapReplays replays = new PcapReplays(new SystemEpochNanoClock());
        final PcapPacketSource a = replays.source("A", "pcap:" + file + "?dst=224.0.31.1:14310&speed=2");
        final Recorder recorder = new Recorder();
        final PacketHandler handler = recorder.line("A");
        final long start = System.nanoTime();
        assertEquals(1, a.poll(handler, 10));
        assertEquals(0, a.poll(handler, 10), "second packet is due 40 ms later at speed 2");
        while (a.poll(handler, 10) == 0)
        {
            Thread.sleep(1);
        }
        final long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertTrue(elapsedMs >= 39 && elapsedMs < 1000, "elapsed " + elapsedMs);
    }

    @Test
    void rejectsPollingFromTwoThreads() throws Exception
    {
        final Path file = tmp.resolve("threads.pcap");
        try (PcapWriter writer = new PcapWriter(file, true))
        {
            write(writer, T0, GROUP_A, 14310, 1, 20);
        }
        final PcapReplays replays = new PcapReplays(new SystemEpochNanoClock());
        final PcapPacketSource a = replays.source("A", "pcap:" + file + "?dst=224.0.31.1:14310");
        final PcapPacketSource b = replays.source("B", "pcap:" + file + "?dst=224.0.32.1:15310");
        a.poll((buffer, offset, length, ts) -> { }, 1);
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final Thread other = new Thread(() ->
        {
            try
            {
                b.poll((buffer, offset, length, ts) -> { }, 1);
            }
            catch (final Throwable t)
            {
                error.set(t);
            }
        });
        other.start();
        other.join();
        assertInstanceOf(IllegalStateException.class, error.get());
    }

    @Test
    void validatesUris()
    {
        final Path file = tmp.resolve("uri.pcap");
        new PcapWriter(file, true).close();
        final PcapReplays replays = new PcapReplays(new SystemEpochNanoClock());
        assertThrows(IllegalArgumentException.class, () -> replays.source("A", "pcap:" + file + "?dst=224.0.31.1"));
        assertThrows(IllegalArgumentException.class, () -> replays.source("A", "pcap:" + file + "?port=1"));
        assertThrows(IllegalArgumentException.class, () -> replays.source("A", "pcap:" + file + "?speed=0"));
        replays.source("A", "pcap://" + file + "?dst=224.0.31.1:1&speed=max");
        assertThrows(IllegalArgumentException.class,
            () -> replays.source("B", "pcap:" + file + "?dst=224.0.31.2:1&speed=1"), "speeds must agree");
        assertThrows(IllegalArgumentException.class,
            () -> replays.source("C", "pcap:" + file + "?dst=224.0.31.1:1&speed=max"), "duplicate filter");
    }

    @Test
    void streamsFilesLargerThanTheReadBuffer()
    {
        final Path file = tmp.resolve("big.pcap");
        final int packets = 20_000;
        try (PcapWriter writer = new PcapWriter(file, true))
        {
            for (int i = 0; i < packets; i++)
            {
                write(writer, T0 + i, GROUP_A, 14310, i, 64 + (i % 1300));
            }
        }
        final UdpFrame udp = new UdpFrame();
        try (PcapReader reader = new PcapReader(file, 2 * PcapReader.MAX_FRAME))
        {
            int count = 0;
            while (reader.next())
            {
                assertEquals(UdpFrame.Result.UDP, udp.parse(reader.linkType(), reader.frame(), 0, reader.frameLength()));
                assertEquals(count, reader.frame().getInt(udp.payloadOffset(), ByteOrder.LITTLE_ENDIAN));
                assertEquals(64 + (count % 1300), udp.payloadLength());
                assertEquals(T0 + count, reader.timestampNs());
                count++;
            }
            assertEquals(packets, count);
        }
    }

    @Test
    void readsPcapngAndBigEndianClassic() throws Exception
    {
        // A UDP frame to embed, built by the writer
        final PcapWriter scratch = new PcapWriter(tmp.resolve("scratch.pcap"), true);
        final MutableDirectBuffer frame = scratch.buildUdpFrame(SRC, 30000, GROUP_A, 14310, payload(5, 30), 0, 30);
        final byte[] ethernet = new byte[scratch.builtLength()];
        frame.getBytes(0, ethernet);
        scratch.close();
        final byte[] rawIp = java.util.Arrays.copyOfRange(ethernet, 14, ethernet.length);

        // pcapng: little-endian section with ns resolution (EPB + SPB), then a big-endian section with µs default
        final ByteBuffer ng = ByteBuffer.allocate(4096);
        ng.order(ByteOrder.LITTLE_ENDIAN);
        sectionHeader(ng);
        interfaceBlock(ng, UdpFrame.LINKTYPE_ETHERNET, 9);
        enhancedPacket(ng, 0, T0, ethernet);
        simplePacket(ng, ethernet);
        ng.order(ByteOrder.BIG_ENDIAN);
        sectionHeader(ng);
        interfaceBlock(ng, UdpFrame.LINKTYPE_RAW, -1);
        enhancedPacket(ng, 0, T0 / 1000, rawIp);
        final Path ngFile = tmp.resolve("capture.pcapng");
        Files.write(ngFile, java.util.Arrays.copyOf(ng.array(), ng.position()));

        final UdpFrame udp = new UdpFrame();
        try (PcapReader reader = new PcapReader(ngFile))
        {
            assertTrue(reader.isPcapng());
            for (int i = 0; i < 3; i++)
            {
                assertTrue(reader.next(), "record " + i);
                assertEquals(UdpFrame.Result.UDP, udp.parse(reader.linkType(), reader.frame(), 0, reader.frameLength()));
                assertEquals(5, reader.frame().getInt(udp.payloadOffset(), ByteOrder.LITTLE_ENDIAN));
                assertEquals(30, udp.payloadLength());
                assertEquals(GROUP_A, udp.destinationIp());
                assertEquals(14310, udp.destinationPort());
                assertEquals(i < 2 ? T0 : T0 / 1000 * 1000, reader.timestampNs(), "record " + i);
            }
            assertFalse(reader.next());
        }

        // classic, big-endian, microseconds, raw IP link type
        final ByteBuffer classic = ByteBuffer.allocate(1024).order(ByteOrder.BIG_ENDIAN);
        classic.putInt(0xA1B2C3D4).putShort((short)2).putShort((short)4).putInt(0).putInt(0).putInt(65535)
            .putInt(UdpFrame.LINKTYPE_RAW);
        classic.putInt((int)(T0 / 1_000_000_000L)).putInt((int)(T0 % 1_000_000_000L / 1000))
            .putInt(rawIp.length).putInt(rawIp.length).put(rawIp);
        final Path beFile = tmp.resolve("be.pcap");
        Files.write(beFile, java.util.Arrays.copyOf(classic.array(), classic.position()));
        try (PcapReader reader = new PcapReader(beFile))
        {
            assertTrue(reader.next());
            assertEquals(UdpFrame.Result.UDP, udp.parse(reader.linkType(), reader.frame(), 0, reader.frameLength()));
            assertEquals(T0 / 1000 * 1000, reader.timestampNs());
            assertFalse(reader.next());
        }
    }

    private static void sectionHeader(final ByteBuffer b)
    {
        b.putInt(0x0A0D0D0A).putInt(28).putInt(0x1A2B3C4D).putShort((short)1).putShort((short)0)
            .putLong(-1L).putInt(28);
    }

    private static void interfaceBlock(final ByteBuffer b, final int linkType, final int tsresol)
    {
        final int length = tsresol >= 0 ? 32 : 20;
        b.putInt(1).putInt(length).putShort((short)linkType).putShort((short)0).putInt(0);
        if (tsresol >= 0)
        {
            b.putShort((short)9).putShort((short)1).put((byte)tsresol).put(new byte[3]); // if_tsresol, padded
            b.putShort((short)0).putShort((short)0); // opt_endofopt
        }
        b.putInt(length);
    }

    private static void enhancedPacket(final ByteBuffer b, final int iface, final long units, final byte[] data)
    {
        final int padded = (data.length + 3) & ~3;
        final int length = 32 + padded;
        b.putInt(6).putInt(length).putInt(iface).putInt((int)(units >>> 32)).putInt((int)units)
            .putInt(data.length).putInt(data.length).put(data).put(new byte[padded - data.length]).putInt(length);
    }

    private static void simplePacket(final ByteBuffer b, final byte[] data)
    {
        final int padded = (data.length + 3) & ~3;
        final int length = 16 + padded;
        b.putInt(3).putInt(length).putInt(data.length).put(data).put(new byte[padded - data.length]).putInt(length);
    }
}
