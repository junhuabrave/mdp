package io.mdp.core.transport.pcap;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Writes a classic pcap file of Ethernet / IPv4 / UDP frames: used by the simulator to record a
 * synthetic feed, and by tests. Short frames are padded to the Ethernet minimum, as on a real wire.
 */
public final class PcapWriter implements AutoCloseable
{
    private static final int ETHERNET_MIN_FRAME = 60;

    private final Path path;
    private final FileChannel file;
    private final boolean nanosecond;
    private final ByteBuffer out = ByteBuffer.allocateDirect(1024 * 1024).order(ByteOrder.LITTLE_ENDIAN);
    private final UnsafeBuffer frame = new UnsafeBuffer(new byte[PcapReader.MAX_FRAME]);
    private int vlanId;
    private short ipId;
    private int frameLength;
    private long records;
    private boolean closed;

    public PcapWriter(final Path path, final boolean nanosecond)
    {
        this.path = path;
        this.nanosecond = nanosecond;
        try
        {
            file = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
        }
        catch (final IOException ex)
        {
            throw new UncheckedIOException("cannot create capture " + path, ex);
        }
        out.putInt(nanosecond ? 0xA1B23C4D : 0xA1B2C3D4)
            .putShort((short)2).putShort((short)4)
            .putInt(0).putInt(0)
            .putInt(65535)
            .putInt(UdpFrame.LINKTYPE_ETHERNET);
    }

    /** Tag subsequent frames with an 802.1Q VLAN id (0 = untagged). */
    public PcapWriter vlan(final int vlanId)
    {
        this.vlanId = vlanId;
        return this;
    }

    /** Build an Ethernet/IPv4/UDP frame into an internal buffer without writing it; see {@link #builtLength()}. */
    public MutableDirectBuffer buildUdpFrame(
        final int sourceIp,
        final int sourcePort,
        final int destinationIp,
        final int destinationPort,
        final DirectBuffer payload,
        final int offset,
        final int length)
    {
        int p = 0;
        // destination MAC: IPv4 multicast mapping, or a fixed unicast MAC
        if ((destinationIp >>> 28) == 0xE)
        {
            frame.putByte(p, (byte)0x01);
            frame.putByte(p + 1, (byte)0x00);
            frame.putByte(p + 2, (byte)0x5E);
            frame.putByte(p + 3, (byte)((destinationIp >>> 16) & 0x7F));
            frame.putByte(p + 4, (byte)(destinationIp >>> 8));
            frame.putByte(p + 5, (byte)destinationIp);
        }
        else
        {
            frame.putInt(p, 0x02000000, ByteOrder.BIG_ENDIAN);
            frame.putShort(p + 4, (short)2, ByteOrder.BIG_ENDIAN);
        }
        frame.putInt(p + 6, 0x02000000, ByteOrder.BIG_ENDIAN);
        frame.putShort(p + 10, (short)1, ByteOrder.BIG_ENDIAN);
        p += 12;
        if (vlanId != 0)
        {
            frame.putShort(p, (short)0x8100, ByteOrder.BIG_ENDIAN);
            frame.putShort(p + 2, (short)(vlanId & 0x0FFF), ByteOrder.BIG_ENDIAN);
            p += 4;
        }
        frame.putShort(p, (short)0x0800, ByteOrder.BIG_ENDIAN);
        p += 2;

        final int ip = p;
        final int totalLength = 20 + 8 + length;
        frame.putByte(ip, (byte)0x45);
        frame.putByte(ip + 1, (byte)0);
        frame.putShort(ip + 2, (short)totalLength, ByteOrder.BIG_ENDIAN);
        frame.putShort(ip + 4, ipId++, ByteOrder.BIG_ENDIAN);
        frame.putShort(ip + 6, (short)0x4000, ByteOrder.BIG_ENDIAN); // don't fragment
        frame.putByte(ip + 8, (byte)32);
        frame.putByte(ip + 9, (byte)17);
        frame.putShort(ip + 10, (short)0, ByteOrder.BIG_ENDIAN);
        frame.putInt(ip + 12, sourceIp, ByteOrder.BIG_ENDIAN);
        frame.putInt(ip + 16, destinationIp, ByteOrder.BIG_ENDIAN);
        frame.putShort(ip + 10, ipChecksum(ip), ByteOrder.BIG_ENDIAN);

        final int udp = ip + 20;
        frame.putShort(udp, (short)sourcePort, ByteOrder.BIG_ENDIAN);
        frame.putShort(udp + 2, (short)destinationPort, ByteOrder.BIG_ENDIAN);
        frame.putShort(udp + 4, (short)(8 + length), ByteOrder.BIG_ENDIAN);
        frame.putShort(udp + 6, (short)0, ByteOrder.BIG_ENDIAN); // checksum optional for IPv4
        frame.putBytes(udp + 8, payload, offset, length);

        int end = udp + 8 + length;
        while (end < ETHERNET_MIN_FRAME)
        {
            frame.putByte(end++, (byte)0);
        }
        frameLength = end;
        return frame;
    }

    public int builtLength()
    {
        return frameLength;
    }

    public void writeUdp(
        final long timestampNs,
        final int sourceIp,
        final int sourcePort,
        final int destinationIp,
        final int destinationPort,
        final DirectBuffer payload,
        final int offset,
        final int length)
    {
        buildUdpFrame(sourceIp, sourcePort, destinationIp, destinationPort, payload, offset, length);
        writeFrame(timestampNs, frame, 0, frameLength);
    }

    /** Write an arbitrary link-layer (Ethernet) frame. */
    public void writeFrame(final long timestampNs, final DirectBuffer buffer, final int offset, final int length)
    {
        if (out.remaining() < 16 + length)
        {
            flush();
        }
        final long seconds = timestampNs / 1_000_000_000L;
        final long nanos = timestampNs % 1_000_000_000L;
        out.putInt((int)seconds)
            .putInt((int)(nanosecond ? nanos : nanos / 1000L))
            .putInt(length)
            .putInt(length);
        buffer.getBytes(offset, out, out.position(), length);
        out.position(out.position() + length);
        records++;
    }

    public long records()
    {
        return records;
    }

    public Path path()
    {
        return path;
    }

    public void flush()
    {
        out.flip();
        try
        {
            while (out.hasRemaining())
            {
                file.write(out);
            }
        }
        catch (final IOException ex)
        {
            throw new UncheckedIOException("cannot write capture " + path, ex);
        }
        out.clear();
    }

    public synchronized void close()
    {
        if (closed)
        {
            return;
        }
        closed = true;
        flush();
        try
        {
            file.close();
        }
        catch (final IOException ex)
        {
            throw new UncheckedIOException(ex);
        }
    }

    private short ipChecksum(final int ip)
    {
        int sum = 0;
        for (int i = 0; i < 20; i += 2)
        {
            sum += frame.getShort(ip + i, ByteOrder.BIG_ENDIAN) & 0xFFFF;
        }
        while ((sum >>> 16) != 0)
        {
            sum = (sum & 0xFFFF) + (sum >>> 16);
        }
        return (short)~sum;
    }
}
