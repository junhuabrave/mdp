package io.mdp.core.transport.pcap;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.agrona.BufferUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Streaming, allocation-free reader for capture files: classic pcap (microsecond or nanosecond
 * timestamps, either byte order) and pcapng (enhanced and simple packet blocks, multiple sections and
 * interfaces, {@code if_tsresol}).
 * <p>
 * The file is read through a fixed direct buffer, so files of any size work. After {@link #next()}
 * returns true, {@link #frame()} holds one link-layer frame until the following call.
 */
public final class PcapReader implements AutoCloseable
{
    public static final int MAX_FRAME = 262_144;

    private static final int CLASSIC_US_MAGIC = 0xA1B2C3D4;
    private static final int CLASSIC_NS_MAGIC = 0xA1B23C4D;
    private static final int PCAPNG_SHB = 0x0A0D0D0A;
    private static final int PCAPNG_BYTE_ORDER_MAGIC = 0x1A2B3C4D;
    private static final int PCAPNG_IDB = 1;
    private static final int PCAPNG_SPB = 3;
    private static final int PCAPNG_EPB = 6;
    private static final int MAX_INTERFACES = 64;

    private final Path path;
    private final FileChannel file;
    private final ByteBuffer byteBuffer;
    private final UnsafeBuffer buffer;
    private final UnsafeBuffer frame = new UnsafeBuffer(0, 0);
    private final boolean pcapng;
    private ByteOrder order;
    private int position;
    private int limit;
    private boolean eof;

    // classic pcap
    private boolean nanosecond;
    private int classicLinkType;

    // pcapng, per interface of the current section
    private final int[] linkTypes = new int[MAX_INTERFACES];
    private final int[] snapLens = new int[MAX_INTERFACES];
    private final long[] unitsPerSecond = new long[MAX_INTERFACES];
    private final int[] binaryExponent = new int[MAX_INTERFACES];
    private int interfaceCount;

    private long timestampNs;
    private int frameLinkType;
    private int frameLength;
    private int originalLength;
    private long records;

    public PcapReader(final Path path)
    {
        this(path, 4 * 1024 * 1024);
    }

    public PcapReader(final Path path, final int bufferSize)
    {
        if (bufferSize < 2 * MAX_FRAME)
        {
            throw new IllegalArgumentException("bufferSize must be at least " + 2 * MAX_FRAME);
        }
        this.path = path;
        try
        {
            file = FileChannel.open(path, StandardOpenOption.READ);
        }
        catch (final IOException ex)
        {
            throw new UncheckedIOException("cannot open capture " + path, ex);
        }
        byteBuffer = ByteBuffer.allocateDirect(bufferSize);
        buffer = new UnsafeBuffer(byteBuffer);

        if (!ensure(4))
        {
            throw new IllegalArgumentException("empty capture file: " + path);
        }
        final int magic = buffer.getInt(0, ByteOrder.LITTLE_ENDIAN);
        if (magic == PCAPNG_SHB)
        {
            pcapng = true;
            order = ByteOrder.LITTLE_ENDIAN; // fixed by the first section header block
        }
        else
        {
            pcapng = false;
            readClassicHeader();
        }
    }

    private void readClassicHeader()
    {
        if (!ensure(24))
        {
            throw new IllegalArgumentException("truncated pcap header: " + path);
        }
        final int le = buffer.getInt(position, ByteOrder.LITTLE_ENDIAN);
        final int be = buffer.getInt(position, ByteOrder.BIG_ENDIAN);
        if (le == CLASSIC_US_MAGIC || le == CLASSIC_NS_MAGIC)
        {
            order = ByteOrder.LITTLE_ENDIAN;
            nanosecond = le == CLASSIC_NS_MAGIC;
        }
        else if (be == CLASSIC_US_MAGIC || be == CLASSIC_NS_MAGIC)
        {
            order = ByteOrder.BIG_ENDIAN;
            nanosecond = be == CLASSIC_NS_MAGIC;
        }
        else
        {
            throw new IllegalArgumentException(String.format("not a pcap/pcapng file (magic 0x%08x): %s", le, path));
        }
        classicLinkType = buffer.getInt(position + 20, order) & 0x0FFF_FFFF; // upper bits carry FCS info
        position += 24;
    }

    /** Advance to the next packet record. @return false at end of file. */
    public boolean next()
    {
        return pcapng ? nextPcapng() : nextClassic();
    }

    private boolean nextClassic()
    {
        if (!ensure(16))
        {
            return endOfFile(16);
        }
        final long seconds = buffer.getInt(position, order) & 0xFFFF_FFFFL;
        final long fraction = buffer.getInt(position + 4, order) & 0xFFFF_FFFFL;
        final int capturedLength = buffer.getInt(position + 8, order);
        final int origLength = buffer.getInt(position + 12, order);
        if (capturedLength < 0 || capturedLength > MAX_FRAME)
        {
            throw new IllegalStateException("corrupt pcap record length " + capturedLength + " in " + path);
        }
        if (!ensure(16 + capturedLength))
        {
            return endOfFile(16 + capturedLength);
        }
        timestampNs = seconds * 1_000_000_000L + (nanosecond ? fraction : fraction * 1000L);
        frameLinkType = classicLinkType;
        setFrame(position + 16, capturedLength, origLength);
        position += 16 + capturedLength;
        return true;
    }

    private boolean nextPcapng()
    {
        while (true)
        {
            if (!ensure(12))
            {
                return endOfFile(12);
            }
            final int blockType = buffer.getInt(position, order);
            if (blockType == PCAPNG_SHB)
            {
                readSectionHeader();
                continue;
            }
            final int blockLength = buffer.getInt(position + 4, order);
            if (blockLength < 12 || (blockLength & 3) != 0 || blockLength > MAX_FRAME + 64)
            {
                throw new IllegalStateException("corrupt pcapng block length " + blockLength + " in " + path);
            }
            if (!ensure(blockLength))
            {
                return endOfFile(blockLength);
            }
            final int body = position + 8;
            final int blockEnd = position + blockLength;
            position = blockEnd;
            switch (blockType)
            {
                case PCAPNG_IDB -> readInterface(body, blockEnd - 4);
                case PCAPNG_EPB ->
                {
                    final int iface = buffer.getInt(body, order);
                    checkInterface(iface);
                    final long units = ((buffer.getInt(body + 4, order) & 0xFFFF_FFFFL) << 32) |
                        (buffer.getInt(body + 8, order) & 0xFFFF_FFFFL);
                    final int capturedLength = buffer.getInt(body + 12, order);
                    final int origLength = buffer.getInt(body + 16, order);
                    if (capturedLength < 0 || body + 20 + capturedLength > blockEnd - 4)
                    {
                        throw new IllegalStateException("corrupt enhanced packet block in " + path);
                    }
                    timestampNs = toNanos(iface, units);
                    frameLinkType = linkTypes[iface];
                    setFrame(body + 20, capturedLength, origLength);
                    return true;
                }
                case PCAPNG_SPB ->
                {
                    checkInterface(0);
                    final int origLength = buffer.getInt(body, order);
                    final int available = blockEnd - 4 - (body + 4);
                    int capturedLength = Math.min(origLength, available);
                    if (snapLens[0] > 0)
                    {
                        capturedLength = Math.min(capturedLength, snapLens[0]);
                    }
                    // simple packet blocks carry no timestamp: keep the previous one
                    frameLinkType = linkTypes[0];
                    setFrame(body + 4, capturedLength, origLength);
                    return true;
                }
                default ->
                {
                    // name resolution, statistics, custom blocks: skip
                }
            }
        }
    }

    private void readSectionHeader()
    {
        if (!ensure(12))
        {
            throw new IllegalStateException("truncated pcapng section header in " + path);
        }
        final int magicLe = buffer.getInt(position + 8, ByteOrder.LITTLE_ENDIAN);
        if (magicLe == PCAPNG_BYTE_ORDER_MAGIC)
        {
            order = ByteOrder.LITTLE_ENDIAN;
        }
        else if (buffer.getInt(position + 8, ByteOrder.BIG_ENDIAN) == PCAPNG_BYTE_ORDER_MAGIC)
        {
            order = ByteOrder.BIG_ENDIAN;
        }
        else
        {
            throw new IllegalStateException("bad pcapng byte-order magic in " + path);
        }
        final int blockLength = buffer.getInt(position + 4, order);
        if (blockLength < 28 || !ensure(blockLength))
        {
            throw new IllegalStateException("truncated pcapng section header in " + path);
        }
        position += blockLength;
        interfaceCount = 0; // interface ids are scoped to a section
    }

    private void readInterface(final int body, final int optionsEnd)
    {
        if (interfaceCount == MAX_INTERFACES)
        {
            throw new IllegalStateException("more than " + MAX_INTERFACES + " interfaces in " + path);
        }
        final int iface = interfaceCount++;
        linkTypes[iface] = buffer.getShort(body, order) & 0xFFFF;
        snapLens[iface] = buffer.getInt(body + 4, order);
        unitsPerSecond[iface] = 1_000_000L;
        binaryExponent[iface] = -1;
        int option = body + 8;
        while (option + 4 <= optionsEnd)
        {
            final int code = buffer.getShort(option, order) & 0xFFFF;
            final int length = buffer.getShort(option + 2, order) & 0xFFFF;
            if (code == 0)
            {
                break;
            }
            if (code == 9 && length >= 1) // if_tsresol
            {
                final int resolution = buffer.getByte(option + 4) & 0xFF;
                final int exponent = resolution & 0x7F;
                if ((resolution & 0x80) != 0)
                {
                    binaryExponent[iface] = exponent;
                }
                else
                {
                    long units = 1;
                    for (int i = 0; i < exponent && units <= Long.MAX_VALUE / 10; i++)
                    {
                        units *= 10;
                    }
                    unitsPerSecond[iface] = units;
                }
            }
            option += 4 + ((length + 3) & ~3);
        }
    }

    private long toNanos(final int iface, final long units)
    {
        final int exponent = binaryExponent[iface];
        if (exponent >= 0)
        {
            final long seconds = exponent >= 64 ? 0 : units >>> exponent;
            final long fraction = exponent >= 64 ? units : units & ((1L << exponent) - 1);
            final double fractionNs = fraction * 1e9 / Math.scalb(1.0, exponent);
            return seconds * 1_000_000_000L + (long)fractionNs;
        }
        final long perSecond = unitsPerSecond[iface];
        final long seconds = units / perSecond;
        final long remainder = units % perSecond;
        final long fractionNs = perSecond <= 1_000_000_000L ?
            remainder * (1_000_000_000L / perSecond) : remainder / (perSecond / 1_000_000_000L);
        return seconds * 1_000_000_000L + fractionNs;
    }

    private void checkInterface(final int iface)
    {
        if (iface < 0 || iface >= interfaceCount)
        {
            throw new IllegalStateException("packet references unknown interface " + iface + " in " + path);
        }
    }

    private void setFrame(final int offset, final int length, final int origLength)
    {
        frame.wrap(buffer, offset, length);
        frameLength = length;
        originalLength = origLength;
        records++;
    }

    private boolean endOfFile(final int needed)
    {
        if (limit - position > 0)
        {
            System.getLogger(PcapReader.class.getName()).log(System.Logger.Level.WARNING,
                "capture " + path + " ends with a truncated record (" + (limit - position) + " of " + needed + " bytes)");
            position = limit;
        }
        return false;
    }

    /** Make {@code bytes} available from {@code position}, compacting and refilling the buffer. */
    private boolean ensure(final int bytes)
    {
        if (limit - position >= bytes)
        {
            return true;
        }
        if (eof)
        {
            return false;
        }
        if (bytes > buffer.capacity())
        {
            throw new IllegalStateException("record of " + bytes + " bytes exceeds read buffer in " + path);
        }
        final int remaining = limit - position;
        if (remaining > 0 && position > 0)
        {
            buffer.putBytes(0, buffer, position, remaining);
        }
        position = 0;
        limit = remaining;
        try
        {
            while (limit < bytes)
            {
                byteBuffer.clear().position(limit);
                final int read = file.read(byteBuffer);
                if (read < 0)
                {
                    eof = true;
                    return false;
                }
                limit += read;
            }
            // top up opportunistically so small records do not cause a read each
            byteBuffer.clear().position(limit);
            final int read = file.read(byteBuffer);
            if (read < 0)
            {
                eof = true;
            }
            else
            {
                limit += read;
            }
        }
        catch (final IOException ex)
        {
            throw new UncheckedIOException("cannot read capture " + path, ex);
        }
        return true;
    }

    public long timestampNs()
    {
        return timestampNs;
    }

    /** Link-layer type of the current frame (LINKTYPE_* values from tcpdump.org). */
    public int linkType()
    {
        return frameLinkType;
    }

    public DirectBuffer frame()
    {
        return frame;
    }

    public int frameLength()
    {
        return frameLength;
    }

    /** Length on the wire; larger than {@link #frameLength()} if the capture was truncated by snaplen. */
    public int originalLength()
    {
        return originalLength;
    }

    public long records()
    {
        return records;
    }

    public boolean isPcapng()
    {
        return pcapng;
    }

    public Path path()
    {
        return path;
    }

    public void close()
    {
        try
        {
            file.close();
        }
        catch (final IOException ignore)
        {
        }
        BufferUtil.free(byteBuffer);
    }
}
