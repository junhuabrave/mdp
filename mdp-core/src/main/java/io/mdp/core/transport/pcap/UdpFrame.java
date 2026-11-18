package io.mdp.core.transport.pcap;

import java.nio.ByteOrder;

import org.agrona.DirectBuffer;

/**
 * Extracts the UDP payload from a captured link-layer frame, in place.
 * <p>
 * Link types: Ethernet (with any number of 802.1Q / 802.1ad tags), Linux cooked v1 and v2, raw IPv4,
 * and BSD loopback. Only IPv4 UDP is accepted. IP fragments are rejected: exchange feeds do not
 * fragment, and a fragment cannot be decoded on its own. Lengths come from the IP and UDP headers,
 * so Ethernet padding on short frames is excluded.
 */
public final class UdpFrame
{
    public static final int LINKTYPE_NULL = 0;
    public static final int LINKTYPE_ETHERNET = 1;
    public static final int LINKTYPE_RAW_BSD = 12;
    public static final int LINKTYPE_RAW = 101;
    public static final int LINKTYPE_LOOP = 108;
    public static final int LINKTYPE_LINUX_SLL = 113;
    public static final int LINKTYPE_IPV4 = 228;
    public static final int LINKTYPE_LINUX_SLL2 = 276;

    private static final int ETHERTYPE_IPV4 = 0x0800;
    private static final int ETHERTYPE_VLAN = 0x8100;
    private static final int ETHERTYPE_QINQ = 0x88A8;
    private static final int IPPROTO_UDP = 17;

    public enum Result
    {
        UDP, NOT_IPV4, NOT_UDP, FRAGMENT, TRUNCATED, UNSUPPORTED_LINK
    }

    private int sourceIp;
    private int sourcePort;
    private int destinationIp;
    private int destinationPort;
    private int payloadOffset;
    private int payloadLength;

    public Result parse(final int linkType, final DirectBuffer frame, final int offset, final int length)
    {
        final int end = offset + length;
        int ip;
        switch (linkType)
        {
            case LINKTYPE_ETHERNET ->
            {
                int typeOffset = offset + 12;
                if (typeOffset + 2 > end)
                {
                    return Result.TRUNCATED;
                }
                int etherType = frame.getShort(typeOffset, ByteOrder.BIG_ENDIAN) & 0xFFFF;
                while (etherType == ETHERTYPE_VLAN || etherType == ETHERTYPE_QINQ)
                {
                    typeOffset += 4;
                    if (typeOffset + 2 > end)
                    {
                        return Result.TRUNCATED;
                    }
                    etherType = frame.getShort(typeOffset, ByteOrder.BIG_ENDIAN) & 0xFFFF;
                }
                if (etherType != ETHERTYPE_IPV4)
                {
                    return Result.NOT_IPV4;
                }
                ip = typeOffset + 2;
            }
            case LINKTYPE_LINUX_SLL ->
            {
                if (offset + 16 > end)
                {
                    return Result.TRUNCATED;
                }
                if ((frame.getShort(offset + 14, ByteOrder.BIG_ENDIAN) & 0xFFFF) != ETHERTYPE_IPV4)
                {
                    return Result.NOT_IPV4;
                }
                ip = offset + 16;
            }
            case LINKTYPE_LINUX_SLL2 ->
            {
                if (offset + 20 > end)
                {
                    return Result.TRUNCATED;
                }
                if ((frame.getShort(offset, ByteOrder.BIG_ENDIAN) & 0xFFFF) != ETHERTYPE_IPV4)
                {
                    return Result.NOT_IPV4;
                }
                ip = offset + 20;
            }
            case LINKTYPE_RAW, LINKTYPE_RAW_BSD, LINKTYPE_IPV4 -> ip = offset;
            case LINKTYPE_NULL, LINKTYPE_LOOP ->
            {
                if (offset + 4 > end)
                {
                    return Result.TRUNCATED;
                }
                // address family, host byte order for NULL, network order for LOOP; AF_INET is 2 everywhere
                final int family = linkType == LINKTYPE_LOOP ?
                    frame.getInt(offset, ByteOrder.BIG_ENDIAN) : guessHostOrderFamily(frame, offset);
                if (family != 2)
                {
                    return Result.NOT_IPV4;
                }
                ip = offset + 4;
            }
            default ->
            {
                return Result.UNSUPPORTED_LINK;
            }
        }
        return parseIpv4(frame, ip, end);
    }

    private static int guessHostOrderFamily(final DirectBuffer frame, final int offset)
    {
        final int little = frame.getInt(offset, ByteOrder.LITTLE_ENDIAN);
        return little <= 0xFFFF ? little : frame.getInt(offset, ByteOrder.BIG_ENDIAN);
    }

    private Result parseIpv4(final DirectBuffer frame, final int ip, final int end)
    {
        if (ip + 20 > end)
        {
            return Result.TRUNCATED;
        }
        final int versionIhl = frame.getByte(ip) & 0xFF;
        if (versionIhl >> 4 != 4)
        {
            return Result.NOT_IPV4;
        }
        final int headerLength = (versionIhl & 0x0F) * 4;
        final int totalLength = frame.getShort(ip + 2, ByteOrder.BIG_ENDIAN) & 0xFFFF;
        final int flagsFragment = frame.getShort(ip + 6, ByteOrder.BIG_ENDIAN) & 0xFFFF;
        if ((flagsFragment & 0x3FFF) != 0) // more-fragments flag or non-zero offset
        {
            return Result.FRAGMENT;
        }
        if ((frame.getByte(ip + 9) & 0xFF) != IPPROTO_UDP)
        {
            return Result.NOT_UDP;
        }
        final int udp = ip + headerLength;
        // totalLength 0 happens with TSO/offloaded captures on the sending host: fall back to the frame
        final int ipEnd = totalLength == 0 ? end : ip + totalLength;
        if (headerLength < 20 || udp + 8 > end || ipEnd > end)
        {
            return Result.TRUNCATED;
        }
        final int udpLength = frame.getShort(udp + 4, ByteOrder.BIG_ENDIAN) & 0xFFFF;
        final int udpEnd = udpLength >= 8 ? Math.min(udp + udpLength, ipEnd) : ipEnd;
        sourceIp = frame.getInt(ip + 12, ByteOrder.BIG_ENDIAN);
        destinationIp = frame.getInt(ip + 16, ByteOrder.BIG_ENDIAN);
        sourcePort = frame.getShort(udp, ByteOrder.BIG_ENDIAN) & 0xFFFF;
        destinationPort = frame.getShort(udp + 2, ByteOrder.BIG_ENDIAN) & 0xFFFF;
        payloadOffset = udp + 8;
        payloadLength = udpEnd - payloadOffset;
        return Result.UDP;
    }

    public int sourceIp()
    {
        return sourceIp;
    }

    public int sourcePort()
    {
        return sourcePort;
    }

    public int destinationIp()
    {
        return destinationIp;
    }

    public int destinationPort()
    {
        return destinationPort;
    }

    public int payloadOffset()
    {
        return payloadOffset;
    }

    public int payloadLength()
    {
        return payloadLength;
    }

    /** Dotted-quad IPv4 address as a big-endian int; for building filters. */
    public static int ipv4(final String dottedQuad)
    {
        final String[] parts = dottedQuad.split("\\.");
        if (parts.length != 4)
        {
            throw new IllegalArgumentException("not an IPv4 address: " + dottedQuad);
        }
        int value = 0;
        for (final String part : parts)
        {
            final int octet = Integer.parseInt(part);
            if (octet < 0 || octet > 255)
            {
                throw new IllegalArgumentException("not an IPv4 address: " + dottedQuad);
            }
            value = (value << 8) | octet;
        }
        return value;
    }

    public static String ipv4(final int address)
    {
        return (address >>> 24) + "." + ((address >>> 16) & 0xFF) + "." + ((address >>> 8) & 0xFF) + "." + (address & 0xFF);
    }
}
