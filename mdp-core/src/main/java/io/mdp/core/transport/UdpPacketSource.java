package io.mdp.core.transport;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.MembershipKey;
import java.util.HashMap;
import java.util.Map;

import org.agrona.BufferUtil;
import org.agrona.LangUtil;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Non-blocking NIO UDP source for unicast or multicast (any-source or source-specific).
 * <p>
 * URI: {@code udp://<group-or-host>:<port>?interface=<name-or-ip>&source=<ip>&rcvbuf=<bytes>}
 * <p>
 * Reads go into a direct buffer wrapped by an {@link UnsafeBuffer}, so decoders read packet bytes
 * in place. Since JDK 11, {@code DatagramChannel.receive} reuses the cached sender address for a
 * steady sender, so steady-state receives do not allocate.
 * <p>
 * Multicast sources leave the group on {@link #deactivate()} so the kernel does not queue stale
 * snapshot packets (review R2-2).
 */
public final class UdpPacketSource implements PacketSource
{
    public static final int DEFAULT_RCVBUF = 16 * 1024 * 1024;
    private static final int MAX_PACKET = 65536;

    private final String name;
    private final DatagramChannel channel;
    private final ByteBuffer byteBuffer = BufferUtil.allocateDirectAligned(MAX_PACKET, 64);
    private final UnsafeBuffer buffer = new UnsafeBuffer(byteBuffer);
    private final EpochNanoClock clock;
    private final InetAddress group;
    private final NetworkInterface networkInterface;
    private final InetAddress sourceAddress;
    private MembershipKey membership;
    private boolean active = true;

    public UdpPacketSource(final String name, final String uri, final EpochNanoClock clock)
    {
        this.name = name;
        this.clock = clock;
        try
        {
            final URI parsed = URI.create(uri);
            if (!"udp".equals(parsed.getScheme()))
            {
                throw new IllegalArgumentException("expected udp:// uri: " + uri);
            }
            final Map<String, String> params = query(parsed.getRawQuery());
            final InetAddress address = InetAddress.getByName(parsed.getHost());
            final int port = parsed.getPort();

            channel = DatagramChannel.open(StandardProtocolFamily.INET);
            channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            channel.setOption(StandardSocketOptions.SO_RCVBUF,
                Integer.parseInt(params.getOrDefault("rcvbuf", String.valueOf(DEFAULT_RCVBUF))));

            if (address.isMulticastAddress())
            {
                group = address;
                networkInterface = resolveInterface(params.get("interface"));
                sourceAddress = params.containsKey("source") ? InetAddress.getByName(params.get("source")) : null;
                // Linux: binding to the group address filters other groups on the same port.
                final boolean linux = System.getProperty("os.name", "").toLowerCase().contains("linux");
                channel.bind(new InetSocketAddress(linux ? address : InetAddress.getByName("0.0.0.0"), port));
                channel.setOption(StandardSocketOptions.IP_MULTICAST_IF, networkInterface);
                join();
            }
            else
            {
                group = null;
                networkInterface = null;
                sourceAddress = null;
                channel.bind(new InetSocketAddress(address, port));
            }
            channel.configureBlocking(false);
        }
        catch (final IOException ex)
        {
            throw new IllegalStateException("cannot open " + name + " " + uri, ex);
        }
    }

    public int poll(final PacketHandler handler, final int limit)
    {
        if (!active)
        {
            return 0;
        }

        int count = 0;
        try
        {
            while (count < limit)
            {
                byteBuffer.clear();
                if (null == channel.receive(byteBuffer))
                {
                    break;
                }
                handler.onPacket(buffer, 0, byteBuffer.position(), clock.nanoTime());
                count++;
            }
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
        return count;
    }

    public void activate()
    {
        if (!active)
        {
            drain();
            if (null != group)
            {
                join();
            }
            active = true;
        }
    }

    public void deactivate()
    {
        if (active)
        {
            active = false;
            if (null != membership)
            {
                membership.drop();
                membership = null;
            }
            drain();
        }
    }

    public String name()
    {
        return name;
    }

    public void close()
    {
        try
        {
            channel.close();
        }
        catch (final IOException ignore)
        {
        }
    }

    private void join()
    {
        try
        {
            membership = null == sourceAddress ?
                channel.join(group, networkInterface) : channel.join(group, networkInterface, sourceAddress);
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    private void drain()
    {
        try
        {
            do
            {
                byteBuffer.clear();
            }
            while (null != channel.receive(byteBuffer));
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    private static NetworkInterface resolveInterface(final String spec) throws IOException
    {
        if (null != spec && !spec.isEmpty())
        {
            NetworkInterface nif = NetworkInterface.getByName(spec);
            if (null == nif)
            {
                nif = NetworkInterface.getByInetAddress(InetAddress.getByName(spec));
            }
            if (null == nif)
            {
                throw new IllegalArgumentException("unknown interface: " + spec);
            }
            return nif;
        }

        final var interfaces = NetworkInterface.getNetworkInterfaces();
        NetworkInterface loopback = null;
        while (interfaces.hasMoreElements())
        {
            final NetworkInterface nif = interfaces.nextElement();
            if (nif.isUp() && nif.supportsMulticast())
            {
                if (!nif.isLoopback())
                {
                    return nif;
                }
                loopback = nif;
            }
        }
        if (null == loopback)
        {
            throw new IllegalStateException("no multicast-capable interface");
        }
        return loopback;
    }

    private static Map<String, String> query(final String rawQuery)
    {
        final Map<String, String> params = new HashMap<>();
        if (null != rawQuery)
        {
            for (final String kv : rawQuery.split("&"))
            {
                final int eq = kv.indexOf('=');
                if (eq > 0)
                {
                    params.put(kv.substring(0, eq), kv.substring(eq + 1));
                }
            }
        }
        return params;
    }
}
