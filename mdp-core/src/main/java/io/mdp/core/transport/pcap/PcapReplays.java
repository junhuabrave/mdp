package io.mdp.core.transport.pcap;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.agrona.concurrent.EpochNanoClock;

/**
 * Creates {@link PcapPacketSource}s from {@code pcap:} URIs, sharing one {@link PcapReplay} per file so
 * the lines of a channel replay in captured order. Use one instance per channel.
 * <p>
 * URI: {@code pcap:<path>?dst=<ip>:<port>&src=<ip>&speed=<1|N|max>}
 * <ul>
 *   <li>{@code path}: absolute ({@code pcap:///data/cme.pcap}) or relative to the working directory
 *       ({@code pcap:captures/cme.pcap})</li>
 *   <li>{@code dst}: the line's destination group and port. Omit on at most one line to take every UDP
 *       packet that no other line matches. An IP of 0.0.0.0 matches any address on that port.</li>
 *   <li>{@code src}: optional source address filter</li>
 *   <li>{@code speed}: {@code 1} (default) for captured timing, {@code N} for N times faster,
 *       {@code max} for as fast as possible. All lines of one file must agree.</li>
 * </ul>
 */
public final class PcapReplays
{
    private final EpochNanoClock clock;
    private final Map<Path, PcapReplay> replays = new HashMap<>();

    public PcapReplays(final EpochNanoClock clock)
    {
        this.clock = clock;
    }

    public static boolean isPcapUri(final String uri)
    {
        return uri.startsWith("pcap:");
    }

    public PcapPacketSource source(final String name, final String uri)
    {
        return source(name, name, uri);
    }

    /**
     * @param name  line name, as used by the arbitrator and metrics
     * @param label name in the replay's log summary, for example {@code snapshot.A}
     */
    public PcapPacketSource source(final String name, final String label, final String uri)
    {
        if (!isPcapUri(uri))
        {
            throw new IllegalArgumentException("expected pcap: uri: " + uri);
        }
        String rest = uri.substring("pcap:".length());
        if (rest.startsWith("//"))
        {
            rest = rest.substring(2);
        }
        final int queryStart = rest.indexOf('?');
        final String pathText = queryStart < 0 ? rest : rest.substring(0, queryStart);
        if (pathText.isEmpty())
        {
            throw new IllegalArgumentException("missing file path in " + uri);
        }
        final Map<String, String> params = new HashMap<>();
        if (queryStart >= 0)
        {
            for (final String pair : rest.substring(queryStart + 1).split("&"))
            {
                if (!pair.isEmpty())
                {
                    final int eq = pair.indexOf('=');
                    if (eq <= 0)
                    {
                        throw new IllegalArgumentException("bad parameter '" + pair + "' in " + uri);
                    }
                    params.put(pair.substring(0, eq), pair.substring(eq + 1));
                }
            }
        }
        for (final String key : params.keySet())
        {
            if (!key.equals("dst") && !key.equals("src") && !key.equals("speed"))
            {
                throw new IllegalArgumentException("unknown parameter '" + key + "' in " + uri);
            }
        }

        int destinationIp = 0;
        int destinationPort = 0;
        final String dst = params.get("dst");
        if (null != dst)
        {
            final int colon = dst.lastIndexOf(':');
            if (colon <= 0)
            {
                throw new IllegalArgumentException("dst must be <ip>:<port> in " + uri);
            }
            destinationIp = UdpFrame.ipv4(dst.substring(0, colon));
            destinationPort = Integer.parseInt(dst.substring(colon + 1));
            if (destinationPort <= 0 || destinationPort > 65535)
            {
                throw new IllegalArgumentException("bad dst port in " + uri);
            }
        }
        final int sourceIp = params.containsKey("src") ? UdpFrame.ipv4(params.get("src")) : 0;
        final String speedText = params.getOrDefault("speed", "1");
        final double speed;
        if ("max".equalsIgnoreCase(speedText))
        {
            speed = PcapReplay.MAX_SPEED;
        }
        else
        {
            speed = Double.parseDouble(speedText);
            if (!(speed > 0) || Double.isInfinite(speed))
            {
                throw new IllegalArgumentException("speed must be > 0 or max in " + uri);
            }
        }

        final Path path = Path.of(pathText).toAbsolutePath().normalize();
        PcapReplay replay = replays.get(path);
        if (null == replay)
        {
            replay = new PcapReplay(path, speed, clock);
            replays.put(path, replay);
        }
        else if (replay.speed() != speed)
        {
            throw new IllegalArgumentException("lines of " + path + " configured with different speeds");
        }
        return new PcapPacketSource(name, replay, replay.addLine(label, destinationIp, destinationPort, sourceIp));
    }
}
