package io.mdp.core.channel;

import java.util.List;
import java.util.concurrent.TimeUnit;

import io.aeron.Aeron;
import io.mdp.core.api.FeedType;
import io.mdp.core.arbitration.LineArbitrator;
import io.mdp.core.config.MdpConfig;
import io.mdp.core.transport.AeronPacketSource;
import io.mdp.core.transport.PacketSource;
import io.mdp.core.transport.UdpPacketSource;
import io.mdp.core.transport.pcap.PcapReplays;
import org.agrona.concurrent.EpochNanoClock;

/**
 * Builds packet sources and arbitration settings from configuration.
 * <ul>
 *   <li>{@code udp://...} gives a {@link UdpPacketSource}</li>
 *   <li>{@code aeron:...} gives an {@link AeronPacketSource}; the stream id comes from {@code <key>.streamId}</li>
 *   <li>{@code pcap:...} replays a capture file (see {@link PcapReplays}); pass the same {@link PcapReplays}
 *       for all feeds of a channel so its lines replay in captured order</li>
 * </ul>
 */
public final class ChannelFactory
{
    private ChannelFactory()
    {
    }

    public static PacketSource[] sources(
        final MdpConfig config,
        final int channelId,
        final FeedType feed,
        final Aeron aeron,
        final EpochNanoClock clock)
    {
        return sources(config, channelId, feed, aeron, clock, new PcapReplays(clock));
    }

    public static PacketSource[] sources(
        final MdpConfig config,
        final int channelId,
        final FeedType feed,
        final Aeron aeron,
        final EpochNanoClock clock,
        final PcapReplays pcapReplays)
    {
        final List<String> lines = config.lines(channelId, feed);
        final PacketSource[] sources = new PacketSource[lines.size()];
        for (int i = 0; i < lines.size(); i++)
        {
            final String line = lines.get(i);
            final String uri = config.lineUri(channelId, feed, line);
            if (uri.startsWith("udp://"))
            {
                sources[i] = new UdpPacketSource(line, uri, clock);
            }
            else if (uri.startsWith("aeron:"))
            {
                if (null == aeron)
                {
                    throw new IllegalStateException("aeron source configured but no Aeron client: " + uri);
                }
                final int streamId = config.lineStreamId(channelId, feed, line, -1);
                if (streamId < 0)
                {
                    throw new IllegalArgumentException("missing streamId for " + uri);
                }
                sources[i] = new AeronPacketSource(line, aeron.addSubscription(uri, streamId));
            }
            else if (PcapReplays.isPcapUri(uri))
            {
                sources[i] = pcapReplays.source(line, feed.name().toLowerCase() + "." + line, uri);
            }
            else
            {
                throw new IllegalArgumentException("unsupported source uri: " + uri);
            }
        }
        return sources;
    }

    public static LineArbitrator.Config arbitration(final MdpConfig config)
    {
        return new LineArbitrator.Config(
            TimeUnit.MICROSECONDS.toNanos(config.getLong("mdp.arbitration.gapTimeoutMicros", 500)),
            config.getInt("mdp.arbitration.reorderSlots", 1024),
            config.getInt("mdp.arbitration.slotSize", 1500),
            TimeUnit.MILLISECONDS.toNanos(config.getLong("mdp.line.timeoutMillis", 2000)),
            config.getLong("mdp.arbitration.resetSeqMax", 1));
    }
}
