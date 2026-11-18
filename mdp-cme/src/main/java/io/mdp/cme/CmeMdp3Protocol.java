package io.mdp.cme;

import io.mdp.core.api.DecoderContext;
import io.mdp.core.api.FeedDecoder;
import io.mdp.core.api.FeedProtocol;
import io.mdp.core.api.PacketHeaderReader;

/**
 * CME MDP 3.0 plug-in ({@code mdp.protocol=cme-mdp3}).
 * <p>
 * Settings:
 * <ul>
 *   <li>{@code mdp.cme.gapPolicy} = {@code CHANNEL_STALE} (default) | {@code INSTRUMENT_RPTSEQ}</li>
 *   <li>{@code mdp.recovery.startLive} = false (default). If true, instruments start live, with no
 *       snapshot needed. Only for benchmarks and replays that begin at a known clean state.</li>
 * </ul>
 */
public final class CmeMdp3Protocol implements FeedProtocol
{
    public static final String NAME = "cme-mdp3";
    public static final int VENUE_ID = 1;

    public String name()
    {
        return NAME;
    }

    public int venueId()
    {
        return VENUE_ID;
    }

    public PacketHeaderReader packetHeader()
    {
        return CmePacketHeader.INSTANCE;
    }

    public FeedDecoder newDecoder(final DecoderContext context)
    {
        final CmeFeedDecoder.GapPolicy policy = CmeFeedDecoder.GapPolicy.valueOf(
            context.config().channelSetting(context.channelId(), "cme.gapPolicy", "CHANNEL_STALE"));
        final boolean startLive = context.config().getBoolean("mdp.recovery.startLive", false);
        return new CmeFeedDecoder(context.channelId(), context.sink(), context.metrics(), policy, startLive);
    }
}
