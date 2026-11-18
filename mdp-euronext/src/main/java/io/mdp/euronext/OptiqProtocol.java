package io.mdp.euronext;

import io.mdp.core.api.DecoderContext;
import io.mdp.core.api.FeedDecoder;
import io.mdp.core.api.FeedProtocol;
import io.mdp.core.api.PacketHeaderReader;

/**
 * Euronext Optiq MDG plug-in ({@code mdp.protocol=euronext-optiq}).
 * <p>
 * Settings:
 * <ul>
 *   <li>{@code mdp.optiq.defaultPriceDecimals} (default 4): used until StandingData gives a symbol's decimals</li>
 *   <li>{@code mdp.optiq.replayBufferPackets} (default 65536): incremental packets kept during recovery</li>
 *   <li>{@code mdp.recovery.startLive} (default false)</li>
 * </ul>
 */
public final class OptiqProtocol implements FeedProtocol
{
    public static final String NAME = "euronext-optiq";
    public static final int VENUE_ID = 2;

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
        return OptiqPacketHeader.INSTANCE;
    }

    public FeedDecoder newDecoder(final DecoderContext context)
    {
        return new OptiqFeedDecoder(
            context.channelId(),
            context.sink(),
            context.metrics(),
            context.config().getInt("mdp.optiq.defaultPriceDecimals", 4),
            context.config().getInt("mdp.optiq.replayBufferPackets", 65536),
            context.config().getBoolean("mdp.recovery.startLive", false));
    }
}
