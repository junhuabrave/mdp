package io.mdp.core.transport.pcap;

import io.mdp.core.transport.PacketHandler;
import io.mdp.core.transport.PacketSource;

/**
 * One line of a {@link PcapReplay}. Created by {@link PcapReplays} from a {@code pcap:} URI.
 */
public final class PcapPacketSource implements PacketSource
{
    private final String name;
    private final PcapReplay replay;
    private final int lineIndex;
    private boolean closed;

    public PcapPacketSource(final String name, final PcapReplay replay, final int lineIndex)
    {
        this.name = name;
        this.replay = replay;
        this.lineIndex = lineIndex;
    }

    public int poll(final PacketHandler handler, final int limit)
    {
        return replay.poll(lineIndex, handler, limit);
    }

    public void activate()
    {
        replay.setActive(lineIndex, true);
    }

    public void deactivate()
    {
        replay.setActive(lineIndex, false);
    }

    public PcapReplay replay()
    {
        return replay;
    }

    public String name()
    {
        return name;
    }

    public void close()
    {
        if (!closed)
        {
            closed = true;
            replay.release();
        }
    }
}
