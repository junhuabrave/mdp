package io.mdp.core.channel;

import io.mdp.core.metrics.MetricsRegistry;
import org.agrona.CloseHelper;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.status.AtomicCounter;

/**
 * Receiver agent: one duty cycle polls every channel it owns. The clock is read once per cycle and
 * shared by all channels' timers (review R2-7).
 */
public final class FeedHandlerAgent implements Agent
{
    private final ChannelHandler[] channels;
    private final String roleName;
    private final AtomicCounter dutyCycles;
    private final AtomicCounter maxCycleNs;
    private long cycleCount;
    private long windowMaxNs;

    public FeedHandlerAgent(final String name, final ChannelHandler[] channels, final MetricsRegistry metrics)
    {
        this.channels = channels;
        this.roleName = "feed-handler-" + name;
        this.dutyCycles = metrics.counter("mdp.agent.dutycycles", "agent", roleName);
        this.maxCycleNs = metrics.gauge("mdp.agent.maxcycle.ns", "agent", roleName);
    }

    public int doWork()
    {
        final long start = System.nanoTime();
        int work = 0;
        for (final ChannelHandler channel : channels)
        {
            work += channel.doWork(start);
        }

        if (work > 0)
        {
            final long cycleNs = System.nanoTime() - start;
            if (cycleNs > windowMaxNs)
            {
                windowMaxNs = cycleNs;
            }
        }
        if ((++cycleCount & 0xFFFF) == 0)
        {
            dutyCycles.setRelease(cycleCount);
            maxCycleNs.setRelease(windowMaxNs);
            windowMaxNs = 0;
        }
        return work;
    }

    public String roleName()
    {
        return roleName;
    }

    public void onClose()
    {
        for (final ChannelHandler channel : channels)
        {
            CloseHelper.quietClose(channel);
        }
    }
}
