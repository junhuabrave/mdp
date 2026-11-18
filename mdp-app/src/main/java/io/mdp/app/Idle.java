package io.mdp.app;

import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.agrona.concurrent.YieldingIdleStrategy;

final class Idle
{
    private Idle()
    {
    }

    /** busy-spin | yield | backoff | sleep */
    static IdleStrategy parse(final String name)
    {
        return switch (name)
        {
            case "busy-spin" -> new BusySpinIdleStrategy();
            case "yield" -> new YieldingIdleStrategy();
            case "sleep" -> new SleepingMillisIdleStrategy(1);
            case "backoff" -> new BackoffIdleStrategy();
            default -> throw new IllegalArgumentException("unknown idle strategy: " + name);
        };
    }
}
