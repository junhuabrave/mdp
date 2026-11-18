package io.mdp.core.consumer;

import io.mdp.core.metrics.LatencyRecorder;
import io.mdp.sbe.BookUpdateDecoder;
import io.mdp.sbe.TradeDecoder;
import org.agrona.concurrent.EpochNanoClock;

/**
 * Records end-to-end latency (local packet receive to listener) for book updates and trades.
 * Snapshot-sourced events are excluded: they reflect recovery, not live latency.
 */
public final class LatencyProbe implements MarketDataListener
{

    private final EpochNanoClock clock;
    private final LatencyRecorder e2e;
    private final LatencyRecorder wire;

    public LatencyProbe(final EpochNanoClock clock, final LatencyRecorder e2e, final LatencyRecorder wire)
    {
        this.clock = clock;
        this.e2e = e2e;
        this.wire = wire;
    }

    public void onBookUpdate(final BookUpdateDecoder update)
    {
        if (!update.flags().snapshot())
        {
            final long receiveTimeNs = update.receiveTimeNs();
            e2e.record(clock.nanoTime() - receiveTimeNs);
            if (null != wire)
            {
                wire.record(receiveTimeNs - update.exchangeTimeNs());
            }
        }
    }

    public void onTrade(final TradeDecoder trade)
    {
        if (!trade.flags().snapshot())
        {
            e2e.record(clock.nanoTime() - trade.receiveTimeNs());
        }
    }
}
