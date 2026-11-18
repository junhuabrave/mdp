package io.mdp.core.sink;

import io.aeron.ExclusivePublication;
import io.aeron.logbuffer.BufferClaim;
import io.mdp.core.metrics.MetricsRegistry;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.status.AtomicCounter;

/**
 * Writes normalized events into an Aeron publication with {@code tryClaim} (zero copy), for
 * split deployments where consumers live in other processes or hosts.
 */
public final class AeronSink extends AbstractNormalizedSink
{
    private final ExclusivePublication publication;
    private final BufferClaim bufferClaim = new BufferClaim();
    private final AtomicCounter backPressured;
    private final AtomicCounter notConnected;

    public AeronSink(
        final ExclusivePublication publication,
        final BackPressurePolicy policy,
        final MetricsRegistry metrics,
        final String name)
    {
        super(policy, metrics, name);
        this.publication = publication;
        this.backPressured = metrics.counter("mdp.sink.backpressure", "sink", name);
        this.notConnected = metrics.counter("mdp.sink.not_connected", "sink", name);
    }

    protected boolean tryClaim(final int templateId, final int length)
    {
        final long result = publication.tryClaim(length, bufferClaim);
        if (result > 0)
        {
            return true;
        }
        if (result == ExclusivePublication.NOT_CONNECTED)
        {
            MetricsRegistry.increment(notConnected);
        }
        else
        {
            MetricsRegistry.increment(backPressured);
        }
        return false;
    }

    protected MutableDirectBuffer claimBuffer()
    {
        return bufferClaim.buffer();
    }

    protected int claimOffset()
    {
        return bufferClaim.offset();
    }

    protected void commitClaim()
    {
        bufferClaim.commit();
    }
}
