package io.mdp.core.sink;

import io.mdp.core.metrics.MetricsRegistry;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;

/**
 * Writes normalized events straight into an Agrona ring buffer (zero copy). The record type id
 * is the SBE template id, and the record bytes are a complete SBE message with header.
 */
public final class RingBufferSink extends AbstractNormalizedSink
{
    private final RingBuffer ringBuffer;
    private int claimIndex;

    public RingBufferSink(
        final RingBuffer ringBuffer, final BackPressurePolicy policy, final MetricsRegistry metrics, final String name)
    {
        super(policy, metrics, name);
        this.ringBuffer = ringBuffer;
    }

    protected boolean tryClaim(final int templateId, final int length)
    {
        final int index = ringBuffer.tryClaim(templateId, length);
        if (index > 0)
        {
            claimIndex = index;
            return true;
        }
        return false;
    }

    protected MutableDirectBuffer claimBuffer()
    {
        return ringBuffer.buffer();
    }

    protected int claimOffset()
    {
        return claimIndex;
    }

    protected void commitClaim()
    {
        ringBuffer.commit(claimIndex);
    }
}
