package io.mdp.core.sink;

import io.mdp.core.consumer.NormalizedMessageHandler;
import io.mdp.core.metrics.MetricsRegistry;
import org.agrona.BufferUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * {@code inline} pipeline mode: encode into a reusable buffer, then call the consumer handler
 * on the receiver thread right away. No thread hand-off; listeners must be fast.
 */
public final class DirectDispatchSink extends AbstractNormalizedSink
{
    private final UnsafeBuffer buffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(1024, 64));
    private final NormalizedMessageHandler handler;
    private int length;

    public DirectDispatchSink(final NormalizedMessageHandler handler, final MetricsRegistry metrics, final String name)
    {
        super(BackPressurePolicy.SPIN, metrics, name);
        this.handler = handler;
    }

    protected boolean tryClaim(final int templateId, final int length)
    {
        this.length = length;
        return true;
    }

    protected MutableDirectBuffer claimBuffer()
    {
        return buffer;
    }

    protected int claimOffset()
    {
        return 0;
    }

    protected void commitClaim()
    {
        handler.onMessage(buffer, 0, length);
    }
}
