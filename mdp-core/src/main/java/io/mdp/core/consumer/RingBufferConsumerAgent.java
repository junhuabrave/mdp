package io.mdp.core.consumer;

import io.mdp.core.metrics.MetricsRegistry;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.agrona.concurrent.status.AtomicCounter;

/**
 * Drains the in-process ring buffer and hands each record to a {@link NormalizedMessageHandler}.
 */
public final class RingBufferConsumerAgent implements Agent
{
    private final RingBuffer ringBuffer;
    private final MessageHandler messageHandler;
    private final int readLimit;
    private final AtomicCounter utilization;
    private final String roleName;
    private int cycles;

    public RingBufferConsumerAgent(
        final RingBuffer ringBuffer,
        final NormalizedMessageHandler handler,
        final int readLimit,
        final MetricsRegistry metrics,
        final String name)
    {
        this.ringBuffer = ringBuffer;
        this.messageHandler = (msgTypeId, buffer, index, length) -> handler.onMessage(buffer, index, length);
        this.readLimit = readLimit;
        this.utilization = metrics.gauge("mdp.ringbuffer.utilization.pct", "ring", name);
        this.roleName = "consumer-" + name;
    }

    public int doWork()
    {
        if ((++cycles & 0x3FF) == 0)
        {
            utilization.setRelease(100L * ringBuffer.size() / ringBuffer.capacity());
        }
        return ringBuffer.read(messageHandler, readLimit);
    }

    public String roleName()
    {
        return roleName;
    }
}
