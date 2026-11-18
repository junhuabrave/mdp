package io.mdp.client.consumer;

import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.concurrent.Agent;

/**
 * Consumes the normalized stream from Aeron (split deployment). Normalized messages are small
 * and fixed length, so they are never fragmented and no assembler is needed.
 */
public final class AeronConsumerAgent implements Agent
{
    private final Subscription subscription;
    private final FragmentHandler fragmentHandler;
    private final int fragmentLimit;
    private final String roleName;

    public AeronConsumerAgent(
        final Subscription subscription, final NormalizedMessageHandler handler, final int fragmentLimit)
    {
        this.subscription = subscription;
        this.fragmentHandler = (buffer, offset, length, header) -> handler.onMessage(buffer, offset, length);
        this.fragmentLimit = fragmentLimit;
        this.roleName = "consumer-aeron-" + subscription.streamId();
    }

    public int doWork()
    {
        return subscription.poll(fragmentHandler, fragmentLimit);
    }

    public String roleName()
    {
        return roleName;
    }
}
