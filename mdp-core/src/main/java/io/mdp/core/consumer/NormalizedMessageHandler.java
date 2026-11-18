package io.mdp.core.consumer;

import org.agrona.DirectBuffer;

/**
 * Receives one complete normalized SBE message (header included) from any transport:
 * ring buffer, Aeron, or the inline sink.
 */
@FunctionalInterface
public interface NormalizedMessageHandler
{
    void onMessage(DirectBuffer buffer, int offset, int length);
}
