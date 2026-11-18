package io.mdp.core.api;

import org.agrona.DirectBuffer;

/**
 * Venue-specific, stateless reader for the packet header. Arbitration only needs the packet
 * sequence number, so the core stays venue-agnostic.
 */
public interface PacketHeaderReader
{
    int headerLength();

    long sequence(DirectBuffer buffer, int offset);

    long sendingTimeNs(DirectBuffer buffer, int offset);

    /** A packet with a header and no messages; it proves the line is alive but carries no data. */
    default boolean isHeartbeat(final DirectBuffer buffer, final int offset, final int length)
    {
        return length <= headerLength();
    }
}
