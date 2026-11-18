package io.mdp.core.transport;

import org.agrona.DirectBuffer;

@FunctionalInterface
public interface PacketHandler
{
    /**
     * @param receiveTimeNs epoch nanoseconds when the packet was read from the transport
     */
    void onPacket(DirectBuffer buffer, int offset, int length, long receiveTimeNs);
}
