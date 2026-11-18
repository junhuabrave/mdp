package io.mdp.core.arbitration;

import org.agrona.DirectBuffer;

/**
 * Output of a {@link LineArbitrator}: packets in strict sequence order, plus gap, reset and line events.
 */
public interface SequencedPacketHandler
{
    void onPacket(DirectBuffer buffer, int offset, int length, long seq, long receiveTimeNs);

    /** Packets {@code [fromSeq, toSeq]} (inclusive) were missed on all lines. */
    void onGap(long fromSeq, long toSeq);

    void onSequenceReset(long newSeq);

    void onLineStatus(int line, boolean up);
}
