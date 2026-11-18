package io.mdp.cme;

import java.nio.ByteOrder;

import io.mdp.core.api.PacketHeaderReader;
import org.agrona.DirectBuffer;

/**
 * MDP 3.0 binary packet header: {@code MsgSeqNum:uint32, SendingTime:uint64}, little-endian.
 */
public final class CmePacketHeader implements PacketHeaderReader
{
    public static final int LENGTH = 12;
    public static final int SEQ_OFFSET = 0;
    public static final int SENDING_TIME_OFFSET = 4;
    /** Each message in a packet is prefixed with {@code MsgSize:uint16}, which includes itself. */
    public static final int MESSAGE_SIZE_LENGTH = 2;

    public static final CmePacketHeader INSTANCE = new CmePacketHeader();

    public int headerLength()
    {
        return LENGTH;
    }

    public long sequence(final DirectBuffer buffer, final int offset)
    {
        return buffer.getInt(offset + SEQ_OFFSET, ByteOrder.LITTLE_ENDIAN) & 0xFFFF_FFFFL;
    }

    public long sendingTimeNs(final DirectBuffer buffer, final int offset)
    {
        return buffer.getLong(offset + SENDING_TIME_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }
}
