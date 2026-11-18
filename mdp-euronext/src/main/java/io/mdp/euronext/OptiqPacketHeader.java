package io.mdp.euronext;

import java.nio.ByteOrder;

import io.mdp.core.api.PacketHeaderReader;
import org.agrona.DirectBuffer;

/**
 * Optiq MDG packet header (as modeled here, verify against the current spec):
 * {@code SendingTime:uint64, PacketSequenceNumber:uint32, PacketFlags:uint16, ChannelID:uint16}.
 */
public final class OptiqPacketHeader implements PacketHeaderReader
{
    public static final int LENGTH = 16;
    public static final int SENDING_TIME_OFFSET = 0;
    public static final int SEQ_OFFSET = 8;
    public static final int FLAGS_OFFSET = 12;
    public static final int CHANNEL_OFFSET = 14;

    public static final OptiqPacketHeader INSTANCE = new OptiqPacketHeader();

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
