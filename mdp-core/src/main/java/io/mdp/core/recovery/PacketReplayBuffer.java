package io.mdp.core.recovery;

import io.mdp.core.arbitration.SequencedPacketHandler;
import org.agrona.BufferUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Bounded FIFO of in-order incremental packets, kept while a channel recovers from a snapshot.
 * When the snapshot completes at sequence L, the packets after L are replayed. Pre-allocated, no
 * allocation while running.
 * <p>
 * The arbitrator delivers packets in order, and the owner calls {@link #clear()} on every gap, so
 * the buffer always holds a contiguous sequence range.
 */
public final class PacketReplayBuffer
{
    private static final long UNSET = -1;

    private final UnsafeBuffer data;
    private final int slotSize;
    private final long[] seqs;
    private final int[] lengths;
    private final long[] receiveTimes;
    private final int capacity;
    private long head;
    private long tail;
    private long droppedUpTo = UNSET;

    public PacketReplayBuffer(final int capacity, final int slotSize)
    {
        this.capacity = capacity;
        this.slotSize = slotSize;
        this.data = new UnsafeBuffer(BufferUtil.allocateDirectAligned(capacity * slotSize, 64));
        this.seqs = new long[capacity];
        this.lengths = new int[capacity];
        this.receiveTimes = new long[capacity];
    }

    /**
     * Store a packet. When full, the oldest packet is evicted and remembered in {@link #droppedUpTo()}
     * (a snapshot must then be newer than that sequence). Oversize packets evict everything.
     */
    public void add(
        final DirectBuffer buffer, final int offset, final int length, final long seq, final long receiveTimeNs)
    {
        if (length > slotSize)
        {
            droppedUpTo = seq;
            clear();
            return;
        }
        if (tail - head == capacity)
        {
            droppedUpTo = seqs[(int)(head % capacity)];
            head++;
        }
        final int index = (int)(tail % capacity);
        data.putBytes(index * slotSize, buffer, offset, length);
        seqs[index] = seq;
        lengths[index] = length;
        receiveTimes[index] = receiveTimeNs;
        tail++;
    }

    public void clear()
    {
        head = tail;
    }

    /** Reset the eviction marker, for example when a new recovery starts. */
    public void resetDropped()
    {
        droppedUpTo = UNSET;
    }

    public boolean isEmpty()
    {
        return head == tail;
    }

    public int size()
    {
        return (int)(tail - head);
    }

    /** Lowest buffered sequence, or -1 if empty. */
    public long firstSeq()
    {
        return isEmpty() ? UNSET : seqs[(int)(head % capacity)];
    }

    /** Highest sequence evicted for lack of space, or -1. */
    public long droppedUpTo()
    {
        return droppedUpTo;
    }

    /** Replay buffered packets with sequence greater than {@code afterSeq}, then clear. */
    public int replay(final long afterSeq, final SequencedPacketHandler handler)
    {
        int count = 0;
        while (head < tail)
        {
            final int index = (int)(head % capacity);
            head++;
            if (seqs[index] > afterSeq)
            {
                handler.onPacket(data, index * slotSize, lengths[index], seqs[index], receiveTimes[index]);
                count++;
            }
        }
        return count;
    }
}
