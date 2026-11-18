package io.mdp.core.transport;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Single-threaded in-memory source for tests, replay and benchmarks. Packets are offered with
 * {@link #offer(DirectBuffer, int, int)} and delivered on the next {@link #poll}.
 * The receive timestamp is taken at poll time, as for a real socket.
 */
public final class InMemoryPacketSource implements PacketSource
{
    private final String name;
    private final EpochNanoClock clock;
    private final UnsafeBuffer[] slots;
    private final int[] lengths;
    private final int mask;
    private long head;
    private long tail;
    private boolean active = true;

    public InMemoryPacketSource(final String name, final EpochNanoClock clock, final int capacity, final int maxPacket)
    {
        if (Integer.bitCount(capacity) != 1)
        {
            throw new IllegalArgumentException("capacity must be a power of two");
        }
        this.name = name;
        this.clock = clock;
        this.slots = new UnsafeBuffer[capacity];
        this.lengths = new int[capacity];
        this.mask = capacity - 1;
        for (int i = 0; i < capacity; i++)
        {
            slots[i] = new UnsafeBuffer(new byte[maxPacket]);
        }
    }

    /** @return false if full or inactive (packet dropped, like a socket). */
    public boolean offer(final DirectBuffer packet, final int offset, final int length)
    {
        if (!active || tail - head >= slots.length)
        {
            return false;
        }
        final int index = (int)(tail & mask);
        slots[index].putBytes(0, packet, offset, length);
        lengths[index] = length;
        tail++;
        return true;
    }

    public int poll(final PacketHandler handler, final int limit)
    {
        int count = 0;
        while (count < limit && head < tail)
        {
            final int index = (int)(head & mask);
            head++;
            handler.onPacket(slots[index], 0, lengths[index], clock.nanoTime());
            count++;
        }
        return count;
    }

    public void activate()
    {
        active = true;
    }

    public void deactivate()
    {
        active = false;
        head = tail;
    }

    public int pending()
    {
        return (int)(tail - head);
    }

    public String name()
    {
        return name;
    }

    public void close()
    {
    }
}
