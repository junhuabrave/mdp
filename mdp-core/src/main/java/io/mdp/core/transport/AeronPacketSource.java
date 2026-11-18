package io.mdp.core.transport;

import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.DirectBuffer;

/**
 * Raw exchange packets relayed over Aeron by the gateway role (split mode), or replayed from a
 * capture. Frame layout: {@code [receiveTimeNs:int64 LE][original packet bytes]}, so the original
 * receive time survives the hop.
 * <p>
 * While deactivated, the subscription is still drained (and the data discarded) so the gateway's
 * publication is never back-pressured by an idle snapshot stream.
 */
public final class AeronPacketSource implements PacketSource
{
    public static final int FRAME_HEADER_LENGTH = Long.BYTES;

    private final String name;
    private final Subscription subscription;
    private final FragmentHandler fragmentHandler;
    private final FragmentHandler discardHandler = (buffer, offset, length, header) -> {};
    private PacketHandler handler;
    private boolean active = true;

    public AeronPacketSource(final String name, final Subscription subscription)
    {
        this.name = name;
        this.subscription = subscription;
        this.fragmentHandler = new FragmentAssembler(this::onFragment);
    }

    public int poll(final PacketHandler handler, final int limit)
    {
        if (!active)
        {
            subscription.poll(discardHandler, limit);
            return 0;
        }
        this.handler = handler;
        return subscription.poll(fragmentHandler, limit);
    }

    public void activate()
    {
        if (!active)
        {
            while (subscription.poll(discardHandler, 256) > 0)
            {
                Thread.onSpinWait();
            }
            active = true;
        }
    }

    public void deactivate()
    {
        active = false;
    }

    public String name()
    {
        return name;
    }

    public void close()
    {
        subscription.close();
    }

    private void onFragment(final DirectBuffer buffer, final int offset, final int length, final io.aeron.logbuffer.Header header)
    {
        final long receiveTimeNs = buffer.getLong(offset, java.nio.ByteOrder.LITTLE_ENDIAN);
        handler.onPacket(buffer, offset + FRAME_HEADER_LENGTH, length - FRAME_HEADER_LENGTH, receiveTimeNs);
    }
}
