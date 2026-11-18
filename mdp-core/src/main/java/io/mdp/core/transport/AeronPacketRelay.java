package io.mdp.core.transport;

import java.nio.ByteOrder;

import io.aeron.ExclusivePublication;
import io.aeron.logbuffer.BufferClaim;
import io.mdp.core.api.FeedDecoder;
import io.mdp.core.metrics.MetricsRegistry;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;

/**
 * Gateway role: instead of decoding, forward arbitrated packets to Aeron as
 * {@code [receiveTimeNs][packet]} frames. The normalizer reads them with {@link AeronPacketSource}.
 * Snapshot packets go on a separate stream and are always forwarded.
 */
public final class AeronPacketRelay implements FeedDecoder
{
    private final ExclusivePublication incremental;
    private final ExclusivePublication snapshot;
    private final BufferClaim claim = new BufferClaim();
    private final UnsafeBuffer frame = new UnsafeBuffer(new byte[65536 + AeronPacketSource.FRAME_HEADER_LENGTH]);
    private final AtomicCounter dropped;
    private final AtomicCounter forwarded;

    public AeronPacketRelay(
        final ExclusivePublication incremental,
        final ExclusivePublication snapshot,
        final MetricsRegistry metrics,
        final int channelId)
    {
        this.incremental = incremental;
        this.snapshot = snapshot;
        final String channel = String.valueOf(channelId);
        this.dropped = metrics.counter("mdp.relay.dropped", "channel", channel);
        this.forwarded = metrics.counter("mdp.relay.forwarded", "channel", channel);
    }

    public void onPacket(
        final DirectBuffer buffer, final int offset, final int length, final long packetSeq, final long receiveTimeNs)
    {
        forward(incremental, buffer, offset, length, receiveTimeNs);
    }

    public void onPacketGap(final long fromSeq, final long toSeq, final long receiveTimeNs)
    {
        // The normalizer sees the gap from the sequence numbers and recovers itself.
    }

    public void onSnapshotPacket(
        final DirectBuffer buffer, final int offset, final int length, final long packetSeq, final long receiveTimeNs)
    {
        if (null != snapshot)
        {
            forward(snapshot, buffer, offset, length, receiveTimeNs);
        }
    }

    public boolean wantsSnapshot()
    {
        return null != snapshot;
    }

    private void forward(
        final ExclusivePublication publication,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final long receiveTimeNs)
    {
        final int frameLength = length + AeronPacketSource.FRAME_HEADER_LENGTH;
        if (frameLength <= publication.maxPayloadLength())
        {
            if (publication.tryClaim(frameLength, claim) > 0)
            {
                final MutableDirectBuffer out = claim.buffer();
                final int o = claim.offset();
                out.putLong(o, receiveTimeNs, ByteOrder.LITTLE_ENDIAN);
                out.putBytes(o + AeronPacketSource.FRAME_HEADER_LENGTH, buffer, offset, length);
                claim.commit();
                MetricsRegistry.increment(forwarded);
                return;
            }
        }
        else
        {
            frame.putLong(0, receiveTimeNs, ByteOrder.LITTLE_ENDIAN);
            frame.putBytes(AeronPacketSource.FRAME_HEADER_LENGTH, buffer, offset, length);
            if (publication.offer(frame, 0, frameLength) > 0)
            {
                MetricsRegistry.increment(forwarded);
                return;
            }
        }
        MetricsRegistry.increment(dropped);
    }
}
