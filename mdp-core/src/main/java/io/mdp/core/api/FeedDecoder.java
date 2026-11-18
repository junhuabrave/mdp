package io.mdp.core.api;

import org.agrona.DirectBuffer;

/**
 * Stateful, per-channel processor of arbitrated (in-order, de-duplicated) packets.
 * <p>
 * Venue adapters decode SBE messages and write normalized events to the
 * {@link io.mdp.core.sink.NormalizedSink} from their {@link DecoderContext}. They own the venue's
 * recovery logic (sequence checks, stale state, snapshot application).
 * <p>
 * The gateway role plugs in a relay implementation that forwards raw packets instead of decoding.
 * <p>
 * All methods are called on one thread, the channel's agent thread.
 */
public interface FeedDecoder
{
    /** In-order incremental packet. {@code receiveTimeNs} is epoch nanos from the host clock. */
    void onPacket(DirectBuffer buffer, int offset, int length, long packetSeq, long receiveTimeNs);

    /** Packets {@code [fromSeq, toSeq]} were lost on every line. */
    void onPacketGap(long fromSeq, long toSeq, long receiveTimeNs);

    /** The incremental feed restarted its sequence numbering. */
    default void onSequenceReset(final long newSeq, final long receiveTimeNs)
    {
    }

    /** Snapshot feed packet, delivered only while {@link #wantsSnapshot()} is true. */
    default void onSnapshotPacket(
        final DirectBuffer buffer, final int offset, final int length, final long packetSeq, final long receiveTimeNs)
    {
    }

    default void onSnapshotGap(final long fromSeq, final long toSeq)
    {
    }

    /** Snapshot sources were just (re)activated; any partial snapshot cycle state is invalid. */
    default void onSnapshotStart()
    {
    }

    /** True while the decoder needs the snapshot feed (startup or recovery). */
    default boolean wantsSnapshot()
    {
        return false;
    }

    /** Instrument definition feed packet, delivered only while {@link #wantsDefinitions()} is true. */
    default void onDefinitionPacket(
        final DirectBuffer buffer, final int offset, final int length, final long packetSeq, final long receiveTimeNs)
    {
    }

    default boolean wantsDefinitions()
    {
        return false;
    }

    /** The normalized sink dropped at least one event during the last packet (back-pressure). */
    default void onSinkDrop(final long receiveTimeNs)
    {
    }

    default void onLineStatus(final FeedType feed, final int line, final boolean up, final long timeNs)
    {
    }
}
