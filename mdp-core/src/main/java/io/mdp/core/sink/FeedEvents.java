package io.mdp.core.sink;

import io.mdp.sbe.FeedEventType;

/**
 * Helper for writing {@code FeedEvent} messages.
 */
public final class FeedEvents
{
    private FeedEvents()
    {
    }

    public static void emit(
        final NormalizedSink sink,
        final int venueId,
        final int channelId,
        final FeedEventType type,
        final long instrumentId,
        final long fromSeq,
        final long toSeq,
        final long timeNs)
    {
        emit(sink, venueId, channelId, type, 0, instrumentId, fromSeq, toSeq, timeNs);
    }

    public static void emit(
        final NormalizedSink sink,
        final int venueId,
        final int channelId,
        final FeedEventType type,
        final int lineId,
        final long instrumentId,
        final long fromSeq,
        final long toSeq,
        final long timeNs)
    {
        sink.feedEvent()
            .venueId(venueId)
            .eventType(type)
            .lineId((short)lineId)
            .channelId(channelId)
            .instrumentId(instrumentId)
            .fromSeq(fromSeq)
            .toSeq(toSeq)
            .timeNs(timeNs);
        sink.commit();
    }
}
