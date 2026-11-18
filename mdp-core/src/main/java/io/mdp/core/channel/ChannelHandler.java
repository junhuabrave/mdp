package io.mdp.core.channel;

import io.mdp.core.api.FeedDecoder;
import io.mdp.core.api.FeedType;
import io.mdp.core.api.PacketHeaderReader;
import io.mdp.core.arbitration.LineArbitrator;
import io.mdp.core.arbitration.SequencedPacketHandler;
import io.mdp.core.metrics.LatencyRecorder;
import io.mdp.core.metrics.MetricsRegistry;
import io.mdp.core.sink.FeedEvents;
import io.mdp.core.sink.NormalizedSink;
import io.mdp.core.transport.PacketHandler;
import io.mdp.core.transport.PacketSource;
import io.mdp.sbe.FeedEventType;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.EpochNanoClock;

/**
 * One exchange channel: polls the incremental lines (always), the snapshot lines (only while the
 * decoder wants them) and the definition lines, arbitrates each feed, and drives the decoder.
 * Owned by a single agent thread.
 */
public final class ChannelHandler implements AutoCloseable
{
    private final int channelId;
    private final int venueId;
    private final FeedDecoder decoder;
    private final NormalizedSink sink;
    private final EpochNanoClock epochClock;
    private final LatencyRecorder decodeLatency;
    private final int pollLimit;
    private final Feed incremental;
    private final Feed snapshot;
    private final Feed definition;
    private long nowNs;

    public ChannelHandler(
        final int channelId,
        final int venueId,
        final PacketHeaderReader headerReader,
        final FeedDecoder decoder,
        final NormalizedSink sink,
        final PacketSource[] incrementalSources,
        final PacketSource[] snapshotSources,
        final PacketSource[] definitionSources,
        final LineArbitrator.Config arbitration,
        final MetricsRegistry metrics,
        final EpochNanoClock epochClock,
        final int pollLimit)
    {
        this.channelId = channelId;
        this.venueId = venueId;
        this.decoder = decoder;
        this.sink = sink;
        this.epochClock = epochClock;
        this.pollLimit = pollLimit;
        this.decodeLatency = metrics.latency("mdp.latency.decode", "channel", String.valueOf(channelId));

        final LineArbitrator.Config quietLines = arbitration.forLoopingFeed();

        incremental = new Feed(FeedType.INCREMENTAL, incrementalSources, headerReader,
            new IncrementalOutput(), arbitration, metrics, true);
        snapshot = snapshotSources.length == 0 ? null : new Feed(FeedType.SNAPSHOT, snapshotSources, headerReader,
            new SnapshotOutput(), quietLines, metrics, false);
        definition = definitionSources.length == 0 ? null : new Feed(FeedType.DEFINITION, definitionSources,
            headerReader, new DefinitionOutput(), quietLines, metrics, false);
    }

    public int channelId()
    {
        return channelId;
    }

    public FeedDecoder decoder()
    {
        return decoder;
    }

    /**
     * @param nowNs monotonic time read once per duty cycle
     */
    public int doWork(final long nowNs)
    {
        this.nowNs = nowNs;
        int work = incremental.poll();
        work += incremental.arbitrator.doTimers(nowNs);

        if (null != snapshot)
        {
            work += snapshot.pollIfWanted(decoder.wantsSnapshot());
        }
        if (null != definition)
        {
            work += definition.pollIfWanted(decoder.wantsDefinitions());
        }
        return work;
    }

    public LineArbitrator incrementalArbitrator()
    {
        return incremental.arbitrator;
    }

    public void close()
    {
        incremental.close();
        if (null != snapshot)
        {
            snapshot.close();
        }
        if (null != definition)
        {
            definition.close();
        }
    }

    private final class Feed
    {
        final FeedType type;
        final PacketSource[] sources;
        final PacketHandler[] lineHandlers;
        final LineArbitrator arbitrator;
        boolean active;

        Feed(
            final FeedType type,
            final PacketSource[] sources,
            final PacketHeaderReader headerReader,
            final SequencedPacketHandler output,
            final LineArbitrator.Config config,
            final MetricsRegistry metrics,
            final boolean active)
        {
            this.type = type;
            this.sources = sources;
            final String[] names = new String[sources.length];
            for (int i = 0; i < sources.length; i++)
            {
                names[i] = sources[i].name();
            }
            this.arbitrator = new LineArbitrator(headerReader, output, config, sources.length, names, metrics,
                String.valueOf(channelId), type.key());
            this.lineHandlers = new PacketHandler[sources.length];
            for (int i = 0; i < sources.length; i++)
            {
                final int line = i;
                lineHandlers[i] = (buffer, offset, length, receiveTimeNs) ->
                    arbitrator.onPacket(line, buffer, offset, length, receiveTimeNs, ChannelHandler.this.nowNs);
            }
            this.active = active;
            if (!active)
            {
                for (final PacketSource source : sources)
                {
                    source.deactivate();
                }
            }
        }

        int poll()
        {
            int work = 0;
            for (int i = 0; i < sources.length; i++)
            {
                work += sources[i].poll(lineHandlers[i], pollLimit);
            }
            return work;
        }

        int pollIfWanted(final boolean wanted)
        {
            if (wanted != active)
            {
                active = wanted;
                for (final PacketSource source : sources)
                {
                    if (wanted)
                    {
                        source.activate();
                    }
                    else
                    {
                        source.deactivate();
                    }
                }
                arbitrator.reset();
                if (wanted && type == FeedType.SNAPSHOT)
                {
                    decoder.onSnapshotStart();
                }
            }
            if (!active)
            {
                return 0;
            }
            return poll() + arbitrator.doTimers(nowNs);
        }

        void close()
        {
            for (final PacketSource source : sources)
            {
                CloseHelper.quietClose(source);
            }
        }
    }

    private final class IncrementalOutput implements SequencedPacketHandler
    {
        public void onPacket(
            final DirectBuffer buffer, final int offset, final int length, final long seq, final long receiveTimeNs)
        {
            decoder.onPacket(buffer, offset, length, seq, receiveTimeNs);
            if (null != sink && sink.pollDropped())
            {
                decoder.onSinkDrop(receiveTimeNs);
            }
            decodeLatency.record(epochClock.nanoTime() - receiveTimeNs);
        }

        public void onGap(final long fromSeq, final long toSeq)
        {
            decoder.onPacketGap(fromSeq, toSeq, epochClock.nanoTime());
        }

        public void onSequenceReset(final long newSeq)
        {
            decoder.onSequenceReset(newSeq, epochClock.nanoTime());
        }

        public void onLineStatus(final int line, final boolean up)
        {
            final long timeNs = epochClock.nanoTime();
            decoder.onLineStatus(FeedType.INCREMENTAL, line, up, timeNs);
            if (null != sink)
            {
                FeedEvents.emit(sink, venueId, channelId, up ? FeedEventType.LINE_UP : FeedEventType.LINE_DOWN,
                    line, 0, 0, 0, timeNs);
            }
        }
    }

    private final class SnapshotOutput implements SequencedPacketHandler
    {
        public void onPacket(
            final DirectBuffer buffer, final int offset, final int length, final long seq, final long receiveTimeNs)
        {
            decoder.onSnapshotPacket(buffer, offset, length, seq, receiveTimeNs);
            if (null != sink && sink.pollDropped())
            {
                decoder.onSinkDrop(receiveTimeNs);
            }
        }

        public void onGap(final long fromSeq, final long toSeq)
        {
            decoder.onSnapshotGap(fromSeq, toSeq);
        }

        public void onSequenceReset(final long newSeq)
        {
        }

        public void onLineStatus(final int line, final boolean up)
        {
        }
    }

    private final class DefinitionOutput implements SequencedPacketHandler
    {
        public void onPacket(
            final DirectBuffer buffer, final int offset, final int length, final long seq, final long receiveTimeNs)
        {
            decoder.onDefinitionPacket(buffer, offset, length, seq, receiveTimeNs);
        }

        public void onGap(final long fromSeq, final long toSeq)
        {
        }

        public void onSequenceReset(final long newSeq)
        {
        }

        public void onLineStatus(final int line, final boolean up)
        {
        }
    }
}
