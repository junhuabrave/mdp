package io.mdp.core.arbitration;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import io.mdp.core.api.PacketHeaderReader;
import io.mdp.core.metrics.MetricsRegistry;
import org.agrona.BufferUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;

/**
 * Venue-agnostic arbitration of N redundant lines (typically A/B) carrying the same sequenced packets.
 * <ul>
 *   <li>First arrival wins and is delivered immediately (no copy on the in-order path).</li>
 *   <li>Duplicates (the losing line) and late packets (after a gap was declared) are dropped and counted.</li>
 *   <li>Out-of-order packets are copied into a pre-allocated reorder buffer while the other line has
 *       {@code gapTimeout} to fill the hole. When the timer expires or the buffer window overflows,
 *       a gap is declared and delivery continues from the lowest buffered packet.</li>
 *   <li>A line restarting at a low sequence ({@code <= resetSeqMax}) is a sequence reset. Epochs make
 *       sure the reset happens once even though every line restarts, and that a lagging line's
 *       old-epoch packets are ignored.</li>
 *   <li>Each line is marked down after {@code lineTimeout} with no packets (heartbeats included).</li>
 * </ul>
 * Single threaded and allocation free after construction.
 */
public final class LineArbitrator
{
    private static final long UNSET = -1;

    /**
     * @param gapTimeoutNs  how long to wait for another line to fill a hole
     * @param reorderSlots  reorder buffer slots (power of two); also the maximum hole width before forcing a gap
     * @param slotSize      maximum packet size the reorder buffer can hold
     * @param lineTimeoutNs silence after which a line is reported down
     * @param resetSeqMax   a backwards jump to a sequence at or below this value is a reset
     * @param loopingFeed   snapshot/definition loops: a line repeating its last sequence also starts a
     *                      new loop (a one-packet loop resends sequence 1 every cycle)
     */
    public record Config(
        long gapTimeoutNs, int reorderSlots, int slotSize, long lineTimeoutNs, long resetSeqMax, boolean loopingFeed)
    {
        public Config(
            final long gapTimeoutNs, final int reorderSlots, final int slotSize, final long lineTimeoutNs,
            final long resetSeqMax)
        {
            this(gapTimeoutNs, reorderSlots, slotSize, lineTimeoutNs, resetSeqMax, false);
        }

        public static Config defaults()
        {
            return new Config(TimeUnit.MICROSECONDS.toNanos(500), 1024, 1500, TimeUnit.SECONDS.toNanos(2), 1);
        }

        /** The same settings for a looping snapshot/definition feed: any restart is a new loop, no line timeout. */
        public Config forLoopingFeed()
        {
            return new Config(gapTimeoutNs, reorderSlots, slotSize, Long.MAX_VALUE / 2, Long.MAX_VALUE, true);
        }
    }

    private final PacketHeaderReader headerReader;
    private final SequencedPacketHandler handler;
    private final Config config;
    private final int lineCount;

    private final long[] lineLastSeq;
    private final long[] lineEpoch;
    private final long[] lineLastReceiveNs;
    private final boolean[] lineUp;
    private final AtomicCounter[] linePackets;
    private final AtomicCounter[] lineWins;
    private final AtomicCounter[] lineDuplicates;
    private final AtomicCounter[] lineLate;
    private final AtomicCounter[] lineGaps;
    private final AtomicCounter[] lineOversize;
    private final AtomicCounter[] lineHeartbeats;
    private final AtomicCounter[] lineUpGauge;
    private final AtomicCounter channelGaps;
    private final AtomicCounter channelGapPackets;
    private final AtomicCounter channelResets;
    private final AtomicCounter reorderBuffered;

    private final UnsafeBuffer slotBuffer;
    private final long[] slotSeq;
    private final int[] slotLength;
    private final long[] slotReceiveTimeNs;
    private final int slotMask;
    private int pending;
    private long gapTimerStartNs;

    private long arbitratorEpoch;
    private long expected = UNSET;
    private long lastGapFrom = UNSET;
    private long lastGapTo = UNSET;

    public LineArbitrator(
        final PacketHeaderReader headerReader,
        final SequencedPacketHandler handler,
        final Config config,
        final int lineCount,
        final String[] lineNames,
        final MetricsRegistry metrics,
        final String channel,
        final String feed)
    {
        if (Integer.bitCount(config.reorderSlots()) != 1)
        {
            throw new IllegalArgumentException("reorderSlots must be a power of two");
        }
        this.headerReader = headerReader;
        this.handler = handler;
        this.config = config;
        this.lineCount = lineCount;

        lineLastSeq = new long[lineCount];
        lineEpoch = new long[lineCount];
        lineLastReceiveNs = new long[lineCount];
        lineUp = new boolean[lineCount];
        linePackets = new AtomicCounter[lineCount];
        lineWins = new AtomicCounter[lineCount];
        lineDuplicates = new AtomicCounter[lineCount];
        lineLate = new AtomicCounter[lineCount];
        lineGaps = new AtomicCounter[lineCount];
        lineOversize = new AtomicCounter[lineCount];
        lineHeartbeats = new AtomicCounter[lineCount];
        lineUpGauge = new AtomicCounter[lineCount];
        for (int i = 0; i < lineCount; i++)
        {
            final String[] attrs = { "channel", channel, "feed", feed, "line", lineNames[i] };
            linePackets[i] = metrics.counter("mdp.line.packets", attrs);
            lineWins[i] = metrics.counter("mdp.line.wins", attrs);
            lineDuplicates[i] = metrics.counter("mdp.line.duplicates", attrs);
            lineLate[i] = metrics.counter("mdp.line.late", attrs);
            lineGaps[i] = metrics.counter("mdp.line.gaps", attrs);
            lineOversize[i] = metrics.counter("mdp.line.oversize", attrs);
            lineHeartbeats[i] = metrics.counter("mdp.line.heartbeats", attrs);
            lineUpGauge[i] = metrics.gauge("mdp.line.up", attrs);
        }
        channelGaps = metrics.counter("mdp.channel.gaps", "channel", channel, "feed", feed);
        channelGapPackets = metrics.counter("mdp.channel.gap.packets", "channel", channel, "feed", feed);
        channelResets = metrics.counter("mdp.channel.sequence.resets", "channel", channel, "feed", feed);
        reorderBuffered = metrics.gauge("mdp.reorder.buffered", "channel", channel, "feed", feed);

        slotBuffer = new UnsafeBuffer(
            BufferUtil.allocateDirectAligned(config.reorderSlots() * config.slotSize(), 64));
        slotSeq = new long[config.reorderSlots()];
        slotLength = new int[config.reorderSlots()];
        slotReceiveTimeNs = new long[config.reorderSlots()];
        slotMask = config.reorderSlots() - 1;
        reset();
    }

    /**
     * Process a packet from {@code line}.
     *
     * @param nowNs monotonic time (cached per duty cycle) for timers and line health
     */
    public void onPacket(
        final int line,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final long receiveTimeNs,
        final long nowNs)
    {
        lineLastReceiveNs[line] = nowNs;
        if (!lineUp[line])
        {
            setLineUp(line, true);
        }
        MetricsRegistry.increment(linePackets[line]);

        if (headerReader.isHeartbeat(buffer, offset, length))
        {
            MetricsRegistry.increment(lineHeartbeats[line]);
            return;
        }

        final long seq = headerReader.sequence(buffer, offset);
        if (!trackLineSequence(line, seq))
        {
            MetricsRegistry.increment(lineDuplicates[line]);
            return;
        }

        if (expected == UNSET)
        {
            expected = seq;
        }

        if (seq == expected)
        {
            deliverInOrder(line, buffer, offset, length, seq, receiveTimeNs, nowNs);
            return;
        }

        if (seq < expected)
        {
            if (seq >= lastGapFrom && seq <= lastGapTo)
            {
                MetricsRegistry.increment(lineLate[line]);
            }
            else
            {
                MetricsRegistry.increment(lineDuplicates[line]);
            }
            return;
        }

        // seq > expected: hole. Make room if the hole is wider than the reorder window.
        while (seq - expected >= slotSeq.length)
        {
            skipForward(seq, nowNs);
        }
        if (seq == expected)
        {
            deliverInOrder(line, buffer, offset, length, seq, receiveTimeNs, nowNs);
            return;
        }

        final int index = (int)(seq & slotMask);
        if (slotSeq[index] == seq)
        {
            MetricsRegistry.increment(lineDuplicates[line]);
            return;
        }
        if (length > config.slotSize())
        {
            MetricsRegistry.increment(lineOversize[line]);
            return;
        }

        slotBuffer.putBytes(index * config.slotSize(), buffer, offset, length);
        slotSeq[index] = seq;
        slotLength[index] = length;
        slotReceiveTimeNs[index] = receiveTimeNs;
        if (pending++ == 0)
        {
            gapTimerStartNs = nowNs;
        }
        reorderBuffered.setRelease(pending);
        MetricsRegistry.increment(lineWins[line]);
    }

    /**
     * Check gap and line timers. Call once per duty cycle.
     *
     * @return amount of work done
     */
    public int doTimers(final long nowNs)
    {
        int work = 0;
        if (pending > 0 && nowNs - gapTimerStartNs >= config.gapTimeoutNs())
        {
            final long lowest = lowestBuffered();
            declareGap(expected, lowest - 1);
            expected = lowest;
            drain(nowNs);
            work++;
        }

        for (int i = 0; i < lineCount; i++)
        {
            if (lineUp[i] && nowNs - lineLastReceiveNs[i] > config.lineTimeoutNs())
            {
                setLineUp(i, false);
                work++;
            }
        }
        return work;
    }

    /** Forget all sequence state, for example when snapshot sources are re-activated. */
    public void reset()
    {
        Arrays.fill(slotSeq, UNSET);
        Arrays.fill(lineLastSeq, UNSET);
        Arrays.fill(lineEpoch, 0);
        arbitratorEpoch = 0;
        pending = 0;
        expected = UNSET;
        lastGapFrom = UNSET;
        lastGapTo = UNSET;
        reorderBuffered.setRelease(0);
    }

    public long expectedSequence()
    {
        return expected;
    }

    public int pending()
    {
        return pending;
    }

    public boolean isLineUp(final int line)
    {
        return lineUp[line];
    }

    /**
     * Per-line sequence bookkeeping and reset detection.
     *
     * @return false if the packet belongs to an older epoch than the arbitrator and must be dropped
     */
    private boolean trackLineSequence(final int line, final long seq)
    {
        final long last = lineLastSeq[line];
        final boolean restarted = seq < last || (config.loopingFeed() && seq == last);
        if (last != UNSET && restarted && seq <= config.resetSeqMax())
        {
            lineEpoch[line]++;
            lineLastSeq[line] = seq;
            if (lineEpoch[line] > arbitratorEpoch)
            {
                arbitratorEpoch = lineEpoch[line];
                sequenceReset(seq);
            }
            return true;
        }

        if (last != UNSET && seq > last + 1)
        {
            MetricsRegistry.increment(lineGaps[line]);
        }
        if (last == UNSET || seq > last)
        {
            lineLastSeq[line] = seq;
        }
        return lineEpoch[line] >= arbitratorEpoch;
    }

    private void deliverInOrder(
        final int line,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final long seq,
        final long receiveTimeNs,
        final long nowNs)
    {
        MetricsRegistry.increment(lineWins[line]);
        expected = seq + 1;
        handler.onPacket(buffer, offset, length, seq, receiveTimeNs);
        if (pending > 0)
        {
            drain(nowNs);
        }
    }

    private void drain(final long nowNs)
    {
        boolean progressed = false;
        while (pending > 0)
        {
            final int index = (int)(expected & slotMask);
            if (slotSeq[index] != expected)
            {
                break;
            }
            final long seq = expected;
            slotSeq[index] = UNSET;
            pending--;
            expected = seq + 1;
            handler.onPacket(slotBuffer, index * config.slotSize(), slotLength[index], seq, slotReceiveTimeNs[index]);
            progressed = true;
        }
        if (progressed && pending > 0)
        {
            gapTimerStartNs = nowNs;
        }
        reorderBuffered.setRelease(pending);
    }

    /** The hole is wider than the reorder window: give up on the oldest missing packets now. */
    private void skipForward(final long targetSeq, final long nowNs)
    {
        final long lowest = pending > 0 ? lowestBuffered() : UNSET;
        if (lowest == UNSET || lowest >= targetSeq)
        {
            declareGap(expected, targetSeq - 1);
            expected = targetSeq;
        }
        else
        {
            declareGap(expected, lowest - 1);
            expected = lowest;
            drain(nowNs);
        }
    }

    private long lowestBuffered()
    {
        long lowest = Long.MAX_VALUE;
        for (final long seq : slotSeq)
        {
            if (seq != UNSET && seq < lowest)
            {
                lowest = seq;
            }
        }
        return lowest == Long.MAX_VALUE ? UNSET : lowest;
    }

    private void declareGap(final long from, final long to)
    {
        if (to < from)
        {
            return;
        }
        MetricsRegistry.increment(channelGaps);
        MetricsRegistry.add(channelGapPackets, to - from + 1);
        lastGapFrom = from;
        lastGapTo = to;
        handler.onGap(from, to);
    }

    private void sequenceReset(final long seq)
    {
        Arrays.fill(slotSeq, UNSET);
        pending = 0;
        reorderBuffered.setRelease(0);
        expected = seq;
        lastGapFrom = UNSET;
        lastGapTo = UNSET;
        MetricsRegistry.increment(channelResets);
        handler.onSequenceReset(seq);
    }

    private void setLineUp(final int line, final boolean up)
    {
        lineUp[line] = up;
        lineUpGauge[line].setRelease(up ? 1 : 0);
        handler.onLineStatus(line, up);
    }
}
