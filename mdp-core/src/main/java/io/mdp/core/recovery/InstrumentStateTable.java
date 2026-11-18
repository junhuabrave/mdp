package io.mdp.core.recovery;

import java.util.function.Consumer;

import org.agrona.collections.Long2ObjectHashMap;

/**
 * Per-instrument sequencing and staleness, for venues with instrument-level sequence numbers
 * (CME {@code RptSeq}). An entry is allocated once, the first time an instrument is seen.
 */
public final class InstrumentStateTable
{
    public static final class State
    {
        final long instrumentId;
        boolean stale = true;
        /** Next expected instrument sequence; 0 = accept the next one as a baseline. */
        long expectedSeq;
        /** Highest instrument sequence seen on the incremental feed (applied or not). */
        long lastSeenSeq;

        State(final long instrumentId)
        {
            this.instrumentId = instrumentId;
        }

        public long instrumentId()
        {
            return instrumentId;
        }

        public boolean isStale()
        {
            return stale;
        }

        public long expectedSeq()
        {
            return expectedSeq;
        }

        public long lastSeenSeq()
        {
            return lastSeenSeq;
        }
    }

    public enum SeqResult
    {
        APPLY,
        DUPLICATE,
        STALE,
        GAP
    }

    private final Long2ObjectHashMap<State> states;
    private int staleCount;

    public InstrumentStateTable(final int initialCapacity)
    {
        states = new Long2ObjectHashMap<>(initialCapacity, 0.65f);
    }

    public State get(final long instrumentId)
    {
        return states.get(instrumentId);
    }

    /**
     * @param createStale whether a newly seen instrument starts stale (needs a snapshot)
     */
    public State getOrCreate(final long instrumentId, final boolean createStale)
    {
        State state = states.get(instrumentId);
        if (null == state)
        {
            state = new State(instrumentId);
            state.stale = createStale;
            states.put(instrumentId, state);
            if (createStale)
            {
                staleCount++;
            }
        }
        return state;
    }

    /**
     * Check an incremental instrument sequence number and advance the state if it can be applied.
     * A {@link SeqResult#GAP} marks the instrument stale.
     */
    public SeqResult onIncremental(final State state, final long seq)
    {
        if (seq > state.lastSeenSeq)
        {
            state.lastSeenSeq = seq;
        }
        if (state.stale)
        {
            return SeqResult.STALE;
        }
        if (state.expectedSeq == 0 || seq == state.expectedSeq)
        {
            state.expectedSeq = seq + 1;
            return SeqResult.APPLY;
        }
        if (seq < state.expectedSeq)
        {
            return SeqResult.DUPLICATE;
        }
        markStale(state);
        return SeqResult.GAP;
    }

    /**
     * A snapshot carrying instrument sequence {@code seq} can recover a stale instrument only if it
     * is at least as new as every incremental already seen (anything older would leave a hole).
     *
     * @return true if the instrument was recovered and the snapshot should be applied
     */
    public boolean tryRecover(final State state, final long seq)
    {
        if (!state.stale || seq < state.lastSeenSeq)
        {
            return false;
        }
        state.stale = false;
        state.expectedSeq = seq + 1;
        state.lastSeenSeq = seq;
        staleCount--;
        return true;
    }

    public void markStale(final State state)
    {
        if (!state.stale)
        {
            state.stale = true;
            staleCount++;
        }
    }

    public void markAllStale()
    {
        for (final State state : states.values())
        {
            markStale(state);
        }
    }

    /** After a channel reset: every instrument is live again and accepts its next sequence as a baseline. */
    public void resetAll()
    {
        for (final State state : states.values())
        {
            state.stale = false;
            state.expectedSeq = 0;
            state.lastSeenSeq = 0;
        }
        staleCount = 0;
    }

    public int staleCount()
    {
        return staleCount;
    }

    public int size()
    {
        return states.size();
    }

    public void forEachStale(final Consumer<State> consumer)
    {
        for (final State state : states.values())
        {
            if (state.stale)
            {
                consumer.accept(state);
            }
        }
    }
}
