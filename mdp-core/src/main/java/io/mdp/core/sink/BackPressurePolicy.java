package io.mdp.core.sink;

public enum BackPressurePolicy
{
    /** Never block the hot path: drop the event, count it, and let the decoder recover (default). */
    DROP_AND_MARK_STALE,
    /** Lossless: spin until space is available. For recorders and back-tests. */
    SPIN
}
