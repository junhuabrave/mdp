package io.mdp.core.sink;

import io.mdp.sbe.BookUpdateEncoder;
import io.mdp.sbe.FeedEventEncoder;
import io.mdp.sbe.InstrumentDefinitionEncoder;
import io.mdp.sbe.InstrumentStatusEncoder;
import io.mdp.sbe.SnapshotBeginEncoder;
import io.mdp.sbe.SnapshotEndEncoder;
import io.mdp.sbe.TradeEncoder;

/**
 * Zero-copy output for normalized events.
 * <p>
 * Usage: call one claim method, fill in the returned encoder (already wrapped, with the SBE header
 * written, and {@code streamSeq} set), then call {@link #commit()}. Exactly one claim may be outstanding at a time.
 * <p>
 * If the claim fails because of back-pressure, the encoder is wrapped over a scratch buffer, so
 * the caller's code path is the same, {@link #commit()} does nothing, and the drop is reported
 * through {@link #pollDropped()}. {@link #feedEvent()} claims are retried for a bounded time,
 * because losing a stale or recovery notice would break the downstream contract.
 */
public interface NormalizedSink
{
    BookUpdateEncoder bookUpdate();

    TradeEncoder trade();

    InstrumentStatusEncoder instrumentStatus();

    InstrumentDefinitionEncoder instrumentDefinition();

    FeedEventEncoder feedEvent();

    /** Start of one book image in the normalized snapshot loop (snapshot publisher only). */
    SnapshotBeginEncoder snapshotBegin();

    SnapshotEndEncoder snapshotEnd();

    void commit();

    /** True if any claim failed since the last call; resets the flag. */
    boolean pollDropped();

    /** Flag bits for the {@code flags} field of normalized messages. */
    final class Flags
    {
        public static final byte NONE = 0;
        public static final byte LAST_IN_EVENT = 1;
        public static final byte SNAPSHOT = 1 << 1;
        public static final byte IMPLIED = 1 << 2;

        private Flags()
        {
        }
    }
}
