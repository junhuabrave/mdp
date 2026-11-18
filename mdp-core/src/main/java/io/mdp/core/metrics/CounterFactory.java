package io.mdp.core.metrics;

import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;

/**
 * Allocates counters in shared memory. Hot threads only write to counters; exporters read them
 * through {@link #countersReader()}, which can be in-process or in another process (Aeron CnC).
 */
public interface CounterFactory
{
    AtomicCounter newCounter(int typeId, String label);

    CountersReader countersReader();
}
