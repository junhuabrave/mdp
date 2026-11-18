package io.mdp.core.metrics;

import io.aeron.Aeron;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;

/**
 * Counters allocated in the Aeron media driver's CnC file. Any process attached to the same
 * Aeron directory (for example the {@code observer} sidecar) can read them without touching
 * the hot process.
 */
public final class AeronCounterFactory implements CounterFactory
{
    private final Aeron aeron;

    public AeronCounterFactory(final Aeron aeron)
    {
        this.aeron = aeron;
    }

    public AtomicCounter newCounter(final int typeId, final String label)
    {
        return aeron.addCounter(typeId, label);
    }

    public CountersReader countersReader()
    {
        return aeron.countersReader();
    }
}
