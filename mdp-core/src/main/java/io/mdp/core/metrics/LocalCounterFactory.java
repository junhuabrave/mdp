package io.mdp.core.metrics;

import java.nio.ByteBuffer;

import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.CountersReader;

/**
 * Counters in process-local off-heap memory. Used when no Aeron media driver is present
 * (all-in-one ring-buffer mode, tests). The OTel exporter reads them in-process.
 */
public final class LocalCounterFactory implements CounterFactory
{
    private final CountersManager countersManager;

    public LocalCounterFactory(final int maxCounters)
    {
        final UnsafeBuffer metaData = new UnsafeBuffer(
            ByteBuffer.allocateDirect(maxCounters * CountersReader.METADATA_LENGTH));
        final UnsafeBuffer values = new UnsafeBuffer(
            ByteBuffer.allocateDirect(maxCounters * CountersReader.COUNTER_LENGTH));
        countersManager = new CountersManager(metaData, values);
    }

    public synchronized AtomicCounter newCounter(final int typeId, final String label)
    {
        return countersManager.newCounter(label, typeId);
    }

    public CountersReader countersReader()
    {
        return countersManager;
    }
}
