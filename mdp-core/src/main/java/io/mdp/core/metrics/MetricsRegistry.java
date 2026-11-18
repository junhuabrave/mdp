package io.mdp.core.metrics;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;

/**
 * Creates named metrics backed by Agrona counters.
 * <p>
 * Counter labels have the form {@code metric.name{key=value,key=value}}. Exporters parse the label
 * into a metric name and attributes, so every exporter (in-process or sidecar) needs only a
 * {@link CountersReader}.
 * <p>
 * Hot-path updates use {@link #increment(AtomicCounter)}, a single-writer plain read plus release
 * store with no atomic read-modify-write.
 */
public final class MetricsRegistry implements AutoCloseable
{
    /** Monotonic counter. */
    public static final int COUNTER_TYPE_ID = 9001;
    /** Point-in-time gauge. */
    public static final int GAUGE_TYPE_ID = 9002;

    private final CounterFactory factory;
    private final List<LatencyRecorder> recorders = new CopyOnWriteArrayList<>();
    private ScheduledExecutorService snapshotExecutor;

    public MetricsRegistry(final CounterFactory factory)
    {
        this.factory = factory;
    }

    public static MetricsRegistry local()
    {
        return new MetricsRegistry(new LocalCounterFactory(4096));
    }

    public AtomicCounter counter(final String name, final String... attributes)
    {
        return factory.newCounter(COUNTER_TYPE_ID, label(name, attributes));
    }

    public AtomicCounter gauge(final String name, final String... attributes)
    {
        return factory.newCounter(GAUGE_TYPE_ID, label(name, attributes));
    }

    public LatencyRecorder latency(final String name, final String... attributes)
    {
        final LatencyRecorder recorder = new LatencyRecorder(this, name, attributes);
        recorders.add(recorder);
        return recorder;
    }

    public CountersReader countersReader()
    {
        return factory.countersReader();
    }

    public List<LatencyRecorder> latencyRecorders()
    {
        return recorders;
    }

    /** Publish histogram percentiles for all recorders. Call from a non-hot thread. */
    public void snapshotHistograms()
    {
        for (final LatencyRecorder recorder : recorders)
        {
            recorder.snapshot();
        }
    }

    /** Start a daemon thread that snapshots histograms periodically. */
    public synchronized void startHistogramSnapshots(final long intervalMs)
    {
        if (null == snapshotExecutor)
        {
            snapshotExecutor = Executors.newSingleThreadScheduledExecutor(
                (r) ->
                {
                    final Thread t = new Thread(r, "mdp-histogram-snapshot");
                    t.setDaemon(true);
                    return t;
                });
            snapshotExecutor.scheduleAtFixedRate(this::snapshotHistograms, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        }
    }

    public synchronized void close()
    {
        if (null != snapshotExecutor)
        {
            snapshotExecutor.shutdownNow();
            snapshotExecutor = null;
        }
    }

    /** Single-writer increment: plain read, release store. No atomic RMW on the hot path. */
    public static void increment(final AtomicCounter counter)
    {
        counter.setRelease(counter.getPlain() + 1);
    }

    /** Single-writer add. */
    public static void add(final AtomicCounter counter, final long delta)
    {
        counter.setRelease(counter.getPlain() + delta);
    }

    public static String label(final String name, final String... attributes)
    {
        if (attributes.length == 0)
        {
            return name;
        }
        if ((attributes.length & 1) != 0)
        {
            throw new IllegalArgumentException("attributes must be key/value pairs: " + name);
        }

        final StringBuilder sb = new StringBuilder(name).append('{');
        for (int i = 0; i < attributes.length; i += 2)
        {
            if (i > 0)
            {
                sb.append(',');
            }
            sb.append(attributes[i]).append('=').append(attributes[i + 1]);
        }
        return sb.append('}').toString();
    }
}
