package io.mdp.core.metrics;

import java.util.concurrent.TimeUnit;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.SingleWriterRecorder;
import org.agrona.concurrent.status.AtomicCounter;

/**
 * Wait-free latency recording on the hot thread, with percentiles published to gauge counters
 * by a low-priority thread calling {@link #snapshot()}.
 * <p>
 * Exactly one thread may call {@link #record(long)}; exactly one (other) thread may call
 * {@link #snapshot()}.
 */
public final class LatencyRecorder
{
    private static final long HIGHEST_TRACKABLE_NS = TimeUnit.SECONDS.toNanos(10);

    private final String name;
    private final SingleWriterRecorder recorder = new SingleWriterRecorder(HIGHEST_TRACKABLE_NS, 3);
    private final Histogram cumulative = new Histogram(HIGHEST_TRACKABLE_NS, 3);
    private Histogram interval;

    private final AtomicCounter p50;
    private final AtomicCounter p90;
    private final AtomicCounter p99;
    private final AtomicCounter p999;
    private final AtomicCounter max;
    private final AtomicCounter count;

    LatencyRecorder(final MetricsRegistry registry, final String name, final String... attributes)
    {
        this.name = name;
        p50 = registry.gauge(name + ".p50", attributes);
        p90 = registry.gauge(name + ".p90", attributes);
        p99 = registry.gauge(name + ".p99", attributes);
        p999 = registry.gauge(name + ".p999", attributes);
        max = registry.gauge(name + ".max", attributes);
        count = registry.counter(name + ".count", attributes);
    }

    public String name()
    {
        return name;
    }

    /** Hot path: record one latency sample in nanoseconds. */
    public void record(final long latencyNs)
    {
        recorder.recordValue(latencyNs < 0 ? 0 : Math.min(latencyNs, HIGHEST_TRACKABLE_NS));
    }

    /** Metrics thread: swap the interval histogram and publish percentiles of the last interval. */
    public synchronized void snapshot()
    {
        interval = recorder.getIntervalHistogram(interval);
        cumulative.add(interval);
        if (interval.getTotalCount() > 0)
        {
            p50.setRelease(interval.getValueAtPercentile(50));
            p90.setRelease(interval.getValueAtPercentile(90));
            p99.setRelease(interval.getValueAtPercentile(99));
            p999.setRelease(interval.getValueAtPercentile(99.9));
            max.setRelease(interval.getMaxValue());
        }
        count.setRelease(cumulative.getTotalCount());
    }

    /** Copy of everything recorded so far (as of the last snapshot). */
    public synchronized Histogram cumulativeCopy()
    {
        return cumulative.copy();
    }
}
