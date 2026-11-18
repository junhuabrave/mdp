package io.mdp.otel;

import java.time.Duration;
import java.util.Map;

import io.mdp.core.metrics.LatencyRecorder;
import io.mdp.core.metrics.MetricsRegistry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricExporter;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OtelCountersExporterTest
{
    @Test
    void exportsCountersGaugesAndLatencyPercentiles()
    {
        final MetricsRegistry metrics = MetricsRegistry.local();
        final AtomicCounter packets = metrics.counter("mdp.line.packets", "channel", "310", "line", "A");
        final AtomicCounter up = metrics.gauge("mdp.line.up", "channel", "310", "line", "A");
        final LatencyRecorder latency = metrics.latency("mdp.latency.decode", "channel", "310");
        MetricsRegistry.add(packets, 42);
        up.setRelease(1);
        for (int i = 1; i <= 1000; i++)
        {
            latency.record(i * 100L);
        }
        metrics.snapshotHistograms();

        final InMemoryMetricExporter exporter = InMemoryMetricExporter.create();
        try (OtelCountersExporter otel = new OtelCountersExporter(
            metrics.countersReader(), exporter, Duration.ofHours(1), Map.of("service.name", "test"), false, false))
        {
            otel.forceFlush();
            final var exported = exporter.getFinishedMetricItems();

            assertEquals(42, value(exported, "mdp.line.packets"));
            assertEquals(1, value(exported, "mdp.line.up"));
            final long p99 = value(exported, "mdp.latency.decode.p99");
            assertTrue(p99 >= 98_000 && p99 <= 100_500, "p99=" + p99);
            final LongPointData point = (LongPointData)exported.stream()
                .filter((m) -> m.getName().equals("mdp.line.packets")).findFirst().orElseThrow()
                .getData().getPoints().iterator().next();
            assertEquals("A", point.getAttributes().get(AttributeKey.stringKey("line")));
        }
    }

    private static long value(final java.util.Collection<MetricData> metrics, final String name)
    {
        final MetricData data = metrics.stream().filter((m) -> m.getName().equals(name)).findFirst().orElseThrow(
            () -> new AssertionError("missing metric " + name));
        return ((LongPointData)data.getData().getPoints().iterator().next()).getValue();
    }
}
