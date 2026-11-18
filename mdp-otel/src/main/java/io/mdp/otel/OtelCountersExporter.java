package io.mdp.otel;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.mdp.core.metrics.MetricsRegistry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.instrumentation.runtimetelemetry.RuntimeTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import org.agrona.concurrent.status.CountersReader;

/**
 * Exports Agrona/Aeron counters as OpenTelemetry metrics (OTLP gRPC to an OTel Collector).
 * <p>
 * Works the same in-process (local counters) and as an out-of-process sidecar (Aeron CnC
 * counters), because it only needs a {@link CountersReader}. Hot threads never call OpenTelemetry:
 * values are read on the SDK's export thread. New counters are found by a periodic scan.
 * <ul>
 *   <li>type {@link MetricsRegistry#COUNTER_TYPE_ID}: monotonic counter</li>
 *   <li>type {@link MetricsRegistry#GAUGE_TYPE_ID}: gauge</li>
 *   <li>Aeron system counters (type 0): gauge {@code aeron.system} with a {@code counter} attribute</li>
 * </ul>
 */
public final class OtelCountersExporter implements AutoCloseable
{
    private static final int AERON_SYSTEM_COUNTER_TYPE_ID = 0;

    private record Series(int counterId, Attributes attributes)
    {
    }

    private final CountersReader countersReader;
    private final Meter meter;
    private final SdkMeterProvider meterProvider;
    private final OpenTelemetrySdk openTelemetry;
    private final RuntimeTelemetry runtimeTelemetry;
    private final ScheduledExecutorService scanner;
    private final Map<String, List<Series>> seriesByMetric = new ConcurrentHashMap<>();
    private final Map<Integer, Boolean> known = new ConcurrentHashMap<>();
    private final boolean exportAeronSystemCounters;

    public OtelCountersExporter(
        final CountersReader countersReader,
        final MetricExporter metricExporter,
        final Duration interval,
        final Map<String, String> resourceAttributes,
        final boolean exportAeronSystemCounters,
        final boolean jvmMetrics)
    {
        this.countersReader = countersReader;
        this.exportAeronSystemCounters = exportAeronSystemCounters;

        final AttributesBuilder resource = Attributes.builder();
        resourceAttributes.forEach(resource::put);
        meterProvider = SdkMeterProvider.builder()
            .setResource(Resource.getDefault().merge(Resource.create(resource.build())))
            .registerMetricReader(PeriodicMetricReader.builder(metricExporter).setInterval(interval).build())
            .build();
        openTelemetry = OpenTelemetrySdk.builder().setMeterProvider(meterProvider).build();
        meter = meterProvider.get("io.mdp");
        runtimeTelemetry = jvmMetrics ? RuntimeTelemetry.create(openTelemetry) : null;

        scanner = Executors.newSingleThreadScheduledExecutor((r) ->
        {
            final Thread t = new Thread(r, "mdp-otel-scan");
            t.setDaemon(true);
            return t;
        });
        scan();
        final long scanMs = Math.max(500, interval.toMillis() / 2);
        scanner.scheduleAtFixedRate(this::scanQuietly, scanMs, scanMs, TimeUnit.MILLISECONDS);
    }

    public static OtelCountersExporter otlp(
        final CountersReader countersReader,
        final String endpoint,
        final Duration interval,
        final Map<String, String> resourceAttributes,
        final boolean exportAeronSystemCounters,
        final boolean jvmMetrics)
    {
        return new OtelCountersExporter(
            countersReader,
            OtlpGrpcMetricExporter.builder().setEndpoint(endpoint).build(),
            interval,
            resourceAttributes,
            exportAeronSystemCounters,
            jvmMetrics);
    }

    /** Register instruments for counters that appeared since the last scan. */
    public void scan()
    {
        countersReader.forEach((counterId, typeId, keyBuffer, label) ->
        {
            if (known.putIfAbsent(counterId, Boolean.TRUE) != null)
            {
                return;
            }
            if (typeId == MetricsRegistry.COUNTER_TYPE_ID || typeId == MetricsRegistry.GAUGE_TYPE_ID)
            {
                final CounterLabel parsed = CounterLabel.parse(label);
                register(parsed.name(), typeId == MetricsRegistry.COUNTER_TYPE_ID, counterId, parsed.attributes());
            }
            else if (exportAeronSystemCounters && typeId == AERON_SYSTEM_COUNTER_TYPE_ID)
            {
                register("aeron.system", false, counterId, Attributes.builder().put("counter", label).build());
            }
        });
    }

    private void register(final String name, final boolean monotonic, final int counterId, final Attributes attributes)
    {
        final List<Series> series = seriesByMetric.computeIfAbsent(name, (n) ->
        {
            final List<Series> list = new CopyOnWriteArrayList<>();
            if (monotonic)
            {
                meter.counterBuilder(n).buildWithCallback((m) -> observe(list, m));
            }
            else
            {
                meter.gaugeBuilder(n).ofLongs().buildWithCallback((m) -> observe(list, m));
            }
            return list;
        });
        series.add(new Series(counterId, attributes));
    }

    private void observe(final List<Series> series, final ObservableLongMeasurement measurement)
    {
        for (final Series s : series)
        {
            if (countersReader.getCounterState(s.counterId()) == CountersReader.RECORD_ALLOCATED)
            {
                measurement.record(countersReader.getCounterValue(s.counterId()), s.attributes());
            }
        }
    }

    private void scanQuietly()
    {
        try
        {
            scan();
        }
        catch (final RuntimeException ex)
        {
            System.getLogger("mdp.otel").log(System.Logger.Level.WARNING, "counter scan failed", ex);
        }
    }

    public int metricCount()
    {
        return seriesByMetric.size();
    }

    public void forceFlush()
    {
        meterProvider.forceFlush().join(5, TimeUnit.SECONDS);
    }

    public void close()
    {
        scanner.shutdownNow();
        if (null != runtimeTelemetry)
        {
            runtimeTelemetry.close();
        }
        meterProvider.close();
    }
}
