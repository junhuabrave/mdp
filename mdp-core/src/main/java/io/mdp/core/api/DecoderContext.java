package io.mdp.core.api;

import io.mdp.core.config.MdpConfig;
import io.mdp.core.metrics.MetricsRegistry;
import io.mdp.core.sink.NormalizedSink;

/**
 * Everything a venue decoder needs for one channel.
 *
 * @param channelId exchange channel id (CME channel, Optiq channel)
 * @param sink      where normalized events are written
 * @param metrics   metrics registry for decoder counters
 * @param config    full configuration, for venue-specific settings
 */
public record DecoderContext(int channelId, NormalizedSink sink, MetricsRegistry metrics, MdpConfig config)
{
}
