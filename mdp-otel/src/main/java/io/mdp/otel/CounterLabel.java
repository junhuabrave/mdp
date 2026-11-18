package io.mdp.otel;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;

/**
 * Parses counter labels of the form {@code metric.name{key=value,key=value}}.
 */
record CounterLabel(String name, Attributes attributes)
{
    static CounterLabel parse(final String label)
    {
        final int brace = label.indexOf('{');
        if (brace < 0 || !label.endsWith("}"))
        {
            return new CounterLabel(label.trim(), Attributes.empty());
        }
        final AttributesBuilder builder = Attributes.builder();
        final String body = label.substring(brace + 1, label.length() - 1);
        for (final String kv : body.split(","))
        {
            final int eq = kv.indexOf('=');
            if (eq > 0)
            {
                builder.put(kv.substring(0, eq).trim(), kv.substring(eq + 1).trim());
            }
        }
        return new CounterLabel(label.substring(0, brace).trim(), builder.build());
    }
}
