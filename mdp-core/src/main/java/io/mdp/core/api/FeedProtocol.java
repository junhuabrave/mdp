package io.mdp.core.api;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/**
 * A venue protocol plug-in, discovered through {@link ServiceLoader}.
 * <p>
 * To add a venue: implement this interface, list it in
 * {@code META-INF/services/io.mdp.core.api.FeedProtocol}, and select it with {@code mdp.protocol=<name>}.
 */
public interface FeedProtocol
{
    /** Unique name used in configuration, for example {@code cme-mdp3}. */
    String name();

    /** Venue id written to every normalized event. */
    int venueId();

    PacketHeaderReader packetHeader();

    FeedDecoder newDecoder(DecoderContext context);

    static FeedProtocol load(final String name)
    {
        final List<String> available = new ArrayList<>();
        for (final FeedProtocol protocol : ServiceLoader.load(FeedProtocol.class))
        {
            if (protocol.name().equals(name))
            {
                return protocol;
            }
            available.add(protocol.name());
        }
        throw new IllegalArgumentException("unknown protocol '" + name + "', available: " + available);
    }
}
