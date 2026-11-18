package io.mdp.core.transport;

/**
 * A single line (for example multicast feed A) delivering raw exchange packets.
 * Implementations: UDP (unicast/multicast/SSM), Aeron (split mode / replay), in-memory (tests, benchmark).
 */
public interface PacketSource extends AutoCloseable
{
    /**
     * Read up to {@code limit} packets.
     *
     * @return number of packets delivered
     */
    int poll(PacketHandler handler, int limit);

    /** Start receiving. Sources are created active. Snapshot sources are toggled by the channel. */
    default void activate()
    {
    }

    /** Stop receiving and discard anything queued, so a later {@link #activate()} sees only fresh data. */
    default void deactivate()
    {
    }

    String name();

    void close();
}
