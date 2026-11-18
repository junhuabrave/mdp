package io.mdp.client.stream;

import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.concurrent.Agent;

/**
 * Agent that feeds a {@link RecoveringBookStream} from the normalized incremental and snapshot streams.
 * <p>
 * Subscribe to both with {@code tether=false} (IPC) or over multicast, so a slow consumer is dropped by
 * Aeron rather than slowing the feed; the resulting gap is repaired from the snapshot loop.
 * The snapshot stream is polled only while recovering; images for synced books are ignored.
 */
public final class MdpStreamClient implements Agent
{
    private final Subscription incremental;
    private final Subscription snapshot;
    private final RecoveringBookStream stream;
    private final int fragmentLimit;
    private final FragmentHandler incrementalHandler;
    private final FragmentHandler snapshotHandler;
    private final String roleName;

    public MdpStreamClient(
        final Subscription incremental,
        final Subscription snapshot,
        final RecoveringBookStream stream,
        final int fragmentLimit)
    {
        this.incremental = incremental;
        this.snapshot = snapshot;
        this.stream = stream;
        this.fragmentLimit = fragmentLimit;
        this.incrementalHandler = (buffer, offset, length, header) -> stream.onIncremental(buffer, offset, length);
        this.snapshotHandler = (buffer, offset, length, header) -> stream.onSnapshot(buffer, offset, length);
        this.roleName = "mdp-client-" + incremental.streamId();
    }

    public int doWork()
    {
        int work = incremental.poll(incrementalHandler, fragmentLimit);
        if (stream.isRecovering())
        {
            work += snapshot.poll(snapshotHandler, fragmentLimit);
        }
        return work;
    }

    public RecoveringBookStream stream()
    {
        return stream;
    }

    public String roleName()
    {
        return roleName;
    }
}
