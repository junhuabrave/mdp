package io.mdp.core.sink;

import java.util.concurrent.TimeUnit;

import io.mdp.core.metrics.MetricsRegistry;
import io.mdp.sbe.BookUpdateEncoder;
import io.mdp.sbe.FeedEventEncoder;
import io.mdp.sbe.InstrumentDefinitionEncoder;
import io.mdp.sbe.InstrumentStatusEncoder;
import io.mdp.sbe.MessageHeaderEncoder;
import io.mdp.sbe.TradeEncoder;
import org.agrona.BufferUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;

/**
 * Shared claim/commit logic. Subclasses only implement the transport-specific claim.
 */
public abstract class AbstractNormalizedSink implements NormalizedSink
{
    private static final long FEED_EVENT_SPIN_LIMIT_NS = TimeUnit.MILLISECONDS.toNanos(5);

    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final BookUpdateEncoder bookUpdate = new BookUpdateEncoder();
    private final TradeEncoder trade = new TradeEncoder();
    private final InstrumentStatusEncoder instrumentStatus = new InstrumentStatusEncoder();
    private final InstrumentDefinitionEncoder instrumentDefinition = new InstrumentDefinitionEncoder();
    private final FeedEventEncoder feedEvent = new FeedEventEncoder();
    private final UnsafeBuffer scratch = new UnsafeBuffer(BufferUtil.allocateDirectAligned(1024, 64));

    private final BackPressurePolicy policy;
    private final AtomicCounter dropped;
    private final AtomicCounter published;
    private boolean usingScratch;
    private boolean droppedSinceLastPoll;

    protected AbstractNormalizedSink(
        final BackPressurePolicy policy, final MetricsRegistry metrics, final String sinkName)
    {
        this.policy = policy;
        this.dropped = metrics.counter("mdp.sink.dropped", "sink", sinkName);
        this.published = metrics.counter("mdp.sink.published", "sink", sinkName);
    }

    /**
     * Try to claim {@code length} bytes for a message of {@code templateId}.
     *
     * @return true if claimed; {@link #claimBuffer()} and {@link #claimOffset()} then describe the space.
     */
    protected abstract boolean tryClaim(int templateId, int length);

    protected abstract MutableDirectBuffer claimBuffer();

    protected abstract int claimOffset();

    protected abstract void commitClaim();

    public BookUpdateEncoder bookUpdate()
    {
        claim(BookUpdateEncoder.TEMPLATE_ID, BookUpdateEncoder.BLOCK_LENGTH, false);
        return bookUpdate.wrapAndApplyHeader(buffer(), offset(), headerEncoder);
    }

    public TradeEncoder trade()
    {
        claim(TradeEncoder.TEMPLATE_ID, TradeEncoder.BLOCK_LENGTH, false);
        return trade.wrapAndApplyHeader(buffer(), offset(), headerEncoder);
    }

    public InstrumentStatusEncoder instrumentStatus()
    {
        claim(InstrumentStatusEncoder.TEMPLATE_ID, InstrumentStatusEncoder.BLOCK_LENGTH, false);
        return instrumentStatus.wrapAndApplyHeader(buffer(), offset(), headerEncoder);
    }

    public InstrumentDefinitionEncoder instrumentDefinition()
    {
        claim(InstrumentDefinitionEncoder.TEMPLATE_ID, InstrumentDefinitionEncoder.BLOCK_LENGTH, false);
        return instrumentDefinition.wrapAndApplyHeader(buffer(), offset(), headerEncoder);
    }

    public FeedEventEncoder feedEvent()
    {
        claim(FeedEventEncoder.TEMPLATE_ID, FeedEventEncoder.BLOCK_LENGTH, true);
        return feedEvent.wrapAndApplyHeader(buffer(), offset(), headerEncoder);
    }

    public void commit()
    {
        if (usingScratch)
        {
            usingScratch = false;
            return;
        }
        commitClaim();
        MetricsRegistry.increment(published);
    }

    public boolean pollDropped()
    {
        final boolean result = droppedSinceLastPoll;
        droppedSinceLastPoll = false;
        return result;
    }

    private void claim(final int templateId, final int blockLength, final boolean mustDeliver)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + blockLength;
        if (tryClaim(templateId, length))
        {
            return;
        }

        if (mustDeliver || policy == BackPressurePolicy.SPIN)
        {
            final long deadline = mustDeliver && policy != BackPressurePolicy.SPIN ?
                System.nanoTime() + FEED_EVENT_SPIN_LIMIT_NS : Long.MAX_VALUE;
            while (System.nanoTime() < deadline)
            {
                Thread.onSpinWait();
                if (tryClaim(templateId, length))
                {
                    return;
                }
            }
        }

        usingScratch = true;
        droppedSinceLastPoll = true;
        MetricsRegistry.increment(dropped);
    }

    private MutableDirectBuffer buffer()
    {
        return usingScratch ? scratch : claimBuffer();
    }

    private int offset()
    {
        return usingScratch ? 0 : claimOffset();
    }
}
