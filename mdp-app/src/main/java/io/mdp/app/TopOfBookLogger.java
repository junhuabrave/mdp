package io.mdp.app;

import java.util.concurrent.TimeUnit;

import io.mdp.core.book.BookListener;
import io.mdp.core.book.PriceLevelBook;
import io.mdp.sbe.FeedEventDecoder;
import io.mdp.sbe.TradeDecoder;

/**
 * Demo listener: counts updates and prints a top-of-book line at most once per second per process.
 * Formatting allocates, but only once a second on the consumer thread.
 */
final class TopOfBookLogger implements BookListener
{
    private static final System.Logger LOG = System.getLogger("mdp.book");
    private final long intervalNs = TimeUnit.SECONDS.toNanos(1);
    private long lastLogNs;
    private long updates;
    private long trades;

    public void onBookChanged(final PriceLevelBook book, final boolean endOfEvent, final long receiveTimeNs)
    {
        updates++;
        final long now = System.nanoTime();
        if (endOfEvent && now - lastLogNs > intervalNs)
        {
            lastLogNs = now;
            LOG.log(System.Logger.Level.INFO, String.format(
                "instrument=%d bid=%d ask=%d levels=%d/%d stale=%s updates=%d trades=%d",
                book.instrumentId(), book.bestBid(), book.bestAsk(),
                book.levels(io.mdp.sbe.Side.BUY), book.levels(io.mdp.sbe.Side.SELL), book.isStale(), updates, trades));
        }
    }

    public void onTrade(final TradeDecoder trade)
    {
        trades++;
    }

    public void onBookStateChanged(final PriceLevelBook book, final boolean stale)
    {
        if (book.instrumentId() % 10 == 1)
        {
            LOG.log(System.Logger.Level.INFO, "instrument=" + book.instrumentId() + (stale ? " STALE" : " LIVE"));
        }
    }

    public void onFeedEvent(final FeedEventDecoder event)
    {
        if (event.instrumentId() == 0)
        {
            LOG.log(System.Logger.Level.INFO, String.format("feed event %s channel=%d line=%d seq=[%d,%d]",
                event.eventType(), event.channelId(), event.lineId(), event.fromSeq(), event.toSeq()));
        }
    }
}
