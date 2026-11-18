package io.mdp.core.book;

import io.mdp.sbe.FeedEventDecoder;
import io.mdp.sbe.TradeDecoder;

/**
 * Strategy-facing callbacks from {@link BookManager}.
 */
public interface BookListener
{
    /**
     * A book changed.
     *
     * @param book          the updated book (owned by the manager; do not keep a reference across threads)
     * @param endOfEvent    true when the exchange event is complete; act on the book only then
     * @param receiveTimeNs local receive time of the packet that caused the change
     */
    default void onBookChanged(final PriceLevelBook book, final boolean endOfEvent, final long receiveTimeNs)
    {
    }

    default void onTrade(final TradeDecoder trade)
    {
    }

    /** Book became stale (do not trade on it) or recovered. */
    default void onBookStateChanged(final PriceLevelBook book, final boolean stale)
    {
    }

    default void onFeedEvent(final FeedEventDecoder event)
    {
    }
}
