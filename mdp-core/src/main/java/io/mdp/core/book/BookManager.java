package io.mdp.core.book;

import io.mdp.core.consumer.MarketDataListener;
import io.mdp.sbe.BookType;
import io.mdp.sbe.BookUpdateDecoder;
import io.mdp.sbe.FeedEventDecoder;
import io.mdp.sbe.TradeDecoder;
import org.agrona.collections.IntHashSet;
import org.agrona.collections.Long2ObjectHashMap;

/**
 * Builds market-by-price books (outright and implied, kept separate) from normalized events and
 * tracks per-book stale state from {@code FeedEvent}s. MBO events are passed through untouched.
 * <p>
 * Books are created on first sight (one allocation per instrument); updates are allocation free.
 */
public final class BookManager implements MarketDataListener
{
    private final Long2ObjectHashMap<PriceLevelBook> books = new Long2ObjectHashMap<>();
    private final Long2ObjectHashMap<PriceLevelBook> impliedBooks = new Long2ObjectHashMap<>();
    private final IntHashSet liveChannels = new IntHashSet();
    private final int depth;
    private final BookListener listener;

    public BookManager(final int depth, final BookListener listener)
    {
        this.depth = depth;
        this.listener = listener;
    }

    public PriceLevelBook book(final long instrumentId)
    {
        return books.get(instrumentId);
    }

    public PriceLevelBook impliedBook(final long instrumentId)
    {
        return impliedBooks.get(instrumentId);
    }

    public int bookCount()
    {
        return books.size();
    }

    public void onBookUpdate(final BookUpdateDecoder update)
    {
        final BookType bookType = update.bookType();
        if (bookType == BookType.MBO)
        {
            return;
        }

        final Long2ObjectHashMap<PriceLevelBook> map = bookType == BookType.IMPLIED ? impliedBooks : books;
        final long instrumentId = update.instrumentId();
        PriceLevelBook book = map.get(instrumentId);
        if (null == book)
        {
            book = new PriceLevelBook(instrumentId, (int)update.channelId(), depth);
            book.priceExponent(update.priceExponent());
            book.stale(!liveChannels.contains((int)update.channelId()));
            map.put(instrumentId, book);
        }

        book.apply(
            update.side(),
            update.action(),
            update.level(),
            update.priceMantissa(),
            update.size(),
            update.numOrders());
        final long receiveTimeNs = update.receiveTimeNs();
        book.lastUpdateNs(receiveTimeNs);
        listener.onBookChanged(book, update.flags().lastInEvent(), receiveTimeNs);
    }

    public void onTrade(final TradeDecoder trade)
    {
        listener.onTrade(trade);
    }

    public void onFeedEvent(final FeedEventDecoder event)
    {
        final long channelId = event.channelId();
        final long instrumentId = event.instrumentId();
        switch (event.eventType())
        {
            case BOOK_STALE -> setStale(channelId, instrumentId, true);
            case RECOVERY_COMPLETE -> setStale(channelId, instrumentId, false);
            case CHANNEL_RESET ->
            {
                clearChannel(books, channelId);
                clearChannel(impliedBooks, channelId);
            }
            default ->
            {
            }
        }
        listener.onFeedEvent(event);
    }

    private void setStale(final long channelId, final long instrumentId, final boolean stale)
    {
        if (instrumentId == 0)
        {
            if (stale)
            {
                liveChannels.remove((int)channelId);
            }
            else
            {
                liveChannels.add((int)channelId);
            }
        }
        else
        {
            setStale(books.get(instrumentId), stale);
            setStale(impliedBooks.get(instrumentId), stale);
            return;
        }
        for (final PriceLevelBook book : books.values())
        {
            if (book.channelId() == channelId)
            {
                setStale(book, stale);
            }
        }
        for (final PriceLevelBook book : impliedBooks.values())
        {
            if (book.channelId() == channelId)
            {
                setStale(book, stale);
            }
        }
    }

    private void setStale(final PriceLevelBook book, final boolean stale)
    {
        if (null != book && book.isStale() != stale)
        {
            book.stale(stale);
            listener.onBookStateChanged(book, stale);
        }
    }

    private static void clearChannel(final Long2ObjectHashMap<PriceLevelBook> map, final long channelId)
    {
        for (final PriceLevelBook book : map.values())
        {
            if (book.channelId() == channelId)
            {
                book.clear();
            }
        }
    }
}
