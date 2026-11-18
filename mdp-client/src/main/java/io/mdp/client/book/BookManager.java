package io.mdp.client.book;

import java.util.Collection;

import io.mdp.client.consumer.MarketDataListener;
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
    private LiveGate liveGate = (book, type) -> true;

    /** Decides whether a book may leave the stale state; used by recovering consumers. */
    @FunctionalInterface
    public interface LiveGate
    {
        boolean mayGoLive(PriceLevelBook book, BookType type);
    }

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

    /** Outright books, for iteration by snapshot publishers. Not a copy: do not modify. */
    public Collection<PriceLevelBook> books()
    {
        return books.values();
    }

    public Collection<PriceLevelBook> impliedBooks()
    {
        return impliedBooks.values();
    }

    public PriceLevelBook book(final BookType bookType, final long instrumentId)
    {
        return (bookType == BookType.IMPLIED ? impliedBooks : books).get(instrumentId);
    }

    /** Book for a snapshot image: created if missing, without waiting for an incremental update. */
    public PriceLevelBook getOrCreateBook(
        final BookType bookType, final long instrumentId, final int channelId, final int priceExponent,
        final int sizeExponent)
    {
        final Long2ObjectHashMap<PriceLevelBook> map = bookType == BookType.IMPLIED ? impliedBooks : books;
        PriceLevelBook book = map.get(instrumentId);
        if (null == book)
        {
            book = new PriceLevelBook(instrumentId, channelId, depth);
            map.put(instrumentId, book);
        }
        book.priceExponent(priceExponent);
        book.sizeExponent(sizeExponent);
        return book;
    }

    /** True once the channel's {@code RECOVERY_COMPLETE} has been seen and no later {@code BOOK_STALE}. */
    public boolean isChannelLive(final int channelId)
    {
        return liveChannels.contains(channelId);
    }

    /** Set a book's stale state, notifying the listener if it changed. */
    public void setBookStale(final PriceLevelBook book, final boolean stale)
    {
        setStale(book, stale);
    }

    /** Mark every book stale, for example after the consumer lost part of the stream. */
    public void markAllStale()
    {
        liveChannels.clear();
        for (final PriceLevelBook book : books.values())
        {
            setStale(book, true);
        }
        for (final PriceLevelBook book : impliedBooks.values())
        {
            setStale(book, true);
        }
    }

    public BookListener listener()
    {
        return listener;
    }

    public void liveGate(final LiveGate liveGate)
    {
        this.liveGate = liveGate;
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
            book.sizeExponent(update.sizeExponent());
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
            setStale(books.get(instrumentId), BookType.MBP, stale);
            setStale(impliedBooks.get(instrumentId), BookType.IMPLIED, stale);
            return;
        }
        for (final PriceLevelBook book : books.values())
        {
            if (book.channelId() == channelId)
            {
                setStale(book, BookType.MBP, stale);
            }
        }
        for (final PriceLevelBook book : impliedBooks.values())
        {
            if (book.channelId() == channelId)
            {
                setStale(book, BookType.IMPLIED, stale);
            }
        }
    }

    private void setStale(final PriceLevelBook book, final BookType type, final boolean stale)
    {
        if (null != book && !stale && !liveGate.mayGoLive(book, type))
        {
            return;
        }
        setStale(book, stale);
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
