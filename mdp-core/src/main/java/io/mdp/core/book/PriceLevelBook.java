package io.mdp.core.book;

import io.mdp.sbe.Side;
import io.mdp.sbe.UpdateAction;

/**
 * Fixed-depth, level-indexed market-by-price book (CME MBP semantics), backed by primitive arrays.
 * Levels are 1-based. Not thread safe: owned by one consumer thread.
 */
public final class PriceLevelBook
{
    public static final long NULL_PRICE = Long.MIN_VALUE;

    private final long instrumentId;
    private final int channelId;
    private final int depth;
    private final Ladder bids;
    private final Ladder asks;
    private int priceExponent;
    private boolean stale = true;
    private long lastUpdateNs;

    public PriceLevelBook(final long instrumentId, final int channelId, final int depth)
    {
        this.instrumentId = instrumentId;
        this.channelId = channelId;
        this.depth = depth;
        this.bids = new Ladder(depth);
        this.asks = new Ladder(depth);
    }

    public void apply(
        final Side side,
        final UpdateAction action,
        final int level,
        final long price,
        final long size,
        final int orders)
    {
        if (action == UpdateAction.CLEAR)
        {
            clear();
            return;
        }

        final Ladder ladder = side == Side.BUY ? bids : asks;
        switch (action)
        {
            case NEW -> ladder.insert(level, price, size, orders);
            case CHANGE, OVERLAY -> ladder.set(level, price, size, orders);
            case DELETE -> ladder.delete(level);
            case DELETE_THRU -> ladder.clear();
            case DELETE_FROM -> ladder.deleteTop(level);
            default ->
            {
            }
        }
    }

    public void clear()
    {
        bids.clear();
        asks.clear();
    }

    public long instrumentId()
    {
        return instrumentId;
    }

    public int channelId()
    {
        return channelId;
    }

    public int depth()
    {
        return depth;
    }

    public int priceExponent()
    {
        return priceExponent;
    }

    public void priceExponent(final int priceExponent)
    {
        this.priceExponent = priceExponent;
    }

    public boolean isStale()
    {
        return stale;
    }

    public void stale(final boolean stale)
    {
        this.stale = stale;
    }

    public long lastUpdateNs()
    {
        return lastUpdateNs;
    }

    public void lastUpdateNs(final long lastUpdateNs)
    {
        this.lastUpdateNs = lastUpdateNs;
    }

    public int levels(final Side side)
    {
        return ladder(side).count;
    }

    /** Price mantissa at a 1-based level, or {@link #NULL_PRICE}. */
    public long price(final Side side, final int level)
    {
        final Ladder l = ladder(side);
        return level >= 1 && level <= l.count ? l.prices[level - 1] : NULL_PRICE;
    }

    public long size(final Side side, final int level)
    {
        final Ladder l = ladder(side);
        return level >= 1 && level <= l.count ? l.sizes[level - 1] : 0;
    }

    public int orders(final Side side, final int level)
    {
        final Ladder l = ladder(side);
        return level >= 1 && level <= l.count ? l.orders[level - 1] : 0;
    }

    public long bestBid()
    {
        return price(Side.BUY, 1);
    }

    public long bestAsk()
    {
        return price(Side.SELL, 1);
    }

    private Ladder ladder(final Side side)
    {
        return side == Side.BUY ? bids : asks;
    }

    public String toString()
    {
        final StringBuilder sb = new StringBuilder("Book[").append(instrumentId).append(stale ? " STALE" : "").append("]\n");
        for (int i = 1; i <= Math.max(bids.count, asks.count); i++)
        {
            sb.append(String.format("%2d  %8d @ %-14d | %-14d @ %8d%n",
                i, size(Side.BUY, i), price(Side.BUY, i), price(Side.SELL, i), size(Side.SELL, i)));
        }
        return sb.toString();
    }

    private static final class Ladder
    {
        final long[] prices;
        final long[] sizes;
        final int[] orders;
        final int depth;
        int count;

        Ladder(final int depth)
        {
            this.depth = depth;
            prices = new long[depth];
            sizes = new long[depth];
            orders = new int[depth];
            clear();
        }

        void insert(final int level, final long price, final long size, final int numOrders)
        {
            if (level < 1 || level > depth)
            {
                return;
            }
            final int i = level - 1;
            final int moveCount = Math.min(count, depth - 1) - i;
            if (moveCount > 0)
            {
                System.arraycopy(prices, i, prices, i + 1, moveCount);
                System.arraycopy(sizes, i, sizes, i + 1, moveCount);
                System.arraycopy(orders, i, orders, i + 1, moveCount);
            }
            prices[i] = price;
            sizes[i] = size;
            orders[i] = numOrders;
            count = Math.min(depth, Math.max(count + 1, level));
        }

        void set(final int level, final long price, final long size, final int numOrders)
        {
            if (level < 1 || level > depth)
            {
                return;
            }
            final int i = level - 1;
            prices[i] = price;
            sizes[i] = size;
            orders[i] = numOrders;
            if (level > count)
            {
                count = level;
            }
        }

        void delete(final int level)
        {
            if (level < 1 || level > count)
            {
                return;
            }
            final int i = level - 1;
            final int moveCount = count - level;
            if (moveCount > 0)
            {
                System.arraycopy(prices, i + 1, prices, i, moveCount);
                System.arraycopy(sizes, i + 1, sizes, i, moveCount);
                System.arraycopy(orders, i + 1, orders, i, moveCount);
            }
            count--;
            prices[count] = NULL_PRICE;
            sizes[count] = 0;
            orders[count] = 0;
        }

        /** CME DeleteFrom: remove the top {@code levels} levels, shifting the rest up. */
        void deleteTop(final int levels)
        {
            final int n = Math.min(levels, count);
            for (int k = 0; k < n; k++)
            {
                delete(1);
            }
        }

        void clear()
        {
            java.util.Arrays.fill(prices, NULL_PRICE);
            java.util.Arrays.fill(sizes, 0);
            java.util.Arrays.fill(orders, 0);
            count = 0;
        }
    }
}
