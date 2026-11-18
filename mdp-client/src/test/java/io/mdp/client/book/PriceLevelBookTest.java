package io.mdp.client.book;

import io.mdp.sbe.Side;
import io.mdp.sbe.UpdateAction;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PriceLevelBookTest
{
    private final PriceLevelBook book = new PriceLevelBook(1, 1, 5);

    @Test
    void newShiftsLevelsDown()
    {
        book.apply(Side.BUY, UpdateAction.NEW, 1, 100, 10, 1);
        book.apply(Side.BUY, UpdateAction.NEW, 1, 101, 20, 2);
        book.apply(Side.BUY, UpdateAction.NEW, 2, 100_5, 5, 1);
        assertEquals(3, book.levels(Side.BUY));
        assertEquals(101, book.price(Side.BUY, 1));
        assertEquals(100_5, book.price(Side.BUY, 2));
        assertEquals(100, book.price(Side.BUY, 3));
    }

    @Test
    void newBeyondDepthDropsLastLevel()
    {
        for (int i = 0; i < 5; i++)
        {
            book.apply(Side.SELL, UpdateAction.NEW, i + 1, 200 + i, 1, 1);
        }
        book.apply(Side.SELL, UpdateAction.NEW, 1, 199, 1, 1);
        assertEquals(5, book.levels(Side.SELL));
        assertEquals(199, book.price(Side.SELL, 1));
        assertEquals(203, book.price(Side.SELL, 5));
    }

    @Test
    void changeDeleteAndDeleteFrom()
    {
        for (int i = 0; i < 4; i++)
        {
            book.apply(Side.BUY, UpdateAction.NEW, i + 1, 100 - i, 10, 1);
        }
        book.apply(Side.BUY, UpdateAction.CHANGE, 2, 99, 77, 3);
        assertEquals(77, book.size(Side.BUY, 2));

        book.apply(Side.BUY, UpdateAction.DELETE, 1, 0, 0, 0);
        assertEquals(3, book.levels(Side.BUY));
        assertEquals(99, book.bestBid());

        book.apply(Side.BUY, UpdateAction.DELETE_FROM, 2, 0, 0, 0);
        assertEquals(1, book.levels(Side.BUY));
        assertEquals(97, book.bestBid());

        book.apply(Side.BUY, UpdateAction.DELETE_THRU, 0, 0, 0, 0);
        assertEquals(0, book.levels(Side.BUY));
        assertEquals(PriceLevelBook.NULL_PRICE, book.bestBid());
    }
}
