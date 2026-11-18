package io.mdp.core.consumer;

import io.mdp.sbe.BookUpdateDecoder;
import io.mdp.sbe.FeedEventDecoder;
import io.mdp.sbe.InstrumentDefinitionDecoder;
import io.mdp.sbe.InstrumentStatusDecoder;
import io.mdp.sbe.TradeDecoder;

/**
 * Consumer callback API. Decoders are flyweights valid only for the duration of the call.
 */
public interface MarketDataListener
{
    default void onBookUpdate(final BookUpdateDecoder update)
    {
    }

    default void onTrade(final TradeDecoder trade)
    {
    }

    default void onInstrumentStatus(final InstrumentStatusDecoder status)
    {
    }

    default void onInstrumentDefinition(final InstrumentDefinitionDecoder definition)
    {
    }

    default void onFeedEvent(final FeedEventDecoder event)
    {
    }

    static MarketDataListener composite(final MarketDataListener... listeners)
    {
        if (listeners.length == 1)
        {
            return listeners[0];
        }
        return new MarketDataListener()
        {
            public void onBookUpdate(final BookUpdateDecoder update)
            {
                for (final MarketDataListener l : listeners)
                {
                    l.onBookUpdate(update);
                }
            }

            public void onTrade(final TradeDecoder trade)
            {
                for (final MarketDataListener l : listeners)
                {
                    l.onTrade(trade);
                }
            }

            public void onInstrumentStatus(final InstrumentStatusDecoder status)
            {
                for (final MarketDataListener l : listeners)
                {
                    l.onInstrumentStatus(status);
                }
            }

            public void onInstrumentDefinition(final InstrumentDefinitionDecoder definition)
            {
                for (final MarketDataListener l : listeners)
                {
                    l.onInstrumentDefinition(definition);
                }
            }

            public void onFeedEvent(final FeedEventDecoder event)
            {
                for (final MarketDataListener l : listeners)
                {
                    l.onFeedEvent(event);
                }
            }
        };
    }
}
