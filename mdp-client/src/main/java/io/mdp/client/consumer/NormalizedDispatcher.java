package io.mdp.client.consumer;

import io.mdp.sbe.BookUpdateDecoder;
import io.mdp.sbe.FeedEventDecoder;
import io.mdp.sbe.InstrumentDefinitionDecoder;
import io.mdp.sbe.InstrumentStatusDecoder;
import io.mdp.sbe.MessageHeaderDecoder;
import io.mdp.sbe.TradeDecoder;
import org.agrona.DirectBuffer;

/**
 * Decodes normalized SBE messages with reusable flyweights and dispatches them to a
 * {@link MarketDataListener}. Allocation free.
 */
public final class NormalizedDispatcher implements NormalizedMessageHandler
{
    private final MessageHeaderDecoder header = new MessageHeaderDecoder();
    private final BookUpdateDecoder bookUpdate = new BookUpdateDecoder();
    private final TradeDecoder trade = new TradeDecoder();
    private final InstrumentStatusDecoder status = new InstrumentStatusDecoder();
    private final InstrumentDefinitionDecoder definition = new InstrumentDefinitionDecoder();
    private final FeedEventDecoder feedEvent = new FeedEventDecoder();
    private final MarketDataListener listener;

    public NormalizedDispatcher(final MarketDataListener listener)
    {
        this.listener = listener;
    }

    public void onMessage(final DirectBuffer buffer, final int offset, final int length)
    {
        header.wrap(buffer, offset);
        if (header.schemaId() != MessageHeaderDecoder.SCHEMA_ID)
        {
            return;
        }

        final int blockLength = header.blockLength();
        final int version = header.version();
        final int bodyOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH;

        switch (header.templateId())
        {
            case BookUpdateDecoder.TEMPLATE_ID ->
                listener.onBookUpdate(bookUpdate.wrap(buffer, bodyOffset, blockLength, version));
            case TradeDecoder.TEMPLATE_ID ->
                listener.onTrade(trade.wrap(buffer, bodyOffset, blockLength, version));
            case InstrumentStatusDecoder.TEMPLATE_ID ->
                listener.onInstrumentStatus(status.wrap(buffer, bodyOffset, blockLength, version));
            case InstrumentDefinitionDecoder.TEMPLATE_ID ->
                listener.onInstrumentDefinition(definition.wrap(buffer, bodyOffset, blockLength, version));
            case FeedEventDecoder.TEMPLATE_ID ->
                listener.onFeedEvent(feedEvent.wrap(buffer, bodyOffset, blockLength, version));
            default ->
            {
                // newer producer, unknown template: skip
            }
        }
    }
}
