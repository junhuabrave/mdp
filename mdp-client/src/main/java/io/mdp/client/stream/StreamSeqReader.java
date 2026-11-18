package io.mdp.client.stream;

import io.mdp.sbe.BookUpdateDecoder;
import io.mdp.sbe.FeedEventDecoder;
import io.mdp.sbe.InstrumentDefinitionDecoder;
import io.mdp.sbe.InstrumentStatusDecoder;
import io.mdp.sbe.MessageHeaderDecoder;
import io.mdp.sbe.SnapshotBeginDecoder;
import io.mdp.sbe.SnapshotEndDecoder;
import io.mdp.sbe.TradeDecoder;
import org.agrona.DirectBuffer;

/**
 * Reads {@code streamSeq} from any normalized message. Allocation free; one instance per thread.
 */
public final class StreamSeqReader
{
    /** Returned for messages without a sequence (older schema version or another schema). */
    public static final long NO_SEQ = 0;

    private final MessageHeaderDecoder header = new MessageHeaderDecoder();
    private final BookUpdateDecoder bookUpdate = new BookUpdateDecoder();
    private final TradeDecoder trade = new TradeDecoder();
    private final InstrumentStatusDecoder status = new InstrumentStatusDecoder();
    private final InstrumentDefinitionDecoder definition = new InstrumentDefinitionDecoder();
    private final FeedEventDecoder feedEvent = new FeedEventDecoder();
    private final SnapshotBeginDecoder snapshotBegin = new SnapshotBeginDecoder();
    private final SnapshotEndDecoder snapshotEnd = new SnapshotEndDecoder();

    public long read(final DirectBuffer buffer, final int offset)
    {
        header.wrap(buffer, offset);
        if (header.schemaId() != MessageHeaderDecoder.SCHEMA_ID || header.version() < 2)
        {
            return NO_SEQ;
        }
        final int body = offset + MessageHeaderDecoder.ENCODED_LENGTH;
        final int blockLength = header.blockLength();
        final int version = header.version();
        return switch (header.templateId())
        {
            case BookUpdateDecoder.TEMPLATE_ID -> bookUpdate.wrap(buffer, body, blockLength, version).streamSeq();
            case TradeDecoder.TEMPLATE_ID -> trade.wrap(buffer, body, blockLength, version).streamSeq();
            case InstrumentStatusDecoder.TEMPLATE_ID -> status.wrap(buffer, body, blockLength, version).streamSeq();
            case InstrumentDefinitionDecoder.TEMPLATE_ID ->
                definition.wrap(buffer, body, blockLength, version).streamSeq();
            case FeedEventDecoder.TEMPLATE_ID -> feedEvent.wrap(buffer, body, blockLength, version).streamSeq();
            case SnapshotBeginDecoder.TEMPLATE_ID -> snapshotBegin.wrap(buffer, body, blockLength, version).streamSeq();
            case SnapshotEndDecoder.TEMPLATE_ID -> snapshotEnd.wrap(buffer, body, blockLength, version).streamSeq();
            default -> NO_SEQ;
        };
    }

    /** Template id of the message last passed to {@link #read}. */
    public int templateId()
    {
        return header.templateId();
    }
}
