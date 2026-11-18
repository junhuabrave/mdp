package io.mdp.euronext;

import java.nio.ByteOrder;

import io.mdp.euronext.sbe.ActionType;
import io.mdp.euronext.sbe.AggressorSide;
import io.mdp.euronext.sbe.BookState;
import io.mdp.euronext.sbe.EndOfSnapshotEncoder;
import io.mdp.euronext.sbe.FullTradeInformationEncoder;
import io.mdp.euronext.sbe.MarketDataUpdateType;
import io.mdp.euronext.sbe.MarketStatusChangeEncoder;
import io.mdp.euronext.sbe.MarketUpdateEncoder;
import io.mdp.euronext.sbe.MessageHeaderEncoder;
import io.mdp.euronext.sbe.OrderSide;
import io.mdp.euronext.sbe.OrderUpdateEncoder;
import io.mdp.euronext.sbe.StandingDataEncoder;
import io.mdp.euronext.sbe.StartOfSnapshotEncoder;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Builds Optiq MDG packets (subset schema) for tests, the simulator and benchmarks.
 */
public final class OptiqPacketWriter
{
    public record Level(MarketDataUpdateType type, int level, long price, long quantity, int orders)
    {
    }

    private final UnsafeBuffer buffer;
    private final MessageHeaderEncoder header = new MessageHeaderEncoder();
    private final MarketUpdateEncoder marketUpdate = new MarketUpdateEncoder();
    private final OrderUpdateEncoder orderUpdate = new OrderUpdateEncoder();
    private final FullTradeInformationEncoder trade = new FullTradeInformationEncoder();
    private final MarketStatusChangeEncoder status = new MarketStatusChangeEncoder();
    private final StandingDataEncoder standingData = new StandingDataEncoder();
    private final StartOfSnapshotEncoder startOfSnapshot = new StartOfSnapshotEncoder();
    private final EndOfSnapshotEncoder endOfSnapshot = new EndOfSnapshotEncoder();
    private int limit;

    public OptiqPacketWriter(final int capacity)
    {
        buffer = new UnsafeBuffer(new byte[capacity]);
    }

    public OptiqPacketWriter begin(final long seq, final long sendingTimeNs, final int channelId)
    {
        buffer.putLong(OptiqPacketHeader.SENDING_TIME_OFFSET, sendingTimeNs, ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(OptiqPacketHeader.SEQ_OFFSET, (int)seq, ByteOrder.LITTLE_ENDIAN);
        buffer.putShort(OptiqPacketHeader.FLAGS_OFFSET, (short)0, ByteOrder.LITTLE_ENDIAN);
        buffer.putShort(OptiqPacketHeader.CHANNEL_OFFSET, (short)channelId, ByteOrder.LITTLE_ENDIAN);
        limit = OptiqPacketHeader.LENGTH;
        return this;
    }

    public OptiqPacketWriter marketUpdate(final int symbol, final long eventTime, final Level... levels)
    {
        marketUpdate.wrapAndApplyHeader(buffer, limit, header)
            .symbolIndex(symbol)
            .eventTime(eventTime)
            .rebroadcastIndicator((short)0);
        final MarketUpdateEncoder.MarketDataUpdatesEncoder group = marketUpdate.marketDataUpdatesCount(levels.length);
        for (final Level l : levels)
        {
            group.next()
                .updateType(l.type())
                .level((short)l.level())
                .price(l.price())
                .quantity(l.quantity())
                .orderCount(l.orders());
        }
        limit += MessageHeaderEncoder.ENCODED_LENGTH + marketUpdate.encodedLength();
        return this;
    }

    public OptiqPacketWriter orderUpdate(
        final int symbol, final long eventTime, final ActionType action, final OrderSide side,
        final long priority, final long price, final long quantity)
    {
        orderUpdate.wrapAndApplyHeader(buffer, limit, header)
            .symbolIndex(symbol)
            .eventTime(eventTime)
            .actionType(action)
            .orderSide(side)
            .orderPriority(priority)
            .orderPx(price)
            .orderQuantity(quantity);
        limit += MessageHeaderEncoder.ENCODED_LENGTH + orderUpdate.encodedLength();
        return this;
    }

    public OptiqPacketWriter trade(
        final int symbol, final long eventTime, final long tradeId, final long price, final long quantity,
        final AggressorSide aggressor)
    {
        trade.wrapAndApplyHeader(buffer, limit, header)
            .symbolIndex(symbol)
            .eventTime(eventTime)
            .tradeUniqueIdentifier(tradeId)
            .price(price)
            .tradeQty(quantity)
            .aggressorSide(aggressor);
        limit += MessageHeaderEncoder.ENCODED_LENGTH + trade.encodedLength();
        return this;
    }

    public OptiqPacketWriter status(final int symbol, final long eventTime, final BookState state)
    {
        status.wrapAndApplyHeader(buffer, limit, header).symbolIndex(symbol).eventTime(eventTime).bookState(state);
        limit += MessageHeaderEncoder.ENCODED_LENGTH + status.encodedLength();
        return this;
    }

    public OptiqPacketWriter standingData(
        final int symbol, final int priceDecimals, final String isin, final String name, final long tickSize)
    {
        standingData.wrapAndApplyHeader(buffer, limit, header)
            .symbolIndex(symbol)
            .priceDecimals((short)priceDecimals)
            .quantityDecimals((short)0)
            .iSINCode(isin)
            .mnemonic(name.length() > 5 ? name.substring(0, 5) : name)
            .instrumentName(name)
            .currencyCode("EUR")
            .tickSize(tickSize);
        limit += MessageHeaderEncoder.ENCODED_LENGTH + standingData.encodedLength();
        return this;
    }

    public OptiqPacketWriter startOfSnapshot(final long lastPacketSeq)
    {
        startOfSnapshot.wrapAndApplyHeader(buffer, limit, header).lastPacketSequenceNumber(lastPacketSeq);
        limit += MessageHeaderEncoder.ENCODED_LENGTH + startOfSnapshot.encodedLength();
        return this;
    }

    public OptiqPacketWriter endOfSnapshot(final long lastPacketSeq)
    {
        endOfSnapshot.wrapAndApplyHeader(buffer, limit, header).lastPacketSequenceNumber(lastPacketSeq);
        limit += MessageHeaderEncoder.ENCODED_LENGTH + endOfSnapshot.encodedLength();
        return this;
    }

    public DirectBuffer buffer()
    {
        return buffer;
    }

    public int length()
    {
        return limit;
    }
}
