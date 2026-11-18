package io.mdp.cme;

import java.nio.ByteOrder;

import io.mdp.cme.sbe.AggressorSide;
import io.mdp.cme.sbe.ChannelReset4Encoder;
import io.mdp.cme.sbe.MDEntryType;
import io.mdp.cme.sbe.MDEntryTypeBook;
import io.mdp.cme.sbe.MDIncrementalRefreshBook46Encoder;
import io.mdp.cme.sbe.MDIncrementalRefreshTradeSummary48Encoder;
import io.mdp.cme.sbe.MDInstrumentDefinitionFuture54Encoder;
import io.mdp.cme.sbe.MDUpdateAction;
import io.mdp.cme.sbe.MessageHeaderEncoder;
import io.mdp.cme.sbe.SecurityStatus30Encoder;
import io.mdp.cme.sbe.SecurityTradingStatus;
import io.mdp.cme.sbe.SecurityUpdateAction;
import io.mdp.cme.sbe.SnapshotFullRefresh52Encoder;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Builds MDP 3.0 packets with the generated encoders. Used by tests, the feed simulator and the
 * benchmark (packets are built ahead of time there, so convenience matters more than allocation).
 */
public final class CmePacketWriter
{
    public record BookEntry(
        int securityId, long rptSeq, char entryType, MDUpdateAction action, int level, long price, int size, int orders)
    {
    }

    public record TradeEntry(
        int securityId, long rptSeq, long price, int size, int orders, AggressorSide aggressor, long tradeId)
    {
    }

    public record SnapshotEntry(char entryType, int level, long price, int size, int orders)
    {
    }

    private final UnsafeBuffer buffer;
    private final MessageHeaderEncoder header = new MessageHeaderEncoder();
    private final MDIncrementalRefreshBook46Encoder book = new MDIncrementalRefreshBook46Encoder();
    private final MDIncrementalRefreshTradeSummary48Encoder trade = new MDIncrementalRefreshTradeSummary48Encoder();
    private final SnapshotFullRefresh52Encoder snapshot = new SnapshotFullRefresh52Encoder();
    private final SecurityStatus30Encoder status = new SecurityStatus30Encoder();
    private final ChannelReset4Encoder channelReset = new ChannelReset4Encoder();
    private final MDInstrumentDefinitionFuture54Encoder definition = new MDInstrumentDefinitionFuture54Encoder();
    private int limit;

    public CmePacketWriter(final int capacity)
    {
        buffer = new UnsafeBuffer(new byte[capacity]);
    }

    public CmePacketWriter begin(final long seq, final long sendingTimeNs)
    {
        buffer.putInt(CmePacketHeader.SEQ_OFFSET, (int)seq, ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(CmePacketHeader.SENDING_TIME_OFFSET, sendingTimeNs, ByteOrder.LITTLE_ENDIAN);
        limit = CmePacketHeader.LENGTH;
        return this;
    }

    public CmePacketWriter book(final long transactTime, final boolean endOfEvent, final BookEntry... entries)
    {
        final int start = limit;
        book.wrapAndApplyHeader(buffer, messageStart(), header)
            .transactTime(transactTime);
        book.matchEventIndicator().clear().endOfEvent(endOfEvent).lastQuoteMsg(endOfEvent);
        final MDIncrementalRefreshBook46Encoder.NoMDEntriesEncoder group = book.noMDEntriesCount(entries.length);
        for (final BookEntry e : entries)
        {
            group.next()
                .securityID(e.securityId())
                .rptSeq(e.rptSeq())
                .mDEntryType(MDEntryTypeBook.get((byte)e.entryType()))
                .mDUpdateAction(e.action())
                .mDPriceLevel((short)e.level())
                .mDEntrySize(e.size())
                .numberOfOrders(e.orders())
                .mDEntryPx().mantissa(e.price());
        }
        book.noOrderIDEntriesCount(0);
        return endMessage(start, book.encodedLength());
    }

    public CmePacketWriter trade(final long transactTime, final boolean endOfEvent, final TradeEntry... entries)
    {
        final int start = limit;
        trade.wrapAndApplyHeader(buffer, messageStart(), header)
            .transactTime(transactTime);
        trade.matchEventIndicator().clear().endOfEvent(endOfEvent).lastTradeMsg(endOfEvent);
        final MDIncrementalRefreshTradeSummary48Encoder.NoMDEntriesEncoder group = trade.noMDEntriesCount(entries.length);
        for (final TradeEntry e : entries)
        {
            group.next()
                .securityID(e.securityId())
                .rptSeq(e.rptSeq())
                .mDEntrySize(e.size())
                .numberOfOrders(e.orders())
                .aggressorSide(e.aggressor())
                .mDUpdateAction(MDUpdateAction.New)
                .mDTradeEntryID(e.tradeId())
                .mDEntryPx().mantissa(e.price());
        }
        trade.noOrderIDEntriesCount(0);
        return endMessage(start, trade.encodedLength());
    }

    public CmePacketWriter snapshot(
        final int securityId,
        final long rptSeq,
        final long lastMsgSeqNumProcessed,
        final long totNumReports,
        final long transactTime,
        final SnapshotEntry... entries)
    {
        final int start = limit;
        snapshot.wrapAndApplyHeader(buffer, messageStart(), header)
            .lastMsgSeqNumProcessed(lastMsgSeqNumProcessed)
            .totNumReports(totNumReports)
            .securityID(securityId)
            .rptSeq(rptSeq)
            .transactTime(transactTime)
            .lastUpdateTime(transactTime)
            .tradeDate(0)
            .mDSecurityTradingStatus(SecurityTradingStatus.ReadyToTrade);
        snapshot.highLimitPrice().mantissa(Long.MAX_VALUE);
        snapshot.lowLimitPrice().mantissa(Long.MAX_VALUE);
        snapshot.maxPriceVariation().mantissa(Long.MAX_VALUE);
        final SnapshotFullRefresh52Encoder.NoMDEntriesEncoder group = snapshot.noMDEntriesCount(entries.length);
        for (final SnapshotEntry e : entries)
        {
            group.next()
                .mDEntrySize(e.size())
                .numberOfOrders(e.orders())
                .mDPriceLevel((byte)e.level())
                .tradingReferenceDate(0xFFFF)
                .openCloseSettlFlag((short)255)
                .mDEntryType(MDEntryType.get((byte)e.entryType()))
                .mDEntryPx().mantissa(e.price());
            group.settlPriceType().clear();
        }
        return endMessage(start, snapshot.encodedLength());
    }

    public CmePacketWriter securityStatus(
        final int securityId, final SecurityTradingStatus tradingStatus, final long transactTime)
    {
        final int start = limit;
        status.wrapAndApplyHeader(buffer, messageStart(), header)
            .transactTime(transactTime)
            .securityGroup("GE")
            .asset("GE")
            .securityID(securityId)
            .tradeDate(0)
            .securityTradingStatus(tradingStatus)
            .haltReason(io.mdp.cme.sbe.HaltReason.GroupSchedule)
            .securityTradingEvent(io.mdp.cme.sbe.SecurityTradingEvent.NoEvent);
        status.matchEventIndicator().clear().endOfEvent(true);
        return endMessage(start, status.encodedLength());
    }

    public CmePacketWriter channelReset(final long transactTime)
    {
        final int start = limit;
        channelReset.wrapAndApplyHeader(buffer, messageStart(), header).transactTime(transactTime);
        channelReset.matchEventIndicator().clear().endOfEvent(true);
        channelReset.noMDEntriesCount(1).next().applID((short)1);
        return endMessage(start, channelReset.encodedLength());
    }

    public CmePacketWriter definition(
        final int securityId, final String symbol, final long totNumReports, final long minPriceIncrement)
    {
        final int start = limit;
        definition.wrapAndApplyHeader(buffer, messageStart(), header)
            .totNumReports(totNumReports)
            .securityUpdateAction(SecurityUpdateAction.Add)
            .lastUpdateTime(0)
            .mDSecurityTradingStatus(SecurityTradingStatus.ReadyToTrade)
            .applID((short)1)
            .marketSegmentID((short)1)
            .underlyingProduct((short)14)
            .securityExchange("XCME")
            .securityGroup("ES")
            .asset("ES")
            .symbol(symbol)
            .securityID(securityId)
            .securityType("FUT")
            .cFICode("FFIXSX")
            .currency("USD")
            .settlCurrency("USD")
            .matchAlgorithm((byte)'F')
            .minTradeVol(1)
            .maxTradeVol(10000);
        definition.matchEventIndicator().clear().endOfEvent(true);
        definition.maturityMonthYear().year(2026).month((short)12).day((short)255).week((short)255);
        definition.minPriceIncrement().mantissa(minPriceIncrement);
        definition.displayFactor().mantissa(1_000_000_000L);
        return endMessage(start, definition.encodedLength());
    }

    public DirectBuffer buffer()
    {
        return buffer;
    }

    public int length()
    {
        return limit;
    }

    public byte[] toBytes()
    {
        final byte[] bytes = new byte[limit];
        buffer.getBytes(0, bytes);
        return bytes;
    }

    private int messageStart()
    {
        return limit + CmePacketHeader.MESSAGE_SIZE_LENGTH;
    }

    private CmePacketWriter endMessage(final int start, final int bodyLength)
    {
        final int msgSize = CmePacketHeader.MESSAGE_SIZE_LENGTH + MessageHeaderEncoder.ENCODED_LENGTH + bodyLength;
        buffer.putShort(start, (short)msgSize, ByteOrder.LITTLE_ENDIAN);
        limit = start + msgSize;
        return this;
    }
}
