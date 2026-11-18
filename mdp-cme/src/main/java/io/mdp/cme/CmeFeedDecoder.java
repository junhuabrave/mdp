package io.mdp.cme;

import java.nio.ByteOrder;
import java.util.Arrays;

import io.mdp.cme.sbe.ChannelReset4Decoder;
import io.mdp.cme.sbe.MDIncrementalRefreshBook46Decoder;
import io.mdp.cme.sbe.MDIncrementalRefreshTradeSummary48Decoder;
import io.mdp.cme.sbe.MDInstrumentDefinitionFuture54Decoder;
import io.mdp.cme.sbe.MessageHeaderDecoder;
import io.mdp.cme.sbe.SecurityStatus30Decoder;
import io.mdp.cme.sbe.SnapshotFullRefresh52Decoder;
import io.mdp.core.api.FeedDecoder;
import io.mdp.core.metrics.MetricsRegistry;
import io.mdp.core.recovery.InstrumentStateTable;
import io.mdp.core.sink.FeedEvents;
import io.mdp.core.sink.NormalizedSink;
import io.mdp.sbe.AggressorSide;
import io.mdp.sbe.BookType;
import io.mdp.sbe.BookUpdateEncoder;
import io.mdp.sbe.FeedEventType;
import io.mdp.sbe.InstrumentStatusEncoder;
import io.mdp.sbe.Side;
import io.mdp.sbe.TradeEncoder;
import io.mdp.sbe.TradingStatus;
import io.mdp.sbe.UpdateAction;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.status.AtomicCounter;

/**
 * CME MDP 3.0 decoder with RptSeq-based recovery (DESIGN §6.3).
 * <ul>
 *   <li>Every instrument tracks {@code RptSeq}. An instrument with a jump goes stale; stale instruments
 *       ignore incrementals but remember the highest RptSeq seen.</li>
 *   <li>While anything is stale (or the channel has never synced) the snapshot feed is joined.
 *       {@code SnapshotFullRefresh52} recovers an instrument only if its RptSeq is at least the
 *       highest incremental RptSeq seen, so no update can fall between snapshot and live data.</li>
 *   <li>Packet gaps: {@code CHANNEL_STALE} marks every instrument stale (conservative, CME's
 *       recommendation); {@code INSTRUMENT_RPTSEQ} lets RptSeq find affected instruments.</li>
 * </ul>
 * Decoding is allocation free: flyweight decoders, raw enum access (an unknown enum value never throws).
 */
public final class CmeFeedDecoder implements FeedDecoder
{
    public enum GapPolicy
    {
        CHANNEL_STALE,
        INSTRUMENT_RPTSEQ
    }

    private static final int VENUE_ID = CmeMdp3Protocol.VENUE_ID;
    private static final byte PRICE_EXPONENT = -9;
    private static final int MATCH_EVENT_END_OF_EVENT = 0x80;
    private static final long NULL_PRICE = Long.MAX_VALUE;
    private static final int NULL_INT32 = Integer.MAX_VALUE;
    private static final BookType[] CLEARED_BOOK_TYPES = { BookType.MBP, BookType.IMPLIED };

    private final int channelId;
    private final NormalizedSink sink;
    private final GapPolicy gapPolicy;
    private final InstrumentStateTable states = new InstrumentStateTable(1024);

    private final MessageHeaderDecoder header = new MessageHeaderDecoder();
    private final MDIncrementalRefreshBook46Decoder book = new MDIncrementalRefreshBook46Decoder();
    private final MDIncrementalRefreshTradeSummary48Decoder trade = new MDIncrementalRefreshTradeSummary48Decoder();
    private final SecurityStatus30Decoder status = new SecurityStatus30Decoder();
    private final ChannelReset4Decoder channelReset = new ChannelReset4Decoder();
    private final SnapshotFullRefresh52Decoder snapshot = new SnapshotFullRefresh52Decoder();
    private final MDInstrumentDefinitionFuture54Decoder definition = new MDInstrumentDefinitionFuture54Decoder();
    private final byte[] symbolBytes = new byte[32];
    private final byte[] securityTypeBytes = new byte[8];
    private final byte[] currencyBytes = new byte[3];

    private final AtomicCounter decoded;
    private final AtomicCounter unknown;
    private final AtomicCounter malformed;
    private final AtomicCounter staleDropped;
    private final AtomicCounter instrumentGaps;
    private final AtomicCounter recoveries;
    private final AtomicCounter staleGauge;
    private final AtomicCounter channelState;
    private final AtomicCounter recoveryDurationMs;

    private boolean synced;
    private boolean recovering;
    private boolean recoveryAnnounced;
    private long recoveryStartNs;
    private boolean snapshotCycleStarted;
    private long snapshotCycleCount;
    private boolean definitionsComplete;
    private long definitionCount;

    public CmeFeedDecoder(
        final int channelId,
        final NormalizedSink sink,
        final MetricsRegistry metrics,
        final GapPolicy gapPolicy,
        final boolean startLive)
    {
        this.channelId = channelId;
        this.sink = sink;
        this.gapPolicy = gapPolicy;
        final String channel = String.valueOf(channelId);
        decoded = metrics.counter("mdp.messages.decoded", "channel", channel);
        unknown = metrics.counter("mdp.messages.unknown", "channel", channel);
        malformed = metrics.counter("mdp.packets.malformed", "channel", channel);
        staleDropped = metrics.counter("mdp.messages.stale_dropped", "channel", channel);
        instrumentGaps = metrics.counter("mdp.instrument.gaps", "channel", channel);
        recoveries = metrics.counter("mdp.recovery.count", "channel", channel);
        staleGauge = metrics.gauge("mdp.instruments.stale", "channel", channel);
        channelState = metrics.gauge("mdp.channel.state", "channel", channel);
        recoveryDurationMs = metrics.gauge("mdp.recovery.last.duration.ms", "channel", channel);

        synced = startLive;
        recovering = !startLive;
        definitionsComplete = startLive;
        channelState.setRelease(recovering ? 1 : 0);
    }

    // ------------------------------------------------------------------ incremental feed

    public void onPacket(
        final DirectBuffer buffer, final int offset, final int length, final long packetSeq, final long receiveTimeNs)
    {
        final int end = offset + length;
        int pos = offset + CmePacketHeader.LENGTH;
        while (pos + CmePacketHeader.MESSAGE_SIZE_LENGTH + MessageHeaderDecoder.ENCODED_LENGTH <= end)
        {
            final int msgSize = buffer.getShort(pos, ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
            if (msgSize < CmePacketHeader.MESSAGE_SIZE_LENGTH + MessageHeaderDecoder.ENCODED_LENGTH ||
                pos + msgSize > end)
            {
                MetricsRegistry.increment(malformed);
                break;
            }

            final int headerOffset = pos + CmePacketHeader.MESSAGE_SIZE_LENGTH;
            header.wrap(buffer, headerOffset);
            final int bodyOffset = headerOffset + MessageHeaderDecoder.ENCODED_LENGTH;
            final int blockLength = header.blockLength();
            final int version = header.version();

            if (header.schemaId() != MessageHeaderDecoder.SCHEMA_ID)
            {
                MetricsRegistry.increment(unknown);
            }
            else
            {
                switch (header.templateId())
                {
                    case MDIncrementalRefreshBook46Decoder.TEMPLATE_ID ->
                        onBook(book.wrap(buffer, bodyOffset, blockLength, version), packetSeq, receiveTimeNs);
                    case MDIncrementalRefreshTradeSummary48Decoder.TEMPLATE_ID ->
                        onTrade(trade.wrap(buffer, bodyOffset, blockLength, version), packetSeq, receiveTimeNs);
                    case SecurityStatus30Decoder.TEMPLATE_ID ->
                        onStatus(status.wrap(buffer, bodyOffset, blockLength, version), packetSeq, receiveTimeNs);
                    case ChannelReset4Decoder.TEMPLATE_ID ->
                        onChannelReset(channelReset.wrap(buffer, bodyOffset, blockLength, version), receiveTimeNs);
                    case MDInstrumentDefinitionFuture54Decoder.TEMPLATE_ID ->
                        onDefinition(definition.wrap(buffer, bodyOffset, blockLength, version), receiveTimeNs);
                    default -> MetricsRegistry.increment(unknown);
                }
                MetricsRegistry.increment(decoded);
            }
            pos += msgSize;
        }
    }

    private void onBook(final MDIncrementalRefreshBook46Decoder msg, final long packetSeq, final long receiveTimeNs)
    {
        final long transactTime = msg.transactTime();
        final boolean endOfEvent = (msg.buffer().getByte(
            msg.offset() + MDIncrementalRefreshBook46Decoder.matchEventIndicatorEncodingOffset()) &
            MATCH_EVENT_END_OF_EVENT) != 0;

        final MDIncrementalRefreshBook46Decoder.NoMDEntriesDecoder entries = msg.noMDEntries();
        int remaining = entries.count();
        while (entries.hasNext())
        {
            entries.next();
            remaining--;
            final int securityId = entries.securityID();
            final long rptSeq = entries.rptSeq();
            final InstrumentStateTable.State state = states.getOrCreate(securityId, !synced);
            if (!checkSequence(state, rptSeq, receiveTimeNs))
            {
                continue;
            }

            final byte entryType = entries.mDEntryTypeRaw();
            final BookType bookType;
            final Side side;
            switch (entryType)
            {
                case '0' -> { bookType = BookType.MBP; side = Side.BUY; }
                case '1' -> { bookType = BookType.MBP; side = Side.SELL; }
                case 'E' -> { bookType = BookType.IMPLIED; side = Side.BUY; }
                case 'F' -> { bookType = BookType.IMPLIED; side = Side.SELL; }
                case 'J' -> { bookType = BookType.MBP; side = Side.BUY; }
                default ->
                {
                    MetricsRegistry.increment(unknown);
                    continue;
                }
            }
            final UpdateAction action = entryType == 'J' ? UpdateAction.CLEAR : updateAction(entries.mDUpdateActionRaw());
            if (null == action)
            {
                MetricsRegistry.increment(unknown);
                continue;
            }

            final long price = entries.mDEntryPx().mantissa();
            final int size = entries.mDEntrySize();
            final int orders = entries.numberOfOrders();
            final byte flags = (byte)((endOfEvent && remaining == 0 ? NormalizedSink.Flags.LAST_IN_EVENT : 0) |
                (bookType == BookType.IMPLIED ? NormalizedSink.Flags.IMPLIED : 0));

            final BookUpdateEncoder out = sink.bookUpdate()
                .venueId(VENUE_ID)
                .bookType(bookType)
                .side(side)
                .action(action)
                .level(entries.mDPriceLevel())
                .priceExponent(PRICE_EXPONENT)
                .channelId(channelId)
                .instrumentId(securityId)
                .priceMantissa(price == NULL_PRICE ? 0 : price)
                .size(size == NULL_INT32 ? 0 : size)
                .numOrders(orders == NULL_INT32 ? 0 : orders)
                .orderId(0)
                .instrumentSeq(rptSeq)
                .packetSeq(packetSeq)
                .exchangeTimeNs(transactTime)
                .receiveTimeNs(receiveTimeNs);
            setFlags(out.buffer(), out.offset() + BookUpdateEncoder.flagsEncodingOffset(), flags);
            sink.commit();
        }
    }

    private void onTrade(
        final MDIncrementalRefreshTradeSummary48Decoder msg, final long packetSeq, final long receiveTimeNs)
    {
        final long transactTime = msg.transactTime();
        final boolean endOfEvent = (msg.buffer().getByte(
            msg.offset() + MDIncrementalRefreshTradeSummary48Decoder.matchEventIndicatorEncodingOffset()) &
            MATCH_EVENT_END_OF_EVENT) != 0;

        final MDIncrementalRefreshTradeSummary48Decoder.NoMDEntriesDecoder entries = msg.noMDEntries();
        int remaining = entries.count();
        while (entries.hasNext())
        {
            entries.next();
            remaining--;
            final int securityId = entries.securityID();
            final long rptSeq = entries.rptSeq();
            final InstrumentStateTable.State state = states.getOrCreate(securityId, !synced);
            if (!checkSequence(state, rptSeq, receiveTimeNs))
            {
                continue;
            }

            final AggressorSide aggressor = switch (entries.aggressorSideRaw())
            {
                case 1 -> AggressorSide.BUY;
                case 2 -> AggressorSide.SELL;
                default -> AggressorSide.NONE;
            };
            final long tradeId = entries.mDTradeEntryID();
            final TradeEncoder out = sink.trade()
                .venueId(VENUE_ID)
                .aggressorSide(aggressor)
                .priceExponent(PRICE_EXPONENT)
                .channelId(channelId)
                .instrumentId(securityId)
                .priceMantissa(entries.mDEntryPx().mantissa())
                .size(entries.mDEntrySize())
                .numOrders(entries.numberOfOrders())
                .tradeId(tradeId == MDIncrementalRefreshTradeSummary48Decoder.NoMDEntriesDecoder.mDTradeEntryIDNullValue() ?
                    0 : tradeId)
                .instrumentSeq(rptSeq)
                .packetSeq(packetSeq)
                .exchangeTimeNs(transactTime)
                .receiveTimeNs(receiveTimeNs);
            setFlags(out.buffer(), out.offset() + TradeEncoder.flagsEncodingOffset(),
                endOfEvent && remaining == 0 ? NormalizedSink.Flags.LAST_IN_EVENT : NormalizedSink.Flags.NONE);
            sink.commit();
        }
    }

    private void onStatus(final SecurityStatus30Decoder msg, final long packetSeq, final long receiveTimeNs)
    {
        final TradingStatus tradingStatus = switch (msg.securityTradingStatusRaw())
        {
            case 2, 18 -> TradingStatus.HALT;
            case 4 -> TradingStatus.CLOSE;
            case 17 -> TradingStatus.OPEN;
            case 15, 21, 24, 25 -> TradingStatus.PRE_OPEN;
            case 26 -> TradingStatus.POST_CLOSE;
            case 103 -> null;
            default -> TradingStatus.UNKNOWN;
        };
        if (null == tradingStatus)
        {
            return;
        }
        final int securityId = msg.securityID();
        final InstrumentStatusEncoder out = sink.instrumentStatus()
            .venueId(VENUE_ID)
            .status(tradingStatus)
            .channelId(channelId)
            .instrumentId(securityId == SecurityStatus30Decoder.securityIDNullValue() ? 0 : securityId)
            .packetSeq(packetSeq)
            .exchangeTimeNs(msg.transactTime())
            .receiveTimeNs(receiveTimeNs);
        setFlags(out.buffer(), out.offset() + InstrumentStatusEncoder.flagsEncodingOffset(),
            NormalizedSink.Flags.LAST_IN_EVENT);
        sink.commit();
    }

    private void onChannelReset(final ChannelReset4Decoder msg, final long receiveTimeNs)
    {
        // All books on the channel are cleared and every instrument restarts its RptSeq baseline.
        states.resetAll();
        synced = true;
        FeedEvents.emit(sink, VENUE_ID, channelId, FeedEventType.CHANNEL_RESET, 0, 0, 0, receiveTimeNs);
        completeRecoveryIfDone(receiveTimeNs);
    }

    private void onDefinition(final MDInstrumentDefinitionFuture54Decoder msg, final long receiveTimeNs)
    {
        Arrays.fill(symbolBytes, (byte)0);
        Arrays.fill(securityTypeBytes, (byte)0);
        msg.getSymbol(symbolBytes, 0);
        msg.getSecurityType(securityTypeBytes, 0);
        msg.getCurrency(currencyBytes, 0);
        sink.instrumentDefinition()
            .venueId(VENUE_ID)
            .priceExponent(PRICE_EXPONENT)
            .channelId(channelId)
            .instrumentId(msg.securityID())
            .minPriceIncrement(msg.minPriceIncrement().mantissa())
            .displayFactor(msg.displayFactor().mantissa())
            .putSymbol(symbolBytes, 0)
            .putSecurityType(securityTypeBytes, 0)
            .putCurrency(currencyBytes, 0)
            .exchangeTimeNs(msg.lastUpdateTime())
            .receiveTimeNs(receiveTimeNs);
        sink.commit();

        final long total = msg.totNumReports();
        if (!definitionsComplete && total != MDInstrumentDefinitionFuture54Decoder.totNumReportsNullValue() &&
            ++definitionCount >= total)
        {
            definitionsComplete = true;
        }
    }

    /**
     * @return true if the entry should be applied
     */
    private boolean checkSequence(final InstrumentStateTable.State state, final long rptSeq, final long timeNs)
    {
        switch (states.onIncremental(state, rptSeq))
        {
            case APPLY:
                return true;
            case GAP:
                MetricsRegistry.increment(instrumentGaps);
                FeedEvents.emit(sink, VENUE_ID, channelId, FeedEventType.BOOK_STALE, state.instrumentId(),
                    state.expectedSeq(), rptSeq, timeNs);
                startRecovery(timeNs);
                MetricsRegistry.increment(staleDropped);
                return false;
            case STALE:
                MetricsRegistry.increment(staleDropped);
                return false;
            default:
                return false;
        }
    }

    // ------------------------------------------------------------------ gaps and recovery

    public void onPacketGap(final long fromSeq, final long toSeq, final long timeNs)
    {
        FeedEvents.emit(sink, VENUE_ID, channelId, FeedEventType.GAP_DETECTED, 0, fromSeq, toSeq, timeNs);
        if (gapPolicy == GapPolicy.CHANNEL_STALE)
        {
            markChannelStale(timeNs);
        }
    }

    public void onSequenceReset(final long newSeq, final long timeNs)
    {
        FeedEvents.emit(sink, VENUE_ID, channelId, FeedEventType.SEQUENCE_RESET, 0, newSeq, newSeq, timeNs);
        markChannelStale(timeNs);
    }

    public void onSinkDrop(final long timeNs)
    {
        // We cannot know which instrument lost an event, so treat it like a packet gap.
        markChannelStale(timeNs);
    }

    private void markChannelStale(final long timeNs)
    {
        states.markAllStale();
        FeedEvents.emit(sink, VENUE_ID, channelId, FeedEventType.BOOK_STALE, 0, 0, 0, timeNs);
        recoveryAnnounced = false;
        startRecovery(timeNs);
    }

    private void startRecovery(final long timeNs)
    {
        staleGauge.setRelease(states.staleCount());
        if (!recovering)
        {
            recovering = true;
            recoveryAnnounced = false;
        }
        if (!recoveryAnnounced)
        {
            recoveryAnnounced = true;
            recoveryStartNs = timeNs;
            channelState.setRelease(1);
            FeedEvents.emit(sink, VENUE_ID, channelId, FeedEventType.RECOVERY_STARTED, 0, 0, 0, timeNs);
        }
    }

    public boolean wantsSnapshot()
    {
        return !synced || states.staleCount() > 0;
    }

    public boolean wantsDefinitions()
    {
        return !definitionsComplete;
    }

    public void onSnapshotStart()
    {
        snapshotCycleStarted = false;
        snapshotCycleCount = 0;
        if (recovering && !recoveryAnnounced)
        {
            // first activation at startup: tell consumers everything is stale until recovered
            FeedEvents.emit(sink, VENUE_ID, channelId, FeedEventType.BOOK_STALE, 0, 0, 0, 0);
            startRecovery(0);
        }
    }

    public void onSnapshotGap(final long fromSeq, final long toSeq)
    {
        // A missing snapshot packet means this loop cannot prove completeness; wait for the next one.
        snapshotCycleStarted = false;
    }

    public void onSnapshotPacket(
        final DirectBuffer buffer, final int offset, final int length, final long packetSeq, final long receiveTimeNs)
    {
        if (packetSeq == 1)
        {
            snapshotCycleStarted = true;
            snapshotCycleCount = 0;
        }

        final int end = offset + length;
        int pos = offset + CmePacketHeader.LENGTH;
        while (pos + CmePacketHeader.MESSAGE_SIZE_LENGTH + MessageHeaderDecoder.ENCODED_LENGTH <= end)
        {
            final int msgSize = buffer.getShort(pos, ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
            if (msgSize < CmePacketHeader.MESSAGE_SIZE_LENGTH + MessageHeaderDecoder.ENCODED_LENGTH ||
                pos + msgSize > end)
            {
                MetricsRegistry.increment(malformed);
                break;
            }
            final int headerOffset = pos + CmePacketHeader.MESSAGE_SIZE_LENGTH;
            header.wrap(buffer, headerOffset);
            final int bodyOffset = headerOffset + MessageHeaderDecoder.ENCODED_LENGTH;
            if (header.schemaId() == MessageHeaderDecoder.SCHEMA_ID)
            {
                switch (header.templateId())
                {
                    case SnapshotFullRefresh52Decoder.TEMPLATE_ID -> onSnapshot(
                        snapshot.wrap(buffer, bodyOffset, header.blockLength(), header.version()),
                        packetSeq, receiveTimeNs);
                    case MDInstrumentDefinitionFuture54Decoder.TEMPLATE_ID -> onDefinition(
                        definition.wrap(buffer, bodyOffset, header.blockLength(), header.version()), receiveTimeNs);
                    default ->
                    {
                    }
                }
            }
            pos += msgSize;
        }
    }

    public void onDefinitionPacket(
        final DirectBuffer buffer, final int offset, final int length, final long packetSeq, final long receiveTimeNs)
    {
        onSnapshotPacket(buffer, offset, length, packetSeq == 1 ? 0 : packetSeq, receiveTimeNs);
    }

    private void onSnapshot(final SnapshotFullRefresh52Decoder msg, final long packetSeq, final long receiveTimeNs)
    {
        final int securityId = msg.securityID();
        final long rptSeq = msg.rptSeq();
        final InstrumentStateTable.State state = states.getOrCreate(securityId, true);

        if (states.tryRecover(state, rptSeq))
        {
            final long transactTime = msg.transactTime();
            emitClear(securityId, rptSeq, packetSeq, transactTime, receiveTimeNs);

            final SnapshotFullRefresh52Decoder.NoMDEntriesDecoder entries = msg.noMDEntries();
            while (entries.hasNext())
            {
                entries.next();
                final BookType bookType;
                final Side side;
                switch (entries.mDEntryTypeRaw())
                {
                    case '0' -> { bookType = BookType.MBP; side = Side.BUY; }
                    case '1' -> { bookType = BookType.MBP; side = Side.SELL; }
                    case 'E' -> { bookType = BookType.IMPLIED; side = Side.BUY; }
                    case 'F' -> { bookType = BookType.IMPLIED; side = Side.SELL; }
                    default ->
                    {
                        continue; // statistics entries (open, settle, volume...) are not book state
                    }
                }
                final long price = entries.mDEntryPx().mantissa();
                final int size = entries.mDEntrySize();
                final int orders = entries.numberOfOrders();
                final BookUpdateEncoder out = sink.bookUpdate()
                    .venueId(VENUE_ID)
                    .bookType(bookType)
                    .side(side)
                    .action(UpdateAction.NEW)
                    .level((short)entries.mDPriceLevel())
                    .priceExponent(PRICE_EXPONENT)
                    .channelId(channelId)
                    .instrumentId(securityId)
                    .priceMantissa(price == NULL_PRICE ? 0 : price)
                    .size(size == NULL_INT32 ? 0 : size)
                    .numOrders(orders == NULL_INT32 ? 0 : orders)
                    .orderId(0)
                    .instrumentSeq(rptSeq)
                    .packetSeq(packetSeq)
                    .exchangeTimeNs(transactTime)
                    .receiveTimeNs(receiveTimeNs);
                setFlags(out.buffer(), out.offset() + BookUpdateEncoder.flagsEncodingOffset(),
                    (byte)(NormalizedSink.Flags.SNAPSHOT | (bookType == BookType.IMPLIED ? NormalizedSink.Flags.IMPLIED : 0)));
                sink.commit();
            }
            MetricsRegistry.increment(recoveries);
            FeedEvents.emit(sink, VENUE_ID, channelId, FeedEventType.RECOVERY_COMPLETE, securityId,
                rptSeq, rptSeq, receiveTimeNs);
            staleGauge.setRelease(states.staleCount());
        }

        if (snapshotCycleStarted)
        {
            snapshotCycleCount++;
            if (snapshotCycleCount >= msg.totNumReports())
            {
                synced = true;
            }
        }
        completeRecoveryIfDone(receiveTimeNs);
    }

    private void emitClear(
        final int securityId, final long rptSeq, final long packetSeq, final long transactTime, final long receiveTimeNs)
    {
        for (final BookType bookType : CLEARED_BOOK_TYPES)
        {
            final BookUpdateEncoder out = sink.bookUpdate()
                .venueId(VENUE_ID)
                .bookType(bookType)
                .side(Side.BUY)
                .action(UpdateAction.CLEAR)
                .level((short)0)
                .priceExponent(PRICE_EXPONENT)
                .channelId(channelId)
                .instrumentId(securityId)
                .priceMantissa(0)
                .size(0)
                .numOrders(0)
                .orderId(0)
                .instrumentSeq(rptSeq)
                .packetSeq(packetSeq)
                .exchangeTimeNs(transactTime)
                .receiveTimeNs(receiveTimeNs);
            setFlags(out.buffer(), out.offset() + BookUpdateEncoder.flagsEncodingOffset(), NormalizedSink.Flags.SNAPSHOT);
            sink.commit();
        }
    }

    private void completeRecoveryIfDone(final long timeNs)
    {
        if (recovering && synced && states.staleCount() == 0)
        {
            recovering = false;
            recoveryAnnounced = false;
            channelState.setRelease(0);
            staleGauge.setRelease(0);
            if (recoveryStartNs > 0 && timeNs > recoveryStartNs)
            {
                recoveryDurationMs.setRelease((timeNs - recoveryStartNs) / 1_000_000);
            }
            FeedEvents.emit(sink, VENUE_ID, channelId, FeedEventType.RECOVERY_COMPLETE, 0, 0, 0, timeNs);
        }
    }

    public boolean isRecovering()
    {
        return recovering;
    }

    public InstrumentStateTable states()
    {
        return states;
    }

    private static UpdateAction updateAction(final short raw)
    {
        return switch (raw)
        {
            case 0 -> UpdateAction.NEW;
            case 1 -> UpdateAction.CHANGE;
            case 2 -> UpdateAction.DELETE;
            case 3 -> UpdateAction.DELETE_THRU;
            case 4 -> UpdateAction.DELETE_FROM;
            case 5 -> UpdateAction.OVERLAY;
            default -> null;
        };
    }

    private static void setFlags(final org.agrona.MutableDirectBuffer buffer, final int index, final byte flags)
    {
        buffer.putByte(index, flags);
    }
}
