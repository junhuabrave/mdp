package io.mdp.euronext;

import java.util.Arrays;

import io.mdp.core.api.FeedDecoder;
import io.mdp.core.arbitration.SequencedPacketHandler;
import io.mdp.core.metrics.MetricsRegistry;
import io.mdp.core.recovery.PacketReplayBuffer;
import io.mdp.core.sink.FeedEvents;
import io.mdp.core.sink.NormalizedSink;
import io.mdp.euronext.sbe.EndOfSnapshotDecoder;
import io.mdp.euronext.sbe.FullTradeInformationDecoder;
import io.mdp.euronext.sbe.MarketStatusChangeDecoder;
import io.mdp.euronext.sbe.MarketUpdateDecoder;
import io.mdp.euronext.sbe.MessageHeaderDecoder;
import io.mdp.euronext.sbe.OrderUpdateDecoder;
import io.mdp.euronext.sbe.StandingDataDecoder;
import io.mdp.euronext.sbe.StartOfSnapshotDecoder;
import io.mdp.sbe.AggressorSide;
import io.mdp.sbe.BookType;
import io.mdp.sbe.BookUpdateEncoder;
import io.mdp.sbe.FeedEventType;
import io.mdp.sbe.Side;
import io.mdp.sbe.TradeEncoder;
import io.mdp.sbe.TradingStatus;
import io.mdp.sbe.UpdateAction;
import org.agrona.DirectBuffer;
import org.agrona.collections.Int2IntHashMap;
import org.agrona.collections.IntHashSet;
import org.agrona.concurrent.status.AtomicCounter;

/**
 * Euronext Optiq MDG decoder with snapshot plus buffered-replay recovery (DESIGN §6.3).
 * <ul>
 *   <li>Optiq has no per-instrument sequence in this model, so any packet gap makes the whole channel stale.</li>
 *   <li>While recovering, incremental packets are copied into a {@link PacketReplayBuffer}. The snapshot
 *       cycle ({@code StartOfSnapshot} ... {@code EndOfSnapshot(L)}) is applied, then buffered packets
 *       with sequence above L are replayed and the channel goes live.</li>
 *   <li>A cycle is accepted only if L covers the gap and the buffer has every packet from L+1 on;
 *       otherwise the decoder waits for the next cycle.</li>
 *   <li>Message length comes from the schema (block length and group dimensions). An unknown template
 *       stops processing the rest of the packet (review R2-6).</li>
 * </ul>
 */
public final class OptiqFeedDecoder implements FeedDecoder
{
    private static final int VENUE_ID = OptiqProtocol.VENUE_ID;
    private static final long NULL_PRICE = Long.MIN_VALUE;
    private static final long NULL_QTY = -1L; // uint64 max value seen as a signed long

    private final int channelId;
    private final NormalizedSink sink;
    private final int defaultPriceDecimals;
    private final Int2IntHashMap priceDecimals = new Int2IntHashMap(-1);
    private final IntHashSet clearedInCycle = new IntHashSet();
    private final PacketReplayBuffer replayBuffer;
    private final SequencedPacketHandler replayHandler;

    private final MessageHeaderDecoder header = new MessageHeaderDecoder();
    private final MarketUpdateDecoder marketUpdate = new MarketUpdateDecoder();
    private final OrderUpdateDecoder orderUpdate = new OrderUpdateDecoder();
    private final FullTradeInformationDecoder trade = new FullTradeInformationDecoder();
    private final MarketStatusChangeDecoder statusChange = new MarketStatusChangeDecoder();
    private final StandingDataDecoder standingData = new StandingDataDecoder();
    private final StartOfSnapshotDecoder startOfSnapshot = new StartOfSnapshotDecoder();
    private final EndOfSnapshotDecoder endOfSnapshot = new EndOfSnapshotDecoder();
    private final byte[] symbolBytes = new byte[32];
    private final byte[] currencyBytes = new byte[3];

    private final AtomicCounter decoded;
    private final AtomicCounter unknown;
    private final AtomicCounter recoveries;
    private final AtomicCounter rejectedCycles;
    private final AtomicCounter channelState;
    private final AtomicCounter replayBuffered;
    private final AtomicCounter recoveryDurationMs;

    private boolean recovering;
    private boolean recoveryAnnounced;
    private long recoveryStartNs;
    private long requiredFrom;
    private long lastIncrementalSeq;
    private boolean cycleActive;

    public OptiqFeedDecoder(
        final int channelId,
        final NormalizedSink sink,
        final MetricsRegistry metrics,
        final int defaultPriceDecimals,
        final int replayBufferPackets,
        final boolean startLive)
    {
        this.channelId = channelId;
        this.sink = sink;
        this.defaultPriceDecimals = defaultPriceDecimals;
        this.replayBuffer = new PacketReplayBuffer(replayBufferPackets, 1500);
        this.replayHandler = new SequencedPacketHandler()
        {
            public void onPacket(
                final DirectBuffer buffer, final int offset, final int length, final long seq, final long rt)
            {
                decodePacket(buffer, offset, length, seq, rt, false);
            }

            public void onGap(final long fromSeq, final long toSeq)
            {
            }

            public void onSequenceReset(final long newSeq)
            {
            }

            public void onLineStatus(final int line, final boolean up)
            {
            }
        };

        final String channel = String.valueOf(channelId);
        decoded = metrics.counter("mdp.messages.decoded", "channel", channel);
        unknown = metrics.counter("mdp.messages.unknown", "channel", channel);
        recoveries = metrics.counter("mdp.recovery.count", "channel", channel);
        rejectedCycles = metrics.counter("mdp.recovery.rejected_cycles", "channel", channel);
        channelState = metrics.gauge("mdp.channel.state", "channel", channel);
        replayBuffered = metrics.gauge("mdp.recovery.replay.buffered", "channel", channel);
        recoveryDurationMs = metrics.gauge("mdp.recovery.last.duration.ms", "channel", channel);

        recovering = !startLive;
        channelState.setRelease(recovering ? 1 : 0);
    }

    // ------------------------------------------------------------------ incremental

    public void onPacket(
        final DirectBuffer buffer, final int offset, final int length, final long packetSeq, final long receiveTimeNs)
    {
        lastIncrementalSeq = packetSeq;
        if (recovering)
        {
            replayBuffer.add(buffer, offset, length, packetSeq, receiveTimeNs);
            replayBuffered.setRelease(replayBuffer.size());
            return;
        }
        decodePacket(buffer, offset, length, packetSeq, receiveTimeNs, false);
    }

    public void onPacketGap(final long fromSeq, final long toSeq, final long timeNs)
    {
        FeedEvents.emit(sink, VENUE_ID, channelId, FeedEventType.GAP_DETECTED, 0, fromSeq, toSeq, timeNs);
        enterRecovery(toSeq, timeNs);
    }

    public void onSequenceReset(final long newSeq, final long timeNs)
    {
        FeedEvents.emit(sink, VENUE_ID, channelId, FeedEventType.SEQUENCE_RESET, 0, newSeq, newSeq, timeNs);
        requiredFrom = 0;
        enterRecovery(newSeq - 1, timeNs);
    }

    public void onSinkDrop(final long timeNs)
    {
        enterRecovery(lastIncrementalSeq, timeNs);
    }

    private void enterRecovery(final long coverUpTo, final long timeNs)
    {
        replayBuffer.clear();
        requiredFrom = Math.max(requiredFrom, coverUpTo);
        if (!recovering)
        {
            recovering = true;
            replayBuffer.resetDropped();
            recoveryAnnounced = false;
        }
        announceRecovery(timeNs);
    }

    private void announceRecovery(final long timeNs)
    {
        if (!recoveryAnnounced)
        {
            recoveryAnnounced = true;
            recoveryStartNs = timeNs;
            channelState.setRelease(1);
            FeedEvents.emit(sink, VENUE_ID, channelId, FeedEventType.BOOK_STALE, 0, 0, 0, timeNs);
            FeedEvents.emit(sink, VENUE_ID, channelId, FeedEventType.RECOVERY_STARTED, 0, 0, 0, timeNs);
        }
    }

    // ------------------------------------------------------------------ snapshot

    public boolean wantsSnapshot()
    {
        return recovering;
    }

    public void onSnapshotStart()
    {
        cycleActive = false;
        announceRecovery(0);
    }

    public void onSnapshotGap(final long fromSeq, final long toSeq)
    {
        cycleActive = false;
    }

    public void onSnapshotPacket(
        final DirectBuffer buffer, final int offset, final int length, final long packetSeq, final long receiveTimeNs)
    {
        decodePacket(buffer, offset, length, packetSeq, receiveTimeNs, true);
    }

    private void onStartOfSnapshot()
    {
        cycleActive = true;
        clearedInCycle.clear();
    }

    private void onEndOfSnapshot(final long lastSeq, final long receiveTimeNs)
    {
        if (!cycleActive || !recovering)
        {
            return;
        }
        cycleActive = false;

        final boolean coversGap = lastSeq >= requiredFrom;
        final boolean noEvictionPastL = replayBuffer.droppedUpTo() <= lastSeq;
        final boolean bufferContiguousFromL = replayBuffer.isEmpty() || replayBuffer.firstSeq() <= lastSeq + 1;
        if (!(coversGap && noEvictionPastL && bufferContiguousFromL))
        {
            MetricsRegistry.increment(rejectedCycles);
            return;
        }

        recovering = false;
        recoveryAnnounced = false;
        replayBuffer.replay(lastSeq, replayHandler);
        replayBuffered.setRelease(0);
        requiredFrom = 0;
        channelState.setRelease(0);
        MetricsRegistry.increment(recoveries);
        if (recoveryStartNs > 0 && receiveTimeNs > recoveryStartNs)
        {
            recoveryDurationMs.setRelease((receiveTimeNs - recoveryStartNs) / 1_000_000);
        }
        FeedEvents.emit(sink, VENUE_ID, channelId, FeedEventType.RECOVERY_COMPLETE, 0, lastSeq, lastSeq, receiveTimeNs);
    }

    // ------------------------------------------------------------------ decoding

    private void decodePacket(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final long packetSeq,
        final long receiveTimeNs,
        final boolean snapshotFeed)
    {
        final int end = offset + length;
        int pos = offset + OptiqPacketHeader.LENGTH;
        while (pos + MessageHeaderDecoder.ENCODED_LENGTH <= end)
        {
            header.wrap(buffer, pos);
            final int body = pos + MessageHeaderDecoder.ENCODED_LENGTH;
            final int blockLength = header.blockLength();
            final int version = header.version();
            final int messageLength;
            if (header.schemaId() != MessageHeaderDecoder.SCHEMA_ID)
            {
                MetricsRegistry.increment(unknown);
                return;
            }

            switch (header.templateId())
            {
                case MarketUpdateDecoder.TEMPLATE_ID ->
                {
                    marketUpdate.wrap(buffer, body, blockLength, version);
                    messageLength = marketUpdate.sbeDecodedLength();
                    if (body + messageLength <= end)
                    {
                        onMarketUpdate(marketUpdate, packetSeq, receiveTimeNs, snapshotFeed);
                    }
                }
                case OrderUpdateDecoder.TEMPLATE_ID ->
                {
                    orderUpdate.wrap(buffer, body, blockLength, version);
                    messageLength = blockLength;
                    onOrderUpdate(orderUpdate, packetSeq, receiveTimeNs, snapshotFeed);
                }
                case FullTradeInformationDecoder.TEMPLATE_ID ->
                {
                    messageLength = blockLength;
                    if (!snapshotFeed)
                    {
                        onTrade(trade.wrap(buffer, body, blockLength, version), packetSeq, receiveTimeNs);
                    }
                }
                case MarketStatusChangeDecoder.TEMPLATE_ID ->
                {
                    messageLength = blockLength;
                    onStatus(statusChange.wrap(buffer, body, blockLength, version), packetSeq, receiveTimeNs);
                }
                case StandingDataDecoder.TEMPLATE_ID ->
                {
                    messageLength = blockLength;
                    onStandingData(standingData.wrap(buffer, body, blockLength, version), receiveTimeNs);
                }
                case StartOfSnapshotDecoder.TEMPLATE_ID ->
                {
                    messageLength = blockLength;
                    if (snapshotFeed)
                    {
                        onStartOfSnapshot();
                    }
                }
                case EndOfSnapshotDecoder.TEMPLATE_ID ->
                {
                    messageLength = blockLength;
                    if (snapshotFeed)
                    {
                        onEndOfSnapshot(endOfSnapshot.wrap(buffer, body, blockLength, version)
                            .lastPacketSequenceNumber(), receiveTimeNs);
                    }
                }
                default ->
                {
                    MetricsRegistry.increment(unknown);
                    return; // cannot size an unknown message: abandon the rest of the packet
                }
            }
            MetricsRegistry.increment(decoded);
            pos = body + messageLength;
        }
    }

    private void onMarketUpdate(
        final MarketUpdateDecoder msg, final long packetSeq, final long receiveTimeNs, final boolean snapshotFeed)
    {
        final int symbol = (int)msg.symbolIndex();
        final long eventTime = msg.eventTime();
        final byte exponent = exponent(symbol);
        if (snapshotFeed)
        {
            if (!cycleActive)
            {
                return;
            }
            if (clearedInCycle.add(symbol))
            {
                emitBook(symbol, BookType.MBP, Side.BUY, UpdateAction.CLEAR, 0, 0, 0, 0, 0, exponent,
                    packetSeq, eventTime, receiveTimeNs, NormalizedSink.Flags.SNAPSHOT);
            }
        }

        final MarketUpdateDecoder.MarketDataUpdatesDecoder updates = msg.marketDataUpdates();
        int remaining = updates.count();
        while (updates.hasNext())
        {
            updates.next();
            remaining--;
            final short type = updates.updateTypeRaw();
            final byte flags = (byte)((snapshotFeed ? NormalizedSink.Flags.SNAPSHOT : 0) |
                (remaining == 0 ? NormalizedSink.Flags.LAST_IN_EVENT : 0));
            if (type == 6)
            {
                emitBook(symbol, BookType.MBP, Side.BUY, UpdateAction.CLEAR, 0, 0, 0, 0, 0, exponent,
                    packetSeq, eventTime, receiveTimeNs, flags);
                continue;
            }
            if (type != 1 && type != 2)
            {
                continue; // statistics (last, open, close) are not book state
            }
            final long qty = updates.quantity();
            final boolean delete = qty == 0 || qty == NULL_QTY;
            final long price = updates.price();
            emitBook(symbol, BookType.MBP, type == 1 ? Side.BUY : Side.SELL,
                delete ? UpdateAction.DELETE : UpdateAction.OVERLAY, updates.level(),
                price == NULL_PRICE ? 0 : price, delete ? 0 : qty, (int)updates.orderCount(), 0, exponent,
                packetSeq, eventTime, receiveTimeNs, flags);
        }
    }

    private void onOrderUpdate(
        final OrderUpdateDecoder msg, final long packetSeq, final long receiveTimeNs, final boolean snapshotFeed)
    {
        if (snapshotFeed && !cycleActive)
        {
            return;
        }
        final UpdateAction action = switch (msg.actionTypeRaw())
        {
            case 1, 5 -> UpdateAction.NEW;
            case 2 -> UpdateAction.CHANGE;
            case 3 -> UpdateAction.DELETE;
            case 4 -> UpdateAction.CLEAR;
            default -> null;
        };
        if (null == action)
        {
            MetricsRegistry.increment(unknown);
            return;
        }
        final int symbol = (int)msg.symbolIndex();
        final long price = msg.orderPx();
        final long qty = msg.orderQuantity();
        emitBook(symbol, BookType.MBO, msg.orderSideRaw() == 1 ? Side.BUY : Side.SELL, action, 0,
            price == NULL_PRICE ? 0 : price, qty == NULL_QTY ? 0 : qty, 1, msg.orderPriority(), exponent(symbol),
            packetSeq, msg.eventTime(), receiveTimeNs,
            (byte)(NormalizedSink.Flags.LAST_IN_EVENT | (snapshotFeed ? NormalizedSink.Flags.SNAPSHOT : 0)));
    }

    private void onTrade(final FullTradeInformationDecoder msg, final long packetSeq, final long receiveTimeNs)
    {
        final int symbol = (int)msg.symbolIndex();
        final AggressorSide aggressor = switch (msg.aggressorSideRaw())
        {
            case 1 -> AggressorSide.BUY;
            case 2 -> AggressorSide.SELL;
            default -> AggressorSide.NONE;
        };
        final TradeEncoder out = sink.trade()
            .venueId(VENUE_ID)
            .aggressorSide(aggressor)
            .priceExponent(exponent(symbol))
            .channelId(channelId)
            .instrumentId(symbol)
            .priceMantissa(msg.price())
            .size(msg.tradeQty())
            .numOrders(0)
            .tradeId(msg.tradeUniqueIdentifier())
            .instrumentSeq(0)
            .packetSeq(packetSeq)
            .exchangeTimeNs(msg.eventTime())
            .receiveTimeNs(receiveTimeNs);
        out.buffer().putByte(out.offset() + TradeEncoder.flagsEncodingOffset(), NormalizedSink.Flags.LAST_IN_EVENT);
        sink.commit();
    }

    private void onStatus(final MarketStatusChangeDecoder msg, final long packetSeq, final long receiveTimeNs)
    {
        final TradingStatus status = switch (msg.bookStateRaw())
        {
            case 2 -> TradingStatus.CLOSE;
            case 3 -> TradingStatus.OPEN;
            case 4 -> TradingStatus.PAUSE;
            case 5 -> TradingStatus.HALT;
            case 6 -> TradingStatus.PRE_OPEN;
            default -> TradingStatus.UNKNOWN;
        };
        sink.instrumentStatus()
            .venueId(VENUE_ID)
            .status(status)
            .channelId(channelId)
            .instrumentId(msg.symbolIndex())
            .packetSeq(packetSeq)
            .exchangeTimeNs(msg.eventTime())
            .receiveTimeNs(receiveTimeNs);
        sink.commit();
    }

    private void onStandingData(final StandingDataDecoder msg, final long receiveTimeNs)
    {
        final int symbol = (int)msg.symbolIndex();
        final int decimals = msg.priceDecimals();
        priceDecimals.put(symbol, decimals);

        Arrays.fill(symbolBytes, (byte)0);
        msg.getInstrumentName(symbolBytes, 0);
        msg.getCurrencyCode(currencyBytes, 0);
        sink.instrumentDefinition()
            .venueId(VENUE_ID)
            .priceExponent((byte)-decimals)
            .channelId(channelId)
            .instrumentId(symbol)
            .minPriceIncrement(msg.tickSize())
            .displayFactor(1)
            .putSymbol(symbolBytes, 0)
            .securityType("EQUITY")
            .putCurrency(currencyBytes, 0)
            .exchangeTimeNs(0)
            .receiveTimeNs(receiveTimeNs);
        sink.commit();
    }

    private void emitBook(
        final int symbol,
        final BookType bookType,
        final Side side,
        final UpdateAction action,
        final int level,
        final long price,
        final long size,
        final int orders,
        final long orderId,
        final byte exponent,
        final long packetSeq,
        final long eventTime,
        final long receiveTimeNs,
        final byte flags)
    {
        final BookUpdateEncoder out = sink.bookUpdate()
            .venueId(VENUE_ID)
            .bookType(bookType)
            .side(side)
            .action(action)
            .level((short)level)
            .priceExponent(exponent)
            .channelId(channelId)
            .instrumentId(symbol)
            .priceMantissa(price)
            .size(size)
            .numOrders(orders)
            .orderId(orderId)
            .instrumentSeq(0)
            .packetSeq(packetSeq)
            .exchangeTimeNs(eventTime)
            .receiveTimeNs(receiveTimeNs);
        out.buffer().putByte(out.offset() + BookUpdateEncoder.flagsEncodingOffset(), flags);
        sink.commit();
    }

    private byte exponent(final int symbol)
    {
        final int decimals = priceDecimals.get(symbol);
        return (byte)-(decimals < 0 ? defaultPriceDecimals : decimals);
    }

    public boolean isRecovering()
    {
        return recovering;
    }
}
