package io.mdp.core.arbitration;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import io.mdp.core.api.PacketHeaderReader;
import io.mdp.core.metrics.MetricsRegistry;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LineArbitratorTest
{
    /** Test packet: [seq:u32][payload:u32], header length 4. */
    private static final PacketHeaderReader HEADER = new PacketHeaderReader()
    {
        public int headerLength()
        {
            return 4;
        }

        public long sequence(final DirectBuffer buffer, final int offset)
        {
            return buffer.getInt(offset, ByteOrder.LITTLE_ENDIAN) & 0xFFFF_FFFFL;
        }

        public long sendingTimeNs(final DirectBuffer buffer, final int offset)
        {
            return 0;
        }
    };

    private static final long GAP_TIMEOUT = 1_000;
    private static final long LINE_TIMEOUT = 1_000_000;

    private final List<Long> delivered = new ArrayList<>();
    private final List<long[]> gaps = new ArrayList<>();
    private final List<Long> resets = new ArrayList<>();
    private final List<String> lineEvents = new ArrayList<>();
    private final UnsafeBuffer packet = new UnsafeBuffer(new byte[64]);
    private LineArbitrator arbitrator;

    @BeforeEach
    void setUp()
    {
        final SequencedPacketHandler handler = new SequencedPacketHandler()
        {
            public void onPacket(final DirectBuffer b, final int o, final int l, final long seq, final long rt)
            {
                assertEquals(seq, b.getInt(o + 4, ByteOrder.LITTLE_ENDIAN), "payload matches sequence");
                delivered.add(seq);
            }

            public void onGap(final long from, final long to)
            {
                gaps.add(new long[]{ from, to });
            }

            public void onSequenceReset(final long newSeq)
            {
                resets.add(newSeq);
            }

            public void onLineStatus(final int line, final boolean up)
            {
                lineEvents.add(line + (up ? "+" : "-"));
            }
        };
        arbitrator = new LineArbitrator(HEADER, handler,
            new LineArbitrator.Config(GAP_TIMEOUT, 8, 64, LINE_TIMEOUT, 1),
            2, new String[]{ "A", "B" }, MetricsRegistry.local(), "1", "incremental");
    }

    private void send(final int line, final long seq, final long now)
    {
        packet.putInt(0, (int)seq, ByteOrder.LITTLE_ENDIAN);
        packet.putInt(4, (int)seq, ByteOrder.LITTLE_ENDIAN);
        arbitrator.onPacket(line, packet, 0, 8, 0, now);
    }

    @Test
    void deliversFirstArrivalAndDropsDuplicates()
    {
        for (long seq = 1; seq <= 5; seq++)
        {
            send(0, seq, seq);
            send(1, seq, seq);
        }
        assertEquals(List.of(1L, 2L, 3L, 4L, 5L), delivered);
        assertTrue(gaps.isEmpty());
    }

    @Test
    void lineBFillsHoleOnLineA()
    {
        send(0, 1, 0);
        send(0, 3, 1);   // A lost 2
        send(0, 4, 2);
        assertEquals(List.of(1L), delivered);
        send(1, 1, 3);
        send(1, 2, 4);   // B fills 2, buffered 3 and 4 drain
        send(1, 3, 5);
        send(1, 4, 6);
        assertEquals(List.of(1L, 2L, 3L, 4L), delivered);
        assertTrue(gaps.isEmpty());
        assertEquals(0, arbitrator.pending());
    }

    @Test
    void declaresGapAfterTimeoutWhenBothLinesMiss()
    {
        send(0, 1, 0);
        send(0, 4, 10);
        send(1, 4, 11);
        arbitrator.doTimers(10 + GAP_TIMEOUT - 1);
        assertTrue(gaps.isEmpty());
        arbitrator.doTimers(10 + GAP_TIMEOUT);
        assertEquals(1, gaps.size());
        assertEquals(2, gaps.get(0)[0]);
        assertEquals(3, gaps.get(0)[1]);
        assertEquals(List.of(1L, 4L), delivered);

        send(1, 2, 2000);  // late arrival is dropped
        assertEquals(List.of(1L, 4L), delivered);
    }

    @Test
    void forcesGapWhenHoleExceedsReorderWindow()
    {
        send(0, 1, 0);
        send(0, 3, 1);
        send(0, 20, 2);   // window is 8 slots
        assertEquals(List.of(1L, 3L, 20L), delivered);
        assertEquals(2, gaps.size());
        assertEquals(2, gaps.get(0)[0]);
        assertEquals(2, gaps.get(0)[1]);
        assertEquals(4, gaps.get(1)[0]);
        assertEquals(19, gaps.get(1)[1]);
    }

    @Test
    void sequenceResetHappensOnceAcrossLines()
    {
        for (long seq = 1; seq <= 3; seq++)
        {
            send(0, seq, seq);
            send(1, seq, seq);
        }
        send(0, 1, 10);   // A restarts
        send(1, 4, 11);   // B still on old epoch: ignored
        send(0, 2, 12);
        send(1, 1, 13);   // B restarts: duplicate of new epoch
        send(1, 2, 14);
        send(1, 3, 15);
        assertEquals(List.of(1L), resets);
        assertEquals(List.of(1L, 2L, 3L, 1L, 2L, 3L), delivered);
    }

    @Test
    void reportsLineDownAndUp()
    {
        send(0, 1, 0);
        send(1, 1, 0);
        send(0, 2, LINE_TIMEOUT);
        arbitrator.doTimers(LINE_TIMEOUT + 1);
        assertEquals(List.of("0+", "1+", "1-"), lineEvents);
        assertFalse(arbitrator.isLineUp(1));
        send(1, 2, LINE_TIMEOUT + 2);
        assertTrue(arbitrator.isLineUp(1));
    }
}
