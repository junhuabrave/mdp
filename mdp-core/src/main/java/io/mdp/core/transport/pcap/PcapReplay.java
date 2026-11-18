package io.mdp.core.transport.pcap;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import io.mdp.core.transport.PacketHandler;
import org.agrona.concurrent.EpochNanoClock;

/**
 * Replays one capture file into several lines (for example incremental A, incremental B and snapshot A
 * of one channel), keeping the capture's global packet order across lines.
 * <p>
 * Each packet is matched to a line by destination address and port (optionally source address). A
 * line may deliver a packet only when that packet is the next one in the file, so the arbitrator sees
 * A and B interleaved exactly as captured. Packets for an inactive line (a snapshot feed the decoder
 * does not currently want) are discarded, as a socket that has left the multicast group would.
 * Packets matching no line, non-UDP traffic and IP fragments are skipped and counted.
 * <p>
 * Pacing: {@code speed=1} replays at captured timing, {@code speed=N} N times faster, and
 * {@code speed=max} as fast as the lines are polled. The receive timestamp handed to the pipeline is
 * the replay clock, as for a live socket, so arbitration timers behave as they would live.
 * <p>
 * Not thread safe: all lines of a replay must be polled by one thread, which is the case for the
 * lines of one channel. This is checked.
 */
public final class PcapReplay implements AutoCloseable
{
    /** Speed value meaning "as fast as possible". */
    public static final double MAX_SPEED = 0.0;

    private static final System.Logger LOG = System.getLogger(PcapReplay.class.getName());

    private static final class Line
    {
        final String name;
        final int destinationIp;
        final int destinationPort;
        final int sourceIp;
        boolean active = true;
        long delivered;
        long discarded;

        Line(final String name, final int destinationIp, final int destinationPort, final int sourceIp)
        {
            this.name = name;
            this.destinationIp = destinationIp;
            this.destinationPort = destinationPort;
            this.sourceIp = sourceIp;
        }

        boolean isWildcard()
        {
            return destinationPort == 0;
        }

        boolean matches(final UdpFrame udp)
        {
            return (destinationPort == 0 || (udp.destinationPort() == destinationPort &&
                (destinationIp == 0 || udp.destinationIp() == destinationIp))) &&
                (sourceIp == 0 || udp.sourceIp() == sourceIp);
        }
    }

    private final PcapReader reader;
    private final UdpFrame udp = new UdpFrame();
    private final double speed;
    private final EpochNanoClock clock;
    private final List<Line> lineList = new ArrayList<>();
    private Line[] lines = new Line[0];
    private int wildcardLine = -1;
    private int references;
    private Thread owner;

    private boolean started;
    private long startNanoTime;
    private long firstTimestampNs = Long.MIN_VALUE;
    private boolean haveHead;
    private int headLine;
    private long headTimestampNs;
    private boolean finished;

    private long skippedNoMatch;
    private long skippedNotUdp;
    private long skippedFragments;
    private long skippedTruncated;

    public PcapReplay(final Path file, final double speed, final EpochNanoClock clock)
    {
        if (speed < 0 || Double.isNaN(speed))
        {
            throw new IllegalArgumentException("speed must be > 0, or 0 for max: " + speed);
        }
        this.reader = new PcapReader(file);
        this.speed = speed;
        this.clock = clock;
    }

    /**
     * Register a line. A destination port of 0 makes the line a wildcard that takes every UDP packet no
     * other line matches. An IP of 0 matches any address.
     *
     * @return the line index to poll with
     */
    public int addLine(final String name, final int destinationIp, final int destinationPort, final int sourceIp)
    {
        if (started)
        {
            throw new IllegalStateException("cannot add lines after replay started: " + name);
        }
        final Line line = new Line(name, destinationIp, destinationPort, sourceIp);
        for (final Line existing : lineList)
        {
            if (existing.destinationIp == destinationIp && existing.destinationPort == destinationPort &&
                existing.sourceIp == sourceIp)
            {
                throw new IllegalArgumentException("lines " + existing.name + " and " + name + " of " +
                    reader.path() + " use the same filter");
            }
        }
        if (line.isWildcard())
        {
            if (wildcardLine >= 0)
            {
                throw new IllegalArgumentException("only one line of " + reader.path() + " may omit dst");
            }
            wildcardLine = lineList.size();
        }
        lineList.add(line);
        lines = lineList.toArray(new Line[0]);
        references++;
        return lineList.size() - 1;
    }

    public int poll(final int lineIndex, final PacketHandler handler, final int limit)
    {
        if (!started)
        {
            started = true;
            owner = Thread.currentThread();
            startNanoTime = System.nanoTime();
        }
        else if (Thread.currentThread() != owner)
        {
            throw new IllegalStateException("capture " + reader.path() +
                " polled from two threads; all lines of a capture must be on one channel/agent");
        }

        int count = 0;
        while (count < limit)
        {
            if (!haveHead && !advance())
            {
                break;
            }
            final Line head = lines[headLine];
            if (!head.active)
            {
                head.discarded++;
                haveHead = false;
                continue;
            }
            if (headLine != lineIndex || !isDue())
            {
                break;
            }
            handler.onPacket(reader.frame(), udp.payloadOffset(), udp.payloadLength(), clock.nanoTime());
            head.delivered++;
            haveHead = false;
            count++;
        }
        return count;
    }

    private boolean isDue()
    {
        if (speed == MAX_SPEED)
        {
            return true;
        }
        final long captureElapsed = headTimestampNs - firstTimestampNs;
        final long replayElapsed = System.nanoTime() - startNanoTime;
        return captureElapsed <= (long)(replayElapsed * speed);
    }

    private boolean advance()
    {
        if (finished)
        {
            return false;
        }
        while (reader.next())
        {
            if (firstTimestampNs == Long.MIN_VALUE)
            {
                firstTimestampNs = reader.timestampNs();
            }
            final UdpFrame.Result result = udp.parse(reader.linkType(), reader.frame(), 0, reader.frameLength());
            if (result != UdpFrame.Result.UDP)
            {
                switch (result)
                {
                    case FRAGMENT -> skippedFragments++;
                    case TRUNCATED -> skippedTruncated++;
                    default -> skippedNotUdp++;
                }
                continue;
            }
            final int line = match();
            if (line < 0)
            {
                skippedNoMatch++;
                continue;
            }
            headLine = line;
            headTimestampNs = reader.timestampNs();
            haveHead = true;
            return true;
        }
        finished = true;
        LOG.log(System.Logger.Level.INFO, summary());
        return false;
    }

    private int match()
    {
        final Line[] lines = this.lines;
        for (int i = 0; i < lines.length; i++)
        {
            if (i != wildcardLine && lines[i].matches(udp))
            {
                return i;
            }
        }
        return wildcardLine >= 0 && lines[wildcardLine].matches(udp) ? wildcardLine : -1;
    }

    public void setActive(final int lineIndex, final boolean active)
    {
        lines[lineIndex].active = active;
        if (!active && haveHead && headLine == lineIndex)
        {
            lines[lineIndex].discarded++;
            haveHead = false;
        }
    }

    /** True once every packet in the file has been delivered or skipped. */
    public boolean isFinished()
    {
        return finished && !haveHead;
    }

    public long delivered(final int lineIndex)
    {
        return lines[lineIndex].delivered;
    }

    public long discarded(final int lineIndex)
    {
        return lines[lineIndex].discarded;
    }

    public long skippedNoMatch()
    {
        return skippedNoMatch;
    }

    public long skippedNotUdp()
    {
        return skippedNotUdp;
    }

    public long skippedFragments()
    {
        return skippedFragments;
    }

    public long skippedTruncated()
    {
        return skippedTruncated;
    }

    public Path path()
    {
        return reader.path();
    }

    public double speed()
    {
        return speed;
    }

    public String summary()
    {
        final StringBuilder sb = new StringBuilder("capture ").append(reader.path()).append(" finished: ")
            .append(reader.records()).append(" records");
        for (final Line line : lines)
        {
            sb.append(", ").append(line.name).append(" delivered=").append(line.delivered)
                .append(" discarded=").append(line.discarded);
        }
        return sb.append(", unmatched=").append(skippedNoMatch).append(" non-udp=").append(skippedNotUdp)
            .append(" fragments=").append(skippedFragments).append(" truncated=").append(skippedTruncated).toString();
    }

    /** Release one line's reference; the file closes when the last line releases. */
    public void release()
    {
        if (--references == 0)
        {
            close();
        }
    }

    public void close()
    {
        reader.close();
    }
}
