# MDP — Low-Latency Market Data Feed Handler

Status: **v0.2** — revised after two review rounds (see §13)
Scope: Java feed handler for SBE-encoded exchange market data (CME MDP 3.0, Euronext Optiq MDG), built on Aeron, SBE and Agrona.

---

## 1. Goals and non-goals

### Goals

| # | Goal | Measure |
|---|------|---------|
| G1 | Low, predictable latency on the hot path | Zero allocation in steady state; no locks; p99 wire-to-listener < 10 µs in-process on a tuned host |
| G2 | Several exchange protocols behind one model | CME MDP 3.0 and Euronext Optiq MDG at launch; adding a venue = one new module, no core changes |
| G3 | Data resilience | A/B line arbitration, gap detection, snapshot recovery, explicit stale/recovered state downstream, no silent data loss |
| G4 | Flexible deployment | Same components run **all-in-one** (one JVM) or **split** (gateway / normalizer / consumers over Aeron) — chosen by config |
| G5 | Observability | Metrics through OpenTelemetry → Prometheus → Grafana, with dashboards and alerts, and no telemetry cost on the hot path |

### Non-goals (v1)

- Order entry (iLink / Optiq OEG).
- Kernel-bypass NICs (Solarflare/Onload, DPDK). The transport interface allows them later.
- Full official exchange schemas in the repo. We ship trimmed schemas that follow the public specs for development and tests. Production builds drop in the official XML (see §5.4).
- Distributed tracing on the hot path. Per-message spans cost too much; latency is measured with histograms instead.

---

## 2. Technology choices

| Concern | Choice | Why |
|---|---|---|
| Wire decoding | **SBE** (`sbe-tool` 1.42.0 codegen) | Flyweight decoders read in place from the receive buffer; no copy, no allocation |
| In-process hand-off | **Agrona `OneToOneRingBuffer` / `ManyToOneRingBuffer`** | Lock-free, off-heap, zero-copy `tryClaim`/`commit` |
| Inter-process transport | **Aeron** 1.54.2 (IPC and UDP) | Same API for shared memory and network; `ExclusivePublication.tryClaim` is zero copy; built-in flow-control counters |
| Threading | **Agrona `Agent` / `AgentRunner`** with configurable `IdleStrategy` | Single-threaded duty cycles, busy-spin or back-off per agent |
| Collections | Agrona primitive maps (`Long2ObjectHashMap`, `Int2ObjectHashMap`) | No boxing |
| Latency histograms | **HdrHistogram** `SingleWriterRecorder` | Wait-free recording on the hot thread |
| Metrics export | **OpenTelemetry SDK** 1.68.0, OTLP → OTel Collector → **Prometheus** → **Grafana** | Vendor-neutral; the collector can also send to Mimir, Datadog and others |
| JVM | Java 21 (LTS) | Generational ZGC, VarHandles |
| Build | Gradle (Kotlin DSL), multi-module | SBE codegen as a build task |

---

## 3. Architecture overview

### 3.1 Logical pipeline

```
                ┌──────────────────────────── Channel Handler (one per exchange channel) ───────────────────────────┐
 UDP mcast A ──▶│ PacketSource A ┐                                                                                   │
                │                ├─▶ LineArbitrator ─▶ FeedDecoder (venue SBE) ─▶ NormalizedSink ──┐               │
 UDP mcast B ──▶│ PacketSource B ┘   (dedupe, reorder,   (per-instrument state,    (tryClaim/commit)  │               │
                │                     gap detection)       recovery logic)                          │               │
 Snapshot A/B ─▶│ PacketSource S ──▶ SnapshotArbitrator ─▶ FeedDecoder.onSnapshot ─────────────────────┤ (only while  │
                │                                                                                     │  recovering) │
                └─────────────────────────────────────────────────────────────────────────────────────┼──────────────┘
                                                                                                      ▼
                                                                    Agrona RingBuffer  or  Aeron Publication
                                                                                                      │
                                                         ┌────────────────────────────────────────────┤
                                                         ▼                                            ▼
                                               ConsumerAgent: BookManager              Downstream processes
                                               ─▶ MarketDataListener(s)                (strategies, recorder)
```

Every stage boundary is an interface. Each interface has an in-process implementation and an Aeron implementation, which is how one codebase gives both deployment modes (§7).

### 3.2 Normalized internal model

Venue messages are turned into one **normalized SBE schema** (`mdp-normalized.xml`, schemaId 900). Downstream code never sees venue formats.

| Template | Id | Key fields |
|---|---|---|
| `BookUpdate` | 1 | venueId, channelId, instrumentId (i64), bookType (MBP/MBO/IMPLIED), side, action (NEW/CHANGE/DELETE/DELETE_THRU/DELETE_FROM/OVERLAY), level, priceMantissa (i64), priceExponent (i8), size, numOrders, orderId, instrumentSeq, packetSeq, exchangeTimeNs, receiveTimeNs, flags (LAST_IN_EVENT, SNAPSHOT, IMPLIED) |
| `Trade` | 2 | instrumentId, price, size, aggressorSide, tradeId, instrumentSeq, packetSeq, times, flags |
| `InstrumentStatus` | 3 | instrumentId, status (PRE_OPEN, OPEN, HALT, PAUSE, CLOSE, POST_CLOSE, UNKNOWN), times |
| `InstrumentDefinition` | 4 | instrumentId, symbol char[32], securityType char[8], currency char[3], priceExponent, minPriceIncrement, displayFactor |
| `FeedEvent` | 5 | eventType (GAP_DETECTED, BOOK_STALE, RECOVERY_STARTED, RECOVERY_COMPLETE, CHANNEL_RESET, SEQUENCE_RESET, LINE_DOWN, LINE_UP), instrumentId (0 = whole channel), fromSeq, toSeq |

Design rules:

- **Fixed length only** (no var-data). Every message has a known maximum size, so `tryClaim(length)` works before encoding.
- **Prices are decimal mantissa + exponent.** CME PRICE9 gives exponent −9. Optiq exponent = −priceDecimals from standing data. No floating point.
- **The SBE message header is included in every record.** Ring-buffer records and Aeron fragments are byte-identical, so a consumer can forward a record to Aeron without re-encoding.
- **Two timestamps:** `exchangeTimeNs` (venue) and `receiveTimeNs` (local, taken once per packet). `receiveTimeNs` uses `SystemEpochNanoClock` (`clock_gettime(CLOCK_REALTIME)`, about 20–30 ns). Every process on a host reads the same kernel clock, so latency is comparable across processes in split mode. A per-process `OffsetEpochNanoClock` calibration could disagree by microseconds. *(Review R1-6)*
- **Event boundaries:** one exchange "event" (for example a CME match, closed by `MatchEventIndicator.EndOfEvent`) can produce many normalized messages. The last one carries `LAST_IN_EVENT`. Consumers should act on a book only at event end, never on a half-applied match. *(Review R1-2)*

---

## 4. Module layout

```
mdp/
├── mdp-schema     normalized SBE schema + generated codecs (depends only on Agrona)
├── mdp-core       interfaces, transports, arbitration, recovery helpers, ring-buffer pipeline,
│                  order book, agents, config, metrics (Aeron/Agrona counters + HdrHistogram)
├── mdp-cme        CME MDP 3.0 schema subset + codegen + CmeMdp3Protocol (FeedProtocol SPI)
├── mdp-euronext   Optiq MDG schema subset + codegen + OptiqProtocol (FeedProtocol SPI)
├── mdp-otel       OpenTelemetry exporter that reads counters (in-process or sidecar)
├── mdp-app        launcher (all-in-one / gateway / normalizer / consumer / observer / recorder),
│                  feed simulator with loss injection, latency benchmark
├── config/        example *.properties per deployment role
└── deploy/        docker-compose: otel-collector, prometheus (+ alert rules), grafana (+ dashboards)
```

Dependency direction: `app → {cme, euronext, otel} → core → schema`. The core has no venue code and no OpenTelemetry dependency.

---

## 5. Extensibility

### 5.1 Protocol SPI

```java
public interface FeedProtocol {               // discovered with java.util.ServiceLoader
    String name();                            // "cme-mdp3", "euronext-optiq"
    int venueId();
    PacketHeaderReader packetHeader();        // stateless: headerLength, sequence, sendingTime, isHeartbeat
    FeedDecoder newDecoder(DecoderContext ctx);
}

public interface FeedDecoder extends SequencedPacketHandler {
    void onPacket(DirectBuffer buf, int offset, int length, long packetSeq, long receiveTimeNs);
    void onPacketGap(long fromSeq, long toSeq);            // from the incremental arbitrator
    void onSequenceReset(long newSeq);
    void onSnapshotPacket(DirectBuffer buf, int offset, int length, long seq, long receiveTimeNs);
    void onSnapshotGap(long fromSeq, long toSeq);
    boolean wantsSnapshot();                               // channel polls snapshot lines only while true
}
```

`DecoderContext` gives the decoder its `NormalizedSink`, a `RecoveryControl`, channel id, clocks, metrics and config.

**Adding a venue** (for example Nasdaq ITCH-over-SBE, B3 UMDF, ICE iMpact) means:
1. Create module `mdp-<venue>` with the venue SBE XML in `src/main/sbe/`. The shared build convention generates codecs.
2. Implement `PacketHeaderReader`, `FeedDecoder` and `FeedProtocol`.
3. Register it in `META-INF/services/io.mdp.core.api.FeedProtocol`.
4. Select it with `mdp.protocol=<name>`.

Inside a decoder, dispatch is a `switch` on templateId. Handlers can be swapped or added per template through a `TemplateHandler` registry (an `Int2ObjectHashMap`) when a deployment needs custom handling, such as statistics messages, without forking the adapter.

### 5.2 Other extension points

| Interface | Built-in implementations | Possible later |
|---|---|---|
| `PacketSource` | `UdpPacketSource` (unicast/multicast/SSM, NIO non-blocking), `AeronPacketSource` | Onload/ef_vi, PCAP replay, DPDK |
| `NormalizedSink` | `RingBufferSink`, `AeronSink` | Chronicle Queue, Kafka (off the hot path) |
| `NormalizedEventSource` | `RingBufferEventSource`, `AeronEventSource` | — |
| `MarketDataListener` | `BookManager` + user listeners | Conflation, MBO book, bar builder |
| `CounterFactory` | Aeron CnC counters, local `CountersManager` | — |
| `IdleStrategy` | busy-spin, yield, backoff, sleep (Agrona) | — |

**Consumer fan-out:** every listener on a consumer agent runs on that one thread, so one slow listener delays all of them. Latency-critical listeners get their own consumer: in all-in-one mode, a dedicated ring buffer per consumer group (the handler writes to N rings, or one ring is copied to N); in split mode, a separate Aeron subscriber. *(Review R1-4)*

### 5.3 Sink claim/commit contract (zero copy)

```java
BookUpdateEncoder e = sink.bookUpdate();   // claims space; SBE header already written
e.instrumentId(id).side(Side.BUY)...;     // decoder writes fields straight into the ring buffer / Aeron term
sink.commit();
```

When the claim fails because of back-pressure, the sink returns an encoder over a scratch buffer, `commit()` does nothing, and the drop policy applies (§6.5).

### 5.4 Exchange schemas

The repo ships **trimmed schemas** that follow the public specs, with official template ids and field names:

- **CME MDP 3.0** (schemaId 1): `ChannelReset4`, `SecurityStatus30`, `MDIncrementalRefreshBook46`, `MDIncrementalRefreshTradeSummary48`, `SnapshotFullRefresh52`, `MDInstrumentDefinitionFuture54` (leading fields). Packet = 12-byte binary packet header (`MsgSeqNum` u32, `SendingTime` u64) followed by messages framed as `MsgSize` u16 + SBE header + body, little-endian.
- **Euronext Optiq MDG**: packet header (`SendingTime` u64, `PacketSequenceNumber` u32, `PacketFlags` u16, `ChannelID` u16) followed by SBE messages: `MarketUpdate`, `OrderUpdate`, `FullTradeInformation`, `MarketStatusChange`, `StandingData`, `StartOfSnapshot`/`EndOfSnapshot`. Some template ids and the message framing are **illustrative**. They must be checked against the current Optiq MDG spec before production use.

For production, replace the XML with the official file (CME `templates_FixBinary.xml`, Euronext Optiq MDG SBE schema). Decoders honor `actingBlockLength` and the message size, so newer schema versions with extra trailing fields still decode. Adapter code uses official field names so it compiles against the official schema with few or no changes. A CI check decodes certified sample PCAPs where they are available.

---

## 6. Data resilience

### 6.1 Line arbitration (A/B, generalizable to N lines)

`LineArbitrator` is venue-agnostic and keys on the packet sequence from `PacketHeaderReader`.

```
on packet(seq, line):
  seq <= lastDelivered        → duplicate: drop, count line.dup (the line that lost the race)
  seq == expected             → deliver, count line.win; drain contiguous packets from the reorder buffer
  seq >  expected             → store in the reorder buffer and start the gap timer if it is not running
timer (gapTimeout, e.g. 200 µs–1 ms) expires, or reorder buffer is full:
  → declare gap [expected, lowestBuffered-1] → decoder.onPacketGap(); skip ahead; drain the buffer
```

- **Late packets:** a packet below `expected` that arrives after the arbitrator already declared a gap and skipped ahead is dropped and counted as `late`, separately from duplicates. A high late count means `gapTimeout` is too tight. *(Review R2-4)*
- **Oversize packets:** a packet larger than a reorder slot cannot be buffered. It is counted (`oversize`), and the arbitrator treats it as lost for this line, so the other line may still fill it. *(Review R2-3)*
- **Reorder buffer:** a pre-allocated ring of slots (power of two, default 1024 × 1500 B, sized for jumbo frames if needed). Packets are copied in only when they arrive out of order. The in-order fast path never copies.
- **Per-line health:** packets, wins, duplicates, the line's own gaps, and time since last packet. A line is `DOWN` after `lineTimeout` with no packets (heartbeats count) and emits `FeedEvent LINE_DOWN/LINE_UP`. One line down is a warning. Both lines down is critical.
- **Sequence reset:** `seq == 1` after `lastDelivered > 1` (CME weekly reset, Optiq SOD) emits `SEQUENCE_RESET` and resyncs. Snapshot arbitrators accept resets because snapshot loops restart at 1.
- **Win ratio** per line is exported. A line that keeps losing shows a routing or NIC problem before it causes a gap.

### 6.2 Gap detection levels

| Level | Detected by | Action |
|---|---|---|
| Packet | `LineArbitrator` (both lines missed it within `gapTimeout`) | `onPacketGap` → decoder policy |
| Instrument | Decoder (CME `RptSeq` jump per SecurityID) | Mark that instrument stale and recover it |
| Snapshot cycle | Snapshot arbitrator | Discard the partial cycle and wait for the next loop |

### 6.3 Recovery strategies (per venue, using core helpers)

**CME MDP 3.0 — RptSeq-based natural recovery**

- Per instrument: `state ∈ {LIVE, STALE}`, `expectedRptSeq`, `lastSeenRptSeq`.
- Packet gap → policy `CHANNEL_STALE` (default, conservative: every instrument on the channel goes stale) or `INSTRUMENT_RPTSEQ` (only instruments whose RptSeq jumps go stale).
- While any instrument is stale, `wantsSnapshot()` is true and the channel joins the snapshot loop.
- `SnapshotFullRefresh52` for a stale instrument is applied only when `snapshot.RptSeq >= lastSeenRptSeq`. It then emits book-clear, the snapshot levels (flag `SNAPSHOT`) and `RECOVERY_COMPLETE`, and sets `expectedRptSeq = RptSeq + 1`. An older snapshot is skipped and the next loop is used. This keeps the order correct without buffering incrementals.
- When no instruments are stale, the channel leaves the snapshot feed, saving CPU and NIC bandwidth.
- **Activating snapshot sources** (`PacketSource.activate()/deactivate()`) *(Review R2-2)*. If the handler stopped polling a socket but stayed in the group, the kernel buffer would fill with old snapshot packets that get replayed on resume. So:
  - The UDP source **leaves the multicast group** on deactivate and joins again on activate. IGMP join latency (milliseconds) is negligible next to the snapshot loop period.
  - The UDP unicast source and the Aeron source **drain and discard** while inactive.
  - The snapshot arbitrator resets on each activation.

**Euronext Optiq — snapshot plus buffered replay**

- On gap: every instrument goes stale and incremental packets are copied into a bounded `PacketReplayBuffer`.
- Snapshot cycle: `StartOfSnapshot` → instrument messages (applied to a reset book, flag `SNAPSHOT`) → `EndOfSnapshot(lastPacketSeq = L)`.
- On end: if the buffer holds `L+1` without a hole, replay the buffered packets with seq > L, go LIVE, and emit `RECOVERY_COMPLETE`. If the buffer overflowed past L+1, wait for the next cycle.

**Instrument definitions:** CME publishes definitions on a separate looping instrument-definition feed. Euronext publishes them as `StandingData` on the snapshot channel. Each channel has an optional third feed type, `definition`, polled at startup and whenever definitions are missing. Books do not depend on definitions (prices are carried as mantissa/exponent), so a missing definition never blocks market data. *(Review R1-3)*

**Implied books (CME):** implied entries (`MDEntryType` E/F) are normalized with `bookType=IMPLIED` and kept in a separate book. Merging outright and implied books is a consumer concern. *(Review R1-8)*

`PacketReplayBuffer`, `InstrumentStateTable` (open-addressing long → state) and `RecoveryControl` live in `mdp-core` so new venues can reuse them.

### 6.4 Downstream contract

Consumers never have to guess whether data is valid:
- `FeedEvent BOOK_STALE` (channel or instrument) comes **before** any data loss can affect a book.
- Snapshot-sourced updates carry the `SNAPSHOT` flag, and `RECOVERY_COMPLETE` marks the return to live.
- `BookManager` exposes `book.isStale()`. Strategies should not trade on stale books.

### 6.5 Back-pressure and overload

- Ring buffer or Aeron publication full → policy `DROP_AND_MARK_STALE` (default: the hot path never blocks) or `SPIN` (lossless, for recorders and back-tests). With `SPIN`, a stalled consumer backs up into the kernel socket buffer and then causes packet loss, so the risk moves rather than disappears.
- How a drop is handled: the sink sets a `dropped` flag. After each packet, the channel handler checks the flag and calls `decoder.onSinkDrop()`. The decoder then treats the drop exactly like a packet gap (CME: channel stale → snapshot; Optiq: stale + buffering). `BOOK_STALE` is emitted with `SPIN` semantics once space returns, because losing the stale notice itself would break the downstream contract. *(Review R1-5)*
- Socket: `SO_RCVBUF` is configurable (default 16 MB). The kernel drop counter (`/proc/net/snmp` `RcvbufErrors` on Linux) is exported by the observer.
- Aeron: when the publication returns `BACK_PRESSURED` or `NOT_CONNECTED`, the result is counted per stream.

### 6.6 Process-level resilience

- **Hot-hot handlers:** run two handler instances, on separate hosts or NICs, each publishing its own normalized stream. The consumer side uses **per-channel primary/backup failover**, not per-event merging. It follows the primary's stream for a channel and switches to the backup when the primary goes stale or silent for longer than `failoverTimeout` while the backup is live. After switching it rebuilds that channel's books from the backup (the backup re-sends a book image on request, or the consumer marks the channel stale until the next snapshot-flagged image).
  *Why not merge per event, as v0.1 proposed:* one packet produces a variable number of normalized events, and snapshot-sourced events differ between instances (each recovers at a different point in the loop), so `(channelId, packetSeq)` is not a unique, comparable key. *(Review R1-1)*
- **Recording/replay:** the `recorder` role uses **Aeron Archive** to record the normalized (and optionally raw) streams. It supports audit, post-trade replay and fast restart of consumers.
- **Fast restart:** a restarted handler starts in `STALE` and recovers from the snapshot feed. No local state is needed.
- **Process supervision** is the job of systemd or k8s. Liveness = duty-cycle heartbeat counter advancing. Readiness = all channels LIVE.

---

## 7. Deployment modes

The same classes are wired differently by `mdp.mode`.

### 7.1 All-in-one (lowest latency)

```
[ JVM ]  ChannelHandler agent(s) ──Agrona RingBuffer──▶ Consumer agent (BookManager + listeners)
         Metrics agent (low priority) ──▶ OTLP
         Optional: Aeron publication of the normalized stream for other local processes (IPC)
```
- Fewest hops. No Aeron is needed unless the stream is shared.
- **Pipeline sub-modes** (`mdp.pipeline`), chosen per deployment *(Review R2-1)*:
  - `ringbuffer` (default): the decoder and the consumer run on separate threads. Decoding never waits on a listener, at the cost of one cross-core hand-off (≈ 50–150 ns plus a cache-line transfer).
  - `inline`: `DirectDispatchSink` encodes into a reusable scratch buffer and calls the consumer handler on the receiver thread right away. This is the lowest latency, but a slow listener delays socket reads, so use it only with trivially fast listeners.
- Threads: 1 receiver (all channels, or one per channel group), 1 consumer, 1 metrics. Pin hot threads to isolated cores with `taskset`/`isolcpus`, outside the JVM.

### 7.2 Split components (flexibility and fan-out)

```
 gateway  (per NIC/site)     : UDP A/B ─▶ LineArbitrator ─▶ Aeron (raw, arbitrated packets + receiveTimeNs)
 normalizer                  : Aeron raw ─▶ FeedDecoder + recovery ─▶ Aeron normalized stream
 consumer(s)                 : Aeron normalized ─▶ BookManager ─▶ strategy listeners      (N processes)
 media driver                : standalone Aeron MediaDriver (dedicated, busy-spin)
 observer (sidecar)          : reads Aeron CnC counters ─▶ OTLP   (zero code in the hot processes)
 recorder                    : Aeron Archive of normalized and/or raw streams
```

- The gateway relays snapshot lines on their own stream, so the normalizer can recover without touching the network.
- The raw relay frame is `[receiveTimeNs u64][original packet bytes]`, so end-to-end latency stays measurable.
- **When to split:** many consumers or languages, isolating venue decoding from strategies, colocating gateways with NICs, independent restarts and upgrades, recording. **When not to:** single-strategy latency-critical setups. Each Aeron IPC hop adds roughly 100–300 ns plus scheduling effects.

### 7.3 Configuration (properties)

```properties
mdp.mode=all-in-one                      # all-in-one | gateway | normalizer | consumer | observer | recorder
mdp.protocol=cme-mdp3
mdp.channels=310
mdp.channel.310.incremental.A=udp://224.0.31.1:14310?interface=eth0
mdp.channel.310.incremental.B=udp://224.0.32.1:15310?interface=eth1
mdp.channel.310.snapshot.A=udp://224.0.31.22:14310?interface=eth0
mdp.channel.310.snapshot.B=udp://224.0.32.22:15310?interface=eth1
mdp.channel.310.gapPolicy=CHANNEL_STALE
mdp.arbitration.gapTimeoutMicros=500
mdp.arbitration.reorderSlots=1024
mdp.line.timeoutMillis=2000
mdp.ringbuffer.capacity=8388608
mdp.backpressure=DROP_AND_MARK_STALE
mdp.idle.receiver=busy-spin
mdp.idle.consumer=busy-spin
mdp.aeron.dir=/dev/shm/aeron-mdp
mdp.aeron.embeddedDriver=false
mdp.output.aeron.channel=aeron:ipc
mdp.output.aeron.streamId=1001
mdp.metrics.otlp.endpoint=http://localhost:4317
mdp.metrics.intervalMillis=1000
```
In split mode, a source is `aeron:<channel>` plus `.streamId=<n>` instead of `udp://`.

---

## 8. Threading and latency engineering

- **Single-writer principle:** each piece of mutable state has exactly one owning agent. Cross-thread communication goes only through the ring buffer, Aeron, or counters written with release semantics.
- **Zero allocation:** flyweight codecs, pre-allocated reorder and replay buffers, primitive maps sized at startup, a cached nano clock per duty cycle, and no lambdas or iterators that capture state on the hot path. Verified with a JFR allocation profile and a `-Xmx`-constrained soak test.
- **Socket I/O:** non-blocking `DatagramChannel` over direct `ByteBuffer`s wrapped by `UnsafeBuffer`. JDK 11+ caches the sender address, so `receive()` does not allocate for a steady sender. Each duty cycle polls all lines in round-robin so one hot line cannot starve the other.
- **JVM flags (documented in the launcher):** `-XX:+UseZGC -XX:+ZGenerational` (or Epsilon for benchmarks), `-XX:+AlwaysPreTouch`, `-Dagrona.disable.bounds.checks=true`, `--add-opens java.base/jdk.internal.misc=ALL-UNNAMED`, large pages where available.
- **OS (documented, not automated):** `isolcpus`/`nohz_full`, IRQ affinity for the NIC, `net.core.rmem_max`, busy-poll sockets, and turning off C-states and frequency scaling.

---

## 9. Observability

### 9.1 Principle: the hot path only increments counters

Hot threads never call OpenTelemetry, never log, and never allocate for telemetry. They write:
- **Counters:** Agrona `AtomicCounter`s with single-writer release stores. In Aeron deployments these live in the media driver's **CnC file**, so any process can read them. Otherwise a local `CountersManager` holds them.
- **Latency:** HdrHistogram `SingleWriterRecorder.recordValue()` (wait-free).

A low-priority **HistogramSnapshotAgent** swaps interval histograms every second and writes p50/p90/p99/p99.9/max/count into gauge counters. As a result, **every metric is a counter**, and one exporter design covers both in-process and sidecar use.

Each counter label is `metric.name{k=v,...}`, for example `mdp.line.packets{channel=310,line=A,feed=incremental}`. The exporter parses the label once into OTel `Attributes` and caches them per counterId.

### 9.2 Metrics catalog (initial)

| Metric | Type | Attributes | Purpose |
|---|---|---|---|
| `mdp.line.packets` | counter | channel, line, feed | Throughput per line |
| `mdp.line.wins` / `mdp.line.duplicates` | counter | channel, line | Arbitration effectiveness |
| `mdp.line.gaps` | counter | channel, line | Loss on each line |
| `mdp.line.up` | gauge 0/1 | channel, line | Line health |
| `mdp.channel.gaps` / `mdp.channel.gap.packets` | counter | channel | Loss after arbitration (the alert metric) |
| `mdp.channel.state` | gauge | channel | 0 LIVE, 1 RECOVERING |
| `mdp.instruments.stale` | gauge | channel | Books not safe to use |
| `mdp.recovery.count` / `mdp.recovery.duration.max` | counter/gauge | channel | Recovery behavior |
| `mdp.reorder.buffered` | gauge | channel | Out-of-order depth |
| `mdp.messages.decoded` / `mdp.messages.unknown` | counter | channel, template | Decode coverage |
| `mdp.sink.dropped` / `mdp.sink.backpressure` | counter | sink | Overload |
| `mdp.ringbuffer.utilization` | gauge | ring | Consumer keeping up |
| `mdp.latency.decode` | p50/p99/p999/max | channel | Receive → normalized commit |
| `mdp.latency.e2e` | p50/p99/p999/max | consumer | Receive → listener |
| `mdp.latency.wire` | p50/p99/max | channel | Exchange SendingTime → receive (needs PTP-synced clock) |
| `mdp.agent.dutycycle` / `mdp.agent.maxcycle.ns` | counter/gauge | agent | Stalls and jitter |
| `aeron.*` | gauge | label | Aeron system counters (back-pressure, NAKs, errors), observer only |
| JVM | — | — | GC pauses, heap, threads (OTel runtime-telemetry, exporter process only) |

### 9.3 Export pipeline

```
 handler JVM(s) ──counters (CnC / local)──▶ mdp-otel exporter (in-process low-prio thread, or `observer` sidecar)
                                            │ OTel SDK: batchCallback over CountersReader
                                            ▼ OTLP gRPC :4317
                                     OpenTelemetry Collector  ──▶ Prometheus (remote-write or scrape :8889)
                                                                       ▼
                                                                    Grafana (provisioned dashboards)
```

`deploy/docker-compose.yml` brings up the collector, Prometheus (with `alerts.yml`) and Grafana with a provisioned datasource and dashboard:
- **Feed Health:** line up/down, packets/s per line, A/B win ratio, gaps, stale instruments, recovery timeline.
- **Latency:** decode, e2e and wire percentiles, with duty-cycle max.
- **Capacity:** ring-buffer utilization, sink drops, Aeron back-pressure, GC pauses.

Initial alerts: `ChannelGap` (gap rate > 0), `LineDown`, `BothLinesDown` (critical), `StaleInstruments > 0 for 30s`, `SinkDrops`, `E2ELatencyP99High`, `RecoveryStuck`.

Logs: lifecycle and errors only, from non-hot threads (`System.Logger`). Hot-path errors go to an Agrona `ErrorHandler` that counts them and hands them to a logging agent through a queue. An `otel` log bridge can be added later without changing the hot path.

---

## 10. Testing and verification

| Layer | Test |
|---|---|
| Unit | `LineArbitrator` (in-order, duplicates, fill from B, timeout gap, buffer overflow, reset), `PriceLevelBook` actions, `PacketReplayBuffer` |
| Venue | Round-trip: encode packets with generated encoders → decoder → normalized events; RptSeq gap → stale → snapshot → live (CME); buffered replay (Optiq) |
| Pipeline | Deterministic in-memory sources with injected loss and reorder on A/B; the final book must equal the simulator's reference book |
| Split mode | Embedded MediaDriver: gateway → normalizer → consumer over Aeron IPC |
| Allocation | Steady-state run under JFR with zero allocation events on hot threads |
| Latency | `LatencyBenchmark` (HdrHistogram output) in all-in-one and split modes; JMH for decoder micro-benchmarks |
| Simulator | `FeedSimulator`: synthetic CME/Optiq traffic over UDP to A/B with configurable loss, reorder and snapshot loop, for demos and soak tests |

---

## 11. Implementation plan

| Phase | Deliverable | Exit criteria |
|---|---|---|
| 1 | Gradle multi-module build, SBE codegen, normalized schema, CME and Optiq schema subsets | `./gradlew build` generates and compiles codecs |
| 2 | Core: transports, `LineArbitrator`, sinks (ring buffer and Aeron), agents, `PriceLevelBook`, config | Arbitrator and book unit tests pass |
| 3 | CME MDP3 adapter + RptSeq recovery | CME round-trip and recovery tests pass |
| 4 | Euronext Optiq adapter + buffered-replay recovery | Optiq tests pass |
| 5 | Metrics (counters, histograms), `mdp-otel` exporter, observer sidecar | Metrics visible in Prometheus from the simulator |
| 6 | Launcher modes, simulator, docker-compose stack, Grafana dashboards, alerts | Demo: simulator with 1% loss on A → no channel gaps; loss on A and B → gap → recovery shown in Grafana |
| 7 | Latency benchmark, allocation verification, README | Published latency numbers; zero hot-path allocation |

## 12. Expected latency budget (in-process, tuned Linux host, before measurement)

| Stage | Estimate | Notes |
|---|---|---|
| NIC → kernel → `DatagramChannel.receive` | 2–8 µs | Kernel UDP stack plus syscall. This dominates. Kernel bypass (Onload/ef_vi) cuts it to ≈1 µs |
| Arbitration (in order, no copy) | 10–30 ns | One sequence compare |
| SBE decode + normalize + `tryClaim/commit` per message | 50–200 ns | Flyweights, no allocation |
| Ring-buffer hand-off to consumer thread | 50–150 ns | Cross-core cache transfer; zero in `inline` mode |
| Book update + listener dispatch | 20–80 ns | Fixed-depth arrays |
| Aeron IPC hop (split mode, each hop) | 100–300 ns | Plus scheduling jitter if not busy-spinning |

The code-path budget (receive returned → listener invoked) is **< 1 µs p50 and < 5 µs p99** on isolated cores. The benchmark measures this path separately from the kernel receive path. *(Review R2-5)*

## 13. Review log

**Round 1 (correctness and data contract)**

| Id | Finding | Resolution |
|---|---|---|
| R1-1 | Hot-hot per-event merge keyed on `(channel, packetSeq)` is unsound: one packet makes a variable number of events, and snapshot events differ between instances | Replaced with per-channel primary/backup failover (§6.6) |
| R1-2 | No event-boundary semantics, so consumers could act on half-applied matches | `LAST_IN_EVENT` flag from `MatchEventIndicator.EndOfEvent` (§3.2) |
| R1-3 | Instrument definitions not covered (CME has a separate definition feed) | Optional `definition` feed type; books do not depend on definitions (§6.3) |
| R1-4 | One consumer thread runs all listeners, so a slow listener delays all of them | Fan-out guidance: rings per consumer group, Aeron subscribers (§5.2) |
| R1-5 | Sink drop → stale path was underspecified | `onSinkDrop` treated as a gap; stale notice published with spin semantics (§6.5) |
| R1-6 | `OffsetEpochNanoClock` calibrations differ across processes, which skews split-mode latency | Use `SystemEpochNanoClock` (host-wide clock) (§3.2) |
| R1-7 | MBO scope open | Interim: MBO passed through, BookManager is MBP (§12 Q4) |
| R1-8 | CME implied prices not handled | Separate `IMPLIED` book type (§6.3) |

**Round 2 (latency, implementability, edge behavior)**

| Id | Finding | Resolution |
|---|---|---|
| R2-1 | All-in-one always pays a ring-buffer hop | `inline` pipeline sub-mode (§7.1) |
| R2-2 | When a gated snapshot socket is not polled, the kernel queues old packets | `activate/deactivate`: leave/join the multicast group; drain for unicast and Aeron (§6.3) |
| R2-3 | A packet larger than the reorder slot had no defined handling | Count `oversize` and treat as lost on that line (§6.1) |
| R2-4 | Late arrivals after a declared gap were mixed into the duplicate count | Separate `late` counter as a `gapTimeout` tuning signal (§6.1) |
| R2-5 | No latency budget to measure against | §12 budget; the benchmark measures the code path separately from kernel receive |
| R2-6 | Euronext framing unverified, so a wrong length could misparse the rest of the packet | The decoder computes each message length from the schema (`blockLength` + group dimensions); an unknown template abandons the rest of the packet and counts `unknown` instead of guessing |
| R2-7 | Timers using `System.nanoTime()` on every check add cost | One cached nano clock read per duty cycle, shared by every channel in the agent |

## 14. Open questions

1. Which CME channels and Euronext segments come first? This sizes the instrument tables and reorder buffers.
2. Is the official CME `templates_FixBinary.xml` and Optiq schema version pinned, or should the build fetch the latest?
3. Is there PTP time sync on the target hosts? Wire-latency metrics depend on it.
4. Is MBO (CME MBOFD, Optiq order updates) in scope for the consumer `BookManager` in v1, or are MBO events only passed through? **Interim decision: v1 normalizes MBO events and passes them through. `BookManager` builds MBP books only, and an MBO book is an extension listener.**
5. Is the metrics backend Prometheus/Grafana self-hosted, or Grafana Cloud/Mimir? It only changes the collector exporter config.
