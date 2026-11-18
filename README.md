# MDP: Low-Latency Market Data Feed Handler

[![CI](https://github.com/junhuabrave/mdp/actions/workflows/ci.yml/badge.svg)](https://github.com/junhuabrave/mdp/actions/workflows/ci.yml)

Java 21 feed handler for SBE exchange market data: **CME MDP 3.0** and **Euronext Optiq MDG**. It is built on **Aeron**, **SBE** and **Agrona**, with OpenTelemetry → Prometheus → Grafana observability.

- **Zero allocation on the hot path**, measured with zero bytes over 4M packets (see [latency profile](#latency-profile)).
- **A/B line arbitration**: first arrival wins, a reorder buffer holds early packets, gaps are declared after a timeout, and sequence resets are handled with epochs.
- **Data resilience**: per-instrument (CME RptSeq) and channel-level gap detection, snapshot recovery, and explicit `BOOK_STALE` / `RECOVERY_COMPLETE` events downstream.
- **One codebase, two deployment shapes**: all-in-one JVM, or split gateway → normalizer → consumers over Aeron.
- **Pluggable venues** through a `FeedProtocol` SPI. CME and Optiq are two modules; add yours the same way.

Architecture, the resilience model and the review log are in [DESIGN.md](DESIGN.md).
The serving layer (subscription API, entitlements, history) and crypto venues are designed in [docs/DESIGN-API-CRYPTO.md](docs/DESIGN-API-CRYPTO.md).

```
 UDP A ─┐                                    ┌─ ring buffer ─▶ BookManager ─▶ your listener     (all-in-one)
 UDP B ─┼─▶ LineArbitrator ─▶ FeedDecoder ──▶┤
 Snap  ─┘   dedupe/reorder/    (CME | Optiq)  └─ Aeron ──────▶ consumers in other processes   (split)
            gap detect         recovery
                     counters (Aeron CnC) ──▶ OTel exporter / observer sidecar ──▶ Collector ─▶ Prometheus ─▶ Grafana
```

## Modules

| Module | Contents |
|---|---|
| `mdp-schema` | Normalized SBE schema (`BookUpdate`, `Trade`, `InstrumentStatus`, `InstrumentDefinition`, `FeedEvent`) |
| `mdp-core` | Transports (UDP multicast/unicast, Aeron, pcap/pcapng replay, in-memory), `LineArbitrator`, recovery helpers, sinks (ring buffer, Aeron, inline), `BookManager`, agents, metrics |
| `mdp-cme` | CME MDP 3.0 schema subset, `CmeFeedDecoder` (RptSeq recovery), `CmePacketWriter` |
| `mdp-euronext` | Optiq MDG schema subset, `OptiqFeedDecoder` (snapshot and buffered-replay recovery), `OptiqPacketWriter` |
| `mdp-otel` | `OtelCountersExporter`: counters, gauges and latency percentiles over OTLP, plus JVM runtime metrics |
| `mdp-app` | Launcher for every role, `FeedSimulator` (loss injection), `LatencyBenchmark` |
| `config/` | Example properties for each role |
| `deploy/` | docker-compose: OTel Collector, Prometheus (with alert rules), Grafana (provisioned dashboard) |

## Quickstart

Requires JDK 21.

```bash
./gradlew build                      # SBE codegen, compile, 30 tests (incl. end-to-end UDP and capture replay)
./gradlew :mdp-app:installDist       # launcher at mdp-app/build/install/mdp-app/bin/mdp-app
```

### 1. Observability stack

```bash
docker compose -f deploy/docker-compose.yml up -d     # or: podman-compose -f deploy/docker-compose.yml up -d
```

Grafana is at http://localhost:3000, under Dashboards → Market Data → **MDP Feed Handler**. Prometheus is at http://localhost:9090, under Alerts.

### 2. All-in-one handler plus simulated CME feed

Run the handler and the simulator in separate terminals:

```bash
mdp-app/build/install/mdp-app/bin/mdp-app config/all-in-one-cme.properties
```

```bash
mdp-app/build/install/mdp-app/bin/mdp-app simulate config/simulator-cme.properties
```

The simulator sends 20k packets/s across 50 instruments. It drops 1% of packets independently on line A and on line B; arbitration fills each loss from the other line. It also forces 0.002% of packets to drop on both lines. Real gaps come from those forced drops plus the 0.01% of packets the two independent 1% losses happen to share. Each gap causes snapshot recovery.

In a 45 s soak against the stack (about 800k packets), 18k single-line losses were all filled from the other line. 121 packets were lost on both lines, producing 117 channel gaps, every one recovered from the snapshot loop within 465 ms. Late arrivals were 0, so the gap timeout never fired too early. Both behaviors show on the dashboard. For Euronext, use `all-in-one-optiq.properties` and `simulator-optiq.properties`.

### 3. Split deployment

Run each component in its own terminal, in this order:

```bash
mdp-app/build/install/mdp-app/bin/mdp-app config/media-driver.properties   # standalone Aeron driver
```

```bash
mdp-app/build/install/mdp-app/bin/mdp-app config/consumer.properties       # strategy side (run N of these)
```

```bash
mdp-app/build/install/mdp-app/bin/mdp-app config/normalizer-cme.properties # decode + recover
```

```bash
mdp-app/build/install/mdp-app/bin/mdp-app config/gateway-cme.properties    # A/B lines -> Aeron
```

```bash
mdp-app/build/install/mdp-app/bin/mdp-app config/observer.properties       # metrics sidecar (reads Aeron CnC)
```

```bash
mdp-app/build/install/mdp-app/bin/mdp-app simulate config/simulator-cme.properties
```

In split mode, the hot processes run no exporter at all. Their counters live in the media driver's shared memory, and the `observer` sidecar exports them, together with Aeron's own driver counters.

### 4. Replay a capture

Any line URI can be a capture file instead of a socket: `pcap:<file>?dst=<group>:<port>&speed=1|N|max`. Lines of one channel that name the same file replay in captured order, so A/B arbitration and snapshot recovery behave as they did on the wire. Classic pcap and pcapng are both supported, with Ethernet (including VLAN tags), Linux cooked or raw IP framing.

Record the simulator's feed (after loss injection) instead of sending it:

```bash
JAVA_OPTS="-Dsim.pcap.out=captures/cme-310.pcap -Dsim.pcap.only=true -Dsim.durationSeconds=30" mdp-app/build/install/mdp-app/bin/mdp-app simulate config/simulator-cme.properties
```

Then replay it, or a real capture with CME's group:port per line:

```bash
mdp-app/build/install/mdp-app/bin/mdp-app config/replay-cme.properties
```

When the file ends, the log prints a summary of packets delivered per line and packets skipped (unmatched, non-UDP, fragments, truncated). Use `speed=max` for fast regression runs. `DeploymentModesTest` replays a lossy capture twice and checks the books come out identical.

### 5. Benchmark

```bash
mdp-app/build/install/mdp-app/bin/mdp-app benchmark --rate 100000 --seconds 10 --warmup 5
```

## Choosing a deployment

| | All-in-one `ringbuffer` | All-in-one `inline` | Split (gateway / normalizer / consumers) |
|---|---|---|---|
| Hops | decoder → ring → consumer thread | none | gateway → Aeron → normalizer → Aeron → N consumers |
| Median latency (measured, M2) | ~210–250 ns | ~125 ns | ~250 ns per Aeron IPC hop |
| Isolation | slow listener does not block decoding | slow listener blocks socket reads | full process isolation, independent restarts |
| Fan-out | one consumer thread per ring | single | any number of processes / hosts / languages |
| Use when | one strategy process, latency critical | trivial listener, absolute minimum latency | many consumers, colocated gateways, recording |

## Latency profile

Measured on an Apple M2 laptop (4 performance + 4 efficiency cores), macOS, JDK 21 with ZGC, at 100k packets/s. Each run: 1M measured packets after 0.5M warm-up packets. Packets are CME `MDIncrementalRefreshBook46` with one entry, spread over 50 instruments. Every scenario runs the real arbitrator, SBE decoder, normalized encoder, transport and `BookManager`. Values are in nanoseconds, with 42 ns clock resolution (Apple Silicon timebase).

| Path | p50 | p90 | p99 | p99.9 | max |
|---|---|---|---|---|---|
| inline: receive → listener | **125** | 125 | 375 | 1,959 | 5.9 ms |
| ring buffer: receive → listener | **209–250** | 250–8,127 | 6 µs – 892 µs | 1–29 ms | 8–45 ms |
| ring buffer: receive → committed to ring | 166 | 375 | 1,333 | 12,375 | 4.4 ms |
| Aeron IPC: receive → listener | **250** | 333–417 | 16–174 µs | 0.24–2.4 ms | 2.6–19 ms |
| UDP loopback: receive → listener | 292 | 583 | 17,919 | 156 µs | 34 ms |
| UDP loopback: send → receive (kernel) | **8,043** | 10,839 | 104,767 | 40 ms | 49 ms |
| UDP loopback: send → listener | 8,375 | 12,751 | 137,727 | 40 ms | 49 ms |

- **Throughput:** 3.36M packets/s sustained through the ring-buffer pipeline on one channel, decoder thread unpaced.
- **Allocation:** 0 bytes on hot threads after warm-up, in every scenario (4M packets measured with `ThreadMXBean`).

How to read this:

- **The code path is about 125–300 ns at the median.** That covers arbitration, SBE decode, normalized encode, the hand-off and the book update. It is within the design budget (< 1 µs p50).
- **The kernel dominates.** A UDP receive through the macOS loopback costs about 8 µs median, roughly 30× the code path. On Linux production hardware, kernel bypass (Onload/ef_vi, DPDK) through a new `PacketSource` is the next big lever.
- **The tails come from the environment, not the code.** Ring-buffer p99 ranged from 6 µs to 892 µs across three identical runs, while p50 stayed at 209–250 ns. macOS cannot pin threads or isolate cores, and busy-spinning threads get moved onto efficiency cores or preempted. Inline mode, with only one busy thread, has the tightest tail. The design target (< 5 µs p99) needs a tuned Linux host: `isolcpus`/`nohz_full`, threads pinned with `taskset`, the NIC IRQs moved off those cores, and C-states disabled. That host has not been measured yet.

## Extending

**New venue.** Create `mdp-<venue>` with its SBE XML in `src/main/sbe/` (the build generates codecs). Implement `PacketHeaderReader`, `FeedDecoder` and `FeedProtocol`, and list the class in `META-INF/services/io.mdp.core.api.FeedProtocol`. Select it with `mdp.protocol=<name>`. The arbitrator, sinks, books, metrics and deployment modes come for free. `InstrumentStateTable` (per-instrument sequencing) and `PacketReplayBuffer` (snapshot plus replay) cover both common recovery styles.

**Consumer logic.** Implement `BookListener` (books with stale state) or `MarketDataListener` (raw normalized flyweights).

**Transport.** Implement `PacketSource`, for example for kernel bypass. `PcapPacketSource` is a compact example.

## Production notes

- **Schemas:** `mdp-cme` and `mdp-euronext` ship *trimmed* schemas for development. Replace them with the official CME `templates_FixBinary.xml` and the Optiq MDG schema, then re-verify against certification PCAPs. Some Optiq template ids and framing in this repo are illustrative (see the schema header comments).
- **Clock:** `mdp.clock=system` (the default on Linux) is comparable across processes. On macOS the default is `offset`, because the realtime clock there has only microsecond resolution.
- **JVM:** see `applicationDefaultJvmArgs` in `mdp-app/build.gradle.kts` (ZGC, pre-touch, bounds checks off, `--add-opens`).
