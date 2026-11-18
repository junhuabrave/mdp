# MDP Serving Layer and Crypto Feeds

Status: **v1.0 design**, after three review rounds (§9). Not implemented yet.
Companion to [DESIGN.md](../DESIGN.md), which covers ingestion (CME MDP 3.0, Euronext Optiq).

This document answers two questions:

1. How does a Market Data API (subscriptions, entitlements, history) sit on top of mdp **without being coupled to it**?
2. How does mdp ingest **crypto venues** (Binance, Coinbase) into the same normalized stream as CME and Euronext?

Short answers: the feed and the API share only a versioned wire contract, which is a normalized stream that behaves like an exchange feed, with incrementals, a snapshot loop and sequence numbers. Crypto venues fit behind the same contract, but **not** behind the current packet-oriented plug-in interface, which has to be split in two (§5.1).

---

## 1. Goals and non-goals

**Goals**
- **Complete decoupling** of ingestion and serving: separate processes, hosts, release cycles and failure domains. Neither side imports the other's code, and neither can slow the other down.
- One normalized contract across traditional (CME, Euronext) and digital-asset (Binance, Coinbase) venues.
- Snapshot-then-updates subscriptions per client, with correct gap and staleness semantics end to end.
- Entitlement checks, and a history query path.
- An MVP small enough to build and demo (§8).

**Non-goals (for this design)**
- Order entry, and private or authenticated crypto channels.
- Cross-venue consolidated books or a best-price feed: these are a consumer of the contract, not part of it.
- A full symbology and reference-data platform. Canonical symbols are mapped in the API tier (§4.5), backed by a static file at first.

---

## 2. Architecture

```
            INGESTION (mdp feed processes)                 │           SERVING (mdp-api processes)
                                                           │
 CME UDP A/B ──▶ gateway ─▶ normalizer ─┐                  │   ┌─────────────────────────────────────────┐
 Optiq UDP A/B ─▶ gateway ─▶ normalizer ─┤                  │   │ mdp-api instance (N, stateless restart) │
 Binance WS×2 + REST ─▶ crypto normalizer┤   contract:      │   │  stream subscriber (mdp-client)         │
 Coinbase WS×2 ───────▶ crypto normalizer┘   ─────────────  │   │   ├─ recovery: snapshot loop + seq      │
                         │                   incremental    ├──▶│   ├─ shards by instrument: book cache    │
                         │                   stream (seq'd) │   │   │   + subscriber lists                 │
                         │                   snapshot loop  │   │   ├─ entitlement check at subscribe      │
                         │                   stream         │   │   └─ client gateways: WS/JSON, gRPC,     │
                         │                   ref-data stream│   │        SBE-over-Aeron (internal)         │
                         ▼                                  │   └─────────────────────────────────────────┘
                  Aeron Archive recorder ─▶ tick store (ClickHouse/kdb+/Parquet) ◀── history API (mdp-api)
```

The vertical line is the decoupling boundary. Only **Aeron streams in the normalized SBE schema** cross it. Every arrow that crosses goes left to right: the serving side never calls the feed side.

---

## 3. The contract: a normalized stream that behaves like an exchange feed

The key decision (from R1-3): **mdp republishes its normalized data the way an exchange publishes market data.** Any consumer can join late, fall behind or restart, and recover on its own without asking the feed. This is exactly the snapshot-and-sequence pattern mdp already implements for CME, so consumers reuse the same algorithm, and the feed never needs a request/response channel.

### 3.1 Streams

| Stream | Content | Who reads it |
|---|---|---|
| `incremental` (one per venue normalizer) | `BookUpdate`, `Trade`, `InstrumentStatus`, `FeedEvent` | every consumer, always |
| `snapshot` (one per venue normalizer) | Full book image per instrument, looping (`SnapshotBegin`, book entries, `SnapshotEnd`) | consumers only while recovering (as mdp does with exchange snapshot lines) |
| `refdata` | `InstrumentDefinition` loop | consumers at startup and on unknown instruments |

Channels: `aeron:udp` multicast across hosts (production) or `aeron:ipc?tether=false` on one host (§3.4).

### 3.2 Sequencing (schema v2)

Today `instrumentSeq` carries the **venue's** sequence (CME RptSeq, Optiq-specific). Downstream it can be non-contiguous and differs in meaning per venue, so a consumer cannot detect a gap with it. Schema v2 adds:

- `streamSeq` (uint64): contiguous per stream, assigned by the normalizer at publication. Gap detection for the whole stream.
- `bookSeq` (uint64): contiguous per `(venueId, instrumentId)`, incremented per book-changing event. Snapshot entries carry the `bookSeq` they represent. A consumer applies incrementals with `bookSeq > snapshot.bookSeq`, which is CME's RptSeq rule, already implemented in `InstrumentStateTable`.

`instrumentSeq` stays as the venue's own number, for audit and debugging.

### 3.3 Other schema v2 changes (additive, SBE version bump)

| Change | Why |
|---|---|
| `sizeExponent` on `BookUpdate`, `Trade`, `InstrumentDefinition` | Crypto sizes are fractional (0.00012 BTC). Sizes become `mantissa × 10^sizeExponent`, like prices (R2-2) |
| `tickSize` and `lotSize` mantissas on `InstrumentDefinition` | Crypto tick and step sizes from `exchangeInfo` |
| `Symbol` stays 32 chars | Fits Binance and Coinbase symbols (`BTCUSDT`, `BTC-USD`) |
| New templates `SnapshotBegin` / `SnapshotEnd` (with `bookSeq`, entry count) | Delimit one instrument's image in the snapshot loop |
| Instrument key is always `(venueId, instrumentId)` | `instrumentId` is venue-local; ids collide across venues (R1-5) |

Crypto `instrumentId`: assigned by the crypto normalizer from a persisted symbol-to-id table, so ids are stable across restarts. They are published on `refdata`.

### 3.4 Neither side can slow the other (R1-2)

- **Slow API, feed unaffected.** A *tethered* Aeron IPC subscriber limits the publisher once the subscriber is a term-window behind, so a slow API would back-pressure the normalizer. API subscriptions therefore use `tether=false` (IPC) or UDP multicast with Aeron's default max-rate flow control. A consumer that falls too far behind is dropped by Aeron, sees a `streamSeq` gap on rejoin, and recovers from the snapshot loop. The feed never waits.
- **Slow feed, API unaffected.** The API serves whatever state it has. Feed silence or staleness reaches clients as status (§4.3), and the API never blocks on the feed.

### 3.5 Code dependencies (enforced)

```
mdp-schema  ◀── mdp-client ◀── mdp-api
    ▲
    └── mdp-core ◀── mdp-cme / mdp-euronext / mdp-crypto-* ◀── mdp-app (feed)
```

- `mdp-client` is a new, small consumer library containing the stream subscriber, sequence and gap tracking, snapshot-loop recovery, and the book builder. It depends only on `mdp-schema` and the Aeron client.
- `mdp-api` depends on `mdp-client`, **never** on `mdp-core`. This is enforced by a Gradle dependency check in CI, so decoupling is a build rule rather than a convention.
- `mdp-schema` is versioned and published as its own artifact.
- **Compatibility rules:** fields are add-only, consumers skip unknown templates, and a schema change ships to consumers before producers start emitting it.
- **Contract tests:** golden capture files of the normalized stream run in both feed and API CI.

---

## 4. Serving layer (mdp-api)

### 4.1 Shard model

- Instruments are hashed to **S shard threads**. Each shard owns its books (MBP top-N plus last trade and status), its per-instrument subscriber lists, and its subscription requests.
- **The stream subscriber thread** reads Aeron and routes each message to its shard's SPSC ring (Agrona `OneToOneRingBuffer`), so the hot path is lock-free.
- **Client I/O threads** (Netty) own sockets and per-client outbound queues. They never touch books.

### 4.2 Snapshot then updates, without a race

A subscribe request is executed **on the shard thread that owns the instrument**:

1. Check entitlements (from a cached decision, §4.4).
2. Serialize the current book with its `bookSeq = N` into the client's queue.
3. Add the client to the instrument's subscriber list.

Steps 2 and 3 happen between two incremental messages on the same thread, so no update can fall between the snapshot and the first delta. The client applies deltas with `bookSeq > N`. If the client sees a `bookSeq` gap, it re-subscribes.

### 4.3 Client protocol semantics

| Message | Meaning |
|---|---|
| `snapshot{instrument, bookSeq, levels, status}` | Replace the client's book. **May be sent at any time**, not only after subscribe (R3-2) |
| `update{instrument, bookSeq, changes}` | Apply the changes if `bookSeq` = last + 1 |
| `status{instrument, stale \| live, reason}` | Propagated from the feed's `BOOK_STALE` and `RECOVERY_COMPLETE` events, and from API-side stream gaps |
| `trade{...}` | Trades are not conflated; they are dropped only after the queue limit (§4.3.1) |

Encodings: JSON over WebSocket (UI and external), SBE over WebSocket or Aeron (internal, low latency), and gRPC server streaming (service-to-service). The semantics are the same in all three.

#### 4.3.1 Slow clients (R3-2)

- Every client has a bounded outbound queue.
- On overflow the client switches to **conflated mode**: pending book deltas for that instrument are discarded and replaced by **one fresh snapshot**, sent when the queue drains. Dropping individual deltas would corrupt the client's book, so conflation is always state-based.
- Trades are summarized: the client gets a `trades_dropped{count}` notice plus the last trade.
- If a client is in conflated mode for longer than T seconds, it is disconnected and a metric records it.
- None of this affects other clients or the shard thread. The shard only enqueues, and enqueue never blocks.

### 4.4 Entitlements

- **When:** checked at subscribe, never per message. The decision is keyed on `(user, venue, product class, data type {L1, L2, trades}, timeliness {realtime, delayed})`.
- **Source:** an entitlement service (or a static policy file in the MVP). Decisions are cached in the API.
- **Revocation:** pushed by the entitlement service. The API removes the affected subscriptions on the shard threads and sends `status{revoked}`.
- **Delayed data:** a separate delayed publisher inside the API, built from a time-shifted ring of the incremental stream with a 15-minute delay where exchanges require it. Delay is a property of the source, not a per-client filter.
- **Usage audit:** every subscribe, unsubscribe and revocation is logged per user, with timestamps, for exchange usage reporting. CME and Euronext require display and non-display reporting.

### 4.5 Symbology

- Clients subscribe by canonical symbol (`BTC/USD@COINBASE`, `ESZ6@CME`).
- The API maps canonical symbols to `(venueId, instrumentId)` using the `refdata` stream plus a mapping table.
- The feed stays venue-native and never learns canonical names.

### 4.6 History

- **Recording:** a standalone recorder process records the incremental and snapshot streams with Aeron Archive. It is just another untethered subscriber, so the feed is unaware of it.
- **Loading:** an ETL job loads recordings into a tick store: trades, top of book, and book deltas with periodic snapshots.
- **Query API (in mdp-api):** time-range queries for trades, BBO and bars; **book at time T** (nearest snapshot plus replay of `bookSeq` deltas); and **replay as a stream** (Aeron Archive replay into the same client protocol, so backtests use live client code).

### 4.7 Operations

- **Stateless restart:** an API instance restarts, joins the streams, recovers from the snapshot loop and accepts clients. Blue/green deploys of the API never touch the feeds, which demonstrates the decoupling.
- **Horizontal scale:** N API instances each subscribe independently (multicast makes the extra subscribers free on the feed side). Clients are load-balanced and sticky per connection.
- **Metrics** (reusing the mdp counter/OTel stack):
  - feed→API latency, from `receiveTimeNs` in every message;
  - API→client latency;
  - per-client queue depth and conflation count;
  - entitlement denials;
  - stream gaps and recoveries.

---

## 5. Crypto ingestion

### 5.1 The plug-in interface must be split (R2-1)

`FeedProtocol` today returns a `PacketHeaderReader` and a `FeedDecoder` whose methods take **sequenced packets**, and the core wraps them with UDP sources, `LineArbitrator` (A/B by packet sequence) and snapshot-line activation. Crypto venues break every one of those assumptions:

| | CME / Optiq | Binance spot | Coinbase |
|---|---|---|---|
| Transport | UDP multicast A/B | WebSocket (TLS/TCP) plus REST snapshot | WebSocket (TLS/TCP), snapshot in-stream |
| Sequence | per channel, per packet | per symbol: `U`..`u` update-id range per event | per connection (`sequence_num`) |
| Loss | packet loss, any time | only at disconnect/reconnect (TCP) | same |
| Book addressing | by level (top N) | by price, absolute quantity, `0` = delete | by price, absolute quantity |
| Encoding | SBE | JSON, or SBE on Binance's SBE market-data streams (to verify) | JSON |

So the claim "`FeedProtocol` is a natural fit" is only half right. The **sink, metrics, ServiceLoader discovery and normalized output** fit. The **transport, arbitration and recovery model** do not. The design:

```java
public interface FeedProtocol {            // unchanged identity and discovery
    String name();
    int venueId();
    FeedChannel newChannel(ChannelContext ctx);   // NEW: the protocol builds its own channel
}
public interface FeedChannel extends AutoCloseable {   // polled by the existing FeedHandlerAgent
    int doWork(long nowNs);
}
public abstract class PacketFeedProtocol implements FeedProtocol {   // CME, Optiq: no code change
    public abstract PacketHeaderReader packetHeader();
    public abstract FeedDecoder newDecoder(DecoderContext ctx);
    public final FeedChannel newChannel(ChannelContext ctx) { /* builds today's ChannelHandler */ }
}
```

`mdp-crypto-core` adds the session-oriented counterpart, shared by every crypto venue:
- `WsConnection`: Netty client over TLS, with ping/pong, reconnect with jittered backoff, and replay of subscriptions on reconnect.
- `SnapshotFetcher`: REST client with rate-limit budgeting (§5.5).
- `SymbolSync`: the per-symbol state machine `BUFFERING → SNAPSHOT_PENDING → LIVE → (gap) → BUFFERING`.
- `PriceBook`: a full-depth, price-keyed book that emits level-addressed top-N updates (§5.3).
- `SessionArbiter`: the redundancy strategy (§5.4).

### 5.2 Binance spot: depth sync

Binance's documented procedure (verify against current docs before implementing):

1. Subscribe to `<symbol>@depth@100ms` (diffs) and `<symbol>@trade`. Buffer diff events, noting the first event's `U`.
2. `GET /api/v3/depth?symbol=X&limit=1000`, which gives `lastUpdateId`.
3. If `lastUpdateId < firstBuffered.U`, the snapshot is too old: fetch it again.
4. Drop buffered events with `u <= lastUpdateId`.
5. The first applied event must satisfy `U <= lastUpdateId + 1 <= u`.
6. Then every event must have `U == previous.u + 1`. Otherwise the symbol has a gap: publish `BOOK_STALE` for that instrument only and restart at step 1.
7. Quantities are absolute; `0` removes the level.

Mapping to the contract:
- each applied diff becomes one event with one `bookSeq` increment;
- the venue `u` goes into `instrumentSeq`;
- the event time `E` goes into `exchangeTimeNs` (millisecond resolution, documented as such).

### 5.3 Book semantics (R2-3)

Downstream books (and `PriceLevelBook`) use CME semantics: level-addressed, top N. The crypto normalizer keeps the **full price-keyed book** internally and **emits level-addressed changes for the top N**, as CME does, including refills (`DELETE` level k, then `NEW` at level N when a deeper level moves up).

- **Pros:** consumers, the API and history see one book model; nothing downstream changes.
- **Cons:** full depth is not visible downstream, and the normalizer does extra work per update (finding a level index in a sorted array, O(log n)).

Full-depth crypto books can be added later as `BookType.PRICE` with price-keyed actions, as an opt-in. This decision is recorded as a trade-off, not hidden.

`PriceBook` implementation: two sorted primitive arrays (price mantissas, sizes) per side with binary search, sized for a 5,000-level snapshot. Updates are mostly near the top, so array shifts are short and allocation-free.

### 5.4 Redundancy (R2-4)

- **Binance:** update ids are global per symbol, so two connections (different network paths, or different source IPs or regions) can run **hot-hot**. A per-symbol dedupe keeps the first arrival by `u`. It is the same idea as A/B arbitration but keyed per symbol, with no reorder buffer, because TCP is in order. A connection that drops does not stale the book while the other is live.
- **Coinbase:** `sequence_num` is per connection, so the two connections cannot be merged. They run **hot-standby**: both connections are subscribed and each keeps its own synced book; the primary publishes. On primary failure or a gap, the arbiter switches to the standby. Its book is already synced, so the switch is one `RECOVERY_COMPLETE` with no stale period.

`SessionArbiter` has two implementations, `DedupeById` and `PrimaryStandby`, selected per venue.

### 5.5 Resync storms and rate limits (R2-5)

A reconnect of a connection carrying 300 symbols means 300 REST snapshots. Binance weights depth snapshots by size (`limit=1000` costs about 50, `5000` costs about 250) against a per-IP budget of about 6,000 per minute. Exceeding it brings HTTP 429 responses, and repeat offenders get 418 (banned). So:

- A **resync scheduler** with a token bucket fed by the `X-MBX-USED-WEIGHT-1M` response header. Symbols are prioritized (configured importance, then age of staleness). It backs off hard on 429 and stops on 418, alerting.
- Snapshot depth is the smallest that covers top N plus a margin (`limit=100` or `500`), not 5,000.
- While a symbol waits, it stays **stale**: published as `BOOK_STALE` and visible to clients. Failing safe is acceptable; getting banned is not.

### 5.6 Parsing and latency (R2-6)

- **Context:** the WAN dominates crypto latency, at milliseconds from venue to colocated cloud region (Binance is in AWS Tokyo, Coinbase in AWS US East). The code path is still kept allocation-free, for stable tails and so that one engine serves all venues.
- **Where Binance offers SBE market-data streams,** use them, with the same codegen as CME. Access requirements (API key type) are still to be verified.
- **JSON otherwise:** a hand-written, allocation-free scanner per message type over the frame's direct buffer (Netty pooled `ByteBuf` wrapped by `UnsafeBuffer`). Decimal strings are parsed straight into a mantissa at the instrument's fixed exponent, with no `BigDecimal` and no `double`.
- **Threads:** the Netty event-loop thread hands frames to the feed agent through an Agrona ring buffer, so the decoder keeps mdp's single-thread-per-channel model and the hot-thread allocation check still applies.

### 5.7 Testing without the venue (R2-7)

- **Recorder:** `.wsrec` files hold timestamped WebSocket frames and REST responses per connection. They are the crypto counterpart of pcap replay.
- **Fake venue:** a local WebSocket/REST server in tests that serves recordings or scripted scenarios:
  - an update-id gap;
  - a snapshot older than the first buffered event;
  - a disconnect in mid-sync;
  - 429 responses, then 418;
  - a standby takeover (Coinbase).
- **Determinism:** replaying the same recording twice must give identical normalized output, the same property already tested for CME captures.

---

## 6. Failure modes

| Failure | Detected by | Effect | Recovery |
|---|---|---|---|
| CME double-line loss | packet sequence gap | channel or instrument stale | exchange snapshot loop (existing) |
| Binance connection drop | WS close or ping timeout | none if the hot-hot peer is live; otherwise symbols stale | reconnect, resync via scheduler |
| Binance update-id gap | `U != prev.u + 1` | that symbol stale | resync that symbol |
| Coinbase sequence gap | `sequence_num` jump | switch to standby, no stale period | resubscribe the failed connection |
| REST rate limit | 429 / weight header | resyncs delayed, symbols stay stale | token bucket and backoff; alert on 418 |
| API falls behind the stream | Aeron drop, `streamSeq` gap on rejoin | affected instruments stale in the API | snapshot-loop stream, no feed involvement |
| API restart | — | clients disconnected | rejoin streams, recover, clients resubscribe |
| Slow client | outbound queue full | that client conflated | fresh snapshot; disconnect after T seconds |
| Normalizer restart | stream EOS / new session | all its instruments stale downstream | new session id, consumers re-snapshot |
| Entitlement revoked | push from entitlement service | subscription removed | — |

---

## 7. What changes in the existing code

| Area | Change | Risk |
|---|---|---|
| `mdp-schema` | v2: `streamSeq`, `bookSeq`, `sizeExponent`, tick/lot size, snapshot templates | Additive; existing tests need sizes at exponent 0 |
| `mdp-core` SPI | `FeedProtocol.newChannel`, plus `PacketFeedProtocol` base class for CME and Optiq | Mechanical refactor; all 30 tests must stay green |
| Normalizer | Assign `streamSeq` and `bookSeq`; publish the snapshot-loop and refdata streams | New code on the normalizer thread; the snapshot loop runs on its own agent, from a book copy |
| New modules | `mdp-client`, `mdp-api`, `mdp-crypto-core`, `mdp-binance`, `mdp-coinbase` | — |
| Build | Gradle rule: `mdp-api` must not depend on `mdp-core` | — |

---

## 8. Delivery plan (MVP first, R3-1)

| Step | Scope | Done when |
|---|---|---|
| 1 | Schema v2, plus `streamSeq`/`bookSeq` and the snapshot-loop stream in the normalizer; `mdp-client` with recovery | A consumer started mid-stream recovers to identical books (CME capture replay) |
| 2 | SPI split; `mdp-crypto-core` and `mdp-binance` (depth and trades, about 5 symbols, single connection) | Live Binance books match `GET /depth` at random checkpoints; fake-venue tests for each §5.2 failure case |
| 3 | `mdp-api` MVP: one shard, WebSocket JSON, snapshot-then-updates, conflation, static entitlement file | Demo: one client subscribes to `ESZ6@CME` and `BTCUSDT@BINANCE` in the same session; killing and restarting the API does not disturb the feed |
| 4 | Binance hot-hot, resync scheduler | Kill one connection under load: no stale period |
| 5 | `mdp-coinbase` with primary/standby | Proves the crypto SPI generalizes to a second venue |
| 6 | History: Archive recorder, tick-store ETL, query endpoints | Book at time T matches the replay |

Steps 1–3 are the demoable slice: **one codebase that ingests CME, Euronext and crypto into one contract, and an independently deployable API that serves all of them.**

---

## 9. Review log

### Strawman v0.1 (before review)

- The API ran as a `BookListener` inside the mdp-app consumer process, reading books directly from `BookManager`.
- `mdp-binance` implemented `FeedProtocol` directly: frames as "packets", Binance `u` as the packet sequence, `LineArbitrator` for two connections.
- `instrumentSeq` was used for client sequencing; sizes were integers; one API process.

### Round 1: decoupling and the contract

| Id | Finding | Resolution |
|---|---|---|
| R1-1 | An API inside the consumer process shares a JVM, a release cycle and a failure domain with the feed. A GC pause or a bug in client handling hits ingestion | API is a separate service; depends only on `mdp-schema` + `mdp-client`; enforced by a build rule (§3.5) |
| R1-2 | Normalizer output is `aeron:ipc` and tethered by default. A slow API back-pressures the normalizer once a term-window behind | API subscribes untethered or over multicast; falling behind means loss plus self-recovery, never feed back-pressure (§3.4) |
| R1-3 | A late-joining or restarted API has no way to rebuild state except asking the feed, which is coupling through request/response | The feed publishes a snapshot loop and refdata loop like an exchange; consumers reuse mdp's own recovery algorithm (§3.1) |
| R1-4 | `instrumentSeq` is the venue's number and not contiguous in the normalized stream, so gaps are undetectable downstream | Add `streamSeq` (per stream) and `bookSeq` (per book) at publication (§3.2) |
| R1-5 | `instrumentId` is venue-local and collides across venues | Key is `(venueId, instrumentId)` everywhere; canonical symbols mapped in the API (§4.5) |
| R1-6 | Independent release cycles need explicit schema compatibility | Add-only fields, skip unknown templates, consumers upgrade first, golden-capture contract tests in both CIs (§3.5) |

### Round 2: crypto fit

| Id | Finding | Resolution |
|---|---|---|
| R2-1 | Crypto breaks the plug-in interface's assumptions: TCP not UDP, per-symbol or per-connection sequences, REST snapshots, loss only on reconnect. Forcing frames through `LineArbitrator` would need one 1.5 MB reorder buffer per symbol to solve a reordering problem TCP does not have | Split the interface: `FeedProtocol.newChannel`, `PacketFeedProtocol` for UDP venues, session-based `mdp-crypto-core` (§5.1) |
| R2-2 | Fractional crypto sizes do not fit integer `size` | `sizeExponent` in schema v2; decimals parsed straight into mantissas (§3.3, §5.6) |
| R2-3 | Crypto books are price-keyed and full-depth; downstream books are level-addressed top N | The normalizer keeps the full book and emits CME-style level changes; full depth deferred as an opt-in `BookType.PRICE` (§5.3) |
| R2-4 | One A/B model does not fit both venues: Binance ids are global per symbol, Coinbase sequences are per connection | `SessionArbiter`: dedupe-by-id (hot-hot) or primary/standby (§5.4) |
| R2-5 | A reconnect resyncs every symbol at once and can exceed REST weight limits, leading to a ban | Resync scheduler with token bucket, priorities, minimal snapshot depth; stale while waiting (§5.5) |
| R2-6 | JSON and WebSocket libraries allocate; latency expectations need to be stated honestly | Pooled direct buffers handed to the feed agent; allocation-free scanner; SBE streams where available; WAN-dominated latency stated (§5.6) |
| R2-7 | Venue-dependent tests are flaky and cannot reproduce edge cases | `.wsrec` recorder, scripted fake venue, determinism test (§5.7) |

### Round 3: correctness at the edges, scope, operations

| Id | Finding | Resolution |
|---|---|---|
| R3-1 | The design spans an API, two venues, history and entitlements, which is too much to deliver as one piece | Staged plan; steps 1–3 are the demoable slice (§8) |
| R3-2 | "Conflate slow clients" by dropping deltas silently corrupts level-addressed books | Conflation is state-based: discard pending deltas, send a fresh snapshot; `snapshot` may arrive at any time in the protocol (§4.3.1) |
| R3-3 | A full-book snapshot loop for thousands of instruments costs bandwidth continuously | A separate stream, rate-limited, read only while recovering, mirroring exchange snapshot lines (§3.1) |
| R3-4 | Timestamps mean different things per venue (CME ns vs crypto ms, exchange vs receive clock) | Both fields kept; resolution documented per venue; history keyed by receive time with exchange time as a column (§5.2, §4.6) |
| R3-5 | Decoupling hides end-to-end latency across processes | `receiveTimeNs` carried in every message; per-hop latency in the API; same OTel stack (§4.7) |
| R3-6 | Normalizer restart semantics were undefined for downstream consumers | Restart starts a new stream session; consumers treat it as a full re-snapshot (§6) |

---

## 10. Open questions

- **Binance SBE market-data streams:** access requirements and coverage (depth diffs vs book ticker only) are still to be confirmed. If they cover depth, use them instead of JSON.
- **Coinbase product:** Advanced Trade WebSocket or Exchange WebSocket (institutional, different auth and channels). Galaxy's flow likely points to Exchange or Prime: decide before step 5.
- **Tick store:** ClickHouse (simple to operate, SQL) or kdb+ (industry standard for ticks, licensed) or Parquet on object storage (cheap, batch). This depends on the query latency needed for history.
- **Snapshot loop source:** a book copy on a separate agent, or replay from Archive. The first is simpler; the second removes snapshot work from the normalizer host.
