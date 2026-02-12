# Backfill Feature Design

## 1. Overview

Backfill is a resident background process that fills historical trade data gaps in the database. It periodically checks data completeness for configured markets and timeframes, detects gaps, and fetches missing data from exchange REST APIs. The fetched trades are aggregated into bars and written to the database, using the same bar schema as real-time aggregation but through an isolated write path to avoid conflicts.

## 2. Configuration

New config fields added to `config.js` `defaultConfig`:

```js
// enable backfill background process
backfill: false,

// backfill start time - how far back to ensure data completeness
// ISO 8601 string or ms timestamp
// e.g. "2024-01-01T00:00:00Z" or 1704067200000
backfillStartTime: null,

// timeframes to ensure completeness for (ms)
// only these values are allowed: 60000, 300000, 900000, 3600000, 14400000, 43200000, 86400000
// which correspond to: 1m, 5m, 15m, 1h, 4h, 12h, 1d
backfillTimeframes: [
  60000,        // 1m
  300000,       // 5m
  900000,       // 15m
  3600000,      // 1h
  14400000,     // 4h
  43200000,     // 12h
  86400000      // 1d
],

// interval between completeness checks (ms, default 10s)
backfillCheckInterval: 10000,

// markets to backfill (defaults to config.pairs if empty)
// allows backfilling a subset of tracked markets
backfillPairs: [],

// max concurrent exchange API requests across all backfill tasks
backfillMaxConcurrency: 1,

// delay between consecutive REST API calls per exchange (ms)
// respects exchange rate limits
backfillRequestDelay: 500,
```

### Config example (`config.json`)

```json
{
  "backfill": true,
  "backfillStartTime": "2024-06-01T00:00:00Z",
  "backfillTimeframes": [60000, 300000, 3600000, 86400000],
  "backfillCheckInterval": 10000,
  "backfillPairs": ["BINANCE:btcusdt", "BINANCE_FUTURES:btcusdt"],
  "backfillMaxConcurrency": 1,
  "backfillRequestDelay": 500
}
```

### Validation rules

- `backfillTimeframes` values must be a subset of `[60000, 300000, 900000, 3600000, 14400000, 43200000, 86400000]`. Invalid values are rejected at startup.
- `backfillStartTime` is required when `backfill` is `true`. Must parse to a valid date in the past.
- Each timeframe in `backfillTimeframes` must also exist in `influxResampleTo` (or be derivable from the base timeframe). At config validation, ensure every backfill timeframe is present in the storage's supported timeframe list.

## 3. Architecture

```
                          ┌─────────────────────────────────┐
                          │         BackfillService         │
                          │  (src/services/backfill.js)     │
                          ├─────────────────────────────────┤
                          │  - checkInterval timer (10s)    │
                          │  - gapDetector                  │
                          │  - gapFiller                    │
                          └────────┬───────────┬────────────┘
                                   │           │
                    ┌──────────────┘           └──────────────┐
                    ▼                                         ▼
          ┌─────────────────┐                    ┌────────────────────┐
          │   Gap Detector  │                    │     Gap Filler     │
          │ (completeness   │                    │ (fetch + aggregate │
          │  check via DB)  │                    │  + write to DB)    │
          └────────┬────────┘                    └─────────┬──────────┘
                   │                                       │
                   ▼                                       ▼
          ┌─────────────────┐                    ┌────────────────────┐
          │   TimescaleDB   │                    │  Exchange REST API │
          │   / InfluxDB    │                    │  (getMissingTrades)│
          │   (query only)  │                    │                    │
          └─────────────────┘                    └────────────────────┘
```

### Component relationships

- **BackfillService** is instantiated in `server.js` after storages are initialized, only if `config.backfill === true`.
- It receives references to `storages` and `exchanges` (same instances used by the real-time pipeline).
- It does NOT inject trades into `server.chunk[]` (the real-time buffer). Instead, it writes aggregated bars directly to the storage layer, bypassing the real-time pipeline entirely.

## 4. Detailed Design

### 4.1 Gap Detector — Completeness Check

The gap detector answers: "For a given market + timeframe, which time buckets between `backfillStartTime` and `now` have no bar data?"

#### Algorithm: Bucket-count approach

For each `(market, timeframe)` pair, query the database for the count of existing bars within a scan window, then compare against the expected count.

**TimescaleDB query:**

```sql
SELECT
  time_bucket($1::interval, bucket) AS scan_bucket,
  COUNT(*) AS bar_count
FROM aggr_bars
WHERE timeframe_ms = $2
  AND market = $3
  AND bucket >= to_timestamp($4 / 1000.0)
  AND bucket < to_timestamp($5 / 1000.0)
GROUP BY 1
ORDER BY 1 ASC
```

The scan window is chunked to limit query scope. For example, for 1m bars, scan in 24h chunks. For 1h bars, scan in 30d chunks.

**Scan chunk sizes by timeframe (to limit query cost):**

| Timeframe | Scan chunk          | Expected bars per chunk |
|-----------|---------------------|-------------------------|
| 1m        | 24h (86400000 ms)   | 1440                    |
| 5m        | 7d                  | 2016                    |
| 15m       | 7d                  | 672                     |
| 1h        | 30d                 | 720                     |
| 4h        | 90d                 | 540                     |
| 12h       | 180d                | 360                     |
| 1d        | 365d                | 365                     |

**Gap identification:**

```
expected_buckets = generate all bucket timestamps in [chunk_start, chunk_end) at the given timeframe
existing_buckets = set of bucket timestamps returned by the query
missing_buckets  = expected_buckets - existing_buckets
```

Missing buckets are coalesced into contiguous gap ranges: `{ from, to }`.

#### Performance considerations

1. **Progressive scanning**: Don't scan the entire `[backfillStartTime, now)` range every 10s. Maintain a **scan cursor** per `(market, timeframe)` that advances forward. One full pass from `backfillStartTime` to `now` may take many cycles. Once the cursor reaches `now`, it resets to `backfillStartTime` and starts the next scan pass.

2. **One chunk per cycle**: Each 10s cycle processes at most **one scan chunk** per `(market, timeframe)`. This bounds the DB query load to a predictable level.

3. **Priority queue**: Gap ranges discovered during scanning are placed into a priority queue sorted by:
   - Priority 1: Smaller timeframes first (1m before 1h) — more granular data unblocks higher-timeframe aggregation.
   - Priority 2: Older gaps first (earlier `from` timestamp).

4. **Recent-data exclusion zone**: Skip checking the most recent `influxResampleInterval * 2` window (default 2 minutes). This avoids false-positive gaps caused by the real-time pipeline's write latency. The real-time pipeline handles this window.

### 4.2 Gap Filler — Backfill Execution

Once gaps are detected, the filler fetches trade data from exchange REST APIs and writes bars to the database.

#### Key principle: backfill writes base timeframe bars, then resamples up

Backfill always works at the **base timeframe** (`config.influxTimeframe`, default 10s), regardless of which higher timeframe had the detected gap. This is because higher timeframes are derived from base timeframe bars via resample. The flow is:

```
1. Gap detected at, e.g., 1h timeframe for BINANCE:btcusdt
   → Gap range: [2024-06-01T12:00:00Z, 2024-06-01T13:00:00Z)

2. Check if base timeframe (10s) bars exist for the same range
   → Query: SELECT COUNT(*) FROM aggr_bars
            WHERE timeframe_ms = 10000
              AND market = 'BINANCE:btcusdt'
              AND bucket >= '2024-06-01T12:00:00Z'
              AND bucket < '2024-06-01T13:00:00Z'
   → Expected: 360 bars (3600s / 10s). If significantly fewer, need to fill.

3. Fetch raw trades via exchange REST API (getMissingTrades pattern)
   → BINANCE: GET /api/v3/aggTrades?symbol=BTCUSDT&startTime=...&endTime=...

4. Aggregate fetched trades into base timeframe bars (same logic as processTrades)

5. Write base timeframe bars to DB via upsertBars (ON CONFLICT DO UPDATE)

6. Resample base bars into higher timeframes for the affected range
   → reuse existing resampleRangeToTimeframe() / resample() logic
```

#### Isolated write path (avoiding conflicts with real-time)

The critical design constraint: backfill must not conflict with the real-time aggregation pipeline.

**Conflict scenarios and mitigations:**

| Scenario | Risk | Mitigation |
|----------|------|------------|
| Backfill writes to a bar bucket that real-time is also writing to | Data overwrite / race condition | Backfill excludes the recent window: never writes to bars within `now - (influxResampleInterval * 3)`. The real-time pipeline owns the "hot" zone. |
| Backfill resample overwrites a higher-TF bar that real-time just resampled | Stale data overwrites fresh data | Same time exclusion. Backfill only resamples bars older than the exclusion zone. |
| Backfill and real-time both call `upsertBars` simultaneously for different time ranges | DB lock contention | Acceptable — PostgreSQL handles concurrent upserts to different rows. No functional conflict, only minor lock contention on the hypertable. |
| Backfill floods the DB connection pool | Real-time writes starve | Backfill uses a **separate connection pool** (or a dedicated subset of the existing pool, e.g., `max: 2`). |

**Exclusion zone:**

```
backfill_write_cutoff = now - (config.influxResampleInterval * 3)

// Backfill will NEVER write bars with bucket >= backfill_write_cutoff
// This zone is owned exclusively by the real-time pipeline
```

The `* 3` factor provides a safety margin beyond the real-time resample interval (default 60s), giving a 3-minute buffer.

#### Backfill execution flow

```
for each gap in priority_queue:
  if gap.to > backfill_write_cutoff:
    gap.to = backfill_write_cutoff   // truncate to safe zone
    if gap.from >= gap.to:
      skip                            // gap is entirely in the hot zone

  exchange = findExchangeForMarket(gap.market)

  if !exchange.getMissingTrades:
    log warning, skip                 // exchange doesn't support REST recovery

  // Fetch trades in small time slices (e.g., 1h per request, same as existing recovery)
  for each slice in gap [from, to) stepped by 1h:
    trades = await exchange.getMissingTrades({ pair, from: slice.start, to: slice.end })

    bars = aggregateToBaseBars(trades, config.influxTimeframe)

    await storage.upsertBars(config.influxTimeframe, market, bars)

    await storage.resampleToHigherTimeframes(market, slice.start, slice.end)

    await sleep(config.backfillRequestDelay)  // rate limit
```

#### Aggregation function (`aggregateToBaseBars`)

This is a standalone function (not reusing `processTrades` which mutates `pendingBars` state). It takes an array of trade objects and produces an array of bar objects:

```js
function aggregateToBaseBars(trades, baseTimeframe) {
  const barsMap = {}

  for (const trade of trades) {
    const bucketTime = Math.floor(trade.timestamp / baseTimeframe) * baseTimeframe

    if (!barsMap[bucketTime]) {
      barsMap[bucketTime] = {
        time: bucketTime,
        market: trade.exchange + ':' + trade.pair,
        open: null, high: null, low: null, close: null,
        cbuy: 0, csell: 0, vbuy: 0, vsell: 0, lbuy: 0, lsell: 0
      }
    }

    const bar = barsMap[bucketTime]
    const side = trade.side === 'buy' ? 'buy' : 'sell'

    if (trade.liquidation) {
      bar['l' + side] += trade.price * trade.size
    } else {
      if (bar.open === null) {
        bar.open = bar.high = bar.low = bar.close = +trade.price
      } else {
        bar.high = Math.max(bar.high, +trade.price)
        bar.low  = Math.min(bar.low, +trade.price)
        bar.close = +trade.price
      }
      bar['c' + side] += trade.count || 1
      bar['v' + side] += trade.price * trade.size
    }
  }

  return Object.values(barsMap).sort((a, b) => a.time - b.time)
}
```

This is functionally identical to the existing `processTrades()` logic in `timescaledb.js` / `influx.js`, but operates on a local variable (no shared state with the real-time pipeline).

### 4.3 BackfillService Lifecycle

#### Startup

```
1. server.js: after initStorages() resolves
2. if config.backfill && config.backfillStartTime:
     validate backfillTimeframes against storage's supported timeframes
     backfillService = new BackfillService(storages, exchanges)
     backfillService.start()
```

#### Main loop (every `backfillCheckInterval`)

```
async tick():
  // Phase 1: Scan one chunk for gaps
  scanResult = gapDetector.scanNextChunk()
  if scanResult.gaps.length:
    gapQueue.enqueue(scanResult.gaps)

  // Phase 2: Fill one gap (or continue filling current gap)
  if gapQueue.length && !currentlyFilling:
    gap = gapQueue.dequeue()
    await gapFiller.fill(gap)
```

The scan and fill phases run **sequentially within the same tick**, but the fill operation is designed to be interruptible — it processes one time slice per tick if the gap is large, then resumes in the next tick.

#### Shutdown

On SIGINT, `backfillService.stop()` is called:
- Clears the interval timer
- Waits for any in-flight REST request / DB write to complete
- Does NOT flush any in-memory state (backfill has no in-memory buffer; it writes directly to DB)

### 4.4 State Persistence

To avoid re-scanning from `backfillStartTime` on every restart, persist the scan cursor:

**File: `products/backfill-state.json`**

```json
{
  "cursors": {
    "BINANCE:btcusdt": {
      "60000": 1717286400000,
      "3600000": 1717286400000
    },
    "BINANCE_FUTURES:btcusdt": {
      "60000": 1717200000000,
      "3600000": 1717200000000
    }
  },
  "updatedAt": 1717372800000
}
```

On startup, the scan cursor resumes from the persisted position. If the file doesn't exist or is stale (> 24h old), scanning restarts from `backfillStartTime`.

### 4.5 InfluxDB Considerations

The design above focuses on TimescaleDB (`upsertBars`, `resampleRangeToTimeframe`). For InfluxDB:

- Backfill writes use `writePoints()` with the base retention policy, same as `importPendingBars()`.
- Resample uses the existing `resample()` method with a specific `range`.
- InfluxDB's `INSERT` is naturally idempotent (same timestamp + tags = overwrite), so `ON CONFLICT` semantics are implicit.
- The exclusion zone logic remains the same.

## 5. Storage Layer Changes

Backfill requires new interfaces on both storage backends (gap detection queries, isolated write access) and a new method on the Exchange base class (fetching trades without emitting into the real-time pipeline).

### 5.1 Current Storage Interfaces (Before)

**TimescaleDB (`src/storage/timescaledb.js`) — existing public methods:**

| Method | Signature | Used by |
|--------|-----------|---------|
| `connect()` | `() → Promise<void>` | server init |
| `save()` | `(trades: Trade[], isExiting: boolean) → Promise<void>` | real-time pipeline (`backupTrades`) |
| `processTrades()` | `(trades: Trade[]) → Promise<void>` | `save()` internal — mutates `this.pendingBars` |
| `flush()` | `(isExiting: boolean) → Promise<void>` | `save()` internal — writes `pendingBars` to DB |
| `upsertBars()` | `(timeframe: number, market: string, bars: Bar[]) → Promise<{fromTsWritten, toTsWritten}>` | `flush()` internal |
| `resampleToHigherTimeframes()` | `(market: string, fromTs: number, toTs: number) → Promise<void>` | `flush()` internal |
| `resampleRangeToTimeframe()` | `(market: string, fromTs: number, toTs: number, targetTimeframe: number) → Promise<void>` | `resampleToHigherTimeframes()` internal |
| `fetch()` | `({from, to, timeframe, markets}) → Promise<output>` | HTTP API (`/historical`) |
| `getPendingBars()` | `(markets: string[], from: number, to: number) → Bar[]` | `fetch()` internal |
| `getPersistedBar()` | `(market: string, timestamp: number) → Promise<Bar\|null>` | `processTrades()` internal |

**InfluxDB (`src/storage/influx.js`) — existing public methods:**

| Method | Signature | Used by |
|--------|-----------|---------|
| `connect()` | `() → Promise<void>` | server init |
| `save()` | `(trades: Trade[], isExiting: boolean) → Promise<void>` | real-time pipeline (`backupTrades`) |
| `processTrades()` | `(trades: Trade[]) → Promise<void>` | `save()` internal — mutates `this.pendingBars` |
| `import()` | `() → Promise<void>` | `save()` internal — `importPendingBars` + `resample` |
| `importPendingBars()` | `() → Promise<{from, to, markets}>` | `import()` internal |
| `writePoints()` | `(points: IPoint[], options: IWriteOptions) → Promise<void>` | `importPendingBars()` internal |
| `resample()` | `(range: TimeRange, fromTimeframe?, toTimeframe?) → Promise<void>` | `import()` internal |
| `executeQuery()` | `(query: string) → Promise<void>` | `resample()` internal |
| `fetch()` | `({from, to, timeframe, markets}) → Promise<output>` | HTTP API (`/historical`) |
| `getPendingBars()` | `(markets: string[], from: number, to: number) → Bar[]` | `fetch()` internal |

**Exchange (`src/exchange.js`) — relevant existing methods:**

| Method | Signature | Used by |
|--------|-----------|---------|
| `getMissingTrades()` | `(range: {pair, from, to}) → Promise<number>` | real-time recovery — **emits trades via `this.emitTrades()`** |
| `emitTrades()` | `(apiId, trades: Trade[]) → void` | injects into `server.chunk[]` via event |
| `recoverSinceLastTrade()` | `(connection: Connection) → void` | reconnection handler |
| `registerRangeForRecovery()` | `(range) → void` | recovery queue |

### 5.2 New Interfaces Required

#### 5.2.1 TimescaleDB — New Methods

**`getExistingBuckets(market, timeframe, from, to)`** — Gap detection query

```js
/**
 * Returns the set of existing bucket timestamps for a given market + timeframe within [from, to).
 * Used by GapDetector to identify missing bars.
 *
 * @param {string} market - e.g. "BINANCE:btcusdt"
 * @param {number} timeframe - timeframe in ms, e.g. 60000
 * @param {number} from - start timestamp in ms (inclusive)
 * @param {number} to - end timestamp in ms (exclusive)
 * @returns {Promise<number[]>} - sorted array of existing bucket timestamps (ms)
 */
async getExistingBuckets(market, timeframe, from, to) {
  const query = `
    SELECT EXTRACT(EPOCH FROM bucket)::bigint * 1000 AS ts
    FROM ${this.tablePath}
    WHERE timeframe_ms = $1
      AND market = $2
      AND bucket >= to_timestamp($3 / 1000.0)
      AND bucket < to_timestamp($4 / 1000.0)
    ORDER BY bucket ASC
  `
  const result = await this.backfillPool.query(query, [timeframe, market, from, to])
  return result.rows.map(row => Number(row.ts))
}
```

**`getBarCount(market, timeframe, from, to)`** — Quick existence check before fetching trades

```js
/**
 * Returns count of existing base-timeframe bars within a range.
 * Used by GapFiller to decide whether base data needs fetching.
 *
 * @param {string} market
 * @param {number} timeframe - base timeframe in ms
 * @param {number} from - start timestamp in ms (inclusive)
 * @param {number} to - end timestamp in ms (exclusive)
 * @returns {Promise<number>}
 */
async getBarCount(market, timeframe, from, to) {
  const query = `
    SELECT COUNT(*) AS cnt
    FROM ${this.tablePath}
    WHERE timeframe_ms = $1
      AND market = $2
      AND bucket >= to_timestamp($3 / 1000.0)
      AND bucket < to_timestamp($4 / 1000.0)
  `
  const result = await this.backfillPool.query(query, [timeframe, market, from, to])
  return Number(result.rows[0].cnt)
}
```

**`createBackfillPool()`** — Isolated connection pool

```js
/**
 * Creates a separate PG connection pool for backfill operations.
 * Prevents backfill DB queries from starving real-time writes.
 *
 * @returns {Pool} - pg Pool instance with limited max connections
 */
createBackfillPool() {
  this.backfillPool = new Pool({
    ...this.poolConfig,      // same host/port/database/credentials
    max: 2,                  // limited: only 2 connections for backfill
    application_name: 'aggr-server-backfill'
  })
  return this.backfillPool
}
```

**`backfillUpsertBars(timeframe, market, bars)`** — Write bars using backfill pool

```js
/**
 * Same logic as upsertBars() but uses the backfill connection pool.
 * Prevents backfill writes from competing with real-time writes on the main pool.
 *
 * @param {number} timeframe
 * @param {string} market
 * @param {Bar[]} bars
 * @returns {Promise<{fromTsWritten: number|null, toTsWritten: number|null}>}
 */
async backfillUpsertBars(timeframe, market, bars) {
  // identical SQL to upsertBars(), but uses this.backfillPool instead of this.pool
}
```

**`backfillResampleRange(market, fromTs, toTs)`** — Resample using backfill pool

```js
/**
 * Same logic as resampleToHigherTimeframes() but uses the backfill connection pool.
 *
 * @param {string} market
 * @param {number} fromTs
 * @param {number} toTs
 * @returns {Promise<void>}
 */
async backfillResampleRange(market, fromTs, toTs) {
  // identical logic to resampleToHigherTimeframes(), but uses this.backfillPool
}
```

**Refactoring note:** To avoid duplicating SQL logic between `upsertBars` / `backfillUpsertBars` and `resampleRangeToTimeframe` / `backfillResampleRange`, extract the SQL-building logic into internal helpers that accept a `pool` parameter:

```js
// Internal: shared SQL logic, pool is injected
async _upsertBarsWithPool(pool, timeframe, market, bars) { ... }
async _resampleRangeWithPool(pool, market, fromTs, toTs, targetTimeframe) { ... }

// Public: real-time path (uses this.pool)
async upsertBars(timeframe, market, bars) {
  return this._upsertBarsWithPool(this.pool, timeframe, market, bars)
}

// Public: backfill path (uses this.backfillPool)
async backfillUpsertBars(timeframe, market, bars) {
  return this._upsertBarsWithPool(this.backfillPool, timeframe, market, bars)
}
```

#### 5.2.2 InfluxDB — New Methods

**`getExistingBuckets(market, timeframe, from, to)`** — Gap detection query

```js
/**
 * Returns the set of existing bucket timestamps for gap detection.
 *
 * @param {string} market
 * @param {number} timeframe - in ms
 * @param {number} from - ms timestamp (inclusive)
 * @param {number} to - ms timestamp (exclusive)
 * @returns {Promise<number[]>} - sorted array of existing bucket timestamps (ms)
 */
async getExistingBuckets(market, timeframe, from, to) {
  const timeframeLitteral = getHms(timeframe)
  const rpName = config.influxRetentionPrefix + timeframeLitteral
  const measurement = config.influxMeasurement + '_' + timeframeLitteral

  const query = `SELECT time FROM "${config.influxDatabase}"."${rpName}"."${measurement}"
    WHERE market = '${market}'
      AND time >= ${from}ms AND time < ${to}ms
    ORDER BY time ASC`

  const result = await this.influx.query(query, { precision: 'ms', epoch: 'ms' })
  return result.map(row => +row.time)
}
```

**`backfillWritePoints(points)`** — Write bars directly (bypasses pendingBars)

```js
/**
 * Write bar points directly to InfluxDB for backfill.
 * Bypasses the pendingBars buffer entirely.
 * Uses the same retention policy and measurement naming as real-time writes.
 *
 * @param {Bar[]} bars - aggregated bars at base timeframe
 * @returns {Promise<void>}
 */
async backfillWritePoints(bars) {
  const points = bars.map(bar => ({
    measurement: 'trades_' + getHms(config.influxTimeframe),
    tags: { market: bar.market },
    fields: {
      open: bar.open, high: bar.high, low: bar.low, close: bar.close,
      vbuy: bar.vbuy, vsell: bar.vsell, cbuy: bar.cbuy, csell: bar.csell,
      lbuy: bar.lbuy, lsell: bar.lsell
    },
    timestamp: +bar.time
  }))

  await this.writePoints(points, { precision: 'ms', retentionPolicy: this.baseRp })
}
```

**`backfillResample(market, from, to)`** — Resample a specific range for backfill

```js
/**
 * Trigger resample for a specific market and time range (backfill use).
 * Wraps the existing resample() method with a targeted range.
 *
 * @param {string} market
 * @param {number} from - ms timestamp
 * @param {number} to - ms timestamp
 * @returns {Promise<void>}
 */
async backfillResample(market, from, to) {
  await this.resample({ from, to, markets: [market] })
}
```

**Note:** InfluxDB is HTTP-based and doesn't have a connection pool in the same sense as PostgreSQL. There is no need for a separate pool — concurrent writes are naturally handled by HTTP. The existing `writePoints()` with retry logic is sufficient.

#### 5.2.3 Exchange — New Method

**Problem:** The current `getMissingTrades()` calls `this.emitTrades(null, trades)` internally, which injects recovered trades into `server.chunk[]` via the event system. This is correct for real-time recovery but wrong for backfill — backfill must NOT pollute the real-time pipeline.

**`fetchHistoricalTrades(range)`** — Fetch trades without emitting

```js
/**
 * Fetch historical trades from exchange REST API for a given time range.
 * Unlike getMissingTrades(), does NOT call this.emitTrades() — returns trades directly.
 * Used exclusively by the backfill service.
 *
 * @param {{pair: string, from: number, to: number}} range
 * @returns {Promise<Trade[]>} - array of trade objects
 */
async fetchHistoricalTrades(range) { ... }
```

**Implementation strategy — per exchange:**

Taking Binance as example (`src/exchanges/binance.js`):

```js
// BEFORE (existing):
getMissingTrades(range, totalRecovered = 0) {
  const endpoint = `...aggTrades?symbol=${range.pair.toUpperCase()}&startTime=${range.from + 1}&endTime=${...}`
  return axios.get(endpoint).then(response => {
    if (response.data.length) {
      const trades = response.data.map(trade => ({
        ...this.formatTrade(trade, range.pair),
        count: trade.l - trade.f + 1,
        timestamp: trade.T
      }))
      this.emitTrades(null, trades)  // <── injects into real-time pipeline
      totalRecovered += trades.length
      range.from = trades[trades.length - 1].timestamp
      if (range.to - range.from > 1000) {
        return this.waitBeforeContinueRecovery().then(() =>
          this.getMissingTrades(range, totalRecovered)
        )
      }
    }
    return totalRecovered
  })
}

// AFTER (new method added):
async fetchHistoricalTrades(range, allTrades = []) {
  const startTime = range.from
  const endTime = Math.min(range.to, startTime + 1000 * 60 * 60) // 1h chunks

  const endpoint = `...aggTrades?symbol=${range.pair.toUpperCase()}&startTime=${startTime + 1}&endTime=${endTime}&limit=1000`
  const response = await axios.get(endpoint)

  if (response.data.length) {
    const trades = response.data.map(trade => ({
      ...this.formatTrade(trade, range.pair),
      count: trade.l - trade.f + 1,
      timestamp: trade.T
    }))
    allTrades.push(...trades)
    range.from = trades[trades.length - 1].timestamp

    if (range.to - range.from > 1000) {
      await sleep(config.backfillRequestDelay)
      return this.fetchHistoricalTrades(range, allTrades)
    }
  }

  return allTrades   // <── returns directly, NO emitTrades()
}
```

Each exchange that currently implements `getMissingTrades()` needs a corresponding `fetchHistoricalTrades()`:

| Exchange | Has `getMissingTrades` | Needs `fetchHistoricalTrades` |
|----------|----------------------|-------------------------------|
| BINANCE | yes | yes |
| BINANCE_FUTURES | yes | yes |
| BYBIT | yes | yes |
| COINBASE | yes | yes |
| BITFINEX | yes | yes |
| BITMEX | yes | yes |
| OKEX | yes | yes |
| KRAKEN | yes | yes |
| HUOBI | yes | yes |
| CRYPTOCOM | yes | yes |
| BITMART | yes | yes |
| Others | no | no (skipped by backfill) |

**Refactoring note:** To avoid code duplication, extract shared REST fetch logic:

```js
// Exchange base class (src/exchange.js):

/**
 * Internal: fetch trades from REST API for a range (no emit).
 * Subclasses implement _fetchTradesPage(range) to do one page of REST API call.
 *
 * @param {{pair: string, from: number, to: number}} range
 * @returns {Promise<Trade[]>}
 */
async fetchHistoricalTrades(range) {
  if (typeof this._fetchTradesPage !== 'function') {
    throw new Error(`${this.id} does not support fetchHistoricalTrades`)
  }

  const allTrades = []

  while (range.to - range.from > 1000) {
    const trades = await this._fetchTradesPage(range)

    if (!trades || !trades.length) break

    allTrades.push(...trades)
    range.from = trades[trades.length - 1].timestamp

    if (range.to - range.from > 1000) {
      await sleep(config.backfillRequestDelay || 500)
    }
  }

  return allTrades
}
```

Then each exchange just implements `_fetchTradesPage(range)` returning `Trade[]`:

```js
// src/exchanges/binance.js:
async _fetchTradesPage(range) {
  const endTime = Math.min(range.to, range.from + 3600000)
  const endpoint = `...aggTrades?symbol=${range.pair.toUpperCase()}&startTime=${range.from + 1}&endTime=${endTime}&limit=1000`
  const response = await axios.get(endpoint)
  return response.data.map(trade => ({
    ...this.formatTrade(trade, range.pair),
    count: trade.l - trade.f + 1,
    timestamp: trade.T
  }))
}
```

#### 5.2.4 BackfillService — Unified Storage Adapter Interface

BackfillService should not need to know whether the underlying storage is TimescaleDB or InfluxDB. Define a common interface that both storage classes satisfy:

```js
/**
 * @interface BackfillStorageAdapter
 *
 * The subset of storage methods that BackfillService depends on.
 * Both TimescaleDbStorage and InfluxStorage implement these.
 */

/**
 * @method getExistingBuckets
 * @param {string} market
 * @param {number} timeframe - ms
 * @param {number} from - ms, inclusive
 * @param {number} to - ms, exclusive
 * @returns {Promise<number[]>}
 */

/**
 * @method getBarCount
 * @param {string} market
 * @param {number} timeframe - ms
 * @param {number} from - ms, inclusive
 * @param {number} to - ms, exclusive
 * @returns {Promise<number>}
 */

/**
 * @method backfillUpsertBars
 * @param {number} timeframe
 * @param {string} market
 * @param {Bar[]} bars
 * @returns {Promise<{fromTsWritten: number|null, toTsWritten: number|null}>}
 */

/**
 * @method backfillResampleRange
 * @param {string} market
 * @param {number} fromTs
 * @param {number} toTs
 * @returns {Promise<void>}
 */
```

BackfillService selects the storage that has `format === 'point'` (same logic as the HTTP API in `server.js:402`):

```js
this.storage = storages.find(s => s.format === 'point')
```

### 5.3 Summary of All Changes

#### New files

| File | Description |
|------|-------------|
| `src/services/backfill.js` | BackfillService — main orchestrator, timer loop |
| `src/services/backfill-detector.js` | GapDetector — progressive scan, gap identification |
| `src/services/backfill-filler.js` | GapFiller — fetch trades, aggregate, write bars |

#### Modified files

| File | Changes |
|------|---------|
| `src/config.js` | Add `backfill`, `backfillStartTime`, `backfillTimeframes`, `backfillCheckInterval`, `backfillPairs`, `backfillMaxConcurrency`, `backfillRequestDelay` to defaults + validation logic |
| `src/server.js` | After `initStorages()`: instantiate and start `BackfillService`. On SIGINT: call `backfillService.stop()` |
| `src/storage/timescaledb.js` | Add `createBackfillPool()`, `getExistingBuckets()`, `getBarCount()`, `backfillUpsertBars()`, `backfillResampleRange()`. Refactor `upsertBars`/`resampleRangeToTimeframe` to extract pool-injectable internal helpers `_upsertBarsWithPool()`, `_resampleRangeWithPool()`. Store pool config in `this.poolConfig` for reuse. |
| `src/storage/influx.js` | Add `getExistingBuckets()`, `backfillWritePoints()`, `backfillResample()` |
| `src/exchange.js` | Add `fetchHistoricalTrades()` base method with `_fetchTradesPage()` dispatch |
| `src/exchanges/binance.js` | Add `_fetchTradesPage()` (extracted from `getMissingTrades` fetch logic) |
| `src/exchanges/binance_futures.js` | Add `_fetchTradesPage()` |
| `src/exchanges/bybit.js` | Add `_fetchTradesPage()` |
| `src/exchanges/coinbase.js` | Add `_fetchTradesPage()` |
| `src/exchanges/bitfinex.js` | Add `_fetchTradesPage()` |
| `src/exchanges/bitmex.js` | Add `_fetchTradesPage()` |
| `src/exchanges/okex.js` | Add `_fetchTradesPage()` |
| `src/exchanges/kraken.js` | Add `_fetchTradesPage()` |
| `src/exchanges/huobi.js` | Add `_fetchTradesPage()` |
| `src/exchanges/cryptocom.js` | Add `_fetchTradesPage()` |
| `src/exchanges/bitmart.js` | Add `_fetchTradesPage()` |
| `index.js` | Add `backfillService.stop()` to SIGINT handler |

## 6. Data Flow Diagram

```
                    ┌──────────────────────────────────────────────────────┐
                    │                    TIME AXIS                         │
                    │                                                      │
                    │  backfillStartTime          cutoff          now      │
                    │       │                        │             │       │
                    │       ▼                        ▼             ▼       │
                    │  ─────┼────────────────────────┼─────────────┼────   │
                    │       │   BACKFILL ZONE        │  EXCLUSION  │       │
                    │       │   (backfill owns)      │   ZONE      │       │
                    │       │                        │  (no-write) │       │
                    │       │                        │             │       │
                    │       │                        │ REAL-TIME   │       │
                    │       │                        │ ZONE        │       │
                    │       │                        │ (real-time  │       │
                    │       │                        │  pipeline   │       │
                    │       │                        │  owns)      │       │
                    └──────────────────────────────────────────────────────┘

Backfill Zone:
  Exchange REST API  ──►  aggregateToBaseBars()  ──►  storage.upsertBars()
                                                          │
                                                          ▼
                                                  storage.resampleToHigherTimeframes()

Real-time Zone:
  Exchange WebSocket ──►  server.chunk[]  ──►  storage.save()  ──►  processTrades()
                                                                        │
                                                                        ▼
                                                                    flush / import
                                                                        │
                                                                        ▼
                                                                  resample (higher TFs)
```

## 7. Edge Cases & Considerations

### 7.1 Exchange doesn't support `getMissingTrades`

Not all exchanges implement the REST trade history endpoint. For those exchanges, the backfill service logs a warning and skips them. The `getMissingTrades` method availability is checked before attempting a fill.

### 7.2 Exchange rate limits

Each exchange has different rate limits. The `backfillRequestDelay` config (default 500ms) adds a pause between consecutive REST calls. Additionally, `backfillMaxConcurrency: 1` means only one exchange is being backfilled at a time, avoiding parallel rate-limit pressure.

If a 429 (rate limited) response is received, the filler should back off exponentially (2s, 4s, 8s, up to 60s) before retrying.

### 7.3 Backfill overlaps with real-time recovery

The existing real-time recovery mechanism (`recoverSinceLastTrade` / `getMissingTrades` in `exchange.js`) handles short-term gaps caused by WebSocket disconnections. Backfill handles longer historical gaps.

**Overlap avoidance:**
- Real-time recovery writes trades into `server.chunk[]`, which goes through the normal `processTrades` → `pendingBars` → `flush` path.
- Backfill writes bars directly to the DB via `upsertBars`, bypassing `server.chunk[]` and `pendingBars`.
- Both use `ON CONFLICT DO UPDATE`, so if the same bar bucket is written by both paths, the last writer wins. Since real-time data is always more current (it includes the latest tick), real-time writes will eventually overwrite any backfill data for overlapping buckets. The exclusion zone prevents backfill from writing to the "hot" zone where real-time operates.

### 7.4 Server restart during backfill

Backfill state (scan cursor) is persisted to disk periodically (every scan cycle). On restart, scanning resumes from the last persisted cursor position. Any partially-written bars from an interrupted fill are safe because `upsertBars` is idempotent (ON CONFLICT DO UPDATE).

### 7.5 Weekends / market closures

For 24/7 crypto markets, this is generally not an issue. However, if a market has extended zero-volume periods, the gap detector will identify missing bars. The backfill filler will query the REST API and receive zero trades, resulting in no bars written. This gap will be detected again in the next scan pass.

**Mitigation:** After N consecutive empty fetches for the same gap, mark that gap as "confirmed empty" in the backfill state. Confirmed-empty gaps are skipped in future scans until the state is reset.

## 8. Logging

All backfill operations are logged with the `[backfill]` prefix:

```
[backfill] starting backfill service (startTime: 2024-06-01T00:00:00Z, timeframes: 1m,5m,1h,1d)
[backfill/detector] scanning BINANCE:btcusdt 1m [2024-06-01 00:00 → 2024-06-02 00:00) ... 47 gaps found
[backfill/filler] filling BINANCE:btcusdt [2024-06-01T03:00:00Z → 2024-06-01T04:00:00Z) ...
[backfill/filler] +342 trades → 360 bars written, resampled to 5m,1h,1d
[backfill/detector] scan pass complete for BINANCE:btcusdt 1m, restarting from backfillStartTime
[backfill] stopping backfill service
```

## 9. Summary

| Aspect | Decision |
|--------|----------|
| Trigger | Timer-based, every `backfillCheckInterval` (default 10s) |
| Gap detection | DB query per (market, timeframe), chunked progressive scan |
| Gap filling | Exchange REST API → aggregate to base bars → upsert to DB → resample up |
| Real-time isolation | Exclusion zone (`now - 3 * resampleInterval`); separate write path (no shared `pendingBars` / `chunk[]`) |
| DB contention | Separate connection pool (or pool subset) for backfill |
| Idempotency | `ON CONFLICT DO UPDATE` ensures re-filling a gap is safe |
| State persistence | Scan cursors saved to `products/backfill-state.json` |
| Rate limiting | Configurable delay between REST calls; exponential backoff on 429 |
| Supported exchanges | Only those with `getMissingTrades()` implementation |
