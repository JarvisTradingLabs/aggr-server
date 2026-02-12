const config = require('../config')
const { getHms, sleep } = require('../helper')

const DAY = 24 * 60 * 60 * 1000
const HOUR = 60 * 60 * 1000
const BACKFILL_MAX_REQUESTS_PER_TICK = 100
const BACKFILL_FETCH_TIMEOUT_MS = 30000

const SCAN_CHUNK_BY_TIMEFRAME = {
  60000: DAY,
  300000: DAY * 7,
  900000: DAY * 7,
  3600000: DAY * 30,
  14400000: DAY * 90,
  43200000: DAY * 180,
  86400000: DAY * 365
}

class BackfillService {
  constructor({ exchanges = [], storages = [] }) {
    this.exchanges = exchanges
    this.storages = storages

    this.storage = this.storages.find(storage =>
      typeof storage.getExistingBuckets === 'function' &&
      typeof storage.getBarCount === 'function' &&
      typeof storage.getFirstByInterval === 'function' &&
      typeof storage.getLastByInterval === 'function' &&
      typeof storage.backfillUpsertBars === 'function' &&
      typeof storage.backfillResampleRange === 'function'
    )

    this.baseTimeframe = Number(config.influxTimeframe)
    this.timeframes = (config.backfillTimeframes || [])
      .map(Number)
      .sort((a, b) => a - b)
    this.primaryGapTimeframe = this.resolvePrimaryGapTimeframe(this.timeframes)

    this.markets = (config.backfillPairs && config.backfillPairs.length
      ? config.backfillPairs
      : config.pairs
    ).slice()

    this.startTime = Number(config.backfillStartTime)
    this.maxConcurrency = Math.max(1, Number(config.backfillMaxConcurrency || 1))
    this.requestDelay = Math.max(0, Number(config.backfillRequestDelay || 0))
    this.checkInterval = Math.max(60000, Number(config.backfillCheckInterval || 60000))

    this.recentExclusionMs = Number(config.influxResampleInterval || 60000) * 2
    this.hotZoneExclusionMs = Number(config.influxResampleInterval || 60000) * 3
    this.maxFillRangePerTick = DAY
    this.maxRequestsPerTick = BACKFILL_MAX_REQUESTS_PER_TICK

    this.exchangeById = this.exchanges.reduce((acc, exchange) => {
      acc[exchange.id] = exchange
      return acc
    }, {})

    this.unsupportedExchangeWarned = {}
    this.scanCursor = {}
    this.noProgressCursor = {}
    this.marketRunning = {}
    this.exchangeRunning = {}
    this.tickRunning = false
    this.tickRemainingRequests = this.maxRequestsPerTick
  }

  async start() {
    if (!this.storage) {
      throw new Error(
        'no storage supports backfill adapter interface (getFirstByInterval/getLastByInterval/getExistingBuckets/getBarCount/backfillUpsertBars/backfillResampleRange)'
      )
    }

    await this.prepareExchanges()

    console.log(
      `[backfill] starting backfill service (startTime: ${new Date(
        this.startTime
      ).toISOString()}, primary gap timeframe: ${getHms(
        this.primaryGapTimeframe,
        true
      )}, configured timeframes: ${this.timeframes
        .map(tf => getHms(tf, true))
        .join(',')})`
    )

    this.tick()
    this.interval = setInterval(() => this.tick(), this.checkInterval)
  }

  async stop() {
    console.log('[backfill] stopping backfill service')
    clearInterval(this.interval)
    this.interval = null

    while (Object.keys(this.marketRunning).length) {
      await sleep(200)
    }
  }

  async prepareExchanges() {
    const exchangeIds = this.markets
      .map(market => (market.match(/([^:]*):(.*)/) || [])[1])
      .filter(Boolean)
      .filter((id, index, all) => all.indexOf(id) === index)

    for (const exchangeId of exchangeIds) {
      const exchange = this.exchangeById[exchangeId]
      if (!exchange || typeof exchange.getProducts !== 'function') {
        continue
      }

      try {
        await exchange.getProducts()
        console.log(`[backfill] preloaded products for ${exchangeId}`)
      } catch (error) {
        console.warn(
          `[backfill] failed to preload products for ${exchangeId}: ${error.message}`
        )
      }
    }
  }

  align(timestamp, timeframe) {
    return Math.floor(timestamp / timeframe) * timeframe
  }

  getCursorKey(market, timeframe) {
    return `${market}|${timeframe}`
  }

  getNoProgressCursor(key, start) {
    const cursor = this.noProgressCursor[key]
    if (!cursor || cursor < start) {
      return start
    }

    return cursor
  }

  async tick() {
    if (this.tickRunning || !this.markets.length) {
      return
    }

    this.tickRunning = true
    this.tickRemainingRequests = this.maxRequestsPerTick

    try {
      const pendingMarkets = this.markets.slice()
      const workers = []
      const workerCount = Math.min(this.maxConcurrency, this.markets.length)

      for (let i = 0; i < workerCount; i++) {
        workers.push(this.tickWorker(pendingMarkets))
      }

      await Promise.all(workers)
    } finally {
      this.tickRunning = false
    }
  }

  async tickWorker(pendingMarkets) {
    while (pendingMarkets.length && this.tickRemainingRequests > 0) {
      const market = pendingMarkets.shift()
      if (!market) {
        break
      }

      try {
        await this.processMarketTick(market)
      } catch (error) {
        console.error(`[backfill] market tick failed for ${market}`, error.message)
      }
    }
  }

  async processMarketTick(market) {
    if (this.tickRemainingRequests <= 0) {
      return
    }

    if (this.marketRunning[market]) {
      return
    }

    const exchangeId = (market.match(/([^:]*):(.*)/) || [])[1]
    if (!exchangeId) {
      return
    }

    if (this.exchangeRunning[exchangeId]) {
      return
    }

    this.marketRunning[market] = true
    this.exchangeRunning[exchangeId] = true

    try {
      const gap = await this.findNextGapForMarket(market)
      if (!gap) {
        return
      }

      const cappedGap = {
        ...gap,
        to: Math.min(gap.to, gap.from + this.maxFillRangePerTick)
      }

      const result = await this.fillGap(cappedGap)
      const key = this.getCursorKey(market, cappedGap.timeframe)

      if ((result.totalTrades || 0) === 0 && (result.totalBars || 0) === 0) {
        const start = this.align(this.startTime, cappedGap.timeframe)
        this.noProgressCursor[key] = Math.max(
          this.getNoProgressCursor(key, start),
          cappedGap.to
        )

        console.warn(
          `[backfill/filler] no progress on ${market} ${getHms(
            cappedGap.timeframe,
            true
          )} [${new Date(cappedGap.from).toISOString()} -> ${new Date(
            cappedGap.to
          ).toISOString()}), skip cursor -> ${new Date(this.noProgressCursor[key]).toISOString()}`
        )
      } else {
        delete this.noProgressCursor[key]
      }
    } finally {
      delete this.marketRunning[market]
      delete this.exchangeRunning[exchangeId]
    }
  }

  async findNextGapForMarket(market) {
    const timeframe = this.primaryGapTimeframe
    const gap = await this.findNextGapByTimeframe(market, timeframe)
    if (!gap) {
      return null
    }

    return {
      ...gap,
      market,
      timeframe
    }
  }

  async findNextGapByTimeframe(market, timeframe) {
    const now = Date.now()
    const cutoff = now - this.recentExclusionMs
    const intervalStart = this.align(this.startTime, timeframe)

    if (cutoff <= intervalStart) {
      return null
    }

    const key = this.getCursorKey(market, timeframe)

    const first = await this.storage.getFirstByInterval(
      market,
      timeframe,
      intervalStart,
      cutoff
    )

    const last = await this.storage.getLastByInterval(
      market,
      timeframe,
      intervalStart,
      cutoff
    )

    if (first === null || last === null) {
      const from = this.getNoProgressCursor(key, intervalStart)
      if (from >= cutoff) {
        return null
      }

      return {
        from,
        to: cutoff
      }
    }

    if (first > intervalStart) {
      const from = this.getNoProgressCursor(key, intervalStart)
      if (from >= first) {
        return null
      }

      return {
        from,
        to: first
      }
    }

    const earliest = this.align(first, timeframe)
    const latest = Math.min(cutoff, this.align(last, timeframe) + timeframe)

    let scanFrom = this.scanCursor[key]
    if (!scanFrom || scanFrom < earliest || scanFrom >= latest) {
      scanFrom = earliest
    }

    const chunkSize = SCAN_CHUNK_BY_TIMEFRAME[timeframe] || DAY
    const scanTo = Math.min(scanFrom + chunkSize, latest)

    if (scanTo > scanFrom) {
      const existingBuckets = await this.storage.getExistingBuckets(
        market,
        timeframe,
        scanFrom,
        scanTo
      )

      const gaps = this.detectGaps(scanFrom, scanTo, timeframe, existingBuckets)
      this.scanCursor[key] = scanTo >= latest ? earliest : scanTo

      if (gaps.length) {
        return gaps[0]
      }
    }

    const tailFrom = this.align(last, timeframe) + timeframe
    if (tailFrom < cutoff) {
      return {
        from: tailFrom,
        to: cutoff
      }
    }

    return null
  }

  detectGaps(from, to, timeframe, existingBuckets) {
    const existing = new Set(existingBuckets.map(ts => this.align(Number(ts), timeframe)))
    const missing = []

    for (let ts = this.align(from, timeframe); ts < to; ts += timeframe) {
      if (!existing.has(ts)) {
        missing.push(ts)
      }
    }

    if (!missing.length) {
      return []
    }

    const gaps = []
    let start = missing[0]
    let previous = missing[0]

    for (let i = 1; i < missing.length; i++) {
      const current = missing[i]

      if (current !== previous + timeframe) {
        gaps.push({ from: start, to: previous + timeframe })
        start = current
      }

      previous = current
    }

    gaps.push({ from: start, to: previous + timeframe })
    return gaps
  }

  async fillGap(gap) {
    const cutoff = Date.now() - this.hotZoneExclusionMs
    const market = gap.market
    const from = gap.from
    const to = Math.min(gap.to, cutoff)

    const result = {
      totalTrades: 0,
      totalBars: 0
    }

    if (to <= from) {
      return result
    }

    const match = market.match(/([^:]*):(.*)/)
    if (!match) {
      console.warn(`[backfill/filler] invalid market "${market}"`)
      return result
    }

    const exchangeId = match[1]
    const pair = match[2]
    const exchange = this.exchangeById[exchangeId]

    if (!exchange) {
      console.warn(`[backfill/filler] exchange ${exchangeId} is not available`)
      return result
    }

    if (typeof exchange.fetchHistoricalTrades !== 'function') {
      if (!this.unsupportedExchangeWarned[exchangeId]) {
        this.unsupportedExchangeWarned[exchangeId] = true
        console.warn(
          `[backfill/filler] ${exchangeId} has no fetchHistoricalTrades support, skipping`
        )
      }
      return result
    }

    console.log(
      `[backfill/filler] filling ${market} ${getHms(gap.timeframe, true)} [${new Date(
        from
      ).toISOString()} -> ${new Date(to).toISOString()})`
    )

    const requestBarsLimit = this.getRequestBarsPerRequest(exchange, pair)
    const requestSliceMs = this.getRequestSliceMs(gap.timeframe, requestBarsLimit)

    for (let cursor = from; cursor < to; cursor += requestSliceMs) {
      const slice = {
        from: cursor,
        to: Math.min(cursor + requestSliceMs, to)
      }

      let trades
      try {
        trades = await this.fetchWithRetry(exchange, {
          pair,
          from: slice.from,
          to: slice.to
        })
      } catch (error) {
        if (error && error.code === 'BACKFILL_TICK_BUDGET_EXHAUSTED') {
          return result
        }

        throw error
      }

      const bars = this.aggregateToBaseBars(trades, market)
      result.totalTrades += trades.length
      result.totalBars += bars.length

      if (bars.length) {
        const writeResult = await this.storage.backfillUpsertBars(
          this.baseTimeframe,
          market,
          bars
        )

        if (writeResult.fromTsWritten !== null) {
          await this.storage.backfillResampleRange(
            market,
            writeResult.fromTsWritten,
            writeResult.toTsWritten + this.baseTimeframe
          )
        }
      } else {
        const existingBaseBarCount = await this.storage.getBarCount(
          market,
          this.baseTimeframe,
          slice.from,
          slice.to
        )

        if (existingBaseBarCount > 0) {
          await this.storage.backfillResampleRange(
            market,
            slice.from,
            slice.to
          )
        }
      }

      if (this.requestDelay > 0) {
        await sleep(this.requestDelay)
      }
    }

    return result
  }

  async fetchWithRetry(exchange, range) {
    let delay = 2000
    let attempt = 0

    while (attempt < 8) {
      if (!this.acquireTickRequestSlot()) {
        const budgetError = new Error('backfill tick request budget exhausted')
        budgetError.code = 'BACKFILL_TICK_BUDGET_EXHAUSTED'
        throw budgetError
      }

      try {
        const trades = await Promise.race([
          exchange.fetchHistoricalTrades(range),
          sleep(BACKFILL_FETCH_TIMEOUT_MS).then(() => {
            const timeoutError = new Error(
              `${exchange.id} ${range.pair} fetch timeout after ${BACKFILL_FETCH_TIMEOUT_MS}ms`
            )
            timeoutError.code = 'BACKFILL_FETCH_TIMEOUT'
            throw timeoutError
          })
        ])

        return Array.isArray(trades) ? trades : []
      } catch (error) {
        const status = error?.response?.status
        const shouldRetry =
          error?.code === 'BACKFILL_FETCH_TIMEOUT' ||
          status === 429 ||
          (status >= 500 && status <= 599)

        if (!shouldRetry) {
          throw error
        }

        attempt++
        console.warn(
          `[backfill/filler] ${exchange.id} ${range.pair} REST retry in ${delay}ms (status ${status || 'unknown'})`
        )

        await sleep(delay)
        delay = Math.min(delay * 2, 60000)
      }
    }

    throw new Error(
      `${exchange.id} ${range.pair} historical fetch exceeded max retries`
    )
  }

  aggregateToBaseBars(trades, market) {
    if (!trades || !trades.length) {
      return []
    }

    const barsByBucket = {}

    for (const trade of trades) {
      const timestamp = Number(trade.timestamp)
      const price = Number(trade.price)
      const size = Number(trade.size)

      if (!isFinite(timestamp) || !isFinite(price) || !isFinite(size) || size <= 0) {
        continue
      }

      const bucket = this.align(timestamp, this.baseTimeframe)

      if (!barsByBucket[bucket]) {
        barsByBucket[bucket] = {
          time: bucket,
          market,
          cbuy: 0,
          csell: 0,
          vbuy: 0,
          vsell: 0,
          lbuy: 0,
          lsell: 0,
          open: null,
          high: null,
          low: null,
          close: null
        }
      }

      const bar = barsByBucket[bucket]
      const side = this.normalizeSide(trade.side)
      const notional = price * size

      if (trade.liquidation) {
        bar['l' + side] += notional
        continue
      }

      if (bar.open === null) {
        bar.open = bar.high = bar.low = bar.close = price
      } else {
        bar.high = Math.max(bar.high, price)
        bar.low = Math.min(bar.low, price)
        bar.close = price
      }

      bar['c' + side] += Number(trade.count || 1)
      bar['v' + side] += notional
    }

    return Object.values(barsByBucket)
      .filter(bar => bar.close !== null)
      .sort((a, b) => a.time - b.time)
  }

  normalizeSide(side) {
    if (side === 'buy' || side === 1 || side === '1' || side === 'Buy') {
      return 'buy'
    }

    return 'sell'
  }

  resolvePrimaryGapTimeframe(timeframes) {
    if (timeframes.indexOf(300000) !== -1) {
      return 300000
    }

    if (!timeframes.length) {
      return 300000
    }

    return timeframes[0]
  }

  getRequestBarsPerRequest(exchange, pair) {
    if (exchange && typeof exchange.getBackfillRequestBarsLimit === 'function') {
      const limit = Number(
        exchange.getBackfillRequestBarsLimit({
          pair,
          timeframe: this.primaryGapTimeframe
        })
      )

      if (isFinite(limit) && limit > 0) {
        return limit
      }
    }

    return 500
  }

  acquireTickRequestSlot() {
    if (!isFinite(this.tickRemainingRequests) || this.tickRemainingRequests <= 0) {
      return false
    }

    this.tickRemainingRequests -= 1
    return true
  }

  getRequestSliceMs(timeframe, barsPerRequest) {
    const tf = Math.max(Number(timeframe || this.baseTimeframe), this.baseTimeframe)
    const requestedSliceMs = tf * Math.max(1, Number(barsPerRequest || 500))

    if (!isFinite(requestedSliceMs) || requestedSliceMs <= 0) {
      return HOUR
    }

    return Math.min(requestedSliceMs, this.maxFillRangePerTick)
  }
}

module.exports = BackfillService
