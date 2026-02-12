const Exchange = require('../exchange')
const axios = require('axios')
const { sleep } = require('../helper')

class BinanceUs extends Exchange {
  constructor() {
    super()

    this.id = 'BINANCE_US'
    this.lastSubscriptionId = 0
    this.subscriptions = {}

    this.endpoints = {
      PRODUCTS: 'https://api.binance.us/api/v3/exchangeInfo'
    }

    this.url = () => 'wss://stream.binance.us:9443/ws'
  }

  formatProducts(data) {
    return data.symbols
      .filter(product => product.status === 'TRADING')
      .map(product => product.symbol.toLowerCase())
  }

  /**
   * Sub
   * @param {WebSocket} api
   * @param {string} pair
   */
  async subscribe(api, pair) {
    if (!(await super.subscribe.apply(this, arguments))) {
      return
    }

    this.subscriptions[pair] = ++this.lastSubscriptionId

    const params = [pair + '@trade']

    api.send(
      JSON.stringify({
        method: 'SUBSCRIBE',
        params,
        id: this.subscriptions[pair]
      })
    )

    // this websocket api have a limit of about 5 messages per second.
    await sleep(250)
  }

  /**
   * Unsub
   * @param {WebSocket} api
   * @param {string} pair
   */
  async unsubscribe(api, pair) {
    if (!(await super.unsubscribe.apply(this, arguments))) {
      return
    }

    const params = [pair + '@trade']

    api.send(
      JSON.stringify({
        method: 'UNSUBSCRIBE',
        params,
        id: this.subscriptions[pair]
      })
    )

    delete this.subscriptions[pair]

    // this websocket api have a limit of about 5 messages per second.
    return new Promise(resolve => setTimeout(resolve, 250))
  }

  onMessage(event, api) {
    const json = JSON.parse(event.data)

    if (json.E) {
      return this.emitTrades(api.id, [
        {
          exchange: this.id,
          pair: json.s.toLowerCase(),
          timestamp: json.E,
          price: +json.p,
          size: +json.q,
          side: json.m ? 'sell' : 'buy'
        }
      ])
    }
  }

  async fetchHistoricalTrades(_range) {
    const range = _range
    const trades = []
    const end = Number(range.to)
    let cursor = Number(range.from)

    while (cursor < end) {
      const requestEnd = Math.min(end, cursor + 1000 * 60 * 60)
      const endpoint = `https://api.binance.us/api/v3/aggTrades?symbol=${range.pair.toUpperCase()}&startTime=${
        cursor + 1
      }&endTime=${requestEnd}&limit=1000`

      const response = await axios.get(endpoint)
      const payload = (response.data || [])
        .filter(trade => trade.T > cursor && trade.T < end)
        .map(trade => ({
          exchange: this.id,
          pair: range.pair,
          timestamp: trade.T,
          price: +trade.p,
          size: +trade.q,
          side: trade.m ? 'sell' : 'buy',
          count: trade.l - trade.f + 1
        }))

      if (!payload.length) {
        break
      }

      trades.push(...payload)

      const nextCursor = payload[payload.length - 1].timestamp
      if (nextCursor <= cursor) {
        break
      }
      cursor = nextCursor
    }

    return trades
  }
}

module.exports = BinanceUs
