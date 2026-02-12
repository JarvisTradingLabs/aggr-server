const Exchange = require('../exchange')

class Noop extends Exchange {
  constructor(options) {
    super(options)

    this.id = 'noop'
  }

  connect() {
    return false
  }

  async fetchHistoricalTrades(_range) {
    return []
  }
}

module.exports = Noop
