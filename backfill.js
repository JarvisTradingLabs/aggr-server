const fs = require('fs')
const config = require('./src/config')
const BackfillService = require('./src/services/backfill')

async function loadExchanges() {
  if (!config.exchanges || !config.exchanges.length) {
    config.exchanges = []
    fs.readdirSync('./src/exchanges/').forEach(file => {
      if (/\.js$/.test(file)) {
        config.exchanges.push(file.replace(/\.js$/, ''))
      } else if (fs.statSync(`./src/exchanges/${file}`).isDirectory()) {
        config.exchanges.push(file)
      }
    })
  }

  const exchanges = []

  for (const name of config.exchanges) {
    const exchange = new (require('./src/exchanges/' + name))()

    config.exchanges[config.exchanges.indexOf(name)] = exchange.id
    exchanges.push(exchange)
  }

  return exchanges
}

async function loadStorages() {
  if (!config.storage || !config.storage.length) {
    return []
  }

  const storages = []

  for (const name of config.storage) {
    const storage = new (require(`./src/storage/${name}`))()
    if (typeof storage.connect === 'function') {
      await storage.connect()
    }
    storages.push(storage)
  }

  return storages
}

async function main() {
  if (!config.backfill) {
    console.warn('[backfill] disabled (set backfill=true to enable)')
    process.exit(0)
  }

  const exchanges = await loadExchanges()
  const storages = await loadStorages()
  const backfillService = new BackfillService({ exchanges, storages })

  await backfillService.start()

  const shutdown = async signal => {
    console.log(`\n${signal}`)
    await backfillService.stop()
    process.exit(0)
  }

  process.on('SIGINT', () => shutdown('SIGINT'))
  process.on('SIGTERM', () => shutdown('SIGTERM'))
}

main().catch(error => {
  console.error('[backfill] fatal error', error)
  process.exit(1)
})
