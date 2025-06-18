go func(data []coingecko.ExchangesTickers) {
	log.InfoL(labels, "START UpsertCoinGeckoExchangesTickers()")
	var start = time.Now()
	store.UpsertCoinGeckoExchangesTickers(&data)
	saveCount()
	log.InfoL(labels, "END UpsertCoinGeckoExchangesTickers() totalTime:%.2fs", time.Since(start).Seconds())
}(exchangesTickers)

DATA_NAMESPACE=_dev
ROWY_PREFIX=dev_
DB_PORT=5432
DB_HOST="forbesdevhpc-dbxtn.forbes.tessell.com"
DB_USER="master"
DB_PASSWORD="wkhzEYwlvpQTGTdR"
DB_NAME="forbes"
DB_SSLMODE=disable
PATCH_SIZE=1000
MON_LIMIT=2000000
CG_RATE_LIMIT=300
COINGECKO_URL="https://pro-api.coingecko.com/api/v3"
COINGECKO_API_KEY=CG-V88xeVE4mSPsP71kS7LVWsDk