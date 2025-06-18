postgres.go

func InsertExchangesProcedure(exchangesMarketTicker []models.ExchangeMarketTickers) error {

	pg := PGConnect()
	valueString := make([]string, 0, len(exchangesMarketTicker))
	valueExchanges := make([]interface{}, 0, len(exchangesMarketTicker)*12)

	var i = 0
	tableName := "exchange_market_ticker_test"
	for y := 0; y < len(exchangesMarketTicker); y++{

		var exchange = exchangesMarketTicker[y]

			var valString = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",i*12+1, i*12+2, i*12+3, i*12+4, i*12+5, i*12+6, i*12+7, i*12+8, i*12+9, i*12+10, i*12+11, i*12+12)
	
			valueString = append(valueString, valString)
	
			valueExchanges = append(valueExchanges, exchange.Symbol)
			valueExchanges = append(valueExchanges, exchange.Exchange)
			valueExchanges = append(valueExchanges, exchange.NumMarkets)
			valueExchanges = append(valueExchanges, exchange.Price)
			valueExchanges = append(valueExchanges, exchange.VolumeByExchange1D)
			valueExchanges = append(valueExchanges, exchange.PriceByExchange1D)
			valueExchanges = append(valueExchanges, exchange.VolumeByExchange7D)
			valueExchanges = append(valueExchanges, exchange.PriceByExchange7D)
			valueExchanges = append(valueExchanges, exchange.VolumeByExchange30D)
			valueExchanges = append(valueExchanges, exchange.PriceByExchange30D)
			valueExchanges = append(valueExchanges, exchange.VolumeByExchange1Y)
			valueExchanges = append(valueExchanges, exchange.PriceByExchange1Y)
			i++
			if len(valueExchanges) >= 65000 || y == len(exchangesMarketTicker) -1{
				updateStatement := " ON CONFLICT (symbol, exchange) DO UPDATE SET exchange = EXCLUDED.exchange, num_markets = EXCLUDED.num_markets, price = EXCLUDED.price, volume_by_pair_1d = EXCLUDED.volume_by_exchange_1d, price_by_exchange_1d = EXCLUDED.price_by_exchange_1d, volume_by_exchange_7d = EXCLUDED.volume_by_exchange_7d, price_by_exchange_7d = EXCLUDED.price_by_exchange_7d, volume_by_exchange_30d = EXCLUDED.volume_by_exchange_30d, price_by_exchange_30d = EXCLUDED.price_by_exchange_30d, volume_by_exchange_1y = EXCLUDED.volume_by_exchange_1y, price_by_exchange_1y = EXCLUDED.price_by_exchange_1y"
				queryStatement := fmt.Sprintf("INSERT INTO %s VALUES %s %s", tableName, strings.Join(valueString, ","), updateStatement)
		
				_, inserterExchangeError := pg.Exec(queryStatement, valueExchanges...)
		
				if inserterExchangeError != nil {
					log.Error("error: %s", inserterExchangeError)
					return inserterExchangeError
				}
	
				valueString = make([]string, 0, len(exchangesMarketTicker))
				valueExchanges = make([]interface{}, 0, len(exchangesMarketTicker)*12)
				i = 0
		}
	}
	return nil
}


func InsertHighLowsFundamentals(fundamentals []models.FundamentalsHighLows) error {

	pg := PGConnect()

	valueString := make([]string, 0, len(fundamentals))
	valueFundamentals := make([]interface{}, 0, len(fundamentals)*11)

	var i = 0

	tableName := "fundamentals_high_low_test"

	for y := 0; y < len(fundamentals); y++ {
		var fundamental = fundamentals[y]

		var valString = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",i*11+1, i*11+2, i*11+3, i*11+4, i*11+5, i*11+6, i*11+7, i*11+8, i*11+9, i*11+10, i*11+11)

		valueString = append(valueString,valString)
		valueFundamentals = append(valueFundamentals, fundamental.Symbol)
		valueFundamentals = append(valueFundamentals, fundamental.High24H)
		valueFundamentals = append(valueFundamentals, fundamental.Low24H)
		valueFundamentals = append(valueFundamentals, fundamental.High7D)
		valueFundamentals = append(valueFundamentals, fundamental.Low7D)
		valueFundamentals = append(valueFundamentals, fundamental.High30D)
		valueFundamentals = append(valueFundamentals, fundamental.Low30D)
		valueFundamentals = append(valueFundamentals, fundamental.High1Y)
		valueFundamentals = append(valueFundamentals, fundamental.Low1Y)
		valueFundamentals = append(valueFundamentals, fundamental.AllTimeHigh)
		valueFundamentals = append(valueFundamentals, fundamental.AllTimeLow)

		i++

		if len(valueFundamentals) >= 65000 || y == len(fundamentals) -1{
			
			updateStatement := "ON CONFLICT (symbol) DO UPDATE SET high_24h = EXCLUDED.high_24h, low_24h = EXCLUDED.low_24h, high_7d = EXCLUDED.high_7d, low_7d = EXCLUDED.low_7d, high_30d = EXCLUDED.high_30d, low_30d = EXCLUDED.low_30d, high_1y = EXCLUDED.high_1y, low_1y = EXCLUDED.low_1y, all_time_high = EXCLUDED.all_time_high, all_time_low = EXCLUDED.all_time_low"

			queryStatement := fmt.Sprintf("INSERT INTO %s VALUES %s %s", tableName, strings.Join(valueString, ","), updateStatement)

			_, inserterError := pg.Exec(queryStatement, valueFundamentals...)

			if inserterError != nil {
				log.Error("error: %s", inserterError)
				return inserterError
			}
			valueString = make([]string, 0, len(fundamentals))
			valueFundamentals = make([]interface{}, 0, len(fundamentals)*11)

			i = 0

		}

	}

	return nil
}


func InsertActiveAssets(activeAssets []models.Assets) error {
	pg := PGConnect()

	valueString := make([]string, 0, len(activeAssets))
	valueAssets := make([]interface{}, 0, len(activeAssets)*2)

	var i = 0

	tableName := "active_markets"

	for y := 0; y < len(activeAssets); y++ {
		var assets = activeAssets[y]
		var valString = fmt.Sprintf("($%d,$%d)",i*2+1, i*2+2)
		valueString = append(valueString, valString)
		valueAssets = append(valueAssets, assets.Base)
		valueAssets = append(valueAssets, assets.Quote)

		i++

		if len(valueAssets) >= 65000 || y == len(activeAssets) -1 {
			updateStatement := "ON CONFLICT (base, quote) DO UPDATE SET base = EXCLUDED.base, quote = EXCLUDED.quote"

			queryStatement := fmt.Sprintf("INSERT INTO %s VALUES %s %s", tableName, strings.Join(valueString, ", "), updateStatement)

			_, inserterError := pg.Exec(queryStatement, valueAssets...)
			if inserterError != nil {
				log.Error("error: %s", inserterError)
				return inserterError
			}

			valueString = make([]string, 0, len(activeAssets))
			valueAssets = make([]interface{}, 0, len(activeAssets)*2)

			i = 0
		}
	}
	return nil
}


func InsertInactiveAssets(inactiveAssets []models.Assets) error {
	pg := PGConnect()

	valueString := make([]string, 0, len(inactiveAssets))
	valueAssets := make([]interface{}, 0, len(inactiveAssets)*2)

	var i = 0

	tableName := "inactive_markets"

	for y := 0; y < len(inactiveAssets); y++ {
		var assets = inactiveAssets[y]
		var valString = fmt.Sprintf("($%d,$%d)",i*2+1, i*2+2)
		valueString = append(valueString, valString)
		valueAssets = append(valueAssets, assets.Base)
		valueAssets = append(valueAssets, assets.Quote)

		i++

		if len(valueAssets) >= 65000 || y == len(inactiveAssets) -1 {
			updateStatement := "ON CONFLICT (base, quote) DO UPDATE SET base = EXCLUDED.base, quote = EXCLUDED.quote"

			queryStatement := fmt.Sprintf("INSERT INTO %s VALUES %s %s", tableName, strings.Join(valueString, ", "), updateStatement)

			_, inserterError := pg.Exec(queryStatement, valueAssets...)
			if inserterError != nil {
				log.Error("error: %s", inserterError)
				return inserterError
			}

			valueString = make([]string, 0, len(inactiveAssets))
			valueAssets = make([]interface{}, 0, len(inactiveAssets)*2)

			i = 0
		}
	}
	return nil
}


func InsertMarketPairs(marketPairs []models.MarketPairs) error {
	pg := PGConnect()
	valueString := make([]string, 0, len(marketPairs))
	valueExchanges := make([]interface{}, 0, len(marketPairs)*13)

	var i = 0
	tableName := "market_pairs"
	for y := 0; y < len(marketPairs); y++{

		var marketPair = marketPairs[y]

			var valString = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",i*13+1, i*13+2, i*13+3, i*13+4, i*13+5, i*13+6, i*13+7, i*13+8, i*13+9, i*13+10, i*13+11,i*13+12, i*13+13)
	
			valueString = append(valueString, valString)
	
			valueExchanges = append(valueExchanges, marketPair.Symbol)
			valueExchanges = append(valueExchanges, marketPair.Exchange)
			valueExchanges = append(valueExchanges, marketPair.Quote)
			valueExchanges = append(valueExchanges, marketPair.Type)
			valueExchanges = append(valueExchanges, marketPair.Pair)
			valueExchanges = append(valueExchanges, marketPair.VolumeByPair1D)
			valueExchanges = append(valueExchanges, marketPair.PriceByPair1D)
			valueExchanges = append(valueExchanges, marketPair.VolumeByPair7D)
			valueExchanges = append(valueExchanges, marketPair.PriceByPair7D)
			valueExchanges = append(valueExchanges, marketPair.VolumeByPair30D)
			valueExchanges = append(valueExchanges, marketPair.PriceByPair30D)
			valueExchanges = append(valueExchanges, marketPair.VolumeByPair1Y)
			valueExchanges = append(valueExchanges, marketPair.PriceByPair1Y)
			i++
			if len(valueExchanges) >= 65000 || y == len(marketPairs) -1{
				updateStatement := " ON CONFLICT (symbol, exchange, quote) DO UPDATE SET  type = EXCLUDED.type, pair = EXCLUDED.pair, volume_by_pair_1d = EXCLUDED.volume_by_pair_1d, price_by_pair_1d = EXCLUDED.price_by_pair_1d, volume_by_pair_7d = EXCLUDED.volume_by_pair_7d, price_by_pair_7d = EXCLUDED.price_by_pair_7d, volume_by_pair_30d = EXCLUDED.volume_by_pair_30d, price_by_pair_30d = EXCLUDED.price_by_pair_30d, volume_by_pair_1y = EXCLUDED.volume_by_pair_1y, price_by_pair_1y = EXCLUDED.price_by_pair_1y"
				queryStatement := fmt.Sprintf("INSERT INTO %s VALUES %s %s", tableName, strings.Join(valueString, ","), updateStatement)
		
				_, inserterExchangeError := pg.Exec(queryStatement, valueExchanges...)
		
				if inserterExchangeError != nil {
					log.Error("error: %s", inserterExchangeError)
					return inserterExchangeError
				}
	
				valueString = make([]string, 0, len(marketPairs))
				valueExchanges = make([]interface{}, 0, len(marketPairs)*13)
				i = 0
		}
	}
	return nil
}


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

bigQuery.go


func (bq *BQStore) GetExchangesMarketTicker() ([]models.ExchangeMarketTickers, error) {
	ctx := context.Background()

	query := bq.Query(`
	with 
		ExchangesPrices AS (
				SELECT
					Base AS Symbol,
					Exchange,
					AVG(Price) AS Price
				FROM
					api-project-901373404215.digital_assets.nomics_exchange_market_ticker
				WHERE
					Exchange NOT IN ('bitmex',
					'hbtc')
					AND Type = 'spot'
					AND Timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 1 Day)
					AND Status = 'active'
					AND Quote IN ('USD',
					'USDT',
					'USDC')
				GROUP BY
					Base,
					Exchange ),
				exchangeHighLight AS (
				SELECT
					COUNT(DISTINCT(MARKET)) AS num_markets,
					Base AS Symbol,
					Exchange
				FROM
					api-project-901373404215.digital_assets.nomics_exchange_market_ticker
				WHERE
					Type = "spot"
					AND Status = "active"
					AND Timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 1 Day)
				GROUP BY
					base,
					Exchange ),
		oneDay AS (
				SELECT
					exchange,
					base AS Symbol,
					volume
				FROM (
					SELECT
						exchange,
						AVG(OneD.Volume) AS volume,
						Base
					FROM
						api-project-901373404215.digital_assets.nomics_exchange_market_ticker
					WHERE
						Timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
						AND OneD.Volume IS NOT NULL
						AND type = "spot"
						AND Status = "active"
					GROUP BY
						exchange,
						base ) AS oneDay 
					),
			sevenDay AS (
				SELECT
					exchange,
					base AS Symbol,
					volume
				FROM (
					SELECT
						exchange,
						AVG(OneD.Volume) AS volume,
						Base
					FROM
						api-project-901373404215.digital_assets.nomics_exchange_market_ticker
					WHERE
						Timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
						AND OneD.Volume IS NOT NULL
						AND type = "spot"
						AND Status = "active"
					GROUP BY
						exchange,
						base ) AS sevenDay 
					),
		thirtyDay AS (
				SELECT
					exchange,
					base AS Symbol,
					volume
				FROM (
					SELECT
						exchange,
						AVG(OneD.Volume) AS volume,
						Base
					FROM
						api-project-901373404215.digital_assets.nomics_exchange_market_ticker
					WHERE
						Timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
						AND OneD.Volume IS NOT NULL
						AND type = "spot"
						AND Status = "active"
					GROUP BY
						exchange,
						base ) AS thirtyDay 
					),
		oneYear AS (
				SELECT
					exchange,
					base AS Symbol,
					volume
				FROM (
					SELECT
						exchange,
						AVG(OneD.Volume) AS volume,
						Base
					FROM
						api-project-901373404215.digital_assets.nomics_exchange_market_ticker
					WHERE
						Timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
						AND OneD.Volume IS NOT NULL
						AND type = "spot"
						AND Status = "active"
					GROUP BY
						exchange,
						base ) AS oneYear 
					),
		oneDayPrice AS (
				SELECT
					CAST(Close AS FLOAT64) price_by_exchange_1d,
					base AS symbol,
					exchange
				FROM (
					SELECT
						AVG(price) AS Close,
						base,
						exchange
					FROM
						api-project-901373404215.digital_assets.nomics_exchange_market_ticker
					WHERE
						Timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
						AND quote IN ('USD',
							'USDT',
							'USDC')
						AND type = "spot"
						AND Status = "active"
					GROUP BY
						base,
						exchange ) AS oneDay ),
		sevenDayPrice AS (
				SELECT
					CAST(Close AS FLOAT64) price_by_exchange_7d,
					base AS symbol,
					exchange
				FROM (
					SELECT
						AVG(price) AS Close,
						base,
						exchange
					FROM
						api-project-901373404215.digital_assets.nomics_exchange_market_ticker
					WHERE
						Timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
						AND quote IN ('USD',
							'USDT',
							'USDC')
						AND type = "spot"
						AND Status = "active"
					GROUP BY
						base,
						exchange ) AS sevenDay ),
		thirtyDayPrice AS (
				SELECT
					CAST(Close AS FLOAT64) price_by_exchange_30d,
					base AS symbol,
					exchange
				FROM (
					SELECT
						AVG(price) AS Close,
						base,
						exchange
					FROM
						api-project-901373404215.digital_assets.nomics_exchange_market_ticker
					WHERE
						Timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
						AND quote IN ('USD',
							'USDT',
							'USDC')
						AND type = "spot"
						AND Status = "active"
					GROUP BY
						base,
						exchange ) AS thirtyDay ),
		oneYearPrice AS (
				SELECT
					CAST(Close AS FLOAT64) price_by_exchange_1y,
					base AS symbol,
					exchange
				FROM (
					SELECT
						AVG(price) AS Close,
						base,
						exchange
					FROM
						api-project-901373404215.digital_assets.nomics_exchange_market_ticker
					WHERE
						Timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
						AND quote IN ('USD',
							'USDT',
							'USDC')
						AND type = "spot"
						AND Status = "active"
					GROUP BY
						base,
						exchange ) AS oneYear )

		SELECT
				oneDay.Symbol as symbol,
				oneDay.exchange,
				exchangeHighLight.num_markets,
				ExchangesPrices.Price as price,
				CAST(oneDay.volume AS float64) AS volume_by_exchange_1d,
				CAST(oneDayPrice.price_by_exchange_1d AS FLOAT64) AS price_by_exchange_1d,
				CAST(sevenDay.volume AS float64) AS volume_by_exchange_7d,
				CAST(sevenDayPrice.price_by_exchange_7d AS FLOAT64) AS price_by_exchange_7d,
				CAST(thirtyDay.volume AS float64) AS volume_by_exchange_30d,
				CAST(thirtyDayPrice.price_by_exchange_30d AS FLOAT64) AS price_by_exchange_30d,
				CAST(oneYear.volume AS float64) AS volume_by_exchange_1y,
				CAST(oneYearPrice.price_by_exchange_1y AS FLOAT64) AS price_by_exchange_1y
			FROM
				ExchangesPrices
				INNER JOIN
					exchangeHighLight
				ON
					exchangeHighLight.symbol = ExchangesPrices.symbol
					AND exchangeHighLight.Exchange = ExchangesPrices.Exchange
				INNER JOIN
					oneDay
				ON
					oneDay.symbol = ExchangesPrices.symbol
					AND oneDay.Exchange = ExchangesPrices.Exchange
				INNER JOIN
					oneDayPrice
				ON
					oneDayPrice.symbol = oneDay.symbol
					AND onedayPrice.Exchange = oneDay.Exchange
			INNER JOIN
					sevenDay
				ON
					sevenDay.symbol = oneDay.symbol
					AND sevenDay.Exchange = oneDay.Exchange
			INNER JOIN
					sevenDayPrice
				ON
					sevenDayPrice.symbol = oneDay.symbol
					AND sevenDayPrice.Exchange = oneDay.Exchange
			INNER JOIN
					thirtyDay
				ON
					thirtyDay.symbol = oneDay.symbol
					AND thirtyDay.Exchange = oneDay.Exchange
			INNER JOIN
					thirtyDayPrice
				ON
					thirtyDayPrice.symbol = thirtyDay.symbol
					AND thirtyDayPrice.Exchange = thirtyDay.Exchange
			INNER JOIN
					oneYear
				ON
					oneYear.symbol = oneDay.symbol
					AND oneYear.Exchange = oneDay.Exchange
			INNER JOIN
					oneYearPrice
				ON
					oneYearPrice.symbol = oneYear.symbol
					AND oneYearPrice.Exchange = oneYear.Exchange
				WHERE
					oneDay.volume IS NOT NULL
					AND oneDayPrice.price_by_exchange_1d IS NOT null
					AND sevenDayPrice.price_by_exchange_7d IS NOT null
					AND thirtyDayPrice.price_by_exchange_30d IS NOT null
					AND oneYearPrice.price_by_exchange_1y IS NOT null
		`)

	job, err := query.Run(ctx)

	if err != nil {
		return nil, err
	}

	log.Debug("Exchange Market Ticker Job ID %s", job.ID())

	status, err := job.Wait(ctx)
	if err != nil {
		return nil, err
	}

	if err := status.Err(); err != nil {
		return nil, err
	}

	it, err := job.Read(ctx)
	if err != nil {
		return nil, err
	}

	var exchangeMarketTickers []models.ExchangeMarketTickers

	for {
		var exchangeMarketTicker models.ExchangeMarketTickers

		err := it.Next(&exchangeMarketTicker)

		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		exchangeMarketTickers = append(exchangeMarketTickers, exchangeMarketTicker)

	}

	return exchangeMarketTickers, nil

}

func (bq *BQStore) GetHighLowFundamentals() ([]models.FundamentalsHighLows, error) {
	ctx := context.Background()

	query := bq.Query(`
	WITH
	allTime AS (
	SELECT
		CAST(MIN(Close) AS FLOAT64) all_time_low,
		CAST(MAX(Close) AS FLOAT64) all_time_high,
		Id AS symbol
	FROM (
		SELECT
		Price AS Close,
		Id
		FROM
			api-project-901373404215.digital_assets.nomics_currencies ) AS allTime
	GROUP BY
		Id ),
	oneDay AS (
	SELECT
		CAST(MAX(Close) AS FLOAT64) high_1d,
		CAST(MIN(Close) AS FLOAT64) low_1d,
		Id AS symbol
	FROM (
		SELECT
		Price AS Close,
		Id
		FROM
			api-project-901373404215.digital_assets.nomics_currencies
		WHERE
		PriceTimestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 24 Hour) ) AS oneDay
	GROUP BY
		Id ),
	sevenDays AS (
	SELECT
		CAST(MAX(Close) AS FLOAT64) high_7d,
		CAST(MIN(Close) AS FLOAT64) low_7d,
		Id AS symbol
	FROM (
		SELECT
		Price AS Close,
		Id
		FROM
			api-project-901373404215.digital_assets.nomics_currencies
		WHERE
		PriceTimestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 7 DAY) ) AS sevenDays
	GROUP BY
		Id ),
	thirtyDays AS (
	SELECT
		CAST(MAX(Close) AS FLOAT64) high_30d,
		CAST(MIN(Close) AS FLOAT64) low_30d,
		Id AS symbol
	FROM (
		SELECT
		Price AS Close,
		Id
		FROM
			api-project-901373404215.digital_assets.nomics_currencies
		WHERE
		PriceTimestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 30 DAY) ) AS thirtyDays
	GROUP BY
		Id ),
	oneYear AS (
	SELECT
		CAST(MAX(Close) AS FLOAT64) high_1y,
		CAST(MIN(Close) AS FLOAT64) low_1y,
		Id AS symbol
	FROM (
		SELECT
		Price AS Close,
		Id
		FROM
			api-project-901373404215.digital_assets.nomics_currencies
		WHERE
		PriceTimestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 365 DAY) ) AS oneYear
	GROUP BY
		Id )
	SELECT
		CAST(MAX(oneDay.high_1d) AS FLOAT64) AS high_24h,
		CAST(MIN(oneDay.low_1d) AS FLOAT64) AS low_24h,
		CAST(MAX(sevenDays.high_7d) AS FLOAT64) AS high_7d,
		CAST(MIN(sevenDays.low_7d) AS FLOAT64) AS low_7d,
		CAST(MAX(thirtyDays.high_30d) AS FLOAT64) AS high_30d,
		CAST(MIN(thirtyDays.low_30d) AS FLOAT64) AS low_30d,
		CAST(MAX(oneYear.high_1y) AS FLOAT64) AS high_1y,
		CAST(MIN(oneYear.low_1y) AS FLOAT64) AS low_1y,
		CAST(MIN(allTime.all_time_low) AS FLOAT64) AS all_time_low,
		CAST(MAX(allTime.all_time_high) AS FLOAT64) AS all_time_high,
		oneDay.symbol
	FROM
		oneDay
	INNER JOIN
		sevenDays
	ON
		sevenDays.symbol = oneDay.symbol
	INNER JOIN
		thirtyDays
	ON
		thirtyDays.symbol = oneDay.symbol
	INNER JOIN
		oneYear
	ON
		oneYear.symbol = oneDay.symbol
	INNER JOIN
		allTime
	ON
		allTime.symbol = oneDay.symbol
	GROUP BY
		oneDay.symbol
	`)

	job, err := query.Run(ctx)
	if err != nil {
		return nil, err
	}
	log.Debug("Fundamentals High Lows Job ID %s", job.ID())

	status, err := job.Wait(ctx)
	if err != nil {
		return nil, err
	}

	if err := status.Err(); err != nil {
		return nil, err
	}

	it, err := job.Read(ctx)

	if err != nil {
		return nil, err
	}

	var highLowFundamentals []models.FundamentalsHighLows

	for {
		var highLowFundamental models.FundamentalsHighLows

		err := it.Next(&highLowFundamental)

		if err == iterator.Done {
			break
		}

		if err != nil {
			return nil, err
		}

		highLowFundamentals = append(highLowFundamentals, highLowFundamental)

	}
	return highLowFundamentals, nil
}

func (bq *BQStore) GetActiveAssets() ([]models.Assets, error) {
	ctx := context.Background()

	query := bq.Query(`
	with allMarkets as(
		SELECT distinct Base, Quote FROM api-project-901373404215.digital_assets.nomics_markets
	  ),
	  AllAssets as(
		SELECT distinct Id, Status FROM api-project-901373404215.digital_assets.nomics_currencies
	  )
	  select allMarkets.Base, allMarkets.Quote
	  from allMarkets
	  INNER Join
	  AllAssets
	  on 
		AllAssets.Id = allMarkets.Base
	  where AllAssets.Status = 'active'
	  group by 
	  allMarkets.Base, allMarkets.Quote
	`)

	job, err := query.Run(ctx)

	if err != nil {
		return nil, err
	}
	log.Debug("Active Assets Job ID %s", job.ID())

	status, err := job.Wait(ctx)

	if err != nil {
		return nil, err
	}

	if err := status.Err(); err != nil {
		return nil, err
	}

	it, err := job.Read(ctx)

	if err != nil {
		return nil, err
	}

	var activeAssets []models.Assets

	for {
		var activeAsset models.Assets

		err := it.Next(&activeAsset)

		if err == iterator.Done {
			break
		}

		if err != nil {
			return nil, err
		}

		activeAssets = append(activeAssets, activeAsset)
	}

	return activeAssets, nil
}

func (bq *BQStore) GetInactiveAssets() ([]models.Assets, error) {
	ctx := context.Background()

	query := bq.Query(`
	with allMarkets as(
		SELECT distinct Base, Quote FROM api-project-901373404215.digital_assets.nomics_markets
	  ),
	  AllAssets as(
		SELECT distinct Id, Status FROM api-project-901373404215.digital_assets.nomics_currencies
	  )
	  select allMarkets.Base, allMarkets.Quote
	  from allMarkets
	  INNER Join
	  AllAssets
	  on 
		AllAssets.Id = allMarkets.Base
	  where AllAssets.Status != 'active'
	  group by 
	  allMarkets.Base, allMarkets.Quote
	`)

	job, err := query.Run(ctx)

	if err != nil {
		return nil, err
	}
	log.Debug("Inactive Assets Job ID %s", job.ID())

	status, err := job.Wait(ctx)

	if err != nil {
		return nil, err
	}

	if err := status.Err(); err != nil {
		return nil, err
	}

	it, err := job.Read(ctx)

	if err != nil {
		return nil, err
	}

	var inactiveAssets []models.Assets

	for {
		var inactiveAsset models.Assets

		err := it.Next(&inactiveAsset)

		if err == iterator.Done {
			break
		}

		if err != nil {
			return nil, err
		}

		inactiveAssets = append(inactiveAssets, inactiveAsset)
	}

	return inactiveAssets, nil
}

func (bq *BQStore) GetMarketPairs() ([]models.MarketPairs, error) {
	ctx := context.Background()

	query := bq.Query(`
	with market as (
		SELECT Base, Exchange, Quote, CONCAT(Base, Quote) as pair
		FROM
		  api-project-901373404215.digital_assets.nomics_markets
		GROUP BY 
		  Base, Exchange, Quote
	  ),
	  oneDayPrice AS (
			  SELECT
				  CAST(Close AS FLOAT64) price_by_pair_1d,
				  CAST(volume AS FLOAT64) volume_by_pair_1d,
				  base AS symbol,
				  quote,
			exchange,
			type
			  FROM (
				  SELECT
					  AVG(price) AS Close,
					  base,
					  quote,
			  exchange,
			  type,
			  AVG(OneD.Volume) AS volume
				  FROM
					  api-project-901373404215.digital_assets.nomics_exchange_market_ticker
				  WHERE
					  Timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
					  AND quote IN ('USD',
						  'USDT',
						  'USDC')
					  AND type = "spot"
					  AND Status = "active"
			  AND OneD.Volume IS NOT NULL
				  GROUP BY
					  base,
					  quote,
			  exchange,
			  type ) AS oneDay ),
	  sevenDayPrice AS (
			  SELECT
				  CAST(Close AS FLOAT64) price_by_pair_7d,
			CAST(volume AS FLOAT64) volume_by_pair_7d,
				  base AS symbol,
				  quote,
			exchange
			  FROM (
				  SELECT
					  AVG(price) AS Close,
					  base,
					  quote,
			  exchange,
			  AVG(OneD.Volume) AS volume
				  FROM
					  api-project-901373404215.digital_assets.nomics_exchange_market_ticker
				  WHERE
					  Timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
					  AND quote IN ('USD',
						  'USDT',
						  'USDC')
					  AND type = "spot"
					  AND Status = "active"
			  AND OneD.Volume IS NOT NULL
				  GROUP BY
					  base,
			  exchange,
					  quote ) AS sevenDay ),
	  thirtyDayPrice AS (
			  SELECT
				  CAST(Close AS FLOAT64) price_by_pair_30d,
			CAST(volume AS FLOAT64) volume_by_pair_30d,
				  base AS symbol,
			exchange,
				  quote
			  FROM (
				  SELECT
					  AVG(price) AS Close,
					  base,
			  exchange,
					  quote,
			  AVG(OneD.Volume) AS volume
				  FROM
					  api-project-901373404215.digital_assets.nomics_exchange_market_ticker
				  WHERE
					  Timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
					  AND quote IN ('USD',
						  'USDT',
						  'USDC')
					  AND type = "spot"
					  AND Status = "active"
			  AND OneD.Volume IS NOT NULL
				  GROUP BY
					  base,
			  exchange,
					  quote ) AS thirtyDay ),
	  oneYearPrice AS (
			  SELECT
				  CAST(Close AS FLOAT64) price_by_pair_1y,
			CAST(volume AS FLOAT64) volume_by_pair_1y,
				  base AS symbol,
			exchange,
				  quote
			  FROM (
				  SELECT
					  AVG(price) AS Close,
					  base,
			  exchange,
					  quote,
			  AVG(OneD.Volume) AS volume
				  FROM
					  api-project-901373404215.digital_assets.nomics_exchange_market_ticker
				  WHERE
					  Timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
					  AND quote IN ('USD',
						  'USDT',
						  'USDC')
					  AND type = "spot"
					  AND Status = "active"
			  AND OneD.Volume IS NOT NULL
				  GROUP BY
					  base,
			  exchange,
					  quote ) AS oneYear )
	  
	  SELECT 
		market.Base, 
		market.Exchange, 
		market.Quote, 
		oneDayPrice.type,
		market.pair,
		oneDayPrice.volume_by_pair_1d,
		oneDayPrice.price_by_pair_1d,
		sevenDayPrice.volume_by_pair_7d,  
		sevenDayPrice.price_by_pair_7d,  
		thirtyDayPrice.volume_by_pair_30d,
		thirtyDayPrice.price_by_pair_30d,
		oneYearPrice.volume_by_pair_1y,
		oneYearPrice.price_by_pair_1y
	  
	  FROM
		market
	  INNER JOIN
		oneDayPrice
	  ON
		oneDayPrice.symbol = market.Base
		AND oneDayPrice.quote = market.Quote
		AND oneDayPrice.exchange = market.Exchange
	  INNER JOIN
		sevenDayPrice
	  ON
		sevenDayPrice.symbol = market.Base
		AND sevenDayPrice.quote = market.Quote
		AND sevenDayPrice.exchange = market.Exchange
	  INNER JOIN
		thirtyDayPrice
	  ON
		thirtyDayPrice.symbol = market.Base
		AND thirtyDayPrice.quote = market.Quote
		AND thirtyDayPrice.exchange = market.Exchange
	  INNER JOIN
		oneYearPrice
	  ON
		oneYearPrice.symbol = market.Base
		AND oneYearPrice.quote = market.Quote
		AND oneYearPrice.exchange = market.Exchange
	WHERE 
		market.Base IS NOT NULL 
		AND market.Exchange IS NOT NULL  
		AND market.Quote IS NOT NULL 
		AND market.pair IS NOT NULL 
		AND oneDayPrice.type IS NOT NULL
		AND oneDayPrice.price_by_pair_1d IS NOT NULL
		AND oneDayPrice.volume_by_pair_1d IS NOT NULL
		AND sevenDayPrice.price_by_pair_7d IS NOT NULL
		AND sevenDayPrice.volume_by_pair_7d IS NOT NULL
		AND thirtyDayPrice.price_by_pair_30d IS NOT NULL
		AND thirtyDayPrice.volume_by_pair_30d IS NOT NULL
		AND oneYearPrice.price_by_pair_1y IS NOT NULL
		AND oneYearPrice.volume_by_pair_1y IS NOT NULL
	`)

	job, err := query.Run(ctx)

	if err != nil {
		return nil, err
	}
	log.Debug("Market Pairs Job ID %s", job.ID())

	status, err := job.Wait(ctx)

	if err != nil {
		return nil, err
	}

	if err := status.Err(); err != nil {
		return nil, err
	}

	it, err := job.Read(ctx)

	if err != nil {
		return nil, err
	}

	var marketPairs []models.MarketPairs

	for {
		var marketPair models.MarketPairs

		err := it.Next(&marketPair)

		if err == iterator.Done {
			break
		}

		if err != nil {
			return nil, err
		}

		marketPairs = append(marketPairs, marketPair)
	}

	return marketPairs, nil
}


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

postgresMapper.go


func MapExchangeMarketTicker(exchanges []models.ExchangeMarketTicker) []models.PGExchangeMarketsTicker {

	var exchangeMarketTickers []models.PGExchangeMarketsTicker

	for _, exchange := range exchanges {
		var exchangeMarketTicker models.PGExchangeMarketsTicker
		exchangeMarketTicker.Base = exchange.Base
		exchangeMarketTicker.Exchange = exchange.Exchange
		exchangeMarketTicker.Market = exchange.Market
		exchangeMarketTicker.Quote = exchange.Quote
		exchangeMarketTicker.Type = exchange.Type
		exchangeMarketTicker.Price = &exchange.Price
		exchangeMarketTicker.Status = exchange.Status
		exchangeMarketTicker.Timestamp = exchange.Timestamp
		exchangeMarketTicker.OneD.Volume = &exchange.Volume
		exchangeMarketTickers = append(exchangeMarketTickers, exchangeMarketTicker)
	}

	return exchangeMarketTickers
}
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
schema.sql

CREATE TABLE "fundamentals_high_low_test" (
	"symbol" TEXT,
	"high_24h" FLOAT,
	"low_24h" FLOAT,
	"high_7d" FLOAT,
	"low_7d" FLOAT,
	"high_30d" FLOAT,
	"low_30d" FLOAT,
	"high_1y" FLOAT,
	"low_1y" FLOAT,
	"all_time_high" FLOAT,
	"all_time_low" FLOAT,
	PRIMARY KEY ("symbol")
  );
  CREATE INDEX ON "fundamentals_high_low_test" ("symbol");
  
  
  CREATE TABLE "exchange_market_ticker_test" (
	"symbol" TEXT,
	"exchange" TEXT,
	"num_markets" NUMERIC,
	"price" FLOAT,
	"volume_by_exchange_1d" FLOAT,
	"price_by_exchange_1d" FLOAT,
	"volume_by_exchange_7d" FLOAT,
	"price_by_exchange_7d" FLOAT,
	"volume_by_exchange_30d" FLOAT,
	"price_by_exchange_30d" FLOAT,
	"volume_by_exchange_1y" FLOAT,
	"price_by_exchange_1y" FLOAT,
	PRIMARY KEY ("symbol", "exchange")
  );
  CREATE INDEX ON "exchange_market_ticker_test" ("symbol");
  CREATE INDEX ON "exchange_market_ticker_test" ("exchange");
  
  CREATE TABLE "active_markets" (
	"base" TEXT,
	"quote" TEXT,
	PRIMARY KEY ("base", "quote")
  );
  CREATE INDEX ON "active_markets" ("base");
  CREATE INDEX ON "active_markets" ("quote");
  
  
  CREATE TABLE "inactive_markets" (
	"base" TEXT,
	"quote" TEXT,
	PRIMARY KEY ("base", "quote")
  );
  CREATE INDEX ON "inactive_markets" ("base");
  CREATE INDEX ON "inactive_markets" ("quote");
  
  CREATE TABLE "market_pairs" (
	"symbol" TEXT,
	"exchange" TEXT,
	"quote" TEXT,
	"type" TEXT,
	"pair" TEXT,
	"volume_by_pair_1d" FLOAT,
	"price_by_pair_1d" FLOAT,
	"volume_by_pair_7d" FLOAT,
	"price_by_pair_7d" FLOAT,
	"volume_by_pair_30d" FLOAT,
	"price_by_pair_30d" FLOAT,
	"volume_by_pair_1y" FLOAT,
	"price_by_pair_1y" FLOAT,
	PRIMARY KEY ("symbol", "exchange", "quote")
  );
  CREATE INDEX ON "market_pairs" ("symbol");
  CREATE INDEX ON "market_pairs" ("exchange");
  CREATE INDEX ON "market_pairs" ("quote");
  CREATE INDEX ON "market_pairs" ("type");
  CREATE INDEX ON "market_pairs" ("pair");

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
bigQuerytype.go


type ExchangeMarketTicker struct {
	Base             string    `bigquery:"base" json:"base"`
	Exchange         string    `bigquery:"exchange" json:"exchange"`
	Market           string    `bigquery:"market" json:"market"`
	Quote            string    `bigquery:"quote" json:"quote"`
	Type             string    `bigquery:"type" json:"type"`
	SubType          string    `bigquery:"supType" json:"supType"`
	Aggregated       bool      `bigquery:"aggregated" json:"aggregated"`
	PriceExclude     bool      `bigquery:"priceExclude" json:"priceExclude"`
	VolumeExclude    bool      `bigquery:"volumeExclude" json:"volumeExclude"`
	BaseSymbol       string    `bigquery:"baseSymbol" json:"baseSymbol"`
	QuoteSymbol      string    `bigquery:"quoteSymbol" json:"quoteSymbol"`
	Price            float64   `bigquery:"price" json:"price"`
	PriceQuote       float64   `bigquery:"priceQuote" json:"priceQuote"`
	VolumeUsd        float64   `bigquery:"volumeUsd" json:"volumeUsd"`
	Status           string    `bigquery:"status" json:"status"`
	Weight           string    `bigquery:"weight" json:"weight"`
	FirstTrade       time.Time `bigquery:"firstTrade" json:"firstTrade"`
	FirstCandle      time.Time `bigquery:"firstCandle" json:"firstCandle"`
	FirstOrderBook   time.Time `bigquery:"firstOrderBook" json:"firstOrderBook"`
	Timestamp        time.Time `bigquery:"timestamp" json:"timestamp"`
	Volume           float64   `bigquery:"volume" json:"volume"`
	VolumeBase       float64   `bigquery:"volumeBase" json:"volumeBase"`
	VolumeBaseChange float64   `bigquery:"volumeBaseChange" json:"volumeBaseChange"`
	VolumeChange     float64   `bigquery:"volumeChange" json:"volumeChange"`
	Trades           float64   `bigquery:"trades" json:"trades"`
	TradesChange     float64   `bigquery:"tradesChange" json:"tradesChange"`
	PriceChange      float64   `bigquery:"priceChange" json:"priceChange"`
	PriceQuoteChange float64   `bigquery:"priceQuoteChange" json:"priceQuoteChange"`
	LastUpdated      time.Time `bigquery:"lastUpdated" json:"lastUpdated"`
}

func ConvertBQFloatToFloat(bqVal bigquery.NullFloat64) *float64 {
	if bqVal.Valid {
		return &bqVal.Float64
	}
	return nil
}

type FundamentalsHighLows struct {
	Symbol      string  `json:"symbol" bigquery:"symbol"`
	High24H     float64 `bigquery:"high_24h" json:"high_24h"`
	Low24H      float64 `bigquery:"low_24h" json:"low_24h"`
	High7D      float64 `bigquery:"high_7d" json:"high_7d"`
	Low7D       float64 `bigquery:"low_7d" json:"low_7d"`
	High30D     float64 `bigquery:"high_30d" json:"high_30d"`
	Low30D      float64 `bigquery:"low_30d" json:"low_30d"`
	High1Y      float64 `bigquery:"high_1y" json:"high_1y"`
	Low1Y       float64 `bigquery:"low_1y" json:"low_1y"`
	AllTimeHigh float64 `bigquery:"all_time_high" json:"all_time_high"`
	AllTimeLow  float64 `bigquery:"all_time_low" json:"all_time_low"`
}

type ExchangeMarketTickers struct {
	Symbol              string  `bigquery:"symbol" json:"symbol"`
	Exchange            string  `bigquery:"exchange" json:"exchange"`
	NumMarkets          int64   `bigquery:"num_markets" json:"num_markets"`
	Price               float64 `bigquery:"price" json:"price"`
	VolumeByExchange1D  float64 `bigquery:"volume_by_exchange_1d" json:"volume_by_exchange_1d"`
	PriceByExchange1D   float64 `bigquery:"price_by_exchange_1d" json:"price_by_exchange_1d"`
	VolumeByExchange7D  float64 `bigquery:"volume_by_exchange_7d" json:"volume_by_exchange_7d"`
	PriceByExchange7D   float64 `bigquery:"price_by_exchange_7d" json:"price_by_exchange_7d"`
	VolumeByExchange30D float64 `bigquery:"volume_by_exchange_30d" json:"volume_by_exchange_30d"`
	PriceByExchange30D  float64 `bigquery:"price_by_exchange_30d" json:"price_by_exchange_30d"`
	VolumeByExchange1Y  float64 `bigquery:"volume_by_exchange_1y" json:"volume_by_exchange_1y"`
	PriceByExchange1Y   float64 `bigquery:"price_by_exchange_1y" json:"price_by_exchange_1y"`
}

type Assets struct {
	Base  string `json:"base" bigquery:"base"`
	Quote string `json:"quote" bigquery:"quote"`
}

type MarketPairs struct {
	Symbol          string  `bigquery:"Base" json:"symbol"`
	Exchange        string  `bigquery:"Exchange" json:"exchange"`
	Quote           string  `bigquery:"Quote" json:"quote"`
	Type            string  `bigquery:"type" json:"type"`
	Pair            string  `bigquery:"pair" json:"pair"`
	VolumeByPair1D  float64 `bigquery:"volume_by_pair_1d" json:"volume_by_pair_1d"`
	PriceByPair1D   float64 `bigquery:"price_by_pair_1d" json:"price_by_pair_1d"`
	VolumeByPair7D  float64 `bigquery:"volume_by_pair_7d" json:"volume_by_pair_7d"`
	PriceByPair7D   float64 `bigquery:"price_by_pair_7d" json:"price_by_pair_7d"`
	VolumeByPair30D float64 `bigquery:"volume_by_pair_30d" json:"volume_by_pair_30d"`
	PriceByPair30D  float64 `bigquery:"price_by_pair_30d" json:"price_by_pair_30d"`
	VolumeByPair1Y  float64 `bigquery:"volume_by_pair_1y" json:"volume_by_pair_1y"`
	PriceByPair1Y   float64 `bigquery:"price_by_pair_1y" json:"price_by_pair_1y"`
}

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

datacunsumer.go 
func GetExchangeMarketTickers(w http.ResponseWriter, r *http.Request) {
	var labels = make(map[string]string)
	var startTime = time.Now()

	labels["handler"] = "GetExchangeMarketTickersBQ"
	log.InfoL(labels, "START GetExchangeMarketTickers()")

	go func() {
		bq, err := store.NewBQStore()
		if err != nil {
			log.Error("Error creating BQStore: %s", err)
			return
		}

		result, err := bq.GetExchangesMarketTicker()

		if err != nil {
			log.Error("Error Getting data from Exchanges MArket BQStore: %s", err)
			return
		}

		// mapExchangeMarketTickers := service.MapExchangeMarketTicker(result)

		inserterErr := store.InsertExchangesProcedure(result)

		if inserterErr != nil {
			log.ErrorL(labels, "Error inserting Exchange Markets Ticker Data to PG: %s", inserterErr)
			return
		}
		log.InfoL(labels, "END store.InsertExchangeMarketsTickerData totalTime:%.2fs", time.Since(startTime).Seconds())
	}()

	w.WriteHeader(200)
	w.Write([]byte("OK"))
}

func GetHighLowsFOrFundamentalsBQ(w http.ResponseWriter, r *http.Request) {
	var labels = make(map[string]string)
	var startTime = time.Now()

	labels["handler"] = "BuildHighLowsFundamentals"

	log.InfoL(labels, "Start GetHighLowsFOrFundamentalsBQ")

	bq, err := store.NewBQStore()

	if err != nil {
		log.Error("Error creating BQStore: %s", err)
		return
	}

	result, err := bq.GetHighLowFundamentals()

	if err != nil {
		log.Error("Error Getting data from Fundamentals BQStore: %s", err)
		return
	}

	inserterErr := store.InsertHighLowsFundamentals(result)

	if inserterErr != nil {
		log.ErrorL(labels, "Error inserting High Lows Fundamentals Data to PG: %s", inserterErr)
		return
	}

	log.InfoL(labels, "END store.BuildHighLowsFundamentals totalTime:%.2fs", time.Since(startTime).Seconds())

	w.WriteHeader(200)
	w.Write([]byte("OK"))
}

func GetActiveAssetsBQ(w http.ResponseWriter, r *http.Request) {
	var labels = make(map[string]string)

	var startTime = time.Now()

	labels["handler"] = "GetActiveAssetsBQ"

	log.InfoL(labels, "Start GetActiveAssetsBQ")

	bq, err := store.NewBQStore()
	if err != nil {
		log.Error("Error creating BQStore: %s", err)
		return
	}

	// build active assets
	activeAssetsResult, err := bq.GetActiveAssets()

	if err != nil {
		log.Error("Error Getting data from Active Assets BQStore: %s", err)
		return
	}

	activeAssetsInserterErr := store.InsertActiveAssets(activeAssetsResult)
	if activeAssetsInserterErr != nil {
		log.ErrorL(labels, "Error inserting Active Assets Data to PG: %s", activeAssetsInserterErr)
		return
	}

	// build inactive assets
	inactiveAssetsResult, err := bq.GetInactiveAssets()

	if err != nil {
		log.Error("Error Getting data from InActive Assets BQStore: %s", err)
		return
	}

	inactiveAssetsInserterErr := store.InsertInactiveAssets(inactiveAssetsResult)
	if inactiveAssetsInserterErr != nil {
		log.ErrorL(labels, "Error inserting InActive Assets Data to PG: %s", inactiveAssetsInserterErr)
		return
	}

	log.InfoL(labels, "END store.InsertActiveAssets totalTime:%.2fs", time.Since(startTime).Seconds())

	w.WriteHeader(200)
	w.Write([]byte("OK"))
}


func GetMarketPairsBQ(w http.ResponseWriter, r *http.Request){

	var labels = make(map[string]string)
	var startTime = time.Now()

	log.InfoL(labels, "Start GetMarketPairsBQ")

	bq, err := store.NewBQStore()
	if err != nil {
		log.Error("Error creating BQStore: %s", err)
		return
	}

	result, err := bq.GetMarketPairs()
	if err != nil {
		log.Error("Error Getting data from Market Pairs BQStore: %s", err)
		return
	}

	inserterErr := store.InsertMarketPairs(result)
	if inserterErr != nil {
		log.ErrorL(labels, "Error inserting Market Pairs Data to PG: %s", inserterErr)
		return
	}
	log.InfoL(labels, "END store.InsertActiveAssets totalTime:%.2fs", time.Since(startTime).Seconds())

	w.WriteHeader(200)
	w.Write([]byte("OK"))


}


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

main.go

r.Handle("/get-exchange-market-ticker-bq", oidcAuthMiddleware.Middleware(http.HandlerFunc(internal.GetExchangeMarketTickers))).Methods(http.MethodPost)
r.Handle("/get-high_low-bq", oidcAuthMiddleware.Middleware(http.HandlerFunc(internal.GetHighLowsFOrFundamentalsBQ))).Methods(http.MethodPost)
r.Handle("/get-active-inactive-bq", oidcAuthMiddleware.Middleware(http.HandlerFunc(internal.GetActiveAssetsBQ))).Methods(http.MethodPost)
r.Handle("/get-market_pairs-bq", oidcAuthMiddleware.Middleware(http.HandlerFunc(internal.GetMarketPairsBQ))).Methods(http.MethodPost)


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


package services

import (
	"context"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/api/iterator"
)

type Section struct {
	Name        string             `json:"name" firestore:"name"`
	BertieTag   string             `json:"bertieTag" firestore:"bertieTag"`
	Description string             `json:"description" firestore:"description"`
	Order       string             `json:"order" firestore:"order"`
	Articles    []EducationArticle `json:"articles" firestore:"articles"`
}
type EducationArticle struct {
	Id                string    `json:"id" firestore:"id"`
	Title             string    `json:"title" firestore:"title"`
	Image             string    `json:"image" firestore:"image"`
	ArticleURL        string    `json:"articleURL" firestore:"articleURL"`
	Author            string    `json:"author" firestore:"author"`
	Type              string    `json:"type" firestore:"type"`
	AuthorType        string    `json:"authorType" firestore:"authorType"`
	AuthorLink        string    `json:"authorLink" firestore:"authorLink"`
	Description       string    `json:"description" firestore:"description"`
	PublishDate       time.Time `json:"publishDate" firestore:"publishDate"`
	Disabled          bool      `json:"disabled" firestore:"disabled"`
	SeniorContributor bool      `json:"seniorContributor" firestore:"seniorContributor"`
	BylineFormat      *int64    `json:"bylineFormat" firestore:"bylineFormat"`
}

type Education struct {
	Section []Section `json:"sections" firestore:"sections"`
}

func GetEducationContentFromBertie(name string, span trace.Span, contentDataSet string, order string) ([]EducationArticle, error) {
	ctx := context.Background()
	client, err := bigquery.NewClient(context.Background(), "api-project-901373404215")
	if err != nil {
		return nil, err
	}

	span.AddEvent("Start Get Articles Data from BQ")

	var orderColumn string

	if order == "" {
		orderColumn = "date"
	} else {
		orderColumn = order
	}

	// education data query
	query := `
	SELECT
		c.id,
		c.title,
		c.date date,
		c.description,
		c.image,
		c.author,
		c.authorType author_type,
		aut.type type,
		aut.inactive disabled,
		aut.seniorContributor senior_contributor,
		aut.bylineFormat byline_format,
		REPLACE(c.uri, "http://", "https://") AS link,
		REPLACE(aut.url, "http://", "https://") AS author_link
	FROM
	api-project-901373404215.Content.` + contentDataSet + ` c,
		UNNEST(c.channelSection) as channelSection
		LEFT JOIN
		api-project-901373404215.Content.v_author_latest aut
		ON
		c.authorNaturalId = aut.naturalId
	WHERE
		c.visible = TRUE
		AND c.preview = FALSE
		AND c.date <= CURRENT_TIMESTAMP()
		AND c.timestamp <= CURRENT_TIMESTAMP()
		AND c.timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 day)
		AND "all" NOT IN UNNEST(spikeFrom)
		AND ( 
			channelSection in ('` + name + `') 
			or
			c.primaryChannelId= '` + name + `'
		)
	GROUP BY
		1,
		2,
		3,
		4,
		5,
		6,
		7,
		8,
		9,
		10,
		11,
		12,
		13
	ORDER BY
		` + orderColumn + ` DESC
	`

	q := client.Query(query)

	it, err := q.Read(ctx)
	if err != nil {
		span.AddEvent(fmt.Sprintf("Error Getting Articles Data from BQ: %s", err))
		return nil, err
	}
	var imageDomain string
	if contentDataSet == "mv_content_latest" {
		imageDomain = ""
	} else {
		imageDomain = "https://staging.damapi.forbes.com"
	}

	var educationArticle []EducationArticle
	for {
		var articale EducationArticle
		var articleFromBQ ArticleFromBQ
		err := it.Next(&articleFromBQ)
		if err == iterator.Done {
			break
		}
		if err != nil {
			span.AddEvent(fmt.Sprintf("Error Maps Articles Data to Struct: %s", err))
			return nil, err
		}

		if articleFromBQ.Id.Valid {
			articale.Id = articleFromBQ.Id.StringVal
		}
		if articleFromBQ.Title.Valid {
			articale.Title = articleFromBQ.Title.StringVal
		}
		if articleFromBQ.Image.Valid {
			articale.Image = imageDomain + articleFromBQ.Image.StringVal
		}
		if articleFromBQ.Author.Valid {
			articale.Author = articleFromBQ.Author.StringVal
		}
		if articleFromBQ.AuthorLink.Valid {
			articale.AuthorLink = articleFromBQ.AuthorLink.StringVal
		}
		if articleFromBQ.AuthorType.Valid {
			articale.AuthorType = articleFromBQ.AuthorType.StringVal
		}
		if articleFromBQ.Description.Valid {
			articale.Description = articleFromBQ.Description.StringVal
		}
		if articleFromBQ.ArticleURL.Valid {
			articale.ArticleURL = articleFromBQ.ArticleURL.StringVal
		}
		if articleFromBQ.Type.Valid {
			articale.Type = articleFromBQ.Type.StringVal
		}
		if articleFromBQ.Disabled.Valid {
			articale.Disabled = articleFromBQ.Disabled.Bool
		}
		if articleFromBQ.SeniorContributor.Valid {
			articale.SeniorContributor = articleFromBQ.SeniorContributor.Bool
		}
		if articleFromBQ.BylineFormat.Valid {
			articale.BylineFormat = &articleFromBQ.BylineFormat.Int64
		} else {
			articale.BylineFormat = nil
		}
		articale.PublishDate = articleFromBQ.PublishDate

		educationArticle = append(educationArticle, articale)
	}

	return educationArticle, nil
}

func GetEducationSectionData(span trace.Span) ([]Section, error) {
	fs := GetFirestoreClient()
	ctx := context.Background()

	sectionCollection := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "education")

	dbSnap := fs.Collection(sectionCollection).Documents(ctx)
	span.AddEvent("Start Get Section Data from FS")

	var sectionEducation []Section
	for {
		var section Section
		doc, err := dbSnap.Next()

		if err == iterator.Done {
			break
		}

		if err := doc.DataTo(&section); err != nil {
			span.AddEvent(fmt.Sprintf("Error Getting Section Data from FS: %s", err))
			return nil, err
		}

		articles, err := GetEducationContentFromBertie(section.Name, span, "mv_content_latest", section.Order)
		if err != nil {
			log.Info("Error Getting Articles from Bertie BQ: %s", err)
			if err != nil {
				return nil, err
			}
		}
		section.Articles = articles

		sectionEducation = append(sectionEducation, section)

	}
	return sectionEducation, nil

}

func GetEducationData(span trace.Span) (*Education, error) {
	var educationData Education

	span.AddEvent("Start Build Education Data")
	sections, err := GetEducationSectionData(span)

	if err != nil {
		log.Info("Error Getting Sections from FS:  %s", err)
		if err != nil {
			return nil, err
		}
		return &educationData, nil
	}

	educationData.Section = sections

	return &educationData, nil
}






func GetEducationData(w http.ResponseWriter, r *http.Request) {
	// update each 5 min
	setResponseHeaders(w, 300)

	labels := make(map[string]string)

	span := trace.SpanFromContext(r.Context())

	defer span.End()

	labels["UUID"] = uuid.New().String()
	labels["function"] = "GetEducationData"
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTime("Get Education Data")

	result, err := services.GetEducationData(span)

	if err != nil {
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		span.SetStatus(codes.Error, err.Error())
		return
	}

	jsonData, err := json.Marshal(result)

	if err != nil {
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		span.SetStatus(codes.Error, err.Error())
		return
	}


	log.EndTimeL(labels, "Education Data", startTime, nil)
	span.SetStatus(codes.Ok, "Education Data")
	w.WriteHeader(http.StatusOK)
	w.Write(jsonData)

}


education.go

v1.HandleFunc("/education", GetEducationData).Methods(http.MethodGet, http.MethodOptions)








valid := CheckPriceTimestamp(item.PriceTimestamp)
if valid {
	ticker.PriceTimestamp.Valid = true
	ticker.PriceTimestamp.Timestamp = item.PriceTimestamp
} else {
	ticker.PriceTimestamp.Valid = false
}


func CheckPriceTimestamp(check time.Time) bool {
	upperLimit := time.Now().AddDate(0, 0, 366)
	lowerLimit := time.Now().AddDate(0, 0, -1825)

	if check.After(lowerLimit) && check.Before(upperLimit) {
		return true
	}
	return false

}






if result.Symbol == "" {
	result, err = store.GetChartData(interval)
	if err != nil {
		log.Error("%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}



doc["lastUpdated"] = article.LastUpdated


UpdatedBy         map[string]interface{} `json:"-" firestore:"_updatedBy,omitempty"`
LastUpdated       time.Time              `json:"lastUpdated" firestore:"lastUpdated"`


if article.UpdatedBy != nil {
	article.LastUpdated = article.UpdatedBy["timestamp"].(time.Time)
}


article.LastUpdated = sectionArticle.LastUpdated


if articles[i].Order == articles[j].Order {
	return articles[i].LastUpdated.After(articles[j].LastUpdated)
}



postgresqlMapper.go

func MapMarkets(markets []nomics.Markets) []nomics.Markets {
	var pgMarkets []nomics.Markets

	for _, market := range markets {
		var pgMarket nomics.Markets
		pgMarket.Exchange = market.Exchange
		pgMarket.Market = market.Market
		pgMarket.Base = market.Base
		pgMarket.Quote = market.Quote

		pgMarkets = append(pgMarkets, pgMarket)
	}
	return pgMarkets
}

line 123 
datacunsumerHandler.go
marketsMap := service.MapMarkets(allMarkets)

throttleChanMarkets := make(chan bool, 20)
var wgMarkets sync.WaitGroup

batch := len(marketsMap) / 20

for i := 0; i <= len(marketsMap)-batch; i += batch {
	throttleChanMarkets <- true
	wgMarkets.Add(1)
	go func(markets []nomics.Markets) {

		if inserterErr := ConsumeMarketsInserter(&markets, r.Context()); inserterErr != nil {
			//log.Info("Loaded %d Markets for Exchange: %s", len(*markets), marketsOptions.Exchange)
			log.ErrorL(labels, "Error Inserting Markets to BQ or PG: %s", inserterErr)
		}
		<-throttleChanMarkets
		wgMarkets.Done()
	}(marketsMap[i : i+batch])
	wgMarkets.Wait()
}










func SaveEducationSection(ctx context.Context, sections []services.Section) {

	fs := GetFirestoreClient()

	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "education")
	for _, section := range sections {
		fs.Collection(collectionName).Doc(section.DocId).Set(ctx, map[string]interface{}{
			"name":         section.Name,
			"bertieTag":    section.BertieTag,
			"description":  section.Description,
			"sectionOrder": section.SectionOrder,
		}, firestore.MergeAll)
		err := DeleteOldArticles(ctx, collectionName, section.DocId)
		if err != nil {
			log.Info("error Can't Delete Old Articles %s", err.Error())
		}
		for _, article := range section.Articles {
			doc := make(map[string]interface{})
			doc["id"] = article.Id
			doc["title"] = article.Title
			doc["image"] = article.Image
			doc["articleURL"] = article.ArticleURL
			doc["author"] = article.Author
			doc["type"] = article.Type
			doc["authorType"] = article.AuthorType
			doc["authorLink"] = article.AuthorLink
			doc["description"] = article.Description
			doc["publishDate"] = article.PublishDate
			doc["disabled"] = article.Disabled
			doc["seniorContributor"] = article.SeniorContributor
			doc["bylineFormat"] = article.BylineFormat
			doc["bertieTag"] = article.BertieTag
			doc["order"] = article.Order
			doc["lastUpdated"] = article.LastUpdated
			// if article.DocId != "" {
			// 	fs.Collection(collectionName).Doc(section.DocId).Collection("articles").Doc(article.DocId).Set(ctx, doc, firestore.MergeAll)
			// } else {
			fs.Collection(collectionName).Doc(section.DocId).Collection("articles").NewDoc().Set(ctx, doc, firestore.MergeAll)

			// 	// }
		}
	}

}

func DeleteOldArticles(ctx context.Context, collectionName string, sectionDocId string) error {
	fs := GetFirestoreClient()
	batch := GetFirestoreClient().Batch()

	dbSnap := fs.Collection(collectionName).Doc(sectionDocId).Collection("articles").Documents(ctx)
	for {
		doc, err := dbSnap.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			return err
		}
		batch.Delete(doc.Ref)
		_, errBatch := batch.Commit(ctx)
		if errBatch != nil {
			fmt.Println(errBatch)
		}
	}
	return nil
}




// map Articles to each Section by Bertie Tag
func MapArticlesToSection(sections []Section, articles []EducationArticle) ([]Section, error) {
	var educationSection []Section
	for _, section := range sections {
		var educationArticles []EducationArticle
		for _, article := range articles {
			if section.BertieTag == article.BertieTag {
				for _, sectionArticle := range section.Articles {
					// if article exist in section map the new value article to it
					if sectionArticle.Title == article.Title {
						article.DocId = sectionArticle.DocId
						article.Order = sectionArticle.Order
						article.LastUpdated = sectionArticle.LastUpdated
						goto ADDArticles
					}
				}
			ADDArticles:
				educationArticles = append(educationArticles, article)
			}
		}
		SortArticles(educationArticles)
		section.Articles = educationArticles
		educationSection = append(educationSection, section)
	}
	return educationSection, nil
}

// sort articles by order if exist if not exist it will sorted by date
func SortArticles(articles []EducationArticle) {
	sort.Slice(articles, func(i, j int) bool {
		if articles[i].Order > 0 || articles[j].Order > 0 {
			if articles[i].Order == articles[j].Order {
				return articles[i].LastUpdated.After(articles[j].LastUpdated)
			}
			return articles[i].Order < articles[j].Order
		} else {
			return articles[i].PublishDate.After(articles[j].PublishDate)
		}
	})
}








--------------------------------------------------------------------------------------------------------------------------------
//main
r.Handle("/consume-exchanges-list", oidcAuthMiddleware.Middleware(http.HandlerFunc(internal.ConsumeExchangesList))).Methods(http.MethodPost)

// data consumers
func ConsumeExchangesList(w http.ResponseWriter, r *http.Request) {
	labels, span := generateSpan(r, "ConsumeExchangesList")
	defer span.End()

	nomicsRateLimiter.Wait(limiterContext)
	startTime := log.StartTimeL(labels, "ConsumeExchangesList")

	data, err := c.GetExchangesList()

	if err != nil {
		log.EndTimeL(labels, "Error getting Assets: %s", startTime, err)
		w.WriteHeader(http.StatusInternalServerError)
	}

	store.UpsertCoinGeckoExchanges(data)
	coingeckoCalls++
	log.EndTimeL(labels, "ConsumeExchangesList", startTime, nil)
	span.SetStatus(codes.Ok, "OK")
	w.WriteHeader(200)
	w.Write([]byte("OK"))

}


// postgers

func UpsertCoinGeckoExchanges(exchangesList *[]coingecko.ExchangeListShort) error {
	pg := PGConnect()

	exchangesListTMP := *exchangesList
	valueString := make([]string, 0, len(*exchangesList))
	for y:=0; y<len(exchangesListTMP); y++ {
		var exchange = exchangesListTMP[y]
		var valString = fmt.Sprintf("('%s','%s')::coingecko_exchange", exchange.ID, exchange.Name)
		valueString = append(valueString, valString)
	}
	exchangesData := strings.Join(valueString, ",")

	exchangeStoredProc := fmt.Sprintf("CALL upsertCoingeckoExchanges(ARRAY[%s])", exchangesData)
	_, inserterError := pg.Exec(exchangeStoredProc)
	if inserterError != nil {
		log.Error("%s", inserterError)
	}
	return nil

	/*
	exchangesListTMP := *exchangesList
	valueString := make([]string, 0, len(*exchangesList))
	valueArgs := make([]interface{}, 0, len(*exchangesList)*2)
	tableName := "coingecko_exchanges"
	var i = 0 //used for argument positions
	for y:=0; y<len(exchangesListTMP); y++ {
		var exchange = exchangesListTMP[y]
		var valString = fmt.Sprintf("($%d,$%d)", i*2+1, i*2+2)
		//pairsString = append(pairsString, fmt.Sprintf("%s/%s", candleData.Base, candleData.Quote))
		valueString = append(valueString, valString)
		valueArgs = append(valueArgs, exchange.ID)
		valueArgs = append(valueArgs, exchange.Name)
		i++

		if len(valueArgs) >= 65000 || y == len(exchangesListTMP)-1 {
			insertStatementCandles := fmt.Sprintf("INSERT INTO %s VALUES %s", tableName, strings.Join(valueString, ","))
			updateStatement := "ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id, name = EXCLUDED.name"

			query := insertStatementCandles + updateStatement
			_, inserterError := pg.Exec(query, valueArgs...)

			if inserterError != nil {
				log.Error("%s", inserterError)
			}

			valueString = make([]string, 0, len(exchangesListTMP))
			valueArgs = make([]interface{}, 0, len(exchangesListTMP)*2)

			i = 0
		}
	}

	return nil
	*/
}



// schema
create table "coingecko_exchanges" (
	"id" text,
	"name" text,
	primary key("id")
)
//bulkInsertProcess
CREATE TYPE coingecko_exchange as (
    id TEXT,
    name Text
)

CREATE OR REPLACE PROCEDURE upsertCoingeckoExchanges (IN exchanges coingecko_exchange[]) 
AS 
$BODY$
DECLARE
    exchange coingecko_exchange;
BEGIN
    FOREACH exchange in ARRAY exchanges LOOP 
        INSERT INTO coingecko_exchanges(id, name)
        Values (exchange.id, exchange.name)
        ON CONFLICT (id) DO UPDATE id = EXCLUDED.id, name =EXCLUDED.name
    END LOOP;
END;
$BODY$ LANGUAGE plpgsql;

// env
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





--------------------------------------------------------------------------------------------------------------------------------
func GetExchangeMetaDataWithoutLimit(ctxO context.Context, labels map[string]string) ([]model.CoingeckoExchangeMetadata, error) {

	ctx, span := tracer.Start(ctxO, "GetExchangeMetaDataWithoutLimit")
	defer span.End()
	startTime := StartTimeL(labels, "Exchange Fundamental Insert")

	var exchangesMetadata []model.CoingeckoExchangeMetadata
	pg := PGConnect()

	query := `
	SELECT 
		id,
        name, 
        year, 
        description, 
        location, 
        logo_url, 
		website_url, 
        twitter_url, 
        facebook_url, 
        youtube_url, 
		linkedin_url, 
        reddit_url, 
        chat_url, 
        slack_url, 
		telegram_url, 
        blog_url, 
        centralized, 
        decentralized, 
		has_trading_incentive, 
        trust_score, 
        trust_score_rank, 
		trade_volume_24h_btc, 
        trade_volume_24h_btc_normalized, 
        last_updated
	FROM 
		public.coingecko_exchange_metadata
	where 
		trust_score is not null 
 	order by trust_score desc
	`

	queryResult, err := pg.QueryContext(ctx, query)

	if err != nil {
		ConsumeTime("Exchange Metadata Data Query", startTime, err)
		span.SetStatus(otelCodes.Error, err.Error())
		return nil, err
	}

	for queryResult.Next() {
		var exchangeMetadata model.CoingeckoExchangeMetadata

		err := queryResult.Scan(&exchangeMetadata.ID, &exchangeMetadata.Name, &exchangeMetadata.Year, &exchangeMetadata.Description, &exchangeMetadata.Location, &exchangeMetadata.LogoURL, &exchangeMetadata.WebsiteURL, &exchangeMetadata.TwitterURL, &exchangeMetadata.FacebookURL, &exchangeMetadata.YoutubeURL, &exchangeMetadata.LinkedinURL, &exchangeMetadata.RedditURL, &exchangeMetadata.ChatURL, &exchangeMetadata.SlackURL, &exchangeMetadata.TelegramURL, &exchangeMetadata.BlogURL, &exchangeMetadata.Centralized, &exchangeMetadata.Decentralized, &exchangeMetadata.HasTradingIncentive, &exchangeMetadata.TrustScore, &exchangeMetadata.TrustScoreRank, &exchangeMetadata.TradeVolume24HBTC, &exchangeMetadata.TradeVolume24HBTCNormalized, &exchangeMetadata.LastUpdated)
		if err != nil {
			span.SetStatus(otelCodes.Error, err.Error())
			ConsumeTime("Exchange Metadata Data Query Scan", startTime, err)
			return nil, err
		}
		exchangesMetadata = append(exchangesMetadata, exchangeMetadata)

	}
	ConsumeTime("Exchange Metadata Data Query", startTime, nil)
	span.SetStatus(otelCodes.Ok, "Success")
	return exchangesMetadata, nil
}

func InsertExchangeFundamentals(ctxO context.Context, exchange ExchangeFundamentals, labels map[string]string) error {
	ctx, span := tracer.Start(ctxO, "InsertExchangeFundamentals")
	defer span.End()

	startTime := StartTimeL(labels, "Exchange Fundamental Insert")

	pg := PGConnect()

	insertStatementsExchange := "INSERT INTO exchange_fundamentals(name, slug, id, logo, exchange_active_market_pairs, nomics, forbes, last_updated) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"
	// convert Exchanges[] and Nomics into json type to make it easy to store in PG table
	nomics, _ := json.Marshal(exchange.Nomics)
	forbes, _ := json.Marshal(exchange.Forbes)

	_, insertError := pg.ExecContext(ctx, insertStatementsExchange, exchange.Name, exchange.Slug, exchange.Id, exchange.Logo, exchange.ExchangeActiveMarketPairs, nomics, forbes, exchange.LastUpdated)
	if insertError != nil {
		span.SetStatus(otelCodes.Error, insertError.Error())
		ConsumeTimeL(labels, "Exchange Fundamental Insert", startTime, insertError)
		return insertError
	}

	ConsumeTimeL(labels, "Exchange Fundamental Insert", startTime, nil)
	span.SetStatus(otelCodes.Ok, "Success")
	return nil
}

func InsertExchangeFundamentalsLatest(ctxO context.Context, exchange ExchangeFundamentals, labels map[string]string) error {
	ctx, span := tracer.Start(ctxO, "InsertExchangeFundamentalsLatest")
	defer span.End()

	startTime := log.StartTimeL(labels, "Exchange Fundamental Latest Insert")

	pg := PGConnect()

	insertStatementsFundamentals := "CALL upsert_exchange_fundamentalslatest ($1, $2, $3, $4, $5, $6, $7, $8)"

	query := insertStatementsFundamentals
	// convert  Nomics and forbes into json type to make it easy to store in PG table
	nomics, _ := json.Marshal(exchange.Nomics)
	forbes, _ := json.Marshal(exchange.Forbes)

	_, insertError := pg.ExecContext(ctx, query, exchange.Name, exchange.Slug, exchange.Id, exchange.Logo, exchange.ExchangeActiveMarketPairs, nomics, forbes, exchange.LastUpdated)

	if insertError != nil {
		span.SetStatus(otelCodes.Error, insertError.Error())
		log.EndTimeL(labels, "Exchange Fundamental Insert", startTime, insertError)
		return insertError
	}

	log.EndTimeL(labels, "Exchange Fundamental Insert", startTime, nil)
	span.SetStatus(otelCodes.Ok, "Success")

	return nil 
}

func GetExchangeProfilePG(ctxO context.Context, slug string) (*model.CoingeckoExchangeMetadata, error) {

	ctx, span := tracer.Start(ctxO, "GetExchangeProfilePG")
	defer span.End()
	startTime := log.StartTime("Get Exchange Profile Data Query")
  	pg := PGConnect()

    query := `
    SELECT
		cem.id, cem.name, ef.slug, cem.year, cem.description, cem.location, cem.logo_url, cem.logo_url as logo,
		cem.website_url, cem.twitter_url, cem.facebook_url, cem.youtube_url, 
		cem.linkedin_url, cem.reddit_url, cem.chat_url, cem.slack_url, cem.telegram_url, 
		cem.blog_url, cem.centralized, cem.decentralized, cem.has_trading_incentive, 
		cem.trust_score, cem.trust_score_rank, cem.trade_volume_24h_btc, 
		cem.trade_volume_24h_btc_normalized, cem.last_updated
	FROM 
		coingecko_exchange_metadata as cem 
	LEFT JOIN 
		exchange_fundamentalslatest as ef
	ON 
		cem.id = ef.id
	WHERE 
		ef.slug = '` + slug + `';
	`
	var exchangeProfile model.CoingeckoExchangeMetadata
	queryResult, err := pg.QueryContext(ctx, query)
	if err != nil {
		span.SetStatus(otelCodes.Error, err.Error())
		log.EndTime("Exchange Profile Data Scan", startTime, err)
		return nil, err
	}
	defer queryResult.Close()
	counter := 0
	for queryResult.Next() {
		counter++
		err := queryResult.Scan(&exchangeProfile.ID, &exchangeProfile.Name, &exchangeProfile.Slug, &exchangeProfile.Year, &exchangeProfile.Description, &exchangeProfile.Location, &exchangeProfile.LogoURL, &exchangeProfile.Logo, &exchangeProfile.WebsiteURL, &exchangeProfile.TwitterURL, &exchangeProfile.FacebookURL, &exchangeProfile.YoutubeURL, &exchangeProfile.LinkedinURL, &exchangeProfile.RedditURL, &exchangeProfile.ChatURL, &exchangeProfile.SlackURL, &exchangeProfile.TelegramURL, &exchangeProfile.BlogURL, &exchangeProfile.Centralized, &exchangeProfile.Decentralized, &exchangeProfile.HasTradingIncentive, &exchangeProfile.TrustScore, &exchangeProfile.TrustScoreRank, &exchangeProfile.TradeVolume24HBTC, &exchangeProfile.TradeVolume24HBTCNormalized, &exchangeProfile.LastUpdated)
		if err != nil {
			span.SetStatus(otelCodes.Error, err.Error())
			log.EndTime("Exchange Profile Data Scan", startTime, err)
			return nil, err
		}
	}
	log.EndTime("Exchange Profile Data Query", startTime, nil)
	if counter == 0 {
		span.SetStatus(otelCodes.Error, "Not Found")
		return nil, nil
	}
	span.SetStatus(otelCodes.Ok, "Success")

	return &exchangeProfile, nil
}

type RelatedAsset struct {
	Symbol   string `json:"symbol" postgres:"symbol"`
	Name     string `json:"name" postgres:"name"`
	Slug     string `json:"slug" postgres:"slug"`
	Price24h string `json:"price24h" postgres:"price_24h"`
}

func GetRelatedAssetsForExchangePG(labels map[string]string, ctxO context.Context, exchangeName string) ([]byte, error) {
	ctx, span := tracer.Start(ctxO, "GetRelatedAssetsForExchangePG")
	defer span.End()
	startTime := log.StartTimeL(labels, "Get Related Assets For Exchange Data Query")
	pg := PGConnect()

	query := `
	select 
		name, UPPER(display_symbol) as symbol, slug, price_24h
	from 
		fundamentalslatest
	where 
		'` + exchangeName + `' = ANY(listed_exchange)
	`

	rows, err := pg.QueryContext(ctx, query)
	if err != nil {
		span.SetStatus(otelCodes.Error, err.Error())
		log.EndTimeL(labels, "Get Related Assets For Exchange Data Query", startTime, err)
		return nil, err
	}
	defer rows.Close()

	relatedAssets := make([]RelatedAsset, 0)
	counter := 0
	for rows.Next() {
		counter++
		var asset RelatedAsset
		err = rows.Scan(
			&asset.Name,
			&asset.Symbol,
			&asset.Slug,
			&asset.Price24h,
		)
		if err != nil {
			span.SetStatus(otelCodes.Error, err.Error())
			log.EndTimeL(labels, "Exchange Profile Data Scan", startTime, err)
			return nil, err
		}
		relatedAssets = append(relatedAssets, asset)
	}
	results, err := json.Marshal(relatedAssets)
	log.EndTimeL(labels, "Get Related Assets For Exchange Data Query", startTime, err)
	if err != nil {
		span.SetStatus(otelCodes.Error, err.Error())
		return nil, err
	}
	span.SetStatus(otelCodes.Ok, "Success")
	return results, nil
}


--------------------------------------------------------------------------------------------------------------------------------
// exchnages
setResponseHeaders(w, 0)
	vars := mux.Vars(r)
	period := vars["period"]
	labels := make(map[string]string)

	span := trace.SpanFromContext(r.Context())
	defer span.End()

	labels["function"] = "BuildExchangeFundamentalsHandler"
	labels["UUID"] = uuid.New().String()
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()
	span.SetAttributes(attribute.String("period", period))
	span.SetAttributes(attribute.String("UUID", labels["UUID"]))
	startTime := log.StartTimeL(labels, "Build Exchange Fundamentals Data ")

	g, ctx := errgroup.WithContext(r.Context())

	bqs, err := store.NewBQStore()
	if err != nil {
		// If an error occurs, the full context is canceled. Which causes all the first two Go Routines to cancel.
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		span.SetStatus(codes.Error, err.Error())
		return
	}

	// Go Routine 1
	// Get The Exchange Metadata elements needed for the Exchanges Fundamentals
	// this will get all exchanges metadata
	var exchangesMetaData []model.CoingeckoExchangeMetadata
	g.Go(func() error {
		results, err := store.GetExchangeMetaDataWithoutLimit(ctx, labels)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return errors.New("error getting Exchanges Metadata CG from PG: " + err.Error())
		}
		log.DebugL(labels, "Received Exchanges Metadata CG %d results from PG", len(results))

		exchangesMetaData = results
		fmt.Println(len(exchangesMetaData))
		return nil

	})

	// Go Routine 2
	// Get The Exchanges Tickers needed for the Exchanges Fundamentals
	exchangeResults := make(map[string]store.ExchangeResults)
	g.Go(func() error {
		results, err := bqs.ExchangeFundamentalsCG(ctx, labels["UUID"])
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return errors.New("error getting Exchanges Tickers Fundamentals CG from BQ: " + err.Error())
		}
		log.DebugL(labels, "Received Exchanges Tickers Fundamentals CG %d results from BQ", len(results))

		exchangeResults = results
		fmt.Println(len(exchangeResults))

		return nil

	})

	// Results from Go Routine 3
	// List of exchangesProfiles in Map of [Name]ExchangeProfile
	exchangesProfiles := make(map[string]model.ExchangeProfile)

	// Go Routine 3
	// Get all Exchange profiles from FS (rowy tables)
	g.Go(func() error {

		e, err := store.GetExchanges(ctx)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return errors.New("Error getting exchanges from rowy: " + err.Error())
		}

		log.DebugL(labels, "Received %d results from FS Exchanges", len(e))

		exchangesProfiles = e
		fmt.Println(len(exchangesProfiles))

		return nil
	})

	span.AddEvent("Waiting for Go Routines to finish")
	err = g.Wait()  // Blocks till all go routines are done
	if err != nil { // If any of the go routines return an error
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal Server Error"))
		span.SetStatus(codes.Error, err.Error())
		return
	}

	// Starts new WaitGroup for the next set of go routines - which don't need to return an error
	var (
		wg sync.WaitGroup
		// Max 10 concurrent requests
		throttleChan = make(chan bool, 10)
	)

	for _, v := range exchangesMetaData {

		throttleChan <- true
		wg.Add(1)
		go func(ctxO context.Context, v model.CoingeckoExchangeMetadata, uuid string) {
			ctx, span := tracer.Start(ctxO, "Go Routine BuildExchangeFundamentalsHandler")
			defer span.End()
			label := make(map[string]string)
			label["symbol"] = v.Name
			span.SetAttributes(attribute.String("exchange", v.Name))
			label["period"] = period
			span.SetAttributes(attribute.String("period", period))
			label["uuid"] = uuid
			span.SetAttributes(attribute.String("uuid", uuid))
			label["spanID"] = span.SpanContext().SpanID().String()
			label["traceID"] = span.SpanContext().TraceID().String()
			// check if the exchange metadata exist in exchange tickers
			if exchangeDataFromCG, ok := exchangeResults[v.ID]; ok {

				// map the exchange metadata to exchanges tickers to build exchange
				e, err := store.CombineExchanges(ctx, v, exchangeDataFromCG, exchangesProfiles)

				if err != nil {
					log.ErrorL(label, "Error combining Exchange Fundamentals for %s: %s", v.ID, err.Error())
					goto waitReturn // If there is an error, skip to the end of the go routine
				}

				// Save the Exchanges Fundamentals to PG
				err = store.InsertExchangeFundamentals(ctx, e, label)
				if err != nil {
					log.ErrorL(label, "Error saving Exchange Fundamentals %s", err)
				}
				// Save the latest Exchanges Fundamentals to PG
				store.InsertExchangeFundamentalsLatest(ctx, e, label)
			}

		waitReturn:
			<-throttleChan // Remove from the throttle channel - allowing another go routine to start
			wg.Done()

		}(r.Context(), v, labels["UUID"])

	}

	wg.Wait()
	log.EndTimeL(labels, "Exchange Fundamentals CG Build ", startTime, nil)
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "ok")
	span.SetStatus(codes.Ok, "Exchange Fundamentals CG Built")




	--------------------------------------------------------------------------------------------------------------------------------
	// Results from Go Routine 8
	// List of exchangesProfiles in Map of [Name]ExchangeProfile
	exchangesProfiles := make(map[string]model.ExchangeProfile)

	// Go Routine 8
	// Get all Exchange profiles from FS (rowy tables)
	g.Go(func() error {

		e, err := store.GetExchanges(ctx)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return errors.New("Error getting exchanges from rowy: " + err.Error())
		}

		log.DebugL(labels, "Received %d results from FS Exchanges", len(e))

		exchangesProfiles = e
		fmt.Println(len(exchangesProfiles))

		return nil
	})
	f.Exchanges, f.ForbesTransparencyVolume = store.CombineExchangeDataCG(f.Exchanges, exchangesMetaData, exchangesProfiles)
	-----------------------------------------------------------------------------------------------------------------------------------------------
	// CombineExchangeMetaData merges the exchange Metadata data with the exchange ticker data. returns []ExchangeBasedFundamentals
func CombineExchangeDataCG(exchangeData []ExchangeBasedFundamentals, profiles map[string]model.CoingeckoExchangeMetadata, exchangeProfiles map[string]model.ExchangeProfile) ([]ExchangeBasedFundamentals, float64) {

	var exchangesResult []ExchangeBasedFundamentals
	var forbesTransparencyVolume float64
	for _, exchange := range exchangeData {
		if profile, ok := profiles[exchange.Exchange]; ok {
			exchange.Slug = strings.ToLower(fmt.Sprintf("%s", strings.ReplaceAll(profile.Name, " ", "-")))
			exchange.Logo = profile.LogoURL
			exchange.Nomics.VolumeByExchange1D = exchange.VolumeByExchange1D
			if exchangeProfile, ok := exchangeProfiles[exchange.Exchange]; ok {
				volumeDiscount := exchangeProfile.VolumeDiscountPercent
				volumeDiscountPercent := 1 - (volumeDiscount / 100)
				exchange.Forbes.VolumeByExchange1D = exchange.VolumeByExchange1D * volumeDiscountPercent
				if volumeDiscountPercent != 0 {
					forbesTransparencyVolume += exchange.Forbes.VolumeByExchange1D
				}
			}
			
			exchangesResult = append(exchangesResult, exchange)
		}

	}
	if len(exchangesResult) > 0 {
		return exchangesResult, forbesTransparencyVolume
	}
	return exchangeData, forbesTransparencyVolume

}

--------------------------------------------------------------------------------------------------------------------------------
func CombineExchanges(ctx0 context.Context, exchangeMetadata model.CoingeckoExchangeMetadata, exchangeData ExchangeResults, exchangeProfiles map[string]model.ExchangeProfile) (ExchangeFundamentals, error) {
	_, span := tracer.Start(ctx0, "CombineExchanges")
	defer span.End()

	span.AddEvent("Start Combine Exchanges Fundamentals")
	var exchange ExchangeFundamentals
	exchange.Name = exchangeData.Name
	exchange.Slug = strings.ToLower(fmt.Sprintf("%s", strings.ReplaceAll(exchangeData.Name, " ", "-")))
	exchange.Logo = exchangeMetadata.LogoURL
	exchange.Id = exchangeData.Id
	exchange.ExchangeActiveMarketPairs = exchangeData.ExchangeActiveMarketPairs
	exchange.Nomics.VolumeByExchange1D = exchangeData.VolumeByExchange1D
	if exchangeProfile, ok := exchangeProfiles[exchange.Name]; ok {
		volumeDiscount := exchangeProfile.VolumeDiscountPercent
		volumeDiscountPercent := 1 - (volumeDiscount / 100)
		exchange.Forbes.VolumeByExchange1D = exchange.Nomics.VolumeByExchange1D * volumeDiscountPercent
	}
	exchange.LastUpdated = time.Now()
	span.SetStatus(otelCodes.Ok, "Success")
	return exchange, nil
}
------------------------------------------------------------------------------------------------
//firestore 1047
exchanges[exchange.Name] = exchange