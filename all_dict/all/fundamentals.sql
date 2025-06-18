with 
		allTime as 
			(
				SELECT 
					CAST(MIN(Close) AS FLOAT) all_time_low, 
					lower(base) as symbol
				FROM ( 
						SELECT 
							AVG(close) as Close, 
							base 
						FROM 
							nomics_ohlcv_candles
						GROUP BY 
							base, 
							timestamp
					) as allTime
				GROUP BY 
					base
			),
		oneDay AS 
			(
				SELECT 
					CAST(MAX(Close) AS FLOAT) high_1d, 
					CAST(MIN(Close) AS FLOAT) low_1d, 
					lower(base) as symbol, 
					CASE WHEN MIN(Volume) = 0 THEN CAST((( MAX(volume) - MIN(volume) )) AS FLOAT)
					ELSE CAST((( MAX(volume) - MIN(volume) /  MIN(volume))) AS FLOAT)
					END 
					as percentage_1d
				FROM
					( 
						SELECT 
							AVG(close) as Close, 
							base, 
							timestamp, 
							volume 
						FROM 
							nomics_ohlcv_candles
						WHERE 
							timestamp >= cast(now() - INTERVAL '24 HOUR' as timestamp)
						GROUP BY 
							base, 
							timestamp, 
							volume
					) as oneDay
				GROUP BY 
				base
			),
		sevenDays AS 
			(
				SELECT 
					CAST(MAX(Close) AS FLOAT) high_7d, 
					CAST(MIN(Close) AS FLOAT) low_7d, 
					lower(base) as symbol, 
					CASE WHEN MIN(Volume) = 0 THEN CAST((( MAX(volume) - MIN(volume) )) AS FLOAT)
					ELSE CAST((( MAX(volume) - MIN(volume) /  MIN(volume))) AS FLOAT)
					END
					as percentage_7d
				FROM 
					( 
						SELECT 
							AVG(close) as Close, 
							base, 
							timestamp,
							volume 
						FROM 
							nomics_ohlcv_candles
						WHERE 
							timestamp >=  cast(now() - INTERVAL '7 DAYS' as timestamp)
						GROUP BY 
							base, 
							timestamp, 
							volume
					) as sevenDays
				GROUP BY 
					base
			),
		thirtyDays AS 
			(
				SELECT 
					CAST(MAX(Close) AS FLOAT) high_30d, 
					CAST(MIN(Close) AS FLOAT) low_30d, 
					lower(base) as symbol, 
					CASE WHEN MIN(Volume) = 0 THEN CAST((( MAX(volume) - MIN(volume) )) AS FLOAT)
					ELSE CAST((( MAX(volume) - MIN(volume) /  MIN(volume))) AS FLOAT)
					END
					as percentage_30d
				FROM 
					(
						SELECT 
							AVG(close) as Close, 
							base, 
							timestamp, 
							volume
						FROM 
							nomics_ohlcv_candles
						WHERE 
							timestamp >=  cast(now() - INTERVAL '30 DAYS' as timestamp)
						GROUP BY 
							base, 
							timestamp, 
							volume
					) as thirtyDays
				GROUP BY 
				base
			),
		oneYear AS 
			(
				SELECT 
					CAST(MAX(Close) AS FLOAT) high_1y, 
					CAST(MIN(Close) AS FLOAT) low_1y, 
					lower(base) as symbol, 
					CASE WHEN MIN(Volume) = 0 THEN CAST((( MAX(volume) - MIN(volume) )) AS FLOAT)
					ELSE CAST((( MAX(volume) - MIN(volume) /  MIN(volume))) AS FLOAT)
					END
					as percentage_1y
				FROM 
					( 
						SELECT 
							AVG(close) as Close, 
							base, 
							timestamp, 
							volume
						FROM 
							nomics_ohlcv_candles
						WHERE 
							timestamp >=  cast(now() - INTERVAL '365 DAYS' as timestamp)
						GROUP BY 
							base, 
							timestamp, 
							volume
					) as oneYear
				GROUP BY 
					base
			),

		YTD AS 
			(
				SELECT 
					CAST(MAX(Close) AS FLOAT) high_ytd, 
					CAST(MIN(Close) AS FLOAT) low_ytd, 
					lower(base) as symbol, 
					CASE WHEN MIN(Volume) = 0 THEN CAST((( MAX(volume) - MIN(volume) )) AS FLOAT)
					ELSE CAST((( MAX(volume) - MIN(volume) /  MIN(volume))) AS FLOAT)
					END 
					as percentage_ytd
				FROM 
					( 
						SELECT 
							AVG(close) as Close, 
							base, 
							timestamp, 
							volume
						FROM 
							nomics_ohlcv_candles
						WHERE 
							timestamp  >= cast(date_trunc('year', current_date) as timestamp) 
						GROUP BY 
							base, 
							timestamp, 
							volume
					) as oneYear
				GROUP BY 
					base
			),
		ExchangesPrices AS 
			( 
				SELECT 
					lower(base) as Symbol, 
					exchange as Market, 
					avg(price) as Close 
				FROM 
					nomics_exchange_market_ticker
				WHERE 
					exchange NOT IN ('bitmex','hbtc') 
					AND timestamp >=  cast(now() - INTERVAL '30 MINUTE' as timestamp)
					AND type = 'spot'
					AND status = 'active'
					AND quote IN ('USD', 'USDT', 'USDC')
				group by 
					base,
					exchange
			)

	select 
		array_to_json(ARRAY_AGG(json_build_object('Market', ExchangesPrices.Market, 'Symbol', ExchangesPrices.Symbol ,'Close', CAST(ExchangesPrices.Close AS FLOAT)))) as Exchanges,
		CAST(MAX(oneDay.high_1d) AS FLOAT) AS high_24h,
		CAST(MIN(oneDay.low_1d) AS FLOAT) AS low_24h,
		CAST(MAX(sevenDays.high_7d) AS FLOAT) AS high_7d,
		CAST(MIN(sevenDays.low_7d) AS FLOAT) AS low_7d,
		CAST(MAX(thirtyDays.high_30d) AS FLOAT) AS high_30d,
		CAST(MIN(thirtyDays.low_30d) AS FLOAT) AS low_30d,
		CAST(MAX(oneYear.high_1y) AS FLOAT) AS high_1y,
 	   	CAST(MIN(oneYear.low_1y) AS FLOAT) AS low_1y,
		CAST(MAX(YTD.high_ytd) AS FLOAT) AS high_ytd,
		CAST(MIN(YTD.low_ytd) AS FLOAT) AS low_ytd,
		CAST(MIN(allTime.all_time_low) AS FLOAT) AS all_time_low,
		oneDay.symbol, 
		oneDay.percentage_1d, 
		sevenDays.percentage_7d, 
		thirtyDays.percentage_30d, 
		oneYear.percentage_1y, 
		YTD.percentage_ytd
	from 
		oneDay 
		INNER JOIN 
			ExchangesPrices 
		ON 
			ExchangesPrices.Symbol = oneDay.symbol
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
		INNER JOIN 
			YTD 
		ON 
			YTD.symbol = oneDay.symbol
	group by 
		oneDay.symbol, 
		oneDay.percentage_1d, 
		sevenDays.percentage_7d, 
		thirtyDays.percentage_30d, 
		oneYear.percentage_1y, 
		YTD.percentage_ytd