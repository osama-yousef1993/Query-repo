with 
		oneDay AS 
			(
				SELECT 
					CAST(SUM(volume) as FLOAT) volume_1d,
					lower(base) as symbol
				FROM 
					( 
						SELECT 
							volume, 
							base, 
							timestamp 
						FROM 
							nomics_ohlcv_candles
						WHERE 
							timestamp >= cast(now() - INTERVAL '24 HOUR' as timestamp)
							and base = 'BTC'
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
						CAST(SUM(volume) as FLOAT) volume_7d,
						lower(base) as symbol
				FROM 
					( 
						SELECT 
							volume,
							base, 
							timestamp 
						FROM 
							nomics_ohlcv_candles
						WHERE 
							timestamp >=  cast(now() - INTERVAL '7 DAYS' as timestamp)
							and base = 'BTC'
						GROUP BY 
							base, 
							timestamp, 
							volume
					) as oneDay
				GROUP BY 
					base
			),
		thirtyDays AS 
			(
				SELECT 
					CAST(SUM(volume) as FLOAT) volume_30d,
					lower(base) as symbol
				FROM 
					( 
						SELECT 
							volume, 
							base, 
							timestamp 
						FROM 
							nomics_ohlcv_candles
						WHERE timestamp >=  cast(now() - INTERVAL '30 DAYS' as timestamp)
							and base = 'BTC'
						GROUP BY 
							base, 
							timestamp, 
							volume
					) as oneDay
				GROUP BY 
					base
			),
		oneYear AS 
			(
				SELECT 
					CAST(SUM(volume) as FLOAT) volume_1y,
					lower(base) as symbol
				FROM 
					( 
						SELECT 
							volume, 
							base, 
							timestamp 
						FROM 
							nomics_ohlcv_candles
						WHERE 
							timestamp >=  cast(now() - INTERVAL '365 DAYS' as timestamp)
							and base = 'BTC'
						GROUP BY 
							base, 
							timestamp, 
							volume
					) as oneDay
				GROUP BY 
					base
			),
		YTD AS 
			(
				SELECT 
					CAST(SUM(volume) as FLOAT) volume_ytd,
					lower(base) as symbol
				FROM 
					( 
						SELECT 
							base, 
							timestamp, 
							volume
						FROM 
							nomics_ohlcv_candles
						WHERE 
							timestamp  >= cast(date_trunc('year', current_date) as timestamp)
							and base = 'BTC'
						GROUP BY 
							base, 
							timestamp,
							volume
					) as YTD
				GROUP BY 
					base
			),
		ExchangesPrices AS 
			( 
				SELECT 
					lower(base) as Symbol
				FROM 
					nomics_exchange_market_ticker
				WHERE 
					exchange = 'binance_us'
					and base = 'BTC'
					AND timestamp >=  cast(now() - INTERVAL '30 MINUTE' as timestamp)
				group by 
					base
			)
		select 
			CAST(SUM(oneDay.volume_1d) AS FLOAT) AS volume_1d,
			CAST(SUM(sevenDays.volume_7d) AS FLOAT) AS volume_7d,
			CAST(SUM(thirtyDays.volume_30d) AS FLOAT) AS volume_30d,
			CAST(SUM(oneYear.volume_1y) AS FLOAT) AS volume_1y,
			CAST(SUM(YTD.volume_ytd) AS FLOAT) AS volume_ytd
		from oneDay 
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
				YTD 
			ON
				 YTD.symbol = oneDay.symbol
		group by 
			oneDay.symbol