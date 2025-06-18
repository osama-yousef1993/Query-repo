with 
		oneDay AS 
			(
				SELECT 
					CAST(Close AS FLOAT) price_by_exchange_1d, 
					lower(base) as symbol
				FROM 
					(
						SELECT 
							AVG(price) as Close, 
							base 
						FROM 
							nomics_exchange_market_ticker
						WHERE 
							timestamp >= cast(now() - INTERVAL '24 HOUR' as timestamp)
							AND base =  'BTC'
							AND exchange = 'binance_us'
							AND quote IN ('USD', 'USDT', 'USDC')
						GROUP BY 
							base
					) as oneDay
				GROUP BY 
					base, Close
			),
		sevenDays AS 
			(
				SELECT 
					CAST(Close AS FLOAT) price_by_exchange_7d, 
					lower(base) as symbol
				FROM 
					( 
						SELECT 
							AVG(price) as Close, 
							base 
						FROM 
							nomics_exchange_market_ticker
						WHERE 
							timestamp >=  cast(now() - INTERVAL '7 DAYS' as timestamp)
							AND base =  'BTC'
							AND exchange = 'binance_us'
							AND quote IN ('USD', 'USDT', 'USDC')
						GROUP BY 
							base
					) as sevenDays
				GROUP BY 
					base, Close
			),
		thirtyDays AS 
			(
				SELECT 
					CAST(Close AS FLOAT) price_by_exchange_30d, 
					lower(base) as symbol
				FROM 
					( 
						SELECT 
							AVG(price) as Close, 
							base 
						FROM 
							nomics_exchange_market_ticker
						WHERE 
							timestamp >=  cast(now() - INTERVAL '30 DAYS' as timestamp)
							AND base =  'BTC'
							AND exchange = 'binance_us'
							AND quote IN ('USD', 'USDT', 'USDC')
						GROUP BY 
							base
					) as thirtyDays
				GROUP BY 
					base, Close
			),
		oneYear AS 
			(
				SELECT 
					CAST(Close AS FLOAT) price_by_exchange_1y, 
					lower(base) as symbol
				FROM 
					( 
						SELECT 
							AVG(price) as Close, 
							base 
						FROM 
							nomics_exchange_market_ticker
						WHERE 
							timestamp >=  cast(now() - INTERVAL '365 DAYS' as timestamp)
							AND base =  'BTC'
							AND exchange = 'binance_us'
							AND quote IN ('USD', 'USDT', 'USDC')
						GROUP BY 
							base
					) as oneYear
				GROUP BY 
					base, Close
			),
		YTD AS 
			(
				SELECT 
					CAST(Close AS FLOAT) price_by_exchange_ytd, 
					lower(base) as symbol
				FROM 
					( 
						SELECT 
							AVG(price) as Close, 
							base 
						FROM 
							nomics_exchange_market_ticker
						WHERE 
							timestamp  >= cast(date_trunc('year', current_date) as timestamp) 
							AND base =  'BTC'
							AND exchange = 'binance_us'
							AND quote IN ('USD', 'USDT', 'USDC')
						GROUP BY 
							base
					) as YTD
				GROUP BY 
					base, Close
			)
		select 
			CAST((oneDay.price_by_exchange_1d) AS FLOAT) AS price_by_exchange_1d,
			CAST((sevenDays.price_by_exchange_7d) AS FLOAT) AS price_by_exchange_7d,
			CAST((thirtyDays.price_by_exchange_30d) AS FLOAT) AS price_by_exchange_30d,
			CAST((oneYear.price_by_exchange_1y) AS FLOAT) AS price_by_exchange_1y,
			CAST((YTD.price_by_exchange_ytd) AS FLOAT) AS price_by_exchange_ytd
		from oneDay 
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