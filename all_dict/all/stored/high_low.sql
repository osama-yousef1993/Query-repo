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
							base
					) as allTime
				GROUP BY 
					base
			),
		oneDay AS 
			(
				SELECT 
					CAST(MAX(Close) AS FLOAT) high_1d, 
					CAST(MIN(Close) AS FLOAT) low_1d, 
					lower(base) as symbol
				FROM
					( 
						SELECT 
							AVG(close) as Close, 
							base
						FROM 
						nomics_ohlcv_candles
						WHERE 
							timestamp >= cast(now() - INTERVAL '4 DAYS' as timestamp)
						GROUP BY 
							base
					) as oneDay
				GROUP BY 
				base
			),
			sevenDays AS 
			(
				SELECT 
					CAST(MAX(Close) AS FLOAT) high_7d, 
					CAST(MIN(Close) AS FLOAT) low_7d, 
					lower(base) as symbol
				FROM
					( 
						SELECT 
							AVG(close) as Close, 
							base
						FROM 
						nomics_ohlcv_candles
						WHERE 
							timestamp >= cast(now() - INTERVAL '7 Days' as timestamp)
						GROUP BY 
							base
					) as sevenDays
				GROUP BY 
				base
			),
			thirtyDays AS 
			(
				SELECT 
					CAST(MAX(Close) AS FLOAT) high_30d, 
					CAST(MIN(Close) AS FLOAT) low_30d, 
					lower(base) as symbol
				FROM 
					(
						SELECT 
							AVG(close) as Close, 
							base
						FROM 
							nomics_ohlcv_candles
						WHERE 
							timestamp >=  cast(now() - INTERVAL '30 DAYS' as timestamp)
						GROUP BY 
							base
					) as thirtyDays
				GROUP BY 
				base
			),
		oneYear AS 
			(
				SELECT 
					CAST(MAX(Close) AS FLOAT) high_1y, 
					CAST(MIN(Close) AS FLOAT) low_1y, 
					lower(base) as symbol
				FROM 
					( 
						SELECT 
							AVG(close) as Close, 
							base
						FROM 
							nomics_ohlcv_candles
						WHERE 
							timestamp >=  cast(now() - INTERVAL '365 DAYS' as timestamp)
						GROUP BY 
							base
					) as oneYear
				GROUP BY 
					base
			),

		YTD AS 
			(
				SELECT 
					CAST(MAX(Close) AS FLOAT) high_ytd, 
					CAST(MIN(Close) AS FLOAT) low_ytd, 
					lower(base) as symbol
				FROM 
					( 
						SELECT 
							AVG(close) as Close, 
							base
						FROM 
							nomics_ohlcv_candles
						WHERE 
							timestamp  >= cast(date_trunc('year', current_date) as timestamp) 
						GROUP BY 
							base
					) as oneYear
				GROUP BY 
					base
			),
			exchanges as (select * from public.Update1D7ExchangeData()),
 			market_pairs as(select * from public.Update1D7MarketPairsData())
	select 
		CAST(oneDay.low_1d AS FLOAT) AS low_24h,
		CAST(oneDay.high_1d AS FLOAT) AS high_24h,
		CAST(sevenDays.low_7d AS FLOAT) AS low_7d,
		CAST(sevenDays.high_7d AS FLOAT) AS high_7d,
		CAST(thirtyDays.high_30d AS FLOAT) AS high_30d,
		CAST(thirtyDays.low_30d AS FLOAT) AS low_30d,
		CAST(oneYear.high_1y AS FLOAT) AS high_1y,
 	   	CAST(oneYear.low_1y AS FLOAT) AS low_1y,
		CAST(YTD.high_ytd AS FLOAT) AS high_ytd,
		CAST(YTD.low_ytd AS FLOAT) AS low_ytd,
		exchanges.exchanges as exchanges,
		market_pairs.marketPairs as marketPairs,
		oneDay.symbol
	from 
		oneDay 
		LEFT JOIN
			exchanges
		ON
			exchanges.symbol = oneday.symbol
		LEFT JOIN
 			market_pairs
 		ON
 			market_pairs.symbol = oneDay.symbol
		LEFT JOIN 
			sevenDays
		ON
		sevenDays.symbol = oneday.symbol
		LEFT JOIN 
			thirtyDays 
		ON 
			thirtyDays.symbol = oneDay.symbol
		LEFT JOIN 
			oneYear 
		ON 
			oneYear.symbol = oneDay.symbol
		LEFT JOIN 
			allTime 
		ON 
			allTime.symbol = oneDay.symbol
		LEFT JOIN 
			YTD 
		ON 
			YTD.symbol = oneDay.symbol