select
		currentPriceForPair1D,
		currentPriceForPair7D,
		currentPriceForPair30D,
		currentPriceForPair1Y,
		currentPriceForPairYTD,
		volume_for_pair_1d,
		volume_for_pair_7d,
		volume_for_pair_30d,
		volume_for_pair_1y,
		volume_for_pair_ytd
from (
	select
		CAST(oneDay.current_price_for_pair_1d AS FLOAT) as currentPriceForPair1D,
		CAST(sevenDays.current_price_for_pair_7d AS FLOAT) as currentPriceForPair7D,
		CAST(thirtyDays.current_price_for_pair_30d AS FLOAT) as currentPriceForPair30D,
		CAST(oneYear.current_price_for_pair_1y AS FLOAT) as currentPriceForPair1Y,
		CAST(YTD.current_price_for_pair_ytd AS FLOAT) as currentPriceForPairYTD,
		CAST(oneDay.volume_for_pair_1d AS FLOAT) as volume_for_pair_1d,
		CAST(sevenDays.volume_for_pair_7d AS FLOAT) as volume_for_pair_7d,
		CAST(thirtyDays.volume_for_pair_30d AS FLOAT) as volume_for_pair_30d,
		CAST(oneYear.volume_for_pair_1y AS FLOAT) as volume_for_pair_1y,
		CAST(YTD.volume_for_pair_ytd AS FLOAT) as volume_for_pair_ytd
	from(
			SELECT 
				Symbol,   
				current_price_for_pair_1d,
				volume_for_pair_1d
			from
				(
				SELECT 
					ticker.Symbol,
					ticker.price as current_price_for_pair_1d,
					one.volume_for_pair_1d
				from (
					select 
						lower(base) as Symbol,
						AVG(price) price
					from 
						nomics_exchange_market_ticker
					where 
						timestamp >= cast(now() - INTERVAL '4 DAYS' as timestamp)
					group by 
						base
				) ticker
				LEFT JOIN
				(
					select 
						lower(base) as Symbol,
						CASE WHEN AVG(volume) is null THEN CAST(0 AS FLOAT)
						ELSE AVG(volume)
						END
						as volume_for_pair_1d
					from 
						nomics_exchange_market_ticker_one_day
					where 
						last_updated >= cast(now() - INTERVAL '4 DAYS' as timestamp)
					group by 
						base
				) one
				ON 
				(
					one.Symbol = ticker.Symbol
				)

			) as oneDay
		) oneDay
	LEFT JOIN
		(
			SELECT 
				Symbol,   
				current_price_for_pair_7d,
				volume_for_pair_7d
			from
			(
				SELECT 
					ticker.Symbol,
					ticker.price as current_price_for_pair_7d,
					seven.volume_for_pair_7d
				from (
					select 
						lower(base) as Symbol,
						AVG(price) price
					from 
						nomics_exchange_market_ticker
					where 
						timestamp >= cast(now() - INTERVAL '7 DAYS' as timestamp)
					group by 
						base
				) ticker
				LEFT JOIN
				(
					select 
						lower(base) as Symbol,
						CASE WHEN AVG(volume) is null THEN CAST(0 AS FLOAT)
						ELSE AVG(volume)
						END
						as volume_for_pair_7d
					from 
						nomics_exchange_market_ticker_seven_days
					where 
						last_updated >= cast(now() - INTERVAL '7 DAYS' as timestamp)
					group by 
						base
				) seven
				ON 
				(
					seven.Symbol = ticker.Symbol
				)

			) as sevenDays
		) sevenDays
	ON 	(
			sevenDays.Symbol = oneDay.Symbol
		)
	LEFT JOIN
		(
			SELECT 
				Symbol,   
				current_price_for_pair_30d,
				volume_for_pair_30d
			from
			(
				SELECT 
					ticker.Symbol,
					ticker.price as current_price_for_pair_30d,
					thirty.volume_for_pair_30d
				from (
					select 
						lower(base) as Symbol,
						AVG(price) price
					from 
						nomics_exchange_market_ticker
					where 
						timestamp >= cast(now() - INTERVAL '30 DAYS' as timestamp)
					group by 
						base
				) ticker
				LEFT JOIN
				(
					select 
						lower(base) as Symbol,
						CASE WHEN AVG(volume) is null THEN CAST(0 AS FLOAT)
						ELSE AVG(volume)
						END
						as volume_for_pair_30d
					from 
						nomics_exchange_market_ticker_thirty_days
					where 
						last_updated >= cast(now() - INTERVAL '30 DAYS' as timestamp)
					group by 
						base
				) thirty
				ON 
				(
					thirty.Symbol = ticker.Symbol
				)

			) as thirtyDays
		) thirtyDays
	ON
		(
			thirtyDays.Symbol = oneDay.Symbol
		)
	LEFT JOIN
		(
			SELECT 
				Symbol,   
				current_price_for_pair_1y,
				volume_for_pair_1y
			from
			(
				SELECT 
					ticker.Symbol,
					ticker.price as current_price_for_pair_1y,
					one.volume_for_pair_1y
				from (
					select 
						lower(base) as Symbol,
						AVG(price) price
					from 
						nomics_exchange_market_ticker
					where 
						timestamp >= cast(now() - INTERVAL '365 DAYS' as timestamp)
					group by 
						base
				) ticker
				LEFT JOIN
				(
					select 
						lower(base) as Symbol,
						CASE WHEN AVG(volume) is null THEN CAST(0 AS FLOAT)
						ELSE AVG(volume)
						END
						as volume_for_pair_1y
					from 
						nomics_exchange_market_ticker_one_year
					where 
						last_updated >= cast(now() - INTERVAL '365 DAYS' as timestamp)
					group by 
						base
				) one
				ON 
				(
					one.Symbol = ticker.Symbol
				)

			) as oneYear
		) oneYear
	ON
		(
			oneYear.Symbol = oneDay.Symbol
		)
	LEFT JOIN 
		(
			SELECT 
				Symbol,   
				current_price_for_pair_ytd,
				volume_for_pair_ytd
			from
			(
				SELECT 
					ticker.Symbol,
					ticker.price as current_price_for_pair_ytd,
					ytd.volume_for_pair_ytd
				from (
					select 
						lower(base) as Symbol,
						AVG(price) price
					from 
						nomics_exchange_market_ticker
					where 
						timestamp >= cast(date_trunc('year', current_date) as timestamp)
					group by 
						base
				) ticker
				LEFT JOIN
				(
					select 
						lower(base) as Symbol,
						CASE WHEN AVG(volume) is null THEN CAST(0 AS FLOAT)
						ELSE AVG(volume)
						END
						as volume_for_pair_ytd
					from 
						nomics_exchange_market_ticker_ytd
					where 
						last_updated >= cast(date_trunc('year', current_date) as timestamp)
					group by 
						base
				) ytd
				ON 
				(
					ytd.Symbol = ticker.Symbol
				)

			) as ytd
		) YTD
	ON
		(
			YTD.Symbol = oneDay.Symbol
		)
) as foo


++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
SELECT 
	Symbol,   
	current_price_for_pair_1d,
	volume_for_pair_1d
from
	(
		SELECT 
			ticker.Symbol,
			ticker.price as current_price_for_pair_1d,
			one.volume_for_pair_1d
		from (
			select 
				lower(base) as Symbol,
				AVG(price) price
			from 
				nomics_exchange_market_ticker
			where 
				timestamp >= cast(now() - INTERVAL '4 DAYS' as timestamp)
			
			group by 
				base
		) ticker
		LEFT JOIN
		(
			select 
				lower(base) as Symbol,
				CASE WHEN AVG(volume) is null THEN CAST(0 AS FLOAT)
				ELSE AVG(volume)
				END
				as volume_for_pair_1d
			from 
				nomics_exchange_market_ticker_one_day
			where 
				last_updated >= cast(now() - INTERVAL '4 DAYS' as timestamp)
			group by 
				base
		) one
		ON 
		(
			one.Symbol = ticker.Symbol
		)
			
	) as oneDay
limit 1
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
select
		currentPriceForPair1D,
		currentPriceForPair7D,
		currentPriceForPair30D,
		currentPriceForPair1Y,
		currentPriceForPairYTD,
		volume_for_pair_1d,
		volume_for_pair_7d,
		volume_for_pair_30d,
		volume_for_pair_1y,
		volume_for_pair_ytd
from (
	select
		CAST(oneDay.current_price_for_pair_1d AS FLOAT) as currentPriceForPair1D,
		CAST(sevenDays.current_price_for_pair_7d AS FLOAT) as currentPriceForPair7D,
		CAST(thirtyDays.current_price_for_pair_30d AS FLOAT) as currentPriceForPair30D,
		CAST(oneYear.current_price_for_pair_1y AS FLOAT) as currentPriceForPair1Y,
		CAST(YTD.current_price_for_pair_ytd AS FLOAT) as currentPriceForPairYTD,
		CAST(oneDay.volume_for_pair_1d AS FLOAT) as volume_for_pair_1d,
		CAST(sevenDays.volume_for_pair_7d AS FLOAT) as volume_for_pair_7d,
		CAST(thirtyDays.volume_for_pair_30d AS FLOAT) as volume_for_pair_30d,
		CAST(oneYear.volume_for_pair_1y AS FLOAT) as volume_for_pair_1y,
		CAST(YTD.volume_for_pair_ytd AS FLOAT) as volume_for_pair_ytd
	from(
			SELECT 
				Symbol,   
				CAST(MIN(Price) AS FLOAT) current_price_for_pair_1d,
				CAST(Min(volume_for_pair_1d) As FLOAT) volume_for_pair_1d
			from
				(
					SELECT 
						lower(ticker.base) as Symbol,
						AVG(ticker.price) price,
						CASE WHEN AVG(one.volume) is null THEN CAST(0 AS FLOAT)
						ELSE AVG(one.volume)
						END
						as volume_for_pair_1d
					from 
						nomics_exchange_market_ticker ticker,
						nomics_exchange_market_ticker_one_day one
					where 
						ticker.timestamp >= cast(now() - INTERVAL '4 DAYS' as timestamp)
						AND one.last_updated >= cast(now() - INTERVAL '4 DAYS' as timestamp)
						AND ticker.base = one.base
						AND ticker.exchange = one.exchange
					group by 
						ticker.base
				) as oneDay
			group by Symbol
		) oneDay
	LEFT JOIN
		(
			SELECT 
				Symbol,   
				CAST(MIN(Price) AS FLOAT) current_price_for_pair_7d,
				CAST(Min(volume_for_pair_7d) As FLOAT) volume_for_pair_7d
			from
				(
					SELECT 
						lower(ticker.base) as Symbol,
						AVG(ticker.price) price,
						CASE WHEN AVG(seven.volume) is null THEN CAST(0 AS FLOAT)
						ELSE AVG(seven.volume)
						END
						as volume_for_pair_7d 
					from 
						nomics_exchange_market_ticker ticker, 
						nomics_exchange_market_ticker_seven_days seven
					where 
						ticker.timestamp >= cast(now() - INTERVAL '7 DAYS' as timestamp)
						AND seven.last_updated >= cast(now() - INTERVAL '7 DAYS' as timestamp)
						AND ticker.base = seven.base
						AND ticker.exchange = seven.exchange
					group by 
						ticker.base
				) as sevenDays
			group by Symbol
		) sevenDays
	ON 	(
			sevenDays.Symbol = oneDay.Symbol
		)
	LEFT JOIN
		(
			SELECT 
 				Symbol,   
 				CAST(MIN(Price) AS FLOAT) current_price_for_pair_30d,
 				CAST(Min(volume_for_pair_30d) As FLOAT) volume_for_pair_30d
 			from
 				(
 					SELECT 
 						lower(ticker.base) as Symbol,
 						AVG(ticker.price) price,
 						CASE WHEN AVG(thirty.volume) is null THEN CAST(0 AS FLOAT)
 						ELSE AVG(thirty.volume)
 						END 
 						as volume_for_pair_30d
 					from 
 						nomics_exchange_market_ticker ticker,
 						nomics_exchange_market_ticker_thirty_days thirty
 					where 
 						ticker.timestamp >= cast(now() - INTERVAL '30 DAYS' as timestamp)
 						AND thirty.last_updated >= cast(now() - INTERVAL '30 DAYS' as timestamp)
 						AND ticker.base = thirty.base
 						AND ticker.exchange = thirty.exchange
 					group by 
 						ticker.base
 				) as thirtyDays
 			group by Symbol
		) thirtyDays
	ON
		(
			thirtyDays.Symbol = oneDay.Symbol
		)
	LEFT JOIN
		(
			SELECT 
 				Symbol,   
 				CAST(MIN(Price) AS FLOAT) current_price_for_pair_1y,
 				CAST(Min(volume_for_pair_1y) As FLOAT) volume_for_pair_1y
 			from
 				(
 					SELECT 
 						lower(ticker.base) as Symbol,
 						AVG(ticker.price) price,
 						CASE WHEN AVG(one.volume) is null THEN CAST(0 AS FLOAT)
 						ELSE AVG(one.volume)
 						END
 						as volume_for_pair_1y
 					from 
 						nomics_exchange_market_ticker ticker,
 						nomics_exchange_market_ticker_one_year one
 					where 
 						ticker.timestamp >= cast(now() - INTERVAL '365 DAYS' as timestamp)
 						AND one.last_updated >= cast(now() - INTERVAL '365 DAYS' as timestamp)
 						AND ticker.base = one.base
 						AND ticker.exchange = one.exchange
 					group by 
 						ticker.base
 				) as oneYear
 			group by Symbol
		) oneYear
	ON
		(
			oneYear.Symbol = oneDay.Symbol
		)
	LEFT JOIN 
		(
			SELECT 
 				Symbol,   
 				CAST(MIN(Price) AS FLOAT) current_price_for_pair_ytd,
 				CAST(Min(volume_for_pair_ytd) As FLOAT) volume_for_pair_ytd
 			from
 				(
 					SELECT 
 						lower(ticker.base) as Symbol,
 						AVG(ticker.price) price,
 						CASE WHEN AVG(ytd.volume) is null THEN CAST(0 AS FLOAT)
 						ELSE AVG(ytd.volume)
 						END
 						as volume_for_pair_ytd
 					from 
 						nomics_exchange_market_ticker ticker,
 						nomics_exchange_market_ticker_ytd ytd
 					where 
 						ticker.timestamp >= cast(date_trunc('year', current_date) as timestamp)
 						AND ytd.last_updated >= cast(date_trunc('year', current_date) as timestamp)
 						AND ticker.base = ytd.base
 						AND ticker.exchange = ytd.exchange
 					group by 
 						ticker.base
 				) as YTD
 			group by Symbol
		) YTD
	ON
		(
			YTD.Symbol = oneDay.Symbol
		)
) as foo
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
select 
	symbol, exchange, quote, pair, status, last_updated, type
from (
	select markets.Symbol, markets.exchange, markets.quote, markets.pair, assets.status, assets.last_updated, ticker.type
	from(
			select
				lower(base) as Symbol, 
				exchange, 
				quote , 
				CONCAT(base, quote) as pair
			from 
				nomics_markets
			group by
				base,
				exchange, 
				quote 
		) markets
	LEFT JOIN
		(
			select
				lower(id) as base,
				status, 
				last_updated
			from 
				nomics_assets
			where status = 'active'
			group by 
				id
		) assets
	ON 	(
			assets.base = markets.Symbol
		)
	LEFT JOIN 
		(
			select
				 lower(base) as base,
				 type
			 from 
				 nomics_exchange_market_ticker
			 group by
				 base,
				 type
		) ticker
	ON	(
		ticker.base = markets.Symbol
		)
) as foo
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
with
	market as 
		(
			select
				lower(base) as Symbol, 
				exchange, 
				quote , 
				CONCAT(base, quote) as pair
			from 
				nomics_markets
			group by
				base,
				exchange, 
				quote
			
		),
	assets as 
		(
			select
				lower(id) as base,
				status, 
				last_updated
			from 
				nomics_assets
			group by 
				id
	
		),
	 ticker as 
		(
			select
				 lower(base) as base,
				 type
			 from 
				 nomics_exchange_market_ticker
			 where 
				 type != ''
			 group by
				 base,
				 type
			limit 63000
		 ),
	oneDay As 
		(
			SELECT 
				Symbol,   
				CAST(MIN(Price) AS FLOAT) current_price_for_pair_1d,
				CAST(Min(volume_for_pair_1d) As FLOAT) volume_for_pair_1d
			from
				(
					SELECT 
						lower(ticker.base) as Symbol,
						AVG(ticker.price) price,
						CASE WHEN AVG(one.volume) is null THEN CAST(0 AS FLOAT)
						ELSE AVG(one.volume)
						END
						as volume_for_pair_1d
					from 
						nomics_exchange_market_ticker ticker,
						nomics_exchange_market_ticker_one_day one
					where 
						ticker.timestamp >= cast(now() - INTERVAL '4 DAYS' as timestamp)
						AND one.last_updated >= cast(now() - INTERVAL '4 DAYS' as timestamp)
						AND ticker.base = one.base
						AND ticker.exchange = one.exchange
					group by 
						ticker.base
				) as oneDay
			group by Symbol
		),
	sevenDays As 
		(
			SELECT 
				Symbol,   
				CAST(MIN(Price) AS FLOAT) current_price_for_pair_7d,
				CAST(Min(volume_for_pair_7d) As FLOAT) volume_for_pair_7d
			from
				(
					SELECT 
						lower(ticker.base) as Symbol,
						AVG(ticker.price) price,
						CASE WHEN AVG(seven.volume) is null THEN CAST(0 AS FLOAT)
						ELSE AVG(seven.volume)
						END
						as volume_for_pair_7d 
					from 
						nomics_exchange_market_ticker ticker, 
						nomics_exchange_market_ticker_seven_days seven
					where 
						ticker.timestamp >= cast(now() - INTERVAL '7 DAYS' as timestamp)
						AND seven.last_updated >= cast(now() - INTERVAL '7 DAYS' as timestamp)
						AND ticker.base = seven.base
						AND ticker.exchange = seven.exchange
					group by 
						ticker.base
				) as sevenDays
			group by Symbol
		)
-- 	thirtyDays As 
-- 		(
-- 			SELECT 
-- 				Symbol,   
-- 				CAST(MIN(Price) AS FLOAT) current_price_for_pair_30d,
-- 				CAST(Min(volume_for_pair_30d) As FLOAT) volume_for_pair_30d
-- 			from
-- 				(
-- 					SELECT 
-- 						lower(ticker.base) as Symbol,
-- 						AVG(ticker.price) price,
-- 						CASE WHEN AVG(thirty.volume) is null THEN CAST(0 AS FLOAT)
-- 						ELSE AVG(thirty.volume)
-- 						END 
-- 						as volume_for_pair_30d
-- 					from 
-- 						nomics_exchange_market_ticker ticker,
-- 						nomics_exchange_market_ticker_thirty_days thirty
-- 					where 
-- 						ticker.timestamp >= cast(now() - INTERVAL '30 DAYS' as timestamp)
-- 						AND thirty.last_updated >= cast(now() - INTERVAL '30 DAYS' as timestamp)
-- 						AND ticker.base = thirty.base
-- 						AND ticker.exchange = thirty.exchange
-- 					group by 
-- 						ticker.base
-- 				) as thirtyDays
-- 			group by Symbol
-- 		),
-- 	oneYear As 
-- 		(
-- 			SELECT 
-- 				Symbol,   
-- 				CAST(MIN(Price) AS FLOAT) current_price_for_pair_1y,
-- 				CAST(Min(volume_for_pair_1y) As FLOAT) volume_for_pair_1y
-- 			from
-- 				(
-- 					SELECT 
-- 						lower(ticker.base) as Symbol,
-- 						AVG(ticker.price) price,
-- 						CASE WHEN AVG(one.volume) is null THEN CAST(0 AS FLOAT)
-- 						ELSE AVG(one.volume)
-- 						END
-- 						as volume_for_pair_1y
-- 					from 
-- 						nomics_exchange_market_ticker ticker,
-- 						nomics_exchange_market_ticker_one_year one
-- 					where 
-- 						ticker.timestamp >= cast(now() - INTERVAL '365 DAYS' as timestamp)
-- 						AND one.last_updated >= cast(now() - INTERVAL '365 DAYS' as timestamp)
-- 						AND ticker.base = one.base
-- 						AND ticker.exchange = one.exchange
-- 					group by 
-- 						ticker.base
-- 				) as oneYear
-- 			group by Symbol
-- 		),
-- 	YTD As 
-- 		(
-- 			SELECT 
-- 				Symbol,   
-- 				CAST(MIN(Price) AS FLOAT) current_price_for_pair_ytd,
-- 				CAST(Min(volume_for_pair_ytd) As FLOAT) volume_for_pair_ytd
-- 			from
-- 				(
-- 					SELECT 
-- 						lower(ticker.base) as Symbol,
-- 						AVG(ticker.price) price,
-- 						CASE WHEN AVG(ytd.volume) is null THEN CAST(0 AS FLOAT)
-- 						ELSE AVG(ytd.volume)
-- 						END
-- 						as volume_for_pair_ytd
-- 					from 
-- 						nomics_exchange_market_ticker ticker,
-- 						nomics_exchange_market_ticker_ytd ytd
-- 					where 
-- 						ticker.timestamp >= cast(date_trunc('year', current_date) as timestamp)
-- 						AND ytd.last_updated >= cast(date_trunc('year', current_date) as timestamp)
-- 						AND ticker.base = ytd.base
-- 						AND ticker.exchange = ytd.exchange
-- 					group by 
-- 						ticker.base
-- 				) as YTD
-- 			group by Symbol
-- 		)

	select 
		Symbol, 
		array_to_json(ARRAY_AGG(json_build_object(
											'base', Symbol, 
											'exchange', exchange, 
											'quote', quote, 
											'pair', pair, 												 
											'pairStatus', pairStatus, 
											'update_timestamp',update_timestamp,
											'TypeOfPair', TypeOfPair,
											'currentPriceForPair1D', CAST(currentPriceForPair1D AS FLOAT),
											'currentPriceForPair7D', CAST(currentPriceForPair7D AS FLOAT),
-- 											'currentPriceForPair30D', CAST(currentPriceForPair30D AS FLOAT),
-- 											'currentPriceForPair1Y', CAST(currentPriceForPair1Y AS FLOAT),
-- 											'currentPriceForPairYTD', CAST(currentPriceForPairYTD AS FLOAT),
											'nomics', json_build_object(
													'volume_for_pair_1d', CAST(volume_for_pair_1d AS FLOAT) ,
													'volume_for_pair_7d', CAST(volume_for_pair_7d AS FLOAT)
-- 													'volume_for_pair_30d', CAST(volume_for_pair_30d AS FLOAT),
-- 													'volume_for_pair_1y', CAST(volume_for_pair_1y AS FLOAT),
-- 													'volume_for_pair_ytd', CAST(volume_for_pair_ytd AS FLOAT)
											),
											'forbes', json_build_object(
															'volume_for_forbes_pair_1d', CAST(volume_for_pair_1d * 0.23 AS FLOAT),
															'volume_for_forbes_pair_7d', CAST(volume_for_pair_7d * 0.23 AS FLOAT)
-- 															'volume_for_forbes_pair_30d', CAST(volume_for_pair_30d * 0.23 AS FLOAT),
-- 															'volume_for_forbes_pair_1y', CAST(volume_for_pair_1y * 0.23 AS FLOAT),
-- 															'volume_for_forbes_pair_ytd', CAST(volume_for_pair_ytd * 0.23 AS FLOAT)
														)
											))) as MarketPairs

				from (
					SELECT
-- 						assets.base as base,
						market.Symbol as Symbol, 
						market.exchange as exchange, 
						market.quote as quote, 
						market.pair as pair, 												 
						assets.status as pairStatus, 
						assets.last_updated as update_timestamp,
						ticker.type as TypeOfPair,
						CAST(oneDay.current_price_for_pair_1d AS FLOAT) as currentPriceForPair1D,
						CAST(sevenDays.current_price_for_pair_7d AS FLOAT) as currentPriceForPair7D,
-- 						CAST(thirtyDays.current_price_for_pair_30d AS FLOAT) as currentPriceForPair30D,
-- 						CAST(oneYear.current_price_for_pair_1y AS FLOAT) as currentPriceForPair1Y,
-- 						CAST(YTD.current_price_for_pair_ytd AS FLOAT) as currentPriceForPairYTD,
						CAST(oneDay.volume_for_pair_1d AS FLOAT) as volume_for_pair_1d,
						CAST(sevenDays.volume_for_pair_7d AS FLOAT) as volume_for_pair_7d,
-- 						CAST(thirtyDays.volume_for_pair_30d AS FLOAT) as volume_for_pair_30d,
-- 						CAST(oneYear.volume_for_pair_1y AS FLOAT) as volume_for_pair_1y,
-- 						CAST(YTD.volume_for_pair_ytd AS FLOAT) as volume_for_pair_ytd,
						CAST(oneDay.volume_for_pair_1d * 0.23 AS FLOAT) as volume_for_forbes_pair_1d,
						CAST(sevenDays.volume_for_pair_7d * 0.23 AS FLOAT) as volume_for_forbes_pair_7d
-- 						CAST(thirtyDays.volume_for_pair_30d * 0.23 AS FLOAT) as volume_for_forbes_pair_30d,
-- 						CAST(oneYear.volume_for_pair_1y * 0.23 AS FLOAT) as volume_for_forbes_pair_1y,
-- 						CAST(YTD.volume_for_pair_ytd * 0.23 AS FLOAT) as volume_for_forbes_pair_ytd
 					from
						assets
						LEFT JOIN 
							market
						ON
							market.Symbol = assets.base
						LEFT JOIN 
							ticker
						ON
							ticker.base = assets.base
						LEFT JOIN 
							oneDay 
						ON
							oneDay.symbol = assets.base
						LEFT JOIN 
							sevenDays 
						ON
							sevenDays.symbol = assets.base
-- 						LEFT JOIN 
-- 							thirtyDays 
-- 						ON
-- 							thirtyDays.symbol = assets.base
-- 						LEFT JOIN 
-- 							oneYear 
-- 						ON
-- 							oneYear.symbol = assets.base
-- 						LEFT JOIN 
-- 							YTD 
-- 						ON
-- 							YTD.symbol = assets.base
						
				)as foo
group by Symbol
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
with 
		intervals_low_high as ( select * from StoredhighlowsData()),
		exchanges as (select * from Update1D7ExchangeData()),
		market_pairs as(select * from Update1D7MarketPairsData())
	select 
		CAST(intervals_low_high.low_24h AS FLOAT) AS low_24h,
		CAST(intervals_low_high.high_24h AS FLOAT) AS high_24h,
		CAST(intervals_low_high.low_7d AS FLOAT) AS low_7d,
		CAST(intervals_low_high.high_7d AS FLOAT) AS high_7d,
		CAST(intervals_low_high.high_30d AS FLOAT) AS high_30d,
		CAST(intervals_low_high.low_30d AS FLOAT) AS low_30d,
		CAST(intervals_low_high.high_1y AS FLOAT) AS high_1y,
 	   	CAST(intervals_low_high.low_1y AS FLOAT) AS low_1y,
		CAST(intervals_low_high.high_ytd AS FLOAT) AS high_ytd,
		CAST(intervals_low_high.low_ytd AS FLOAT) AS low_ytd,
		exchanges.exchanges as exchanges,
		market_pairs.marketPairs as marketPairs,
		intervals_low_high.symbol
	from 
		intervals_low_high 
		LEFT JOIN
			exchanges
		ON
			exchanges.symbol = intervals_low_high.symbol
		LEFT JOIN
			market_pairs
		ON
			market_pairs.symbol = intervals_low_high.symbol
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
create or replace function public.StoredhighlowsData()
	returns Table (symbol text, 
				  high_24h float, low_24h float,
				  high_7d float, low_7d float,
				  high_30d float, low_30d float,
				  high_1y float, low_1y float,
				  high_ytd float, low_ytd float )
as
$func$
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
			) 
	select 
		oneDay.symbol,
		CAST(oneDay.low_1d AS FLOAT) AS low_24h,
		CAST(oneDay.high_1d AS FLOAT) AS high_24h,
		CAST(sevenDays.low_7d AS FLOAT) AS low_7d,
		CAST(sevenDays.high_7d AS FLOAT) AS high_7d,
		CAST(thirtyDays.high_30d AS FLOAT) AS high_30d,
		CAST(thirtyDays.low_30d AS FLOAT) AS low_30d,
		CAST(oneYear.high_1y AS FLOAT) AS high_1y,
 	   	CAST(oneYear.low_1y AS FLOAT) AS low_1y,
		CAST(YTD.high_ytd AS FLOAT) AS high_ytd,
		CAST(YTD.low_ytd AS FLOAT) AS low_ytd	
	from 
		oneDay 
		INNER JOIN 
			sevenDays
		ON
		sevenDays.symbol = oneday.symbol
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
$func$
Language sql
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
DROP FUNCTION IF EXISTS public.Craete1D7MarketPairsData();
create or replace function public.Craete1D7MarketPairsData()
	returns Table (symbol text , marketPairs jsonb)
as
$func$
with
	market as 
		(
			select
				lower(base) as Symbol, 
				exchange, 
				quote , 
				CONCAT(base, quote) as pair
			from 
				nomics_markets
			group by
				base,
				exchange, 
				quote
			
		),
	assets as 
		(
			select
				lower(id) as base,
				status, 
				last_updated
			from 
				nomics_assets
			group by 
				id
	
		),
	 ticker as 
		(
			select
				 lower(base) as base,
				 type
			 from 
				 nomics_exchange_market_ticker
			 where 
				 type != ''
			 group by
				 base,
				 type
		 ),
	oneDay As 
		(
			SELECT 
				Symbol,   
				CAST(MIN(Price) AS FLOAT) current_price_for_pair_1d,
				CAST(Min(volume_for_pair_1d) As FLOAT) volume_for_pair_1d
			from
				(
					SELECT 
						lower(ticker.base) as Symbol,
						AVG(ticker.price) price,
						CASE WHEN AVG(one.volume) is null THEN CAST(0 AS FLOAT)
						ELSE AVG(one.volume)
						END
						as volume_for_pair_1d
					from 
						nomics_exchange_market_ticker ticker,
						nomics_exchange_market_ticker_one_day one
					where 
						ticker.timestamp >= cast(now() - INTERVAL '4 DAYS' as timestamp)
						AND one.last_updated >= cast(now() - INTERVAL '4 DAYS' as timestamp)
						AND ticker.base = one.base
						AND ticker.exchange = one.exchange
					group by 
						ticker.base
				) as oneDay
			group by Symbol
		),
	sevenDays As 
		(
			SELECT 
				Symbol,   
				CAST(MIN(Price) AS FLOAT) current_price_for_pair_7d,
				CAST(Min(volume_for_pair_7d) As FLOAT) volume_for_pair_7d
			from
				(
					SELECT 
						lower(ticker.base) as Symbol,
						AVG(ticker.price) price,
						CASE WHEN AVG(seven.volume) is null THEN CAST(0 AS FLOAT)
						ELSE AVG(seven.volume)
						END
						as volume_for_pair_7d 
					from 
						nomics_exchange_market_ticker ticker, 
						nomics_exchange_market_ticker_seven_days seven
					where 
						ticker.timestamp >= cast(now() - INTERVAL '7 DAYS' as timestamp)
						AND seven.last_updated >= cast(now() - INTERVAL '7 DAYS' as timestamp)
						AND ticker.base = seven.base
						AND ticker.exchange = seven.exchange
					group by 
						ticker.base
				) as sevenDays
			group by Symbol
		)
-- 	thirtyDays As 
-- 		(
-- 			SELECT 
-- 				Symbol,   
-- 				CAST(MIN(Price) AS FLOAT) current_price_for_pair_30d,
-- 				CAST(Min(volume_for_pair_30d) As FLOAT) volume_for_pair_30d
-- 			from
-- 				(
-- 					SELECT 
-- 						lower(ticker.base) as Symbol,
-- 						AVG(ticker.price) price,
-- 						CASE WHEN AVG(thirty.volume) is null THEN CAST(0 AS FLOAT)
-- 						ELSE AVG(thirty.volume)
-- 						END 
-- 						as volume_for_pair_30d
-- 					from 
-- 						nomics_exchange_market_ticker ticker,
-- 						nomics_exchange_market_ticker_thirty_days thirty
-- 					where 
-- 						ticker.timestamp >= cast(now() - INTERVAL '30 DAYS' as timestamp)
-- 						AND thirty.last_updated >= cast(now() - INTERVAL '30 DAYS' as timestamp)
-- 						AND ticker.base = thirty.base
-- 						AND ticker.exchange = thirty.exchange
-- 					group by 
-- 						ticker.base
-- 				) as thirtyDays
-- 			group by Symbol
-- 		),
-- 	oneYear As 
-- 		(
-- 			SELECT 
-- 				Symbol,   
-- 				CAST(MIN(Price) AS FLOAT) current_price_for_pair_1y,
-- 				CAST(Min(volume_for_pair_1y) As FLOAT) volume_for_pair_1y
-- 			from
-- 				(
-- 					SELECT 
-- 						lower(ticker.base) as Symbol,
-- 						AVG(ticker.price) price,
-- 						CASE WHEN AVG(one.volume) is null THEN CAST(0 AS FLOAT)
-- 						ELSE AVG(one.volume)
-- 						END
-- 						as volume_for_pair_1y
-- 					from 
-- 						nomics_exchange_market_ticker ticker,
-- 						nomics_exchange_market_ticker_one_year one
-- 					where 
-- 						ticker.timestamp >= cast(now() - INTERVAL '365 DAYS' as timestamp)
-- 						AND one.last_updated >= cast(now() - INTERVAL '365 DAYS' as timestamp)
-- 						AND ticker.base = one.base
-- 						AND ticker.exchange = one.exchange
-- 					group by 
-- 						ticker.base
-- 				) as oneYear
-- 			group by Symbol
-- 		),
-- 	YTD As 
-- 		(
-- 			SELECT 
-- 				Symbol,   
-- 				CAST(MIN(Price) AS FLOAT) current_price_for_pair_ytd,
-- 				CAST(Min(volume_for_pair_ytd) As FLOAT) volume_for_pair_ytd
-- 			from
-- 				(
-- 					SELECT 
-- 						lower(ticker.base) as Symbol,
-- 						AVG(ticker.price) price,
-- 						CASE WHEN AVG(ytd.volume) is null THEN CAST(0 AS FLOAT)
-- 						ELSE AVG(ytd.volume)
-- 						END
-- 						as volume_for_pair_ytd
-- 					from 
-- 						nomics_exchange_market_ticker ticker,
-- 						nomics_exchange_market_ticker_ytd ytd
-- 					where 
-- 						ticker.timestamp >= cast(date_trunc('year', current_date) as timestamp)
-- 						AND ytd.last_updated >= cast(date_trunc('year', current_date) as timestamp)
-- 						AND ticker.base = ytd.base
-- 						AND ticker.exchange = ytd.exchange
-- 					group by 
-- 						ticker.base
-- 				) as YTD
-- 			group by Symbol
-- 		)

	select 
		Symbol, 
		array_to_json(ARRAY_AGG(json_build_object(
											'base', Symbol, 
											'exchange', exchange, 
											'quote', quote, 
											'pair', pair, 												 
											'pairStatus', pairStatus, 
											'update_timestamp',update_timestamp,
											'TypeOfPair', TypeOfPair,
											'currentPriceForPair1D', CAST(currentPriceForPair1D AS FLOAT),
											'currentPriceForPair7D', CAST(currentPriceForPair7D AS FLOAT),
-- 											'currentPriceForPair30D', CAST(currentPriceForPair30D AS FLOAT),
-- 											'currentPriceForPair1Y', CAST(currentPriceForPair1Y AS FLOAT),
-- 											'currentPriceForPairYTD', CAST(currentPriceForPairYTD AS FLOAT),
											'nomics', json_build_object(
													'volume_for_pair_1d', CAST(volume_for_pair_1d AS FLOAT) ,
													'volume_for_pair_7d', CAST(volume_for_pair_7d AS FLOAT)
-- 													'volume_for_pair_30d', CAST(volume_for_pair_30d AS FLOAT),
-- 													'volume_for_pair_1y', CAST(volume_for_pair_1y AS FLOAT),
-- 													'volume_for_pair_ytd', CAST(volume_for_pair_ytd AS FLOAT)
											),
											'forbes', json_build_object(
															'volume_for_forbes_pair_1d', CAST(volume_for_pair_1d * 0.23 AS FLOAT),
															'volume_for_forbes_pair_7d', CAST(volume_for_pair_7d * 0.23 AS FLOAT)
-- 															'volume_for_forbes_pair_30d', CAST(volume_for_pair_30d * 0.23 AS FLOAT),
-- 															'volume_for_forbes_pair_1y', CAST(volume_for_pair_1y * 0.23 AS FLOAT),
-- 															'volume_for_forbes_pair_ytd', CAST(volume_for_pair_ytd * 0.23 AS FLOAT)
														)
											))) as MarketPairs

				from (
					SELECT
-- 						assets.base as base,
						market.Symbol as Symbol, 
						market.exchange as exchange, 
						market.quote as quote, 
						market.pair as pair, 												 
						assets.status as pairStatus, 
						assets.last_updated as update_timestamp,
						ticker.type as TypeOfPair,
						CAST(oneDay.current_price_for_pair_1d AS FLOAT) as currentPriceForPair1D,
						CAST(sevenDays.current_price_for_pair_7d AS FLOAT) as currentPriceForPair7D,
-- 						CAST(thirtyDays.current_price_for_pair_30d AS FLOAT) as currentPriceForPair30D,
-- 						CAST(oneYear.current_price_for_pair_1y AS FLOAT) as currentPriceForPair1Y,
-- 						CAST(YTD.current_price_for_pair_ytd AS FLOAT) as currentPriceForPairYTD,
						CAST(oneDay.volume_for_pair_1d AS FLOAT) as volume_for_pair_1d,
						CAST(sevenDays.volume_for_pair_7d AS FLOAT) as volume_for_pair_7d,
-- 						CAST(thirtyDays.volume_for_pair_30d AS FLOAT) as volume_for_pair_30d,
-- 						CAST(oneYear.volume_for_pair_1y AS FLOAT) as volume_for_pair_1y,
-- 						CAST(YTD.volume_for_pair_ytd AS FLOAT) as volume_for_pair_ytd,
						CAST(oneDay.volume_for_pair_1d * 0.23 AS FLOAT) as volume_for_forbes_pair_1d,
						CAST(sevenDays.volume_for_pair_7d * 0.23 AS FLOAT) as volume_for_forbes_pair_7d
-- 						CAST(thirtyDays.volume_for_pair_30d * 0.23 AS FLOAT) as volume_for_forbes_pair_30d,
-- 						CAST(oneYear.volume_for_pair_1y * 0.23 AS FLOAT) as volume_for_forbes_pair_1y,
-- 						CAST(YTD.volume_for_pair_ytd * 0.23 AS FLOAT) as volume_for_forbes_pair_ytd
 					from
						assets
						LEFT JOIN 
							market
						ON
							market.Symbol = assets.base
						LEFT JOIN 
							ticker
						ON
							ticker.base = assets.base
						LEFT JOIN 
							oneDay 
						ON
							oneDay.symbol = assets.base
						LEFT JOIN 
							sevenDays 
						ON
							sevenDays.symbol = assets.base
-- 						LEFT JOIN 
-- 							thirtyDays 
-- 						ON
-- 							thirtyDays.symbol = assets.base
-- 						LEFT JOIN 
-- 							oneYear 
-- 						ON
-- 							oneYear.symbol = assets.base
-- 						LEFT JOIN 
-- 							YTD 
-- 						ON
-- 							YTD.symbol = assets.base
						
				)as foo
group by Symbol
$func$
Language sql
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
Drop function IF EXISTS public.Create1D7ExchangeData();
create or replace function public.Create1D7ExchangeData()
	returns Table (symbol text,exchanges jsonb)
as
$func$
with 
		allTime as 
			(
			SELECT lower(base) as symbol 
			FROM 
				nomics_ohlcv_candles
			GROUP BY 
				base
			),
		ExchangesPrices AS 
			( 
				SELECT 
					lower(base) as Symbol, 
					exchange as Market,
					avg(Price) as Price
				FROM 
					nomics_exchange_market_ticker
				WHERE 
					exchange NOT IN ('bitmex','hbtc')
					AND type = 'spot'
					AND timestamp >=  cast(now() - INTERVAL '4 DAYS' as timestamp)
					AND status = 'active'
					AND quote IN ('USD', 'USDT', 'USDC')
				group by 
					base,
					exchange
			),
		exchangeMetadata as (
			select 
				id, 
				name,
				logo_url
			from 
				nomics_exchange_metadata
			group by
			id, 
				name,
				logo_url
		    ),
		exchangeHighLight as (
			select 
				max(num_markets) as num_markets,
				exchange
			from 
				nomics_exchange_highlight
			group by
				exchange
		    ),
	    oneDay as (
            SELECT 
                exchange,
                min(volume) as volume
            FROM 
                (
                    select 
                        exchange, 
                        CASE WHEN avg(volume) is null THEN CAST(0 AS FLOAT)
                        ELSE avg(volume)
                        END
                        as volume
                    from 
                        nomics_exchange_market_ticker_one_day
                    where 
                        last_updated >= cast(now() - INTERVAL '4 DAYS' as timestamp)
                    group by 
                        exchange
                ) as oneDay
            group by 
                exchange
        ),
		oneDayPrice AS 
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
							timestamp >= cast(now() - INTERVAL '4 DAYS' as timestamp)
							AND quote IN ('USD', 'USDT', 'USDC')
						GROUP BY 
							base
					) as oneDay
			),
		sevenDay as (
            SELECT 
                exchange,
                min(volume) as volume
            FROM 
                (
                    select 
                        exchange, 
                        CASE WHEN avg(volume) is null THEN CAST(0 AS FLOAT)
                        ELSE avg(volume)
                        END
                        as volume
                    from 
                        nomics_exchange_market_ticker_seven_days
                    where 
                        last_updated >= cast(now() - INTERVAL '7 DAY' as timestamp)
                    group by 
                        exchange
                ) as sevenDay
            group by 
                exchange
        ),
		sevenDayPrice AS 
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
							timestamp >= cast(now() - INTERVAL '7 Day' as timestamp)
							AND quote IN ('USD', 'USDT', 'USDC')
						GROUP BY 
							base
					) as sevenDay
			)
			
			select symbol,
			array_to_json(ARRAY_AGG(json_build_object('market',market, 'close',close,
										'time',now(), 
										'slug',slug,     
										'number_of_active_pairs_for_assets',number_of_active_pairs_for_assets,
									    'price_by_exchange_1d',price_by_exchange_1d,
									    'price_by_exchange_7d',price_by_exchange_1d,
									    'nomics',json_build_object(
											'nomics_volume_by_exchange_1d', nomics_volume_by_exchange_1d,
									   		'nomics_volume_by_exchange_7d', nomics_volume_by_exchange_7d)
									   )
					  )) as exchanges
			
			from (
				Select
					ExchangesPrices.Market as Market,
					CASE WHEN ExchangesPrices.Price = 0 THEN null ELSE ExchangesPrices.Price END as Close,
					exchangeMetadata.id as Slug, 
					cast(exchangeHighLight.num_markets as int) as number_of_active_pairs_for_assets,
					cast(oneDay.volume as float) as nomics_volume_by_exchange_1d,
					CAST((oneDayPrice.price_by_exchange_1d) AS FLOAT) as price_by_exchange_1d,
					cast(sevenDay.volume as float) as nomics_volume_by_exchange_7d,
					CAST((sevenDayPrice.price_by_exchange_7d) AS FLOAT) as price_by_exchange_7d,
					oneDayPrice.symbol as symbol
				from 
					allTime 
					INNER JOIN 
						ExchangesPrices 
					ON 
						ExchangesPrices.Symbol = allTime.symbol
					INNER JOIN 
						exchangeMetadata
					ON
						exchangeMetadata.id = ExchangesPrices.Market
					INNER Join 
						exchangeHighLight
					ON 
						exchangeHighLight.exchange = exchangeMetadata.id
					INNER Join 
						oneDay
					ON 
						oneDay.exchange = exchangeMetadata.id

					INNER JOIN
						oneDayPrice 

					ON
						oneDayPrice.symbol = allTime.symbol
					INNER Join 
						sevenDay
					ON 
						sevenDay.exchange = exchangeMetadata.id

					INNER JOIN
						sevenDayPrice 
						on
						sevenDayPrice.symbol = allTime.symbol
			) as foo
		group by symbol
		$func$
		Language sql