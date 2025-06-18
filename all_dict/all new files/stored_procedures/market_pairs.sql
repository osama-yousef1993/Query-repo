DROP FUNCTION public.Update1D7MarketPairsData();
create or replace function public.Update1D7MarketPairsData()
	returns Table (symbol text, marketPairs jsonb)
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
						ticker.timestamp >= cast(now() - INTERVAL '3 DAYS' as timestamp)
						AND one.last_updated >= cast(now() - INTERVAL '3 DAYS' as timestamp)
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
		),
	thirtyDays As 
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
		),
	oneYear As 
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
		),
	YTD As 
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
		)

	select 
		base, 
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
											'currentPriceForPair30D', CAST(currentPriceForPair30D AS FLOAT),
											'currentPriceForPair1Y', CAST(currentPriceForPair1Y AS FLOAT),
											'currentPriceForPairYTD', CAST(currentPriceForPairYTD AS FLOAT),
											'nomics', json_build_object(
													'volume_for_pair_1d', volume_for_pair_1d ,
													'volume_for_pair_7d', CAST(volume_for_pair_7d AS FLOAT),
													'volume_for_pair_30d', CAST(volume_for_pair_30d AS FLOAT),
													'volume_for_pair_1y', CAST(volume_for_pair_1y AS FLOAT),
													'volume_for_pair_ytd', CAST(volume_for_pair_ytd AS FLOAT)),
											'forbes', json_build_object(
															'volume_for_forbes_pair_1d', CAST(volume_for_pair_1d * ($1) AS FLOAT),
															'volume_for_forbes_pair_7d', CAST(volume_for_pair_7d * 0.23 AS FLOAT),
															'volume_for_forbes_pair_30d', CAST(volume_for_pair_30d * 0.23 AS FLOAT),
															'volume_for_forbes_pair_1y', CAST(volume_for_pair_1y * 0.23 AS FLOAT),
															'volume_for_forbes_pair_ytd', CAST(volume_for_pair_ytd * 0.23 AS FLOAT)
														)
											))) as MarketPairs

				from (
					SELECT
						assets.base as base,
						market.Symbol as symbol, 
						market.exchange as exchange, 
						market.quote as quote, 
						market.pair as pair, 												 
						assets.status as pairStatus, 
						assets.last_updated as update_timestamp,
						ticker.type as TypeOfPair,
						CAST(oneDay.current_price_for_pair_1d AS FLOAT) as currentPriceForPair1D,
						CAST(sevenDays.current_price_for_pair_7d AS FLOAT) as currentPriceForPair7D,
						CAST(thirtyDays.current_price_for_pair_30d AS FLOAT) as currentPriceForPair30D,
						CAST(oneYear.current_price_for_pair_1y AS FLOAT) as currentPriceForPair1Y,
						CAST(YTD.current_price_for_pair_ytd AS FLOAT) as currentPriceForPairYTD,
						CAST(oneDay.volume_for_pair_1d AS FLOAT) as volume_for_pair_1d,
						CAST(sevenDays.volume_for_pair_7d AS FLOAT) as volume_for_pair_7d,
						CAST(thirtyDays.volume_for_pair_30d AS FLOAT) as volume_for_pair_30d,
						CAST(oneYear.volume_for_pair_1y AS FLOAT) as volume_for_pair_1y,
						CAST(YTD.volume_for_pair_ytd AS FLOAT) as volume_for_pair_ytd,
						CAST(oneDay.volume_for_pair_1d * 0.23 AS FLOAT) as volume_for_forbes_pair_1d,
						CAST(sevenDays.volume_for_pair_7d * 0.23 AS FLOAT) as volume_for_forbes_pair_7d,
						CAST(thirtyDays.volume_for_pair_30d * 0.23 AS FLOAT) as volume_for_forbes_pair_30d,
						CAST(oneYear.volume_for_pair_1y * 0.23 AS FLOAT) as volume_for_forbes_pair_1y,
						CAST(YTD.volume_for_pair_ytd * 0.23 AS FLOAT) as volume_for_forbes_pair_ytd
					from
						assets
						INNER JOIN 
							market
						ON
							market.Symbol = assets.base
						INNER JOIN 
							ticker
						ON
							ticker.base = assets.base
						INNER JOIN 
							oneDay 
						ON
							oneDay.symbol = assets.base
						INNER JOIN 
							sevenDays 
						ON
							sevenDays.symbol = assets.base
						INNER JOIN 
							thirtyDays 
						ON
							thirtyDays.symbol = assets.base
						INNER JOIN 
							oneYear 
						ON
							oneYear.symbol = assets.base
						INNER JOIN 
							YTD 
						ON
							YTD.symbol = assets.base
						
				)as foo
group by base
$func$
Language sql


++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

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
			where base = any(select id from public.activeassets())
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
			where id = any(select id from public.activeassets())
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
				and 
				 base = any(select id from public.activeassets())
			 group by
				 base,
				 type
		 ),
	oneDay As 
		(
			SELECT 
				Symbol,   
				current_price_for_pair_1d,
				volume_for_pair_1d
			from
				(
					SELECT 
						ticker.Symbol, CAST(MIN(ticker.price) as FLOAT) as current_price_for_pair_1d,
						CAST(MIN(oneDay.volume_for_pair_1d) AS FLOAT) as volume_for_pair_1d
					from (
						select 
							lower(base) as Symbol,
							AVG(price) price
						from 
							nomics_exchange_market_ticker
						where 
							timestamp >= cast(now() - INTERVAL '24 HOUR' as timestamp)
							AND base = any(select id from public.activeassets())
						group by 
							base

					) as ticker
					LEFT JOIN(
						select
							lower(base) as Symbol,
							CASE WHEN AVG(volume) is null THEN CAST(0 AS FLOAT)
							ELSE AVG(volume)
							END
							as volume_for_pair_1d
						from 
							nomics_exchange_market_ticker_one_day
						where 
							last_updated >= cast(now() - INTERVAL '24 HOUR' as timestamp)
							AND base = any(select id from public.activeassets())
						group by 
							base
					) as oneDay
					ON (
						oneDay.Symbol = ticker.Symbol
					)
					group by ticker.Symbol
				) as oneDay
		),
	sevenDays As 
		(
			SELECT 
				Symbol,   
				current_price_for_pair_7d,
				volume_for_pair_7d
			from
				(
					SELECT 
						ticker.Symbol, CAST(MIN(ticker.price) as FLOAT) as current_price_for_pair_7d,
						CAST(MIN(oneDay.volume_for_pair_7d) AS FLOAT) as volume_for_pair_7d
					from (
						select 
							lower(base) as Symbol,
							AVG(price) price
						from 
							nomics_exchange_market_ticker
						where 
							timestamp >= cast(now() - INTERVAL '7 DAYS' as timestamp)
							AND base = any(select id from public.activeassets())
						group by 
							base

					) as ticker
					LEFT JOIN(
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
						AND base = any(select id from public.activeassets())
						group by 
							base
					) as oneDay
					ON (
						oneDay.Symbol = ticker.Symbol
					)
					group by ticker.Symbol
				) as sevenDay
		),
	thirtyDays As 
		(
			SELECT 
				Symbol,   
				current_price_for_pair_30d,
				volume_for_pair_30d
			from
				(
					SELECT 
						ticker.Symbol, CAST(MIN(ticker.price) as FLOAT) as current_price_for_pair_30d,
						CAST(MIN(oneDay.volume_for_pair_30d) AS FLOAT) as volume_for_pair_30d
					from (
						select 
							lower(base) as Symbol,
							AVG(price) price
						from 
							nomics_exchange_market_ticker
						where 
							timestamp >= cast(now() - INTERVAL '30 DAYS' as timestamp)
							AND base = any(select id from public.activeassets())
						group by 
							base

					) as ticker
					LEFT JOIN(
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
						AND base = any(select id from public.activeassets())
						group by 
							base
					) as oneDay
					ON (
						oneDay.Symbol = ticker.Symbol
					)
					group by ticker.Symbol
				) as thirtyDays
		),
	oneYear As 
		(
			SELECT 
				Symbol,   
				current_price_for_pair_1y,
				volume_for_pair_1y
			from
				(
					SELECT 
						ticker.Symbol, CAST(MIN(ticker.price) as FLOAT) as current_price_for_pair_1y,
						CAST(MIN(oneDay.volume_for_pair_1y) AS FLOAT) as volume_for_pair_1y
					from (
						select 
							lower(base) as Symbol,
							AVG(price) price
						from 
							nomics_exchange_market_ticker
						where 
							timestamp >= cast(now() - INTERVAL '365 DAYS' as timestamp)
							AND base = any(select id from public.activeassets())
						group by 
							base

					) as ticker
					LEFT JOIN(
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
						AND base = any(select id from public.activeassets())
						group by 
							base
					) as oneDay
					ON (
						oneDay.Symbol = ticker.Symbol
					)
					group by ticker.Symbol
				) as oneYear
		),
	YTD As 
		(
			SELECT 
				Symbol,   
				current_price_for_pair_ytd,
				volume_for_pair_ytd
			from
				(
					SELECT 
						ticker.Symbol, CAST(MIN(ticker.price) as FLOAT) as current_price_for_pair_ytd,
						CAST(MIN(oneDay.volume_for_pair_ytd) AS FLOAT) as volume_for_pair_ytd
					from (
						select 
							lower(base) as Symbol,
							AVG(price) price
						from 
							nomics_exchange_market_ticker
						where 
							timestamp >= cast(date_trunc('year', current_date) as timestamp)
							AND base = any(select id from public.activeassets())
						group by 
							base

					) as ticker
					LEFT JOIN(
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
						AND base = any(select id from public.activeassets())
						group by 
							base
					) as oneDay
					ON (
						oneDay.Symbol = ticker.Symbol
					)
					group by ticker.Symbol
				) as ytd
		)

	select 
		base, 
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
											'currentPriceForPair30D', CAST(currentPriceForPair30D AS FLOAT),
											'currentPriceForPair1Y', CAST(currentPriceForPair1Y AS FLOAT),
											'currentPriceForPairYTD', CAST(currentPriceForPairYTD AS FLOAT),
											'nomics', json_build_object(
													'volume_for_pair_1d', volume_for_pair_1d ,
													'volume_for_pair_7d', CAST(volume_for_pair_7d AS FLOAT),
													'volume_for_pair_30d', CAST(volume_for_pair_30d AS FLOAT),
													'volume_for_pair_1y', CAST(volume_for_pair_1y AS FLOAT),
													'volume_for_pair_ytd', CAST(volume_for_pair_ytd AS FLOAT)),
											'forbes', json_build_object(
															'volume_for_forbes_pair_1d', CAST(volume_for_pair_1d * 0.25 AS FLOAT),
															'volume_for_forbes_pair_7d', CAST(volume_for_pair_7d * 0.23 AS FLOAT),
															'volume_for_forbes_pair_30d', CAST(volume_for_pair_30d * 0.23 AS FLOAT),
															'volume_for_forbes_pair_1y', CAST(volume_for_pair_1y * 0.23 AS FLOAT),
															'volume_for_forbes_pair_ytd', CAST(volume_for_pair_ytd * 0.23 AS FLOAT)
														)
											))) as MarketPairs

				from (
					SELECT
						assets.base as base,
						market.Symbol as symbol, 
						market.exchange as exchange, 
						market.quote as quote, 
						market.pair as pair, 												 
						assets.status as pairStatus, 
						assets.last_updated as update_timestamp,
						ticker.type as TypeOfPair,
						CAST(oneDay.current_price_for_pair_1d AS FLOAT) as currentPriceForPair1D,
						CAST(sevenDays.current_price_for_pair_7d AS FLOAT) as currentPriceForPair7D,
						CAST(thirtyDays.current_price_for_pair_30d AS FLOAT) as currentPriceForPair30D,
						CAST(oneYear.current_price_for_pair_1y AS FLOAT) as currentPriceForPair1Y,
						CAST(YTD.current_price_for_pair_ytd AS FLOAT) as currentPriceForPairYTD,
						CAST(oneDay.volume_for_pair_1d AS FLOAT) as volume_for_pair_1d,
						CAST(sevenDays.volume_for_pair_7d AS FLOAT) as volume_for_pair_7d,
						CAST(thirtyDays.volume_for_pair_30d AS FLOAT) as volume_for_pair_30d,
						CAST(oneYear.volume_for_pair_1y AS FLOAT) as volume_for_pair_1y,
						CAST(YTD.volume_for_pair_ytd AS FLOAT) as volume_for_pair_ytd,
						CAST(oneDay.volume_for_pair_1d * 0.23 AS FLOAT) as volume_for_forbes_pair_1d,
						CAST(sevenDays.volume_for_pair_7d * 0.23 AS FLOAT) as volume_for_forbes_pair_7d,
						CAST(thirtyDays.volume_for_pair_30d * 0.23 AS FLOAT) as volume_for_forbes_pair_30d,
						CAST(oneYear.volume_for_pair_1y * 0.23 AS FLOAT) as volume_for_forbes_pair_1y,
						CAST(YTD.volume_for_pair_ytd * 0.23 AS FLOAT) as volume_for_forbes_pair_ytd
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
						LEFT JOIN 
							thirtyDays 
						ON
							thirtyDays.symbol = assets.base
						LEFT JOIN 
							oneYear 
						ON
							oneYear.symbol = assets.base
						LEFT JOIN 
							YTD 
						ON
							YTD.symbol = assets.base
						
				)as foo
group by base



++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
working with all active assets from nomics_currencies_tickers
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

create or replace function public.MarketPairsData()
	returns Table (symbol text, marketPairs jsonb)
As 
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
			where base = any(select id from public.activeassets())
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
			where id = any(select id from public.activeassets())
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
				and 
				 base = any(select id from public.activeassets())
			 group by
				 base,
				 type
		 ),
	oneDay As 
		(
			SELECT 
				Symbol,   
				current_price_for_pair_1d,
				volume_for_pair_1d
			from
				(
					SELECT 
						ticker.Symbol, CAST(MIN(ticker.price) as FLOAT) as current_price_for_pair_1d,
						CAST(MIN(oneDay.volume_for_pair_1d) AS FLOAT) as volume_for_pair_1d
					from (
						select 
							lower(base) as Symbol,
							AVG(price) price
						from 
							nomics_exchange_market_ticker
						where 
							timestamp >= cast(now() - INTERVAL '24 HOUR' as timestamp)
							AND base = any(select id from public.activeassets())
						group by 
							base

					) as ticker
					LEFT JOIN(
						select
							lower(base) as Symbol,
							CASE WHEN AVG(volume) is null THEN CAST(0 AS FLOAT)
							ELSE AVG(volume)
							END
							as volume_for_pair_1d
						from 
							nomics_exchange_market_ticker_one_day
						where 
							last_updated >= cast(now() - INTERVAL '24 HOUR' as timestamp)
							AND base = any(select id from public.activeassets())
						group by 
							base
					) as oneDay
					ON (
						oneDay.Symbol = ticker.Symbol
					)
					group by ticker.Symbol
				) as oneDay
		),
	sevenDays As 
		(
			SELECT 
				Symbol,   
				current_price_for_pair_7d,
				volume_for_pair_7d
			from
				(
					SELECT 
						ticker.Symbol, CAST(MIN(ticker.price) as FLOAT) as current_price_for_pair_7d,
						CAST(MIN(oneDay.volume_for_pair_7d) AS FLOAT) as volume_for_pair_7d
					from (
						select 
							lower(base) as Symbol,
							AVG(price) price
						from 
							nomics_exchange_market_ticker
						where 
							timestamp >= cast(now() - INTERVAL '7 DAYS' as timestamp)
							AND base = any(select id from public.activeassets())
						group by 
							base

					) as ticker
					LEFT JOIN(
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
						AND base = any(select id from public.activeassets())
						group by 
							base
					) as oneDay
					ON (
						oneDay.Symbol = ticker.Symbol
					)
					group by ticker.Symbol
				) as sevenDay
		),
	thirtyDays As 
		(
			SELECT 
				Symbol,   
				current_price_for_pair_30d,
				volume_for_pair_30d
			from
				(
					SELECT 
						ticker.Symbol, CAST(MIN(ticker.price) as FLOAT) as current_price_for_pair_30d,
						CAST(MIN(oneDay.volume_for_pair_30d) AS FLOAT) as volume_for_pair_30d
					from (
						select 
							lower(base) as Symbol,
							AVG(price) price
						from 
							nomics_exchange_market_ticker
						where 
							timestamp >= cast(now() - INTERVAL '30 DAYS' as timestamp)
							AND base = any(select id from public.activeassets())
						group by 
							base

					) as ticker
					LEFT JOIN(
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
						AND base = any(select id from public.activeassets())
						group by 
							base
					) as oneDay
					ON (
						oneDay.Symbol = ticker.Symbol
					)
					group by ticker.Symbol
				) as thirtyDays
		),
	oneYear As 
		(
			SELECT 
				Symbol,   
				current_price_for_pair_1y,
				volume_for_pair_1y
			from
				(
					SELECT 
						ticker.Symbol, CAST(MIN(ticker.price) as FLOAT) as current_price_for_pair_1y,
						CAST(MIN(oneDay.volume_for_pair_1y) AS FLOAT) as volume_for_pair_1y
					from (
						select 
							lower(base) as Symbol,
							AVG(price) price
						from 
							nomics_exchange_market_ticker
						where 
							timestamp >= cast(now() - INTERVAL '365 DAYS' as timestamp)
							AND base = any(select id from public.activeassets())
						group by 
							base

					) as ticker
					LEFT JOIN(
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
						AND base = any(select id from public.activeassets())
						group by 
							base
					) as oneDay
					ON (
						oneDay.Symbol = ticker.Symbol
					)
					group by ticker.Symbol
				) as oneYear
		),
	YTD As 
		(
			SELECT 
				Symbol,   
				current_price_for_pair_ytd,
				volume_for_pair_ytd
			from
				(
					SELECT 
						ticker.Symbol, CAST(MIN(ticker.price) as FLOAT) as current_price_for_pair_ytd,
						CAST(MIN(oneDay.volume_for_pair_ytd) AS FLOAT) as volume_for_pair_ytd
					from (
						select 
							lower(base) as Symbol,
							AVG(price) price
						from 
							nomics_exchange_market_ticker
						where 
							timestamp >= cast(date_trunc('year', current_date) as timestamp)
							AND base = any(select id from public.activeassets())
						group by 
							base

					) as ticker
					LEFT JOIN(
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
						AND base = any(select id from public.activeassets())
						group by 
							base
					) as oneDay
					ON (
						oneDay.Symbol = ticker.Symbol
					)
					group by ticker.Symbol
				) as ytd
		)

	select 
		base, 
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
											'currentPriceForPair30D', CAST(currentPriceForPair30D AS FLOAT),
											'currentPriceForPair1Y', CAST(currentPriceForPair1Y AS FLOAT),
											'currentPriceForPairYTD', CAST(currentPriceForPairYTD AS FLOAT),
											'nomics', json_build_object(
													'volume_for_pair_1d', volume_for_pair_1d ,
													'volume_for_pair_7d', CAST(volume_for_pair_7d AS FLOAT),
													'volume_for_pair_30d', CAST(volume_for_pair_30d AS FLOAT),
													'volume_for_pair_1y', CAST(volume_for_pair_1y AS FLOAT),
													'volume_for_pair_ytd', CAST(volume_for_pair_ytd AS FLOAT)),
											'forbes', json_build_object(
															'volume_for_forbes_pair_1d', CAST(volume_for_pair_1d AS FLOAT),
															'volume_for_forbes_pair_7d', CAST(volume_for_pair_7d  AS FLOAT),
															'volume_for_forbes_pair_30d', CAST(volume_for_pair_30d AS FLOAT),
															'volume_for_forbes_pair_1y', CAST(volume_for_pair_1y AS FLOAT),
															'volume_for_forbes_pair_ytd', CAST(volume_for_pair_ytd AS FLOAT)
														)
											))) as MarketPairs

				from (
					SELECT
						assets.base as base,
						market.Symbol as symbol, 
						market.exchange as exchange, 
						market.quote as quote, 
						market.pair as pair, 												 
						assets.status as pairStatus, 
						assets.last_updated as update_timestamp,
						ticker.type as TypeOfPair,
						CAST(oneDay.current_price_for_pair_1d AS FLOAT) as currentPriceForPair1D,
						CAST(sevenDays.current_price_for_pair_7d AS FLOAT) as currentPriceForPair7D,
						CAST(thirtyDays.current_price_for_pair_30d AS FLOAT) as currentPriceForPair30D,
						CAST(oneYear.current_price_for_pair_1y AS FLOAT) as currentPriceForPair1Y,
						CAST(YTD.current_price_for_pair_ytd AS FLOAT) as currentPriceForPairYTD,
						CAST(oneDay.volume_for_pair_1d AS FLOAT) as volume_for_pair_1d,
						CAST(sevenDays.volume_for_pair_7d AS FLOAT) as volume_for_pair_7d,
						CAST(thirtyDays.volume_for_pair_30d AS FLOAT) as volume_for_pair_30d,
						CAST(oneYear.volume_for_pair_1y AS FLOAT) as volume_for_pair_1y,
						CAST(YTD.volume_for_pair_ytd AS FLOAT) as volume_for_pair_ytd,
						CAST(oneDay.volume_for_pair_1d * 0.23 AS FLOAT) as volume_for_forbes_pair_1d,
						CAST(sevenDays.volume_for_pair_7d * 0.23 AS FLOAT) as volume_for_forbes_pair_7d,
						CAST(thirtyDays.volume_for_pair_30d * 0.23 AS FLOAT) as volume_for_forbes_pair_30d,
						CAST(oneYear.volume_for_pair_1y * 0.23 AS FLOAT) as volume_for_forbes_pair_1y,
						CAST(YTD.volume_for_pair_ytd * 0.23 AS FLOAT) as volume_for_forbes_pair_ytd
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
						LEFT JOIN 
							thirtyDays 
						ON
							thirtyDays.symbol = assets.base
						LEFT JOIN 
							oneYear 
						ON
							oneYear.symbol = assets.base
						LEFT JOIN 
							YTD 
						ON
							YTD.symbol = assets.base
						
				)as foo
group by base

$func$
Language sql;



select * from public.MarketPairsData();