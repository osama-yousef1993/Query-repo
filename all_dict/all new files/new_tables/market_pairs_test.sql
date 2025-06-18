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
	oneYear As 
		(
			SELECT 
				Symbol,   
				current_price_for_pair_1y,
				volume_for_pair_1y
			from
				(
					SELECT 
						ticker.Symbol, CAST(ticker.price as FLOAT) as current_price_for_pair_1y,
						CAST(oneDay.volume_for_pair_1y AS FLOAT) as volume_for_pair_1y
					from (
						select 
							lower(base) as Symbol,
							price
						from 
							nomics_exchange_market_ticker
						where 
							timestamp >= cast(now() - INTERVAL '365 DAYS' as timestamp)
							AND base = any(select id from public.activeassets())
						group by 
							base,
							price

					) as ticker
					LEFT JOIN(
						select
							lower(base) as Symbol,
							volume
							as volume_for_pair_1y
						from 
							nomics_exchange_market_ticker_one_year
						where 
							last_updated >= cast(now() - INTERVAL '365 DAYS' as timestamp)
						AND base = any(select id from public.activeassets())
						group by 
							base,
						volume
					) as oneDay
					using(Symbol)
-- 					ON (
-- 						oneDay.Symbol = ticker.Symbol
-- 					)
-- 					group by ticker.Symbol
				) as oneYear
		)
	

-- 	select 
-- 		base, 
-- 		array_to_json(ARRAY_AGG(json_build_object(
-- 											'base', Symbol, 
-- 											'exchange', exchange, 
-- 											'quote', quote, 
-- 											'pair', pair, 												 
-- 											'pairStatus', pairStatus, 
-- 											'update_timestamp',update_timestamp,
-- 											'TypeOfPair', TypeOfPair,
-- 											'currentPriceForPair1Y', CAST(currentPriceForPair1Y AS FLOAT),
-- 											'nomics', json_build_object(
-- 													'volume_for_pair_1y', CAST(volume_for_pair_1y AS FLOAT)
-- 											)
-- 											))) as MarketPairs

		SELECT
			assets.base as base,
			market.Symbol as symbol, 
			market.exchange as exchange, 
			market.quote as quote, 
			market.pair as pair, 												 
			assets.status as pairStatus, 
			assets.last_updated as update_timestamp,
			ticker.type as TypeOfPair,
			CAST(oneYear.current_price_for_pair_1y AS FLOAT) as currentPriceForPair1Y,
			CAST(oneYear.volume_for_pair_1y AS FLOAT) as volume_for_pair_1y
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
				oneYear 
			ON
				oneYear.symbol = assets.base
-- group by assets.base
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
SELECT 
	Symbol,   
	current_price_for_pair_1y,
	volume_for_pair_1y
from
	(
		SELECT 
			ticker.Symbol, CAST(ticker.price as FLOAT) as current_price_for_pair_1y,
			CAST(oneDay.volume_for_pair_1y AS FLOAT) as volume_for_pair_1y
		from (
			select 
				lower(base) as Symbol,
				price
			from 
				nomics_exchange_market_ticker
			where 
				timestamp >= cast(now() - INTERVAL '365 DAYS' as timestamp)
				AND base = any(select id from public.activeassets())
			group by 
				base,
				price

		) as ticker
		LEFT JOIN(
			select
				lower(base) as Symbol,
				volume
				as volume_for_pair_1y
			from 
				nomics_exchange_market_ticker_one_year
			where 
				last_updated >= cast(now() - INTERVAL '365 DAYS' as timestamp)
			AND base = any(select id from public.activeassets())
			group by 
				base,
			volume
		) as oneDay
	 ON (
 		oneDay.Symbol = ticker.Symbol
		)
group by ticker.Symbol, ticker.price, oneDay.volume_for_pair_1y
)
as foo