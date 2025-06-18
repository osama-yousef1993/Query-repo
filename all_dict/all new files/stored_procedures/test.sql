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
-- 						USING(base)
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




++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
create or replace function public.ExchangesData()
	returns Table (symbol text,exchanges jsonb)
as
$func$
with 
	allTime as 
			(
			SELECT distinct lower(base) as symbol 
			FROM 
				nomics_ohlcv_candles
			where base = any(select * from public.activeassets())
			GROUP BY 
				base,
				timestamp
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
					AND base = any(select id from public.activeassets())
					AND type = 'spot'
					AND timestamp >=  cast(now() - INTERVAL '24 HOUR' as timestamp)
					AND status = 'active'
					AND quote IN ('USD', 'USDT', 'USDC')
				group by 
					base,
					exchange,
					timestamp
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
                        last_updated >= cast(now() - INTERVAL '24 HOUR' as timestamp)
						AND base = any(select id from public.activeassets())
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
							timestamp >= cast(now() - INTERVAL '24 HOUR' as timestamp)
							AND quote IN ('USD', 'USDT', 'USDC')
							AND base = any(select id from public.activeassets())
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
					AND base = any(select id from public.activeassets())
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
						AND base = any(select id from public.activeassets())
						GROUP BY 
							base
					) as sevenDay
			),
		thirtyDay as (
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
                        nomics_exchange_market_ticker_thirty_days
                    where 
                        last_updated >= cast(now() - INTERVAL '30 DAY' as timestamp)
					AND base = any(select id from public.activeassets())
                    group by 
                        exchange
                ) as thirtyDay
            group by 
                exchange
        ),
		thirtyDayPrice AS 
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
							timestamp >= cast(now() - INTERVAL '30 Day' as timestamp)
							AND quote IN ('USD', 'USDT', 'USDC')
						AND base = any(select id from public.activeassets())
						GROUP BY 
							base
					) as thirtyDayPrice
			),
		oneYear as (
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
                        nomics_exchange_market_ticker_one_year
                    where 
                        last_updated >= cast(now() - INTERVAL '365 DAY' as timestamp)
					AND base = any(select id from public.activeassets())
                    group by 
                        exchange
                ) as oneYear
            group by 
                exchange
        ),
		oneYearPrice AS 
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
							timestamp >= cast(now() - INTERVAL '365 Day' as timestamp)
							AND quote IN ('USD', 'USDT', 'USDC')
						AND base = any(select id from public.activeassets())
						GROUP BY 
							base
					) as oneYearPrice
			),
		YTD as (
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
                        nomics_exchange_market_ticker_ytd
                    where 
                        last_updated >= cast(date_trunc('year', current_date) as timestamp)
					AND base = any(select id from public.activeassets())
                    group by 
                        exchange
                ) as YTD
            group by 
                exchange
        ),
		YTDPrice AS 
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
							timestamp >= cast(date_trunc('year', current_date) as timestamp) 
							AND quote IN ('USD', 'USDT', 'USDC')
						AND base = any(select id from public.activeassets())
						GROUP BY 
							base
					) as YTDPrice
			)
			
			select symbol,
			array_to_json(ARRAY_AGG(json_build_object('market',market, 'close',close,
										'time',now(), 
										'slug',slug,     
										'number_of_active_pairs_for_assets',number_of_active_pairs_for_assets,
									    'price_by_exchange_1d',price_by_exchange_1d,
									    'price_by_exchange_7d',price_by_exchange_7d,
										'price_by_exchange_30d',price_by_exchange_30d,
										'price_by_exchange_1y',price_by_exchange_1y,
										'price_by_exchange_ytd',price_by_exchange_ytd,
									    'nomics',json_build_object(
											'nomics_volume_by_exchange_1d', nomics_volume_by_exchange_1d,
									   		'nomics_volume_by_exchange_7d', nomics_volume_by_exchange_7d,
											'nomics_volume_by_exchange_30d', nomics_volume_by_exchange_30d,
											'nomics_volume_by_exchange_1y', nomics_volume_by_exchange_1y,
											'nomics_volume_by_exchange_ytd', nomics_volume_by_exchange_ytd
										)
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
					cast(thirtyDay.volume as float) as nomics_volume_by_exchange_30d,
					CAST((thirtyDayPrice.price_by_exchange_30d) AS FLOAT) as price_by_exchange_30d,
					cast(oneYear.volume as float) as nomics_volume_by_exchange_1y,
					CAST((oneYearPrice.price_by_exchange_1y) AS FLOAT) as price_by_exchange_1y,
					cast(YTD.volume as float) as nomics_volume_by_exchange_ytd,
					CAST((YTDPrice.price_by_exchange_ytd) AS FLOAT) as price_by_exchange_ytd,
					ExchangesPrices.Symbol
				from
					allTime
					LEFT JOIN
					ExchangesPrices 
					USING(base)
					LEFT JOIN 
						exchangeMetadata
					ON
						exchangeMetadata.id = ExchangesPrices.Market
					LEFT Join 
						exchangeHighLight
					ON 
						exchangeHighLight.exchange = exchangeMetadata.id
					LEFT Join 
						oneDay
					ON 
						oneDay.exchange = exchangeMetadata.id

					LEFT JOIN
						oneDayPrice
					using (symbol)
-- 					ON
-- 						oneDayPrice.symbol = ExchangesPrices.Symbol
					LEFT Join 
						sevenDay
					ON 
						sevenDay.exchange = exchangeMetadata.id

					LEFT JOIN
						sevenDayPrice 
					using (symbol)
-- 						on
-- 						sevenDayPrice.symbol = ExchangesPrices.Symbol
					LEFT Join 
						thirtyDay
					ON 
						thirtyDay.exchange = exchangeMetadata.id

					LEFT JOIN
						thirtyDayPrice 
					using (symbol)
-- 					on
-- 						thirtyDayPrice.symbol = ExchangesPrices.Symbol
					LEFT Join 
						oneYear
					ON 
						oneYear.exchange = exchangeMetadata.id
					LEFT JOIN
						oneYearPrice
					using (symbol)
-- 						on
-- 						oneYearPrice.symbol = ExchangesPrices.Symbol
				LEFT Join 
						YTD
					ON 
						YTD.exchange = exchangeMetadata.id
				LEFT JOIN
						YTDPrice
				using (symbol)
-- 						on
-- 						YTDPrice.symbol = ExchangesPrices.Symbol
			) as foo
		group by symbol
		$func$
		Language sql
		
select * from public.ExchangesData()


++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
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
					where base = any (select id from public.activeAssets())
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
							timestamp >= cast(now() - INTERVAL '24 HOUR' as timestamp)
						and base = any (select id from public.activeAssets())
						GROUP BY 
							base,
							timestamp
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
						and base = any (select id from public.activeAssets())
						GROUP BY 
							base,
						timestamp
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
							date_trunc('day', timestamp) as day , count(*),
							base,
							AVG(close) as close
						FROM nomics_ohlcv_candles
						WHERE 
							timestamp >= cast(now() - INTERVAL '30 DAYS' as timestamp)
							and base = any (select id from public.activeAssets())
						group by 
							1,
							base
						ORDER BY 1
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
							date_trunc('day', timestamp) as day , count(*),
							base,
							AVG(close) as close
						FROM nomics_ohlcv_candles
						WHERE 
							timestamp >= cast(now() - INTERVAL '365 DAYS' as timestamp)
							and base = any (select id from public.activeAssets())
						group by 
							1,
							base
						ORDER BY 1
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
							date_trunc('day', timestamp) as day , count(*),
							base,
							AVG(close) as close
						FROM nomics_ohlcv_candles
						WHERE 
							timestamp >= cast(date_trunc('year', current_date) as timestamp)
							and base = any (select id from public.activeAssets())
						group by 
							1,
							base
						ORDER BY 1
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
		USING(symbol)
-- 		ON
-- 		sevenDays.symbol = oneday.symbol
		INNER JOIN 
			thirtyDays
		USING(symbol)
-- 		ON 
-- 			thirtyDays.symbol = oneDay.symbol
		INNER JOIN 
			oneYear
		USING(symbol)
-- 		ON 
-- 			oneYear.symbol = oneDay.symbol
		INNER JOIN 
			allTime
		USING(symbol)
-- 		ON 
-- 			allTime.symbol = oneDay.symbol
		INNER JOIN 
			YTD
		USING(symbol)
-- 		ON 
-- 			YTD.symbol = oneDay.symbol
$func$
Language sql