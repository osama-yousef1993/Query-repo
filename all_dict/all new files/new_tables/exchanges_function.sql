create or replace function public.ExchangesData()
	returns Table (symbol text,exchanges jsonb)
as
$func$
with 

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
					AND timestamp >=  cast(now() - INTERVAL '3 DAY' as timestamp)
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
					ExchangesPrices 
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
	

select * from public.ExchangesData();