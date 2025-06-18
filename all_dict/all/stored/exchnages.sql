Drop function public.Update1D7ExchangeData();
create or replace function public.Update1D7ExchangeData()
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
					AND timestamp >=  cast(now() - INTERVAL '3 DAYS' as timestamp)
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
                        last_updated >= cast(now() - INTERVAL '3 DAYS' as timestamp)
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
							timestamp >= cast(now() - INTERVAL '3 DAYS' as timestamp)
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
					oneDayPrice.symbol
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