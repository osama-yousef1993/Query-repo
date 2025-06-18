
create or replace function public.MarketPairsData()
	returns Table (base TEXT, exchange TEXT, quote TEXT, pair TEXT, pairStatus TEXT,
					update_timestamp TIMESTAMPTZ, TypeOfPair TEXT, price FLOAT, volume FLOAT )
As 
$func$
	with
		assets as 
			(
				select
					id,
					status, 
					last_updated
				from 
					nomics_assets
				where id = any(select id from public.activeassets())
				group by 
					id

			),
		market as 
			(
				select
					base, 
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

			ticker as 
			(
				SELECT 
					base,   
					price,
					volume,
					type
				from
					(
						SELECT 
							ticker.base, CAST(ticker.price as FLOAT) as price, ticker.type,
							CAST(oneDay.volume AS FLOAT) as volume
						from (
							select 
								base,
								price,
								type,
								row_number() OVER ()
							from 
								nomics_exchange_market_ticker
							where 
								timestamp >= cast(now() - INTERVAL '365 DAYS' as timestamp)
								AND base = any(select id from public.activeassets())
							group by base,
								price,
								type

						) as ticker
							LEFT JOIN(
							select
								base,
								volume,
								row_number() OVER ()
							from 
								nomics_exchange_market_ticker_one_year
							where 
								last_updated >= cast(now() - INTERVAL '365 DAYS' as timestamp)
								AND base = any(select id from public.activeassets())
								group by base,
								volume
						) as oneDay
						USING (row_number)
					) as oneYear
				)

			SELECT
				assets.id as base,
				market.exchange as exchange, 
				market.quote as quote, 
				market.pair as pair, 												 
				assets.status as status, 
				assets.last_updated as last_updated,
				ticker.type as type,
				ticker.price as price,
				ticker.volume as volume
			from
				assets
				LEFT JOIN 
					market
				ON
					market.base = assets.id
				LEFT JOIN 
					ticker
				ON
					ticker.base = assets.id
			where
				market.exchange is not null
				and market.quote  is not null
				and market.pair  is not null
			group by 
				assets.id,
				market.exchange, 
				market.quote , 
				market.pair, 												 
				assets.status, 
				assets.last_updated,
				ticker.type,
				ticker.price,
				ticker.volume

$func$
Language sql;

select * public.MarketPairsData();


