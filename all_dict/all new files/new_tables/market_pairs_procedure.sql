create or replace PROCEDURE buildExchnages()
LANGUAGE SQL
as $$
	INSERT INTO nomics_market_pairs(base, exchange, quote, pair, pair_status, last_updated, type_of_pair, current_price_for_pair, volume_for_pair) (
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
					),
				oneYear_ticker as(
					select
						lower(base) as Symbol,
						volume
					from 
						nomics_exchange_market_ticker_one_year
					where 
						last_updated >= cast(now() - INTERVAL '365 DAYS' as timestamp)
					AND base = any(select id from public.activeassets())
					group by 
						base,
						volume
				)
					SELECT
						assets.base as base,
						market.exchange as exchange, 
						market.quote as quote, 
						market.pair as pair, 												 
						assets.status as pairStatus, 
						assets.last_updated as update_timestamp,
						ticker.type as TypeOfPair,
						CAST(oneYear.price AS FLOAT),
						CAST(oneYear_ticker.volume AS FLOAT)
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
						LEFT JOIN
							oneYear_ticker
						on 
							oneYear_ticker.symbol = assets.base
				group by 
						assets.base,
						market.exchange , 
						market.quote , 
						market.pair , 												 
						assets.status , 
						assets.last_updated ,
						ticker.type ,
						oneYear.price,
						oneYear_ticker.volume
	)
	on conflict (base, exchange, quote) do Update set 
	base = EXCLUDED.base,
	exchange = EXCLUDED.exchange,
	quote = EXCLUDED.quote,
	pair = EXCLUDED.pair,
	pair_status = EXCLUDED.pair_status,
	last_updated = EXCLUDED.last_updated,
	type_of_pair = EXCLUDED.type_of_pair,
	current_price_for_pair = EXCLUDED.current_price_for_pair,
	volume_for_pair = EXCLUDED.volume_for_pair
$$;

CALL  buildExchnages();




++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
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
		),
	oneYear_ticker as(
		select
			lower(base) as Symbol,
			volume
		from 
			nomics_exchange_market_ticker_one_year
		where 
			last_updated >= cast(now() - INTERVAL '365 DAYS' as timestamp)
		AND base = any(select id from public.activeassets())
		group by 
			base,
			volume
	)
		SELECT
			assets.base as base,
			market.exchange as exchange, 
			market.quote as quote, 
			market.pair as pair, 												 
			assets.status as pairStatus, 
			assets.last_updated as update_timestamp,
			ticker.type as TypeOfPair,
			CAST(oneYear.price AS FLOAT),
			CAST(oneYear_ticker.volume AS FLOAT)
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
			LEFT JOIN
				oneYear_ticker
			on 
				oneYear_ticker.symbol = assets.base
	group by 
			assets.base,
			market.exchange , 
			market.quote , 
			market.pair , 												 
			assets.status , 
			assets.last_updated ,
			ticker.type ,
			oneYear.price,
			oneYear_ticker.volume