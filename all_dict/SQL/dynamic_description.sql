
SELECT 
		global_market_cap,
		((global_market_cap - market_cap_24h ) / global_market_cap) * 100 as change_24h,
		assets_count,
		volume_24h,
		json_build_object('btc', (btc_market_cap / global_market_cap) * 100,'eth', (eth_market_cap / global_market_cap)* 100) as dominance,
		trending
	FROM
		(
			SELECT 
				COUNT(symbol) AS assets_count,
				(
				SELECT  sum(volume_24h)
				FROM (
					SELECT (nomics::JSON ->> 'volume_1d')::float as volume_24h,last_updated, price_24h,
					ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY last_updated asc) AS row_num
					FROM fundamentals f
					WHERE source = 'coingecko'
					AND last_updated >= now() - interval '24 hour'
					and (nomics::JSON ->> 'volume_1d')::float != 0
				) AS f2
				WHERE 
				 row_num = 1

				) as volume_24h,
				(
					SELECT  sum(market_cap)
					FROM (
						SELECT market_cap, ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY last_updated asc) AS row_num
						FROM fundamentals f
						WHERE source = 'coingecko'
						AND last_updated >= now() - interval '24 hour'
						and market_cap > 0
					) AS f2
					WHERE 
					 row_num = 1
				) as market_cap_24h,
			(
						SELECT  sum(market_cap)
						FROM (
							SELECT market_cap ,  ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY last_updated desc) AS row_num
							FROM fundamentals f
							WHERE source = 'coingecko'
							AND last_updated >= cast(now() - interval '24 hour' as timestamp)
							and market_cap > 0
						) AS f2
						WHERE 
						 row_num = 1
				) as global_market_cap,
				(
					SELECT 
						market_cap AS btc_market_cap
					FROM 
						fundamentalslatest
					WHERE 
						source = 'coingecko'
						AND 
						symbol = 'bitcoin' 
				) AS btc_market_cap,

				(
					SELECT 
						market_cap AS eth_market_cap
					FROM 
						fundamentalslatest
					WHERE 
						source = 'coingecko'
						AND 
						symbol = 'ethereum' 
				) AS eth_market_cap,
			(
				select
				array_to_json(ARRAY_AGG(json_build_object('Name', name, 'Slug', slug, 'change_24h', change_value_24h))) as trending
				from(
				select 
					name,
					slug,
					change_value_24h
				from 
					public.tradedAssetsPagination_BySource(2,10,'market_cap','desc','coingecko')
				) as fo
			) as trending
			FROM 
				fundamentalslatest
			WHERE 
				last_updated >= cast(now() - interval '24 HOUR' as timestamp)
				and 
				source = 'coingecko'
  				and 
 				status = 'active'
			
		) AS FOO







++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
with current_cap as (
	SELECT  symbol, market_cap, last_updated
		FROM (
			SELECT market_cap,symbol,last_updated, ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY last_updated desc) AS row_num
			FROM fundamentalslatest f
			WHERE source = 'coingecko'
			AND last_updated >= cast(now() - interval '24 hour' as timestamp)
			and market_cap > 0
		) AS f2
		WHERE 
		 row_num = 1
),
prev_cap as (
	SELECT  symbol, market_cap, last_updated
		FROM (
			SELECT market_cap,symbol,last_updated,
			ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY last_updated desc) AS row_num
			FROM fundamentals f
			WHERE source = 'coingecko'
			AND last_updated >= cast(now() - interval '24 hour' as timestamp)
			and market_cap > 0
		) AS f2
		WHERE 
		 row_num = 1
)
select 
current_cap.symbol,
prev_cap.symbol,
current_cap.market_cap,
prev_cap.market_cap,
current_cap.last_updated,
prev_cap.last_updated
from
prev_cap
left join
current_cap 
on current_cap.symbol = prev_cap.symbol
-- where current_cap.symbol is not null



-- 1,257,740,585,037.012
-- 1191237712038.693
-- 1190229014206.693
-- 1190995607205.693
-- 3958
-- 3880
-- 0078