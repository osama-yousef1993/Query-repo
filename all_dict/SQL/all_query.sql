with 
trending as (
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
),
prev_market_cap as (
	select prev_cap as prev_market_cap from get_prev_market_cap_24h()
-- 	SELECT  
-- 		sum(market_cap) as prev_market_cap
-- 	FROM (
-- 		SELECT 
-- 			market_cap,
-- 			ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY last_updated asc) AS row_num
-- 		FROM 
-- 			fundamentals f
-- 		WHERE 
-- 			source = 'coingecko'
-- 			AND last_updated >= now() - interval '24 hour'
-- 			and market_cap > 0 
-- 		) AS f2
-- 	WHERE 
-- 	 	row_num = 1
),
global_market_cap as(
select market_cap as market_cap_24h from get_global_market_cap_24h()
-- 	SELECT  
-- 		sum(market_cap) as market_cap_24h
-- 	FROM (
-- 		SELECT 
-- 			market_cap,
-- 			ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY last_updated desc) AS row_num
-- 		FROM 
-- 			fundamentals f
-- 		WHERE 
-- 			source = 'coingecko'
-- 			AND last_updated >= now() - interval '24 hour'
-- 			and market_cap > 0 
-- 		) AS f2
-- 	WHERE 
-- 	 	row_num = 1
),
volume_24h as (
	select 
		(trade_volume_24h_btc * price) as volume_24h
	from (
		select 
			sum(trade_volume_24h_btc) as trade_volume_24h_btc,
			(select price_24h from fundamentalslatest where symbol = 'bitcoin') as price
		from 
			coingecko_exchange_metadata
		where 
			last_updated >= cast(now() - interval '24 HOUR' as timestamp)
	) as fo
),
dynamic_description as (
	SELECT
		assets_count,
		json_build_object('btc', (btc_market_cap / global_market_cap.market_cap_24h) * 100,'eth', (eth_market_cap / global_market_cap.market_cap_24h)* 100) as dominance,
		global_market_cap.market_cap_24h,
		volume_24h.volume_24h,
		((global_market_cap.market_cap_24h - prev_market_cap.prev_market_cap ) / global_market_cap.market_cap_24h) * 100 as change_24h,
		trending.trending as trending
	
	FROM
		(
			SELECT 
				COUNT(symbol) AS assets_count,
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
				) AS eth_market_cap
			FROM 
				fundamentalslatest
			WHERE 
				last_updated >= cast(now() - interval '24 HOUR' as timestamp)
				and 
				source = 'coingecko'
  				and 
 				status = 'active'
	)as fo,
	global_market_cap,
	prev_market_cap,
	trending,
	volume_24h
)

select
	dynamic_description.market_cap_24h,
	dynamic_description.change_24h,
	dynamic_description.assets_count,
	dynamic_description.volume_24h,
	dynamic_description.dominance,
	dynamic_description.trending
from 
	dynamic_description
	
	
	
-- 34,874,751,874.16659
-- 34,168,788,544
	
	
	
	
-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
SELECT id, name, year, description, location, logo_url, website_url, twitter_url, facebook_url, youtube_url, linkedin_url, reddit_url, chat_url, slack_url, telegram_url, blog_url, centralized, decentralized, has_trading_incentive, trust_score, trust_score_rank, trade_volume_24h_btc, trade_volume_24h_btc_normalized, last_updated
	FROM public.coingecko_exchange_metadata
	where name = 'Binance' 
	limit 1;


select 
trade_volume_24h_btc * price
from (
	select sum(trade_volume_24h_btc) as trade_volume_24h_btc,
(select price_24h from fundamentalslatest where symbol = 'bitcoin') as price
from coingecko_exchange_metadata
where last_updated >= cast(now() - interval '24 HOUR' as timestamp)
) as fo

-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
CREATE OR REPLACE FUNCTION get_global_market_cap_24h()
RETURNS TABLE(market_cap float)
AS $$
BEGIN
	RETURN QUERY EXECUTE format('SELECT  
		sum(market_cap) as market_cap_24h
	FROM (
		SELECT market_cap ,  ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY last_updated desc) AS row_num
		FROM fundamentals f
		WHERE source = ''coingecko''
		AND last_updated >= cast(now() - interval ''24 hour'' as timestamp)
		and market_cap > 0
		) AS f2
	WHERE 
		row_num = 1');
END;
$$ LANGUAGE plpgsql;

-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
with prev as (
	SELECT  
		symbol,market_cap , last_updated
	FROM (
		SELECT
			symbol,market_cap , last_updated,
			ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY last_updated asc) AS row_num
		FROM 
			fundamentals f
		WHERE 
			source = 'coingecko'
			AND last_updated >= now() - interval '24 hour'
			and market_cap > 0 
		) AS f2
	WHERE 
 	 	row_num = 1
 	and
 	symbol = 'bitcoin'
),
curr as (
	SELECT  
		symbol,market_cap , last_updated
	FROM (
		SELECT symbol,market_cap , last_updated,  ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY last_updated desc) AS row_num
		FROM fundamentals f
		WHERE source = 'coingecko'
		AND last_updated >= cast(now() - interval '24 hour' as timestamp)
		and market_cap > 0
		) AS f2
	WHERE 
 		row_num = 1
and	symbol = 'bitcoin'
	
)
select
prev.symbol,prev.market_cap ,
curr.symbol,curr.market_cap , curr.last_updated, prev.last_updated
from 
prev
left join
curr
on curr.symbol = prev.symbol
where prev.symbol = 'bitcoin'
-- 1174419892985.7979
-- 1178649419180.8652
-- 1175050447048.2158

-- 1170515115933.4707
-- 1170904964483.4707

-- 1169674638789.5093
-- 1169276315206.878

-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
SELECT  
		sum(market_cap) as prev_market_cap
	FROM (
		SELECT 
			market_cap,
			ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY last_updated asc) AS row_num
		FROM 
			fundamentals f
		WHERE 
			source = 'coingecko'
			AND last_updated >= now() - interval '24 hour'
			and market_cap > 0 
		) AS f2
	WHERE 
	 	row_num = 1

-- 1173557102386.9353
-- 1176735902291.8604
-- 1168917288945.018

-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

select
	array_to_json(ARRAY_AGG(json_build_object('Name', name, 'Slug', slug, 'change_24h', percentage_24h))) as trending
from(
	select 
		name,
		slug,
		percentage_24h
	from 
		public.tradedAssetsPagination_BySource(100,0,'price_24h','desc','coingecko')
	where
		change_value_24h is not null
	limit 2
) as fo
	

-- SELECT 
-- 	symbol,
-- 	display_symbol,						  
-- 	name,
-- 	slug,
-- 	logo,
-- 	temporary_data_delay,
-- 	price_24h,
-- 	percentage_24h,
-- 	change_value_24h,						  
-- 	market_cap,
-- 	(nomics::json->>'volume_1d')::float as volume_1d,
-- 	count(symbol) OVER() AS full_count
-- 	from fundamentalslatest 
-- 	where source = 'coingecko'
-- 	and name != ''''                                        
--     and market_cap is not null
-- 	and change_value_24h is not null
-- 	order by price_24h desc NULLS LAST limit 100 offset 0


-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
DATA_NAMESPACE=_dev
ROWY_PREFIX=dev_
DB_PORT=5432
DB_HOST="forbesdevhpc-dbxtn.forbes.tessell.com"
DB_USER="master"
DB_PASSWORD="wkhzEYwlvpQTGTdR"
DB_NAME="forbes"
DB_SSLMODE=disable
PATCH_SIZE=1000
MON_LIMIT=2000000
CG_RATE_LIMIT=300
COINGECKO_URL="https://pro-api.coingecko.com/api/v3"
COINGECKO_API_KEY=CG-V88xeVE4mSPsP71kS7LVWsDk