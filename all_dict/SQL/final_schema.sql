
select sum(d.volume_by_exchange_1d)
FROM fundamentalslatest f,
jsonb_to_recordset(f.exchanges::jsonb) AS d(volume_by_exchange_1d float)
WHERE jsonb_typeof(f.exchanges::jsonb) = 'array'

select d.Price, d.Time
FROM nomics_chart_data f,
json_to_recordset(f.prices::json) AS d(Time timestamp, Price float)
WHERE symbol = 'bitcoin'


SELECT is_index,cast(prices::json-> 0 ->>'Price' as float) as first,cast(prices::json->0->>'Time' as timestamp), source, target_resolution_seconds, prices, symbol, "interval"
	FROM public.nomics_chart_data
	where symbol = 'bitcoin'

-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
CREATE OR REPLACE FUNCTION get_market_cap_24h()
RETURNS TABLE(cap float, prev_cap float, cap_change_24h float)
AS $$
BEGIN
	RETURN QUERY EXECUTE format('SELECT 
		market_cap ,
		prev_market_cap ,
		((prev_market_cap - market_cap)/market_cap) * 100 as change_24h
	FROM (
		SELECT 
		(
			SELECT  sum(m)
			FROM (
				SELECT market_cap as m, ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY last_updated desc) AS row_num
				FROM fundamentals f
				WHERE source = ''%s''
				AND last_updated >= now() - interval ''%s''
				and market_cap > 0
			) AS f2
			WHERE 
			 row_num = 1

		) as prev_market_cap,
		(
			SELECT  sum(p)
			FROM (
				SELECT market_cap as p, ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY last_updated desc) AS row_num
				FROM fundamentals f
				WHERE source = ''%s''
				AND last_updated >= now() - interval ''%s''
				and market_cap > 0
			) AS f2
			WHERE 
			 row_num = 1

		) as market_cap
	) as fo;', 'coingecko', '24 hour', 'coingecko','1 hour');
END;
$$ LANGUAGE plpgsql;

-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
CREATE OR REPLACE FUNCTION get_golbal_market_cap_24h()
RETURNS float
AS $$
DECLARE
    global_market_cap_24h float;
BEGIN
	SELECT  
			sum(market_cap) into global_market_cap_24h
		FROM (
			SELECT market_cap ,  ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY last_updated desc) AS row_num
			FROM fundamentals f
			WHERE source = 'coingecko'
			AND last_updated >= cast(now() - interval '24 hour' as timestamp)
			and market_cap > 0
			) AS f2
		WHERE 
			row_num = 1;
    RETURN global_market_cap_24h;
END;
$$ LANGUAGE plpgsql;




select * from get_golbal_market_cap_24h()


-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++



CREATE OR REPLACE FUNCTION get_prev_market_cap_24h()
RETURNS TABLE(prev_cap float)
AS $$
BEGIN
	RETURN QUERY EXECUTE format('SELECT  
		sum(market_cap) as prev_market_cap
	FROM (
		SELECT 
			market_cap,
			ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY last_updated asc) AS row_num
		FROM 
			fundamentals f
		WHERE 
			source = ''coingecko''
			AND last_updated >= now() - interval ''24 hour''
			and market_cap > 0 
		) AS f2
	WHERE 
	 	row_num = 1');
END;
$$ LANGUAGE plpgsql;




select * from get_volume_24h_prev_market_cap_24h()






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



select market_cap from get_golbal_market_cap_24h()


-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

Create OR REPLACE function GenerateDynamicDescription()
RETURNS Table(current_market_cap float, change_24h float, current_volume_24h float, dominance json, assets_number bigint, trendings json)
AS $$
BEGIN
RETURN QUERY SELECT 
		market_cap ,
		((market_cap - market_cap_24h) / market_cap_24h) * 100 as change_24h,
		volume_24h,
		json_build_object('btc', (btc_market_cap / market_cap) * 100,'eth', (eth_market_cap / market_cap)* 100) as dominance,
		assets_count,
		trending
	FROM
		(
			SELECT 
				SUM(market_cap) AS market_cap,
				COUNT(symbol) AS assets_count,
				(
				 select  
				 	sum((nomics::JSON ->> 'volume_1d')::float) as volume_24h
				 FROM 
						fundamentalslatest
				WHERE 
				 		last_updated >= cast(now() - interval '24 HOUR' as timestamp)
				 		AND
						source = 'coingecko'
  					    and status = 'active'
				) as volume_24h,
				(
					select * from get_market_cap_24h()
				) as market_cap_24h,
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
				array_to_json(ARRAY_AGG(json_build_object('Name', name, 'Slug', slug, 'change24H', change_value_24h))) as trending
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
			
		) AS FOO;
END;
$$ LANGUAGE PLPGSQL;



CREATE OR REPLACE FUNCTION get_market_cap_24h()
RETURNS float
AS $$
DECLARE
    market_cap_48h float;
BEGIN
SELECT  sum(market_cap) into market_cap_48h
FROM (
	SELECT market_cap, ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY last_updated asc) AS row_num
	FROM fundamentals f
	WHERE source = 'coingecko'
 	AND last_updated >= now() - interval '24 hour'
	and market_cap > 0
) AS f2
WHERE 
 row_num = 1;
    RETURN market_cap_48h;
END;
$$ LANGUAGE plpgsql;



SELECT 
		global_market_cap,
		market_cap_24h,
		((market_cap_24h - global_market_cap) / global_market_cap) * 100 as change_24h,
		assets_count,
		volume_24h,
		json_build_object('btc', (btc_market_cap / global_market_cap) * 100,'eth', (eth_market_cap / global_market_cap)* 100) as dominance,
		trending
	FROM
		(
			SELECT 
				SUM(market_cap) AS global_market_cap,
				COUNT(symbol) AS assets_count,
				(
				 select  
				 	sum((nomics::JSON ->> 'volume_1d')::float) as volume_24h
				 FROM 
						fundamentalslatest
				WHERE 
						source = 'coingecko'
--   					    and status = 'active'
-- 						and market_cap > 0
			
				) as volume_24h,
				(
					select * from get_market_cap_24h()
				) as market_cap_24h,
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
--  				and 
-- 				status = 'active'
			
		) AS FOO
	

  select 
market_cap,
prev_market_cap,
((prev_market_cap - market_cap)/market_cap) * 100 as change_btc
from (
	SELECT 
	market_cap,
	(
		SELECT  market_cap
		FROM (
			SELECT symbol,market_cap, last_updated, ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY last_updated desc) AS row_num
			FROM fundamentals f
			WHERE source = 'coingecko'
			AND last_updated >= now() - interval '24 hour'
			and market_cap > 0
		) AS f2
		WHERE 
		 row_num = 1
		 and symbol = 'bitcoin'
		
	) as prev_market_cap
	FROM 
		fundamentalslatest
	WHERE 
		last_updated >= cast(now() - interval '24 HOUR' as timestamp)
		and 
		source = 'coingecko'
		and 
		status = 'active'
		and symbol = 'bitcoin'
) as fo

