select
			name, 
			slug,
			display_symbol,
			(market_cap_usd / global_market_cap) * 100 as market_cap_dominance,
			nfts_count
		from (
			select
				name, 
				slug,
				symbol as display_symbol,
				market_cap_usd,
				(
					SELECT  
						sum(market_cap_usd)
					from 
						nftdatalatesttest
				) as global_market_cap,
				(
					SELECT  
						count(id)
					from 
						nftdatalatesttest
				) as nfts_count

			from 
				nftdatalatesttest
			where 
				name in ('CryptoPunks', 'Bored Ape Yacht Club')
			order by 
				name asc 
		) as fo
		


create table global_description_test (
	"market_cap" FLOAT,
	"change_24h" FLOAT,
	"volume_24h" FLOAT,
	"dominance" JSON,
	"assets_count" INT,
	"trending" JSON,
	"last_updated" TIMESTAMPTZ DEFAULT ( Now()),
	"type" TEXT
)
ALTER TABLE global_description ADD COLUMN "type" TEXT;




CREATE PROCEDURE InsertGlobalDescription(market_cap FLOAT,change_24h FLOAT,volume_24h FLOAT,dominance JSON,assets_count NUMERIC,trending JSON, last_updated timestamp, type TEXT)
LANGUAGE SQL
AS $BODY$
  INSERT INTO global_description
	VALUES (market_cap ,change_24h ,volume_24h ,dominance ,assets_count, trending, last_updated, type);
$BODY$;






select * from global_description

SELECT 
	market_cap, 
	change_24h, 
	volume_24h, 
	dominance, 
	assets_count, 
	trending,
	last_updated,
	type
FROM 
	public.global_description_test
Where type = 'NFT'
order by 
	last_updated desc 
limit 1;



SELECT 
		market_cap, 
		change_24h, 
		volume_24h, 
		dominance, 
		assets_count, 
		trending,
		last_updated
	FROM 
		public.global_description_test
	where type = 'NFT'
	order by 
		last_updated desc 
	limit 1;


CREATE PROCEDURE InsertGlobalDescription(market_cap FLOAT,change_24h FLOAT,volume_24h FLOAT,dominance JSON,assets_count NUMERIC,trending JSON, last_updated timestamp, type TEXT)
LANGUAGE SQL
AS $BODY$
  INSERT INTO global_description
	VALUES (market_cap ,change_24h ,volume_24h ,dominance ,assets_count, trending, last_updated, type);
$BODY$;



select
		array_to_json(ARRAY_AGG(json_build_object('Name', name, 'Slug', slug, 'change_24h', volume_24h_percentage_change_usd))) as trending
	from(
			select 
				name,
				slug,
				volume_24h_percentage_change_usd
			from 
				public.NFTPagination(100,0,'market_cap_usd','desc')
			order by 
				volume_24h_percentage_change_usd desc
			limit 2
		) as fo






select
	name, 
	slug,
	display_symbol,
	(market_cap_usd / global_market_cap) * 100 as market_cap_dominance
from (
	select
	name, 
	slug,
	symbol as display_symbol,
	market_cap_usd,
	(
		SELECT  
			sum(market_cap_usd)
		from 
	 		nftdatalatesttest
	) as global_market_cap
	from 
		nftdatalatesttest
 	where 
 		name in ('CryptoPunks', 'Bored Ape Yacht Club')
	order by 
		name asc 
) as fo




select
		array_to_json(ARRAY_AGG(json_build_object('Name', name, 'Slug', slug, 'change_24h', volume_24h_percentage_change_usd))) as trending
	from(
			select 
				name,
				slug,
				volume_24h_percentage_change_usd
			from 
				public.NFTPagination(100,0,'market_cap_usd','desc')
			order by 
				volume_24h_percentage_change_usd desc
			limit 2
		) as fo



CREATE OR REPLACE FUNCTION NFTPagination(lim int,pageNum int, sortBy Text ,  direction Text)
RETURNS Table (
	name Text,
	slug Text,
	volume_24h_percentage_change_usd FLOAT
) AS $$

BEGIN
  RETURN QUERY EXECUTE format(' 
	SELECT 
		name,
		slug,
		volume_24h_percentage_change_usd
	FROM 
		public.nftdatalatesttest
	where 
		market_cap_usd is not null
	order by %s %s NULLS LAST limit %s offset %s',
							  sortBy,
							  direction,
							  lim,
							  lim*pageNum
							 ) USING sortBy,lim,pageNum,direction;
END
$$ LANGUAGE plpgsql;


-- SELECT name, slug, volume_24h_percentage_change_usd, market_cap_usd
-- 	FROM public.nftdatalatesttest
-- 		where market_cap_usd is not null
--  		and volume_24h_percentage_change_usd != 0
-- 	order by market_cap_usd desc NULLS LAST limit 100


select
		array_to_json(ARRAY_AGG(json_build_object('Name', name, 'Slug', slug, 'change_24h', volume_24h_percentage_change_usd))) as trending
	from(
			select 
				name,
				slug,
				volume_24h_percentage_change_usd
			from 
				public.NFTPagination(100,0,'market_cap_usd','desc')
			order by 
				volume_24h_percentage_change_usd desc
			limit 2
		) as fo

    select 
	sum(market_cap_usd) as market_cap,
	sum(volume_24h_usd) as trading_volume,
	(
		select count(id) over() as full_count
		FROM public.nftdatalatest
		where last_updated >= cast(now() - interval '24 hour' as timestamp)
		limit 1
	) as full_count
FROM public.nftdatalatesttest
where last_updated >= cast(now() - interval '24 hour' as timestamp);


-- 6,257,940,071.292167
-- 13,491,133,788


create  table NFTDataLatestTest (
	"id" TEXT,
	"contract_address" TEXT,
	"asset_platform_id" TEXT,
	"name" TEXT,
	"symbol" TEXT,
	"image" TEXT,
	"description" TEXT,
	"native_currency" TEXT,
	"floor_price_usd" FLOAT,
	"market_cap_usd" FLOAT,
	"volume_24h_usd" FLOAT,
	"floor_price_native" FLOAT,
	"market_cap_native" FLOAT,
	"volume_24h_native" FLOAT,
	"floor_price_in_usd_24h_percentage_change" FLOAT,
	"number_of_unique_addresses" INTEGER,
	"number_of_unique_addresses_24h_percentage_change" FLOAT,
	"total_supply" FLOAT,
	"slug" TEXT,
	"website_url" TEXT,
	"twitter_url" TEXT,
	"discord_url" TEXT,
	"last_updated" TIMESTAMPTZ DEFAULT ( Now()),
	primary key("id")
);

ALTER TABLE NFTDataLatestTest ADD COLUMN "native_currency_symbol" TEXT;
ALTER TABLE NFTDataLatestTest ADD COLUMN "market_cap_24h_percentage_change_usd" FLOAT;
ALTER TABLE NFTDataLatestTest ADD COLUMN "market_cap_24h_percentage_change_native" FLOAT;
ALTER TABLE NFTDataLatestTest ADD COLUMN "volume_24h_percentage_change_usd" FLOAT;
ALTER TABLE NFTDataLatestTest ADD COLUMN "volume_24h_percentage_change_native" FLOAT;

select * from NFTDataLatest

select * from global_description


CREATE OR REPLACE FUNCTION NFTPagination(lim int,pageNum int, sortBy Text ,  direction Text)
RETURNS Table (
	name Text,
	slug Text,
	volume_24h_percentage_change_usd FLOAT
) AS $$

BEGIN
  RETURN QUERY EXECUTE format(' 
	SELECT 
		name,
		slug,
		volume_24h_percentage_change_usd
	FROM 
		public.nftdatalatest
	where 
		market_cap_usd is not null
	order by %s %s NULLS LAST limit %s offset %s',
							  sortBy,
							  direction,
							  lim,
							  lim*pageNum
							 ) USING sortBy,lim,pageNum,direction;
END
$$ LANGUAGE plpgsql;



