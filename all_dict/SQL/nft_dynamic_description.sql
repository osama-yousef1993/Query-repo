CREATE OR REPLACE FUNCTION nftPagination_BySource_1(lim int,pageNum int, sortBy Text ,  direction Text, source text)
RETURNS Table (
	symbol Text,
	display_symbol Text,
	name Text,
	slug Text,
	logo Text,
	temporary_data_delay bool,
	price_24h float,
	percentage_1h float,
	percentage_24h float,
	percentage_7d float,
	change_value_24h float,
	market_cap float,
	volume_1d float,
	percentage_volume_1d float,
	full_count bigint
) AS $$

BEGIN
  RETURN QUERY EXECUTE format('SELECT 
	symbol,
	display_symbol,						  
	name,
	slug,
	logo,
	temporary_data_delay,
	price_24h,
	percentage_1h,
	percentage_24h,
	percentage_7d,
	change_value_24h,						  
	market_cap,
	(nomics::json->>''%s'')::float as volume_1d,
	(nomics::json->>''percentageVolume_1d'')::float as percentage_volume_1d,
	count(symbol) OVER() AS full_count
	from NFTDataLatest 
	where source = ''%s''
	and name != ''''                                        
  and market_cap is not null
  and status = ''%s''
	order by %s %s NULLS LAST limit %s offset %s',
							  quote_ident('volume_1d'),
							  source,
                'active',
							  sortBy,
							  direction,
							  lim,
							  lim*pageNum
							 ) USING sortBy,lim,pageNum,direction,source;
END
$$ LANGUAGE plpgsql;



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



-- Add field type for InsertGlobalDescription PROCEDURE So we can use the same Table with different types of Data
CREATE PROCEDURE InsertGlobalDescription(market_cap FLOAT,change_24h FLOAT,volume_24h FLOAT,dominance JSON,assets_count NUMERIC,trending JSON, last_updated timestamp, type TEXT)
LANGUAGE SQL
AS $BODY$
  INSERT INTO global_description
	VALUES (market_cap ,change_24h ,volume_24h ,dominance ,assets_count, trending, last_updated, type);
$BODY$;


-- create NFTPagination So we can use it to build the Trending for NFTs Dynamic Description
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


-- we need to modify the NFTDataLatest and add those field
ALTER TABLE "NFTDataLatest" ADD COLUMN "native_currency_symbol" TEXT;
ALTER TABLE "NFTDataLatest" ADD COLUMN "market_cap_24h_percentage_change_usd" FLOAT;
ALTER TABLE "NFTDataLatest" ADD COLUMN "market_cap_24h_percentage_change_native" FLOAT;
ALTER TABLE "NFTDataLatest" ADD COLUMN "volume_24h_percentage_change_usd" FLOAT;
ALTER TABLE "NFTDataLatest" ADD COLUMN "volume_24h_percentage_change_native" FLOAT;


-- we need to modify the global_description and add this field
-- So we can use it for Assets and NFTs
ALTER TABLE global_description ADD COLUMN "type" FLOAT;