CREATE OR REPLACE FUNCTION nft_chains_filter(lim integer, pageNum int, sortBy text, direction text, chain_id text)
returns Table(
	id TEXT,
	contract_address TEXT,
	asset_platform_id TEXT,
	name TEXT,
	symbol TEXT,
	image TEXT,
	description TEXT,
	native_currency TEXT ,
	floor_price_usd FLOAT,
	market_cap_usd FLOAT,
	volume_24h_usd FLOAT,
	floor_price_native FLOAT,
	market_cap_native FLOAT,
	volume_24h_native FLOAT,
	floor_price_in_usd_24h_percentage_change FLOAT,
	number_of_unique_addresses INTEGER,
	number_of_unique_addresses_24h_percentage_change FLOAT,
	total_supply FLOAT,
	slug TEXT,
	website_url TEXT,
	twitter_url TEXT,
	discord_url TEXT,
	last_updated TIMESTAMPTZ,
	full_count bigint
)
AS $$
BEGIN
    RETURN QUERY EXECUTE FORMAT(
        'SELECT 
             id,
             contract_address,
             asset_platform_id,
             name,
             symbol,
             image,
             description,
             native_currency,
             floor_price_usd,
             market_cap_usd,
             volume_24h_usd,
             floor_price_native,
             market_cap_native,
             volume_24h_native,
             floor_price_in_usd_24h_percentage_change,
             number_of_unique_addresses,
             number_of_unique_addresses_24h_percentage_change,
             total_supply,
             slug,
             website_url,
             twitter_url,
             discord_url,
             last_updated,
			 count(id) OVER() AS full_count
        FROM 
            public.nftdatalatest
	    where 
            asset_platform_id = ''%s''
	    order by %s %s NULLS LAST 
        limit %s offset %s',
                            chain_id,
                            sortBy,
                            direction,
                            lim,
                            lim*pageNum
                            ) USING chain_id,sortBy,direction,lim,pageNum;
END
$$ LANGUAGE plpgsql;