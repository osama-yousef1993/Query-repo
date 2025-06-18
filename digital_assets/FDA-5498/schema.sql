CREATE TABLE fundamentals
(
    symbol TEXT,
    name TEXT,
    slug TEXT,
    logo TEXT,
    float_type TEXT,
    display_symbol TEXT,
    original_symbol TEXT,
    source TEXT,
    temporary_data_delay BOOLEAN,
    number_of_active_market_pairs INTEGER,
    high_24h FLOAT,
    low_24h FLOAT,
    high_7d FLOAT,
    low_7d FLOAT,
    high_30d FLOAT,
    low_30d FLOAT,
    high_1y FLOAT,
    low_1y FLOAT,
    high_ytd FLOAT,
    low_ytd FLOAT,
    price_24h FLOAT,
    price_7d FLOAT,
    price_30d FLOAT,
    price_1y FLOAT,
    price_ytd FLOAT,
    percentage_24h FLOAT,
    percentage_7d FLOAT,
    percentage_30d FLOAT,
    percentage_1y FLOAT,
    percentage_ytd FLOAT,
    market_cap FLOAT,
    market_cap_percent_change_1d FLOAT,
    market_cap_percent_change_7d FLOAT,
    market_cap_percent_change_30d FLOAT,
    market_cap_percent_change_1y FLOAT,
    market_cap_percent_change_ytd FLOAT,
    circulating_supply NUMERIC,
    supply NUMERIC,
    all_time_low FLOAT,
    all_time_high FLOAT,
    date TIMESTAMPTZ,
    change_value_24h FLOAT,
    listed_exchange VARCHAR(100)[],
    market_pairs JSON,
    exchanges JSON,
    nomics JSON,
    forbes JSON,
    last_updated TIMESTAMPTZ DEFAULT Now(),
    percentage_1h FLOAT,
);
CREATE INDEX ON "fundamentals" ("symbol");
CREATE INDEX ON "fundamentals" ("last_updated");


CREATE TABLE exchange_fundamentals 
(
    name TEXT,
    slug TEXT,
    id TEXT,
    logo TEXT,
    exchange_active_market_pairs NUMERIC,
    nomics JSON,
    forbes JSON,
    last_updated TIMESTAMPTZ DEFAULT Now()
);

CREATE TABLE chart_data_fundamentals
(
    symbol TEXT,
    forbes TEXT,
    time TIMESTAMPTZ,
    price FLOAT,
    data_source TEXT
);

CREATE TABLE nomics_chart_data 
(
    is_index BOOLEAN,
    source TEXT,
    target_resolution_seconds INTEGER,
    prices JSON,
    symbol TEXT,
    interval TEXT Primary key
);



CREATE TABLE fundamentalslatest
(
    symbol TEXT,
    name TEXT,
    slug TEXT,
    logo TEXT,
    float_type TEXT,
    display_symbol TEXT,
    original_symbol TEXT,
    source TEXT,
    temporary_data_delay BOOLEAN,
    number_of_active_market_pairs INTEGER,
    high_24h FLOAT,
    low_24h FLOAT,
    high_7d FLOAT,
    low_7d FLOAT,
    high_30d FLOAT,
    low_30d FLOAT,
    high_1y FLOAT,
    low_1y FLOAT,
    high_ytd FLOAT,
    low_ytd FLOAT,
    price_24h FLOAT,
    price_7d FLOAT,
    price_30d FLOAT,
    price_1y FLOAT,
    price_ytd FLOAT,
    percentage_24h FLOAT,
    percentage_7d FLOAT,
    percentage_30d FLOAT,
    percentage_1y FLOAT,
    percentage_ytd FLOAT,
    market_cap FLOAT,
    market_cap_percent_change_1d FLOAT,
    market_cap_percent_change_7d FLOAT,
    market_cap_percent_change_30d FLOAT,
    market_cap_percent_change_1y FLOAT,
    market_cap_percent_change_ytd FLOAT,
    circulating_supply NUMERIC,
    supply NUMERIC,
    all_time_low FLOAT,
    all_time_high FLOAT,
    date TIMESTAMPTZ,
    change_value_24h FLOAT,
    listed_exchange VARCHAR(100)[],
    market_pairs JSON,
    exchanges JSON,
    nomics JSON,
    forbes JSON,
    last_updated TIMESTAMPTZ DEFAULT Now(),
    forbes_transparency_volume FLOAT,
    status TEXT,
    percentage_1h FLOAT,
    PRIMARY KEY (symbol)
);


CREATE PROCEDURE upsertFundamentalsLatest(symbol TEXT,name TEXT,slug TEXT,logo TEXT,float_type TEXT,display_symbol TEXT,original_symbol TEXT,source TEXT,temporary_data_delay BOOLEAN,number_of_active_market_pairs INTEGER,high_24h FLOAT,low_24h FLOAT,high_7d FLOAT,low_7d FLOAT,high_30d FLOAT,low_30d FLOAT,high_1y FLOAT,low_1y FLOAT,high_ytd FLOAT,low_ytd FLOAT,price_24h FLOAT,price_7d FLOAT,price_30d FLOAT,price_1y FLOAT,price_ytd FLOAT,percentage_24h FLOAT,percentage_7d FLOAT,percentage_30d FLOAT,percentage_1y FLOAT,percentage_ytd FLOAT,market_cap FLOAT,market_cap_percent_change_1d FLOAT,market_cap_percent_change_7d FLOAT,market_cap_percent_change_30d FLOAT,market_cap_percent_change_1y FLOAT,market_cap_percent_change_ytd FLOAT,circulating_supply NUMERIC,supply NUMERIC,all_time_low FLOAT,all_time_high FLOAT,date TIMESTAMPTZ,change_value_24h FLOAT,listed_exchange VARCHAR(100)[],market_pairs JSON,exchanges JSON,nomics JSON,forbes JSON,last_updated timestamp, forbes_transparency_volume FLOAT, status TEXT,percentage_1h FLOAT)
LANGUAGE SQL
AS $BODY$
    INSERT INTO fundamentalslatest 

	VALUES (symbol, name, slug, logo, float_type, display_symbol, original_symbol, source, temporary_data_delay, number_of_active_market_pairs, high_24h, low_24h, high_7d, low_7d, high_30d, low_30d, high_1y, low_1y, high_ytd, low_ytd, price_24h, price_7d, price_30d, price_1y, price_ytd, percentage_24h, percentage_7d, percentage_30d, percentage_1y, percentage_ytd,  market_cap, market_cap_percent_change_1d, market_cap_percent_change_7d, market_cap_percent_change_30d, market_cap_percent_change_1y, market_cap_percent_change_ytd, circulating_supply, supply, all_time_low, all_time_high, date, change_value_24h, listed_exchange, market_pairs, exchanges, nomics, forbes,last_updated, forbes_transparency_volume, status, percentage_1h) ON CONFLICT (symbol) DO UPDATE SET symbol = EXCLUDED.symbol, name = EXCLUDED.name, slug = EXCLUDED.slug, logo = EXCLUDED.logo, float_type = EXCLUDED.float_type, display_symbol = EXCLUDED.display_symbol, original_symbol = EXCLUDED.original_symbol, source = EXCLUDED.source, temporary_data_delay = EXCLUDED.temporary_data_delay, number_of_active_market_pairs = EXCLUDED.number_of_active_market_pairs, high_24h = EXCLUDED.high_24h, low_24h = EXCLUDED.low_24h, high_7d = EXCLUDED.high_7d, low_7d = EXCLUDED.low_7d, high_30d = EXCLUDED.high_30d, low_30d = EXCLUDED.low_30d, high_1y = EXCLUDED.high_1y, low_1y = EXCLUDED.low_1y, high_ytd = EXCLUDED.high_ytd, low_ytd = EXCLUDED.low_ytd, price_24h = EXCLUDED.price_24h, price_7d = EXCLUDED.price_7d, price_30d = EXCLUDED.price_30d, price_1y = EXCLUDED.price_1y, price_ytd = EXCLUDED.price_ytd,market_cap_percent_change_1d = EXCLUDED.market_cap_percent_change_1d, market_cap_percent_change_7d = EXCLUDED.market_cap_percent_change_7d,market_cap_percent_change_30d  = EXCLUDED.market_cap_percent_change_30d,market_cap_percent_change_1y = EXCLUDED.market_cap_percent_change_1y,market_cap_percent_change_ytd  = EXCLUDED.market_cap_percent_change_ytd, circulating_supply = EXCLUDED.circulating_supply, supply = EXCLUDED.supply, all_time_low = EXCLUDED.all_time_low, all_time_high = EXCLUDED.all_time_high, date = EXCLUDED.date, change_value_24h = EXCLUDED.change_value_24h, listed_exchange = EXCLUDED.listed_exchange, market_pairs = EXCLUDED.market_pairs, exchanges = EXCLUDED.exchanges, nomics = EXCLUDED.nomics, last_updated = EXCLUDED.last_updated, status = EXCLUDED.status, percentage_24h = EXCLUDED.percentage_24h, percentage_7d = EXCLUDED.percentage_7d, percentage_30d = EXCLUDED.percentage_30d, percentage_1y = EXCLUDED.percentage_1y, percentage_ytd = EXCLUDED.percentage_ytd, percentage_1h = EXCLUDED.percentage_1h, market_cap = EXCLUDED.market_cap;

$BODY$;


CREATE
OR REPLACE PROCEDURE updateChartData(CANDLES JSONB, SYM TEXT) AS $ $ declare trgt_res_seconds int [] := array [14400,43200,432000,1296000];

declare intervals int[] := array[ '14400', 
'43200', 
'432000', 
'1296000' ];
times interval[] := array[ '7 DAYS', 
'30 DAYS', 
'1 YEARS', 
'50 YEARS' ];
lastInsertedTime TEXT;
candlesRefined json;
idx int := 1;
sec int;
begin foreach sec in array intervals loop -- get the last time

-- Get the time from the last inserted candle in the chart data by symbol and target resoltion second
lastInsertedTime := (
  SELECT 
    prices ->-1 -> 'Time' as timestamp 
  FROM 
    nomics_chart_data 
  where 
    target_resolution_seconds = sec 
    and symbol = sym
);
-- compare all of the candles times to the last inserted time
-- if its > that the las inserted time we will include it in the candlesRefined object.
-- This helps avoid dupliacates from being entered
candlesRefined := (
  SELECT 
    json_agg(value) 
  FROM 
    jsonb_array_elements(candles) 
  where 
    cast(
      cast(value -> 'Time' as TEXT) as timestamp
    ) > lastInsertedTime :: timestamp
);

if candlesRefined is not NULL 
then 

insert into nomics_chart_data -- 3. insert the results into nomics_chart_data  
select  -- 2. Get all of the information from step 1, and append all of the refinedCandles to it
  is_index, 
  source, 
  target_resolution_seconds, 
  cast(
    json_agg(prices):: jsonb || candlesRefined :: jsonb as json
  ) as prices, 
  symbol, 
  interval 
from 
  (
    select -- 1. get all prices, and exclude prices that fall out of range 
      is_index, 
      source, 
      target_resolution_seconds, 
      json_array_elements(prices) as prices, 
      symbol, 
      interval 
    from 
      nomics_chart_data 
    where 
      symbol = SYM 
      and target_resolution_seconds = sec
  ) as foo 
where 
  cast(prices ->> 'Time' as timestamp) >= cast(now() - times[idx] as timestamp) 
group by 
  is_index, 
  source, 
  target_resolution_seconds, 
  symbol, 
  interval ON CONFLICT(interval) DO 
UPDATE 
SET 
  prices = EXCLUDED.prices;
END IF;
idx := idx + 1;
--raise info '%',candlesRefined;
end loop;
end;


$ $ LANGUAGE PLPGSQL;

	VALUES (symbol, name, slug, logo, float_type, display_symbol, original_symbol, source, temporary_data_delay, number_of_active_market_pairs, 
    high_24h, low_24h, high_7d, low_7d, high_30d, low_30d, high_1y, low_1y, high_ytd, low_ytd, price_24h, price_7d, price_30d, price_1y, price_ytd, 
    percentage_24h, percentage_7d, percentage_30d, percentage_1y, percentage_ytd, market_cap, 
    market_cap_percent_change_1d, market_cap_percent_change_7d, market_cap_percent_change_30d, market_cap_percent_change_1y, market_cap_percent_change_ytd, 
    circulating_supply, supply, all_time_low, all_time_high, date, change_value_24h, listed_exchange, market_pairs, exchanges, nomics, forbes,last_updated, forbes_transparency_volume) 
    ON CONFLICT (symbol) DO UPDATE SET symbol = EXCLUDED.symbol, name = EXCLUDED.name, slug = EXCLUDED.slug, logo = EXCLUDED.logo, float_type = EXCLUDED.float_type, 
    display_symbol = EXCLUDED.display_symbol, original_symbol = EXCLUDED.original_symbol, source = EXCLUDED.source, temporary_data_delay = EXCLUDED.temporary_data_delay, 
    number_of_active_market_pairs = EXCLUDED.number_of_active_market_pairs, high_24h = EXCLUDED.high_24h, low_24h = EXCLUDED.low_24h, high_7d = EXCLUDED.high_7d, low_7d = EXCLUDED.low_7d, 
    high_30d = EXCLUDED.high_30d, low_30d = EXCLUDED.low_30d, high_1y = EXCLUDED.high_1y, low_1y = EXCLUDED.low_1y, high_ytd = EXCLUDED.high_ytd, low_ytd = EXCLUDED.low_ytd, 
    price_24h = EXCLUDED.price_24h, price_7d = EXCLUDED.price_7d, price_30d = EXCLUDED.price_30d, price_1y = EXCLUDED.price_1y, price_ytd = EXCLUDED.price_ytd,
    percentage_24h = EXCLUDED.percentage_24h, percentage_7d = EXCLUDED.percentage_7d, percentage_30d = EXCLUDED.percentage_30d, 
    percentage_1y = EXCLUDED.percentage_1y, percentage_ytd = EXCLUDED.percentage_ytd, market_cap = EXCLUDED.market_cap, 
    market_cap_percent_change_1d = EXCLUDED.market_cap_percent_change_1d, market_cap_percent_change_7d = EXCLUDED.market_cap_percent_change_7d,
    market_cap_percent_change_30d  = EXCLUDED.market_cap_percent_change_30d,market_cap_percent_change_1y = EXCLUDED.market_cap_percent_change_1y,
    market_cap_percent_change_ytd  = EXCLUDED.market_cap_percent_change_ytd, circulating_supply = EXCLUDED.circulating_supply, 
    supply = EXCLUDED.supply, all_time_low = EXCLUDED.all_time_low, all_time_high = EXCLUDED.all_time_high, 
    date = EXCLUDED.date, change_value_24h = EXCLUDED.change_value_24h, listed_exchange = EXCLUDED.listed_exchange, 
    market_pairs = EXCLUDED.market_pairs, exchanges = EXCLUDED.exchanges, nomics = EXCLUDED.nomics, 
    last_updated = EXCLUDED.last_updated, forbes_transparency_volume= EXCLUDED.forbes_transparency_volume;
$BODY$;


-- Returns all chart data.
-- if the interval is not 24hrs it returns all data for 7d/30d/1y/max, all charts are appended with the last ticker from 24h
-- if the interval is 24h it returns all data it returns all data  24h/7d/30d/1y/max,
CREATE or REPLACE FUNCTION getChartData(intval TEXT,symb TEXT)
RETURNS Table (
is_index bool, 
source TEXT, 
target_resolution_seconds int, 
prices jsonb,
symbol TEXT,
tm_interval TEXT,
status TEXT
) AS $$
#variable_conflict use_column
begin
--If we are not requesting a 24 hour chart return every chart 
--exluding 24hr. The charts will be used in FDA API
if intval not like  '%24h%'
then
RETURN QUERY select
is_index, 
a.source as source , 
a.target_resolution_seconds as target_resolution_seconds , 
--append 24 hour cadle to chart that will be returned
a.prices::jsonb || b.prices::jsonb as prices,
a.symbol as symbol,
a.interval as tm_interval,
c.status as status
from (
  --get chart for specified interval
SELECT 
			is_index, 
			source, 
			target_resolution_seconds, 
			prices, 
			symbol,
			interval
		FROM 
			nomics_chart_data
		WHERE 
			target_resolution_seconds != 900
      			and assetType = 'FT'
	    order by target_resolution_seconds asc
) a -- 
 join (
--get last candle from 24 hr chart
SELECT symbol, prices->-1 as prices
FROM   nomics_chart_data 
where target_resolution_seconds = 900 and symbol = symb and assetType = 'FT') b
 on  b.symbol = a.symbol
join (select symbol,status from fundamentalslatest where symbol = symb ) c
on a.symbol = c.symbol;
--If we are not requesting a 24 hour chart return every chart 
--exluding 24hr. The charts will be used in FDA API
else
RETURN QUERY select
is_index, 
a.source as source , 
a.target_resolution_seconds as target_resolution_seconds , 
--append 24 hour cadle to chart that will be returned
a.prices::jsonb as prices,
a.symbol as symbol,
a.interval as tm_interval,
c.status as status
from (
  --get chart for specified interval
SELECT 
			is_index, 
			source, 
			target_resolution_seconds, 
			prices, 
			symbol,
			interval
		FROM 
			nomics_chart_data
			where symbol = symb
      			and assetType = 'FT'
		order by target_resolution_seconds asc
) a 
join (select symbol,status from fundamentalslatest where symbol = symb ) c
on a.symbol = c.symbol;

 end if;
 end;
	
	$$
	language PLPGSQL;
  
 
 CREATE PROCEDURE removeEntryFromFundamentalslatest(sym TEXT)
LANGUAGE SQL
AS $BODY$
	delete from fundamentalslatest where symbol ilike sym

$BODY$;

/*
  Takes a limit, a pageNumber,a string to sort by, and the direction,and source(our data source ex:coingecko)
*/
CREATE OR REPLACE FUNCTION tradedAssetsPagination_BySource_1(lim int,pageNum int, sortBy Text ,  direction Text, source text)
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
	count(symbol) OVER() AS full_count,
  market_cap_percent_change_1d double precision
	from fundamentalslatest 
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

/*
  Takes a source as input and returns all traded assets
*/
CREATE OR REPLACE FUNCTION searchTradedAssetsBySource( source text)
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
	status Text,
  market_cap_percent_change_1d float,
  rank_number bigint
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
    status,
    market_cap_percent_change_1d,
    RANK () OVER ( 
      partition by status
      ORDER BY market_cap desc 
    ) rank_number
	from fundamentalslatest 
	where source = ''%s''
    and name != ''''                                        
    and market_cap is not null',
							  quote_ident('volume_1d'),
							  source
							 ) USING source;
END
$$ LANGUAGE plpgsql;

/*
  DEPRECATED in favour of getCategoriesV2()
*/
CREATE OR REPLACE FUNCTION getCategories()
RETURNS Table (
  id Text,
  name Text,
  market_cap float,
  market_cap_change_24h float,
  content Text,
  top_3_coins Text[],
  volume_24h float,
  last_updated TIMESTAMPTZ,
  markets json

) AS $$
BEGIN
  RETURN QUERY EXECUTE format('SELECT 
			id,
			name,
			market_cap,
			market_cap_change_24h,
			content,
			top_3_coins::text[] as top_3_coins,
			volume_24h,
			last_updated,
			markets
		FROM 
			public.coingecko_categories'
  );
END
$$ LANGUAGE plpgsql;

/**
* Get all categories list from coingecko_categories
*/
CREATE OR REPLACE FUNCTION getCategoriesV2()
RETURNS Table (
  id Text,
  name Text,
  market_cap float,
  market_cap_change_24h float,
  content Text,
  top_3_coins Text[],
  volume_24h float,
  last_updated TIMESTAMPTZ,
  markets json,
  inactive boolean
) AS $$
BEGIN
  RETURN QUERY EXECUTE format('SELECT 
			id,
			name,
			market_cap,
			market_cap_change_24h,
			content,
			top_3_coins::text[] as top_3_coins,
			volume_24h,
			last_updated,
			markets,
      coalesce(inactive, false) as inactive
		FROM 
			public.coingecko_categories'
  );
END
$$ LANGUAGE plpgsql;

/* stored procedure for get crypto content */
create or replace function getcryptocontent(slg text)
	RETURNS Table (
	symbol Text,
	display_symbol Text,
	slug Text,
	status Text,
	market_cap float,
	price_24h float,
	number_of_active_market_pairs int,
	COALESCE(description, '') as description,
	COALESCE(name, '') as name,
	COALESCE(website_url, '') as website_url,
  COALESCE(blog_url, '') as blog_url,
  COALESCE(discord_url, '') as discord_url,
  COALESCE(facebook_url, '') as facebook_url,
  COALESCE(github_url, '') as github_url,
  COALESCE(medium_url, '') as medium_url,
  COALESCE(reddit_url, '') as reddit_url,
  COALESCE(telegram_url, '') as telegram_url,
  COALESCE(twitter_url, '') as twitter_url,
  COALESCE(whitepaper_url, '') as whitepaper_url,
  COALESCE(youtube_url, '') as youtube_url,
  COALESCE(bitcointalk_url, '') as bitcointalk_url,
  COALESCE(blockexplorer_url, '') as blockexplorer_url,
  COALESCE(logo_url, '') as logo_url,
  forbesMetaDataDescription
) 
as
$func$
	
	SELECT 
    symbol,
	display_symbol,
	slug,
	status,
	market_cap,
	price_24h,
	number_of_active_market_pairs,
	description,
	name,
	website_url,
    blog_url,
    discord_url,
    facebook_url,
    github_url,
    medium_url,
    reddit_url,
    telegram_url,
    twitter_url,
    whitepaper_url,
    youtube_url,
    bitcointalk_url,
    blockexplorer_url,
    logo_url,
    forbesMetaDataDescription
FROM   (SELECT symbol,
			   display_symbol,
               slug,
               status,
               market_cap,
               price_24h,
               number_of_active_market_pairs
        FROM   fundamentalslatest
        WHERE  slug = slg) a
       LEFT JOIN (SELECT id,
                         description,
                         name,
                         website_url,
                         blog_url,
                         discord_url,
                         facebook_url,
                         github_url,
                         medium_url,
                         reddit_url,
                         telegram_url,
                         twitter_url,
                         whitepaper_url,
                         youtube_url,
                         bitcointalk_url,
                         blockexplorer_url,
                         logo_url,
                         forbesMetaDataDescription
                         
                  FROM   coingecko_asset_metadata) b
              ON a.symbol = b.id
$func$
Language sql



-- get tob exchanges by trust score
create or replace FUNCTION getTopExchanges()
RETURNS Table(
  id text,
	name TEXT,
	year INTEGER,
	description TEXT,
	location TEXT,
	logo_url TEXT,
	website_url TEXT,
	twitter_url TEXT,
	facebook_url TEXT,
	youtube_url TEXT,
	linkedin_url TEXT,
	reddit_url TEXT,
	chat_url TEXT,
	slack_url TEXT,
	telegram_url TEXT,
	blog_url TEXT,
	centralized BOOLEAN,
	decentralized BOOLEAN,
	has_trading_incentive BOOLEAN,
	trust_score INTEGER,
	trust_score_rank INTEGER,
	trade_volume_24h_btc FLOAT,
	trade_volume_24h_btc_normalized FLOAT,
	last_updated TIMESTAMPTZ
)
as
$$
DECLARE lim int := 5;
BEGIN
   RETURN QUERY EXECUTE format('
							SELECT 
                id as symbol,
                name as exchange_name, 
                year as exchange_year, 
                description, 
                location, 
                logo_url, 
                website_url, 
                twitter_url, 
                facebook_url, 
                youtube_url, 
                linkedin_url, 
                reddit_url, 
                chat_url, 
                slack_url, 
                telegram_url, 
                blog_url, 
                centralized, 
                decentralized, 
                has_trading_incentive, 
                trust_score, 
                trust_score_rank, 
                trade_volume_24h_btc, 
                trade_volume_24h_btc_normalized, 
                last_updated
              FROM 
                public.coingecko_exchange_metadata
              where 
                trust_score is not null 
              order by trust_score desc
              limit %s;', lim) USING lim; 
	
END;
$$
Language plpgsql;

CREATE TABLE exchange_fundamentalslatest
(
    name TEXT,
    slug TEXT,
    id TEXT,
    logo TEXT,
    exchange_active_market_pairs NUMERIC,
    nomics JSON,
    forbes JSON,
    last_updated TIMESTAMPTZ DEFAULT Now(),
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS categories_fundamentals
(
    id TEXT,
    name TEXT,
    total_tokens INTEGER,
    average_percentage_24h FLOAT,
    volume_24h FLOAT,
    price_24h FLOAT,
    average_price FLOAT,
    market_cap FLOAT,
    market_cap_percentage_24h FLOAT,
    price_weight_index FLOAT,
    market_cap_weight_index FLOAT,
    index_market_cap_24h FLOAT,
    index_market_cap_percentage_24h FLOAT,
    divisor FLOAT,
    top_gainers JSON,
    top_movers JSON,
    last_updated TIMESTAMPTZ DEFAULT Now(),
    forbesID,
    forbesName,
    slug,
    inactive BOOLEAN DEFAULT false
    PRIMARY KEY (id)
);

ALTER TABLE categories_fundamentals ADD COLUMN inactive BOOLEAN DEFAULT false;

CREATE PROCEDURE upsert_exchange_fundamentalslatest(name TEXT,slug TEXT,id TEXT,logo TEXT,exchange_active_market_pairs NUMERIC,nomics JSON,forbes JSON, last_updated timestamp)
LANGUAGE SQL
AS $BODY$
  INSERT INTO exchange_fundamentalslatest 
	VALUES (name, slug, id, logo, exchange_active_market_pairs, nomics, forbes, last_updated) 
  ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id, name = EXCLUDED.name, slug = EXCLUDED.slug, 
  logo = EXCLUDED.logo, exchange_active_market_pairs = EXCLUDED.exchange_active_market_pairs,
  nomics = EXCLUDED.nomics, forbes = EXCLUDED.forbes, last_updated = EXCLUDED.last_updated;
$BODY$;

CREATE INDEX idx_ds_source_mc ON fundamentalslatest (display_symbol,source,market_cap);
CREATE INDEX idx_symbol ON fundamentalslatest (symbol);
CREATE INDEX market_cap ON fundamentalslatest (market_cap);
CREATE INDEX idx_listed_exchange ON fundamentalslatest (listed_exchange);

CREATE INDEX idx_marketcap_pct_1y ON fundamentalslatest (market_cap_percent_change_1y);
CREATE INDEX idx_marketcap_pct_ytd ON fundamentalslatest (market_cap_percent_change_ytd);
CREATE INDEX idx_marketcap_pct_30d ON fundamentalslatest (market_cap_percent_change_30d);
CREATE INDEX idx_marketcap_pct_1y ON fundamentalslatest (market_cap_percent_change_1y);
CREATE INDEX idx_marketcap_pct_7d ON fundamentalslatest (market_cap_percent_change_7d);
CREATE INDEX idx_marketcap_pct_1d ON fundamentalslatest (market_cap_percent_change_1d);

CREATE INDEX idx_pct_1y ON fundamentalslatest (percentage_1y);
CREATE INDEX idx_pct_ytd ON fundamentalslatest (percentage_ytd);
CREATE INDEX idx_pct_30d ON fundamentalslatest (percentage_30d);
CREATE INDEX idx_pct_1y ON fundamentalslatest (percentage_1y);
CREATE INDEX idx_pct_7d ON fundamentalslatest (percentage_7d);
CREATE INDEX idx_pct_1d ON fundamentalslatest (percentage_1d);



-- Returns all chart data.
-- if the interval is not 24hrs it returns all data for 7d/30d/1y/max, all charts are appended with the last ticker from 24h
-- if the interval is 24h it returns all data it returns all data  24h/7d/30d/1y/max,
-- We will use asset Type to determine from where tha chart data will return (FT, NFT)
CREATE or REPLACE FUNCTION getFTNFTChartData(intval TEXT,symb TEXT, assetsTp TEXT)
RETURNS Table (
is_index bool, 
source TEXT, 
target_resolution_seconds int, 
prices jsonb,
symbol TEXT,
tm_interval TEXT,
status TEXT
) AS $$
#variable_conflict use_column
begin
--If we are not requesting a 24 hour chart return every chart 
--exluding 24hr. The charts will be used in FDA API
-- we will check for the type if it is NFT or FT
if assetsTp = 'NFT' 
then 
    if intval not like  '%24h%'
    then
    RETURN QUERY select
    is_index, 
    a.source as source , 
    a.target_resolution_seconds as target_resolution_seconds , 
    --append 24 hour cadle to chart that will be returned
    a.prices::jsonb || b.prices::jsonb as prices,
    a.symbol as symbol,
    a.interval as tm_interval,
    c.status as status
    from (
    --get chart for specified interval
    SELECT 
                is_index, 
                source, 
                target_resolution_seconds, 
                prices, 
                symbol,
                interval
            FROM 
                nomics_chart_data
            WHERE 
                target_resolution_seconds != 900
                    and "assetType" = assetsTp
            order by target_resolution_seconds asc
    ) a -- 
    join (
    --get last candle from 24 hr chart
    SELECT symbol, prices->-1 as prices
    FROM   nomics_chart_data 
    where target_resolution_seconds = 900 and symbol = symb and "assetType" = assetsTp) b
    on  b.symbol = a.symbol
     join (
			select id as symbol, CASE StatusResult
							when 0 Then 'active'
							Else 'comatoken'
							end as status
			from
			(
				select id, 
				EXTRACT(DAY FROM Now() - last_updated) AS StatusResult
				from nftdatalatest where Id = symb 
			) c
		) c
    	on a.symbol = c.symbol;
    --If we are not requesting a 24 hour chart return every chart 
    --exluding 24hr. The charts will be used in FDA API
    else
    RETURN QUERY select
    is_index, 
    a.source as source , 
    a.target_resolution_seconds as target_resolution_seconds , 
    --append 24 hour cadle to chart that will be returned
    a.prices::jsonb as prices,
    a.symbol as symbol,
    a.interval as tm_interval,
    c.status as status
    from (
    --get chart for specified interval
    SELECT 
                is_index, 
                source, 
                target_resolution_seconds, 
                prices, 
                symbol,
                interval
            FROM 
                nomics_chart_data
                where symbol = symb
                    and "assetType" = assetsTp
            order by target_resolution_seconds asc
    ) a 
     join (
			select id as symbol, CASE StatusResult
							when 0 Then 'active'
							Else 'comatoken'
							end as status
			from
			(
				select id, 
				EXTRACT(DAY FROM Now() - last_updated) AS StatusResult
				from nftdatalatest where Id = symb 
			) c
		) c
    on a.symbol = c.symbol;
    end if;
else
   if intval not like  '%24h%'
    then
    RETURN QUERY select
    is_index, 
    a.source as source , 
    a.target_resolution_seconds as target_resolution_seconds , 
    --append 24 hour cadle to chart that will be returned
    a.prices::jsonb || b.prices::jsonb as prices,
    a.symbol as symbol,
    a.interval as tm_interval,
    c.status as status
    from (
    --get chart for specified interval
    SELECT 
                is_index, 
                source, 
                target_resolution_seconds, 
                prices, 
                symbol,
                interval
            FROM 
                nomics_chart_data
            WHERE 
                target_resolution_seconds != 900
                    and "assetType" = assetsTp
            order by target_resolution_seconds asc
    ) a -- 
    join (
    --get last candle from 24 hr chart
    SELECT symbol, prices->-1 as prices
    FROM   nomics_chart_data 
    where target_resolution_seconds = 900 and symbol = symb and "assetType" = assetsTp) b
    on  b.symbol = a.symbol
    join (select symbol,status from fundamentalslatest where symbol = symb ) c
    on a.symbol = c.symbol;
    --If we are not requesting a 24 hour chart return every chart 
    --exluding 24hr. The charts will be used in FDA API
    else
    RETURN QUERY select
    is_index, 
    a.source as source , 
    a.target_resolution_seconds as target_resolution_seconds , 
    --append 24 hour cadle to chart that will be returned
    a.prices::jsonb as prices,
    a.symbol as symbol,
    a.interval as tm_interval,
    c.status as status
    from (
    --get chart for specified interval
    SELECT 
                is_index, 
                source, 
                target_resolution_seconds, 
                prices, 
                symbol,
                interval
            FROM 
                nomics_chart_data
                where symbol = symb
                    and "assetType" = assetsTp
            order by target_resolution_seconds asc
    ) a 
    join (select symbol,status from fundamentalslatest where symbol = symb ) c
    on a.symbol = c.symbol;
    end if;
end if;			
end;
$$
language PLPGSQL;



CREATE OR REPLACE FUNCTION public.getcategoriesfundamentals(
	)
    RETURNS TABLE(id text, name text, total_tokens integer, average_percentage_24h double precision, volume_24h double precision, price_24h double precision, average_price double precision, market_cap double precision, market_cap_percentage_24h double precision, top_gainers json, top_movers json, forbesname text, slug text, last_updated timestamp) 
    LANGUAGE 'sql'

AS $BODY$
	SELECT 
		id,
		name,
		total_tokens,
		average_percentage_24h,
		volume_24h,
		price_24h,
		average_price,
		market_cap,
		market_cap_percentage_24h,
		top_gainers,
		top_movers,
		"forbesName" as forbesName,
		slug,
		last_updated
	FROM public.categories_fundamentals
  where COALESCE(inactive, false) = false;
$BODY$;

-- DEPRECATED in favour of GetNFTCollectionWithRank(text)
CREATE OR REPLACE FUNCTION public.GetNFTCollection (collectionSlug text)
RETURNS TABLE (
    id TEXT,
    contract_address TEXT,
    asset_platform_id TEXT,
    name TEXT,
    symbol TEXT,
    display_symbol TEXT,
    image TEXT,
    large_image TEXT,
    description TEXT,
    native_currency TEXT,
    floor_price_usd DOUBLE PRECISION,
    market_cap_usd DOUBLE PRECISION,
    volume_24h_usd DOUBLE PRECISION,
    floor_price_native DOUBLE PRECISION,
    market_cap_native DOUBLE PRECISION,
    volume_24h_native DOUBLE PRECISION,
    floor_price_in_usd_24h_percentage_change DOUBLE PRECISION,
    volume_24h_percentage_change_usd DOUBLE PRECISION,
    number_of_unique_addresses INT,
    number_of_unique_addresses_24h_percentage_change DOUBLE PRECISION,
    slug TEXT,
    total_supply DOUBLE PRECISION,
    website_url Text,
    twitter_url Text,
    discord_url Text,
    explorers JSON,
    last_updated TIMESTAMPTZ,
    avg_sale_price_1d DOUBLE PRECISION,
    avg_sale_price_7d DOUBLE PRECISION,
    avg_sale_price_30d DOUBLE PRECISION,
    avg_sale_price_90d DOUBLE PRECISION,
    avg_sale_price_ytd double precision,
    avg_total_sales_pct_change_1d DOUBLE PRECISION,
    avg_total_sales_pct_change_7d DOUBLE PRECISION,
    avg_total_sales_pct_change_30d DOUBLE PRECISION,
    avg_total_sales_pct_change_90d DOUBLE PRECISION,
    avg_total_sales_pct_change_ytd double precision,
    total_sales_1d DOUBLE PRECISION,
    total_sales_7d DOUBLE PRECISION,
    total_sales_30d DOUBLE PRECISION,
    total_sales_90d DOUBLE PRECISION,
    total_sales_ytd double precision,
    avg_sales_price_change_1d DOUBLE PRECISION,
    avg_sales_price_change_7d DOUBLE PRECISION,
    avg_sales_price_change_30d DOUBLE PRECISION,
    avg_sales_price_change_90d DOUBLE PRECISION,
    avg_sales_price_change_ytd double precision,
    native_currency_symbol TEXT,
    market_cap_24h_percentage_change_usd FLOAT,
    market_cap_24h_percentage_change_native FLOAT,
    volume_24h_percentage_change_native FLOAT,
    volume_usd_1d double precision,
    volume_usd_7d double precision,
    volume_usd_30d double precision,
    volume_usd_90d double precision, 
    volume_usd_ytd double precision,
    volume_native_1d double precision,
    volume_native_7d double precision,
    volume_native_30d double precision,
    volume_native_90d double precision,
    volume_native_ytd double precision,
    pct_change_volume_usd_1d double precision,
    pct_change_volume_usd_7d double precision,
    pct_change_volume_usd_30d double precision,
    pct_change_volume_usd_90d double precision,
    pct_change_volume_usd_ytd double precision,
    pct_change_volume_native_1d double precision,
    pct_change_volume_native_7d double precision,
    pct_change_volume_native_30d double precision,
    pct_change_volume_native_90d double precision,
    pct_change_volume_native_ytd double precision,
    lowest_floor_price_24h_usd double precision,
    highest_floor_price_24h_usd DOUBLE PRECISION,
    lowest_floor_price_24h_native DOUBLE PRECISION,
    highest_floor_price_24h_native DOUBLE PRECISION,
    floor_price_24h_percentage_change_usd DOUBLE PRECISION,
    floor_price_24h_percentage_change_native double precision,
    lowest_floor_price_24h_percentage_change_usd double precision,
    lowest_floor_price_24h_percentage_change_native double precision,
    highest_floor_price_24h_percentage_change_usd double precision,
    highest_floor_price_24h_percentage_change_native double precision,
    lowest_floor_price_7d_usd double precision,
    highest_floor_price_7d_usd DOUBLE PRECISION,
    lowest_floor_price_7d_native DOUBLE PRECISION,
    highest_floor_price_7d_native DOUBLE PRECISION,
    floor_price_7d_percentage_change_usd DOUBLE PRECISION,
    floor_price_7d_percentage_change_native double precision,
    lowest_floor_price_7d_percentage_change_usd double precision,
    lowest_floor_price_7d_percentage_change_native double precision,
    highest_floor_price_7d_percentage_change_usd double precision,
    highest_floor_price_7d_percentage_change_native double precision,
    lowest_floor_price_30d_usd double precision,
    highest_floor_price_30d_usd DOUBLE PRECISION,
    lowest_floor_price_30d_native DOUBLE PRECISION,
    highest_floor_price_30d_native DOUBLE PRECISION,
    floor_price_30d_percentage_change_usd DOUBLE PRECISION,
    floor_price_30d_percentage_change_native double precision,
    lowest_floor_price_30d_percentage_change_usd double precision,
    lowest_floor_price_30d_percentage_change_native double precision,
    highest_floor_price_30d_percentage_change_usd double precision,
    highest_floor_price_30d_percentage_change_native double precision,
    lowest_floor_price_90d_usd double precision,
    highest_floor_price_90d_usd DOUBLE PRECISION,
    lowest_floor_price_90d_native DOUBLE PRECISION,
    highest_floor_price_90d_native DOUBLE PRECISION,
    floor_price_90d_percentage_change_usd DOUBLE PRECISION,
    floor_price_90d_percentage_change_native double precision,
    lowest_floor_price_90d_percentage_change_usd double precision, 
    lowest_floor_price_90d_percentage_change_native double precision,
    highest_floor_price_90d_percentage_change_usd double precision,
    highest_floor_price_90d_percentage_change_native double precision,
    lowest_floor_price_ytd_usd double precision,
    highest_floor_price_ytd_usd DOUBLE PRECISION,
    lowest_floor_price_ytd_native DOUBLE PRECISION,
    highest_floor_price_ytd_native DOUBLE PRECISION,
    floor_price_ytd_percentage_change_usd DOUBLE PRECISION,
    floor_price_ytd_percentage_change_native double precision,
    lowest_floor_price_ytd_percentage_change_usd double precision,
    lowest_floor_price_ytd_percentage_change_native double precision,
    highest_floor_price_ytd_percentage_change_usd double precision,
    highest_floor_price_ytd_percentage_change_native double precision
) AS $$
BEGIN
    RETURN QUERY EXECUTE format('
        SELECT 
          id,
          contract_address,
          asset_platform_id,
          name,
          symbol,
          symbol as display_symbol,
          image,
          large_image,
          description,
          native_currency,
          floor_price_usd,
          market_cap_usd,
          volume_24h_usd,
          floor_price_native,
          market_cap_native,
          volume_24h_native,
          floor_price_in_usd_24h_percentage_change,
          volume_24h_percentage_change_usd,
          number_of_unique_addresses,
          number_of_unique_addresses_24h_percentage_change,
          slug,
          total_supply,
          website_url,
          twitter_url,
          discord_url,
          explorers,
          last_updated,
          avg_sale_price_1d,
          avg_sale_price_7d,
          avg_sale_price_30d,
          avg_sale_price_90d,
          avg_sale_price_ytd,
          avg_total_sales_pct_change_1d,
          avg_total_sales_pct_change_7d,
          avg_total_sales_pct_change_30d,
          avg_total_sales_pct_change_90d,
          avg_total_sales_pct_change_ytd,
          total_sales_1d,
          total_sales_7d,
          total_sales_30d,
          total_sales_90d,
          total_sales_ytd,
          avg_sales_price_change_1d,
          avg_sales_price_change_7d,
          avg_sales_price_change_30d,
          avg_sales_price_change_90d,
          avg_sales_price_change_ytd,
          native_currency_symbol,
          market_cap_24h_percentage_change_usd,
          market_cap_24h_percentage_change_native,
          volume_24h_percentage_change_native,
          volume_usd_1d,
          volume_usd_7d,
          volume_usd_30d,
          volume_usd_90d,
          volume_usd_ytd,
          volume_native_1d,
          volume_native_7d,
          volume_native_30d,
          volume_native_90d,
          volume_native_ytd,
          pct_change_volume_usd_1d,
          pct_change_volume_usd_7d,
          pct_change_volume_usd_30d,
          pct_change_volume_usd_90d,
          pct_change_volume_usd_ytd,
          pct_change_volume_native_1d,
          pct_change_volume_native_7d,
          pct_change_volume_native_30d,
          pct_change_volume_native_90d,
          pct_change_volume_native_ytd,
          lowest_floor_price_24h_usd,
          highest_floor_price_24h_usd,
          lowest_floor_price_24h_native,
          highest_floor_price_24h_native,
          floor_price_24h_percentage_change_usd,
          floor_price_24h_percentage_change_native,
          lowest_floor_price_24h_percentage_change_usd,
          lowest_floor_price_24h_percentage_change_native,
          highest_floor_price_24h_percentage_change_usd,
          highest_floor_price_24h_percentage_change_native,
          lowest_floor_price_7d_usd,
          highest_floor_price_7d_usd,
          lowest_floor_price_7d_native,
          highest_floor_price_7d_native,
          floor_price_7d_percentage_change_usd,
          floor_price_7d_percentage_change_native,
          lowest_floor_price_7d_percentage_change_usd,
          lowest_floor_price_7d_percentage_change_native,
          highest_floor_price_7d_percentage_change_usd,
          highest_floor_price_7d_percentage_change_native,
          lowest_floor_price_30d_usd,
          highest_floor_price_30d_usd,
          lowest_floor_price_30d_native,
          highest_floor_price_30d_native,
          floor_price_30d_percentage_change_usd,
          floor_price_30d_percentage_change_native,
          lowest_floor_price_30d_percentage_change_usd,
          lowest_floor_price_30d_percentage_change_native,
          highest_floor_price_30d_percentage_change_usd,
          highest_floor_price_30d_percentage_change_native,
          lowest_floor_price_90d_usd,
          highest_floor_price_90d_usd,
          lowest_floor_price_90d_native,
          highest_floor_price_90d_native,
          floor_price_90d_percentage_change_usd,
          floor_price_90d_percentage_change_native,
          lowest_floor_price_90d_percentage_change_usd,
          lowest_floor_price_90d_percentage_change_native,
          highest_floor_price_90d_percentage_change_usd,
          highest_floor_price_90d_percentage_change_native,
          lowest_floor_price_ytd_usd,
          highest_floor_price_ytd_usd,
          lowest_floor_price_ytd_native,
          highest_floor_price_ytd_native,
          floor_price_ytd_percentage_change_usd,
          floor_price_ytd_percentage_change_native,
          lowest_floor_price_ytd_percentage_change_usd,
          lowest_floor_price_ytd_percentage_change_native,
          highest_floor_price_ytd_percentage_change_usd,
          highest_floor_price_ytd_percentage_change_native
        FROM 
          public.nftdatalatest
        WHERE slug = %L',
        collectionSlug
    );
END
$$ LANGUAGE plpgsql;


-- Get An NFT Collection from its slug.
CREATE OR REPLACE FUNCTION public.GetNFTCollectionWithRank (collectionSlug text)
RETURNS TABLE (
    id TEXT,
    contract_address TEXT,
    asset_platform_id TEXT,
    name TEXT,
    symbol TEXT,
    rank INT,
    prev_ranked_slug TEXT,
    next_ranked_slug TEXT,
    display_symbol TEXT,
    image TEXT,
    large_image TEXT,
    description TEXT,
    native_currency TEXT,
    floor_price_usd DOUBLE PRECISION,
    market_cap_usd DOUBLE PRECISION,
    volume_24h_usd DOUBLE PRECISION,
    floor_price_native DOUBLE PRECISION,
    market_cap_native DOUBLE PRECISION,
    volume_24h_native DOUBLE PRECISION,
    floor_price_in_usd_24h_percentage_change DOUBLE PRECISION,
    volume_24h_percentage_change_usd DOUBLE PRECISION,
    number_of_unique_addresses INT,
    number_of_unique_addresses_24h_percentage_change DOUBLE PRECISION,
    slug TEXT,
    total_supply DOUBLE PRECISION,
    website_url Text,
    twitter_url Text,
    discord_url Text,
    explorers JSON,
    last_updated TIMESTAMPTZ,
    avg_sale_price_1d DOUBLE PRECISION,
    avg_sale_price_7d DOUBLE PRECISION,
    avg_sale_price_30d DOUBLE PRECISION,
    avg_sale_price_90d DOUBLE PRECISION,
    avg_sale_price_ytd double precision,
    avg_total_sales_pct_change_1d DOUBLE PRECISION,
    avg_total_sales_pct_change_7d DOUBLE PRECISION,
    avg_total_sales_pct_change_30d DOUBLE PRECISION,
    avg_total_sales_pct_change_90d DOUBLE PRECISION,
    avg_total_sales_pct_change_ytd double precision,
    total_sales_1d DOUBLE PRECISION,
    total_sales_7d DOUBLE PRECISION,
    total_sales_30d DOUBLE PRECISION,
    total_sales_90d DOUBLE PRECISION,
    total_sales_ytd double precision,
    avg_sales_price_change_1d DOUBLE PRECISION,
    avg_sales_price_change_7d DOUBLE PRECISION,
    avg_sales_price_change_30d DOUBLE PRECISION,
    avg_sales_price_change_90d DOUBLE PRECISION,
    avg_sales_price_change_ytd double precision,
    native_currency_symbol TEXT,
    market_cap_24h_percentage_change_usd FLOAT,
    market_cap_24h_percentage_change_native FLOAT,
    volume_24h_percentage_change_native FLOAT,
    volume_usd_1d double precision,
    volume_usd_7d double precision,
    volume_usd_30d double precision,
    volume_usd_90d double precision, 
    volume_usd_ytd double precision,
    volume_native_1d double precision,
    volume_native_7d double precision,
    volume_native_30d double precision,
    volume_native_90d double precision,
    volume_native_ytd double precision,
    pct_change_volume_usd_1d double precision,
    pct_change_volume_usd_7d double precision,
    pct_change_volume_usd_30d double precision,
    pct_change_volume_usd_90d double precision,
    pct_change_volume_usd_ytd double precision,
    pct_change_volume_native_1d double precision,
    pct_change_volume_native_7d double precision,
    pct_change_volume_native_30d double precision,
    pct_change_volume_native_90d double precision,
    pct_change_volume_native_ytd double precision,
    lowest_floor_price_24h_usd double precision,
    highest_floor_price_24h_usd DOUBLE PRECISION,
    lowest_floor_price_24h_native DOUBLE PRECISION,
    highest_floor_price_24h_native DOUBLE PRECISION,
    floor_price_24h_percentage_change_usd DOUBLE PRECISION,
    floor_price_24h_percentage_change_native double precision,
    lowest_floor_price_24h_percentage_change_usd double precision,
    lowest_floor_price_24h_percentage_change_native double precision,
    highest_floor_price_24h_percentage_change_usd double precision,
    highest_floor_price_24h_percentage_change_native double precision,
    lowest_floor_price_7d_usd double precision,
    highest_floor_price_7d_usd DOUBLE PRECISION,
    lowest_floor_price_7d_native DOUBLE PRECISION,
    highest_floor_price_7d_native DOUBLE PRECISION,
    floor_price_7d_percentage_change_usd DOUBLE PRECISION,
    floor_price_7d_percentage_change_native double precision,
    lowest_floor_price_7d_percentage_change_usd double precision,
    lowest_floor_price_7d_percentage_change_native double precision,
    highest_floor_price_7d_percentage_change_usd double precision,
    highest_floor_price_7d_percentage_change_native double precision,
    lowest_floor_price_30d_usd double precision,
    highest_floor_price_30d_usd DOUBLE PRECISION,
    lowest_floor_price_30d_native DOUBLE PRECISION,
    highest_floor_price_30d_native DOUBLE PRECISION,
    floor_price_30d_percentage_change_usd DOUBLE PRECISION,
    floor_price_30d_percentage_change_native double precision,
    lowest_floor_price_30d_percentage_change_usd double precision,
    lowest_floor_price_30d_percentage_change_native double precision,
    highest_floor_price_30d_percentage_change_usd double precision,
    highest_floor_price_30d_percentage_change_native double precision,
    lowest_floor_price_90d_usd double precision,
    highest_floor_price_90d_usd DOUBLE PRECISION,
    lowest_floor_price_90d_native DOUBLE PRECISION,
    highest_floor_price_90d_native DOUBLE PRECISION,
    floor_price_90d_percentage_change_usd DOUBLE PRECISION,
    floor_price_90d_percentage_change_native double precision,
    lowest_floor_price_90d_percentage_change_usd double precision, 
    lowest_floor_price_90d_percentage_change_native double precision,
    highest_floor_price_90d_percentage_change_usd double precision,
    highest_floor_price_90d_percentage_change_native double precision,
    lowest_floor_price_ytd_usd double precision,
    highest_floor_price_ytd_usd DOUBLE PRECISION,
    lowest_floor_price_ytd_native DOUBLE PRECISION,
    highest_floor_price_ytd_native DOUBLE PRECISION,
    floor_price_ytd_percentage_change_usd DOUBLE PRECISION,
    floor_price_ytd_percentage_change_native double precision,
    lowest_floor_price_ytd_percentage_change_usd double precision,
    lowest_floor_price_ytd_percentage_change_native double precision,
    highest_floor_price_ytd_percentage_change_usd double precision,
    highest_floor_price_ytd_percentage_change_native double precision,
    next_up JSONB
) AS $$
BEGIN
    RETURN QUERY EXECUTE format('
        SELECT 
          id,
          contract_address,
          asset_platform_id,
          name,
          symbol,
          rank,
          prev_ranked_slug,
          next_asset_1->>''slug'' as next_ranked_slug,
          symbol as display_symbol,
          image,
          large_image,
          description,
          native_currency,
          floor_price_usd,
          market_cap_usd,
          volume_24h_usd,
          floor_price_native,
          market_cap_native,
          volume_24h_native,
          floor_price_in_usd_24h_percentage_change,
          volume_24h_percentage_change_usd,
          number_of_unique_addresses,
          number_of_unique_addresses_24h_percentage_change,
          slug,
          total_supply,
          website_url,
          twitter_url,
          discord_url,
          COALESCE(explorers, ''[]''::JSON ),
          last_updated,
          avg_sale_price_1d,
          avg_sale_price_7d,
          avg_sale_price_30d,
          avg_sale_price_90d,
          avg_sale_price_ytd,
          avg_total_sales_pct_change_1d,
          avg_total_sales_pct_change_7d,
          avg_total_sales_pct_change_30d,
          avg_total_sales_pct_change_90d,
          avg_total_sales_pct_change_ytd,
          total_sales_1d,
          total_sales_7d,
          total_sales_30d,
          total_sales_90d,
          total_sales_ytd,
          avg_sales_price_change_1d,
          avg_sales_price_change_7d,
          avg_sales_price_change_30d,
          avg_sales_price_change_90d,
          avg_sales_price_change_ytd,
          native_currency_symbol,
          market_cap_24h_percentage_change_usd,
          market_cap_24h_percentage_change_native,
          volume_24h_percentage_change_native,
          volume_usd_1d,
          volume_usd_7d,
          volume_usd_30d,
          volume_usd_90d,
          volume_usd_ytd,
          volume_native_1d,
          volume_native_7d,
          volume_native_30d,
          volume_native_90d,
          volume_native_ytd,
          pct_change_volume_usd_1d,
          pct_change_volume_usd_7d,
          pct_change_volume_usd_30d,
          pct_change_volume_usd_90d,
          pct_change_volume_usd_ytd,
          pct_change_volume_native_1d,
          pct_change_volume_native_7d,
          pct_change_volume_native_30d,
          pct_change_volume_native_90d,
          pct_change_volume_native_ytd,
          lowest_floor_price_24h_usd,
          highest_floor_price_24h_usd,
          lowest_floor_price_24h_native,
          highest_floor_price_24h_native,
          floor_price_24h_percentage_change_usd,
          floor_price_24h_percentage_change_native,
          lowest_floor_price_24h_percentage_change_usd,
          lowest_floor_price_24h_percentage_change_native,
          highest_floor_price_24h_percentage_change_usd,
          highest_floor_price_24h_percentage_change_native,
          lowest_floor_price_7d_usd,
          highest_floor_price_7d_usd,
          lowest_floor_price_7d_native,
          highest_floor_price_7d_native,
          floor_price_7d_percentage_change_usd,
          floor_price_7d_percentage_change_native,
          lowest_floor_price_7d_percentage_change_usd,
          lowest_floor_price_7d_percentage_change_native,
          highest_floor_price_7d_percentage_change_usd,
          highest_floor_price_7d_percentage_change_native,
          lowest_floor_price_30d_usd,
          highest_floor_price_30d_usd,
          lowest_floor_price_30d_native,
          highest_floor_price_30d_native,
          floor_price_30d_percentage_change_usd,
          floor_price_30d_percentage_change_native,
          lowest_floor_price_30d_percentage_change_usd,
          lowest_floor_price_30d_percentage_change_native,
          highest_floor_price_30d_percentage_change_usd,
          highest_floor_price_30d_percentage_change_native,
          lowest_floor_price_90d_usd,
          highest_floor_price_90d_usd,
          lowest_floor_price_90d_native,
          highest_floor_price_90d_native,
          floor_price_90d_percentage_change_usd,
          floor_price_90d_percentage_change_native,
          lowest_floor_price_90d_percentage_change_usd,
          lowest_floor_price_90d_percentage_change_native,
          highest_floor_price_90d_percentage_change_usd,
          highest_floor_price_90d_percentage_change_native,
          lowest_floor_price_ytd_usd,
          highest_floor_price_ytd_usd,
          lowest_floor_price_ytd_native,
          highest_floor_price_ytd_native,
          floor_price_ytd_percentage_change_usd,
          floor_price_ytd_percentage_change_native,
          lowest_floor_price_ytd_percentage_change_usd,
          lowest_floor_price_ytd_percentage_change_native,
          highest_floor_price_ytd_percentage_change_usd,
          highest_floor_price_ytd_percentage_change_native,
          jsonb_build_array(  
          	jsonb_set( next_asset_1 , ''{rank}'', to_jsonb(rank+1) , true), 
          	jsonb_set( next_asset_2 , ''{rank}'', to_jsonb(rank+2) , true),
          	jsonb_set( next_asset_3 , ''{rank}'', to_jsonb(rank+3) , true),
          	jsonb_set( next_asset_4 , ''{rank}'', to_jsonb(rank+4) , true)
          ) as next_up
        FROM 
          (	
            SELECT 
              *,
              CAST ( ROW_NUMBER () over (order by volume_24h_usd desc, slug desc) AS int) AS rank,
              LAG (slug, 1, '''') over (order by volume_24h_usd desc, slug desc) AS prev_ranked_slug,
              LEAD ( jsonb_build_object(''slug'' , slug, ''image'', jsonb_build_object(''small'' , image, ''large'', large_image), ''name'', name ) , 1, ''{}''::JSONB) 
              	OVER (order by volume_24h_usd desc, slug desc) AS next_asset_1,
              LEAD ( jsonb_build_object(''slug'' , slug, ''image'', jsonb_build_object(''small'' , image, ''large'', large_image), ''name'', name ) , 2, ''{}''::JSONB) 
              	OVER (order by volume_24h_usd desc, slug desc) AS next_asset_2,
              LEAD ( jsonb_build_object(''slug'' , slug, ''image'', jsonb_build_object(''small'' , image, ''large'', large_image), ''name'', name ) , 3, ''{}''::JSONB) 
              	OVER (order by volume_24h_usd desc, slug desc) AS next_asset_3,
              LEAD ( jsonb_build_object(''slug'' , slug, ''image'', jsonb_build_object(''small'' , image, ''large'', large_image), ''name'', name ) , 4, ''{}''::JSONB) 
              	OVER (order by volume_24h_usd desc, slug desc) AS next_asset_4
            FROM
              public.nftdatalatest
            WHERE
              volume_24h_percentage_change_usd is not null
          ) as T
        WHERE slug = %L',
        collectionSlug
    );
END
$$ LANGUAGE plpgsql;



CREATE
or REPLACE FUNCTION getCategoriesChartData(intval TEXT, symb TEXT, assetsTp TEXT) RETURNS Table (
    is_index bool,
    source TEXT,
    target_resolution_seconds int,
    prices jsonb,
    symbol TEXT,
    tm_interval TEXT,
    status TEXT
) AS $$ #variable_conflict use_column
begin 
--If we are not requesting a 24 hour chart return every chart 
--exluding 24hr. The charts will be used in FDA API
-- we will check for the type CATEGORY
if intval not like '%24h%' then RETURN QUERY
select
    is_index,
    a.source as source,
    a.target_resolution_seconds as target_resolution_seconds,
    --append 24 hour cadle to chart that will be returned
    b.prices :: jsonb || a.prices :: jsonb as prices,
    a.symbol as symbol,
    a.interval as tm_interval,
    c.status as status
from
    (
        --get chart for specified interval
        SELECT
            is_index,
            source,
            target_resolution_seconds,
            prices,
            symbol,
            interval
        FROM
            nomics_chart_data
        WHERE
            target_resolution_seconds != 900
            and "assetType" = assetsTp
        order by
            target_resolution_seconds asc
    ) a -- 
    join (
        --get last candle from 24 hr chart
        SELECT
            symbol,
            prices -> -1 as prices
        FROM
            nomics_chart_data
        where
            target_resolution_seconds = 900
            and symbol = symb
            and "assetType" = assetsTp
    ) b on b.symbol = a.symbol
    join (
        select
            id as symbol,
            CASE
                StatusResult
                when 0 Then 'active'
                Else 'comatoken'
            end as status
        from
            (
                select
                    id,
                    EXTRACT(
                        DAY
                        FROM
                            Now() - last_updated
                    ) AS StatusResult
                from
                    categories_fundamentals
                where
                    Id = symb
            ) c
    ) c on a.symbol = c.symbol;

--If we are not requesting a 24 hour chart return every chart 
--exluding 24hr. The charts will be used in FDA API
else RETURN QUERY
select
    is_index,
    a.source as source,
    a.target_resolution_seconds as target_resolution_seconds,
    --append 24 hour cadle to chart that will be returned
    a.prices :: jsonb as prices,
    a.symbol as symbol,
    a.interval as tm_interval,
    c.status as status
from
    (
        --get chart for specified interval
        SELECT
            is_index,
            source,
            target_resolution_seconds,
            prices,
            symbol,
            interval
        FROM
            nomics_chart_data
        where
            symbol = symb
            and "assetType" = assetsTp
        order by
            target_resolution_seconds asc
    ) a
    join (
        select
            id as symbol,
            CASE
                StatusResult
                when 0 Then 'active'
                Else 'comatoken'
            end as status
        from
            (
                select
                    id,
                    EXTRACT(
                        DAY
                        FROM
                            Now() - last_updated
                    ) AS StatusResult
                from
                    categories_fundamentals
                where
                    Id = symb
            ) c
    ) c on a.symbol = c.symbol;

end if;

end;

$$ language PLPGSQL;

CREATE INDEX idx_target_resolution_seconds ON nomics_chart_data (target_resolution_seconds);



CREATE
OR REPLACE FUNCTION public.GetNFTCollectionTest (collectionSlug text) RETURNS TABLE (
    id TEXT,
    contract_address TEXT,
    asset_platform_id TEXT,
    name TEXT,
    symbol TEXT,
    rank INT,
    prev_ranked_slug TEXT,
    next_ranked_slug TEXT,
    display_symbol TEXT,
    image TEXT,
    large_image TEXT,
    description TEXT,
    native_currency TEXT,
    floor_price_usd DOUBLE PRECISION,
    market_cap_usd DOUBLE PRECISION,
    volume_24h_usd DOUBLE PRECISION,
    floor_price_native DOUBLE PRECISION,
    market_cap_native DOUBLE PRECISION,
    volume_24h_native DOUBLE PRECISION,
    floor_price_in_usd_24h_percentage_change DOUBLE PRECISION,
    volume_24h_percentage_change_usd DOUBLE PRECISION,
    number_of_unique_addresses INT,
    number_of_unique_addresses_24h_percentage_change DOUBLE PRECISION,
    slug TEXT,
    total_supply DOUBLE PRECISION,
    website_url Text,
    twitter_url Text,
    discord_url Text,
    explorers JSON,
    last_updated TIMESTAMPTZ,
    avg_sale_price_1d DOUBLE PRECISION,
    avg_sale_price_7d DOUBLE PRECISION,
    avg_sale_price_30d DOUBLE PRECISION,
    avg_sale_price_90d DOUBLE PRECISION,
    avg_sale_price_ytd double precision,
    avg_total_sales_pct_change_1d DOUBLE PRECISION,
    avg_total_sales_pct_change_7d DOUBLE PRECISION,
    avg_total_sales_pct_change_30d DOUBLE PRECISION,
    avg_total_sales_pct_change_90d DOUBLE PRECISION,
    avg_total_sales_pct_change_ytd double precision,
    total_sales_1d DOUBLE PRECISION,
    total_sales_7d DOUBLE PRECISION,
    total_sales_30d DOUBLE PRECISION,
    total_sales_90d DOUBLE PRECISION,
    total_sales_ytd double precision,
    avg_sales_price_change_1d DOUBLE PRECISION,
    avg_sales_price_change_7d DOUBLE PRECISION,
    avg_sales_price_change_30d DOUBLE PRECISION,
    avg_sales_price_change_90d DOUBLE PRECISION,
    avg_sales_price_change_ytd double precision,
    native_currency_symbol TEXT,
    market_cap_24h_percentage_change_usd FLOAT,
    market_cap_24h_percentage_change_native FLOAT,
    volume_24h_percentage_change_native FLOAT,
    volume_usd_1d double precision,
    volume_usd_7d double precision,
    volume_usd_30d double precision,
    volume_usd_90d double precision, 
    volume_usd_ytd double precision,
    volume_native_1d double precision,
    volume_native_7d double precision,
    volume_native_30d double precision,
    volume_native_90d double precision,
    volume_native_ytd double precision,
    pct_change_volume_usd_1d double precision,
    pct_change_volume_usd_7d double precision,
    pct_change_volume_usd_30d double precision,
    pct_change_volume_usd_90d double precision,
    pct_change_volume_usd_ytd double precision,
    pct_change_volume_native_1d double precision,
    pct_change_volume_native_7d double precision,
    pct_change_volume_native_30d double precision,
    pct_change_volume_native_90d double precision,
    pct_change_volume_native_ytd double precision,
    lowest_floor_price_24h_usd double precision,
    highest_floor_price_24h_usd DOUBLE PRECISION,
    lowest_floor_price_24h_native DOUBLE PRECISION,
    highest_floor_price_24h_native DOUBLE PRECISION,
    floor_price_24h_percentage_change_usd DOUBLE PRECISION,
    floor_price_24h_percentage_change_native double precision,
    lowest_floor_price_24h_percentage_change_usd double precision,
    lowest_floor_price_24h_percentage_change_native double precision,
    highest_floor_price_24h_percentage_change_usd double precision,
    highest_floor_price_24h_percentage_change_native double precision,
    lowest_floor_price_7d_usd double precision,
    highest_floor_price_7d_usd DOUBLE PRECISION,
    lowest_floor_price_7d_native DOUBLE PRECISION,
    highest_floor_price_7d_native DOUBLE PRECISION,
    floor_price_7d_percentage_change_usd DOUBLE PRECISION,
    floor_price_7d_percentage_change_native double precision,
    lowest_floor_price_7d_percentage_change_usd double precision,
    lowest_floor_price_7d_percentage_change_native double precision,
    highest_floor_price_7d_percentage_change_usd double precision,
    highest_floor_price_7d_percentage_change_native double precision,
    lowest_floor_price_30d_usd double precision,
    highest_floor_price_30d_usd DOUBLE PRECISION,
    lowest_floor_price_30d_native DOUBLE PRECISION,
    highest_floor_price_30d_native DOUBLE PRECISION,
    floor_price_30d_percentage_change_usd DOUBLE PRECISION,
    floor_price_30d_percentage_change_native double precision,
    lowest_floor_price_30d_percentage_change_usd double precision,
    lowest_floor_price_30d_percentage_change_native double precision,
    highest_floor_price_30d_percentage_change_usd double precision,
    highest_floor_price_30d_percentage_change_native double precision,
    lowest_floor_price_90d_usd double precision,
    highest_floor_price_90d_usd DOUBLE PRECISION,
    lowest_floor_price_90d_native DOUBLE PRECISION,
    highest_floor_price_90d_native DOUBLE PRECISION,
    floor_price_90d_percentage_change_usd DOUBLE PRECISION,
    floor_price_90d_percentage_change_native double precision,
    lowest_floor_price_90d_percentage_change_usd double precision, 
    lowest_floor_price_90d_percentage_change_native double precision,
    highest_floor_price_90d_percentage_change_usd double precision,
    highest_floor_price_90d_percentage_change_native double precision,
    lowest_floor_price_ytd_usd double precision,
    highest_floor_price_ytd_usd DOUBLE PRECISION,
    lowest_floor_price_ytd_native DOUBLE PRECISION,
    highest_floor_price_ytd_native DOUBLE PRECISION,
    floor_price_ytd_percentage_change_usd DOUBLE PRECISION,
    floor_price_ytd_percentage_change_native double precision,
    lowest_floor_price_ytd_percentage_change_usd double precision,
    lowest_floor_price_ytd_percentage_change_native double precision,
    highest_floor_price_ytd_percentage_change_usd double precision,
    highest_floor_price_ytd_percentage_change_native double precision,
    next_up JSONB,
    questions JSON
) AS $$
BEGIN
    RETURN QUERY EXECUTE format('
        SELECT 
          id,
          contract_address,
          asset_platform_id,
          name,
          symbol,
          rank,
          prev_ranked_slug,
          next_asset_1->>''slug'' as next_ranked_slug,
          symbol as display_symbol,
          image,
          large_image,
          description,
          native_currency,
          floor_price_usd,
          market_cap_usd,
          volume_24h_usd,
          floor_price_native,
          market_cap_native,
          volume_24h_native,
          floor_price_in_usd_24h_percentage_change,
          volume_24h_percentage_change_usd,
          number_of_unique_addresses,
          number_of_unique_addresses_24h_percentage_change,
          slug,
          total_supply,
          website_url,
          twitter_url,
          discord_url,
          COALESCE(explorers, ''[]''::JSON ),
          last_updated,
          avg_sale_price_1d,
          avg_sale_price_7d,
          avg_sale_price_30d,
          avg_sale_price_90d,
          avg_sale_price_ytd,
          avg_total_sales_pct_change_1d,
          avg_total_sales_pct_change_7d,
          avg_total_sales_pct_change_30d,
          avg_total_sales_pct_change_90d,
          avg_total_sales_pct_change_ytd,
          total_sales_1d,
          total_sales_7d,
          total_sales_30d,
          total_sales_90d,
          total_sales_ytd,
          avg_sales_price_change_1d,
          avg_sales_price_change_7d,
          avg_sales_price_change_30d,
          avg_sales_price_change_90d,
          avg_sales_price_change_ytd,
          native_currency_symbol,
          market_cap_24h_percentage_change_usd,
          market_cap_24h_percentage_change_native,
          volume_24h_percentage_change_native,
          volume_usd_1d,
          volume_usd_7d,
          volume_usd_30d,
          volume_usd_90d,
          volume_usd_ytd,
          volume_native_1d,
          volume_native_7d,
          volume_native_30d,
          volume_native_90d,
          volume_native_ytd,
          pct_change_volume_usd_1d,
          pct_change_volume_usd_7d,
          pct_change_volume_usd_30d,
          pct_change_volume_usd_90d,
          pct_change_volume_usd_ytd,
          pct_change_volume_native_1d,
          pct_change_volume_native_7d,
          pct_change_volume_native_30d,
          pct_change_volume_native_90d,
          pct_change_volume_native_ytd,
          lowest_floor_price_24h_usd,
          highest_floor_price_24h_usd,
          lowest_floor_price_24h_native,
          highest_floor_price_24h_native,
          floor_price_24h_percentage_change_usd,
          floor_price_24h_percentage_change_native,
          lowest_floor_price_24h_percentage_change_usd,
          lowest_floor_price_24h_percentage_change_native,
          highest_floor_price_24h_percentage_change_usd,
          highest_floor_price_24h_percentage_change_native,
          lowest_floor_price_7d_usd,
          highest_floor_price_7d_usd,
          lowest_floor_price_7d_native,
          highest_floor_price_7d_native,
          floor_price_7d_percentage_change_usd,
          floor_price_7d_percentage_change_native,
          lowest_floor_price_7d_percentage_change_usd,
          lowest_floor_price_7d_percentage_change_native,
          highest_floor_price_7d_percentage_change_usd,
          highest_floor_price_7d_percentage_change_native,
          lowest_floor_price_30d_usd,
          highest_floor_price_30d_usd,
          lowest_floor_price_30d_native,
          highest_floor_price_30d_native,
          floor_price_30d_percentage_change_usd,
          floor_price_30d_percentage_change_native,
          lowest_floor_price_30d_percentage_change_usd,
          lowest_floor_price_30d_percentage_change_native,
          highest_floor_price_30d_percentage_change_usd,
          highest_floor_price_30d_percentage_change_native,
          lowest_floor_price_90d_usd,
          highest_floor_price_90d_usd,
          lowest_floor_price_90d_native,
          highest_floor_price_90d_native,
          floor_price_90d_percentage_change_usd,
          floor_price_90d_percentage_change_native,
          lowest_floor_price_90d_percentage_change_usd,
          lowest_floor_price_90d_percentage_change_native,
          highest_floor_price_90d_percentage_change_usd,
          highest_floor_price_90d_percentage_change_native,
          lowest_floor_price_ytd_usd,
          highest_floor_price_ytd_usd,
          lowest_floor_price_ytd_native,
          highest_floor_price_ytd_native,
          floor_price_ytd_percentage_change_usd,
          floor_price_ytd_percentage_change_native,
          lowest_floor_price_ytd_percentage_change_usd,
          lowest_floor_price_ytd_percentage_change_native,
          highest_floor_price_ytd_percentage_change_usd,
          highest_floor_price_ytd_percentage_change_native,
          jsonb_build_array(  
          	jsonb_set( next_asset_1 , ''{rank}'', to_jsonb(rank+1) , true), 
          	jsonb_set( next_asset_2 , ''{rank}'', to_jsonb(rank+2) , true),
          	jsonb_set( next_asset_3 , ''{rank}'', to_jsonb(rank+3) , true),
          	jsonb_set( next_asset_4 , ''{rank}'', to_jsonb(rank+4) , true)
          ) as next_up,
          questions
        FROM 
          (	
            SELECT 
              *,
              CAST ( ROW_NUMBER () over (order by volume_24h_usd desc, slug desc) AS int) AS rank,
              LAG (slug, 1, '''') over (order by volume_24h_usd desc, slug desc) AS prev_ranked_slug,
              LEAD ( jsonb_build_object(''slug'' , slug, ''image'', jsonb_build_object(''small'' , image, ''large'', large_image), ''name'', name ) , 1, ''{}''::JSONB) 
              	OVER (order by volume_24h_usd desc, slug desc) AS next_asset_1,
              LEAD ( jsonb_build_object(''slug'' , slug, ''image'', jsonb_build_object(''small'' , image, ''large'', large_image), ''name'', name ) , 2, ''{}''::JSONB) 
              	OVER (order by volume_24h_usd desc, slug desc) AS next_asset_2,
              LEAD ( jsonb_build_object(''slug'' , slug, ''image'', jsonb_build_object(''small'' , image, ''large'', large_image), ''name'', name ) , 3, ''{}''::JSONB) 
              	OVER (order by volume_24h_usd desc, slug desc) AS next_asset_3,
              LEAD ( jsonb_build_object(''slug'' , slug, ''image'', jsonb_build_object(''small'' , image, ''large'', large_image), ''name'', name ) , 4, ''{}''::JSONB) 
              	OVER (order by volume_24h_usd desc, slug desc) AS next_asset_4
            FROM
              public.nftdatalatest
            WHERE
              volume_24h_percentage_change_usd is not null
          ) as T
        WHERE slug = %L',
        collectionSlug
    );
END
$$ LANGUAGE plpgsql;