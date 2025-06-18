CREATE TABLE fundamentals (
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
  listed_exchange VARCHAR(100) [],
  market_pairs JSON,
  exchanges JSON,
  nomics JSON,
  forbes JSON,
  last_updated TIMESTAMPTZ DEFAULT Now(),
  percentage_1h FLOAT,
);

CREATE INDEX ON "fundamentals" ("symbol");

CREATE INDEX ON "fundamentals" ("last_updated");

CREATE TABLE exchange_fundamentals (
  name TEXT,
  slug TEXT,
  id TEXT,
  logo TEXT,
  exchange_active_market_pairs NUMERIC,
  nomics JSON,
  forbes JSON,
  last_updated TIMESTAMPTZ DEFAULT Now()
);

CREATE TABLE chart_data_fundamentals (
  symbol TEXT,
  forbes TEXT,
  time TIMESTAMPTZ,
  price FLOAT,
  data_source TEXT
);

CREATE TABLE nomics_chart_data (
  is_index BOOLEAN,
  source TEXT,
  target_resolution_seconds INTEGER,
  prices JSON,
  symbol TEXT,
  interval TEXT Primary key
);

CREATE TABLE fundamentalslatest (
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
  listed_exchange VARCHAR(100) [],
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

CREATE PROCEDURE upsertFundamentalsLatest(
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
  listed_exchange VARCHAR(100) [],
  market_pairs JSON,
  exchanges JSON,
  nomics JSON,
  forbes JSON,
  last_updated timestamp,
  forbes_transparency_volume FLOAT,
  status TEXT,
  percentage_1h FLOAT
) LANGUAGE SQL AS $ BODY $
INSERT INTO
  fundamentalslatest
VALUES
  (
    symbol,
    name,
    slug,
    logo,
    float_type,
    display_symbol,
    original_symbol,
    source,
    temporary_data_delay,
    number_of_active_market_pairs,
    high_24h,
    low_24h,
    high_7d,
    low_7d,
    high_30d,
    low_30d,
    high_1y,
    low_1y,
    high_ytd,
    low_ytd,
    price_24h,
    price_7d,
    price_30d,
    price_1y,
    price_ytd,
    percentage_24h,
    percentage_7d,
    percentage_30d,
    percentage_1y,
    percentage_ytd,
    market_cap,
    market_cap_percent_change_1d,
    market_cap_percent_change_7d,
    market_cap_percent_change_30d,
    market_cap_percent_change_1y,
    market_cap_percent_change_ytd,
    circulating_supply,
    supply,
    all_time_low,
    all_time_high,
    date,
    change_value_24h,
    listed_exchange,
    market_pairs,
    exchanges,
    nomics,
    forbes,
    last_updated,
    forbes_transparency_volume,
    status,
    percentage_1h
  ) ON CONFLICT (symbol) DO
UPDATE
SET
  symbol = EXCLUDED.symbol,
  name = EXCLUDED.name,
  slug = EXCLUDED.slug,
  logo = EXCLUDED.logo,
  float_type = EXCLUDED.float_type,
  display_symbol = EXCLUDED.display_symbol,
  original_symbol = EXCLUDED.original_symbol,
  source = EXCLUDED.source,
  temporary_data_delay = EXCLUDED.temporary_data_delay,
  number_of_active_market_pairs = EXCLUDED.number_of_active_market_pairs,
  high_24h = EXCLUDED.high_24h,
  low_24h = EXCLUDED.low_24h,
  high_7d = EXCLUDED.high_7d,
  low_7d = EXCLUDED.low_7d,
  high_30d = EXCLUDED.high_30d,
  low_30d = EXCLUDED.low_30d,
  high_1y = EXCLUDED.high_1y,
  low_1y = EXCLUDED.low_1y,
  high_ytd = EXCLUDED.high_ytd,
  low_ytd = EXCLUDED.low_ytd,
  price_24h = EXCLUDED.price_24h,
  price_7d = EXCLUDED.price_7d,
  price_30d = EXCLUDED.price_30d,
  price_1y = EXCLUDED.price_1y,
  price_ytd = EXCLUDED.price_ytd,
  market_cap_percent_change_1d = EXCLUDED.market_cap_percent_change_1d,
  market_cap_percent_change_7d = EXCLUDED.market_cap_percent_change_7d,
  market_cap_percent_change_30d = EXCLUDED.market_cap_percent_change_30d,
  market_cap_percent_change_1y = EXCLUDED.market_cap_percent_change_1y,
  market_cap_percent_change_ytd = EXCLUDED.market_cap_percent_change_ytd,
  circulating_supply = EXCLUDED.circulating_supply,
  supply = EXCLUDED.supply,
  all_time_low = EXCLUDED.all_time_low,
  all_time_high = EXCLUDED.all_time_high,
  date = EXCLUDED.date,
  change_value_24h = EXCLUDED.change_value_24h,
  listed_exchange = EXCLUDED.listed_exchange,
  market_pairs = EXCLUDED.market_pairs,
  exchanges = EXCLUDED.exchanges,
  nomics = EXCLUDED.nomics,
  last_updated = EXCLUDED.last_updated,
  status = EXCLUDED.status,
  percentage_24h = EXCLUDED.percentage_24h,
  percentage_7d = EXCLUDED.percentage_7d,
  percentage_30d = EXCLUDED.percentage_30d,
  percentage_1y = EXCLUDED.percentage_1y,
  percentage_ytd = EXCLUDED.percentage_ytd,
  percentage_1h = EXCLUDED.percentage_1h,
  market_cap = EXCLUDED.market_cap;

$ BODY $;

CREATE
OR REPLACE PROCEDURE updateChartData(CANDLES JSONB, SYM TEXT) AS $ $ declare trgt_res_seconds int [] := array [14400,43200,432000,1296000];

declare intervals int [] := array [ '14400', 
'43200', 
'432000', 
'1296000' ];

times interval [] := array [ '7 DAYS', 
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
    prices -> -1 -> 'Time' as timestamp
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

if candlesRefined is not NULL then
insert into
  nomics_chart_data -- 3. insert the results into nomics_chart_data  
select
  -- 2. Get all of the information from step 1, and append all of the refinedCandles to it
  is_index,
  source,
  target_resolution_seconds,
  cast(
    json_agg(prices) :: jsonb || candlesRefined :: jsonb as json
  ) as prices,
  symbol,
  interval
from
  (
    select
      -- 1. get all prices, and exclude prices that fall out of range 
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
  cast(prices ->> 'Time' as timestamp) >= cast(now() - times [idx] as timestamp)
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

VALUES
  (
    symbol,
    name,
    slug,
    logo,
    float_type,
    display_symbol,
    original_symbol,
    source,
    temporary_data_delay,
    number_of_active_market_pairs,
    high_24h,
    low_24h,
    high_7d,
    low_7d,
    high_30d,
    low_30d,
    high_1y,
    low_1y,
    high_ytd,
    low_ytd,
    price_24h,
    price_7d,
    price_30d,
    price_1y,
    price_ytd,
    percentage_24h,
    percentage_7d,
    percentage_30d,
    percentage_1y,
    percentage_ytd,
    market_cap,
    market_cap_percent_change_1d,
    market_cap_percent_change_7d,
    market_cap_percent_change_30d,
    market_cap_percent_change_1y,
    market_cap_percent_change_ytd,
    circulating_supply,
    supply,
    all_time_low,
    all_time_high,
    date,
    change_value_24h,
    listed_exchange,
    market_pairs,
    exchanges,
    nomics,
    forbes,
    last_updated,
    forbes_transparency_volume
  ) ON CONFLICT (symbol) DO
UPDATE
SET
  symbol = EXCLUDED.symbol,
  name = EXCLUDED.name,
  slug = EXCLUDED.slug,
  logo = EXCLUDED.logo,
  float_type = EXCLUDED.float_type,
  display_symbol = EXCLUDED.display_symbol,
  original_symbol = EXCLUDED.original_symbol,
  source = EXCLUDED.source,
  temporary_data_delay = EXCLUDED.temporary_data_delay,
  number_of_active_market_pairs = EXCLUDED.number_of_active_market_pairs,
  high_24h = EXCLUDED.high_24h,
  low_24h = EXCLUDED.low_24h,
  high_7d = EXCLUDED.high_7d,
  low_7d = EXCLUDED.low_7d,
  high_30d = EXCLUDED.high_30d,
  low_30d = EXCLUDED.low_30d,
  high_1y = EXCLUDED.high_1y,
  low_1y = EXCLUDED.low_1y,
  high_ytd = EXCLUDED.high_ytd,
  low_ytd = EXCLUDED.low_ytd,
  price_24h = EXCLUDED.price_24h,
  price_7d = EXCLUDED.price_7d,
  price_30d = EXCLUDED.price_30d,
  price_1y = EXCLUDED.price_1y,
  price_ytd = EXCLUDED.price_ytd,
  percentage_24h = EXCLUDED.percentage_24h,
  percentage_7d = EXCLUDED.percentage_7d,
  percentage_30d = EXCLUDED.percentage_30d,
  percentage_1y = EXCLUDED.percentage_1y,
  percentage_ytd = EXCLUDED.percentage_ytd,
  market_cap = EXCLUDED.market_cap,
  market_cap_percent_change_1d = EXCLUDED.market_cap_percent_change_1d,
  market_cap_percent_change_7d = EXCLUDED.market_cap_percent_change_7d,
  market_cap_percent_change_30d = EXCLUDED.market_cap_percent_change_30d,
  market_cap_percent_change_1y = EXCLUDED.market_cap_percent_change_1y,
  market_cap_percent_change_ytd = EXCLUDED.market_cap_percent_change_ytd,
  circulating_supply = EXCLUDED.circulating_supply,
  supply = EXCLUDED.supply,
  all_time_low = EXCLUDED.all_time_low,
  all_time_high = EXCLUDED.all_time_high,
  date = EXCLUDED.date,
  change_value_24h = EXCLUDED.change_value_24h,
  listed_exchange = EXCLUDED.listed_exchange,
  market_pairs = EXCLUDED.market_pairs,
  exchanges = EXCLUDED.exchanges,
  nomics = EXCLUDED.nomics,
  last_updated = EXCLUDED.last_updated,
  forbes_transparency_volume = EXCLUDED.forbes_transparency_volume;

$ BODY $;

-- Returns all chart data.
-- if the interval is not 24hrs it returns all data for 7d/30d/1y/max, all charts are appended with the last ticker from 24h
-- if the interval is 24h it returns all data it returns all data  24h/7d/30d/1y/max,
CREATE
or REPLACE FUNCTION getChartData(intval TEXT, symb TEXT) RETURNS Table (
  is_index bool,
  source TEXT,
  target_resolution_seconds int,
  prices jsonb,
  symbol TEXT,
  tm_interval TEXT,
  status TEXT
) AS $ $ #variable_conflict use_column
begin --If we are not requesting a 24 hour chart return every chart 
--exluding 24hr. The charts will be used in FDA API
if intval not like '%24h%' then RETURN QUERY
select
  is_index,
  a.source as source,
  a.target_resolution_seconds as target_resolution_seconds,
  --append 24 hour cadle to chart that will be returned
  a.prices :: jsonb || b.prices :: jsonb as prices,
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
      and assetType = 'FT'
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
      and assetType = 'FT'
  ) b on b.symbol = a.symbol
  join (
    select
      symbol,
      status
    from
      fundamentalslatest
    where
      symbol = symb
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
      and assetType = 'FT'
    order by
      target_resolution_seconds asc
  ) a
  join (
    select
      symbol,
      status
    from
      fundamentalslatest
    where
      symbol = symb
  ) c on a.symbol = c.symbol;

end if;

end;

$ $ language PLPGSQL;

CREATE PROCEDURE removeEntryFromFundamentalslatest(sym TEXT) LANGUAGE SQL AS $ BODY $
delete from
  fundamentalslatest
where
  symbol ilike sym $ BODY $;

/*
 Takes a limit, a pageNumber,a string to sort by, and the direction,and source(our data source ex:coingecko)
 */
CREATE
OR REPLACE FUNCTION tradedAssetsPagination_BySource_1(
  lim int,
  pageNum int,
  sortBy Text,
  direction Text,
  source text
) RETURNS Table (
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
) AS $ $ BEGIN RETURN QUERY EXECUTE format(
  'SELECT 
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
  lim * pageNum
) USING sortBy,
lim,
pageNum,
direction,
source;

END $ $ LANGUAGE plpgsql;

/*
 Takes a source as input and returns all traded assets
 */
CREATE
OR REPLACE FUNCTION searchTradedAssetsBySource(source text) RETURNS Table (
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
) AS $ $ BEGIN RETURN QUERY EXECUTE format(
  'SELECT 
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

END $ $ LANGUAGE plpgsql;

/*
 DEPRECATED in favour of getCategoriesV2()
 */
CREATE
OR REPLACE FUNCTION getCategories() RETURNS Table (
  id Text,
  name Text,
  market_cap float,
  market_cap_change_24h float,
  content Text,
  top_3_coins Text [],
  volume_24h float,
  last_updated TIMESTAMPTZ,
  markets json
) AS $ $ BEGIN RETURN QUERY EXECUTE format(
  'SELECT 
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

END $ $ LANGUAGE plpgsql;

/**
 * Get all categories list from coingecko_categories
 *Deprecated for V3
 */
CREATE
OR REPLACE FUNCTION getCategoriesV2() RETURNS Table (
  id Text,
  name Text,
  market_cap float,
  market_cap_change_24h float,
  content Text,
  top_3_coins Text [],
  volume_24h float,
  last_updated TIMESTAMPTZ,
  markets json,
  inactive boolean
) AS $ $ BEGIN RETURN QUERY EXECUTE format(
  'SELECT 
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

END $ $ LANGUAGE plpgsql;

/* stored procedure for get crypto content */
create
or replace function getcryptocontent(slg text) RETURNS Table (
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
) as $ func $
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
FROM
  (
    SELECT
      symbol,
      display_symbol,
      slug,
      status,
      market_cap,
      price_24h,
      number_of_active_market_pairs
    FROM
      fundamentalslatest
    WHERE
      slug = slg
  ) a
  LEFT JOIN (
    SELECT
      id,
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
    FROM
      coingecko_asset_metadata
  ) b ON a.symbol = b.id $ func $ Language sql -- get tob exchanges by trust score
  -- deprecated for getTopExchangesV2
  create
  or replace FUNCTION getTopExchanges() RETURNS Table(
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
  ) as $ $ DECLARE lim int := 5;

BEGIN RETURN QUERY EXECUTE format(
  '
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
              limit %s;',
  lim
) USING lim;

END;

$ $ Language plpgsql;

CREATE TABLE exchange_fundamentalslatest (
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

-- get a list of exchange fundamentals.
create
or replace FUNCTION getExchangeFundamentals() RETURNS Table(
  name TEXT,
  slug TEXT,
  id text,
  logo TEXT,
  exchange_active_market_pairs NUMERIC,
  nomics JSON,
  forbes JSON,
  last_updated TIMESTAMPTZ
) as $ $ BEGIN RETURN QUERY EXECUTE format(
  '
							SELECT 
                name, 
                slug,
                id,
                logo,
                exchange_active_market_pairs,
                nomics,
                forbes,
                last_updated
              FROM 
                public.exchange_fundamentalslatest
              '
);

END;

$ $ Language plpgsql;

CREATE TABLE IF NOT EXISTS categories_fundamentals (
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
  inactive BOOLEAN DEFAULT false PRIMARY KEY (id)
);

ALTER TABLE
  categories_fundamentals
ADD
  COLUMN inactive BOOLEAN DEFAULT false;

CREATE PROCEDURE upsert_exchange_fundamentalslatest(
  name TEXT,
  slug TEXT,
  id TEXT,
  logo TEXT,
  exchange_active_market_pairs NUMERIC,
  nomics JSON,
  forbes JSON,
  last_updated timestamp
) LANGUAGE SQL AS $ BODY $
INSERT INTO
  exchange_fundamentalslatest
VALUES
  (
    name,
    slug,
    id,
    logo,
    exchange_active_market_pairs,
    nomics,
    forbes,
    last_updated
  ) ON CONFLICT (id) DO
UPDATE
SET
  id = EXCLUDED.id,
  name = EXCLUDED.name,
  slug = EXCLUDED.slug,
  logo = EXCLUDED.logo,
  exchange_active_market_pairs = EXCLUDED.exchange_active_market_pairs,
  nomics = EXCLUDED.nomics,
  forbes = EXCLUDED.forbes,
  last_updated = EXCLUDED.last_updated;

$ BODY $;

CREATE INDEX idx_ds_source_mc ON fundamentalslatest (display_symbol, source, market_cap);

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
CREATE
or REPLACE FUNCTION getFTNFTChartData(intval TEXT, symb TEXT, assetsTp TEXT) RETURNS Table (
  is_index bool,
  source TEXT,
  target_resolution_seconds int,
  prices jsonb,
  symbol TEXT,
  tm_interval TEXT,
  status TEXT
) AS $ $ #variable_conflict use_column
begin --If we are not requesting a 24 hour chart return every chart 
--exluding 24hr. The charts will be used in FDA API
-- we will check for the type if it is NFT or FT
if assetsTp = 'NFT' then if intval not like '%24h%' then RETURN QUERY
select
  is_index,
  a.source as source,
  a.target_resolution_seconds as target_resolution_seconds,
  --append 24 hour cadle to chart that will be returned
  a.prices :: jsonb || b.prices :: jsonb as prices,
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
          nftdatalatest
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
          nftdatalatest
        where
          Id = symb
      ) c
  ) c on a.symbol = c.symbol;

end if;

else if intval not like '%24h%' then RETURN QUERY
select
  is_index,
  a.source as source,
  a.target_resolution_seconds as target_resolution_seconds,
  --append 24 hour cadle to chart that will be returned
  a.prices :: jsonb || b.prices :: jsonb as prices,
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
      symbol,
      status
    from
      fundamentalslatest
    where
      symbol = symb
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
      symbol,
      status
    from
      fundamentalslatest
    where
      symbol = symb
  ) c on a.symbol = c.symbol;

end if;

end if;

end;

$ $ language PLPGSQL;

CREATE
OR REPLACE FUNCTION public.getcategoriesfundamentalsV2() RETURNS TABLE(
  id text,
  name text,
  total_tokens integer,
  average_percentage_24h double precision,
  volume_24h double precision,
  price_24h double precision,
  average_price double precision,
  market_cap double precision,
  market_cap_percentage_24h double precision,
  top_gainers json,
  top_movers json,
  forbesname text,
  slug text,
  last_updated timestamp,
  is_highlighted
) LANGUAGE 'sql' AS $ BODY $
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
  last_updated,
  is_highlighted
FROM
  public.categories_fundamentals
where
  COALESCE(inactive, false) = false
  and top_gainers :: jsonb != 'null' :: jsonb
  and top_movers :: jsonb != '[]' :: jsonb;

$ BODY $;

-- DEPRECATED in favour of GetNFTCollectionWithRank(text)
CREATE
OR REPLACE FUNCTION public.GetNFTCollection (collectionSlug text) RETURNS TABLE (
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
) AS $ $ BEGIN RETURN QUERY EXECUTE format(
  '
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

END $ $ LANGUAGE plpgsql;

-- Get An NFT Collection from its slug.
CREATE
OR REPLACE FUNCTION public.GetNFTCollectionWithRank (collectionSlug text) RETURNS TABLE (
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
) AS $ $ BEGIN RETURN QUERY EXECUTE format(
  '
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
          coalesce(large_image, '''') as large_image,
          description,
          native_currency,
          COALESCE(floor_price_usd, 0) as floor_price_usd,
          COALESCE(market_cap_usd, 0) as market_cap_usd,
          COALESCE(volume_24h_usd, 0) as volume_24h_usd,
          COALESCE(floor_price_native, 0) as floor_price_native,
          COALESCE(market_cap_native, 0) as market_cap_native,
          COALESCE(volume_24h_native, 0) as volume_24h_native,
          COALESCE(floor_price_in_usd_24h_percentage_change, 0) as floor_price_in_usd_24h_percentage_change,
          COALESCE(volume_24h_percentage_change_usd, 0) as volume_24h_percentage_change_usd,
          number_of_unique_addresses,
          number_of_unique_addresses_24h_percentage_change,
          slug,
          total_supply,
          COALESCE(website_url, '''') as website_url,
          COALESCE(twitter_url, '''') as twitter_url,
          COALESCE(discord_url, '''') as discord_url,
          COALESCE(explorers, ''[]''::JSON ) as explorers,
          last_updated,
          COALESCE(avg_sale_price_1d, 0) as avg_sale_price_1d,
          COALESCE(avg_sale_price_7d, 0) as avg_sale_price_7d,
          COALESCE(avg_sale_price_30d, 0) as avg_sale_price_30d,
          COALESCE(avg_sale_price_90d, 0) as avg_sale_price_90d,
          COALESCE(avg_sale_price_ytd, 0) as avg_sale_price_ytd,
          COALESCE(avg_total_sales_pct_change_1d, 0) as avg_total_sales_pct_change_1d,
          COALESCE(avg_total_sales_pct_change_7d, 0) as avg_total_sales_pct_change_7d,
          COALESCE(avg_total_sales_pct_change_30d, 0) as avg_total_sales_pct_change_30d,
          COALESCE(avg_total_sales_pct_change_90d, 0) as avg_total_sales_pct_change_90d,
          COALESCE(avg_total_sales_pct_change_ytd, 0) as avg_total_sales_pct_change_ytd,
          COALESCE(total_sales_1d, 0) as total_sales_1d,
          COALESCE(total_sales_7d, 0) as total_sales_7d,
          COALESCE(total_sales_30d, 0) as total_sales_30d,
          COALESCE(total_sales_90d, 0) as total_sales_90d,
          COALESCE(total_sales_ytd, 0) as total_sales_ytd,
          COALESCE(avg_sales_price_change_1d, 0) as avg_sales_price_change_1d,
          COALESCE(avg_sales_price_change_7d, 0) as avg_sales_price_change_7d,
          COALESCE(avg_sales_price_change_30d, 0) as avg_sales_price_change_30d,
          COALESCE(avg_sales_price_change_90d, 0) as avg_sales_price_change_90d,
          COALESCE(avg_sales_price_change_ytd, 0) as avg_sales_price_change_ytd,
          native_currency_symbol,
          COALESCE(market_cap_24h_percentage_change_usd, 0) as market_cap_24h_percentage_change_usd,
          COALESCE(market_cap_24h_percentage_change_native, 0) as market_cap_24h_percentage_change_native,
          COALESCE(volume_24h_percentage_change_native, 0) as volume_24h_percentage_change_native,
          COALESCE(volume_usd_1d, 0) as volume_usd_1d,
          COALESCE(volume_usd_7d, 0) as volume_usd_7d,
          COALESCE(volume_usd_30d, 0) as volume_usd_30d,
          COALESCE(volume_usd_90d, 0) as volume_usd_90d,
          COALESCE(volume_usd_ytd, 0) as volume_usd_ytd,
          COALESCE(volume_native_1d, 0) as volume_native_1d,
          COALESCE(volume_native_7d, 0) as volume_native_7d,
          COALESCE(volume_native_30d, 0) as volume_native_30d,
          COALESCE(volume_native_90d, 0) as volume_native_90d,
          COALESCE(volume_native_ytd, 0) as volume_native_ytd,
          COALESCE(pct_change_volume_usd_1d, 0) as pct_change_volume_usd_1d,
          COALESCE(pct_change_volume_usd_7d, 0) as pct_change_volume_usd_7d,
          COALESCE(pct_change_volume_usd_30d, 0) as pct_change_volume_usd_30d,
          COALESCE(pct_change_volume_usd_90d, 0) as pct_change_volume_usd_90d,
          COALESCE(pct_change_volume_usd_ytd, 0) as pct_change_volume_usd_ytd,
          COALESCE(pct_change_volume_native_1d, 0) as pct_change_volume_native_1d,
          COALESCE(pct_change_volume_native_7d, 0) as pct_change_volume_native_7d,
          COALESCE(pct_change_volume_native_30d, 0) as pct_change_volume_native_30d,
          COALESCE(pct_change_volume_native_90d, 0) as pct_change_volume_native_90d,
          COALESCE(pct_change_volume_native_ytd, 0) as pct_change_volume_native_ytd,
          COALESCE(lowest_floor_price_24h_usd, 0) as lowest_floor_price_24h_usd,
          COALESCE(highest_floor_price_24h_usd, 0) as highest_floor_price_24h_usd,
          COALESCE(lowest_floor_price_24h_native, 0) as lowest_floor_price_24h_native,
          COALESCE(highest_floor_price_24h_native, 0) as highest_floor_price_24h_native,
          COALESCE(floor_price_24h_percentage_change_usd, 0) as floor_price_24h_percentage_change_usd,
          COALESCE(floor_price_24h_percentage_change_native, 0) as floor_price_24h_percentage_change_native,
          COALESCE(lowest_floor_price_24h_percentage_change_usd, 0) as lowest_floor_price_24h_percentage_change_usd,
          COALESCE(lowest_floor_price_24h_percentage_change_native, 0) as lowest_floor_price_24h_percentage_change_native,
          COALESCE(highest_floor_price_24h_percentage_change_usd, 0) as highest_floor_price_24h_percentage_change_usd,
          COALESCE(highest_floor_price_24h_percentage_change_native, 0) as highest_floor_price_24h_percentage_change_native,
          COALESCE(lowest_floor_price_7d_usd, 0) as lowest_floor_price_7d_usd,
          COALESCE(highest_floor_price_7d_usd, 0) as highest_floor_price_7d_usd,
          COALESCE(lowest_floor_price_7d_native, 0) as lowest_floor_price_7d_native,
          COALESCE(highest_floor_price_7d_native, 0) as highest_floor_price_7d_native,
          COALESCE(floor_price_7d_percentage_change_usd, 0) as floor_price_7d_percentage_change_usd,
          COALESCE(floor_price_7d_percentage_change_native, 0) as floor_price_7d_percentage_change_native,
          COALESCE(lowest_floor_price_7d_percentage_change_usd, 0) as lowest_floor_price_7d_percentage_change_usd,
          COALESCE(lowest_floor_price_7d_percentage_change_native, 0) as lowest_floor_price_7d_percentage_change_native,
          COALESCE(highest_floor_price_7d_percentage_change_usd, 0) as highest_floor_price_7d_percentage_change_usd,
          COALESCE(highest_floor_price_7d_percentage_change_native, 0) as highest_floor_price_7d_percentage_change_native,
          COALESCE(lowest_floor_price_30d_usd, 0) as lowest_floor_price_30d_usd,
          COALESCE(highest_floor_price_30d_usd, 0) as highest_floor_price_30d_usd,
          COALESCE(lowest_floor_price_30d_native, 0) as lowest_floor_price_30d_native,
          COALESCE(highest_floor_price_30d_native, 0) as highest_floor_price_30d_native,
          COALESCE(floor_price_30d_percentage_change_usd, 0) as floor_price_30d_percentage_change_usd,
          COALESCE(floor_price_30d_percentage_change_native, 0) as floor_price_30d_percentage_change_native,
          COALESCE(lowest_floor_price_30d_percentage_change_usd, 0) as lowest_floor_price_30d_percentage_change_usd,
          COALESCE(lowest_floor_price_30d_percentage_change_native, 0) as lowest_floor_price_30d_percentage_change_native,
          COALESCE(highest_floor_price_30d_percentage_change_usd, 0) as highest_floor_price_30d_percentage_change_usd,
          COALESCE(highest_floor_price_30d_percentage_change_native, 0) as highest_floor_price_30d_percentage_change_native,
          COALESCE(lowest_floor_price_90d_usd, 0) as lowest_floor_price_90d_usd,
          COALESCE(highest_floor_price_90d_usd, 0) as highest_floor_price_90d_usd,
          COALESCE(lowest_floor_price_90d_native, 0) as lowest_floor_price_90d_native,
          COALESCE(highest_floor_price_90d_native, 0) as highest_floor_price_90d_native,
          COALESCE(floor_price_90d_percentage_change_usd, 0) as floor_price_90d_percentage_change_usd,
          COALESCE(floor_price_90d_percentage_change_native, 0) as floor_price_90d_percentage_change_native,
          COALESCE(lowest_floor_price_90d_percentage_change_usd, 0) as lowest_floor_price_90d_percentage_change_usd,
          COALESCE(lowest_floor_price_90d_percentage_change_native, 0) as lowest_floor_price_90d_percentage_change_native,
          COALESCE(highest_floor_price_90d_percentage_change_usd, 0) as highest_floor_price_90d_percentage_change_usd,
          COALESCE(highest_floor_price_90d_percentage_change_native, 0) as highest_floor_price_90d_percentage_change_native,
          COALESCE(lowest_floor_price_ytd_usd, 0) as lowest_floor_price_ytd_usd,
          COALESCE(highest_floor_price_ytd_usd, 0) as highest_floor_price_ytd_usd,
          COALESCE(lowest_floor_price_ytd_native, 0) as lowest_floor_price_ytd_native,
          COALESCE(highest_floor_price_ytd_native, 0) as highest_floor_price_ytd_native,
          COALESCE(floor_price_ytd_percentage_change_usd, 0) as floor_price_ytd_percentage_change_usd,
          COALESCE(floor_price_ytd_percentage_change_native, 0) as floor_price_ytd_percentage_change_native,
          COALESCE(lowest_floor_price_ytd_percentage_change_usd, 0) as lowest_floor_price_ytd_percentage_change_usd,
          COALESCE(lowest_floor_price_ytd_percentage_change_native, 0) as lowest_floor_price_ytd_percentage_change_native,
          COALESCE(highest_floor_price_ytd_percentage_change_usd, 0) as highest_floor_price_ytd_percentage_change_usd,
          COALESCE(highest_floor_price_ytd_percentage_change_native, 0) as highest_floor_price_ytd_percentage_change_native,
          jsonb_build_array(  
          	jsonb_set( next_asset_1 , ''{rank}'', to_jsonb(rank+1) , true), 
          	jsonb_set( next_asset_2 , ''{rank}'', to_jsonb(rank+2) , true),
          	jsonb_set( next_asset_3 , ''{rank}'', to_jsonb(rank+3) , true),
          	jsonb_set( next_asset_4 , ''{rank}'', to_jsonb(rank+4) , true)
          ) as next_up,
          COALESCE(questions, ''[]''::JSON )
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
              and is_active = true 
          ) as T
        WHERE slug = %L',
  collectionSlug
);

END $ $ LANGUAGE plpgsql;

CREATE
or REPLACE FUNCTION getCategoriesChartData(intval TEXT, symb TEXT, assetsTp TEXT) RETURNS Table (
  is_index bool,
  source TEXT,
  target_resolution_seconds int,
  prices jsonb,
  symbol TEXT,
  tm_interval TEXT,
  status TEXT
) AS $ $ #variable_conflict use_column
begin --If we are not requesting a 24 hour chart return every chart 
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

$ $ language PLPGSQL;

CREATE INDEX idx_target_resolution_seconds ON nomics_chart_data (target_resolution_seconds);

/**
 * Get all categories list from coingecko_categories
 */
CREATE
OR REPLACE FUNCTION public.getcategoriesv3() RETURNS TABLE(
  id text,
  name text,
  market_cap double precision,
  market_cap_change_24h double precision,
  content text,
  top_3_coins text [],
  volume_24h double precision,
  last_updated timestamp with time zone,
  markets json,
  inactive boolean
) LANGUAGE 'plpgsql' AS $ BODY $ BEGIN RETURN QUERY EXECUTE format(
  'select 
	id,
	name,
	market_cap,
	market_cap_percentage_24h as market_cap_change_24h,
	coalesce(content, '''') as content,
	ARRAY	(
   		SELECT elem->>''logo''
    	FROM json_array_elements(top_gainers) AS elem
	)::text[] as top_3_coins,
	volume_24h,
	last_updated,
	markets,
	coalesce(inactive, false) as inactive
	from 
	categories_fundamentals
	WHERE json_typeof(top_gainers) = ''array''
  '
);

END $ BODY $;

CREATE
OR REPLACE FUNCTION public.getcategoriesV4() RETURNS TABLE(
  id text,
  name text,
  market_cap double precision,
  market_cap_change_24h double precision,
  content text,
  top_3_coins text [],
  volume_24h double precision,
  last_updated timestamp with time zone,
  markets json,
  inactive boolean,
  is_highlighted boolean
) LANGUAGE 'plpgsql' AS $ BODY $ BEGIN RETURN QUERY EXECUTE format(
  'select 
	id,
	name,
	market_cap,
	market_cap_percentage_24h as market_cap_change_24h,
	coalesce(content, '''') as content,
	ARRAY	(
   		SELECT elem->>''logo''
    	FROM json_array_elements(top_gainers) AS elem
	)::text[] as top_3_coins,
	volume_24h,
	last_updated,
  coalesce(markets, ''[]'') as markets,
	coalesce(inactive, false) as inactive,
  is_highlighted
	from 
	categories_fundamentals
	WHERE json_typeof(top_gainers) = ''array''
  '
);

END $ BODY $;

/*
 Gets all information needed for the traded assests search.
 deprecated for searchtradedassetsbysourcev3
 */
CREATE
OR REPLACE FUNCTION public.searchtradedassetsbysourcev2(source text) RETURNS TABLE(
  symbol text,
  display_symbol text,
  name text,
  slug text,
  logo text,
  temporary_data_delay boolean,
  price_24h double precision,
  percentage_1h double precision,
  percentage_24h double precision,
  percentage_7d double precision,
  change_value_24h double precision,
  market_cap double precision,
  volume_1d double precision,
  status text,
  market_cap_percent_change_1d double precision,
  date_added timestamp with time zone,
  rank_number bigint,
  platform_currency_id text,
  description text
) LANGUAGE 'plpgsql' AS $ BODY $ BEGIN RETURN QUERY EXECUTE format(
  'select
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
  volume_1d,
  status,
  market_cap_percent_change_1d,
  date_added,
  rank_number,
  coalesce(platform_currency_id, '''') as platform_currency_id,
  coalesce(description, '''') as description
from
  (
    SELECT
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
      date_added,
      RANK () OVER (
        partition by status
        ORDER BY
          market_cap desc
      ) rank_number
    from
      fundamentalslatest
    where
      source = ''%s''
      and name != ''''
      and market_cap is not null
  ) assets
  left join (
    select
      id,
      platform_currency_id,
      description
    from
      coingecko_asset_metadata
  ) metadata on assets.symbol = metadata.id
',
  quote_ident('volume_1d'),
  source
) USING source;

END $ BODY $;

/*
 Gets all information needed for the traded assests search.
 */
CREATE
OR REPLACE FUNCTION public.searchtradedassetsbysourcev3(source text) RETURNS TABLE(
  symbol text,
  display_symbol text,
  name text,
  slug text,
  logo text,
  temporary_data_delay boolean,
  price_24h double precision,
  percentage_1h double precision,
  percentage_24h double precision,
  percentage_7d double precision,
  change_value_24h double precision,
  market_cap double precision,
  volume_1d double precision,
  status text,
  market_cap_percent_change_1d double precision,
  date_added timestamp with time zone,
  rank_number bigint,
  platform_currency_id text,
  description text,
  platforms json
) LANGUAGE 'plpgsql' AS $ BODY $ BEGIN RETURN QUERY EXECUTE format(
  'select
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
  volume_1d,
  status,
  market_cap_percent_change_1d,
  date_added,
  rank_number,
  coalesce(platform_currency_id, '''') as platform_currency_id,
  coalesce(description, '''') as description,
  coalesce(platforms, ''{}'') as platforms
from
  (
    SELECT
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
      date_added,
      RANK () OVER (
        partition by status
        ORDER BY
          market_cap desc
      ) rank_number
    from
      fundamentalslatest
    where
      source = ''%s''
      and name != ''''
      and market_cap is not null
  ) assets
  left join (
    select
      id,
      platform_currency_id,
      description,
      platforms
    from
      coingecko_asset_metadata
  ) metadata on assets.symbol = metadata.id
',
  quote_ident('volume_1d'),
  source
) USING source;

END $ BODY $;

CREATE
OR REPLACE FUNCTION public.GetFundamentals(symbolIN text) RETURNS TABLE(
  symbol text,
  name text,
  slug text,
  logo text,
  float_type text,
  display_symbol text,
  original_symbol text,
  source text,
  temporary_data_delay boolean,
  number_of_active_market_pairs INTEGER,
  high_24h double precision,
  low_24h double precision,
  high_7d double precision,
  low_7d double precision,
  high_30d double precision,
  low_30d double precision,
  high_1y double precision,
  low_1y double precision,
  high_ytd double precision,
  low_ytd double precision,
  price_24h double precision,
  price_7d double precision,
  price_30d double precision,
  price_1y double precision,
  price_ytd double precision,
  percentage_1h double precision,
  percentage_24h double precision,
  percentage_7d double precision,
  percentage_30d double precision,
  percentage_1y double precision,
  percentage_ytd double precision,
  market_cap double precision,
  market_cap_percent_change_1d double precision,
  market_cap_percent_change_7d double precision,
  market_cap_percent_change_30d double precision,
  market_cap_percent_change_1y double precision,
  market_cap_percent_change_ytd double precision,
  circulating_supply NUMERIC,
  supply NUMERIC,
  all_time_low double precision,
  all_time_high double precision,
  date TIMESTAMPTZ,
  change_value_24h double precision,
  listed_exchange VARCHAR(100) [],
  market_pairs JSON,
  exchanges JSON,
  nomics JSON,
  forbes JSON,
  last_updated TIMESTAMPTZ,
  platforms JSON
) LANGUAGE 'plpgsql' AS $ BODY $ BEGIN RETURN QUERY EXECUTE format(
  '
	 SELECT 
      symbol,
      name,
      slug,
      logo,
      float_type,
      display_symbol,
      original_symbol,
      source,
      temporary_data_delay,
      number_of_active_market_pairs,
      high_24h,
      low_24h,
      high_7d,
      low_7d,
      high_30d,
      low_30d,
      high_1y,
      low_1y,
      high_ytd,
      low_ytd,
      price_24h,
      price_7d,
      price_30d,
      price_1y,
      price_ytd,
      percentage_1h,
      percentage_24h,
      percentage_7d,
      percentage_30d,
      percentage_1y,
      percentage_ytd,
      market_cap,
      market_cap_percent_change_1d,
      market_cap_percent_change_7d,
      market_cap_percent_change_30d,
      market_cap_percent_change_1y,
      market_cap_percent_change_ytd,
      circulating_supply,
      supply,
      all_time_low,
      all_time_high,
      date,
      change_value_24h,
      listed_exchange,
      market_pairs,
      exchanges,
      nomics,
      forbes,
      last_updated,
      COALESCE(platforms, ''{}'') as platforms
from
  (
		 SELECT 
			 symbol,
			 name,
			 slug,
			 logo,
			 float_type,
			 display_symbol,
			 original_symbol,
			 source,
			 temporary_data_delay,
			 number_of_active_market_pairs,
			 high_24h,
			 low_24h,
			 high_7d,
			 low_7d,
			 high_30d,
			 low_30d,
			 high_1y,
			 low_1y,
			 high_ytd,
			 low_ytd,
			 price_24h,
			 price_7d,
			 price_30d,
			 price_1y,
			 price_ytd,
			 percentage_1h,
			 percentage_24h,
			 percentage_7d,
			 percentage_30d,
			 percentage_1y,
			 percentage_ytd,
			 market_cap,
			 market_cap_percent_change_1d,
			 market_cap_percent_change_7d,
			 market_cap_percent_change_30d,
			 market_cap_percent_change_1y,
			 market_cap_percent_change_ytd,
			 circulating_supply,
			 supply,
			 all_time_low,
			 all_time_high,
			 date,
			 change_value_24h,
			 listed_exchange,
			 market_pairs,
			 exchanges,
			 nomics,
			 forbes,
			 last_updated
		 FROM 
			 fundamentalslatest
		 where symbol = ''%s''
		 ORDER BY 
			 last_updated desc
		 limit 1
  ) assets
  left join (
    select
      id,
      platforms
    from
      coingecko_asset_metadata
  ) metadata on assets.symbol = metadata.id
',
  symbolIN
) USING symbolIN;

END $ BODY $;

-- This SQL script enables the pg_trgm extension in PostgreSQL.
-- The pg_trgm extension provides functions and operators for determining the similarity of text based on trigram matching.
-- Trigrams are groups of three consecutive characters, and this extension is useful for text search and fuzzy string matching.
CREATE EXTENSION pg_trgm;

/*
 FUNCTION: fuzzySearch_categoriesfundamentals
 
 DESCRIPTION:
 This function performs a fuzzy search on the categories fundamentals data. It returns a paginated and sorted result set based on the provided parameters. If a search term is provided, it filters the results based on the similarity of the category name to the search term.
 
 PARAMETERS:
 - lim (int): The limit on the number of rows to return.
 - page_num (int): The page number for pagination.
 - sort_by (text): The column name to sort the results by.
 - direction (text): The direction of sorting ('ASC' or 'DESC').
 - search_term (text): The term to search for in the category names.
 
 RETURNS:
 TABLE (
 id (text): The ID of the category.
 name (text): The name of the category.
 total_tokens (integer): The total number of tokens in the category.
 average_percentage_24h (double precision): The average percentage change in the last 24 hours.
 volume_24h (double precision): The volume in the last 24 hours.
 price_24h (double precision): The price in the last 24 hours.
 average_price (double precision): The average price.
 market_cap (double precision): The market capitalization.
 market_cap_percentage_24h (double precision): The market cap percentage change in the last 24 hours.
 top_gainers (json): The top gainers in the category.
 top_movers (json): The top movers in the category.
 forbesname (text): The Forbes name of the category.
 slug (text): The slug of the category.
 last_updated (timestamp): The last updated timestamp.
 is_highlighted (boolean): Indicates if the category is highlighted.
 
 USAGE:
 - To retrieve a paginated and sorted list of categories fundamentals.
 - To perform a fuzzy search on the category names based on the provided search term.
 */
CREATE
OR REPLACE FUNCTION fuzzySearch_categoriesfundamentals(
  lim int,
  page_num int,
  sort_by text,
  direction text,
  search_term text
) RETURNS TABLE (
  id text,
  name text,
  total_tokens integer,
  average_percentage_24h double precision,
  volume_24h double precision,
  price_24h double precision,
  average_price double precision,
  market_cap double precision,
  market_cap_percentage_24h double precision,
  top_gainers json,
  top_movers json,
  forbesname text,
  slug text,
  last_updated timestamp,
  is_highlighted boolean
) AS $ $ BEGIN IF search_term = '' Then RETURN QUERY EXECUTE format(
  '
        SELECT 
            t.id, 
            t.name, 
            t.total_tokens, 
            t.average_percentage_24h, 
            t.volume_24h,
            t.price_24h,
            t.average_price, 
            t.market_cap, 
            t.market_cap_percentage_24h, 
            t.top_gainers,
            t.top_movers,
            t.forbesname,
            t.slug, 
            t.last_updated,
            t.is_highlighted
        FROM 
            public.GetCategoriesFundamentalsV2() as t
        ORDER BY t.%s %s NULLS LAST
        LIMIT %s
        OFFSET %s;',
  sort_by,
  direction,
  lim,
  lim * page_num
);

Else RETURN QUERY EXECUTE format(
  '
        SELECT 
            t.id, 
            t.name, 
            t.total_tokens, 
            t.average_percentage_24h, 
            t.volume_24h,
            t.price_24h,
            t.average_price, 
            t.market_cap, 
            t.market_cap_percentage_24h, 
            t.top_gainers,
            t.top_movers,
            t.forbesname,
            t.slug, 
            t.last_updated,
            t.is_highlighted
        FROM 
            public.GetCategoriesFundamentalsV2() as t
        WHERE 
            SIMILARITY(lower(t.name), ''%s'') > 0.1
        ORDER BY t.%s %s NULLS LAST
        LIMIT %s
        OFFSET %s;',
  search_term,
  sort_by,
  direction,
  lim,
  lim * page_num
);

END IF;

END;

$ $ LANGUAGE plpgsql;

/*
 FUNCTION: public.fuzzysearch_fundamentalslatest
 
 DESCRIPTION:
 This function performs a fuzzy search on the 'fundamentalslatest' table and returns a paginated result set based on the provided parameters. 
 It supports sorting and filtering based on various criteria and can handle both exact and fuzzy matches for the search term.
 
 PARAMETERS:
 - lim (integer): The limit on the number of rows to return.
 - page_num (integer): The page number for pagination.
 - sort_by (text): The column name to sort the results by.
 - direction (text): The direction of sorting ('asc' or 'desc').
 - source (text): The source of the data.
 - search_term (text): The term to search for in the 'name' or 'display_symbol' columns.
 
 RETURNS:
 - TABLE (
 symbol text,
 display_symbol text,
 name text,
 slug text,
 logo text,
 temporary_data_delay boolean,
 price_24h double precision,
 percentage_1h double precision,
 percentage_24h double precision,
 percentage_7d double precision,
 change_value_24h double precision,
 market_cap double precision,
 volume_1d double precision,
 status text,
 market_cap_percent_change_1d double precision,
 date_added timestamp with time zone,
 rank_number bigint,
 platform_currency_id text,
 description text,
 platforms json,
 full_count bigint
 )
 
 LOGIC:
 - If 'search_term' is empty, it performs a simple query with sorting and pagination.
 - If 'search_term' is provided, it performs a fuzzy search using similarity and exact match checks.
 - The results are ordered by exact match first (if any), followed by the specified sort order.
 - The function uses Common Table Expressions (CTEs) to separate exact matches from non-exact matches and combines them in the final result set.
 */
CREATE
OR REPLACE FUNCTION public.fuzzysearch_fundamentalslatest(
  lim integer,
  page_num integer,
  sort_by text,
  direction text,
  source text,
  search_term text
) RETURNS TABLE(
  symbol text,
  display_symbol text,
  name text,
  slug text,
  logo text,
  temporary_data_delay boolean,
  price_24h double precision,
  percentage_1h double precision,
  percentage_24h double precision,
  percentage_7d double precision,
  change_value_24h double precision,
  market_cap double precision,
  volume_1d double precision,
  status text,
  market_cap_percent_change_1d double precision,
  date_added timestamp with time zone,
  rank_number bigint,
  platform_currency_id text,
  description text,
  platforms json,
  full_count bigint
) LANGUAGE 'plpgsql' AS $ BODY $ BEGIN IF search_term = '' THEN RETURN QUERY EXECUTE format(
  ' SELECT
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
    date_added,
    RANK () OVER (
    partition by status
    ORDER BY
      market_cap desc
    ) rank_number,
     coalesce(platform_currency_id, '''') as platform_currency_id,
     coalesce(description, '''') as description,
     coalesce(platforms, ''{}'') as platforms,
     count(symbol) over() as full_count
  from
    fundamentalslatest assets
  left join (
  select
    id,
    platform_currency_id,
    description,
    platforms
  from
    coingecko_asset_metadata
  ) metadata on assets.symbol = metadata.id
  where
    source = ''%s''
    and name != ''''
    and market_cap is not null
    and status = ''active''
  order by %s %s
  limit  %s
  offset %s;',
  quote_ident('volume_1d'),
  source,
  sort_by,
  direction,
  lim,
  lim * page_num
) USING source,
direction,
sort_by,
lim,
page_num;

ELSE RETURN QUERY EXECUTE format(
  ' with assets as (SELECT
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
    date_added,
    RANK () OVER (
    partition by status
    ORDER BY
      market_cap desc
    ) rank_number,
     coalesce(platform_currency_id, '''') as platform_currency_id,
     coalesce(description, '''') as description,
     coalesce(platforms, ''{}'') as platforms,
     count(symbol) over() as full_count,
     CASE 
  WHEN lower(name) = ''%s'' OR lower(display_symbol) = ''%s'' THEN 1
     ELSE 0
     END AS exact_match
  from
    fundamentalslatest as assets
  left join (
  select
    id,
    platform_currency_id,
    description,
    platforms
  from
    coingecko_asset_metadata
  ) metadata on assets.symbol = metadata.id
  where
    status = ''active''
    and source = ''%s'' 
    and name != ''''
    and market_cap is not null 
    and
    (
    SIMILARITY(lower(display_symbol), ''%s'') > 0.4
    OR SIMILARITY(lower(name), ''%s'') > 0.4
    OR starts_with(lower(name), ''%s'')
    OR lower(name) = ''%s''
    OR lower(display_symbol) = ''%s'' 
    )
  order by
  case when
  lower(name) =''%s''  or lower(display_symbol) = ''%s'' 
  THEN 1
  ELSE 0
  END DESC,
  market_cap desc),
 exatct_matches AS (
  select *
  from assets where exact_match = 1
  ),
  non_exatct_matches AS (
  select *
  from assets 
   order by
    %s %s
  LIMIT %s - (SELECT count(exact_match) FROM  assets where exact_match = 1  ) OFFSET %s
  )
SELECT symbol,
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
    volume_1d,
    status,
    market_cap_percent_change_1d,
    date_added,
    rank_number,
     platform_currency_id,
     description,
     platforms,
     full_count 
FROM exatct_matches
union all
Select symbol,
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
    volume_1d,
    status,
    market_cap_percent_change_1d,
    date_added,
    rank_number,
     platform_currency_id,
     description,
     platforms,
     full_count from non_exatct_matches',
  quote_ident('volume_1d'),
  search_term,
  search_term,
  source,
  search_term,
  search_term,
  search_term,
  search_term,
  search_term,
  search_term,
  search_term,
  sort_by,
  direction,
  lim,
  lim * page_num
) USING source,
direction,
sort_by,
lim,
page_num,
search_term;

END IF;

END $ BODY $;

/*
 Function: public.fuzzysearch_nftfundamentalslatest
 
 Description:
 This function performs a fuzzy search on the `nftdatalatest` table and returns a paginated list of NFT fundamentals. 
 It supports sorting and pagination, and can filter results based on a search term.
 
 Parameters:
 - lim (integer): The limit on the number of rows to return.
 - page_num (integer): The page number for pagination.
 - sort_by (text): The column name to sort the results by.
 - direction (text): The direction of sorting ('ASC' or 'DESC').
 - search_term (text): The term to search for in the `symbol` and `name` columns.
 
 Returns:
 - TABLE: A table containing the following columns:
 - id (text)
 - contract_address (text)
 - asset_platform_id (text)
 - name (text)
 - symbol (text)
 - display_symbol (text)
 - image (text)
 - description (text)
 - native_currency (text)
 - floor_price_usd (double precision)
 - market_cap_usd (double precision)
 - volume_24h_usd (double precision)
 - floor_price_native (double precision)
 - market_cap_native (double precision)
 - volume_24h_native (double precision)
 - floor_price_in_usd_24h_percentage_change (double precision)
 - volume_24h_percentage_change_usd (double precision)
 - number_of_unique_addresses (integer)
 - number_of_unique_addresses_24h_percentage_change (double precision)
 - slug (text)
 - total_supply (double precision)
 - last_updated (timestamp with time zone)
 - rank (integer)
 - full_count (bigint)
 
 Behavior:
 - If `search_term` is an empty string, the function returns all active NFTs sorted and paginated based on the provided parameters.
 - If `search_term` is not empty, the function performs a fuzzy search on the `symbol` and `name` columns using the `SIMILARITY` function and returns the filtered results.
 */
-- FUNCTION: public.fuzzysearch_nftfundamentalslatest(integer, integer, text, text, text)
-- DROP FUNCTION IF EXISTS public.fuzzysearch_nftfundamentalslatest(integer, integer, text, text, text);
CREATE
OR REPLACE FUNCTION public.fuzzysearch_nftfundamentalslatest(
  lim integer,
  page_num integer,
  sort_by text,
  direction text,
  search_term text
) RETURNS TABLE(
  id text,
  contract_address text,
  asset_platform_id text,
  name text,
  symbol text,
  display_symbol text,
  image text,
  description text,
  native_currency text,
  floor_price_usd double precision,
  market_cap_usd double precision,
  volume_24h_usd double precision,
  floor_price_native double precision,
  market_cap_native double precision,
  volume_24h_native double precision,
  floor_price_in_usd_24h_percentage_change double precision,
  volume_24h_percentage_change_usd double precision,
  number_of_unique_addresses integer,
  number_of_unique_addresses_24h_percentage_change double precision,
  slug text,
  total_supply double precision,
  last_updated timestamp with time zone,
  rank integer,
  full_count bigint
) LANGUAGE 'plpgsql' AS $ BODY $ BEGIN IF search_term = '' THEN RETURN QUERY EXECUTE format(
  '
            SELECT 
                t.id as id,
                contract_address,
                asset_platform_id,
                name,
                symbol,
                symbol as display_symbol,
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
                volume_24h_percentage_change_usd,
                number_of_unique_addresses,
                number_of_unique_addresses_24h_percentage_change,
                slug,
                total_supply,
                last_updated,
                rank rank,
                COUNT(t.id) OVER() AS full_count
            FROM
                public.nftdatalatest AS t
			left join (
        		select
        		id,
        			CAST(ROW_NUMBER() OVER (ORDER BY volume_24h_usd DESC, slug DESC) AS INT) AS rank
        			from
        			public.nftdatalatest
        			where
           				volume_24h_percentage_change_usd IS NOT NULL
          				and is_active = true
      				) b 
      				on t.id = b.id
            	WHERE
                t.volume_24h_percentage_change_usd IS NOT NULL
                AND t.is_active = true
            		ORDER BY %s %s NULLS LAST
            		LIMIT %s
            		OFFSET %s;',
  sort_by,
  direction,
  lim,
  lim * page_num
);

ELSE RETURN QUERY EXECUTE format(
  '
           with nfts as (SELECT 
                t.id as id,
                contract_address,
                asset_platform_id,
                name,
                symbol,
                symbol as display_symbol,
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
                volume_24h_percentage_change_usd,
                number_of_unique_addresses,
                number_of_unique_addresses_24h_percentage_change,
                slug,
                total_supply,
                last_updated,
                rank rank,
                COUNT(t.id) OVER() AS full_count,
				CASE WHEN lower(symbol) = ''%s'' or lower(name) = ''%s'' THEN 1
				ELSE 0
				END AS exact_match
            FROM
                public.nftdatalatest AS t
			left join (
        		select
        		id,
        			CAST(ROW_NUMBER() OVER (ORDER BY volume_24h_usd DESC, slug DESC) AS INT) AS rank
        			from
        			public.nftdatalatest
        			where
           				volume_24h_percentage_change_usd IS NOT NULL
          				and is_active = true
      				) b 
      				on t.id = b.id
            WHERE 
                (SIMILARITY(lower(t.symbol), ''%s'') > 0.4
                OR SIMILARITY(lower(t.name), ''%s'') > 0.5)
                AND t.is_active = true
			),
			 exact_matches AS (
				select *
				from nfts where exact_match = 1
  			),
			non_exact_matches AS (
				select *
					from nfts 
  	 			order by
  				%s %s
				LIMIT %s - (SELECT count(exact_match) FROM  nfts where exact_match = 1  ) OFFSET %s
  			)
			select 
				id,
                contract_address,
                asset_platform_id,
                name,
                symbol,
                symbol as display_symbol,
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
                volume_24h_percentage_change_usd,
                number_of_unique_addresses,
                number_of_unique_addresses_24h_percentage_change,
                slug,
                total_supply,
                last_updated,
                rank rank,
                full_count
				from exact_matches
			UNION ALL
			select 
				id,
                contract_address,
                asset_platform_id,
                name,
                symbol,
                symbol as display_symbol,
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
                volume_24h_percentage_change_usd,
                number_of_unique_addresses,
                number_of_unique_addresses_24h_percentage_change,
                slug,
                total_supply,
                last_updated,
                rank rank,
                full_count
				from non_exact_matches',
  search_term,
  search_term,
  search_term,
  search_term,
  sort_by,
  direction,
  lim,
  lim * page_num
);

END IF;

END;

$ BODY $;

/*
 FUNCTION: public.get_assets_in_category
 
 DESCRIPTION:
 This function retrieves a paginated list of assets within a specified category, sorted and filtered based on the provided parameters.
 
 PARAMETERS:
 - lim (integer): The maximum number of records to return.
 - page_num (integer): The page number for pagination.
 - sort_by (text): The column by which to sort the results.
 - direction (text): The direction of sorting (ASC or DESC).
 - search_term (text): The search term used to filter the assets by category.
 
 RETURNS:
 - TABLE (
 symbol (text),
 display_symbol (text),
 name (text),
 slug (text),
 logo (text),
 temporary_data_delay (boolean),
 price_24h (double precision),
 percentage_1h (double precision),
 percentage_24h (double precision),
 percentage_7d (double precision),
 change_value_24h (double precision),
 market_cap (double precision),
 volume_1d (double precision),
 status (text),
 market_cap_percent_change_1d (double precision),
 rank_number (bigint)
 ): A table containing the asset details.
 
 USAGE:
 This function can be used to fetch a list of assets within a specific category, with options for pagination, sorting, and filtering.
 */
CREATE
OR REPLACE FUNCTION public.get_assets_in_category(
  lim integer,
  page_num integer,
  sort_by text,
  direction text,
  search_term text
) RETURNS TABLE(
  symbol text,
  display_symbol text,
  name text,
  slug text,
  logo text,
  temporary_data_delay boolean,
  price_24h double precision,
  percentage_1h double precision,
  percentage_24h double precision,
  percentage_7d double precision,
  change_value_24h double precision,
  market_cap double precision,
  volume_1d double precision,
  status text,
  market_cap_percent_change_1d double precision,
  rank_number bigint
) LANGUAGE 'plpgsql' AS $ BODY $ BEGIN RETURN QUERY EXECUTE format(
  '
        SELECT 
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
            (nomics::json->>''volume_1d'')::float AS volume_1d,
			status,
            market_cap_percent_change_1d
        FROM
            fundamentalslatest
        WHERE
            symbol IN (
                SELECT 
                    json_data->>''id'' AS id  
                FROM 
                    categories_fundamentals,
                    jsonb_array_elements(markets::jsonb) AS json_data
                WHERE 
                    id = ''%s''
            )
        ORDER BY %s %s NULLS LAST
        LIMIT %s
        OFFSET %s;',
  search_term,
  sort_by,
  direction,
  lim,
  lim * page_num
) USING sort_by,
lim,
page_num,
direction,
search_term;

END;

$ BODY $;

/*
 FUNCTION: public.get_nfts_by_chain
 
 DESCRIPTION:
 This function retrieves a paginated list of NFTs (Non-Fungible Tokens) based on the specified blockchain platform.
 It allows sorting and searching through the NFTs and returns various details about each NFT.
 
 PARAMETERS:
 - lim (integer): The number of records to return per page.
 - page_num (integer): The page number to retrieve.
 - sort_by (text): The column name to sort the results by.
 - direction (text): The direction of sorting ('ASC' for ascending, 'DESC' for descending).
 - search_term (text): The blockchain platform ID to filter the NFTs by.
 
 RETURNS:
 - TABLE: A table containing the following columns:
 - id (text): The unique identifier of the NFT.
 - contract_address (text): The contract address of the NFT.
 - asset_platform_id (text): The blockchain platform ID.
 - name (text): The name of the NFT.
 - symbol (text): The symbol of the NFT.
 - display_symbol (text): The display symbol of the NFT.
 - image (text): The image URL of the NFT.
 - description (text): The description of the NFT.
 - native_currency (text): The native currency of the NFT.
 - floor_price_usd (double precision): The floor price of the NFT in USD.
 - market_cap_usd (double precision): The market capitalization of the NFT in USD.
 - volume_24h_usd (double precision): The 24-hour trading volume of the NFT in USD.
 - floor_price_native (double precision): The floor price of the NFT in its native currency.
 - market_cap_native (double precision): The market capitalization of the NFT in its native currency.
 - volume_24h_native (double precision): The 24-hour trading volume of the NFT in its native currency.
 - floor_price_in_usd_24h_percentage_change (double precision): The 24-hour percentage change in floor price in USD.
 - volume_24h_percentage_change_usd (double precision): The 24-hour percentage change in trading volume in USD.
 - number_of_unique_addresses (integer): The number of unique addresses holding the NFT.
 - number_of_unique_addresses_24h_percentage_change (double precision): The 24-hour percentage change in the number of unique addresses.
 - slug (text): The slug of the NFT.
 - total_supply (double precision): The total supply of the NFT.
 - last_updated (timestamp with time zone): The last updated timestamp of the NFT data.
 - rank (integer): The rank of the NFT based on trading volume and slug.
 - full_count (bigint): The total count of NFTs matching the criteria.
 
 NOTES:
 - The function uses dynamic SQL to construct the query based on the input parameters.
 - The results are ordered by the specified column and direction, with NULL values placed last.
 - The function supports pagination through the 'lim' and 'page_num' parameters.
 */
CREATE
OR REPLACE FUNCTION public.get_nfts_by_chain(
  lim integer,
  page_num integer,
  sort_by text,
  direction text,
  search_term text
) RETURNS TABLE(
  id text,
  contract_address text,
  asset_platform_id text,
  name text,
  symbol text,
  display_symbol text,
  image text,
  description text,
  native_currency text,
  floor_price_usd double precision,
  market_cap_usd double precision,
  volume_24h_usd double precision,
  floor_price_native double precision,
  market_cap_native double precision,
  volume_24h_native double precision,
  floor_price_in_usd_24h_percentage_change double precision,
  volume_24h_percentage_change_usd double precision,
  number_of_unique_addresses integer,
  number_of_unique_addresses_24h_percentage_change double precision,
  slug text,
  total_supply double precision,
  last_updated timestamp with time zone,
  rank integer,
  full_count bigint
) LANGUAGE 'plpgsql' AS $ BODY $ BEGIN RETURN QUERY EXECUTE format(
  '
            SELECT 
                t.id,
                t.contract_address,
                t.asset_platform_id,
                t.name,
                t.symbol,
                t.symbol as display_symbol,
                t.image,
                t.description,
                t.native_currency,
                t.floor_price_usd,
                t.market_cap_usd,
                t.volume_24h_usd,
                t.floor_price_native,
                t.market_cap_native,
                t.volume_24h_native,
                t.floor_price_in_usd_24h_percentage_change,
                t.volume_24h_percentage_change_usd,
                t.number_of_unique_addresses,
                t.number_of_unique_addresses_24h_percentage_change,
                t.slug,
                t.total_supply,
                t.last_updated,
                CAST(ROW_NUMBER() OVER (ORDER BY t.volume_24h_usd DESC, t.slug DESC) AS INT) AS rank,
                COUNT(t.id) OVER() AS full_count
            FROM
                public.nftdatalatest AS t
            WHERE 
                asset_platform_id = ''%s''
                AND t.is_active = true
            ORDER BY t.%s %s NULLS LAST
            LIMIT %s
            OFFSET %s;',
  search_term,
  sort_by,
  direction,
  lim,
  lim * page_num
);

END;

$ BODY $;

Create Table custom_categories (
  category_name TEXT,
  category_slug TEXT,
  category_path TEXT,
  category_type TEXT,
  is_highlighted boolean,
  inactive boolean,
  category_fields JSON,
  last_updated TIMESTAMPTZ DEFAULT Now(),
  PRIMARY KEY (category_name)
)
/*
 FUNCTION: public.fuzzysearch_fundamentals__category
 
 DESCRIPTION:
 This function performs a fuzzy search on the 'fundamentalslatest' table based on the provided search term and category ID.
 It returns a paginated and sorted list of assets with their associated metadata.
 
 PARAMETERS:
 - lim (integer): The limit on the number of records to return.
 - page_num (integer): The page number for pagination.
 - sort_by (text): The column by which to sort the results.
 - direction (text): The direction of sorting ('ASC' or 'DESC').
 - search_term (text): The term to search for in the 'display_symbol' and 'name' columns.
 - category_id (text): The ID of the category to filter the results.
 
 RETURNS:
 - TABLE (
 symbol text,
 display_symbol text,
 name text,
 slug text,
 logo text,
 temporary_data_delay boolean,
 price_24h double precision,
 percentage_1h double precision,
 percentage_24h double precision,
 percentage_7d double precision,
 change_value_24h double precision,
 market_cap double precision,
 volume_1d double precision,
 status text,
 market_cap_percent_change_1d double precision,
 rank_number bigint,
 date_added timestamp with time zone,
 platform_currency_id text,
 description text,
 platforms json,
 full_count bigint
 )
 
 NOTES:
 - If the search term is empty, the function performs a simple query without fuzzy matching.
 - If the search term is provided, the function uses the SIMILARITY function to perform fuzzy matching on the 'display_symbol' and 'name' columns.
 - The function joins the results with the 'coingecko_asset_metadata' table to include additional metadata.
 - The results are ordered by the specified column and direction, with NULL values appearing last.
 - The function supports pagination by using the 'lim' and 'page_num' parameters.
 */
CREATE
OR REPLACE FUNCTION public.fuzzysearch_fundamentals__category(
  lim integer,
  page_num integer,
  sort_by text,
  direction text,
  search_term text,
  category_id text
) RETURNS TABLE(
  symbol text,
  display_symbol text,
  name text,
  slug text,
  logo text,
  temporary_data_delay boolean,
  price_24h double precision,
  percentage_1h double precision,
  percentage_24h double precision,
  percentage_7d double precision,
  change_value_24h double precision,
  market_cap double precision,
  volume_1d double precision,
  status text,
  market_cap_percent_change_1d double precision,
  rank_number bigint,
  date_added timestamp with time zone,
  platform_currency_id text,
  description text,
  platforms json,
  full_count bigint
) LANGUAGE 'plpgsql' COST 100 VOLATILE PARALLEL UNSAFE ROWS 1000 AS $ BODY $ BEGIN IF search_term = '' THEN RETURN QUERY EXECUTE format(
  '
            SELECT 
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
                volume_1d,
                status,
                market_cap_percent_change_1d,
                rank_number,
                date_added,
                coalesce(platform_currency_id, '''') as platform_currency_id,
                coalesce(description, '''') as description,
                coalesce(platforms, ''{}'') as platforms,
				full_count
            FROM (
                SELECT 
                    symbol,
                    display_symbol,						  
                    name,
          unt
                FROM fundamentalslatest
                WHERE symbol IN (
                    SELECT json_data->>''id'' AS id  
                    FROM categories_fundamentals,
                    jsonb_array_elements(markets::jsonb) AS json_data
                    WHERE id = ''%s''
                )
                ORDER BY %s %s NULLS LAST
                LIMIT %s
                OFFSET %s
            ) AS assets
            LEFT JOIN (
                SELECT
                    id,
                    platform_currency_id,
                    description,
                    platforms
                FROM coingecko_asset_metadata  
            ) metadata ON assets.symbol = metadata.id',
  category_id,
  sort_by,
  direction,
  lim,
  lim * page_num,
  sort_by,
  direction,
  lim,
  lim * page_num
);

ELSE RETURN QUERY EXECUTE format(
  '
            SELECT 
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
                volume_1d,
                status,
                market_cap_percent_change_1d,
                rank_number,
                date_added,
                coalesce(platform_currency_id, '''') as platform_currency_id,
                coalesce(description, '''') as description,
                coalesce(platforms, ''{}'') as platforms,
				full_count
            FROM (
                SELECT 
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
					(nomics::json->>''volume_1d'')::float AS volume_1d,
                    status,
                    market_cap_percent_change_1d,
                    RANK() OVER (
                        PARTITION BY status
                        ORDER BY market_cap DESC
                    ) rank_number,
                    date_added,
					count(symbol) OVER() AS full_count
                FROM fundamentalslatest
                WHERE symbol IN (
                    SELECT json_data->>''id'' AS id  
                    FROM categories_fundamentals,
                    jsonb_array_elements(markets::jsonb) AS json_data
                    WHERE id = ''%s''
                )
                AND (SIMILARITY(lower(display_symbol), ''%s'') > 0.2
                OR SIMILARITY(lower(name), ''%s'') > 0.2)
                ORDER BY %s %s NULLS LAST
                LIMIT %s
                OFFSET %s
            ) AS assets
            LEFT JOIN (
                SELECT
                    id,
                    platform_currency_id,
                    description,
                    platforms
                FROM coingecko_asset_metadata  
            ) metadata ON assets.symbol = metadata.id',
  category_id,
  search_term,
  search_term,
  sort_by,
  direction,
  lim,
  lim * page_num,
  sort_by,
  direction,
  lim,
  lim * page_num
);

END IF;

END;

$ BODY $;

/*
 FUNCTION: public.fuzzysearch_nfts_by_chain
 
 DESCRIPTION:
 This function performs a fuzzy search on NFTs by chain and returns a paginated, sorted list of NFTs. 
 It supports both exact and fuzzy search based on the provided search term. The function also calculates 
 the rank and full count of the results.
 
 PARAMETERS:
 - lim (integer): The limit on the number of records to return.
 - page_num (integer): The page number for pagination.
 - sort_by (text): The column name to sort the results by.
 - direction (text): The direction of sorting (ASC or DESC).
 - search_term (text): The term to search for in the NFT name or symbol.
 - chain_id (text): The ID of the blockchain to filter the NFTs by.
 
 RETURNS:
 - TABLE: A table containing the following columns:
 - id (text): The ID of the NFT.
 - contract_address (text): The contract address of the NFT.
 - asset_platform_id (text): The ID of the asset platform.
 - name (text): The name of the NFT.
 - symbol (text): The symbol of the NFT.
 - display_symbol (text): The display symbol of the NFT.
 - image (text): The image URL of the NFT.
 - description (text): The description of the NFT.
 - native_currency (text): The native currency of the NFT.
 - floor_price_usd (double precision): The floor price of the NFT in USD.
 - market_cap_usd (double precision): The market cap of the NFT in USD.
 - volume_24h_usd (double precision): The 24-hour trading volume of the NFT in USD.
 - floor_price_native (double precision): The floor price of the NFT in its native currency.
 - market_cap_native (double precision): The market cap of the NFT in its native currency.
 - volume_24h_native (double precision): The 24-hour trading volume of the NFT in its native currency.
 - floor_price_in_usd_24h_percentage_change (double precision): The 24-hour percentage change in floor price in USD.
 - volume_24h_percentage_change_usd (double precision): The 24-hour percentage change in trading volume in USD.
 - number_of_unique_addresses (integer): The number of unique addresses holding the NFT.
 - number_of_unique_addresses_24h_percentage_change (double precision): The 24-hour percentage change in the number of unique addresses.
 - slug (text): The slug of the NFT.
 - total_supply (double precision): The total supply of the NFT.
 - last_updated (timestamp with time zone): The last updated timestamp of the NFT data.
 - rank (integer): The rank of the NFT based on trading volume and slug.
 - full_count (bigint): The total count of NFTs matching the search criteria.
 
 NOTES:
 - If the search_term is an empty string, the function performs an exact search based on the chain_id.
 - If the search_term is not empty, the function performs a fuzzy search using the SIMILARITY function.
 - The results are ordered by the specified sort_by column and direction, with NULL values appearing last.
 - The function uses window functions to calculate the rank and full count of the results.
 */
CREATE
OR REPLACE FUNCTION public.fuzzysearch_nfts_by_chain(
  lim integer,
  page_num integer,
  sort_by text,
  direction text,
  search_term text,
  chain_id text
) RETURNS TABLE(
  id text,
  contract_address text,
  asset_platform_id text,
  name text,
  symbol text,
  display_symbol text,
  image text,
  description text,
  native_currency text,
  floor_price_usd double precision,
  market_cap_usd double precision,
  volume_24h_usd double precision,
  floor_price_native double precision,
  market_cap_native double precision,
  volume_24h_native double precision,
  floor_price_in_usd_24h_percentage_change double precision,
  volume_24h_percentage_change_usd double precision,
  number_of_unique_addresses integer,
  number_of_unique_addresses_24h_percentage_change double precision,
  slug text,
  total_supply double precision,
  last_updated timestamp with time zone,
  rank integer,
  full_count bigint
) LANGUAGE 'plpgsql' AS $ BODY $ BEGIN IF search_term = '' THEN RETURN QUERY EXECUTE format(
  '
      SELECT 
        t.id,
        contract_address,
        asset_platform_id,
        name,
        symbol,
        symbol as display_symbol,
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
        volume_24h_percentage_change_usd,
        number_of_unique_addresses,
        number_of_unique_addresses_24h_percentage_change,
        slug,
        total_supply,
        last_updated,
        rank,
        COUNT(t.id) OVER() AS full_count
      FROM
        public.nftdatalatest AS t
      left join (
        select
        id,
        CAST(ROW_NUMBER() OVER (ORDER BY volume_24h_usd DESC, slug DESC) AS INT) AS rank
        from
        public.nftdatalatest
        where
           volume_24h_percentage_change_usd IS NOT NULL
          and is_active = true
      ) b 
      on t.id = b.id
      WHERE 
        asset_platform_id = ''%s''
        AND t.is_active = true
      ORDER BY %s %s NULLS LAST
      LIMIT %s
      OFFSET %s;',
  chain_id,
  sort_by,
  direction,
  lim,
  lim * page_num
);

ELSE RETURN QUERY EXECUTE format(
  '
      SELECT 
        t.id,
        contract_address,
        asset_platform_id,
        name,
        symbol,
        symbol as display_symbol,
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
        volume_24h_percentage_change_usd,
        number_of_unique_addresses,
        number_of_unique_addresses_24h_percentage_change,
        slug,
        total_supply,
        last_updated,
        rank,
        COUNT(t.id) OVER() AS full_count
      FROM
        public.nftdatalatest AS t
      join (
        select
        id,
        CAST(ROW_NUMBER() OVER (ORDER BY volume_24h_usd DESC, slug DESC) AS INT) AS rank
        from
        public.nftdatalatest
        where
           volume_24h_percentage_change_usd IS NOT NULL
          and is_active = true
      ) b 
      on t.id = b.id
      WHERE 
        t.asset_platform_id = ''%s''
        AND (
        
        SIMILARITY(lower(t.symbol), ''%s'') > 0.2
        OR SIMILARITY(lower(t.name), ''%s'') > 0.3)
        AND t.is_active = true
      ORDER BY 
      CASE 
      WHEN lower(t.name) = ''%s'' OR lower(t.symbol) = ''%s'' THEN 1
      ELSE 0 
      END DESC,
      %s %s NULLS LAST
      LIMIT %s
      OFFSET %s;',
  chain_id,
  search_term,
  search_term,
  search_term,
  search_term,
  sort_by,
  direction,
  lim,
  lim * page_num
);

END IF;

END;

$ BODY $;

/*
 Function: GetFundamentalsV2
 
 Description:
 This function retrieves the latest fundamental data for a given symbol and column from the `fundamentalslatest` table. It returns a table with various financial metrics and metadata.
 
 Parameters:
 - valueIN (text): The symbol of the asset to retrieve data for.
 - columnIN (text): The column to filter the data by.
 
 Returns:
 - TABLE:
 - symbol (text): The symbol of the asset.
 - name (text): The name of the asset.
 - slug (text): The slug of the asset.
 - logo (text): The logo URL of the asset.
 - float_type (text): The float type of the asset.
 - display_symbol (text): The display symbol of the asset.
 - original_symbol (text): The original symbol of the asset.
 - source (text): The source of the data.
 - temporary_data_delay (boolean): Indicates if there is a temporary data delay.
 - number_of_active_market_pairs (integer): The number of active market pairs.
 - high_24h (double precision): The highest price in the last 24 hours.
 - low_24h (double precision): The lowest price in the last 24 hours.
 - high_7d (double precision): The highest price in the last 7 days.
 - low_7d (double precision): The lowest price in the last 7 days.
 - high_30d (double precision): The highest price in the last 30 days.
 - low_30d (double precision): The lowest price in the last 30 days.
 - high_1y (double precision): The highest price in the last year.
 - low_1y (double precision): The lowest price in the last year.
 - high_ytd (double precision): The highest price year-to-date.
 - low_ytd (double precision): The lowest price year-to-date.
 - price_24h (double precision): The price 24 hours ago.
 - price_7d (double precision): The price 7 days ago.
 - price_30d (double precision): The price 30 days ago.
 - price_1y (double precision): The price 1 year ago.
 - price_ytd (double precision): The price year-to-date.
 - percentage_1h (double precision): The percentage change in the last hour.
 - percentage_24h (double precision): The percentage change in the last 24 hours.
 - percentage_7d (double precision): The percentage change in the last 7 days.
 - percentage_30d (double precision): The percentage change in the last 30 days.
 - percentage_1y (double precision): The percentage change in the last year.
 - percentage_ytd (double precision): The percentage change year-to-date.
 - market_cap (double precision): The market capitalization.
 - market_cap_percent_change_1d (double precision): The market cap percentage change in the last day.
 - market_cap_percent_change_7d (double precision): The market cap percentage change in the last 7 days.
 - market_cap_percent_change_30d (double precision): The market cap percentage change in the last 30 days.
 - market_cap_percent_change_1y (double precision): The market cap percentage change in the last year.
 - market_cap_percent_change_ytd (double precision): The market cap percentage change year-to-date.
 - circulating_supply (numeric): The circulating supply of the asset.
 - supply (numeric): The total supply of the asset.
 - all_time_low (double precision): The all-time low price.
 - all_time_high (double precision): The all-time high price.
 - date (timestamptz): The date of the data.
 - change_value_24h (double precision): The change in value in the last 24 hours.
 - listed_exchange (varchar(100)[]): The list of exchanges where the asset is listed.
 - market_pairs (json): The market pairs data in JSON format.
 - exchanges (json): The exchanges data in JSON format.
 - nomics (json): The Nomics data in JSON format.
 - forbes (json): The Forbes data in JSON format.
 - last_updated (timestamptz): The last updated timestamp.
 - platforms (json): The platforms data in JSON format, defaulting to an empty JSON object if null.
 
 Usage:
 CALL public.GetFundamentalsV2('BTC', 'symbol');
 */
CREATE
OR REPLACE FUNCTION public.GetFundamentalsV2(valueIN text, columnIN text) RETURNS TABLE(
  symbol text,
  name text,
  slug text,
  logo text,
  float_type text,
  display_symbol text,
  original_symbol text,
  source text,
  temporary_data_delay boolean,
  number_of_active_market_pairs INTEGER,
  high_24h double precision,
  low_24h double precision,
  high_7d double precision,
  low_7d double precision,
  high_30d double precision,
  low_30d double precision,
  high_1y double precision,
  low_1y double precision,
  high_ytd double precision,
  low_ytd double precision,
  price_24h double precision,
  price_7d double precision,
  price_30d double precision,
  price_1y double precision,
  price_ytd double precision,
  percentage_1h double precision,
  percentage_24h double precision,
  percentage_7d double precision,
  percentage_30d double precision,
  percentage_1y double precision,
  percentage_ytd double precision,
  market_cap double precision,
  market_cap_percent_change_1d double precision,
  market_cap_percent_change_7d double precision,
  market_cap_percent_change_30d double precision,
  market_cap_percent_change_1y double precision,
  market_cap_percent_change_ytd double precision,
  circulating_supply NUMERIC,
  supply NUMERIC,
  all_time_low double precision,
  all_time_high double precision,
  date TIMESTAMPTZ,
  change_value_24h double precision,
  listed_exchange VARCHAR(100) [],
  market_pairs JSON,
  exchanges JSON,
  nomics JSON,
  forbes JSON,
  last_updated TIMESTAMPTZ,
  platforms JSON
) LANGUAGE 'plpgsql' AS $ BODY $ BEGIN RETURN QUERY EXECUTE format(
  '
	 SELECT 
      symbol,
      name,
      slug,
      logo,
      float_type,
      display_symbol,
      original_symbol,
      source,
      temporary_data_delay,
      number_of_active_market_pairs,
      high_24h,
      low_24h,
      high_7d,
      low_7d,
      high_30d,
      low_30d,
      high_1y,
      low_1y,
      high_ytd,
      low_ytd,
      price_24h,
      price_7d,
      price_30d,
      price_1y,
      price_ytd,
      percentage_1h,
      percentage_24h,
      percentage_7d,
      percentage_30d,
      percentage_1y,
      percentage_ytd,
      market_cap,
      market_cap_percent_change_1d,
      market_cap_percent_change_7d,
      market_cap_percent_change_30d,
      market_cap_percent_change_1y,
      market_cap_percent_change_ytd,
      circulating_supply,
      supply,
      all_time_low,
      all_time_high,
      date,
      change_value_24h,
      listed_exchange,
      market_pairs,
      exchanges,
      nomics,
      forbes,
      last_updated,
      COALESCE(platforms, ''{}'') as platforms
from
  (
		 SELECT 
			 symbol,
			 name,
			 slug,
			 logo,
			 float_type,
			 display_symbol,
			 original_symbol,
			 source,
			 temporary_data_delay,
			 number_of_active_market_pairs,
			 high_24h,
			 low_24h,
			 high_7d,
			 low_7d,
			 high_30d,
			 low_30d,
			 high_1y,
			 low_1y,
			 high_ytd,
			 low_ytd,
			 price_24h,
			 price_7d,
			 price_30d,
			 price_1y,
			 price_ytd,
			 percentage_1h,
			 percentage_24h,
			 percentage_7d,
			 percentage_30d,
			 percentage_1y,
			 percentage_ytd,
			 market_cap,
			 market_cap_percent_change_1d,
			 market_cap_percent_change_7d,
			 market_cap_percent_change_30d,
			 market_cap_percent_change_1y,
			 market_cap_percent_change_ytd,
			 circulating_supply,
			 supply,
			 all_time_low,
			 all_time_high,
			 date,
			 change_value_24h,
			 listed_exchange,
			 market_pairs,
			 exchanges,
			 nomics,
			 forbes,
			 last_updated
		 FROM 
			 fundamentalslatest
		 where %s = ''%s''
		 ORDER BY 
			 market_cap desc
		 limit 1
  ) assets
  left join (
    select
      id,
      platforms
    from
      coingecko_asset_metadata
  ) metadata on assets.symbol = metadata.id
',
  columnIN,
  valueIN,
) USING columnIN,
valueIN;

END $ BODY $;

/*
 Function: fuzzysearch_categories_fundamentals
 
 Description:
 This function performs a fuzzy search on the `categories_fundamentals` table and returns a paginated result set based on the provided search term, sorting, and pagination parameters. If the search term is empty, it returns all active categories. Otherwise, it performs a fuzzy search on the category names.
 
 Parameters:
 - lim (integer): The number of records to return.
 - page_num (integer): The page number for pagination.
 - sort_by (text): The column name to sort the results by.
 - direction (text): The direction of sorting (ASC or DESC).
 - search_term (text): The term to search for in the category names.
 
 Returns:
 - TABLE: A table containing the following columns:
 - id (text): The ID of the category.
 - name (text): The name of the category.
 - total_tokens (integer): The total number of tokens in the category.
 - average_percentage_24h (double precision): The average percentage change in the last 24 hours.
 - volume_24h (double precision): The volume in the last 24 hours.
 - price_24h (double precision): The price in the last 24 hours.
 - average_price (double precision): The average price.
 - market_cap (double precision): The market capitalization.
 - market_cap_percentage_24h (double precision): The market cap percentage change in the last 24 hours.
 - top_gainers (json): The top gainers in the category.
 - top_movers (json): The top movers in the category.
 - forbesname (text): The Forbes name of the category.
 - slug (text): The slug of the category.
 - last_updated (timestamp with time zone): The last updated timestamp.
 - is_highlighted (boolean): Indicates if the category is highlighted.
 
 Usage:
 - To retrieve all active categories with pagination and sorting.
 - To perform a fuzzy search on category names with pagination and sorting.
 */
CREATE
OR REPLACE FUNCTION public.fuzzysearch_categories_fundamentals(
  lim integer,
  page_num integer,
  sort_by text,
  direction text,
  search_term text
) RETURNS TABLE(
  id text,
  name text,
  total_tokens integer,
  average_percentage_24h double precision,
  volume_24h double precision,
  price_24h double precision,
  average_price double precision,
  market_cap double precision,
  market_cap_percentage_24h double precision,
  top_gainers json,
  top_movers json,
  forbesname text,
  slug text,
  last_updated timestamp with time zone,
  is_highlighted boolean
) LANGUAGE 'plpgsql' AS $ BODY $ BEGIN IF search_term = '' THEN RETURN QUERY EXECUTE format(
  'SELECT id,
       NAME,
       total_tokens,
       average_percentage_24h,
       volume_24h,
       price_24h,
       average_price,
       market_cap,
       market_cap_percentage_24h,
       top_gainers,
       top_movers,
       "forbesName" AS forbesname,
       slug,
       last_updated,
       is_highlighted
FROM   PUBLIC.categories_fundamentals
WHERE  COALESCE(inactive, false) = false
AND    top_gainers::jsonb != ''null''::jsonb
AND    top_movers::jsonb != ''[]''::jsonb
ORDER BY %s %s
LIMIT %s
OFFSET %s
;',
  sort_by,
  direction,
  lim,
  lim * page_num
) USING direction,
sort_by,
lim,
page_num;

ELSE RETURN QUERY EXECUTE format(
  '
            with categories as (SELECT id,
              NAME,
			  inactive,
              total_tokens,
              average_percentage_24h,
              volume_24h,
              price_24h,
              average_price,
              market_cap,
              market_cap_percentage_24h,
              top_gainers,
              top_movers,
              "forbesName" AS forbesname,
              slug,
              last_updated,
              is_highlighted,
              CASE
                     WHEN Lower(NAME) = ''%s'' THEN 1
                     ELSE 0
              END AS exact_match
       FROM   PUBLIC.categories_fundamentals
       WHERE  COALESCE(inactive, false) = false
       AND    top_gainers::jsonb != ''null''::jsonb
       AND    top_movers::jsonb != ''[]''::jsonb
       AND    (
                     similarity(lower(NAME), ''%s'') > 0.3
              OR     starts_with(lower(NAME), ''%s'')
              )
), exact_matches AS (
         SELECT   *
         FROM     categories
         WHERE    exact_match = 1
         ORDER BY %s %s
), non_exact_matches AS (
         SELECT   *
         FROM     categories
         WHERE    exact_match = 0
         ORDER BY %s %s
         LIMIT %s - (
                         SELECT count(exact_match)
                         FROM   categories
                         WHERE  exact_match = 1
                     )
         OFFSET %s
)
SELECT 
		id,
 	   NAME,
       total_tokens,
       average_percentage_24h,
       volume_24h,
       price_24h,
       average_price,
       market_cap,
       market_cap_percentage_24h,
       top_gainers,
       top_movers,
       forbesname,
       slug,
       last_updated,
       is_highlighted
FROM   exact_matches
UNION ALL
SELECT
id,
 NAME,
       total_tokens,
       average_percentage_24h,
       volume_24h,
       price_24h,
       average_price,
       market_cap,
       market_cap_percentage_24h,
       top_gainers,
       top_movers,
       forbesname,
       slug,
       last_updated,
       is_highlighted
FROM   non_exact_matches;',
  search_term,
  search_term,
  search_term,
  sort_by,
  direction,
  sort_by,
  direction,
  lim,
  lim * page_num
) USING direction,
sort_by,
lim,
page_num,
search_term;

END IF;

END $ BODY $;

create
or replace FUNCTION getTopExchangesV2() RETURNS Table(
  forbes_id text,
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
) as $ $ DECLARE lim int := 5;

BEGIN RETURN QUERY EXECUTE format(
  '
							
SELECT FORBES_ID,
	SYMBOL,
	EXCHANGE_NAME,
	EXCHANGE_YEAR,
	DESCRIPTION,
	LOCATION,
	LOGO_URL,
	WEBSITE_URL,
	TWITTER_URL,
	FACEBOOK_URL,
	YOUTUBE_URL,
	LINKEDIN_URL,
	REDDIT_URL,
	CHAT_URL,
	SLACK_URL,
	TELEGRAM_URL,
	BLOG_URL,
	CENTRALIZED,
	DECENTRALIZED,
	HAS_TRADING_INCENTIVE,
	TRUST_SCORE,
	TRUST_SCORE_RANK,
	TRADE_VOLUME_24H_BTC,
	TRADE_VOLUME_24H_BTC_NORMALIZED,
	LAST_UPDATED
FROM
	(SELECT ID AS SYMBOL,
			NAME AS EXCHANGE_NAME,
			YEAR AS EXCHANGE_YEAR,
			DESCRIPTION,
			LOCATION,
			LOGO_URL,
			WEBSITE_URL,
			TWITTER_URL,
			FACEBOOK_URL,
			YOUTUBE_URL,
			LINKEDIN_URL,
			REDDIT_URL,
			CHAT_URL,
			SLACK_URL,
			TELEGRAM_URL,
			BLOG_URL,
			CENTRALIZED,
			DECENTRALIZED,
			HAS_TRADING_INCENTIVE,
			TRUST_SCORE,
			TRUST_SCORE_RANK,
			TRADE_VOLUME_24H_BTC,
			TRADE_VOLUME_24H_BTC_NORMALIZED,
			LAST_UPDATED
		FROM PUBLIC.COINGECKO_EXCHANGE_METADATA
		WHERE TRUST_SCORE IS NOT NULL
		ORDER BY TRUST_SCORE DESC
	LIMIT %s) EXCHNAGES
LEFT JOIN
	(SELECT FORBES_ID,
			COINGECKO_ID
		FROM PUBLIC.FORBES_EXCHANGES) FORBES_EXCHANGES 
ON FORBES_EXCHANGES.COINGECKO_ID = EXCHNAGES.SYMBOL',
  lim
) USING lim;

END;

$ $ Language plpgsql;

ALTER Table
  'fundamentalslatest'
ADD
  COLUMN forebs_id text;