CREATE TABLE coingecko_assets
(
	id text,
	symbol text,
    name text,
	platforms text,
    last_updated TIMESTAMPTZ DEFAULT (Now()),
	primary key (id)
);

CREATE TABLE coingecko_asset_metadata
(
	id text,
	original_symbol text,
	description text,
	name text,
	website_url text,
	logo_url text,
	blog_url text,
	slack_url text,
	discord_url text,
	facebook_url text,
	github_url text,
	bitbucket_url text,
	medium_url text,
	reddit_url text,
	telegram_url text,
	twitter_url text,
	youtube_url text,
	whitepaper_url text,
	blockexplorer_url text,
	bitcointalk_url text,
	platform_currency_id text,
	platform_contract_address text,
	ico_start_date TIMESTAMPTZ,
	ico_end_date TIMESTAMPTZ,
	ico_total_raised text,
	ico_total_raised_currency text,
	alexa_rank integer,
	facebook_likes integer,
	twitter_followers integer,
	reddit_average_posts_48h float,
	reddit_average_comments_48h float,
	reddit_subscribers integer,
	reddit_accounts_active_48h integer,
	telegram_channel_user_count integer,
	repo_forks integer,
	repo_stars integer,
	repo_subscribers integer,
	repo_total_issues integer,
	repo_closed_issues integer,
	repo_pull_requests_merged integer,
	repo_pull_request_contributors integer,
	repo_code_additions_4_weeks integer,
	repo_code_deletions_4_weeks integer,
	repo_commit_count_4_weeks integer,
	genesis_date DATE,
	last_updated TIMESTAMPTZ DEFAULT (Now()),
	primary key (id)
);


CREATE TABLE coingecko_exchanges
(
	id text,
	name text,
	primary key(id)
);

CREATE TABLE coingecko_exchanges_tickers
(
	name text,
	tickers json,
	primary key(name)
);


CREATE TABLE coingecko_exchange_metadata
(
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
	last_updated TIMESTAMPTZ DEFAULT ( Now()),
	primary key (id)
);

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
    last_updated TIMESTAMPTZ DEFAULT Now()
);

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
    PRIMARY KEY (symbol)
);

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