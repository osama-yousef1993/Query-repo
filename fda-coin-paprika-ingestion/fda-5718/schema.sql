/*
 - Create a table named "coinpaprika_assets" to store information about cryptocurrency assets that will be come from CoinPaprika client.
 -- The "id" column is a text field and serves as the primary key to uniquely identify each asset (e.g., btc-bitcoin, xrp-xrp).
 -- The "name" column is a text field to store the name of the cryptocurrency asset (e.g., Bitcoin, XRP).
 -- The "symbol" column is a text field to store the symbol of the cryptocurrency asset (e.g., BTC, XRP).
 -- The "rank" column is an integer field representing the rank of the asset (e.g., based on market capitalization).
 -- The "is_new" column is a boolean field indicating whether the asset is newly listed.
 -- The "is_active" column is a boolean field indicating whether the asset is active.
 -- The "last_updated" column is a timestamp field to record the last update time, defaulting to the current timestamp.
 
 */
CREATE TABLE "coinpaprika_assets" (
  "id" text PRIMARY KEY,
  "name" text,
  "symbol" text,
  "rank" integer,
  "is_new" bool,
  "is_active" bool,
  "last_updated" timestamp DEFAULT 'Now()'
);

-- Create an index on the "id" column to optimize queries filtering by "id".
CREATE INDEX ON "coinpaprika_assets" ("id");

-- Create an index on the "name" column to optimize queries filtering or searching by "name".
CREATE INDEX ON "coinpaprika_assets" ("name");

-- Create an index on the "symbol" column to optimize queries filtering or searching by "symbol".
CREATE INDEX ON "coinpaprika_assets" ("symbol");

/*
 
 - Create a table named "coinpaprika_asset_metadata" to store detailed metadata about cryptocurrency assets.
 -- The "id" column is a text field and serves as the primary key to uniquely identify each asset (e.g., btc-bitcoin, xrp-xrp).
 -- The "name" column is a text field to store the name of the cryptocurrency asset (e.g., Bitcoin, XRP).
 -- The "symbol" column is a text field to store the symbol of the cryptocurrency asset (e.g., BTC, XRP).
 -- The "rank" column is an integer field representing the rank of the asset (e.g., based on market capitalization).
 -- The "is_new" column is a boolean field indicating whether the asset is newly listed.
 -- The "is_active" column is a boolean field indicating whether the asset is active.
 -- The "logo" column is a text field to store the URL or path to the asset's logo.
 -- The "tags" column is a JSON field to store related tags or categories for the asset.
 -- The "team" column is a JSON field to store information about the team behind the asset.
 -- The "parent" column is a JSON field to store data about the parent cryptocurrency (if applicable).
 -- The "description" column is a text field to provide a detailed description of the asset.
 -- The "message" column is a text field for any additional messages or notes related to the asset.
 -- The "open_source" column is a boolean field indicating whether the asset is open source.
 -- The "started_at" column is a timestamp field to record the date when the asset was launched.
 -- The "development_status" column is a text field to describe the development status of the asset.
 -- The "hardware_wallet" column is a boolean field indicating whether the asset supports hardware wallets.
 -- The "proof_type" column is a text field to specify the consensus or proof type (e.g., PoW, PoS).
 -- The "org_structure" column is a text field to describe the organizational structure of the asset.
 -- The "hash_algorithm" column is a text field to specify the hash algorithm used by the asset.
 -- The "links" column is a JSON field to store related links (e.g., website, social media, etc.).
 -- The "whitepaper" column is a JSON field to store information about the asset's whitepaper.
 -- The "first_data_at" column is a timestamp field to record the earliest available data for the asset.
 -- The "last_updated" column is a timestamp field to record the last update time, defaulting to the current timestamp.
 */
CREATE TABLE "coinpaprika_asset_metadata" (
  "id" text PRIMARY KEY,
  "coinpaprika_id" text,
  "name" text,
  "symbol" text,
  "rank" integer,
  "is_new" bool,
  "is_active" bool,
  "logo" text,
  "tags" JSON DEFAULT '[{}]',
  "team" JSON DEFAULT '[{}]',
  "parent" JSON DEFAULT '{}',
  "description" text,
  "message" text,
  "open_source" bool,
  "started_at" timestamp,
  "development_status" text,
  "hardware_wallet" bool,
  "proof_type" text,
  "org_structure" text,
  "hash_algorithm" text,
  "links" JSON DEFAULT '{}',
  "whitepaper" JSON DEFAULT '{}',
  "first_data_at" timestamp,
  "last_updated" timestamp DEFAULT 'Now()'
);

-- Create an index on the "id" column to optimize queries filtering by "id".
CREATE INDEX ON "coinpaprika_asset_metadata" ("id");

-- Create an index on the "name" column to optimize queries filtering or searching by "name".
CREATE INDEX ON "coinpaprika_asset_metadata" ("name");

-- Create an index on the "symbol" column to optimize queries filtering or searching by "symbol".
CREATE INDEX ON "coinpaprika_asset_metadata" ("symbol");

-- Add a foreign key constraint on the "coinpaprika_id" column in the "coinpaprika_asset_metadata" table to ensure referential integrity
ALTER TABLE
  "coinpaprika_asset_metadata"
ADD
  FOREIGN KEY ("coinpaprika_id") REFERENCES "coinpaprika_assets" ("id");

/*
 - Create the "coinpaprika_exchanges" table to store detailed information about cryptocurrency exchanges
 - id: Unique identifier for each exchange
 - name: Name of the exchange
 - active: Indicates if the exchange is currently active (true/false)
 - website_status: Indicates the operational status of the exchange's website (true/false)
 - api_status: Indicates the operational status of the exchange's API (true/false)
 - description: A textual description of the exchange
 - message: Any additional message or announcement from the exchange
 - twitter: Twitter handle or URL for the exchange (optional)
 - website: Official website URL for the exchange
 - markets_data_fetched: Flag to indicate whether market data has been fetched for the exchange (true/false)
 - adjusted_rank: The exchange's rank after applying any adjustments (e.g., for data accuracy)
 - reported_rank: The exchange's rank as reported by the source (no adjustments)
 - currencies: Number of different cryptocurrencies supported on the exchange
 - markets: Number of markets available on the exchange (e.g., BTC/USD, ETH/USD)
 - fiats: List of fiat currencies supported by the exchange, stored as a JSON array
 - reported_volume_24h_usd: Reported 24-hour trading volume in USD for the exchange
 - adjusted_volume_24h_usd: Adjusted 24-hour trading volume in USD (after any corrections)
 - reported_volume_7d_usd: Reported 7-day trading volume in USD for the exchange
 - adjusted_volume_7d_usd: Adjusted 7-day trading volume in USD
 - reported_volume_30d_usd: Reported 30-day trading volume in USD
 - adjusted_volume_30d_usd: Adjusted 30-day trading volume in USD
 - confidence_score: Confidence score based on the data quality and reliability
 - last_updated: Timestamp indicating when the exchange data was last updated
 */
CREATE TABLE "coinpaprika_exchanges" (
  "id" text PRIMARY KEY,
  "name" text,
  "active" bool,
  "website_status" bool,
  "api_status" bool,
  "description" text,
  "message" text,
  "twitter" varchar2(200),
  "website" varchar2(200),
  "markets_data_fetched" bool,
  "adjusted_rank" int,
  "reported_rank" int,
  "currencies" int,
  "markets" int,
  "fiats" JSON DEFAULT '[]',
  "reported_volume_24h_usd" float,
  "adjusted_volume_24h_usd" float,
  "reported_volume_7d_usd" float,
  "adjusted_volume_7d_usd" float,
  "reported_volume_30d_usd" float,
  "adjusted_volume_30d_usd" float,
  "confidence_score" float,
  "last_updated" timestamp DEFAULT 'Now()'
);

/*
 - Create an index on the "id" column of the "coinpaprika_exchanges" table for faster lookups by exchange ID
 */
CREATE INDEX ON "coinpaprika_exchanges" ("id");

/*
 - Create an index on the "name" column of the "coinpaprika_exchanges" table to speed up searches by exchange name
 */
CREATE INDEX ON "coinpaprika_exchanges" ("name");

/*
 - Create the "coinpaprika_exchanges_markets" table to store market-specific data for each exchange
 - exchange_id: Foreign key referencing the exchange's ID in "coinpaprika_exchanges"
 - pair: The trading pair (e.g., BTC/USD)
 - base_currency_id: ID of the base currency in the trading pair
 - base_currency_name: Name of the base currency in the trading pair
 - quote_currency_id: ID of the quote currency in the trading pair
 - quote_currency_name: Name of the quote currency in the trading pair
 - market_url: URL to the market's specific page on the exchange's platform
 - category: The category of the market (e.g., spot market, futures market)
 - fee_type: Type of fees charged for trades (e.g., maker/taker fees)
 - outlier: Flag to indicate whether the market data is considered an outlier
 - reported_volume_24h_share: Share of the market's 24-hour trading volume in USD
 - price_usd: Current price of the trading pair in USD
 - volume_24h_usd: 24-hour trading volume in USD for the specific market
 - trust_score: Trust score indicating the reliability of the market's data
 - last_updated: Timestamp of when the market data was last updated
 */
CREATE TABLE "coinpaprika_exchanges_markets" (
  "exchange_id" text,
  "pair" text,
  "base_currency_id" text,
  "base_currency_name" text,
  "quote_currency_id" text,
  "quote_currency_name" text,
  "market_url" text,
  "category" text,
  "fee_type" text,
  "outlier" bool,
  "reported_volume_24h_share" float,
  "price_usd" float,
  "volume_24h_usd" float,
  "trust_score" text,
  "last_updated" timestamp DEFAULT 'Now()'
);

/*
 - Create an index on the "exchange_id" column of the "coinpaprika_exchanges_markets" table for faster lookups by exchange ID
 */
CREATE INDEX ON "coinpaprika_exchanges_markets" ("exchange_id");

/*
 - Create an index on the "pair" column of the "coinpaprika_exchanges_markets" table for faster searches by trading pair
 */
CREATE INDEX ON "coinpaprika_exchanges_markets" ("pair");

/*
 - Add a foreign key constraint on the "exchange_id" column in the "coinpaprika_exchanges_markets" table to ensure referential integrity
 */
ALTER TABLE
  "coinpaprika_exchanges_markets"
ADD
  FOREIGN KEY ("exchange_id") REFERENCES "coinpaprika_exchanges" ("id");