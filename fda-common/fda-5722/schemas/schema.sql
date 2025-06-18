-- This script creates a table named 'forbes_exchanges' with the following columns:
-- - forbes_id: a text field that serves as the primary key.
-- - name: a text field for the name of the exchange.
-- - coingecko_id: a text field for the CoinGecko ID of the exchange.
-- - coinpaprika_id: a text field for the CoinPaprika ID of the exchange.
-- - last_updated: a timestamp with time zone that defaults to the current time.
--
-- Additionally, the script creates indexes on the 'name', 'coingecko_id', and 'coinpaprika_id' columns to improve query performance.
create table forbes_exchanges (
    forbes_id text primary key,
    name text,
    coingecko_id text,
    coinpaprika_id text,
    last_updated timestamptz default now()
)

CREATE INDEX ON "forbes_exchanges" (name);
CREATE INDEX ON "forbes_exchanges" (coingecko_id);
CREATE INDEX ON "forbes_exchanges" (coinpaprika_id);

-- This script creates a table named 'forbes_assets' with the following columns:
-- - forbesID: a text field that serves as the primary key.
-- - symbol: a text field for the symbol of the asset.
-- - name: a text field for the name of the asset.
-- - coingecko_id: a text field for the CoinGecko ID of the asset.
-- - coinpaprika_id: a text field for the CoinPaprika ID of the asset.
-- - contractAddress: a text field for the contract address of the asset.
-- - last_updated: a timestamp with time zone that defaults to the current time.
--
-- Additionally, the script creates indexes on the 'forbesID', 'coingecko_id', and 'coinpaprika_id' columns to improve query performance.
CREATE TABLE "forbes_assets" (
  "id" text PRIMARY KEY,
  "symbol" text,
  "name" text,
  "coingecko_id" text,
  "coinpaprika_id" text,
  "contractAddress" text,
  "last_updated" timestamp DEFAULT 'Now()'
);

CREATE INDEX ON "forbes_assets" ("id");

CREATE INDEX ON "forbes_assets" ("coingecko_id");

CREATE INDEX ON "forbes_assets" ("coinpaprika_id");
