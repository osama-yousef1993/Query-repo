CREATE TABLE "exchange_market_ticker" (
  "base" TEXT,
  "exchange" TEXT,
  "market" TEXT,
  "quote" TEXT,
  "type" TEXT,
  "sub_type" TEXT,
  "aggregated" BOOLEAN,
  "price_exclude" BOOLEAN,
  "volume_exclude" BOOLEAN,
  "base_symbol" TEXT,
  "quote_symbol" TEXT,
  "price" FLOAT,
  "price_quote" FLOAT,
  "volume_usd" FLOAT,
  "status" TEXT,
  "weight" TEXT,
  "first_trade" TIMESTAMPTZ,
  "first_candle" TIMESTAMPTZ,
  "first_order_book" TIMESTAMPTZ,
  "timestamp" TIMESTAMPTZ,
  "total_trades" FLOAT,
  "volume" FLOAT,
  "volume_base" FLOAT,
  "volume_base_change" FLOAT,
  "volume_change" FLOAT,
  "trades" FLOAT,
  "trades_change" FLOAT,
  "price_change" FLOAT,
  "price_quote_change" FLOAT,
  "last_updated" TIMESTAMPTZ,
  PRIMARY KEY ("base", "exchange", "market", "quote", "timestamp", "last_updated", "price", "price_quote")
);

CREATE INDEX ON "exchange_market_ticker" ("base");

CREATE INDEX ON "exchange_market_ticker" ("exchange");

CREATE INDEX ON "exchange_market_ticker" ("market");

CREATE INDEX ON "exchange_market_ticker" ("quote");

CREATE INDEX ON "exchange_market_ticker" ("timestamp");

CREATE INDEX ON "exchange_market_ticker" ("last_updated");

CREATE INDEX ON "exchange_market_ticker" ("price");

CREATE INDEX ON "exchange_market_ticker" ("price_quote");

