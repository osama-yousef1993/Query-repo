-- -- DROP TYPE IF EXISTS exchangesValues;
--   CREATE TYPE exchangesValues AS (
--     "base" TEXT,
--     "exchange" TEXT,
--     "market" TEXT,
--     "quote" TEXT,
--     "type" TEXT,
--     "sub_type" TEXT,
--     "aggregated" BOOLEAN,
-- 	  "price_exclude" BOOLEAN,
--     "volume_exclude" BOOLEAN,
--     "base_symbol" TEXT,
--     "quote_symbol" TEXT,
--     "price" FLOAT,
--     "price_quote" FLOAT,
--     "volume_usd" FLOAT,
--     "status" TEXT,
--     "weight" TEXT,
--     "first_trade" TIMESTAMPTZ,
--     "first_candle" TIMESTAMPTZ,
--     "first_order_book" TIMESTAMPTZ,
--    "timestamp" TIMESTAMPTZ,
--    "total_trades" FLOAT
-- );
CREATE OR REPLACE PROCEDURE upsertExchangesMarketTicker(
	base TEXT,
  	exchange TEXT,
	market TEXT,
	quote TEXT,
	type TEXT,
	sub_type TEXT,
	aggregated BOOLEAN,
	price_exclude BOOLEAN,
	volume_exclude BOOLEAN,
	base_symbol TEXT,
	quote_symbol TEXT,
	price FLOAT,
	price_quote FLOAT,
	volume_usd FLOAT,
	status TEXT,
	weight TEXT,
	first_trade TIMESTAMPTZ,
	first_candle TIMESTAMPTZ,
	first_order_book TIMESTAMPTZ,
	"timestamp" TIMESTAMPTZ,
	total_trades FLOAT)
LANGUAGE sql
AS 
$BODY$	
		INSERT INTO nomics_exchange_market_ticker_test_delete
		Values(base, exchange, market, quote, type, sub_type, aggregated, price_exclude, volume_exclude,
		base_symbol, quote_symbol, price, price_quote, volume_usd, status, weight, first_trade, first_candle,
		first_order_book, timestamp, total_trades) 
		ON CONFLICT (base, exchange, market, quote, timestamp) 
		DO UPDATE SET exchange = EXCLUDED.exchange, market = EXCLUDED.market, quote = EXCLUDED.quote,
		type = EXCLUDED.type, sub_type = EXCLUDED.sub_type, aggregated = EXCLUDED.aggregated,
		price_exclude = EXCLUDED.price_exclude, volume_exclude = EXCLUDED.volume_exclude,
		base_symbol = EXCLUDED.base_symbol, quote_symbol = EXCLUDED.quote_symbol, price = EXCLUDED.price,
		price_quote = EXCLUDED.price_quote, volume_usd = EXCLUDED.volume_usd, status = EXCLUDED.status,
		weight = EXCLUDED.weight, first_trade = EXCLUDED.first_trade, first_candle = EXCLUDED.first_candle,
		first_order_book = EXCLUDED.first_order_book, timestamp = EXCLUDED.timestamp, total_trades = EXCLUDED.total_trades;
$BODY$ ;



++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

-- -- DROP TYPE IF EXISTS exchangesValues;
--   CREATE TYPE exchangesValues AS (
--     "base" TEXT,
--     "exchange" TEXT,
--     "market" TEXT,
--     "quote" TEXT,
--     "type" TEXT,
--     "sub_type" TEXT,
--     "aggregated" BOOLEAN,
-- 	  "price_exclude" BOOLEAN,
--     "volume_exclude" BOOLEAN,
--     "base_symbol" TEXT,
--     "quote_symbol" TEXT,
--     "price" FLOAT,
--     "price_quote" FLOAT,
--     "volume_usd" FLOAT,
--     "status" TEXT,
--     "weight" TEXT,
--     "first_trade" TIMESTAMPTZ,
--     "first_candle" TIMESTAMPTZ,
--     "first_order_book" TIMESTAMPTZ,
--    "timestamp" TIMESTAMPTZ,
--    "total_trades" FLOAT
-- );
CREATE OR REPLACE PROCEDURE upsertExchangesMarketTicker(r1 RECORD, r2 RECORD, r3 RECORD, r4 RECORD, r5 RECORD, r6 RECORD, r7 RECORD, r8 RECORD, r9 RECORD, r10 RECORD)
AS 
$$
DECLARE
	stat Text;
BEGIN

		stat := 'INSERT INTO nomics_exchange_market_ticker_test_delete(
		base, exchange, market, quote, type, sub_type, aggregated, price_exclude, volume_exclude,
		base_symbol, quote_symbol, price, price_quote, volume_usd, status, weight, first_trade, first_candle,
		first_order_book, timestamp, total_trades) 
		Values ' || r1, r2, r3, r4, r5, r6, r7, r8, r9, r10  ||'
		ON CONFLICT (base, exchange, market, quote, timestamp) 
		DO UPDATE SET exchange = EXCLUDED.exchange, market = EXCLUDED.market, quote = EXCLUDED.quote,
		type = EXCLUDED.type, sub_type = EXCLUDED.sub_type, aggregated = EXCLUDED.aggregated,
		price_exclude = EXCLUDED.price_exclude, volume_exclude = EXCLUDED.volume_exclude,
		base_symbol = EXCLUDED.base_symbol, quote_symbol = EXCLUDED.quote_symbol, price = EXCLUDED.price,
		price_quote = EXCLUDED.price_quote, volume_usd = EXCLUDED.volume_usd, status = EXCLUDED.status,
		weight = EXCLUDED.weight, first_trade = EXCLUDED.first_trade, first_candle = EXCLUDED.first_candle,
		first_order_book = EXCLUDED.first_order_book, timestamp = EXCLUDED.timestamp, total_trades = EXCLUDED.total_trades;';
	 RAISE NOTICE '
		INSERT INTO nomics_exchange_market_ticker_test_delete(
		base, exchange, market, quote, type, sub_type, aggregated, price_exclude, volume_exclude,
		base_symbol, quote_symbol, price, price_quote, volume_usd, status, weight, first_trade, first_candle,
		first_order_book, timestamp, total_trades) 
		Values %, %, %, %, %, %, %,%, %, 
		ON CONFLICT (base, exchange, market, quote, timestamp) 
		DO UPDATE SET exchange = EXCLUDED.exchange, market = EXCLUDED.market, quote = EXCLUDED.quote,
		type = EXCLUDED.type, sub_type = EXCLUDED.sub_type, aggregated = EXCLUDED.aggregated,
		price_exclude = EXCLUDED.price_exclude, volume_exclude = EXCLUDED.volume_exclude,
		base_symbol = EXCLUDED.base_symbol, quote_symbol = EXCLUDED.quote_symbol, price = EXCLUDED.price,
		price_quote = EXCLUDED.price_quote, volume_usd = EXCLUDED.volume_usd, status = EXCLUDED.status,
		weight = EXCLUDED.weight, first_trade = EXCLUDED.first_trade, first_candle = EXCLUDED.first_candle,
		first_order_book = EXCLUDED.first_order_book, timestamp = EXCLUDED.timestamp, total_trades = EXCLUDED.total_trades;', r1, r2, r3, r4, r5, r6, r7, r8, r9, r10 ;
	 EXECUTE(stat);
		
END;
$$ LANGUAGE plpgsql;



++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- -- DROP TYPE IF EXISTS exchangesValues;
    CREATE TYPE exchangesValues AS (
      base TEXT,
      exchange TEXT,
      market TEXT,
      quote TEXT,
      type TEXT,
      sub_type  TEXT,
      aggregated BOOLEAN,
  	  price_exclude BOOLEAN,
      volume_exclude BOOLEAN,
      base_symbol TEXT,
      quote_symbol TEXT,
      price FLOAT,
      price_quote FLOAT,
      volume_usd FLOAT,
      status TEXT,
      weight TEXT,
      first_trade TIMESTAMP WITH TIME ZONE,
      first_candle TIMESTAMP WITH TIME ZONE,
     first_order_book TIMESTAMP WITH TIME ZONE,
	timestamp TIMESTAMP WITH TIME ZONE,
	total_trades FLOAT
  );
CREATE OR REPLACE PROCEDURE upsertExchangesMarketTicker(r1 exchangesValues, r2 exchangesValues, r3 exchangesValues, r4 exchangesValues, r5 exchangesValues, r6 exchangesValues, r7 exchangesValues, r8 exchangesValues, r9 exchangesValues, r10 exchangesValues)
AS 
$BODY$
DECLARE
	stat Text;
BEGIN
		stat := 'INSERT INTO nomics_exchange_market_ticker_test_delete(
		base, exchange, market, quote, type, sub_type, aggregated, price_exclude, volume_exclude,
		base_symbol, quote_symbol, price, price_quote, volume_usd, status, weight, first_trade, first_candle,
		first_order_book, timestamp, total_trades) 
		Values ' || r1||','|| r2 ||','|| r3||','|| r4||','||r5||','||r6||','||r7||','||r8||','||r9||','||r10 ||'
		ON CONFLICT (base, exchange, market, quote, timestamp) 
		DO UPDATE SET exchange = EXCLUDED.exchange, market = EXCLUDED.market, quote = EXCLUDED.quote,
		type = EXCLUDED.type, sub_type = EXCLUDED.sub_type, aggregated = EXCLUDED.aggregated,
		price_exclude = EXCLUDED.price_exclude, volume_exclude = EXCLUDED.volume_exclude,
		base_symbol = EXCLUDED.base_symbol, quote_symbol = EXCLUDED.quote_symbol, price = EXCLUDED.price,
		price_quote = EXCLUDED.price_quote, volume_usd = EXCLUDED.volume_usd, status = EXCLUDED.status,
		weight = EXCLUDED.weight, first_trade = EXCLUDED.first_trade, first_candle = EXCLUDED.first_candle,
		first_order_book = EXCLUDED.first_order_book, timestamp = EXCLUDED.timestamp, total_trades = EXCLUDED.total_trades;';
-- 	 RAISE NOTICE '
-- 		INSERT INTO nomics_exchange_market_ticker_test_delete(
-- 		base, exchange, market, quote, type, sub_type, aggregated, price_exclude, volume_exclude,
-- 		base_symbol, quote_symbol, price, price_quote, volume_usd, status, weight, first_trade, first_candle,
-- 		first_order_book, timestamp, total_trades) 
-- 		Values %
-- 		ON CONFLICT (base, exchange, market, quote, timestamp) 
-- 		DO UPDATE SET exchange = EXCLUDED.exchange, market = EXCLUDED.market, quote = EXCLUDED.quote,
-- 		type = EXCLUDED.type, sub_type = EXCLUDED.sub_type, aggregated = EXCLUDED.aggregated,
-- 		price_exclude = EXCLUDED.price_exclude, volume_exclude = EXCLUDED.volume_exclude,
-- 		base_symbol = EXCLUDED.base_symbol, quote_symbol = EXCLUDED.quote_symbol, price = EXCLUDED.price,
-- 		price_quote = EXCLUDED.price_quote, volume_usd = EXCLUDED.volume_usd, status = EXCLUDED.status,
-- 		weight = EXCLUDED.weight, first_trade = EXCLUDED.first_trade, first_candle = EXCLUDED.first_candle,
-- 		first_order_book = EXCLUDED.first_order_book, timestamp = EXCLUDED.timestamp, total_trades = EXCLUDED.total_trades;', upsertExchangesMarketTicker.valueString;
	 EXECUTE(stat);
		
END;
$BODY$ LANGUAGE plpgsql;



