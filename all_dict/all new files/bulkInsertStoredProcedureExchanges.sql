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
      first_trade TIMESTAMPTZ,
      first_candle TIMESTAMPTZ,
     first_order_book TIMESTAMPTZ,
	timestamp TIMESTAMPTZ,
	total_trades FLOAT
  );



CREATE OR REPLACE PROCEDURE upsertExchangesMarketTicker(IN r1 exchangesValues[])
AS 
$BODY$
Declare 
	rec exchangesValues;
BEGIN
	 	FOREACH rec in ARRAY r1 loop
			INSERT INTO nomics_exchange_market_ticker_test_delete(
			base, exchange, market, quote, type, sub_type, aggregated, price_exclude, volume_exclude,
			base_symbol, quote_symbol, price, price_quote, volume_usd, status, weight, first_trade, first_candle,
			first_order_book, timestamp, total_trades) 
			Values (rec.base, rec.exchange, rec.market, rec.quote, rec.type, rec.sub_type, rec.aggregated, rec.price_exclude, rec.volume_exclude,
			rec.base_symbol, rec.quote_symbol, rec.price, rec.price_quote, rec.volume_usd, rec.status, rec.weight, rec.first_trade, rec.first_candle,
			rec.first_order_book, rec.timestamp, rec.total_trades)
			ON CONFLICT (base, exchange, market, quote, timestamp) 
			DO UPDATE SET exchange = EXCLUDED.exchange, market = EXCLUDED.market, quote = EXCLUDED.quote,
			type = EXCLUDED.type, sub_type = EXCLUDED.sub_type, aggregated = EXCLUDED.aggregated,
			price_exclude = EXCLUDED.price_exclude, volume_exclude = EXCLUDED.volume_exclude,
			base_symbol = EXCLUDED.base_symbol, quote_symbol = EXCLUDED.quote_symbol, price = EXCLUDED.price,
			price_quote = EXCLUDED.price_quote, volume_usd = EXCLUDED.volume_usd, status = EXCLUDED.status,
			weight = EXCLUDED.weight, first_trade = EXCLUDED.first_trade, first_candle = EXCLUDED.first_candle,
			first_order_book = EXCLUDED.first_order_book, timestamp = EXCLUDED.timestamp, total_trades = EXCLUDED.total_trades;
	END LOOP;
		
END;
$BODY$ LANGUAGE plpgsql;