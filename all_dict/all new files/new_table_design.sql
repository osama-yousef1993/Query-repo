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

CREATE INDEX ON "exchange_market_ticker" 
    ("base", "exchange", "market", "quote", "timestamp", "last_updated", "price") 
    INCLUDE ("type", "status", "volume", "price_change" );

-- CREATE INDEX ON "exchange_market_ticker" ("exchange");

-- CREATE INDEX ON "exchange_market_ticker" ("market");

-- CREATE INDEX ON "exchange_market_ticker" ("quote");

-- CREATE INDEX ON "exchange_market_ticker" ("timestamp");

-- CREATE INDEX ON "exchange_market_ticker" ("last_updated");

-- CREATE INDEX ON "exchange_market_ticker" ("price");

-- CREATE INDEX ON "exchange_market_ticker" ("price_quote");






CREATE TABLE "exchange_market_ticker_procedure" (
  "base" TEXT,
  "exchange" TEXT,
  "market" TEXT,
  "quote" TEXT,
  "type" TEXT,
  "price" FLOAT,
  "status" TEXT,
  "timestamp" TIMESTAMPTZ,
  "volume" FLOAT,
  PRIMARY KEY ("base", "exchange", "market", "quote", "timestamp", "price", "status", "volume")
);

CREATE INDEX ON "exchange_market_ticker_procedure" 
    ("base", "exchange", "market", "quote", "timestamp", "price", "status", "volume");







create or replace PROCEDURE buildExchnages()
LANGUAGE SQL
as $$
	INSERT INTO test_exchange_market_ticker(base, exchange, market, quote, type, sub_type, aggregated, price_exclude, volume_exclude, base_symbol, quote_symbol, 
									   price, price_quote, volume_usd, status, weight, first_trade, first_candle, first_order_book, "timestamp", total_trades,
									  volume, volume_base, volume_base_change, volume_change, trades, trades_change, price_change, price_quote_change, last_updated) (
		
			with ticker as (
				select base, exchange, market, quote, type, sub_type, aggregated, price_exclude, volume_exclude,
					   base_symbol, quote_symbol, price, price_quote, volume_usd, status, weight, first_trade, 
					   first_candle, first_order_book, "timestamp", total_trades
				from (
					select base, exchange, market, quote, type, sub_type, aggregated, price_exclude, volume_exclude,
						   base_symbol, quote_symbol, price, price_quote, volume_usd, status, weight, first_trade, 
						   first_candle, first_order_book, "timestamp", total_trades, 
						   row_number() OVER(PARTITION BY base, exchange, market, quote ORDER BY timestamp desc) AS row_num
					from nomics_exchange_market_ticker
					where
					timestamp >= cast(now() - INTERVAL '365 DAYS' as timestamp)
					AND base = any(select id from public.activeassets())
					AND quote IN ('USD', 'USDT', 'USDC')
				) as foo
				where row_num = 1
			),
			oneYear as (
				select 
					base, exchange, market, volume, volume_base, volume_base_change, volume_change, trades, trades_change, price_change,
					price_quote_change, last_updated
				from (
					select 
						base, exchange, market, volume, volume_base, volume_base_change, volume_change, trades, trades_change, price_change,
						price_quote_change, last_updated,
						row_number() OVER(PARTITION BY base, exchange, market ORDER BY last_updated desc) AS row_num
					from nomics_exchange_market_ticker_one_year
					where 
						last_updated >= cast(now() - INTERVAL '365 DAYS' as timestamp)
						AND base = any(select id from public.activeassets())
						AND volume IS NOT NULL
					)as foo
				where row_num = 1
			)

			select 
				ticker.base, ticker.exchange, ticker.market, ticker.quote, ticker.type, ticker.sub_type, ticker.aggregated, ticker.price_exclude, ticker.volume_exclude,
				ticker.base_symbol, ticker.quote_symbol, ticker.price, ticker.price_quote, ticker.volume_usd, ticker.status, ticker.weight, ticker.first_trade, 
				ticker.first_candle, ticker.first_order_book, ticker.timestamp, ticker.total_trades,
				oneYear.volume, oneYear.volume_base, oneYear.volume_base_change, oneYear.volume_change, oneYear.trades, oneYear.trades_change,
				oneYear.price_change, oneYear.price_quote_change, oneYear.last_updated
			from 
				oneYear
				INNER JOIN 
					ticker
				ON
					ticker.base = oneYear.base
					AND ticker.exchange = oneYear.exchange
					AND ticker.market = oneYear.market
	)
	on conflict (base, exchange, market, quote, timestamp, last_updated, price, price_quote) do Update set 
	base = EXCLUDED.base,
	exchange = EXCLUDED.exchange,
	market = EXCLUDED.market,
	quote = EXCLUDED.quote,
	type = EXCLUDED.type,
	sub_type = EXCLUDED.sub_type,
	aggregated = EXCLUDED.aggregated,
	price_exclude = EXCLUDED.price_exclude,
	volume_exclude = EXCLUDED.volume_exclude,
	base_symbol = EXCLUDED.base_symbol,
	quote_symbol = EXCLUDED.quote_symbol,
	price = EXCLUDED.price,
	price_quote = EXCLUDED.price_quote,
	volume_usd = EXCLUDED.volume_usd,
	status = EXCLUDED.status,
	weight = EXCLUDED.weight,
	first_trade = EXCLUDED.first_trade,
	first_candle = EXCLUDED.first_candle,
	first_order_book = EXCLUDED.first_order_book,
	timestamp = EXCLUDED.timestamp,
	total_trades = EXCLUDED.total_trades,
	volume = EXCLUDED.volume,
	volume_base = EXCLUDED.volume_base,
	volume_base_change = EXCLUDED.volume_base_change,
	volume_change = EXCLUDED.volume_change,
	trades = EXCLUDED.trades,
	trades_change = EXCLUDED.trades_change,
	price_change = EXCLUDED.price_change,
	price_quote_change = EXCLUDED.price_quote_change,
	last_updated = EXCLUDED.last_updated
$$;

CALL  buildExchnages();












with oneDay as (
	SELECT base, exchange, market, quote, type, price_1d, status, volume_1d
	from(
		SELECT 
			base, exchange, market, quote, type, avg(price) as price_1d, status, avg(volume) as volume_1d
		from 
			exchange_market_ticker_procedure
		where 
			timestamp >= cast(now() - interval '24 HOUR' as timestamp)
			AND status = 'active'
		group by 
			base, exchange, market, quote, type, status
	) as foo
),
sevenDays as (
	SELECT base, exchange, market, price_7d, volume_7d
	from(
		SELECT 
			base, exchange, market, price as price_7d, volume as volume_7d
		from 
			exchange_market_ticker_procedure
		where 
			timestamp >= cast(now() - interval '7 DAY' as timestamp)
 		group by 
			base, exchange, market
	) as foo
)
	
select 
	oneDay.base, oneDay.exchange, oneDay.market, oneDay.quote, oneDay.type, oneDay.status, oneDay.volume_1d, oneDay.price_1d,
	sevenDays.volume_7d, sevenDays.price_7d
from 
	oneDay
INNER JOIN
	sevenDays
ON 
	sevenDays.base = oneDay.base
		
		
		
		
		
		