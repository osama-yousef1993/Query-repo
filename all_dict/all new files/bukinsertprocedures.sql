-- Exchanges Market Ticker
CREATE TYPE exchangesMarket AS (
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
    first_trade last_updatedTZ,
    first_candle TIMESTAMPTZ,
    first_order_book TIMESTAMPTZ,
    timestamp TIMESTAMPTZ,
	total_trades FLOAT
  );



CREATE OR REPLACE PROCEDURE upsertExchangesMarketTicker(IN r1 exchangesMarket[])
AS 
$BODY$
Declare 
	rec exchangesMarket;
BEGIN
	 	FOREACH rec in ARRAY r1 loop
			INSERT INTO nomics_exchange_market_ticker(
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



-- Exchanges Market Ticker Intervals
Create TYPE exchangeIntervals AS (
    base TEXT,
    last_updated TIMESTAMPTZ,
    market TEXT,
    exchange TEXT,
    volume FLOAT,
    volume_base FLOAT,
    volume_base_change FLOAT,
    volume_change FLOAT,
    trades FLOAT,
    trades_change FLOAT,
    price_change FLOAT,
    price_quote_change FLOAT
);
CREATE OR REPLACE PROCEDURE upsertExchangesMarketTickerOneDay(IN intervals exchangeIntervals[])
AS 
$BODY$
DECLARE
    exchangeInt exchangeIntervals;
BEGIN
    FOREACH exchangeInt in ARRAY intervals loop
        INSERT INTO nomics_exchange_market_ticker_one_day
        VALUES (exchangeInt.base, exchangeInt.exchange, exchangeInt.last_updated, exchangeInt.market, exchangeInt.volume, exchangeInt.volume_base, 
                exchangeInt.volume_base_change, exchangeInt.volume_change, exchangeInt.trades, exchangeInt.trades_change ,exchangeInt.price_change, exchangeInt.price_quote_change)
        ON CONFLICT (last_updated, base, market, exchange) 
        DO UPDATE SET volume = EXCLUDED.volume, volume_base = EXCLUDED.volume_base, 
        volume_base_change = EXCLUDED.volume_base_change, volume_change = EXCLUDED.volume_change, 
        trades = EXCLUDED.trades, trades_change = EXCLUDED.trades_change, price_change = EXCLUDED.price_change, 
        price_quote_change = EXCLUDED.price_quote_change;
    END LOOP;
END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE upsertExchangesMarketTickerSevenDays(IN intervals exchangeIntervals[])
AS 
$BODY$
DECLARE
    exchangeInt exchangeIntervals;
BEGIN
    FOREACH exchangeInt in ARRAY intervals loop
        INSERT INTO nomics_exchange_market_ticker_seven_days
        VALUES (exchangeInt.base, exchangeInt.exchange, exchangeInt.last_updated, exchangeInt.market, exchangeInt.volume, exchangeInt.volume_base, 
                exchangeInt.volume_base_change, exchangeInt.volume_change, exchangeInt.trades, exchangeInt.trades_change ,exchangeInt.price_change, exchangeInt.price_quote_change)
        ON CONFLICT (last_updated, base, market, exchange) 
        DO UPDATE SET volume = EXCLUDED.volume, volume_base = EXCLUDED.volume_base, 
        volume_base_change = EXCLUDED.volume_base_change, volume_change = EXCLUDED.volume_change, 
        trades = EXCLUDED.trades, trades_change = EXCLUDED.trades_change, price_change = EXCLUDED.price_change, 
        price_quote_change = EXCLUDED.price_quote_change;
    END LOOP;
END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE upsertExchangesMarketTickerThirtyDays(IN intervals exchangeIntervals[])
AS 
$BODY$
DECLARE
    exchangeInt exchangeIntervals;
BEGIN
    FOREACH exchangeInt in ARRAY intervals loop
        INSERT INTO nomics_exchange_market_ticker_thirty_days
        VALUES (exchangeInt.base, exchangeInt.exchange, exchangeInt.last_updated, exchangeInt.market, exchangeInt.volume, exchangeInt.volume_base, 
                exchangeInt.volume_base_change, exchangeInt.volume_change, exchangeInt.trades, exchangeInt.trades_change ,exchangeInt.price_change, exchangeInt.price_quote_change)
        ON CONFLICT (last_updated, base, market, exchange) 
        DO UPDATE SET volume = EXCLUDED.volume, volume_base = EXCLUDED.volume_base, 
        volume_base_change = EXCLUDED.volume_base_change, volume_change = EXCLUDED.volume_change, 
        trades = EXCLUDED.trades, trades_change = EXCLUDED.trades_change, price_change = EXCLUDED.price_change, 
        price_quote_change = EXCLUDED.price_quote_change;
    END LOOP;
END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE upsertExchangesMarketTickerOneYear(IN intervals exchangeIntervals[])
AS 
$BODY$
DECLARE
    exchangeInt exchangeIntervals;
BEGIN
    FOREACH exchangeInt in ARRAY intervals loop
        INSERT INTO nomics_exchange_market_ticker_one_year
        VALUES (exchangeInt.base, exchangeInt.exchange, exchangeInt.last_updated, exchangeInt.market, exchangeInt.volume, exchangeInt.volume_base, 
                exchangeInt.volume_base_change, exchangeInt.volume_change, exchangeInt.trades, exchangeInt.trades_change ,exchangeInt.price_change, exchangeInt.price_quote_change)
        ON CONFLICT (last_updated, base, market, exchange) 
        DO UPDATE SET volume = EXCLUDED.volume, volume_base = EXCLUDED.volume_base, 
        volume_base_change = EXCLUDED.volume_base_change, volume_change = EXCLUDED.volume_change, 
        trades = EXCLUDED.trades, trades_change = EXCLUDED.trades_change, price_change = EXCLUDED.price_change, 
        price_quote_change = EXCLUDED.price_quote_change;
    END LOOP;
END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE upsertExchangesMarketTickerYTD(IN intervals exchangeIntervals[])
AS 
$BODY$
DECLARE
    exchangeInt exchangeIntervals;
BEGIN
    FOREACH exchangeInt in ARRAY intervals loop
        INSERT INTO nomics_exchange_market_ticker_ytd
        VALUES (exchangeInt.base, exchangeInt.exchange, exchangeInt.last_updated, exchangeInt.market, exchangeInt.volume, exchangeInt.volume_base, 
                exchangeInt.volume_base_change, exchangeInt.volume_change, exchangeInt.trades, exchangeInt.trades_change ,exchangeInt.price_change, exchangeInt.price_quote_change)
        ON CONFLICT (last_updated, base, market, exchange) 
        DO UPDATE SET volume = EXCLUDED.volume, volume_base = EXCLUDED.volume_base, 
        volume_base_change = EXCLUDED.volume_base_change, volume_change = EXCLUDED.volume_change, 
        trades = EXCLUDED.trades, trades_change = EXCLUDED.trades_change, price_change = EXCLUDED.price_change, 
        price_quote_change = EXCLUDED.price_quote_change;
    END LOOP;
END;
$BODY$
LANGUAGE plpgsql;


-- Nomics Markets
CREATE TYPE marketsValue AS (
  base TEXT,
  exchange TEXT,
  market TEXT,
  quote TEXT
);

CREATE OR REPLACE PROCEDURE upsertMarkets(IN Markets marketsValue[])
AS 
$BODY$
DECLARE
    marketVal marketsValue;
BEGIN
    FOREACH marketVal in ARRAY Markets loop
        INSERT INTO nomics_markets
        VALUES (marketVal.base, marketVal.exchange,marketVal.market, marketVal.quote);
    END LOOP;
END;
$BODY$
LANGUAGE plpgsql;


-- Nomics candles
CREATE TYPE candlesValues AS (
  base TEXT,
  timestamp TIMESTAMPTZ,
  open FLOAT,
  high FLOAT,
  close FLOAT,
  low FLOAT,
  volume FLOAT,
  num_trades FLOAT,
  price_outlier FLOAT,
  volume_outlier FLOAT,
  quote TEXT
);

CREATE OR REPLACE PROCEDURE upsertCandles(IN candles candlesValues[])
AS 
$BODY$
DECLARE
    candle candlesValues;
BEGIN
    FOREACH candle in ARRAY candles loop
        INSERT INTO nomics_ohlcv_candles
        VALUES (candle.base, candle.timestamp, candle.open, candle.high, candle.close, candle.low, candle.volume, 
        candle.num_trades, candle.price_outlier, candle.volume_outlier, candle.quote)
        ON CONFLICT (base, timestamp) DO UPDATE SET base = EXCLUDED.base, 
        timestamp = EXCLUDED.timestamp, open = EXCLUDED.open, high = EXCLUDED.high, 
        close = EXCLUDED.close, low = EXCLUDED.low, volume = EXCLUDED.volume, 
        num_trades = EXCLUDED.num_trades, price_outlier = EXCLUDED.price_outlier, 
        volume_outlier = EXCLUDED.volume_outlier, quote = EXCLUDED.quote;
    END LOOP;
END;
$BODY$
LANGUAGE plpgsql;


-- Nomics Assets
CREATE Type assetsValues AS (
  id TEXT,
  currency TEXT,
  symbol TEXT,
  name TEXT,
  logo_url TEXT,
  status TEXT
);

CREATE OR REPLACE PROCEDURE upsertAssets(IN assets assetsValues[], INOUT last_updated_value TIMESTAMPTZ  DEFAULT null)
AS 
$BODY$
DECLARE
    asset assetsValues;
BEGIN
    FOREACH asset in ARRAY assets loop
        INSERT INTO nomics_assets
        VALUES (asset.id, asset.currency, asset.symbol, asset.name, asset.logo_url, asset.status)
        ON CONFLICT (id) DO UPDATE SET currency = EXCLUDED.currency, symbol = EXCLUDED.symbol, 
        name = EXCLUDED.name, logo_url = EXCLUDED.logo_url, status = EXCLUDED.status, 
        last_updated = Now() RETURNING last_updated INTO last_updated_value;
    END LOOP;
END;
$BODY$
LANGUAGE plpgsql;



-- Nomics Currencies Ticker

CREATE Type nomicsCurrenciesTickers AS(
  id TEXT,
  last_updated TIMESTAMPTZ,
  price FLOAT,
  status TEXT,
  price_date TIMESTAMPTZ,
  price_timestamp TIMESTAMPTZ,
  circulating_supply FLOAT,
  max_supply FLOAT,
  marketcap FLOAT,
  transparent_marketcap FLOAT,
  marketcap_dominance FLOAT,
  num_exchanges NUMERIC,
  num_pairs NUMERIC,
  num_pairs_unmapped NUMERIC,
  first_candle TIMESTAMPTZ,
  first_trade TIMESTAMPTZ,
  first_order_book TIMESTAMPTZ,
  first_priced_at TIMESTAMPTZ,
  rank NUMERIC,
  rank_delta NUMERIC,
  high FLOAT,
  high_timestamp TIMESTAMPTZ,
  platform_currency TEXT
);

CREATE OR REPLACE PROCEDURE upsertCurrenciesTickers(IN currencies nomicsCurrenciesTickers[])
AS 
$BODY$
DECLARE
    currency nomicsCurrenciesTickers;
BEGIN
    FOREACH currency in ARRAY currencies loop
        INSERT INTO nomics_currencies_tickers
        VALUES (currency.id, currency.last_updated, currency.price, currency.status, currency.price_date, currency.price_timestamp,
        currency.circulating_supply, currency.max_supply, currency.marketcap, currency.transparent_marketcap, currency.marketcap_dominance, currency.num_exchanges,
        currency.num_pairs, currency.num_pairs_unmapped, currency.first_candle, currency.first_trade, currency.first_order_book, currency.first_priced_at,
        currency.rank, currency.rank_delta, currency.high, currency.high_timestamp, currency.platform_currency)
        ON CONFLICT (id, last_updated) DO UPDATE SET price = EXCLUDED.price, status = EXCLUDED.status, price_date = EXCLUDED.price_date, 
        price_timestamp = EXCLUDED.price_timestamp, circulating_supply = EXCLUDED.circulating_supply, max_supply = EXCLUDED.max_supply, 
        marketcap = EXCLUDED.marketcap, transparent_marketcap = EXCLUDED.transparent_marketcap, marketcap_dominance = EXCLUDED.marketcap_dominance, 
        num_exchanges = EXCLUDED.num_exchanges, num_pairs = EXCLUDED.num_pairs, num_pairs_unmapped = EXCLUDED.num_pairs_unmapped, 
        first_candle = EXCLUDED.first_candle, first_trade = EXCLUDED.first_trade, first_order_book = EXCLUDED.first_order_book, 
        first_priced_at = EXCLUDED.first_priced_at, rank = EXCLUDED.rank, rank_delta = EXCLUDED.rank_delta, high = EXCLUDED.high, 
        high_timestamp = EXCLUDED.high_timestamp, platform_currency = EXCLUDED.platform_currency;
    END LOOP;
END;
$BODY$
LANGUAGE plpgsql;

-- Nomics Currencies Tickers Interval
CREATE Type nomicsCurrenciesTickersIntervals as (
  id TEXT,
  last_updated TIMESTAMPTZ,
  volume FLOAT,
  price_change FLOAT,
  price_change_pct FLOAT,
  volume_change FLOAT,
  volume_change_pct FLOAT,
  marketcap_change FLOAT,
  marketcap_change_pct FLOAT,
  transparent_marketcap_change FLOAT,
  transparent_marketcap_change_pct FLOAT,
  volume_transparency_grade NUMERIC,
  volume_transparency json
);

CREATE OR REPLACE PROCEDURE upsertCurrenciesTickersOneHour(IN currencies nomicsCurrenciesTickersIntervals[])
AS 
$BODY$
DECLARE
    currency nomicsCurrenciesTickersIntervals;
BEGIN
    FOREACH currency in ARRAY currencies loop
        INSERT INTO nomics_currencies_tickers_one_hour
        VALUES (currency.id, currency.last_updated, currency.volume, currency.price_change, currency.price_change_pct, 
        currency.volume_change, currency.volume_change_pct, currency.marketcap_change, currency.marketcap_change_pct, 
        currency.transparent_marketcap_change, currency.transparent_marketcap_change_pct, currency.volume_transparency_grade, currency.volume_transparency)
        ON CONFLICT (id, last_updated) DO UPDATE SET volume = EXCLUDED.volume, price_change = EXCLUDED.price_change, 
        price_change_pct = EXCLUDED.price_change_pct, volume_change = EXCLUDED.volume_change, volume_change_pct = EXCLUDED.volume_change_pct, 
        marketcap_change = EXCLUDED.marketcap_change, marketcap_change_pct = EXCLUDED.marketcap_change_pct, 
        transparent_marketcap_change = EXCLUDED.transparent_marketcap_change, transparent_marketcap_change_pct = EXCLUDED.transparent_marketcap_change_pct, 
        volume_transparency_grade = EXCLUDED.volume_transparency_grade, volume_transparency = EXCLUDED.volume_transparency;
    END LOOP;
END;
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE PROCEDURE upsertCurrenciesTickersOneDay(IN currencies nomicsCurrenciesTickersIntervals[])
AS 
$BODY$
DECLARE
    currency nomicsCurrenciesTickersIntervals;
BEGIN
    FOREACH currency in ARRAY currencies loop
        INSERT INTO nomics_currencies_tickers_one_day
        VALUES (currency.id, currency.last_updated, currency.volume, currency.price_change, currency.price_change_pct, 
        currency.volume_change, currency.volume_change_pct, currency.marketcap_change, currency.marketcap_change_pct, 
        currency.transparent_marketcap_change, currency.transparent_marketcap_change_pct, currency.volume_transparency_grade, currency.volume_transparency)
        ON CONFLICT (id, last_updated) DO UPDATE SET volume = EXCLUDED.volume, price_change = EXCLUDED.price_change, 
        price_change_pct = EXCLUDED.price_change_pct, volume_change = EXCLUDED.volume_change, volume_change_pct = EXCLUDED.volume_change_pct, 
        marketcap_change = EXCLUDED.marketcap_change, marketcap_change_pct = EXCLUDED.marketcap_change_pct, 
        transparent_marketcap_change = EXCLUDED.transparent_marketcap_change, transparent_marketcap_change_pct = EXCLUDED.transparent_marketcap_change_pct, 
        volume_transparency_grade = EXCLUDED.volume_transparency_grade, volume_transparency = EXCLUDED.volume_transparency;
    END LOOP;
END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE upsertCurrenciesTickersSevenDays(IN currencies nomicsCurrenciesTickersIntervals[])
AS 
$BODY$
DECLARE
    currency nomicsCurrenciesTickersIntervals;
BEGIN
    FOREACH currency in ARRAY currencies loop
        INSERT INTO nomics_currencies_tickers_seven_days
        VALUES (currency.id, currency.last_updated, currency.volume, currency.price_change, currency.price_change_pct, 
        currency.volume_change, currency.volume_change_pct, currency.marketcap_change, currency.marketcap_change_pct, 
        currency.transparent_marketcap_change, currency.transparent_marketcap_change_pct, currency.volume_transparency_grade, currency.volume_transparency)
        ON CONFLICT (id, last_updated) DO UPDATE SET volume = EXCLUDED.volume, price_change = EXCLUDED.price_change, 
        price_change_pct = EXCLUDED.price_change_pct, volume_change = EXCLUDED.volume_change, volume_change_pct = EXCLUDED.volume_change_pct, 
        marketcap_change = EXCLUDED.marketcap_change, marketcap_change_pct = EXCLUDED.marketcap_change_pct, 
        transparent_marketcap_change = EXCLUDED.transparent_marketcap_change, transparent_marketcap_change_pct = EXCLUDED.transparent_marketcap_change_pct, 
        volume_transparency_grade = EXCLUDED.volume_transparency_grade, volume_transparency = EXCLUDED.volume_transparency;
    END LOOP;
END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE upsertCurrenciesTickersThirtyDays(IN currencies nomicsCurrenciesTickersIntervals[])
AS 
$BODY$
DECLARE
    currency nomicsCurrenciesTickersIntervals;
BEGIN
    FOREACH currency in ARRAY currencies loop
        INSERT INTO nomics_currencies_tickers_thirty_days
        VALUES (currency.id, currency.last_updated, currency.volume, currency.price_change, currency.price_change_pct, 
        currency.volume_change, currency.volume_change_pct, currency.marketcap_change, currency.marketcap_change_pct, 
        currency.transparent_marketcap_change, currency.transparent_marketcap_change_pct, currency.volume_transparency_grade, currency.volume_transparency)
        ON CONFLICT (id, last_updated) DO UPDATE SET volume = EXCLUDED.volume, price_change = EXCLUDED.price_change, 
        price_change_pct = EXCLUDED.price_change_pct, volume_change = EXCLUDED.volume_change, volume_change_pct = EXCLUDED.volume_change_pct, 
        marketcap_change = EXCLUDED.marketcap_change, marketcap_change_pct = EXCLUDED.marketcap_change_pct, 
        transparent_marketcap_change = EXCLUDED.transparent_marketcap_change, transparent_marketcap_change_pct = EXCLUDED.transparent_marketcap_change_pct, 
        volume_transparency_grade = EXCLUDED.volume_transparency_grade, volume_transparency = EXCLUDED.volume_transparency;
    END LOOP;
END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE upsertCurrenciesTickersOneYear(IN currencies nomicsCurrenciesTickersIntervals[])
AS 
$BODY$
DECLARE
    currency nomicsCurrenciesTickersIntervals;
BEGIN
    FOREACH currency in ARRAY currencies loop
        INSERT INTO nomics_currencies_tickers_one_year
        VALUES (currency.id, currency.last_updated, currency.volume, currency.price_change, currency.price_change_pct, 
        currency.volume_change, currency.volume_change_pct, currency.marketcap_change, currency.marketcap_change_pct, 
        currency.transparent_marketcap_change, currency.transparent_marketcap_change_pct, currency.volume_transparency_grade, currency.volume_transparency)
        ON CONFLICT (id, last_updated) DO UPDATE SET volume = EXCLUDED.volume, price_change = EXCLUDED.price_change, 
        price_change_pct = EXCLUDED.price_change_pct, volume_change = EXCLUDED.volume_change, volume_change_pct = EXCLUDED.volume_change_pct, 
        marketcap_change = EXCLUDED.marketcap_change, marketcap_change_pct = EXCLUDED.marketcap_change_pct, 
        transparent_marketcap_change = EXCLUDED.transparent_marketcap_change, transparent_marketcap_change_pct = EXCLUDED.transparent_marketcap_change_pct, 
        volume_transparency_grade = EXCLUDED.volume_transparency_grade, volume_transparency = EXCLUDED.volume_transparency;
    END LOOP;
END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE upsertCurrenciesTickersYTD(IN currencies nomicsCurrenciesTickersIntervals[])
AS 
$BODY$
DECLARE
    currency nomicsCurrenciesTickersIntervals;
BEGIN
    FOREACH currency in ARRAY currencies loop
        INSERT INTO nomics_currencies_tickers_ytd
        VALUES (currency.id, currency.last_updated, currency.volume, currency.price_change, currency.price_change_pct, 
        currency.volume_change, currency.volume_change_pct, currency.marketcap_change, currency.marketcap_change_pct, 
        currency.transparent_marketcap_change, currency.transparent_marketcap_change_pct, currency.volume_transparency_grade, currency.volume_transparency)
        ON CONFLICT (id, last_updated) DO UPDATE SET volume = EXCLUDED.volume, price_change = EXCLUDED.price_change, 
        price_change_pct = EXCLUDED.price_change_pct, volume_change = EXCLUDED.volume_change, volume_change_pct = EXCLUDED.volume_change_pct, 
        marketcap_change = EXCLUDED.marketcap_change, marketcap_change_pct = EXCLUDED.marketcap_change_pct, 
        transparent_marketcap_change = EXCLUDED.transparent_marketcap_change, transparent_marketcap_change_pct = EXCLUDED.transparent_marketcap_change_pct, 
        volume_transparency_grade = EXCLUDED.volume_transparency_grade, volume_transparency = EXCLUDED.volume_transparency;
    END LOOP;
END;
$BODY$
LANGUAGE plpgsql;