create or replace PROCEDURE buildExchnages()
LANGUAGE SQL
as $$
	INSERT INTO exchange_market_ticker(base, exchange, market, quote, type, sub_type, aggregated, price_exclude, volume_exclude, base_symbol, quote_symbol, 
									   price, price_quote, volume_usd, status, weight, first_trade, first_candle, first_order_book, "timestamp", total_trades,
									  volume, volume_base, volume_base_change, volume_change, trades, trades_change, price_change, price_quote_change, last_updated) (
		
			with YTD as (
					select 
						base, volume, volume_base, volume_base_change, volume_change, trades, trades_change, price_change, price_quote_change, last_updated
					from 
						nomics_exchange_market_ticker_ytd
					where 
						last_updated >= cast(now() - INTERVAL '365 DAYS' as timestamp)
					AND base = any(select id from public.activeassets())

				),
				YTDPrice AS 
					(
					SELECT 
						base, exchange, market, quote, type, sub_type, aggregated, price_exclude, volume_exclude,
						base_symbol, quote_symbol, price, price_quote, volume_usd, status, weight, first_trade, 
						first_candle, first_order_book, "timestamp", total_trades
					FROM 
						nomics_exchange_market_ticker
					WHERE 
						timestamp >= cast(now() - INTERVAL '365 DAYS' as timestamp)
						AND quote IN ('USD', 'USDT', 'USDC')
					AND base = any(select id from public.activeassets())
					)
			select 
				distinct YTDPrice.base, YTDPrice.exchange, YTDPrice.market, YTDPrice.quote, YTDPrice.type, YTDPrice.sub_type, YTDPrice.aggregated, YTDPrice.price_exclude, YTDPrice.volume_exclude,
				YTDPrice.base_symbol, YTDPrice.quote_symbol, YTDPrice.price, YTDPrice.price_quote, YTDPrice.volume_usd, YTDPrice.status, YTDPrice.weight, YTDPrice.first_trade, 
				YTDPrice.first_candle, YTDPrice.first_order_book, YTDPrice.timestamp, YTDPrice.total_trades,
				YTD.volume, YTD.volume_base, YTD.volume_base_change, YTD.volume_change, YTD.trades, YTD.trades_change, YTD.price_change, YTD.price_quote_change, YTD.last_updated
			from 
				YTDPrice
				LEFT JOIN
				YTD
				using(base)
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







SELECT 
	MAX(Close) as high_1y, 
	MIN(Close) as low_1y, 
	symbol 
FROM 
	(
	SELECT 
		b.Close, 
		a.symbol 
	FROM 
		(
		SELECT 
			approx_quantiles(close, 4) as quantiles, 
			approx_quantiles(close, 4) [offset(3) ] + (1.5 * (approx_quantiles(close, 4) [offset(3) ] - approx_quantiles(close, 4) [offset(1) ])) as upperfence, 
			approx_quantiles(close, 4) [offset(1) ] - (1.5 * (approx_quantiles(close, 4) [offset(3) ] - approx_quantiles(close, 4) [offset(1) ])) as lowerfence, 
			lower(Base) as symbol 
		FROM 
			(
			SELECT 
				AVG(close) close,
				Base, 
				timestamp 
			FROM 
				api-project-901373404215.digital_assets.nomics_exchange_market_ticker c 
			WHERE 
				timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(),Interval 365 DAY) 
			GROUP BY 
				base, 
				timestamp
			) 
		GROUP BY 
			base
		) a  --IN "a" we work on calculating the upper fence an lower fence of all outliers 
		Join (
		SELECT 
			AVG(DISTINCT(Close)) Close, 
			lower(Base) as symbol, 
			Timestamp, 
		FROM 
			api-project-901373404215.digital_assets.nomics_exchange_market_ticker c 
		WHERE 
			Timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY) 
		GROUP BY 
			Base, 
			Timestamp
		) b on ( --in B we Pull all of the the assets within a time frame
		b.symbol = a.symbol 
		AND (b.Close BETWEEN a.lowerfence AND a.upperfence) --We then Join a on b, but only include closes that reside between the upper and lower fence
		)
	) 
GROUP BY 
	symbol





SELECT
	t.base,
	t.exchange,
	AVG(b.Close) as close
FROM(
  SELECT
      base,
      exchange,
      approx_quantiles(Close, 4) [offset(3) ] as upperfence
    FROM(
      select
        base,
        price as close,
        exchange
      FROM 
        api-project-901373404215.digital_assets.nomics_exchange_market_ticker
      where 
        Timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
        AND quote IN ('USD','USDT','USDC')
        AND base = 'BTC'
        AND exchange = 'binance'
    ) as foo
    group by
      base, exchange
) as t
JOIN (
  select
    base,
    price as close,
    exchange
  FROM 
    api-project-901373404215.digital_assets.nomics_exchange_market_ticker
  where 
    Timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
    AND quote IN ('USD','USDT','USDC')
    AND base = 'BTC'
    AND exchange = 'binance'
	) as b
	ON (
	b.base = a.base
	and 
	b.close <= a.upperfence
	)
group by 
  t.base,
t.exchange
  
  



