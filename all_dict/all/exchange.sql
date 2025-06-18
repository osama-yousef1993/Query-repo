with 
		allTime as 
			(
			SELECT lower(base) as symbol 
			FROM 
				nomics_ohlcv_candles
			where base = 'BTC'
			GROUP BY 
				base
			),
		ExchangesPrices AS 
			( 
				SELECT 
					lower(base) as Symbol, 
					exchange as Market
				FROM 
					nomics_exchange_market_ticker
				WHERE 
					exchange NOT IN ('bitmex','hbtc')
					AND base = 'BTC'
					AND type = 'spot'
					AND timestamp >=  cast(now() - INTERVAL '30 MINUTE' as timestamp)
					AND status = 'active'
					AND quote IN ('USD', 'USDT', 'USDC')
				group by 
					base,
					exchange
			)
		select 
			array_to_json(ARRAY_AGG(json_build_object('Market', ExchangesPrices.Market, 'Symbol', ExchangesPrices.Symbol))) as Exchanges,
			allTime.symbol
		from 
			allTime 
			INNER JOIN 
				ExchangesPrices 
			ON 
				ExchangesPrices.Symbol = allTime.symbol
		group by 
			allTime.symbol