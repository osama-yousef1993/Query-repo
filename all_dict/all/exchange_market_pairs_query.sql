with 
	fundExchanges as (
		with
		oneDay as 
			(
				SELECT 
					lower(base) as symbol
				FROM 
					( 
						SELECT base 
						FROM 
							nomics_ohlcv_candles
						where timestamp >= cast(now() - INTERVAL '24 HOUR' as timestamp)
						GROUP BY 
							base, 
							timestamp
					) as oneDay
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
					AND type = 'spot'
					AND timestamp >=  cast(now() - INTERVAL '8 HOUR' as timestamp)
					AND status = 'active'
					AND quote IN ('USD', 'USDT', 'USDC')
				group by 
					base,
					exchange
			)
		select 
			array_to_json(ARRAY_AGG(json_build_object('Market', ExchangesPrices.Market, 'Symbol', ExchangesPrices.Symbol))) as Exchanges,
			oneDay.symbol
		from 
			oneDay 
			INNER JOIN 
				ExchangesPrices 
			ON 
				ExchangesPrices.Symbol = oneDay.symbol
		group by 
			oneDay.symbol
	),
	fundMarketPairs as (
		with
	market as (
		select
			lower(base) as Symbol, 
			exchange, 
			quote , 
			CONCAT(base, quote) as pair
		from 
			nomics_markets
		group by
			base,
			exchange, 
			quote
			
	),
	assets as (
		select
			lower(id) as base,
			status, 
			last_updated
		from 
			nomics_assets
		group by 
			id
	
	),
	ticker as (
		select
			lower(base) as base,
			type
		from 
			nomics_exchange_market_ticker
		where 
			type != ''
		group by
			base,
			type
	),
	oneDay As 
			(
				SELECT 
					Symbol,   
					CAST(MIN(Price) AS FLOAT) current_price_for_pair_1d
				from
					(
						SELECT 
							lower(base) as Symbol,
							exchange,
							AVG(price) price
						from 
							nomics_exchange_market_ticker
						where 
							timestamp >= cast(now() - INTERVAL '24 HOUR' as timestamp)
						group by 
							base, 
							exchange
					) as oneDay
				group by Symbol
			),
		sevenDays As 
			(
				SELECT 
					Symbol,   
					CAST(MIN(Price) AS FLOAT) current_price_for_pair_7d
				from
					(
						SELECT 
							lower(base) as Symbol,
							exchange,
							AVG(price) price
						from 
							nomics_exchange_market_ticker
						where 
							timestamp >= cast(now() - INTERVAL '7 DAYS' as timestamp)
						group by 
							base, 
							exchange
					) as sevenDays
				group by Symbol
			),
		thirtyDays As 
			(
				SELECT 
					Symbol,   
					CAST(MIN(Price) AS FLOAT) current_price_for_pair_30d
				from
					(
						SELECT 
							lower(base) as Symbol,
							exchange,
							AVG(price) price
						from 
							nomics_exchange_market_ticker
						where 
							timestamp >= cast(now() - INTERVAL '30 DAYS' as timestamp)
						group by 
							base, 
							exchange
					) as thirtyDays
				group by Symbol
			),
		oneYear As 
			(
				SELECT 
					Symbol,   
					CAST(MIN(Price) AS FLOAT) current_price_for_pair_1y
				from
					(
						SELECT 
							lower(base) as Symbol,
							exchange,
							AVG(price) price
						from 
							nomics_exchange_market_ticker
						where 
							timestamp >= cast(now() - INTERVAL '365 DAYS' as timestamp)
						group by 
							base, 
							exchange
					) as oneYear
				group by Symbol
			),
		YTD As 
			(
				SELECT 
					Symbol,   
					CAST(MIN(Price) AS FLOAT) current_price_for_pair_ytd
				from
					(
						SELECT 
							lower(base) as Symbol,
							exchange,
							AVG(price) price
						from 
							nomics_exchange_market_ticker
						where 
							timestamp >= cast(date_trunc('year', current_date) as timestamp)
						group by 
							base, 
							exchange
					) as YTD
				group by Symbol
			)

		select 
			assets.base, 
			array_to_json(ARRAY_AGG(json_build_object(
												'base', market.Symbol, 
												'exchange', market.exchange, 
												'quote', market.quote, 
												'pair', market.pair, 												 
												'pairStatus', assets.status, 
												'update_timestamp', assets.last_updated,
												'TypeOfPair', ticker.type,
												'currentPriceForPair1D', CAST(oneDay.current_price_for_pair_1d AS FLOAT),
												'currentPriceForPair7D', CAST(sevenDays.current_price_for_pair_7d AS FLOAT),
												'currentPriceForPair30D', CAST(thirtyDays.current_price_for_pair_30d AS FLOAT),
												'currentPriceForPair1Y', CAST(oneYear.current_price_for_pair_1y AS FLOAT),
												'currentPriceForPairYTD', CAST(YTD.current_price_for_pair_ytd AS FLOAT)
												))) as MarketPairs
		from 
			assets
			INNER JOIN 
				market
			ON
				market.Symbol = assets.base
			INNER JOIN 
				ticker
			ON
				ticker.base = assets.base
			INNER JOIN 
				oneDay 
			ON
				oneDay.symbol = assets.base
			INNER JOIN 
				sevenDays 
			ON
				sevenDays.symbol = assets.base
			INNER JOIN 
				thirtyDays 
			ON
				thirtyDays.symbol = assets.base
			INNER JOIN 
				oneYear 
			ON
				oneYear.symbol = assets.base
			INNER JOIN 
				YTD 
			ON
				YTD.symbol = assets.base
			group by 
				assets.base
	)
select 
	fundExchanges.Exchanges,
	fundMarketPairs.MarketPairs,
	fundExchanges.symbol
from 
	fundExchanges
	INNER JOIN 
		fundMarketPairs
	ON
		fundMarketPairs.base = fundExchanges.symbol

	
	
	
	
	
	
	
	