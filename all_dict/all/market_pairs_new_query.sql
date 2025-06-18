with
	allTime as 
			(
				SELECT 
					lower(base) as symbol
				FROM 
					( 
						SELECT base 
						FROM 
							nomics_ohlcv_candles
						GROUP BY 
							base, 
							timestamp
					) as allTime
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
					AND timestamp >=  cast(now() - INTERVAL '10 HOUR' as timestamp)
					AND status = 'active'
					AND quote IN ('USD', 'USDT', 'USDC')
				group by 
					base,
					exchange
			),
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
					CAST(Min(volume_for_pair_1d) As FLOAT) volume_for_pair_1d,  
					CAST(MIN(Price) AS FLOAT) current_price_for_pair_1d
				from
				(
					select
						oneDay.Symbol,
						oneDay.volume_for_pair_1d,
						ticker.price
					from 
					(
						(
						SELECT 
							AVG(volume) as volume_for_pair_1d , 
							lower(base) as Symbol, 
							exchange
						from 
							nomics_exchange_market_ticker_one_day
						where 
							last_updated >= cast(now() - INTERVAL '24 HOUR' as timestamp)
						group by 
							base, 
							exchange
					) as oneDay
					JOIN (
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
						) as ticker
						On (
							oneDay.Symbol = ticker.Symbol
						)
					)
				) as oneDay
				group by Symbol
			),
		sevenDays As 
			(
				SELECT 
					Symbol, 
					CAST(Min(volume_for_pair_7d) As FLOAT) volume_for_pair_7d,  
					CAST(MIN(Price) AS FLOAT) current_price_for_pair_7d
				from
				(
					select
						sevenDay.Symbol,
						sevenDay.volume_for_pair_7d,
						ticker.price
					from 
					(
						(
						SELECT 
							AVG(volume) as volume_for_pair_7d , 
							lower(base) as Symbol, 
							exchange
						from 
							nomics_exchange_market_ticker_seven_days
						where 
							last_updated >= cast(now() - INTERVAL '7 DAYS' as timestamp)
						group by 
							base, 
							exchange
					) as sevenDay
					JOIN (
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
						) as ticker
						On (
							sevenDay.Symbol = ticker.Symbol
						)
					)
				) as sevenDay
				group by Symbol
			),
		thirtyDays As 
			(
				SELECT 
					Symbol, 
					CAST(Min(volume_for_pair_30d) As FLOAT) volume_for_pair_30d,  
					CAST(MIN(Price) AS FLOAT) current_price_for_pair_30d
				from
				(
					select
						thirtyDay.Symbol,
						thirtyDay.volume_for_pair_30d,
						ticker.price
					from 
					(
						(
						SELECT 
							AVG(volume) as volume_for_pair_30d , 
							lower(base) as Symbol, 
							exchange
						from 
							nomics_exchange_market_ticker_thirty_days
						where 
							last_updated >= cast(now() - INTERVAL '30 DAYS' as timestamp)
						group by 
							base, 
							exchange
					) as thirtyDay
					JOIN (
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
						) as ticker
						On (
							thirtyDay.Symbol = ticker.Symbol
						)
					)
				) as thirtyDay
				group by Symbol
			),
		oneYear As 
			(
				SELECT 
					Symbol, 
					CAST(Min(volume_for_pair_1y) As FLOAT) volume_for_pair_1y,  
					CAST(MIN(Price) AS FLOAT) current_price_for_pair_1y
				from
				(
					select
						oneYear.Symbol,
						oneYear.volume_for_pair_1y,
						ticker.price
					from 
					(
						(
						SELECT 
							AVG(volume) as volume_for_pair_1y , 
							lower(base) as Symbol, 
							exchange
						from 
							nomics_exchange_market_ticker_one_year
						where 
							last_updated >= cast(now() - INTERVAL '365 DAYS' as timestamp)
						group by 
							base, 
							exchange
					) as oneYear
					JOIN (
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
						) as ticker
						On (
							oneYear.Symbol = ticker.Symbol
						)
					)
				) as oneYear
				group by Symbol
			),
		YTD As 
			(
				SELECT 
					Symbol, 
					CAST(Min(volume_for_pair_ytd) As FLOAT) volume_for_pair_ytd,  
					CAST(MIN(Price) AS FLOAT) current_price_for_pair_ytd
				from
				(
					select
						ytd.Symbol,
						ytd.volume_for_pair_ytd,
						ticker.price
					from 
					(
						(
						SELECT 
							AVG(volume) as volume_for_pair_ytd , 
							lower(base) as Symbol, 
							exchange
						from 
							nomics_exchange_market_ticker_ytd
						where 
							last_updated >= cast(date_trunc('year', current_date) as timestamp)
						group by 
							base, 
							exchange
					) as ytd
					JOIN (
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
						) as ticker
						On (
							ytd.Symbol = ticker.Symbol
						)
					)
				) as ytd
				group by Symbol
			)

select 
	array_to_json(ARRAY_AGG(json_build_object('Market', ExchangesPrices.Market, 'Symbol', ExchangesPrices.Symbol))) as Exchanges,
	allTime.symbol,
	array_to_json(ARRAY_AGG(json_build_object(
                                        'base', market.Symbol, 
                                        'exchange', market.exchange, 
                                        'quote', market.quote, 
                                        'pair', market.pair, 												 
										'pairStatus', assets.status, 
										'update_timestamp', assets.last_updated,
                                        'type', ticker.type,
                                        'current_price_for_pair_1d', CAST(oneDay.current_price_for_pair_1d AS FLOAT),
                                        'current_price_for_pair_7d', CAST(sevenDays.current_price_for_pair_7d AS FLOAT),
                                        'current_price_for_pair_30d', CAST(thirtyDays.current_price_for_pair_30d AS FLOAT),
                                        'current_price_for_pair_1y', CAST(oneYear.current_price_for_pair_1y AS FLOAT),
                                        'current_price_for_pair_ytd', CAST(YTD.current_price_for_pair_ytd AS FLOAT),
                                        'volume_for_pair_1d', CAST(oneDay.volume_for_pair_1d AS FLOAT),
                                        'volume_for_pair_7d', CAST(sevenDays.volume_for_pair_7d AS FLOAT),
                                        'volume_for_pair_30d', CAST(thirtyDays.volume_for_pair_30d AS FLOAT),
                                        'volume_for_pair_1y', CAST(oneYear.volume_for_pair_1y AS FLOAT),
                                        'volume_for_pair_ytd', CAST(YTD.volume_for_pair_ytd AS FLOAT)
                                        ))) as MarketPairs
from
	allTime 
	INNER JOIN 
		ExchangesPrices 
	ON 
		ExchangesPrices.Symbol = allTime.symbol
	INNER JOIN
		assets
	ON
		assets.base = allTime.symbol
	INNER JOIN 
		market
	ON
		market.Symbol = allTime.symbol
	INNER JOIN 
		ticker
	ON
		ticker.base = allTime.symbol
	INNER JOIN 
		oneDay 
	ON
		oneDay.symbol = allTime.symbol
	INNER JOIN 
		sevenDays 
	ON
		sevenDays.symbol = allTime.symbol
	INNER JOIN 
		thirtyDays 
	ON
		thirtyDays.symbol = allTime.symbol
	INNER JOIN 
		oneYear 
	ON
		oneYear.symbol = allTime.symbol
	INNER JOIN 
		YTD 
	ON
		YTD.symbol = allTime.symbol
	group by 
		allTime.symbol
		
		
	
	
	
	