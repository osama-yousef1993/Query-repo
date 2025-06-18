with 
		allTime As 
			(
				SELECT 
					Symbol, 
					type,
                    CAST(MIN(price_1d) AS FLOAT) current_price_for_pair_1d,
                    CAST(MIN(price_7d) AS FLOAT) current_price_for_pair_7d,
                    CAST(MIN(price_30d) AS FLOAT) current_price_for_pair_30d,
                    CAST(MIN(price_1y) AS FLOAT) current_price_for_pair_1y,
                    CAST(MIN(price_ytd) AS FLOAT) current_price_for_pair_ytd
				from 
					(
						SELECT 
							type, 
							lower(base) Symbol,
							(	
								select 
									AVG(price) price 
								from 
									nomics_exchange_market_ticker 
								where 
									timestamp >= cast(now() - INTERVAL '24 HOUR' as timestamp)
							) as price_1d,
							(	
								select 
									AVG(price) price 
								from 
									nomics_exchange_market_ticker 
								where 
									timestamp >= cast(now() - INTERVAL '7 DAYS' as timestamp)
							) as price_7d,
							(	
								select 
									AVG(price) price 
								from 
									nomics_exchange_market_ticker 
								where 
									timestamp >= cast(now() - INTERVAL '30 DAYS' as timestamp)
							) as price_30d,
							(	
								select 
									AVG(price) price 
								from 
									nomics_exchange_market_ticker 
								where 
									timestamp >= cast(now() - INTERVAL '365 DAYS' as timestamp)
							) as price_1y,
							(	
								select 
									AVG(price) price 
								from 
									nomics_exchange_market_ticker 
								where 
									timestamp >= cast(date_trunc('year', current_date) as timestamp)
							) as price_ytd
							
						from 
							nomics_exchange_market_ticker
						where 
							base = 'BTC'
							and exchange = 'binance_us'
						and type != ''
					) as allTime
				group by 
					Symbol, 
					type
			),
		oneDay As 
			(
				SELECT 
					Symbol, 
					CAST(Min(volume_for_pair_1d) As FLOAT) volume_for_pair_1d
				from 
					(
						SELECT 
							AVG(volume) as volume_for_pair_1d , 
							lower(base) as Symbol, 
							exchange
						from 
							nomics_exchange_market_ticker_one_day
						where 
							base = 'BTC'
							and exchange = 'binance_us'
							and last_updated >= cast(now() - INTERVAL '24 HOUR' as timestamp)
						group by 
							base, 
							exchange
					) as oneDay
				group by 
					Symbol
			),
		sevenDays As 
			(
				SELECT 
					Symbol, 
					CAST(Min(volume_for_pair_7d) As FLOAT) volume_for_pair_7d
				from 
					(
						SELECT 
							AVG(volume) as volume_for_pair_7d , 
							lower(base) as Symbol,
							exchange
						from 
							nomics_exchange_market_ticker_seven_days
						where 
                             base = 'BTC'
							and exchange = 'binance_us'
							and last_updated >= cast(now() - INTERVAL '7 DAYS' as timestamp)
						group by 
							base, 
							exchange
					) as sevenDay
				group by 
					Symbol
			),
		thirtyDays As 
			(
				SELECT 
					Symbol,
					CAST(Min(volume_for_pair_30d) As FLOAT) volume_for_pair_30d
				from 
					(
						SELECT 
							AVG(volume) as volume_for_pair_30d , 
							lower(base) as Symbol, 
							exchange
						from 
							nomics_exchange_market_ticker_thirty_days thirty
						where 
							base = 'BTC'
							and exchange = 'binance_us'
							and last_updated >= cast(now() - INTERVAL '30 DAYS' as timestamp)
						group by 
							base, 
							exchange
					) as thirtyDay
				group by 
					Symbol
			),
		oneYear As 
			(
				SELECT 
					Symbol,
					CAST(Min(volume_for_pair_1y) As FLOAT) volume_for_pair_1y
				from 
					(
						SELECT 
							AVG(volume) as volume_for_pair_1y , 
							lower(base) as Symbol, 
							exchange
						from 
							nomics_exchange_market_ticker_one_year
						where 
							base = 'BTC'
							and exchange = 'binance_us'
							and last_updated >= cast(now() - INTERVAL '365 DAYS' as timestamp)
						group by 
							base, 
							exchange
					) as oneyear
				group by 
					Symbol
			),
		YTD As 
			(
				SELECT 
					Symbol, 
					CAST(Min(volume_for_pair_ytd) As FLOAT) volume_for_pair_ytd
				from 
					(
						SELECT 
							AVG(volume) as volume_for_pair_ytd , 
							lower(base) as Symbol, 
							exchange
						from 
							nomics_exchange_market_ticker_ytd ytd
						where 
							base = 'BTC'
							and exchange = 'binance_us'
							and last_updated  >= cast(date_trunc('year', current_date) as timestamp)
						group by 
							base, 
							exchange
					) as ytd
				group by 
					Symbol
			)
		SELECT
			CAST(allTime.current_price_for_pair_1d AS FLOAT) AS current_price_for_pair_1d,
			CAST(allTime.current_price_for_pair_7d AS FLOAT) AS current_price_for_pair_7d,
			CAST(allTime.current_price_for_pair_30d AS FLOAT) AS current_price_for_pair_30d,
			CAST(allTime.current_price_for_pair_1y AS FLOAT) AS current_price_for_pair_1y,
			CAST(allTime.current_price_for_pair_ytd AS FLOAT) AS current_price_for_pair_ytd,
			CAST(oneDay.volume_for_pair_1d AS FLOAT) AS volume_for_pair_1d,
			CAST(sevenDays.volume_for_pair_7d AS FLOAT) AS volume_for_pair_7d,
			CAST(thirtyDays.volume_for_pair_30d AS FLOAT) AS volume_for_pair_30d,
			CAST(oneYear.volume_for_pair_1y AS FLOAT) AS volume_for_pair_1y,
			CAST(YTD.volume_for_pair_ytd AS FLOAT) AS volume_for_pair_ytd,
			allTime.type

		from oneDay 
			INNER JOIN 
				sevenDays 
			ON
			 	sevenDays.symbol = oneDay.symbol
			INNER JOIN 
				thirtyDays 
			ON
			 	thirtyDays.symbol = oneDay.symbol
			INNER JOIN 
				oneYear 
			ON
			 	oneYear.symbol = oneDay.symbol
			INNER JOIN 
				YTD 
			ON
			 	YTD.symbol = oneDay.symbol
			INNER JOIN 
				allTime 
			ON
				 allTime.symbol = oneDay.symbol
		group by 
			oneDay.symbol,
			allTime.type,
			oneDay.volume_for_pair_1d,
			sevenDays.volume_for_pair_7d,
			thirtyDays.volume_for_pair_30d,
			oneYear.volume_for_pair_1y,
			YTD.volume_for_pair_ytd,
            allTime.current_price_for_pair_1d,
            allTime.current_price_for_pair_7d,
            allTime.current_price_for_pair_30d,
            allTime.current_price_for_pair_1y,
            allTime.current_price_for_pair_ytd