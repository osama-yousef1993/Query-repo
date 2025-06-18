
market pairs volume 

with 
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
							base = '0XCX'
							and exchange = 'currency'
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
							base = '0XCX'
							and exchange = 'currency'
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
							nomics_exchange_market_ticker_thirty_days
						where 
							base = '0XCX'
							and exchange = 'currency'
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
							base = '0XCX'
							and exchange = 'currency'
							and last_updated >= cast(now() - INTERVAL '365 DAYS' as timestamp)
						group by 
							base, 
							exchange
					) as oneYear
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
							base = '0XCX'
							and exchange = 'currency'
							and last_updated  >= cast(date_trunc('year', current_date) as timestamp)
						group by 
							base, 
							exchange
					) as ytd
				group by 
					Symbol
			)
		SELECT
			CAST(MIN(oneDay.volume_for_pair_1d) AS FLOAT) AS volume_for_pair_1d,
			CAST(MIN(sevenDays.volume_for_pair_7d) AS FLOAT) AS volume_for_pair_7d,
			CAST(MIN(thirtyDays.volume_for_pair_30d) AS FLOAT) AS volume_for_pair_30d,
			CAST(MIN(oneYear.volume_for_pair_1y) AS FLOAT) AS volume_for_pair_1y,
			CAST(MIN(YTD.volume_for_pair_ytd) AS FLOAT) AS volume_for_pair_ytd

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
		group by 
			oneDay.symbol


++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
