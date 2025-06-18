with 
		oneDay AS 
				(
					SELECT 
						main.id as symbol,
						((SUM(main.marketcap - oneDay.marketcap_change) - SUM(history.market_cap)) / SUM(history.market_cap))  as Market_cap_percent_change1D
					FROM 
						nomics_currencies_tickers main , 
						nomics_currencies_tickers_one_day oneDay, 
						nomics_market_cap_history history
					where 
						main.id = oneDay.id
						and oneDay.id = 'ETH'
						and history.timestamp >= cast(now() - INTERVAL '24 HOUR' as timestamp)
						and oneDay.last_updated >= cast(now() - INTERVAL '24 HOUR' as timestamp)
				group by main.id
				),
		sevenDays AS 
				(
					SELECT 
						main.id as symbol,
						((SUM(main.marketcap - sevenDays.marketcap_change) - SUM(history.market_cap)) / SUM(history.market_cap))  as Market_cap_percent_change7D
					FROM 
						nomics_currencies_tickers main, 
						nomics_currencies_tickers_seven_days sevenDays, 
						nomics_market_cap_history history
					where 
						main.id = sevenDays.id
						and sevenDays.id = 'ETH'
						and history.timestamp >= cast(now() - INTERVAL '7 DAYS' as timestamp)
						and sevenDays.last_updated >= cast(now() - INTERVAL '7 DAYS' as timestamp)
					group by main.id
				),
		thirtyDays AS 
				(
					SELECT 
						main.id as symbol,
						((SUM(main.marketcap - thirtyDays.marketcap_change) - SUM(history.market_cap)) / SUM(history.market_cap))  as Market_cap_percent_change30D
					FROM 
						nomics_currencies_tickers main, 
						nomics_currencies_tickers_thirty_days thirtyDays, 
						nomics_market_cap_history history
					where 
						main.id = thirtyDays.id
						and thirtyDays.id = 'ETH'
						and history.timestamp >= cast(now() - INTERVAL '30 DAYS' as timestamp)
						and thirtyDays.last_updated >= cast(now() - INTERVAL '30 DAYS' as timestamp)
					group by main.id
				),
		oneYear AS 
				(
					SELECT 
						main.id as symbol,
						((SUM(main.marketcap - oneYear.marketcap_change) - SUM(history.market_cap)) / SUM(history.market_cap))  as Market_cap_percent_change1Y
					FROM 
						nomics_currencies_tickers main, 
						nomics_currencies_tickers_one_year oneYear, 
						nomics_market_cap_history history
					where 
						main.id = oneYear.id
						and oneYear.id = 'ETH'
						and history.timestamp >= cast(now() - INTERVAL '365 DAYS' as timestamp)
						and oneYear.last_updated >= cast(now() - INTERVAL '365 DAYS' as timestamp)
					group by main.id
				),

		YTD AS 
				(
					SELECT 
						main.id as symbol,
						((SUM(main.marketcap - ytd.marketcap_change) - SUM(history.market_cap)) / SUM(history.market_cap))  as Market_cap_percent_changeYTD
					FROM 
						nomics_currencies_tickers main, 
						nomics_currencies_tickers_ytd ytd, 
						nomics_market_cap_history history
					where 
						main.id = ytd.id
						and ytd.id = 'ETH'
						and history.timestamp  >= cast(date_trunc('year', current_date) as timestamp)
						and ytd.last_updated  >= cast(date_trunc('year', current_date) as timestamp) 
					group by main.id
				)
		select 
			CAST(oneDay.Market_cap_percent_change1D AS FLOAT) AS Market_cap_percent_change1D,
			CAST(sevenDays.Market_cap_percent_change7D AS FLOAT) AS Market_cap_percent_change7D,
			CAST(thirtyDays.Market_cap_percent_change30D AS FLOAT) AS Market_cap_percent_change30D,
			CAST(oneYear.Market_cap_percent_change1Y AS FLOAT) AS Market_cap_percent_change1Y,
			CAST(YTD.Market_cap_percent_changeYTD AS FLOAT) AS Market_cap_percent_changeYTD
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