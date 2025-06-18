with 
	allTime as (
			SELECT 
				CAST(MIN(circulating_supply) AS FLOAT) circulating_supply, 
				CAST(MIN(price) AS FLOAT) price24h,
				max_supply,
				num_pairs,
				CAST(MIN(marketcap) AS FLOAT) marketcap, 
				id
			FROM nomics_currencies_tickers
			where id = 'BTC'
			group by 
				id,
				num_pairs,
				max_supply
			order by marketcap desc
			limit 1
			),
	oneDay AS (
			SELECT 
				CAST(MIN(oneDay.volume) AS FLOAT) volume_24h, 
				CAST(MIN(oneDay.price_change) AS FLOAT) change_value_24h, 
				CAST(MIN(oneDay.price_change_pct) AS FLOAT) percentage_24h,
				oneDay.id,
			((SUM(main.marketcap - oneDay.marketcap_change) - SUM(history.market_cap)) / SUM(history.market_cap))  as Market_cap_percent_change1D
			FROM 
				nomics_currencies_tickers main , 
				nomics_currencies_tickers_one_day oneDay, 
				nomics_market_cap_history history
			where main.id = oneDay.id
				and history.timestamp >= cast(now() - INTERVAL '24 HOUR' as timestamp)
				and oneDay.last_updated >= cast(now() - INTERVAL '24 HOUR' as timestamp)
				and oneDay.id = 'BTC'
			group by 
				oneDay.id
			),

	sevenDays AS (
			SELECT 
				CAST(MIN(sevenDays.price_change) AS FLOAT) price_7d, 
				CAST(MIN(sevenDays.price_change_pct) AS FLOAT) percentage_7d,
				sevenDays.id,
				((SUM(main.marketcap - sevenDays.marketcap_change) - SUM(history.market_cap)) / SUM(history.market_cap))  as Market_cap_percent_change7D
			FROM 
				nomics_currencies_tickers main, 
				nomics_currencies_tickers_seven_days sevenDays, 
				nomics_market_cap_history history
			where main.id = sevenDays.id
					and sevenDays.id = 'BTC'
					and history.timestamp >= cast(now() - INTERVAL '7 DAYS' as timestamp)
					and sevenDays.last_updated >= cast(now() - INTERVAL '7 DAYS' as timestamp)
			group by 
				sevenDays.id
				),
	thirtyDays AS (SELECT 
				CAST(MIN(thirtyDays.price_change) AS FLOAT) price_30d, 
				CAST(MIN(thirtyDays.price_change_pct) AS FLOAT) percentage_30d,
				thirtyDays.id,
				 ((SUM(main.marketcap - thirtyDays.marketcap_change) - SUM(history.market_cap)) / SUM(history.market_cap))  as Market_cap_percent_change30D
			FROM 
			   nomics_currencies_tickers main, 
				nomics_currencies_tickers_thirty_days thirtyDays, 
				nomics_market_cap_history history
			where main.id = thirtyDays.id
					and thirtyDays.id = 'BTC'
					and history.timestamp >= cast(now() - INTERVAL '30 DAYS' as timestamp)
					and thirtyDays.last_updated >= cast(now() - INTERVAL '30 DAYS' as timestamp)
			group by 
				thirtyDays.id
				),
	oneYear AS (SELECT 
				CAST(MIN(oneYear.price_change) AS FLOAT) price_1y, 
				CAST(MIN(oneYear.price_change_pct) AS FLOAT) percentage_1y,
				oneYear.id,
				((SUM(main.marketcap - oneYear.marketcap_change) - SUM(history.market_cap)) / SUM(history.market_cap))  as Market_cap_percent_change1Y
			FROM 
				nomics_currencies_tickers main, 
				nomics_currencies_tickers_one_year oneYear, 
				nomics_market_cap_history history
			where main.id = oneYear.id
					and oneYear.id = 'BTC'
					and history.timestamp >= cast(now() - INTERVAL '365 DAYS' as timestamp)
					and oneYear.last_updated >= cast(now() - INTERVAL '365 DAYS' as timestamp)
			group by 
				oneYear.id
			),

	YTD AS (SELECT 
				CAST(MIN(ytd.price_change) AS FLOAT) price_ytd, 
				CAST(MIN(ytd.price_change_pct) AS FLOAT) percentage_ytd,
				ytd.id,
			((SUM(main.marketcap - ytd.marketcap_change) - SUM(history.market_cap)) / SUM(history.market_cap))  as Market_cap_percent_changeYTD
			FROM 
				nomics_currencies_tickers main, 
				nomics_currencies_tickers_ytd ytd, 
				nomics_market_cap_history history
			where main.id = ytd.id
				and ytd.id = 'BTC'
				and history.timestamp  >= cast(date_trunc('year', current_date) as timestamp)
				and ytd.last_updated  >= cast(date_trunc('year', current_date) as timestamp)
			group by 
				ytd.id
		),
	metaData AS (SELECT 
				original_symbol,
				id
			FROM nomics_ticker_metadata
			where id = 'BTC'
		)
	select num_pairs,
			max_supply,
			metaData.original_symbol,
			CAST(MIN(allTime.circulating_supply) AS FLOAT) circulating_supply, 
			CAST(MIN(allTime.marketcap) AS FLOAT) marketcap, 
			CAST(MIN(allTime.price24h) AS FLOAT) price24h,
			CAST(MIN(sevenDays.price_7d) AS FLOAT) price_7d,
			CAST(MIN(thirtyDays.price_30d) AS FLOAT) price_30d, 
			CAST(MIN(oneYear.price_1y) AS FLOAT) price_1y, 
			CAST(MIN(YTD.price_ytd) AS FLOAT) price_ytd, 
			CAST(MIN(oneDay.volume_24h) AS FLOAT) volume_24h, 
			CAST(MIN(oneDay.change_value_24h) AS FLOAT) change_value_24h, 
			CAST(MIN(oneDay.percentage_24h) AS FLOAT) percentage_24h,
			CAST(MIN(sevenDays.percentage_7d) AS FLOAT) percentage_7d, 
			CAST(MIN(thirtyDays.percentage_30d) AS FLOAT) percentage_30d, 
			CAST(MIN(oneYear.percentage_1y) AS FLOAT) percentage_1y,
			CAST(MIN(YTD.percentage_ytd) AS FLOAT) percentage_ytd,
			CAST(oneDay.Market_cap_percent_change1D AS FLOAT) AS Market_cap_percent_change1D,
			CAST(sevenDays.Market_cap_percent_change7D AS FLOAT) AS Market_cap_percent_change7D,
			CAST(thirtyDays.Market_cap_percent_change30D AS FLOAT) AS Market_cap_percent_change30D,
			CAST(oneYear.Market_cap_percent_change1Y AS FLOAT) AS Market_cap_percent_change1Y,
			CAST(YTD.Market_cap_percent_changeYTD AS FLOAT) AS Market_cap_percent_changeYTD
		from allTime
				INNER JOIN 
					sevenDays 
				ON 
					sevenDays.id = allTime.id
				INNER JOIN 
					thirtyDays 
				ON 
					thirtyDays.id = allTime.id
				INNER JOIN 
					oneYear 
				ON 
					oneYear.id = allTime.id
				INNER JOIN 
					oneDay 
				ON 
					oneDay.id = allTime.id
				INNER JOIN 
					YTD 
				ON 
					YTD.id = allTime.id
				INNER JOIN 
					metaData 
				ON 
					metaData.id = allTime.id
		group by 
			allTime.id, 
			metaData.original_symbol, 
			num_pairs, max_supply,
			oneDay.Market_cap_percent_change1D,
			sevenDays.Market_cap_percent_change7D,
			thirtyDays.Market_cap_percent_change30D,
			oneYear.Market_cap_percent_change1Y,
			YTD.Market_cap_percent_changeYTD
				
				
				
				
				
				
				
				
				
				
				