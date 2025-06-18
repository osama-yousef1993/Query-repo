with 
	oneDay as (
		SELECT sum(ticker.volume) as volume, ticker.id as base
		from 
			nomics_currencies_tickers_one_day ticker,
			nomics_exchange_market_ticker_one_day market
		where 
		ticker.id = market.base
		and ticker.last_updated >=  cast(now() - INTERVAL '24 HOUR' as timestamp)
		and ticker.id = 'BTC'
		and market.exchange = 'binance_us'
		group by ticker.id
	),
	sevenDays as (
		SELECT sum(volume) as volume, id as base
		from nomics_currencies_tickers_seven_days
		where last_updated >=  cast(now() - INTERVAL '7 DAYS' as timestamp)
		and id = 'BTC'
		group by id
	),
	thirtyDays as (
		SELECT sum(volume) as volume, id as base
		from nomics_currencies_tickers_thirty_days
		where last_updated >=  cast(now() - INTERVAL '30 DAYS' as timestamp)
		and id = 'BTC'
		group by id
	),
	oneYear as (
		SELECT sum(volume) as volume, id as base
		from nomics_currencies_tickers_one_year
		where last_updated >=  cast(now() - INTERVAL '365 DAYS' as timestamp)
		and id = 'BTC'
		group by id
	),
	ytd as (
		SELECT sum(volume) as volume, id as base
		from nomics_currencies_tickers_ytd
		where last_updated >=  cast(date_trunc('year', current_date) as timestamp)
		and id = 'BTC'
		group by id
	)
	select 
		cast(oneDay.volume as FLOAT) as volume_for_Pair_1d,
		cast(sevenDays.volume as FLOAT) as volume_for_Pair_7d,
		cast(thirtyDays.volume as FLOAT) as volume_for_Pair_30d,
		cast(oneYear.volume as FLOAT) as volume_for_Pair_1y,
		cast(ytd.volume as FLOAT) as volume_for_Pair_ytd
	from 
		oneDay
		INNER JOIN 
			sevenDays
		ON 
			sevenDays.base = oneDay.base
		INNER JOIN 
			thirtyDays
		ON 
			thirtyDays.base = oneDay.base
		INNER JOIN 
			oneYear
		ON 
			oneYear.base = oneDay.base
		INNER JOIN 
			ytd
		ON 
			ytd.base = oneDay.base
			
		
		
		
		
		
		
		
		
		
		
	
	