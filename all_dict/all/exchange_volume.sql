with 
		exchangeMetadata as (
			select 
				id, 
				name,
				logo_url
			from 
				nomics_exchange_metadata
			where 
				id = '1inchv2'
		),
		exchangeHighLight as (
			select 
				num_markets,
				exchange
			from 
				nomics_exchange_highlight
			where 
				exchange = '1inchv2'
		),
	oneDay as (
		SELECT 
			exchange, 
			min(volume) as volume
		FROM 
			(
				select 
					exchange, 
					avg(volume) as volume
				from 
					nomics_exchange_market_ticker_one_day
				where 
					last_updated >= cast(now() - INTERVAL '24 HOUR' as timestamp)
					and exchange = '1inchv2'
				group by 
					exchange
			) as oneDay
		group by 
			exchange
	),
	sevenDays as (
		SELECT 
			exchange, 
			min(volume) as volume
		FROM 
			(
				select 
					exchange, 
					avg(volume) as volume
				from 
					nomics_exchange_market_ticker_seven_days
				where 
					last_updated >= cast(now() - INTERVAL '7 DAYS' as timestamp)
					and exchange = '1inchv2'
				group by 
					exchange
			) as sevenDays
		group by 
			exchange
	),
	thirtyDays as (
		SELECT 
			exchange, 
			min(volume) as volume
		FROM 
			(
				select 
					exchange, 
					avg(volume) as volume
				from 
					nomics_exchange_market_ticker_thirty_days
				where 
					last_updated >= cast(now() - INTERVAL '30 DAYS' as timestamp)
					and exchange = '1inchv2'
				group by 
					exchange
			) as thirtyDays
		group by 
			exchange
	),
	oneYear as (
		SELECT 
			exchange, 
			min(volume) as volume
		FROM 
			(
				select 
					exchange, 
					avg(volume) as volume
				from 
					nomics_exchange_market_ticker_one_year
				where 
					last_updated >= cast(now() - INTERVAL '365 DAYS' as timestamp)
					and exchange = '1inchv2'
				group by exchange
			) as oneYear
		group by 
			exchange
	),
	YTD as (
		SELECT 
			exchange, 
			min(volume) as volume
		FROM 
			(
				select 
					exchange, 
					avg(volume) as volume
				from 
					nomics_exchange_market_ticker_ytd
				where 
					last_updated >= cast(date_trunc('year', current_date) as timestamp)
					and exchange = '1inchv2'
				group by exchange
			) as YTD
		group by 
			exchange
	)
	select 
		exchangeMetadata.id, 
		exchangeMetadata.name, 
		exchangeMetadata.logo_url, 
		cast(exchangeHighLight.num_markets as int),
		cast(oneDay.volume as float) as volume_exchange_1d,
		cast(sevenDays.volume as float) as volume_exchange_7d,
		cast(thirtyDays.volume as float) as volume_exchange_30d,
		cast(oneYear.volume as float) as volume_exchange_1y,
		cast(YTD.volume as float) as volume_exchange_ytd
	from 
		exchangeMetadata
		INNER Join 
			exchangeHighLight
		ON 
			exchangeHighLight.exchange = exchangeMetadata.id
		INNER Join 
			oneDay
		ON 
			oneDay.exchange = exchangeMetadata.id
		INNER Join 
			sevenDays
		ON 
			sevenDays.exchange = exchangeMetadata.id
		INNER Join 
			thirtyDays
		ON 
			thirtyDays.exchange = exchangeMetadata.id
		INNER Join 
			oneYear
		ON 
			oneYear.exchange = exchangeMetadata.id
		INNER Join 
			YTD
		ON 
			YTD.exchange = exchangeMetadata.id