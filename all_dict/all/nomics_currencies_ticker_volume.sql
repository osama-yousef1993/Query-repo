with 
	oneDay AS 
		(
			select 
				((max_volume - min_volume) / min_volume) as percentage_1d, 
				volume
			from 
				(
					select 
					volume,
						(select 
							max(volume)  
							from nomics_currencies_tickers_one_day 
							where last_updated >= cast(now() - INTERVAL '24 HOUR' as timestamp)
						 and id = 'BTC'
						) as max_volume, 
						(select 
							min(volume)  
							from nomics_currencies_tickers_one_day 
							where last_updated <= cast(now() - INTERVAL '24 HOUR' as timestamp)
						 and id = 'BTC'
						) as min_volume
					from 
						nomics_currencies_tickers_one_day
					where 
						last_updated >= cast(now() - INTERVAL '24 HOUR' as timestamp)
					and id = 'BTC'
					group by volume, last_updated
					order by last_updated desc
					limit 1
				) as oneDay
		),

	sevenDays AS 
		(
			select 
				((max_volume - min_volume) / min_volume) as percentage_7d, 
				volume
			from 
				(
					select 
					volume,
						(select 
							max(volume)  
							from nomics_currencies_tickers_seven_days 
							where last_updated >= cast(now() - INTERVAL '7 DAYS' as timestamp)
						 and id = 'BTC'
						) as max_volume, 
						(select 
							min(volume)  
							from nomics_currencies_tickers_seven_days 
							where last_updated >= cast(now() - INTERVAL '7 DAYS' as timestamp)
						 and id = 'BTC'
						) as min_volume

					from 
						nomics_currencies_tickers_seven_days
					where 
						last_updated >= cast(now() - INTERVAL '7 DAYS' as timestamp)
					and id = 'BTC'
					group by volume, last_updated
					order by last_updated desc
					limit 1
				) as sevenDays
		),
	thirtyDays AS 
		(
			select 
				((max_volume - min_volume) / min_volume) as percentage_30d, 
				volume
			from 
				(
					select 
					volume,
						(select 
							max(volume)  
							from nomics_currencies_tickers_thirty_days 
							where last_updated >= cast(now() - INTERVAL '30 DAYS' as timestamp)
						 and id = 'BTC'
						) as max_volume, 
						(select 
							min(volume)  
							from nomics_currencies_tickers_thirty_days 
							where last_updated >= cast(now() - INTERVAL '30 DAYS' as timestamp)
						 and id = 'BTC'
						) as min_volume
					from 
						nomics_currencies_tickers_thirty_days
					where 
						last_updated >= cast(now() - INTERVAL '30 DAYS' as timestamp)
					and id = 'BTC'
					group by volume, last_updated
					order by last_updated desc
					limit 1
				) as thirtyDays
		),
	oneYear AS 
		(
			select 
				((max_volume - min_volume) / min_volume) as percentage_1y, 
				volume
			from 
				(
					select 
					volume,
						(select 
							max(volume)  
							from nomics_currencies_tickers_one_year 
							where last_updated >= cast(now() - INTERVAL '365 DAYS' as timestamp)
						 and id = 'BTC'
						) as max_volume, 
						(select 
							min(volume)  
							from nomics_currencies_tickers_one_year 
							where last_updated >= cast(now() - INTERVAL '365 DAYS' as timestamp)
						 and id = 'BTC'
						) as min_volume
					from 
						nomics_currencies_tickers_one_year
					where 
						last_updated >= cast(now() - INTERVAL '365 DAYS' as timestamp)
					and id = 'BTC'
					group by volume, last_updated
					order by last_updated desc
					limit 1
				) as oneYear
		),

	YTD AS 
		(
			select 
				((max_volume - min_volume) / min_volume) as percentage_ytd, 
				volume
			from 
				(
					select 
					volume,
						(select 
							max(volume)  
							from nomics_currencies_tickers_one_year 
							where last_updated >= cast(date_trunc('year', current_date) as timestamp)
						 and id = 'BTC'
						) as max_volume, 
						(select 
							min(volume)  
							from nomics_currencies_tickers_one_year 
							where last_updated >= cast(date_trunc('year', current_date) as timestamp)
						 and id = 'BTC'
						) as min_volume
					from 
						nomics_currencies_tickers_one_year
					where 
						last_updated >= cast(date_trunc('year', current_date) as timestamp)
					and id = 'BTC'
					group by volume, last_updated
					order by last_updated desc
					limit 1
				) as ytd	
	   )
	select 
			CAST((oneDay.percentage_1d) AS FLOAT) percentage_1d,
			CAST((oneDay.volume) AS FLOAT) volume_1d,
			CAST((sevenDays.percentage_7d) AS FLOAT) percentage_7d,
			CAST((sevenDays.volume) AS FLOAT) volume_7d,
			CAST((thirtyDays.percentage_30d) AS FLOAT) percentage_30d,
			CAST((thirtyDays.volume) AS FLOAT) volume_30d,
			CAST((oneYear.percentage_1y) AS FLOAT) percentage_1y,
			CAST((oneYear.volume) AS FLOAT) volume_1y,
			CAST((YTD.percentage_ytd) AS FLOAT) percentage_ytd,
			CAST((YTD.volume) AS FLOAT) volume_ytd
		from 
			oneDay,
			sevenDays,
			thirtyDays,
			oneYear,
			YTD