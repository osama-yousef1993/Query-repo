with oneYear as 
	(
	select base, exchange, market, quote, type, price as close, avg(volume) as volume,  timestamp
	from public.exchange_market_ticker
	where timestamp >= cast(now() - INTERVAL '365 Days' as timestamp)
	group by base, exchange, market, quote, type, timestamp, price
	)
select 
	 base, exchange, market, quote, type,
	 cast(low_24h as float ) as low_24h,
	 cast(low_7d as float) as low_7d,
	 cast(low_30d as float) as low_30d,
	 cast(low_1y as float) as low_1y,
	 cast(volume_1y as float ) as volume_1y,
	 cast(volume_30d as float) as volume_30d,
	 cast(volume_7d as float) as volume_7d,
	 cast(volume_24h as float) as volume_24h
from 
	(
		select 
			CAST(MIN( CASE WHEN timestamp >= cast(now() - INTERVAL '365 Days' as timestamp) THEN oneYear.Close END ) AS FLOAT) AS low_1y, 
			CAST(MIN(CASE WHEN  timestamp >=cast(now() - INTERVAL '30 Days' as timestamp) THEN oneYear.Close END) AS FLOAT) AS low_30d, 
			CAST(MIN( CASE WHEN timestamp >=cast(now() - INTERVAL '7 Days' as timestamp) THEN oneYear.Close  END) AS FLOAT) AS low_7d,
			CAST(MIN( CASE WHEN timestamp >= cast(now() - INTERVAL '3 Days' as timestamp)  THEN oneYear.Close END) AS FLOAT) AS low_24h,
			CAST(MIN( CASE WHEN timestamp >= cast(now() - INTERVAL '365 Days' as timestamp) THEN oneYear.volume END ) AS FLOAT) AS volume_1y, 
			CAST(MIN(CASE WHEN  timestamp >=cast(now() - INTERVAL '30 Days' as timestamp) THEN oneYear.volume END) AS FLOAT) AS volume_30d, 
			CAST(MIN( CASE WHEN timestamp >=cast(now() - INTERVAL '7 Days' as timestamp) THEN oneYear.volume  END) AS FLOAT) AS volume_7d,
			CAST(MIN( CASE WHEN timestamp >= cast(now() - INTERVAL '3 Days' as timestamp)  THEN oneYear.volume END) AS FLOAT) AS volume_24h,
			base, exchange, market, quote, type
		from oneYear
		where base = 'BTC'
		group by base, exchange, market, quote, type
	) as test



++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
