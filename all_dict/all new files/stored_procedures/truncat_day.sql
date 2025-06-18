SELECT 
	date_trunc('day', timestamp) as day , count(*),
	base,
	AVG(close) as close
FROM nomics_ohlcv_candles
WHERE 
	timestamp >= cast(now() - INTERVAL '365 year' as timestamp)
group by 
	1,
	base
ORDER BY 1