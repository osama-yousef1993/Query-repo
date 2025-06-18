create or replace FUNCTION public.GetActiveMarketPairs()
	returns TABLE (base text,quote text)	
	AS
	$func$
	WITH allMarkets AS
  		(SELECT *
   		FROM nomics_markets),
     		allAssets AS
  		(SELECT *
   		FROM nomics_assets)
	SELECT allMarkets.base,
       	   allMarkets.quote
	FROM allAssets
	INNER JOIN allMarkets ON allAssets.Id = allMarkets.base
	WHERE status = 'active'
	GROUP BY allMarkets.base,
         	 allMarkets.quote
	$func$
	LANGUAGE sql;
	
	
select * from public.GetActiveMarketPairs()