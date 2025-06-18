-- Returns all chart data.
-- if the interval is not 24hrs it returns all data for 7d/30d/1y/max, all charts are appended with the last ticker from 24h
-- if the interval is 24h it returns all data it returns all data  24h/7d/30d/1y/max,
CREATE or REPLACE FUNCTION getFTNFTChartData(intval TEXT,symb TEXT, assetsTp TEXT)
RETURNS Table (
is_index bool, 
source TEXT, 
target_resolution_seconds int, 
prices jsonb,
symbol TEXT,
tm_interval TEXT,
status TEXT
) AS $$
#variable_conflict use_column
begin
--If we are not requesting a 24 hour chart return every chart 
--exluding 24hr. The charts will be used in FDA API
if assetsTp = 'NFT' 
then 
    if intval not like  '%24h%'
    then
    RETURN QUERY select
    is_index, 
    a.source as source , 
    a.target_resolution_seconds as target_resolution_seconds , 
    --append 24 hour cadle to chart that will be returned
    a.prices::jsonb || b.prices::jsonb as prices,
    a.symbol as symbol,
    a.interval as tm_interval,
    c.status as status
    from (
    --get chart for specified interval
    SELECT 
                is_index, 
                source, 
                target_resolution_seconds, 
                prices, 
                symbol,
                interval
            FROM 
                nomics_chart_data
            WHERE 
                target_resolution_seconds != 900
                    and "assetType" = assetsTp
            order by target_resolution_seconds asc
    ) a -- 
    join (
    --get last candle from 24 hr chart
    SELECT symbol, prices->-1 as prices
    FROM   nomics_chart_data 
    where target_resolution_seconds = 900 and symbol = symb and "assetType" = assetsTp) b
    on  b.symbol = a.symbol
     join (
			select id as symbol, CASE StatusResult
							when 0 Then 'active'
							Else 'comatoken'
							end as status
			from
			(
				select id, 
				EXTRACT(DAY FROM Now() - last_updated) AS StatusResult
				from nftdatalatest where Id = symb 
			) c
		) c
    	on a.symbol = c.symbol;
    --If we are not requesting a 24 hour chart return every chart 
    --exluding 24hr. The charts will be used in FDA API
    else
    RETURN QUERY select
    is_index, 
    a.source as source , 
    a.target_resolution_seconds as target_resolution_seconds , 
    --append 24 hour cadle to chart that will be returned
    a.prices::jsonb as prices,
    a.symbol as symbol,
    a.interval as tm_interval,
    c.status as status
    from (
    --get chart for specified interval
    SELECT 
                is_index, 
                source, 
                target_resolution_seconds, 
                prices, 
                symbol,
                interval
            FROM 
                nomics_chart_data
                where symbol = symb
                    and "assetType" = assetsTp
            order by target_resolution_seconds asc
    ) a 
     join (
			select id as symbol, CASE StatusResult
							when 0 Then 'active'
							Else 'comatoken'
							end as status
			from
			(
				select id, 
				EXTRACT(DAY FROM Now() - last_updated) AS StatusResult
				from nftdatalatest where Id = symb 
			) c
		) c
    on a.symbol = c.symbol;
    end if;
else
   if intval not like  '%24h%'
    then
    RETURN QUERY select
    is_index, 
    a.source as source , 
    a.target_resolution_seconds as target_resolution_seconds , 
    --append 24 hour cadle to chart that will be returned
    a.prices::jsonb || b.prices::jsonb as prices,
    a.symbol as symbol,
    a.interval as tm_interval,
    c.status as status
    from (
    --get chart for specified interval
    SELECT 
                is_index, 
                source, 
                target_resolution_seconds, 
                prices, 
                symbol,
                interval
            FROM 
                nomics_chart_data
            WHERE 
                target_resolution_seconds != 900
                    and "assetType" = assetsTp
            order by target_resolution_seconds asc
    ) a -- 
    join (
    --get last candle from 24 hr chart
    SELECT symbol, prices->-1 as prices
    FROM   nomics_chart_data 
    where target_resolution_seconds = 900 and symbol = symb and "assetType" = assetsTp) b
    on  b.symbol = a.symbol
    join (select symbol,status from fundamentalslatest where symbol = symb ) c
    on a.symbol = c.symbol;
    --If we are not requesting a 24 hour chart return every chart 
    --exluding 24hr. The charts will be used in FDA API
    else
    RETURN QUERY select
    is_index, 
    a.source as source , 
    a.target_resolution_seconds as target_resolution_seconds , 
    --append 24 hour cadle to chart that will be returned
    a.prices::jsonb as prices,
    a.symbol as symbol,
    a.interval as tm_interval,
    c.status as status
    from (
    --get chart for specified interval
    SELECT 
                is_index, 
                source, 
                target_resolution_seconds, 
                prices, 
                symbol,
                interval
            FROM 
                nomics_chart_data
                where symbol = symb
                    and "assetType" = assetsTp
            order by target_resolution_seconds asc
    ) a 
    join (select symbol,status from fundamentalslatest where symbol = symb ) c
    on a.symbol = c.symbol;
    end if;
end if;			
end;
$$
language PLPGSQL;
  