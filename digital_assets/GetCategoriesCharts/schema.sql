CREATE
or REPLACE FUNCTION getCategoriesChartData(intval TEXT, symb TEXT, assetsTp TEXT) RETURNS Table (
    is_index bool,
    source TEXT,
    target_resolution_seconds int,
    prices jsonb,
    symbol TEXT,
    tm_interval TEXT,
    status TEXT
) AS $$ #variable_conflict use_column
begin 
--If we are not requesting a 24 hour chart return every chart 
--exluding 24hr. The charts will be used in FDA API
-- we will check for the type CATEGORY
if intval not like '%24h%' then RETURN QUERY
select
    is_index,
    a.source as source,
    a.target_resolution_seconds as target_resolution_seconds,
    --append 24 hour cadle to chart that will be returned
    b.prices :: jsonb || a.prices :: jsonb as prices,
    a.symbol as symbol,
    a.interval as tm_interval,
    c.status as status
from
    (
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
        order by
            target_resolution_seconds asc
    ) a -- 
    join (
        --get last candle from 24 hr chart
        SELECT
            symbol,
            prices -> -1 as prices
        FROM
            nomics_chart_data
        where
            target_resolution_seconds = 900
            and symbol = symb
            and "assetType" = assetsTp
    ) b on b.symbol = a.symbol
    join (
        select
            id as symbol,
            CASE
                StatusResult
                when 0 Then 'active'
                Else 'comatoken'
            end as status
        from
            (
                select
                    id,
                    EXTRACT(
                        DAY
                        FROM
                            Now() - last_updated
                    ) AS StatusResult
                from
                    categories_fundamentals
                where
                    Id = symb
            ) c
    ) c on a.symbol = c.symbol;

--If we are not requesting a 24 hour chart return every chart 
--exluding 24hr. The charts will be used in FDA API
else RETURN QUERY
select
    is_index,
    a.source as source,
    a.target_resolution_seconds as target_resolution_seconds,
    --append 24 hour cadle to chart that will be returned
    a.prices :: jsonb as prices,
    a.symbol as symbol,
    a.interval as tm_interval,
    c.status as status
from
    (
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
        where
            symbol = symb
            and "assetType" = assetsTp
        order by
            target_resolution_seconds asc
    ) a
    join (
        select
            id as symbol,
            CASE
                StatusResult
                when 0 Then 'active'
                Else 'comatoken'
            end as status
        from
            (
                select
                    id,
                    EXTRACT(
                        DAY
                        FROM
                            Now() - last_updated
                    ) AS StatusResult
                from
                    categories_fundamentals
                where
                    Id = symb
            ) c
    ) c on a.symbol = c.symbol;

end if;

end;

$$ language PLPGSQL;