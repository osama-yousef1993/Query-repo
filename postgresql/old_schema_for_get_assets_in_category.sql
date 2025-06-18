
BEGIN
    RETURN QUERY EXECUTE format('
        SELECT 
            symbol,
            display_symbol,						  
            name,
            slug,
            logo,
            temporary_data_delay,
            price_24h,
            percentage_1h,
            percentage_24h,
            percentage_7d,
            change_value_24h,						  
            market_cap,
            (nomics::json->>''volume_1d'')::float AS volume_1d,
			status,
            market_cap_percent_change_1d,
			RANK () OVER (
        partition by status
        ORDER BY
          market_cap desc
      ) rank_number
        FROM
            fundamentalslatest
        WHERE
            symbol IN (
                SELECT 
                    json_data->>''id'' AS id  
                FROM 
                    categories_fundamentals,
                    jsonb_array_elements(markets::jsonb) AS json_data
                WHERE 
                    id = ''%s''
            )
        ORDER BY %s %s NULLS LAST
        LIMIT %s
        OFFSET %s;',
		search_term,
        sort_by,
        direction,
        lim,
        lim * page_num
    ) USING sort_by, lim, page_num, direction, search_term;
END;



CREATE OR REPLACE FUNCTION public.get_assets_in_category(
    lim integer,
    page_num integer,
    sort_by text,
    direction text,
    search_term text
)
RETURNS TABLE(
    symbol text,
    display_symbol text,
    name text,
    slug text,
    logo text,
    temporary_data_delay boolean,
    price_24h double precision,
    percentage_1h double precision,
    percentage_24h double precision,
    percentage_7d double precision,
    change_value_24h double precision,
    market_cap double precision,
    volume_1d double precision,
    status text,
    market_cap_percent_change_1d double precision,
    rank_number bigint
) 
LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
    RETURN QUERY EXECUTE format('
       SELECT 
            symbol,
            display_symbol,						  
            name,
            slug,
            logo,
            temporary_data_delay,
            price_24h,
            percentage_1h,
            percentage_24h,
            percentage_7d,
            change_value_24h,						  
            market_cap,
            (nomics::json->>''volume_1d'')::float AS volume_1d,
			status,
            market_cap_percent_change_1d,
            RANK () OVER (
                partition by status
                ORDER BY
                  market_cap desc
              ) rank_number
        FROM
            fundamentalslatest
        WHERE
            symbol IN (
                SELECT 
                    json_data->>''id'' AS id  
                FROM 
                    categories_fundamentals,
                    jsonb_array_elements(markets::jsonb) AS json_data
                WHERE 
                    id = ''%s''
            )
        ORDER BY %s %s NULLS LAST
        LIMIT %s
        OFFSET %s;
       ',
		search_term,
        sort_by,
        direction,
        lim,
        lim * page_num
    ) USING sort_by, lim, page_num, direction, search_term;
END;
$BODY$;