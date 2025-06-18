CREATE OR REPLACE FUNCTION fuzzySearch_categoriesfundamentals(
    lim int,
    page_num int,
    sort_by text,
    direction text,
    search_term text
)
RETURNS TABLE (
    id text,
    name text,
    total_tokens integer,
    average_percentage_24h double precision,
    volume_24h double precision,
    price_24h double precision,
    average_price double precision,
    market_cap double precision,
    market_cap_percentage_24h double precision,
    top_gainers json,
    top_movers json,
    forbesname text,
    slug text,
    last_updated timestamp,
    is_highlighted boolean
) AS $$
BEGIN
	IF search_term = '' Then
	RETURN QUERY EXECUTE format('
        SELECT 
            t.id, 
            t.name, 
            t.total_tokens, 
            t.average_percentage_24h, 
            t.volume_24h,
            t.price_24h,
            t.average_price, 
            t.market_cap, 
            t.market_cap_percentage_24h, 
            t.top_gainers,
            t.top_movers,
            t.forbesname,
            t.slug, 
            t.last_updated,
            t.is_highlighted
        FROM 
            public.GetCategoriesFundamentalsV2() as t
        ORDER BY t.%s %s NULLS LAST
        LIMIT %s
        OFFSET %s;',
        sort_by,
        direction,
        lim,
        lim * page_num
    );
	Else
    RETURN QUERY EXECUTE format('
        SELECT 
            t.id, 
            t.name, 
            t.total_tokens, 
            t.average_percentage_24h, 
            t.volume_24h,
            t.price_24h,
            t.average_price, 
            t.market_cap, 
            t.market_cap_percentage_24h, 
            t.top_gainers,
            t.top_movers,
            t.forbesname,
            t.slug, 
            t.last_updated,
            t.is_highlighted
        FROM 
            public.GetCategoriesFundamentalsV2() as t
        WHERE 
            SIMILARITY(lower(t.name), ''%s'') > 0.1
        ORDER BY t.%s %s NULLS LAST
        LIMIT %s
        OFFSET %s;',
        search_term,
        sort_by,
        direction,
        lim,
        lim * page_num
    );
	END IF;
END;
$$ LANGUAGE plpgsql;




CREATE OR REPLACE FUNCTION fuzzySearch_categoriesfundamentals(
    lim int,
    page_num int,
    sort_by text,
    direction text,
    search_term text
)
RETURNS TABLE (
    id text,
    name text,
    total_tokens integer,
    average_percentage_24h double precision,
    volume_24h double precision,
    price_24h double precision,
    average_price double precision,
    market_cap double precision,
    market_cap_percentage_24h double precision,
    top_gainers json,
    top_movers json,
    forbesname text,
    slug text,
    last_updated timestamp,
    is_highlighted boolean
) AS $$
BEGIN
	IF search_term = '' Then
	RETURN QUERY EXECUTE format('
        SELECT 
            t.id, 
            t.name, 
            t.total_tokens, 
            t.average_percentage_24h, 
            t.volume_24h,
            t.price_24h,
            t.average_price, 
            t.market_cap, 
            t.market_cap_percentage_24h, 
            t.top_gainers,
            t.top_movers,
            t.forbesname,
            t.slug, 
            t.last_updated,
            t.is_highlighted
        FROM 
            public.GetCategoriesFundamentalsV2() as t
        ORDER BY t.%s %s NULLS LAST
        LIMIT %s
        OFFSET %s;',
        sort_by,
        direction,
        lim,
        lim * page_num
    );
	Else
    RETURN QUERY EXECUTE format('
        SELECT 
            t.id, 
            t.name, 
            t.total_tokens, 
            t.average_percentage_24h, 
            t.volume_24h,
            t.price_24h,
            t.average_price, 
            t.market_cap, 
            t.market_cap_percentage_24h, 
            t.top_gainers,
            t.top_movers,
            t.forbesname,
            t.slug, 
            t.last_updated,
            t.is_highlighted
        FROM 
            public.GetCategoriesFundamentalsV2() as t
        WHERE 
            SIMILARITY(lower(t.name), ''%s'') > 0.1
        ORDER BY t.%s %s NULLS LAST
        LIMIT %s
        OFFSET %s;',
        search_term,
        sort_by,
        direction,
        lim,
        lim * page_num
    );
	END IF;
END;
$$ LANGUAGE plpgsql;