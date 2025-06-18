SELECT 
    array_to_json(ARRAY_AGG(json_build_object(
                'base', Symbol, 
                'exchange', exchange, 
                'quote', quote, 
                'pair', pair, 												 
                'pairStatus', status, 
                'update_timestamp', last_updated,
                'TypeOfPair', type,
                'currentPriceForPair1D', CAST(current_price_for_pair_1d AS FLOAT),
                'currentPriceForPair7D', CAST(current_price_for_pair_7d AS FLOAT),
                'currentPriceForPair30D', CAST(current_price_for_pair_30d AS FLOAT),
                'currentPriceForPair1Y', CAST(current_price_for_pair_1y AS FLOAT),
                'currentPriceForPairYTD', CAST(current_price_for_pair_ytd AS FLOAT)
                ))) as MarketPairs

from 
    (
        select 
            market.Symbol as Symbol,  market.exchange AS exchange, market.quote AS quote, market.pair as pair, 
            assets.status as status, assets.last_updated as last_updated, 
            ticker.type as type, CAST(oneDay.current_price_for_pair_1d AS FLOAT) as current_price_for_pair_1d,
            CAST(sevenDays.current_price_for_pair_7d AS FLOAT) as current_price_for_pair_7d , 
            CAST(thirtyDays.current_price_for_pair_30d AS FLOAT) as current_price_for_pair_30d, 
            CAST(oneYear.current_price_for_pair_1y AS FLOAT) as current_price_for_pair_1y, 
            CAST(YTD.current_price_for_pair_ytd AS FLOAT) as current_price_for_pair_ytd
        from 
            (
                select
                    lower(id) as base,
                    status, 
                    last_updated
                from 
                    nomics_assets
                group by 
                    id
            ) assets
            Join (
                    select
                        lower(base) as Symbol, 
                        exchange, 
                        quote , 
                        CONCAT(base, quote) as pair
                    from 
                        nomics_markets
                    group by
                        base,
                        exchange, 
                        quote
                ) markets
            Join (
                    SELECT 
                        Symbol,   
                        CAST(MIN(Price) AS FLOAT) current_price_for_pair_1y
                    from
                        (
                            SELECT 
                                lower(base) as Symbol,
                                AVG(price) price
                            from 
                                nomics_exchange_market_ticker
                            where 
                                timestamp >= cast(now() - INTERVAL '365 DAYS' as timestamp)
                            group by 
                                base
                        ) as oneYear
                    group by Symbol
                ) oneYear
            Join (
                    SELECT 
                        Symbol,   
                        CAST(MIN(Price) AS FLOAT) current_price_for_pair_ytd
                    from
                        (
                            SELECT 
                                lower(base) as Symbol,
                                AVG(price) price
                            from 
                                nomics_exchange_market_ticker
                            where 
                                timestamp >= cast(date_trunc('year', current_date) as timestamp)
                            group by 
                                base
                        ) as YTD
                    group by Symbol
                ) YTD
            Join (
                    SELECT 
                        Symbol,   
                        CAST(MIN(Price) AS FLOAT) current_price_for_pair_1d
                    from
                        (
                            SELECT 
                                lower(base) as Symbol,
                                AVG(price) price
                            from 
                                nomics_exchange_market_ticker
                            where 
                                timestamp >= cast(now() - INTERVAL '24 HOUR' as timestamp)
                            group by 
                                base
                        ) as oneDay
                    group by Symbol
                ) oneDay
            Join (
                    SELECT 
                        Symbol,   
                        CAST(MIN(Price) AS FLOAT) current_price_for_pair_30d
                    from
                        (
                            SELECT 
                                lower(base) as Symbol,
                                AVG(price) price
                            from 
                                nomics_exchange_market_ticker
                            where 
                                timestamp >= cast(now() - INTERVAL '30 DAYS' as timestamp)
                            group by 
                                base
                        ) as thirtyDays
                    group by Symbol
                ) thirtyDays
            Join(
                    SELECT 
                        Symbol,   
                        CAST(MIN(Price) AS FLOAT) current_price_for_pair_7d
                    from
                        (
                            SELECT 
                                lower(base) as Symbol,
                                AVG(price) price
                            from 
                                nomics_exchange_market_ticker
                            where 
                                timestamp >= cast(now() - INTERVAL '7 DAYS' as timestamp)
                            group by 
                                base
                        ) as sevenDays
                    group by Symbol
                )sevenDays
            Join(
                    select
                        lower(base) as base,
                        type
                    from 
                        nomics_exchange_market_ticker
                    where 
                        type != ''
                    group by
                        base,
                        type
                ) ticker
        ON 
            (
                assets.base = markets.symbol 
                AND
                assets.base = oneYear.symbol
                AND
                assets.base = YTD.symbol
                AND
                assets.base = oneDay.symbol
                AND
                assets.base = thirtyDays.symbol
                AND
                assets.base = sevenDays.symbol
                AND
                assets.base = ticker.base
            )
    )


select 
    GA_VisitorId, ga_fullVisitorID, GA_referralGroup, ave, total
from 
    (
        select 
            traffic.GA_VisitorId, subscribed_traffic.ga_fullVisitorID, traffic.GA_referralGroup, 
            avg(distinct cast(traffic.session_id as FLOAT64)) AS ave, count(distinct(traffic.session_id)) as total
        from 
            (
                SELECT
                    GA_VisitorId, ga_fullVisitorID, GA_visitStartTime,GA_referralGroup, concat(GA_fullVisitorId,GA_visitId) as session_id
                FROM  
                    `api-project-901373404215.DataMart.v_DataMart`
            ) traffic 
        Join 
            (
                SELECT 
                    ga_fullVisitorID, GA_visitStartTime, concat(GA_fullVisitorId,GA_visitId) as session_id
                FROM 
                    `api-project-901373404215.DataMart.v_events_datamart`
                WHERE 
                    GA_eventAction = "subscribesuccess" 
                    AND (GA_eventLabel LIKE "r8w03as%" OR GA_eventLabel LIKE "rkpevdb%")
            ) subscribed_traffic

        On 
            (
                subscribed_traffic.ga_fullVisitorID = traffic.ga_fullVisitorID
                AND traffic.GA_visitStartTime< TIMESTAMP_SECONDS(subscribed_traffic.GA_visitStartTime)
            )
        GROUP BY
            subscribed_traffic.ga_fullVisitorID, traffic.GA_referralGroup, traffic.GA_VisitorId
    )