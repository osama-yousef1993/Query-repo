with 
    before_date as (
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

        ), 
    after_date as (
        select 
            t1.GA_visitorId as after_GA_visitorId, t1.status, t2.GA_VisitorId,
            avg(distinct cast(t2.session_id as FLOAT64)) AS after_ave, count(distinct(t2.session_id)) as after_total
        from 
            `api-project-901373404215.piano.v_subscriber_detail` as t1,
            `api-project-901373404215.DataMart.v_DataMart` as t2
        where 
            t1.status = 'active'
            and t1.GA_visitorId = t2.GA_VisitorId
        group by 
            t1.GA_visitorId, t1.status, t2.GA_VisitorId

    )
select 
    before_date.ga_fullVisitorID, before_date.GA_referralGroup, before_date.ave, before_date.total,
    after_date.after_GA_visitorId, after_date.status, after_date.after_ave, after_date.after_total
from 
    before_date
INNER JOIN 
    after_date 
on 
    before_date.GA_VisitorId = after_date.GA_visitorId



SELECT 
    Symbol,   
    current_price_for_pair_ytd,
    volume_for_pair_ytd
from
(
    SELECT 
        ticker.Symbol,
        ticker.price as current_price_for_pair_ytd,
        ytd.volume_for_pair_ytd
    from (
        select 
            lower(base) as Symbol,
            AVG(price) price
        from 
            nomics_exchange_market_ticker
        where 
            timestamp >= cast(date_trunc('year', current_date) as timestamp)
        group by 
            base
    ) ticker
    LEFT JOIN
    (
        select 
            lower(base) as Symbol,
            CASE WHEN AVG(volume) is null THEN CAST(0 AS FLOAT)
            ELSE AVG(volume)
            END
            as volume_for_pair_ytd
        from 
            nomics_exchange_market_ticker_ytd
        where 
            last_updated >= cast(date_trunc('year', current_date) as timestamp)
        group by 
            base
    ) ytd
    ON 
    (
        ytd.Symbol = ticker.Symbol
    )
        
) as ytd