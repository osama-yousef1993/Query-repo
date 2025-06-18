with yesterday_floor_price as (
    SELECT
        id, 
        SUM(floorprice_usd) floorprice_usd, 
        SUM(floorprice_native) floorprice_native
    FROM
    (
        SELECT
            id ,
            CAST(floorprice_usd AS FLOAT64) floorprice_usd,
            CAST(floorprice_native AS FLOAT64) floorprice_native,
            ROW_NUMBER() OVER (
                PARTITION BY ID
                ORDER BY Occurance_Time DESC
            ) as row_num
        FROM
            api-project-901373404215.digital_assets.Digital_Assets_NFT_MarketData_dev c 
        WHERE
            DATE(occurance_time) = DATE(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 day))
        ORDER BY
            Occurance_Time
    ) as test
    WHERE
        row_num = 1
    GROUP BY
        id
),
today_floor_price as (
    SELECT
        id, 
        SUM(floorprice_usd) floorprice_usd, 
        SUM(floorprice_native) floorprice_native
    FROM
    (
        SELECT
            id ,
            CAST(floorprice_usd AS FLOAT64) floorprice_usd,
            CAST(floorprice_native AS FLOAT64) floorprice_native,
            ROW_NUMBER() OVER (
                PARTITION BY ID
                ORDER BY Occurance_Time DESC
            ) as row_num
        FROM
            api-project-901373404215.digital_assets.Digital_Assets_NFT_MarketData_dev c 
        WHERE
            occurance_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 hour)
        ORDER BY
            Occurance_Time
    ) as test
    WHERE
        row_num = 1
    GROUP BY
        id
)

select 
today_floor_price.id,
((today_floor_price.floorprice_usd - yesterday_floor_price.floorprice_usd) / NULLIF(yesterday_floor_price.floorprice_usd, 0)) * 100 AS floor_price_24h_percentage_change_usd,
((today_floor_price.floorprice_native - yesterday_floor_price.floorprice_native) / NULLIF(yesterday_floor_price.floorprice_native, 0)) * 100 AS floor_price_24h_percentage_change_native
from 
today_floor_price,
yesterday_floor_price
