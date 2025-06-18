WITH aggregated_data AS (
    SELECT
        id,
        ARRAY_AGG(STRUCT(
            'Time', time, 
            'Price', Price, 
            'floorpricenative', floorpricenative
        ) ORDER BY time) as beprices
    FROM
    (
        SELECT
            id ,
            Occurance_Time as time,
            CAST(floorprice_usd AS FLOAT64) Price,
            CAST(floorprice_native AS FLOAT64) floorpricenative,
            ROW_NUMBER() OVER (
                PARTITION BY ID, CAST(FLOOR(UNIX_SECONDS(Occurance_Time)/ 900 )AS INT64 )
                ORDER BY Occurance_Time
            ) as row_num
        FROM
            api-project-901373404215.digital_assets.Digital_Assets_NFT_MarketData_dev c 
        WHERE
            Occurance_Time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 day)
        ORDER BY
            Occurance_Time
    ) as test
    WHERE
        row_num = 1
    GROUP BY
        id
)
SELECT
    id,
    beprices[OFFSET(0)].Price AS lowest_floor_price,
    beprices[OFFSET(0)].Time AS lowest_floor_time,
    beprices[OFFSET(ARRAY_LENGTH(beprices) - 1)].Price AS highest_floor_price,
    beprices[OFFSET(ARRAY_LENGTH(beprices) - 1)].Time AS highest_floor_time,
    ((beprices[OFFSET(ARRAY_LENGTH(beprices) - 1)].Price - beprices[OFFSET(0)].Price) / beprices[OFFSET(0)].Price) * 100 floor_price_percent_change,
FROM
    aggregated_data
where id = 'elite-ape-entry-coins'
