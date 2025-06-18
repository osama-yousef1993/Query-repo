WITH aggregated_data AS (
    SELECT
        id,
        ARRAY_AGG(STRUCT(
            'Time', time, 
            'volume_native', volume_native, 
            'volume_usd', volume_usd
        ) ORDER BY time) as beprices
    FROM
    (
        SELECT
            id ,
            Occurance_Time as time,
            CAST(AVG(volumeNative) AS FLOAT64) volume_native,
		    CAST(AVG(volumeUSD) AS FLOAT64) volume_usd,
            ROW_NUMBER() OVER (
                PARTITION BY ID, CAST(FLOOR(UNIX_SECONDS(Occurance_Time)/ 129600 )AS INT64 )
                ORDER BY Occurance_Time
            ) as row_num
        FROM
            api-project-901373404215.digital_assets.Digital_Assets_NFT_MarketData_dev c 
        WHERE
            Occurance_Time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 day)
        GROUP BY id, Occurance_Time
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
    beprices[OFFSET(0)].volume_usd AS lowest_volume_usd,
    beprices[OFFSET(0)].volume_native AS lowest_volume_native,
    beprices[OFFSET(0)].Time AS lowest_floor_time,
    beprices[OFFSET(ARRAY_LENGTH(beprices) - 1)].volume_usd AS highest_volume_usd,
    beprices[OFFSET(ARRAY_LENGTH(beprices) - 1)].volume_native AS highest_volume_native,
    beprices[OFFSET(ARRAY_LENGTH(beprices) - 1)].Time AS highest_floor_time,
    ((beprices[OFFSET(ARRAY_LENGTH(beprices) - 1)].volume_usd - beprices[OFFSET(0)].volume_usd) / NULLIF(beprices[OFFSET(0)].volume_usd, 0)) * 100 volume_usd_percent_change,
    ((beprices[OFFSET(ARRAY_LENGTH(beprices) - 1)].volume_native - beprices[OFFSET(0)].volume_native) / NULLIF(beprices[OFFSET(0)].volume_native, 0)) * 100 volume_native_percent_change,
FROM
    aggregated_data
