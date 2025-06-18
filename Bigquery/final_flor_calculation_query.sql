WITH
  yesterday_floor_price AS (
  SELECT
    id,
    SUM(floorprice_usd) floorprice_usd,
    SUM(floorprice_native) floorprice_native
  FROM (
    SELECT
      id,
      Occurance_Time AS time,
      CAST(floorprice_usd AS FLOAT64) floorprice_usd,
      CAST(floorprice_native AS FLOAT64) floorprice_native,
      ROW_NUMBER() OVER (PARTITION BY ID ORDER BY Occurance_Time DESC ) AS row_num
    FROM
      api-project-901373404215.digital_assets.Digital_Assets_NFT_MarketData_dev c
    WHERE
      DATE(occurance_time) = DATE(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 day))
    ORDER BY
      Occurance_Time ) AS test
  WHERE
    row_num = 1
  GROUP BY
    id ),
  today_floor_price AS (
  SELECT
    id,
    SUM(floorprice_usd) floorprice_usd,
    SUM(floorprice_native) floorprice_native
  FROM (
    SELECT
      id,
      Occurance_Time AS time,
      CAST(floorprice_usd AS FLOAT64) floorprice_usd,
      CAST(floorprice_native AS FLOAT64) floorprice_native,
      ROW_NUMBER() OVER (PARTITION BY ID ORDER BY Occurance_Time DESC ) AS row_num
    FROM
      api-project-901373404215.digital_assets.Digital_Assets_NFT_MarketData_dev c
    WHERE
      occurance_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 hour)
    ORDER BY
      Occurance_Time ) AS test
  WHERE
    row_num = 1
  GROUP BY
    id ),
  high_low_price AS (
  SELECT
    id,
    ARRAY_AGG(STRUCT( 'Time',
        time,
        'floorpriceusd',
        floorpriceusd,
        'floorpricenative',
        floorpricenative )
    ORDER BY
      time) AS beprices
  FROM (
    SELECT
      id,
      Occurance_Time AS time,
      CAST(floorprice_usd AS FLOAT64) floorpriceusd,
      CAST(floorprice_native AS FLOAT64) floorpricenative,
      ROW_NUMBER() OVER (PARTITION BY ID, CAST(FLOOR(UNIX_SECONDS(Occurance_Time)/ 900 )AS INT64 )
      ORDER BY
        Occurance_Time ) AS row_num
    FROM
      api-project-901373404215.digital_assets.Digital_Assets_NFT_MarketData_dev c
    WHERE
      Occurance_Time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 day)
    ORDER BY
      Occurance_Time ) AS test
  WHERE
    row_num = 1
  GROUP BY
    id )
SELECT
  today_floor_price.id,
  high_low_price.beprices[OFFSET(0)].Time AS lowest_floor_time,
  high_low_price.beprices[OFFSET(ARRAY_LENGTH(beprices) - 1)].Time AS highest_floor_time,
  high_low_price.beprices[OFFSET(0)].floorpriceusd AS lowest_floor_usd_price,
  high_low_price.beprices[OFFSET(ARRAY_LENGTH(beprices) - 1)].floorpriceusd AS highest_floor_usd_price,
  high_low_price.beprices[OFFSET(0)].floorpricenative AS lowest_floor_native_price,
  high_low_price.beprices[OFFSET(ARRAY_LENGTH(beprices) - 1)].floorpricenative AS highest_floor_native_price,
  ((today_floor_price.floorprice_usd - yesterday_floor_price.floorprice_usd) / NULLIF(yesterday_floor_price.floorprice_usd, 0)) * 100 AS floor_price_24h_percentage_change_usd,
  ((today_floor_price.floorprice_native - yesterday_floor_price.floorprice_native) / NULLIF(yesterday_floor_price.floorprice_native, 0)) * 100 AS floor_price_24h_percentage_change_native
FROM
  today_floor_price
LEFT JOIN
  yesterday_floor_price
ON
  today_floor_price.id = yesterday_floor_price.id
LEFT JOIN
  high_low_price
ON
  today_floor_price.id = high_low_price.id