with high_low_price_oneDay AS (
  select
    id,
    -- beprices_1d[OFFSET(0)].floorpriceusd AS lowest_floor_price_24h_usd,
    -- beprices_1d[OFFSET(0)].floorpricenative AS lowest_floor_price_24h_native,
    -- beprices_1d[OFFSET(ARRAY_LENGTH(beprices_1d) - 1)].floorpriceusd AS highest_floor_price_24h_usd,
    -- beprices_1d[OFFSET(ARRAY_LENGTH(beprices_1d) - 1)].floorpricenative AS highest_floor_price_24h_native,
    (SELECT MIN(floorpriceusd) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS lowest_floor_price_24h_usd,
    (SELECT MAX(floorpriceusd) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS highest_floor_price_24h_usd,
    (SELECT MIN(floorpricenative) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS lowest_floor_price_24h_native,
    (SELECT MAX(floorpricenative) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS highest_floor_price_24h_native,
    ((beprices_1d[OFFSET(ARRAY_LENGTH(beprices_1d) - 1)].floorpriceusd - beprices_1d[OFFSET(0)].floorpriceusd) / NULLIF(beprices_1d[OFFSET(0)].floorpriceusd, 0)) AS floor_price_24h_percentage_change_usd,
    ((beprices_1d[OFFSET(ARRAY_LENGTH(beprices_1d) - 1)].floorpricenative - beprices_1d[OFFSET(0)].floorpricenative) / NULLIF(beprices_1d[OFFSET(0)].floorpricenative, 0)) AS floor_price_24h_percentage_change_native,
    (SELECT MAX((cur.floorpriceusd - beprices_1d[OFFSET(off-1)].floorpriceusd) / NULLIF(beprices_1d[OFFSET(off-1)].floorpriceusd, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS highest_floor_price_24h_percentage_change_usd,
    (SELECT MAX((cur.floorpricenative - beprices_1d[OFFSET(off-1)].floorpricenative) / NULLIF(beprices_1d[OFFSET(off-1)].floorpricenative, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS highest_floor_price_24h_percentage_change_native,
    (SELECT MIN((cur.floorpriceusd - beprices_1d[OFFSET(off-1)].floorpriceusd) / NULLIF(beprices_1d[OFFSET(off-1)].floorpriceusd, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS lowest_floor_price_24h_percentage_change_usd,
    (SELECT MIN((cur.floorpricenative - beprices_1d[OFFSET(off-1)].floorpricenative) / NULLIF(beprices_1d[OFFSET(off-1)].floorpricenative, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS lowest_floor_price_24h_percentage_change_native
  from(
    SELECT
      id,
      ARRAY_AGG(STRUCT( 'Time',
          time,
          'floorpriceusd',
          floorpriceusd,
          'floorpricenative',
          floorpricenative )
      ORDER BY
        time) AS beprices_1d,
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
  ),
  high_low_price_sevenDay AS (
  select
    id,
    -- beprices_1d[OFFSET(0)].floorpriceusd AS lowest_floor_price_7d_usd,
    -- beprices_1d[OFFSET(0)].floorpricenative AS lowest_floor_price_7d_native,
    -- beprices_1d[OFFSET(ARRAY_LENGTH(beprices_1d) - 1)].floorpriceusd AS highest_floor_price_7d_usd,
    -- beprices_1d[OFFSET(ARRAY_LENGTH(beprices_1d) - 1)].floorpricenative AS highest_floor_price_7d_native,
    (SELECT MIN(floorpriceusd) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS lowest_floor_price_7d_usd,
    (SELECT MAX(floorpriceusd) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS highest_floor_price_7d_usd,
    (SELECT MIN(floorpricenative) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS lowest_floor_price_7d_native,
    (SELECT MAX(floorpricenative) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS highest_floor_price_7d_native,
    ((beprices_1d[OFFSET(ARRAY_LENGTH(beprices_1d) - 1)].floorpriceusd - beprices_1d[OFFSET(0)].floorpriceusd) / NULLIF(beprices_1d[OFFSET(0)].floorpriceusd, 0)) AS floor_price_7d_percentage_change_usd,
    ((beprices_1d[OFFSET(ARRAY_LENGTH(beprices_1d) - 1)].floorpricenative - beprices_1d[OFFSET(0)].floorpricenative) / NULLIF(beprices_1d[OFFSET(0)].floorpricenative, 0)) AS floor_price_7d_percentage_change_native,
    (SELECT MAX((cur.floorpriceusd - beprices_1d[OFFSET(off-1)].floorpriceusd) / NULLIF(beprices_1d[OFFSET(off-1)].floorpriceusd, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS highest_floor_price_7d_percentage_change_usd,
    (SELECT MAX((cur.floorpricenative - beprices_1d[OFFSET(off-1)].floorpricenative) / NULLIF(beprices_1d[OFFSET(off-1)].floorpricenative, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS highest_floor_price_7d_percentage_change_native,
    (SELECT MIN((cur.floorpriceusd - beprices_1d[OFFSET(off-1)].floorpriceusd) / NULLIF(beprices_1d[OFFSET(off-1)].floorpriceusd, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS lowest_floor_price_7d_percentage_change_usd,
    (SELECT MIN((cur.floorpricenative - beprices_1d[OFFSET(off-1)].floorpricenative) / NULLIF(beprices_1d[OFFSET(off-1)].floorpricenative, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS lowest_floor_price_7d_percentage_change_native
  from(
    SELECT
      id,
      ARRAY_AGG(STRUCT( 'Time',
          time,
          'floorpriceusd',
          floorpriceusd,
          'floorpricenative',
          floorpricenative )
      ORDER BY
        time) AS beprices_1d,
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
        Occurance_Time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 day)
      ORDER BY
        Occurance_Time ) AS test
    WHERE
      row_num = 1
    GROUP BY
      id )
  ),
  high_low_price_thirtyDay AS (
  select
    id,
    -- beprices_1d[OFFSET(0)].floorpriceusd AS lowest_floor_price_30d_usd,
    -- beprices_1d[OFFSET(0)].floorpricenative AS lowest_floor_price_30d_native,
    -- beprices_1d[OFFSET(ARRAY_LENGTH(beprices_1d) - 1)].floorpriceusd AS highest_floor_price_30d_usd,
    -- beprices_1d[OFFSET(ARRAY_LENGTH(beprices_1d) - 1)].floorpricenative AS highest_floor_price_30d_native,
    (SELECT MIN(floorpriceusd) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS lowest_floor_price_30d_usd,
    (SELECT MAX(floorpriceusd) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS highest_floor_price_30d_usd,
    (SELECT MIN(floorpricenative) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS lowest_floor_price_30d_native,
    (SELECT MAX(floorpricenative) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS highest_floor_price_30d_native,
    ((beprices_1d[OFFSET(ARRAY_LENGTH(beprices_1d) - 1)].floorpriceusd - beprices_1d[OFFSET(0)].floorpriceusd) / NULLIF(beprices_1d[OFFSET(0)].floorpriceusd, 0)) AS floor_price_30d_percentage_change_usd,
    ((beprices_1d[OFFSET(ARRAY_LENGTH(beprices_1d) - 1)].floorpricenative - beprices_1d[OFFSET(0)].floorpricenative) / NULLIF(beprices_1d[OFFSET(0)].floorpricenative, 0)) AS floor_price_30d_percentage_change_native,
    (SELECT MAX((cur.floorpriceusd - beprices_1d[OFFSET(off-1)].floorpriceusd) / NULLIF(beprices_1d[OFFSET(off-1)].floorpriceusd, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS highest_floor_price_30d_percentage_change_usd,
    (SELECT MAX((cur.floorpricenative - beprices_1d[OFFSET(off-1)].floorpricenative) / NULLIF(beprices_1d[OFFSET(off-1)].floorpricenative, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS highest_floor_price_30d_percentage_change_native,
    (SELECT MIN((cur.floorpriceusd - beprices_1d[OFFSET(off-1)].floorpriceusd) / NULLIF(beprices_1d[OFFSET(off-1)].floorpriceusd, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS lowest_floor_price_30d_percentage_change_usd,
    (SELECT MIN((cur.floorpricenative - beprices_1d[OFFSET(off-1)].floorpricenative) / NULLIF(beprices_1d[OFFSET(off-1)].floorpricenative, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS lowest_floor_price_30d_percentage_change_native
  from(
    SELECT
      id,
      ARRAY_AGG(STRUCT( 'Time',
          time,
          'floorpriceusd',
          floorpriceusd,
          'floorpricenative',
          floorpricenative )
      ORDER BY
        time) AS beprices_1d,
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
        Occurance_Time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 day)
      ORDER BY
        Occurance_Time ) AS test
    WHERE
      row_num = 1
    GROUP BY
      id )
  ),
  high_low_price_ninetyDay AS (
  select
    id,
    -- beprices_1d[OFFSET(0)].floorpriceusd AS lowest_floor_price_90d_usd,
    -- beprices_1d[OFFSET(0)].floorpricenative AS lowest_floor_price_90d_native,
    -- beprices_1d[OFFSET(ARRAY_LENGTH(beprices_1d) - 1)].floorpriceusd AS highest_floor_price_90d_usd,
    -- beprices_1d[OFFSET(ARRAY_LENGTH(beprices_1d) - 1)].floorpricenative AS highest_floor_price_90d_native,
    (SELECT MIN(floorpriceusd) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS lowest_floor_price_90d_usd,
    (SELECT MAX(floorpriceusd) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS highest_floor_price_90d_usd,
    (SELECT MIN(floorpricenative) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS lowest_floor_price_90d_native,
    (SELECT MAX(floorpricenative) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS highest_floor_price_90d_native,
    ((beprices_1d[OFFSET(ARRAY_LENGTH(beprices_1d) - 1)].floorpriceusd - beprices_1d[OFFSET(0)].floorpriceusd) / NULLIF(beprices_1d[OFFSET(0)].floorpriceusd, 0)) AS floor_price_90d_percentage_change_usd,
    ((beprices_1d[OFFSET(ARRAY_LENGTH(beprices_1d) - 1)].floorpricenative - beprices_1d[OFFSET(0)].floorpricenative) / NULLIF(beprices_1d[OFFSET(0)].floorpricenative, 0)) AS floor_price_90d_percentage_change_native,
    (SELECT MAX((cur.floorpriceusd - beprices_1d[OFFSET(off-1)].floorpriceusd) / NULLIF(beprices_1d[OFFSET(off-1)].floorpriceusd, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS highest_floor_price_90d_percentage_change_usd,
    (SELECT MAX((cur.floorpricenative - beprices_1d[OFFSET(off-1)].floorpricenative) / NULLIF(beprices_1d[OFFSET(off-1)].floorpricenative, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS highest_floor_price_90d_percentage_change_native,
    (SELECT MIN((cur.floorpriceusd - beprices_1d[OFFSET(off-1)].floorpriceusd) / NULLIF(beprices_1d[OFFSET(off-1)].floorpriceusd, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS lowest_floor_price_90d_percentage_change_usd,
    (SELECT MIN((cur.floorpricenative - beprices_1d[OFFSET(off-1)].floorpricenative) / NULLIF(beprices_1d[OFFSET(off-1)].floorpricenative, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS lowest_floor_price_90d_percentage_change_native
  from(
    SELECT
      id,
      ARRAY_AGG(STRUCT( 'Time',
          time,
          'floorpriceusd',
          floorpriceusd,
          'floorpricenative',
          floorpricenative )
      ORDER BY
        time) AS beprices_1d,
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
        Occurance_Time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 day)
      ORDER BY
        Occurance_Time ) AS test
    WHERE
      row_num = 1
    GROUP BY
      id )
  ),
  high_low_price_ytd AS (
  select
    id,
    -- beprices_1d[OFFSET(0)].floorpriceusd AS lowest_floor_price_ytd_usd,
    -- beprices_1d[OFFSET(0)].floorpricenative AS lowest_floor_price_ytd_native,
    -- beprices_1d[OFFSET(ARRAY_LENGTH(beprices_1d) - 1)].floorpriceusd AS highest_floor_price_ytd_usd,
    -- beprices_1d[OFFSET(ARRAY_LENGTH(beprices_1d) - 1)].floorpricenative AS highest_floor_price_ytd_native,
    (SELECT MIN(floorpriceusd) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS lowest_floor_price_ytd_usd,
    (SELECT MAX(floorpriceusd) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS highest_floor_price_ytd_usd,
    (SELECT MIN(floorpricenative) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS lowest_floor_price_ytd_native,
    (SELECT MAX(floorpricenative) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS highest_floor_price_ytd_native,
    ((beprices_1d[OFFSET(ARRAY_LENGTH(beprices_1d) - 1)].floorpriceusd - beprices_1d[OFFSET(0)].floorpriceusd) / NULLIF(beprices_1d[OFFSET(0)].floorpriceusd, 0)) AS floor_price_ytd_percentage_change_usd,
    ((beprices_1d[OFFSET(ARRAY_LENGTH(beprices_1d) - 1)].floorpricenative - beprices_1d[OFFSET(0)].floorpricenative) / NULLIF(beprices_1d[OFFSET(0)].floorpricenative, 0)) AS floor_price_ytd_percentage_change_native,
    (SELECT MAX((cur.floorpriceusd - beprices_1d[OFFSET(off-1)].floorpriceusd) / NULLIF(beprices_1d[OFFSET(off-1)].floorpriceusd, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS highest_floor_price_ytd_percentage_change_usd,
    (SELECT MAX((cur.floorpricenative - beprices_1d[OFFSET(off-1)].floorpricenative) / NULLIF(beprices_1d[OFFSET(off-1)].floorpricenative, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS highest_floor_price_ytd_percentage_change_native,
    (SELECT MIN((cur.floorpriceusd - beprices_1d[OFFSET(off-1)].floorpriceusd) / NULLIF(beprices_1d[OFFSET(off-1)].floorpriceusd, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS lowest_floor_price_ytd_percentage_change_usd,
    (SELECT MIN((cur.floorpricenative - beprices_1d[OFFSET(off-1)].floorpricenative) / NULLIF(beprices_1d[OFFSET(off-1)].floorpricenative, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS lowest_floor_price_ytd_percentage_change_native
  from(
    SELECT
      id,
      ARRAY_AGG(STRUCT( 'Time',
          time,
          'floorpriceusd',
          floorpriceusd,
          'floorpricenative',
          floorpricenative )
      ORDER BY
        time) AS beprices_1d,
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
        Occurance_time >= CAST(DATE_TRUNC(CURRENT_DATE(), year) AS Timestamp)
      ORDER BY
        Occurance_Time ) AS test
    WHERE
      row_num = 1
    GROUP BY
      id )
  )

select
    high_low_price_ytd.id,

    lowest_floor_price_24h_usd,
    lowest_floor_price_24h_native,
    highest_floor_price_24h_usd,
    highest_floor_price_24h_native,
    floor_price_24h_percentage_change_usd,
    floor_price_24h_percentage_change_native,
    lowest_floor_price_24h_percentage_change_usd,
    lowest_floor_price_24h_percentage_change_native,
    highest_floor_price_24h_percentage_change_usd,
    highest_floor_price_24h_percentage_change_native,

    lowest_floor_price_7d_usd,
    lowest_floor_price_7d_native,
    highest_floor_price_7d_usd,
    highest_floor_price_7d_native,
    floor_price_7d_percentage_change_usd,
    floor_price_7d_percentage_change_native,
    lowest_floor_price_7d_percentage_change_usd,
    lowest_floor_price_7d_percentage_change_native,
    highest_floor_price_7d_percentage_change_usd,
    highest_floor_price_7d_percentage_change_native,

    lowest_floor_price_30d_usd,
    lowest_floor_price_30d_native,
    highest_floor_price_30d_usd,
    highest_floor_price_30d_native,
    floor_price_30d_percentage_change_usd,
    floor_price_30d_percentage_change_native,
    lowest_floor_price_30d_percentage_change_usd,
    lowest_floor_price_30d_percentage_change_native,
    highest_floor_price_30d_percentage_change_usd,
    highest_floor_price_30d_percentage_change_native,

    lowest_floor_price_90d_usd,
    lowest_floor_price_90d_native,
    highest_floor_price_90d_usd,
    highest_floor_price_90d_native,
    floor_price_90d_percentage_change_usd,
    floor_price_90d_percentage_change_native,
    lowest_floor_price_90d_percentage_change_usd,
    lowest_floor_price_90d_percentage_change_native,
    highest_floor_price_90d_percentage_change_usd,
    highest_floor_price_90d_percentage_change_native,

    lowest_floor_price_ytd_usd,
    lowest_floor_price_ytd_native,
    highest_floor_price_ytd_usd,
    highest_floor_price_ytd_native,
    floor_price_ytd_percentage_change_usd,
    floor_price_ytd_percentage_change_native,
    lowest_floor_price_ytd_percentage_change_usd,
    lowest_floor_price_ytd_percentage_change_native,
    highest_floor_price_ytd_percentage_change_usd,
    highest_floor_price_ytd_percentage_change_native,
from 
  high_low_price_ytd
  LEFT JOIN
    high_low_price_ninetyDay
  ON
    high_low_price_ytd.id = high_low_price_ninetyDay.id
  LEFT JOIN
    high_low_price_thirtyDay
  ON
    high_low_price_ytd.id = high_low_price_thirtyDay.id
  LEFT JOIN
    high_low_price_sevenDay
  ON
    high_low_price_ytd.id = high_low_price_sevenDay.id
  LEFT JOIN
    high_low_price_oneDay
  ON
    high_low_price_ytd.id = high_low_price_oneDay.id
    where high_low_price_ytd.id = 'bored-ape-yacht-club'