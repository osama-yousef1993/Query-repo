WITH
  oneday AS (
  SELECT
    id,
    AVG(floorPrice_native) avg_floor_price_1d,
    AVG(one_day_average_sale_price) avg_sale_price_1d,
    SUM(one_day_sales) total_sales_1d,
    AVG(one_day_sales_24h_percentage_change) avg_total_sales_pct_change_1d,
    AVG(one_day_average_sale_price_24h_percentage_change) avg_sales_price_change_1d,
  FROM (
    SELECT
      id,
      occurance_time,
      floorPrice_native,
      one_day_average_sale_price,
      one_day_sales,
      one_day_sales_24h_percentage_change,
      one_day_average_sale_price_24h_percentage_change,
      ROW_NUMBER() OVER (PARTITION BY id ORDER BY occurance_time DESC) AS rn
    FROM
      api-project-901373404215.digital_assets.Digital_Assets_NFT_MarketData_dev
    WHERE
      DATE(occurance_time) = CURRENT_DATE() )
  WHERE
    rn = 1
  GROUP BY
    id ),
  sevenDay AS (
  SELECT
    id,
    AVG(floorPrice_native) avg_floor_price_7d,
    AVG(one_day_average_sale_price) avg_sale_price_7d,
    SUM(one_day_sales) total_sales_7d,
    AVG(one_day_sales_24h_percentage_change) avg_total_sales_pct_change_7d,
    AVG(one_day_average_sale_price_24h_percentage_change) avg_sales_price_change_7d,
  FROM (
    SELECT
      id,
      occurance_time,
      floorPrice_native,
      one_day_average_sale_price,
      one_day_sales,
      one_day_sales_24h_percentage_change,
      one_day_average_sale_price_24h_percentage_change,
      ROW_NUMBER() OVER (PARTITION BY id, DATE(occurance_time)
      ORDER BY
        occurance_time DESC) AS rn
    FROM
      api-project-901373404215.digital_assets.Digital_Assets_NFT_MarketData_dev )
  WHERE
    rn = 1
    AND occurance_time >= CURRENT_TIMESTAMP - INTERVAL 7 day
  GROUP BY
    id ),
  thirtyDay AS (
  SELECT
    id,
    AVG(floorPrice_native) avg_floor_price_30d,
    AVG(one_day_average_sale_price) avg_sale_price_30d,
    SUM(one_day_sales) total_sales_30d,
    AVG(one_day_sales_24h_percentage_change) avg_total_sales_pct_change_30d,
    AVG(one_day_average_sale_price_24h_percentage_change) avg_sales_price_change_30d,
  FROM (
    SELECT
      id,
      occurance_time,
      floorPrice_native,
      one_day_average_sale_price,
      one_day_sales,
      one_day_sales_24h_percentage_change,
      one_day_average_sale_price_24h_percentage_change,
      ROW_NUMBER() OVER (PARTITION BY id, DATE(occurance_time)
      ORDER BY
        occurance_time DESC) AS rn
    FROM
      api-project-901373404215.digital_assets.Digital_Assets_NFT_MarketData_dev )
  WHERE
    rn = 1
    AND occurance_time >= CURRENT_TIMESTAMP - INTERVAL 30 day
  GROUP BY
    id ),
  ninetyDay AS (
  SELECT
    id,
    AVG(floorPrice_native) avg_floor_price_90d,
    AVG(one_day_average_sale_price) avg_sale_price_90d,
    SUM(one_day_sales) total_sales_90d,
    AVG(one_day_sales_24h_percentage_change) avg_total_sales_pct_change_90d,
    AVG(one_day_average_sale_price_24h_percentage_change) avg_sales_price_change_90d,
  FROM (
    SELECT
      id,
      occurance_time,
      floorPrice_native,
      one_day_average_sale_price,
      one_day_sales,
      one_day_sales_24h_percentage_change,
      one_day_average_sale_price_24h_percentage_change,
      ROW_NUMBER() OVER (PARTITION BY id, DATE(occurance_time)
      ORDER BY
        occurance_time DESC) AS rn
    FROM
      api-project-901373404215.digital_assets.Digital_Assets_NFT_MarketData_dev )
  WHERE
    rn = 1
    AND occurance_time >= CURRENT_TIMESTAMP - INTERVAL 90 day
  GROUP BY
    id ),
  YTD AS (
  SELECT
    id,
    AVG(floorPrice_native) avg_floor_price_ytd,
    AVG(one_day_average_sale_price) avg_sale_price_ytd,
    SUM(one_day_sales) total_sales_ytd,
    AVG(one_day_sales_24h_percentage_change) avg_total_sales_pct_change_ytd,
    AVG(one_day_average_sale_price_24h_percentage_change) avg_sales_price_change_ytd,
  FROM (
    SELECT
      id,
      occurance_time,
      floorPrice_native,
      one_day_average_sale_price,
      one_day_sales,
      one_day_sales_24h_percentage_change,
      one_day_average_sale_price_24h_percentage_change,
      ROW_NUMBER() OVER (PARTITION BY id, DATE(occurance_time)
      ORDER BY
        occurance_time DESC) AS rn
    FROM
      api-project-901373404215.digital_assets.Digital_Assets_NFT_MarketData_dev )
  WHERE
    rn = 1
    AND occurance_time >= CAST(DATE_TRUNC(CURRENT_DATE(), year) AS Timestamp)
  GROUP BY
    id ),
  date_ranges AS (
  SELECT
    CURRENT_DATE() AS current_date,
    DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS one_day_ago,
    DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AS seven_days_ago,
    DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AS thirty_days_ago,
    DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) AS ninety_days_ago,
    DATE_TRUNC(CURRENT_DATE(), YEAR) AS year_start_date ),
  volume_intervals AS (
  SELECT
    da.id,
    'current' AS period,
    SUM(CASE
        WHEN DATE(da.occurance_time) = CURRENT_DATE() THEN da.volumeUSD
        ELSE 0
    END
      ) AS volumeUSD_1d,
    SUM(CASE
        WHEN DATE(da.occurance_time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 6 DAY) AND CURRENT_DATE() THEN da.volumeUSD
        ELSE 0
    END
      ) AS volumeUSD_7d,
    SUM(CASE
        WHEN DATE(da.occurance_time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 29 DAY) AND CURRENT_DATE() THEN da.volumeUSD
        ELSE 0
    END
      ) AS volumeUSD_30d,
    SUM(CASE
        WHEN DATE(da.occurance_time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 89 DAY) AND CURRENT_DATE() THEN da.volumeUSD
        ELSE 0
    END
      ) AS volumeUSD_90d,
    SUM(CASE
        WHEN DATE(da.occurance_time) BETWEEN DATE_TRUNC(CURRENT_DATE(), YEAR) AND CURRENT_DATE() THEN da.volumeUSD
        ELSE 0
    END
      ) AS volumeUSD_ytd,
    SUM(CASE
        WHEN DATE(da.occurance_time) = CURRENT_DATE() THEN da.volumeNative
        ELSE 0
    END
      ) AS volumeNative_1d,
    SUM(CASE
        WHEN DATE(da.occurance_time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 6 DAY) AND CURRENT_DATE() THEN da.volumeNative
        ELSE 0
    END
      ) AS volumeNative_7d,
    SUM(CASE
        WHEN DATE(da.occurance_time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 29 DAY) AND CURRENT_DATE() THEN da.volumeNative
        ELSE 0
    END
      ) AS volumeNative_30d,
    SUM(CASE
        WHEN DATE(da.occurance_time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 89 DAY) AND CURRENT_DATE() THEN da.volumeNative
        ELSE 0
    END
      ) AS volumeNative_90d,
    SUM(CASE
        WHEN DATE(da.occurance_time) BETWEEN DATE_TRUNC(CURRENT_DATE(), YEAR) AND CURRENT_DATE() THEN da.volumeNative
        ELSE 0
    END
      ) AS volumeNative_ytd
  FROM
    api-project-901373404215.digital_assets.Digital_Assets_NFT_MarketData_dev AS da
  GROUP BY
    da.id
  UNION ALL
  SELECT
    da.id,
    'previous' AS period,
    SUM(CASE
        WHEN DATE(da.occurance_time) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) THEN da.volumeUSD
        ELSE 0
    END
      ) AS volumeUSD_1d,
    SUM(CASE
        WHEN DATE(da.occurance_time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 13 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) THEN da.volumeUSD
        ELSE 0
    END
      ) AS volumeUSD_7d,
    SUM(CASE
        WHEN DATE(da.occurance_time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 59 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN da.volumeUSD
        ELSE 0
    END
      ) AS volumeUSD_30d,
    SUM(CASE
        WHEN DATE(da.occurance_time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 179 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) THEN da.volumeUSD
        ELSE 0
    END
      ) AS volumeUSD_90d,
    SUM(CASE
        WHEN DATE(da.occurance_time) BETWEEN DATE_SUB(DATE_TRUNC(CURRENT_DATE(), YEAR), INTERVAL 1 YEAR) AND DATE_TRUNC(CURRENT_DATE(), YEAR) THEN da.volumeUSD
        ELSE 0
    END
      ) AS volumeUSD_ytd,
    SUM(CASE
        WHEN DATE(da.occurance_time) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) THEN da.volumeNative
        ELSE 0
    END
      ) AS volumeNative_1d,
    SUM(CASE
        WHEN DATE(da.occurance_time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 13 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) THEN da.volumeNative
        ELSE 0
    END
      ) AS volumeNative_7d,
    SUM(CASE
        WHEN DATE(da.occurance_time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 59 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN da.volumeNative
        ELSE 0
    END
      ) AS volumeNative_30d,
    SUM(CASE
        WHEN DATE(da.occurance_time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 179 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) THEN da.volumeNative
        ELSE 0
    END
      ) AS volumeNative_90d,
    SUM(CASE
        WHEN DATE(da.occurance_time) BETWEEN DATE_SUB(DATE_TRUNC(CURRENT_DATE(), YEAR), INTERVAL 1 YEAR) AND DATE_TRUNC(CURRENT_DATE(), YEAR) THEN da.volumeNative
        ELSE 0
    END
      ) AS volumeNative_ytd
  FROM
    api-project-901373404215.digital_assets.Digital_Assets_NFT_MarketData_dev AS da
  GROUP BY
    da.id ),
  percentage_change AS (
  SELECT
    current_table.id,
    100 * ((current_table.volumeUSD_1d - previous_table.volumeUSD_1d) / NULLIF(previous_table.volumeUSD_1d, 0)) AS pct_change_volumeUSD_1d,
    100 * ((current_table.volumeUSD_7d - previous_table.volumeUSD_7d) / NULLIF(previous_table.volumeUSD_7d, 0)) AS pct_change_volumeUSD_7d,
    100 * ((current_table.volumeUSD_30d - previous_table.volumeUSD_30d) / NULLIF(previous_table.volumeUSD_30d, 0)) AS pct_change_volumeUSD_30d,
    100 * ((current_table.volumeUSD_90d - previous_table.volumeUSD_90d) / NULLIF(previous_table.volumeUSD_90d, 0)) AS pct_change_volumeUSD_90d,
    100 * ((current_table.volumeUSD_ytd - previous_table.volumeUSD_ytd) / NULLIF(previous_table.volumeUSD_ytd, 0)) AS pct_change_volumeUSD_ytd,
    100 * ((current_table.volumeNative_1d - previous_table.volumeNative_1d) / NULLIF(previous_table.volumeNative_1d, 0)) AS pct_change_volumeNative_1d,
    100 * ((current_table.volumeNative_7d - previous_table.volumeNative_7d) / NULLIF(previous_table.volumeNative_7d, 0)) AS pct_change_volumeNative_7d,
    100 * ((current_table.volumeNative_30d - previous_table.volumeNative_30d) / NULLIF(previous_table.volumeNative_30d, 0)) AS pct_change_volumeNative_30d,
    100 * ((current_table.volumeNative_90d - previous_table.volumeNative_90d) / NULLIF(previous_table.volumeNative_90d, 0)) AS pct_change_volumeNative_90d,
    100 * ((current_table.volumeNative_ytd - previous_table.volumeNative_ytd) / NULLIF(previous_table.volumeNative_ytd, 0)) AS pct_change_volumeNative_ytd
  FROM (
    SELECT
      *
    FROM
      volume_intervals
    WHERE
      period = 'current') AS current_table
  JOIN (
    SELECT
      *
    FROM
      volume_intervals
    WHERE
      period = 'previous') AS previous_table
  ON
    current_table.id = previous_table.id )
SELECT
  YTD.id AS id,
  avg_floor_price_1d,
  avg_sale_price_1d,
  total_sales_1d,
  avg_total_sales_pct_change_1d,
  avg_sales_price_change_1d,
  avg_floor_price_7d,
  avg_sale_price_7d,
  total_sales_7d,
  avg_total_sales_pct_change_7d,
  avg_sales_price_change_7d,
  avg_floor_price_30d,
  avg_sale_price_30d,
  total_sales_30d,
  avg_total_sales_pct_change_30d,
  avg_sales_price_change_30d,
  avg_floor_price_90d,
  avg_sale_price_90d,
  total_sales_90d,
  avg_total_sales_pct_change_90d,
  avg_sales_price_change_90d,
  avg_floor_price_ytd,
  avg_sale_price_ytd,
  total_sales_ytd,
  avg_total_sales_pct_change_ytd,
  avg_sales_price_change_ytd,
  IFNULL(pct_change_volumeUSD_1d,0) pct_change_volumeUSD_1d,
  IFNULL(pct_change_volumeUSD_7d,0) pct_change_volumeUSD_7d,
  IFNULL(pct_change_volumeUSD_30d,0) pct_change_volumeUSD_30d,
  IFNULL(pct_change_volumeUSD_90d,0) pct_change_volumeUSD_90d,
  IFNULL(pct_change_volumeUSD_ytd,0) pct_change_volumeUSD_ytd,
  IFNULL(pct_change_volumeNative_1d,0) pct_change_volumeNative_1d,
  IFNULL(pct_change_volumeNative_7d,0) pct_change_volumeNative_7d,
  IFNULL( pct_change_volumeNative_30d,0) pct_change_volumeNative_30d,
  IFNULL(pct_change_volumeNative_90d,0) pct_change_volumeNative_90d,
  IFNULL(pct_change_volumeNative_ytd, 0) pct_change_volumeNative_ytd
FROM
  YTD
LEFT JOIN
  ninetyDay
ON
  YTD.id = ninetyDay.id
LEFT JOIN
  thirtyDay
ON
  YTD.id = thirtyday.id
LEFT JOIN
  sevenDay
ON
  YTD.id = sevenday.id
LEFT JOIN
  oneDay
ON
  YTD.id = oneDay.id
LEFT JOIN
  percentage_change
ON
  YTD.id = percentage_change.id