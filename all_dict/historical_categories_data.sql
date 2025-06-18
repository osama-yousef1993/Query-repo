SELECT
  day_start,
  ARRAY_AGG(STRUCT('symbol',
      symbol,
      'Price',
      total_price)) AS beprices
FROM (
  SELECT
    symbol,
    TIMESTAMP_TRUNC(time, DAY) AS day_start,
    MAX(price) AS total_price -- Using MAX() to get one value per 24-hour interval
  FROM (
    SELECT
      ID AS symbol,
      Occurance_Time AS time,
      CAST(Price AS FLOAT64) AS price,
      ROW_NUMBER() OVER (PARTITION BY ID, TIMESTAMP_TRUNC(Occurance_Time, DAY)
      ORDER BY
        Occurance_Time) AS row_num
    FROM
      api-project-901373404215.digital_assets.Digital_Asset_MarketData c
    WHERE
      Occurance_Time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
      -- AND ID = 'bitcoin'
      ) AS foo
  WHERE
    row_num = 1 -- Only select the first row within each 24-hour interval
  GROUP BY
    day_start,
    symbol
  ORDER BY
    day_start DESC ) AS fo
GROUP BY
  day_start
order by day_start desc