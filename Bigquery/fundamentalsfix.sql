	WITH allTime AS (
		SELECT
			CAST(MIN(Price) AS FLOAT64) all_time_low,
			CAST(MAX(Price) AS FLOAT64) all_time_high,
			ID AS symbol
		FROM
			api-project-901373404215.digital_assets.Digital_Asset_MarketData_dev_updated
		GROUP BY
			ID
	),
	oneHour AS (
		SELECT
			CAST(MAX(Price) AS FLOAT64) high_1h,
			CAST(MIN(Price) AS FLOAT64) low_1h,
			ANY_VALUE(
				STRUCT(Occurance_Time, MarketCap, Volume, Price)
				having MIN Occurance_Time
			) AS open_value,
			ANY_VALUE(
				STRUCT(Occurance_Time, MarketCap, Volume, Price)
				having MAX Occurance_Time
			) AS close_value,
			ID AS symbol
		FROM
			api-project-901373404215.digital_assets.Digital_Asset_MarketData_dev
		WHERE
			Occurance_Time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
		GROUP BY
			ID
	),
	oneDay AS (
		SELECT
			CAST(MAX(Price) AS FLOAT64) high_1d,
			CAST(MIN(Price) AS FLOAT64) low_1d,
			ANY_VALUE(
				STRUCT(Occurance_Time, MarketCap, Volume)
				having MIN Occurance_Time
			) AS open_value,
			ANY_VALUE(
				STRUCT(Occurance_Time, MarketCap, Volume, Price)
				having MAX Occurance_Time
			) AS close_value,
			ID AS symbol
		FROM
			api-project-901373404215.digital_assets.Digital_Asset_MarketData_dev
		WHERE
			Occurance_Time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 Hour)
		GROUP BY
			ID
	),
	sevenDays AS (
		SELECT
			CAST(MAX(Price) AS FLOAT64) high_7d,
			CAST(MIN(Price) AS FLOAT64) low_7d,
			ANY_VALUE(
				STRUCT(Occurance_Time, MarketCap, Volume)
				having MIN Occurance_Time
			) AS open_value,
			ID as symbol
		FROM
			api-project-901373404215.digital_assets.Digital_Asset_MarketData_dev
		WHERE
			Occurance_Time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
		GROUP BY
			ID
	),
	thirtyDays AS (
		SELECT
			CAST(MAX(Price) AS FLOAT64) high_30d,
			CAST(MIN(Price) AS FLOAT64) low_30d,
			ANY_VALUE(
				STRUCT(Occurance_Time, MarketCap, Volume)
				having MIN Occurance_Time
			) AS open_value,
			ID AS symbol
		FROM
			api-project-901373404215.digital_assets.Digital_Asset_MarketData_dev
		WHERE
			Occurance_Time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
		GROUP BY
			ID
	),
	oneYear AS (
		SELECT
			CAST(MAX(Price) AS FLOAT64) high_1y,
			CAST(MIN(Price) AS FLOAT64) low_1y,
			ANY_VALUE(
				STRUCT(Occurance_Time, MarketCap, Volume)
				having MIN Occurance_Time
			) AS open_value,
			ID AS symbol
		FROM
			api-project-901373404215.digital_assets.Digital_Asset_MarketData_dev
		WHERE
			Occurance_Time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
		GROUP BY
			ID
	),
	YTD AS (
		SELECT
			CAST(MAX(Price) AS FLOAT64) high_ytd,
			CAST(MIN(Price) AS FLOAT64) low_ytd,
			ANY_VALUE(
				STRUCT(Occurance_Time, MarketCap, Volume, Price)
				having MIN Occurance_Time
			) AS open_value,
			ID AS symbol
		FROM
			api-project-901373404215.digital_assets.Digital_Asset_MarketData_dev
		WHERE
			Occurance_Time >= CAST(DATE_TRUNC(CURRENT_DATE(), year) AS Timestamp)
		GROUP BY
			ID
	),
	market_pairs AS (
		SELECT
			tickers.CoinID AS symbol,
			COUNT(CONCAT(tickers.CoinID, tickers.Target)) AS num_markets
		FROM
			api-project-901373404215.digital_assets.Digital_Asset_Exchanges_Tickers_Data_dev d
			JOIN UNNEST(d.Tickers) AS tickers
		WHERE
			tickers.Timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 hour)
		GROUP BY
			tickers.CoinID
	)
	SELECT
		CAST(oneHour.high_1h AS FLOAT64) AS high_1h,
		CAST(oneHour.low_1h AS FLOAT64) AS low_1h,
		CAST(oneHour.open_value.MarketCap AS FLOAT64) AS market_cap_open_1h,
		CAST(oneDay.close_value.MarketCap AS FLOAT64) AS market_cap_close_1h,
		CAST(oneHour.open_value.Volume AS FLOAT64) AS volume_open_1h,
		CAST(oneDay.close_value.Volume AS FLOAT64) AS volume_close_1h,
		CAST(oneHour.open_value.Price AS FLOAT64) AS price_open_1h,
		CAST(oneDay.close_value.Price AS FLOAT64) AS price_close_1h,
		CAST(oneDay.high_1d AS FLOAT64) AS high_24h,
		CAST(oneDay.low_1d AS FLOAT64) AS low_24h,
		CAST(oneDay.open_value.MarketCap AS FLOAT64) AS market_cap_open_24h,
		CAST(oneDay.close_value.MarketCap AS FLOAT64) AS market_cap_close_24h,
		CAST(oneDay.open_value.Volume AS FLOAT64) AS volume_open_24h,
		CAST(oneDay.close_value.Volume AS FLOAT64) AS volume_close_24h,
		CAST(sevenDays.high_7d AS FLOAT64) AS high_7d,
		CAST(sevenDays.low_7d AS FLOAT64) AS low_7d,
		CAST(sevenDays.open_value.MarketCap AS FLOAT64) AS market_cap_open_7d,
		CAST(oneDay.close_value.MarketCap AS FLOAT64) AS market_cap_close_7d,
		CAST(sevenDays.open_value.Volume AS FLOAT64) AS volume_open_7d,
		CAST(oneDay.close_value.Volume AS FLOAT64) AS volume_close_7d,
		CAST(thirtyDays.high_30d AS FLOAT64) AS high_30d,
		CAST(thirtyDays.low_30d AS FLOAT64) AS low_30d,
		CAST(thirtyDays.open_value.MarketCap AS FLOAT64) AS market_cap_open_30d,
		CAST(oneDay.close_value.MarketCap AS FLOAT64) AS market_cap_close_30d,
		CAST(thirtyDays.open_value.Volume AS FLOAT64) AS volume_open_30d,
		CAST(oneDay.close_value.Volume AS FLOAT64) AS volume_close_30d,
		CAST(oneYear.high_1y AS FLOAT64) AS high_1y,
		CAST(oneYear.low_1y AS FLOAT64) AS low_1y,
		CAST(oneYear.open_value.MarketCap AS FLOAT64) AS market_cap_open_1y,
		CAST(oneDay.close_value.MarketCap AS FLOAT64) AS market_cap_close_1y,
		CAST(oneYear.open_value.Volume AS FLOAT64) AS volume_open_1y,
		CAST(oneDay.close_value.Volume AS FLOAT64) AS volume_close_1y,
		CAST(YTD.high_ytd AS FLOAT64) AS high_ytd,
		CAST(YTD.low_ytd AS FLOAT64) AS low_ytd,
		CAST(YTD.open_value.MarketCap AS FLOAT64) AS market_cap_open_ytd,
		CAST(oneDay.close_value.MarketCap AS FLOAT64) AS market_cap_close_ytd,
		CAST(YTD.open_value.Volume AS FLOAT64) AS volume_open_ytd,
		CAST(oneDay.close_value.Volume AS FLOAT64) AS volume_close_ytd,
		CAST(YTD.open_value.Price AS FLOAT64) AS price_open_ytd,
		CAST(oneDay.close_value.Price AS FLOAT64) AS price_close_ytd,
		CAST(allTime.all_time_low AS FLOAT64) AS all_time_low,
		CAST(allTime.all_time_high AS FLOAT64) AS all_time_high,
		CASE
			When market_pairs.num_markets is null Then 0
			ELSE market_pairs.num_markets
		END as number_of_active_market_pairs,
		alltime.symbol
	FROM
		allTime
		left JOIN sevenDays ON sevenDays.symbol = allTime.symbol
		left JOIN thirtyDays ON thirtyDays.symbol = allTime.symbol
		left JOIN oneYear ON oneYear.symbol = allTime.symbol
		left JOIN YTD ON YTD.symbol = allTime.symbol
		left JOIN oneDay ON oneDay.symbol = allTime.symbol
		left JOIN oneHour ON oneHour.symbol = allTime.symbol
		LEFT JOIN market_pairs ON market_pairs.symbol = allTime.symbol





















WITH
  allTime AS (
  SELECT
    CAST(MIN(Price) AS FLOAT64) all_time_low,
    CAST(MAX(Price) AS FLOAT64) all_time_high,
    ID AS symbol
  FROM
    api-project-901373404215.digital_assets.Digital_Asset_MarketData_dev_updated
      -- WHERE
    -- forbes_id IS NOT NULL
    -- id = 'subsquid'
  GROUP BY
    ID ),
  old_allTime AS (
  SELECT
    CAST(MIN(Price) AS FLOAT64) all_time_low,
    CAST(MAX(Price) AS FLOAT64) all_time_high,
    ID AS symbol,
    REGEXP_REPLACE(forbes_id, '-[0-9]+$', '') as forbes_id
  FROM
    api-project-901373404215.digital_assets.Digital_Asset_MarketData_dev_updated
  -- WHERE
  --   forbes_id IS NOT NULL
    -- and
    -- id = 'subsquid'
  GROUP BY
  ID,
    REGEXP_REPLACE(forbes_id, '-[0-9]+$', '') )
SELECT
  allTime.symbol AS symbol,
  old_allTime.symbol AS old_symbol,
  old_allTime.forbes_id AS forbes_id,
  allTime.all_time_low AS all_time_low,
  old_allTime.all_time_low AS old_all_time_low,
  allTime.all_time_high AS all_time_high,
  old_allTime.all_time_high AS old_all_time_high
FROM
  allTime
LEFT JOIN
  old_allTime
ON
  allTime.symbol = old_allTime.symbol







SELECT
  CAST(MIN(Price) AS FLOAT64) AS all_time_low,
  CAST(MAX(Price) AS FLOAT64) AS all_time_high,
  STRING_AGG(DISTINCT ID, ', ') AS symbol,
  REGEXP_REPLACE(forbes_id, '-[0-9]+$', '') AS forbes_id
FROM
  api-project-901373404215.digital_assets.Digital_Asset_MarketData_dev_updated
WHERE
  -- forbes_id IS NOT NULL
  -- AND 
  id = 'bitcoin'
GROUP BY
  REGEXP_REPLACE(forbes_id, '-[0-9]+$', '')







CREATE TABLE `api-project-901373404215.digital_assets.Digital_Asset_MarketData_dev_updated` AS
SELECT
  *
FROM
  `api-project-901373404215.digital_assets.Digital_Asset_MarketData_dev`;


UPDATE api-project-901373404215.digital_assets.Digital_Asset_MarketData_dev_updated AS target
SET forbes_id = (
  SELECT MIN(forbes_id)
  FROM api-project-901373404215.digital_assets.Digital_Asset_MarketData_dev_updated AS source
  WHERE source.ID = target.ID
    AND source.forbes_id IS NOT NULL
)
WHERE target.forbes_id IS NULL;




MERGE INTO `api-project-901373404215.digital_assets.Digital_Asset_MarketData_dev_updated` target
USING (
  SELECT 
    id,
    forbes_id, Occurance_Time
  FROM (
    SELECT 
      id,
      forbes_id,
      Occurance_Time,
      ROW_NUMBER() OVER(PARTITION BY id ORDER BY Occurance_Time DESC) as rn
    FROM 
      `api-project-901373404215.digital_assets.Digital_Asset_MarketData_dev_updated`
  ) 
  WHERE rn = 1
) source
ON target.id = source.id AND target.forbes_id != source.forbes_id
WHEN MATCHED THEN
  UPDATE SET forbes_id = source.forbes_id




WITH
  allTime AS (
  SELECT
    all_time_low,
    all_time_high,
    forbes_id,
    COALESCE(num_markets, 0) AS number_of_active_market_pairs
  FROM (
    SELECT
      CAST(MIN(Price) AS FLOAT64) all_time_low,
      CAST(MAX(Price) AS FLOAT64) all_time_high,
      ID AS symbol,
      forbes_id
    FROM
      api-project-901373404215.digital_assets.Digital_Asset_MarketData_dev_updated
    WHERE
      forbes_id IS NOT NULL
    GROUP BY
      ID,
      forbes_id ) AS s
  LEFT JOIN (
    SELECT
      tickers.CoinID AS symbol,
      COUNT(CONCAT(tickers.CoinID, tickers.Target)) AS num_markets
    FROM
      api-project-901373404215.digital_assets.Digital_Asset_Exchanges_Tickers_Data_dev d
    JOIN
      UNNEST(d.Tickers) AS tickers
    WHERE
      tickers.Timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 hour)
    GROUP BY
      tickers.CoinID ) AS t
  ON
    s.symbol = t.symbol ),
  oneHour AS (
  SELECT
    CAST(MAX(Price) AS FLOAT64) high_1h,
    CAST(MIN(Price) AS FLOAT64) low_1h,
    ANY_VALUE( STRUCT(Occurance_Time,
        MarketCap,
        Volume,
        Price)
    HAVING
      MIN Occurance_Time ) AS open_value,
    ANY_VALUE( STRUCT(Occurance_Time,
        MarketCap,
        Volume,
        Price)
    HAVING
      MAX Occurance_Time ) AS close_value,
    -- ID AS symbol,
    forbes_id
  FROM
    api-project-901373404215.digital_assets.Digital_Asset_MarketData_dev_updated
  WHERE
    Occurance_Time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
    -- and source = 'coingecko'
  GROUP BY
    -- ID,
    forbes_id ),
  oneDay AS (
  SELECT
    CAST(MAX(Price) AS FLOAT64) high_1d,
    CAST(MIN(Price) AS FLOAT64) low_1d,
    ANY_VALUE( STRUCT(Occurance_Time,
        MarketCap,
        Volume)
    HAVING
      MIN Occurance_Time ) AS open_value,
    ANY_VALUE( STRUCT(Occurance_Time,
        MarketCap,
        Volume,
        Price)
    HAVING
      MAX Occurance_Time ) AS close_value,
    -- ID AS symbol,
    forbes_id
  FROM
    api-project-901373404215.digital_assets.Digital_Asset_MarketData_dev_updated
  WHERE
    Occurance_Time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 Hour)
    -- and source = 'coingecko'
  GROUP BY
    -- ID,
    forbes_id ),
  sevenDays AS (
  SELECT
    CAST(MAX(Price) AS FLOAT64) high_7d,
    CAST(MIN(Price) AS FLOAT64) low_7d,
    ANY_VALUE( STRUCT(Occurance_Time,
        MarketCap,
        Volume)
    HAVING
      MIN Occurance_Time ) AS open_value,
    -- ID as symbol,
    forbes_id
  FROM
    api-project-901373404215.digital_assets.Digital_Asset_MarketData_dev_updated
  WHERE
    Occurance_Time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    -- and source = 'coingecko'
  GROUP BY
    -- ID,
    forbes_id ),
  thirtyDays AS (
  SELECT
    CAST(MAX(Price) AS FLOAT64) high_30d,
    CAST(MIN(Price) AS FLOAT64) low_30d,
    ANY_VALUE( STRUCT(Occurance_Time,
        MarketCap,
        Volume)
    HAVING
      MIN Occurance_Time ) AS open_value,
    -- ID AS symbol,
    forbes_id
  FROM
    api-project-901373404215.digital_assets.Digital_Asset_MarketData_dev_updated
  WHERE
    Occurance_Time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
    -- and source = 'coingecko'
    -- and id = 'cat-token'
  GROUP BY
    -- ID,
    forbes_id ),
  oneYear AS (
  SELECT
    CAST(MAX(Price) AS FLOAT64) high_1y,
    CAST(MIN(Price) AS FLOAT64) low_1y,
    ANY_VALUE( STRUCT(Occurance_Time,
        MarketCap,
        Volume)
    HAVING
      MIN Occurance_Time ) AS open_value,
    -- ID AS symbol,
    forbes_id
  FROM
    api-project-901373404215.digital_assets.Digital_Asset_MarketData_dev_updated
  WHERE
    Occurance_Time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
    -- and source = 'coingecko'
    -- and id = 'cat-token'
  GROUP BY
    -- ID,
    forbes_id ),
  YTD AS (
  SELECT
    CAST(MAX(Price) AS FLOAT64) high_ytd,
    CAST(MIN(Price) AS FLOAT64) low_ytd,
    ANY_VALUE( STRUCT(Occurance_Time,
        MarketCap,
        Volume,
        Price)
    HAVING
      MIN Occurance_Time ) AS open_value,
    -- ID AS symbol,
    forbes_id
  FROM
    api-project-901373404215.digital_assets.Digital_Asset_MarketData_dev_updated
  WHERE
    Occurance_Time >= CAST(DATE_TRUNC(CURRENT_DATE(), year) AS Timestamp)
    -- and source = 'coingecko'
    -- and id = 'cat-token'
  GROUP BY
    -- ID,
    forbes_id )
  -- market_pairs AS (
  -- SELECT
  --   tickers.CoinID AS symbol,
  --   COUNT(CONCAT(tickers.CoinID, tickers.Target)) AS num_markets
  -- FROM
  --   api-project-901373404215.digital_assets.Digital_Asset_Exchanges_Tickers_Data_dev d
  -- JOIN
  --   UNNEST(d.Tickers) AS tickers
  -- WHERE
  --   tickers.Timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 hour)
  -- GROUP BY
  --   tickers.CoinID )
SELECT
  CAST(oneHour.high_1h AS FLOAT64) AS high_1h,
  CAST(oneHour.low_1h AS FLOAT64) AS low_1h,
  CAST(oneHour.open_value.MarketCap AS FLOAT64) AS market_cap_open_1h,
  CAST(oneDay.close_value.MarketCap AS FLOAT64) AS market_cap_close_1h,
  CAST(oneHour.open_value.Volume AS FLOAT64) AS volume_open_1h,
  CAST(oneDay.close_value.Volume AS FLOAT64) AS volume_close_1h,
  CAST(oneHour.open_value.Price AS FLOAT64) AS price_open_1h,
  CAST(oneDay.close_value.Price AS FLOAT64) AS price_close_1h,
  CAST(oneDay.high_1d AS FLOAT64) AS high_24h,
  CAST(oneDay.low_1d AS FLOAT64) AS low_24h,
  CAST(oneDay.open_value.MarketCap AS FLOAT64) AS market_cap_open_24h,
  CAST(oneDay.close_value.MarketCap AS FLOAT64) AS market_cap_close_24h,
  CAST(oneDay.open_value.Volume AS FLOAT64) AS volume_open_24h,
  CAST(oneDay.close_value.Volume AS FLOAT64) AS volume_close_24h,
  CAST(sevenDays.high_7d AS FLOAT64) AS high_7d,
  CAST(sevenDays.low_7d AS FLOAT64) AS low_7d,
  CAST(sevenDays.open_value.MarketCap AS FLOAT64) AS market_cap_open_7d,
  CAST(oneDay.close_value.MarketCap AS FLOAT64) AS market_cap_close_7d,
  CAST(sevenDays.open_value.Volume AS FLOAT64) AS volume_open_7d,
  CAST(oneDay.close_value.Volume AS FLOAT64) AS volume_close_7d,
  CAST(thirtyDays.high_30d AS FLOAT64) AS high_30d,
  CAST(thirtyDays.low_30d AS FLOAT64) AS low_30d,
  CAST(thirtyDays.open_value.MarketCap AS FLOAT64) AS market_cap_open_30d,
  CAST(oneDay.close_value.MarketCap AS FLOAT64) AS market_cap_close_30d,
  CAST(thirtyDays.open_value.Volume AS FLOAT64) AS volume_open_30d,
  CAST(oneDay.close_value.Volume AS FLOAT64) AS volume_close_30d,
  CAST(oneYear.high_1y AS FLOAT64) AS high_1y,
  CAST(oneYear.low_1y AS FLOAT64) AS low_1y,
  CAST(oneYear.open_value.MarketCap AS FLOAT64) AS market_cap_open_1y,
  CAST(oneDay.close_value.MarketCap AS FLOAT64) AS market_cap_close_1y,
  CAST(oneYear.open_value.Volume AS FLOAT64) AS volume_open_1y,
  CAST(oneDay.close_value.Volume AS FLOAT64) AS volume_close_1y,
  CAST(YTD.high_ytd AS FLOAT64) AS high_ytd,
  CAST(YTD.low_ytd AS FLOAT64) AS low_ytd,
  CAST(YTD.open_value.MarketCap AS FLOAT64) AS market_cap_open_ytd,
  CAST(oneDay.close_value.MarketCap AS FLOAT64) AS market_cap_close_ytd,
  CAST(YTD.open_value.Volume AS FLOAT64) AS volume_open_ytd,
  CAST(oneDay.close_value.Volume AS FLOAT64) AS volume_close_ytd,
  CAST(YTD.open_value.Price AS FLOAT64) AS price_open_ytd,
  CAST(oneDay.close_value.Price AS FLOAT64) AS price_close_ytd,
  CAST(allTime.all_time_low AS FLOAT64) AS all_time_low,
  CAST(allTime.all_time_high AS FLOAT64) AS all_time_high,
  -- CASE
  -- 	When market_pairs.num_markets is null Then 0
  -- 	ELSE market_pairs.num_markets
  -- END as number_of_active_market_pairs,
  -- alltime.symbol,
  alltime.number_of_active_market_pairs,
  alltime.forbes_id
FROM
  allTime
LEFT JOIN
  sevenDays
ON
  sevenDays.forbes_id = allTime.forbes_id
LEFT JOIN
  thirtyDays
ON
  thirtyDays.forbes_id = allTime.forbes_id
LEFT JOIN
  oneYear
ON
  oneYear.forbes_id = allTime.forbes_id
LEFT JOIN
  YTD
ON
  YTD.forbes_id = allTime.forbes_id
LEFT JOIN
  oneDay
ON
  oneDay.forbes_id = allTime.forbes_id
LEFT JOIN
  oneHour
ON
  oneHour.forbes_id = allTime.forbes_id
  -- LEFT JOIN market_pairs ON market_pairs.symbol = allTime.symbol
  -- where alltime.symbol = 'cat-token'




MERGE INTO `api-project-901373404215.digital_assets.Digital_Asset_MarketData_dev_updated` target
USING (
  SELECT 
    id,
    forbes_id, Occurance_Time
  FROM (
    SELECT 
      id,
      forbes_id,
      Occurance_Time,
      ROW_NUMBER() OVER(PARTITION BY id ORDER BY Occurance_Time DESC) as rn
    FROM 
      `api-project-901373404215.digital_assets.Digital_Asset_MarketData_dev_updated`
  ) 
  WHERE rn = 1
) source
ON target.id = source.id
WHEN MATCHED THEN
  UPDATE SET forbes_id = source.forbes_id




-- CREATE TABLE `api-project-901373404215.digital_assets.Digital_Asset_MarketData_dev_updated` AS
-- SELECT
--   *
-- FROM
--   `api-project-901373404215.digital_assets.Digital_Asset_MarketData_dev`;


select * from `api-project-901373404215.digital_assets.Digital_Asset_MarketData_dev_updated`
where 
forbes_id is not null

order by Occurance_Time desc
limit 100


-- stop all scheduler for coingecko and coinpaprika to build assets.
-- remove all fundamentals from PG table.
-- run the consume locally to fix the slug(forbes_id) issue.
-- then run all these query below to fix the difference in forbes_id for all records.
-- build fundamentals again to fill it with correct data.

CREATE TABLE `api-project-901373404215.digital_assets.Digital_Asset_MarketData_dev_updated` AS
SELECT
  *
FROM
  `api-project-901373404215.digital_assets.Digital_Asset_MarketData_dev`;


MERGE INTO `api-project-901373404215.digital_assets.Digital_Asset_MarketData_dev_updated` target
USING (
  SELECT 
    id,
    forbes_id, Occurance_Time
  FROM (
    SELECT 
      id,
      forbes_id,
      Occurance_Time,
      ROW_NUMBER() OVER(PARTITION BY id ORDER BY Occurance_Time DESC) as rn
    FROM 
      `api-project-901373404215.digital_assets.Digital_Asset_MarketData_dev_updated`
  ) 
  WHERE rn = 1
) source
ON target.id = source.id
WHEN MATCHED THEN
  UPDATE SET forbes_id = source.forbes_id;


DROP TABLE `api-project-901373404215.digital_assets.Digital_Asset_MarketData_dev`;

ALTER TABLE `api-project-901373404215.digital_assets.Digital_Asset_MarketData_dev_updated`
RENAME TO `Digital_Asset_MarketData_dev`;