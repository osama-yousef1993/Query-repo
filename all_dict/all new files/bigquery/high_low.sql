WITH
 	allTime AS (
 	SELECT
 		CAST(MIN(Close) AS FLOAT64) all_time_low,
 		CAST(MAX(Close) AS FLOAT64) all_time_high,
 		Id AS symbol
 	FROM (
 		SELECT
 		Price AS Close,
 		Id
 		FROM
 			api-project-901373404215.digital_assets.nomics_currencies ) AS allTime
 	GROUP BY
 		Id ),
 	oneDay AS (
 	SELECT
 		CAST(MAX(Close) AS FLOAT64) high_1d,
 		CAST(MIN(Close) AS FLOAT64) low_1d,
 		Id AS symbol
 	FROM (
 		SELECT
 		Price AS Close,
 		Id
 		FROM
 			api-project-901373404215.digital_assets.nomics_currencies
 		WHERE
 		PriceTimestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 24 Hour) ) AS oneDay
 	GROUP BY
 		Id ),
 	sevenDays AS (
 	SELECT
 		CAST(MAX(Close) AS FLOAT64) high_7d,
 		CAST(MIN(Close) AS FLOAT64) low_7d,
 		Id AS symbol
 	FROM (
 		SELECT
 		Price AS Close,
 		Id
 		FROM
 			api-project-901373404215.digital_assets.nomics_currencies
 		WHERE
 		PriceTimestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 7 DAY) ) AS sevenDays
 	GROUP BY
 		Id ),
 	thirtyDays AS (
 	SELECT
 		CAST(MAX(Close) AS FLOAT64) high_30d,
 		CAST(MIN(Close) AS FLOAT64) low_30d,
 		Id AS symbol
 	FROM (
 		SELECT
 		Price AS Close,
 		Id
 		FROM
 			api-project-901373404215.digital_assets.nomics_currencies
 		WHERE
 		PriceTimestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 30 DAY) ) AS thirtyDays
 	GROUP BY
 		Id ),
 	oneYear AS (
 	SELECT
 		CAST(MAX(Close) AS FLOAT64) high_1y,
 		CAST(MIN(Close) AS FLOAT64) low_1y,
 		Id AS symbol
 	FROM (
 		SELECT
 		Price AS Close,
 		Id
 		FROM
 			api-project-901373404215.digital_assets.nomics_currencies
 		WHERE
 		PriceTimestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 365 DAY) ) AS oneYear
 	GROUP BY
 		Id )
 	SELECT
 		CAST(MAX(oneDay.high_1d) AS FLOAT64) AS high_24h,
 		CAST(MIN(oneDay.low_1d) AS FLOAT64) AS low_24h,
 		CAST(MAX(sevenDays.high_7d) AS FLOAT64) AS high_7d,
 		CAST(MIN(sevenDays.low_7d) AS FLOAT64) AS low_7d,
 		CAST(MAX(thirtyDays.high_30d) AS FLOAT64) AS high_30d,
 		CAST(MIN(thirtyDays.low_30d) AS FLOAT64) AS low_30d,
 		CAST(MAX(oneYear.high_1y) AS FLOAT64) AS high_1y,
 		CAST(MIN(oneYear.low_1y) AS FLOAT64) AS low_1y,
 		CAST(MIN(allTime.all_time_low) AS FLOAT64) AS all_time_low,
 		CAST(MAX(allTime.all_time_high) AS FLOAT64) AS all_time_high,
 		oneDay.symbol
 	FROM
 		oneDay
 	INNER JOIN
 		sevenDays
 	ON
 		sevenDays.symbol = oneDay.symbol
 	INNER JOIN
 		thirtyDays
 	ON
 		thirtyDays.symbol = oneDay.symbol
 	INNER JOIN
 		oneYear
 	ON
 		oneYear.symbol = oneDay.symbol
 	INNER JOIN
 		allTime
 	ON
 		allTime.symbol = oneDay.symbol
 	GROUP BY
 		oneDay.symbol