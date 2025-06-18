with 
 		ExchangesPrices AS (
 				SELECT
 					Base AS Symbol,
 					Exchange,
 					AVG(Price) AS Price
 				FROM
 					api-project-901373404215.digital_assets.nomics_exchange_market_ticker
 				WHERE
 					Exchange NOT IN ('bitmex',
 					'hbtc')
 					AND Type = 'spot'
 					AND Timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 1 Day)
 					AND Status = 'active'
 					AND Quote IN ('USD',
 					'USDT',
 					'USDC')
 				GROUP BY
 					Base,
 					Exchange ),
 				exchangeHighLight AS (
 				SELECT
 					COUNT(DISTINCT(MARKET)) AS num_markets,
 					Base AS Symbol,
 					Exchange
 				FROM
 					api-project-901373404215.digital_assets.nomics_exchange_market_ticker
 				WHERE
 					Type = "spot"
 					AND Status = "active"
 					AND Timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 1 Day)
 				GROUP BY
 					base,
 					Exchange ),
 		oneDay AS (
 				SELECT
 					exchange,
 					base AS Symbol,
 					volume
 				FROM (
 					SELECT
 						exchange,
 						AVG(OneD.Volume) AS volume,
 						Base
 					FROM
 						api-project-901373404215.digital_assets.nomics_exchange_market_ticker
 					WHERE
 						Timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
 						AND OneD.Volume IS NOT NULL
 						AND type = "spot"
 						AND Status = "active"
 					GROUP BY
 						exchange,
 						base ) AS oneDay 
 					),
 			sevenDay AS (
 				SELECT
 					exchange,
 					base AS Symbol,
 					volume
 				FROM (
 					SELECT
 						exchange,
 						AVG(OneD.Volume) AS volume,
 						Base
 					FROM
 						api-project-901373404215.digital_assets.nomics_exchange_market_ticker
 					WHERE
 						Timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
 						AND OneD.Volume IS NOT NULL
 						AND type = "spot"
 						AND Status = "active"
 					GROUP BY
 						exchange,
 						base ) AS sevenDay 
 					),
 		thirtyDay AS (
 				SELECT
 					exchange,
 					base AS Symbol,
 					volume
 				FROM (
 					SELECT
 						exchange,
 						AVG(OneD.Volume) AS volume,
 						Base
 					FROM
 						api-project-901373404215.digital_assets.nomics_exchange_market_ticker
 					WHERE
 						Timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
 						AND OneD.Volume IS NOT NULL
 						AND type = "spot"
 						AND Status = "active"
 					GROUP BY
 						exchange,
 						base ) AS thirtyDay 
 					),
 		oneYear AS (
 				SELECT
 					exchange,
 					base AS Symbol,
 					volume
 				FROM (
 					SELECT
 						exchange,
 						AVG(OneD.Volume) AS volume,
 						Base
 					FROM
 						api-project-901373404215.digital_assets.nomics_exchange_market_ticker
 					WHERE
 						Timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
 						AND OneD.Volume IS NOT NULL
 						AND type = "spot"
 						AND Status = "active"
 					GROUP BY
 						exchange,
 						base ) AS oneYear 
 					),
 		oneDayPrice AS (
 				SELECT
 					CAST(Close AS FLOAT64) price_by_exchange_1d,
 					base AS symbol,
 					exchange
 				FROM (
 					SELECT
 						AVG(price) AS Close,
 						base,
 						exchange
 					FROM
 						api-project-901373404215.digital_assets.nomics_exchange_market_ticker
 					WHERE
 						Timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
 						AND quote IN ('USD',
 							'USDT',
 							'USDC')
 						AND type = "spot"
 						AND Status = "active"
 					GROUP BY
 						base,
 						exchange ) AS oneDay ),
 		sevenDayPrice AS (
 				SELECT
 					CAST(Close AS FLOAT64) price_by_exchange_7d,
 					base AS symbol,
 					exchange
 				FROM (
 					SELECT
 						AVG(price) AS Close,
 						base,
 						exchange
 					FROM
 						api-project-901373404215.digital_assets.nomics_exchange_market_ticker
 					WHERE
 						Timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
 						AND quote IN ('USD',
 							'USDT',
 							'USDC')
 						AND type = "spot"
 						AND Status = "active"
 					GROUP BY
 						base,
 						exchange ) AS sevenDay ),
 		thirtyDayPrice AS (
 				SELECT
 					CAST(Close AS FLOAT64) price_by_exchange_30d,
 					base AS symbol,
 					exchange
 				FROM (
 					SELECT
 						AVG(price) AS Close,
 						base,
 						exchange
 					FROM
 						api-project-901373404215.digital_assets.nomics_exchange_market_ticker
 					WHERE
 						Timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
 						AND quote IN ('USD',
 							'USDT',
 							'USDC')
 						AND type = "spot"
 						AND Status = "active"
 					GROUP BY
 						base,
 						exchange ) AS thirtyDay ),
 		oneYearPrice AS (
 				SELECT
 					CAST(Close AS FLOAT64) price_by_exchange_1y,
 					base AS symbol,
 					exchange
 				FROM (
 					SELECT
 						AVG(price) AS Close,
 						base,
 						exchange
 					FROM
 						api-project-901373404215.digital_assets.nomics_exchange_market_ticker
 					WHERE
 						Timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
 						AND quote IN ('USD',
 							'USDT',
 							'USDC')
 						AND type = "spot"
 						AND Status = "active"
 					GROUP BY
 						base,
 						exchange ) AS oneYear )

 		SELECT
 				oneDay.Symbol as symbol,
 				oneDay.exchange,
 				exchangeHighLight.num_markets,
 				ExchangesPrices.Price as price,
 				CAST(oneDay.volume AS float64) AS volume_by_exchange_1d,
 				CAST(oneDayPrice.price_by_exchange_1d AS FLOAT64) AS price_by_exchange_1d,
 				CAST(sevenDay.volume AS float64) AS volume_by_exchange_7d,
 				CAST(sevenDayPrice.price_by_exchange_7d AS FLOAT64) AS price_by_exchange_7d,
 				CAST(thirtyDay.volume AS float64) AS volume_by_exchange_30d,
 				CAST(thirtyDayPrice.price_by_exchange_30d AS FLOAT64) AS price_by_exchange_30d,
 				CAST(oneYear.volume AS float64) AS volume_by_exchange_1y,
 				CAST(oneYearPrice.price_by_exchange_1y AS FLOAT64) AS price_by_exchange_1y
 			FROM
 				ExchangesPrices
 				INNER JOIN
 					exchangeHighLight
 				ON
 					exchangeHighLight.symbol = ExchangesPrices.symbol
 					AND exchangeHighLight.Exchange = ExchangesPrices.Exchange
 				INNER JOIN
 					oneDay
 				ON
 					oneDay.symbol = ExchangesPrices.symbol
 					AND oneDay.Exchange = ExchangesPrices.Exchange
 				INNER JOIN
 					oneDayPrice
 				ON
 					oneDayPrice.symbol = oneDay.symbol
 					AND onedayPrice.Exchange = oneDay.Exchange
 			INNER JOIN
 					sevenDay
 				ON
 					sevenDay.symbol = oneDay.symbol
 					AND sevenDay.Exchange = oneDay.Exchange
 			INNER JOIN
 					sevenDayPrice
 				ON
 					sevenDayPrice.symbol = oneDay.symbol
 					AND sevenDayPrice.Exchange = oneDay.Exchange
 			INNER JOIN
 					thirtyDay
 				ON
 					thirtyDay.symbol = oneDay.symbol
 					AND thirtyDay.Exchange = oneDay.Exchange
 			INNER JOIN
 					thirtyDayPrice
 				ON
 					thirtyDayPrice.symbol = thirtyDay.symbol
 					AND thirtyDayPrice.Exchange = thirtyDay.Exchange
 			INNER JOIN
 					oneYear
 				ON
 					oneYear.symbol = oneDay.symbol
 					AND oneYear.Exchange = oneDay.Exchange
 			INNER JOIN
 					oneYearPrice
 				ON
 					oneYearPrice.symbol = oneYear.symbol
 					AND oneYearPrice.Exchange = oneYear.Exchange
 				WHERE
 					oneDay.volume IS NOT NULL
 					AND oneDayPrice.price_by_exchange_1d IS NOT null
 					AND sevenDayPrice.price_by_exchange_7d IS NOT null
 					AND thirtyDayPrice.price_by_exchange_30d IS NOT null
 					AND oneYearPrice.price_by_exchange_1y IS NOT null