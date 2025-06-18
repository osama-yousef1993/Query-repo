with market as (
 		SELECT Base, Exchange, Quote, CONCAT(Base, Quote) as pair
 		FROM
 		  api-project-901373404215.digital_assets.nomics_markets
 		GROUP BY 
 		  Base, Exchange, Quote
 	  ),
 	  oneDayPrice AS (
 			  SELECT
 				  CAST(Close AS FLOAT64) price_by_pair_1d,
 				  CAST(volume AS FLOAT64) volume_by_pair_1d,
 				  base AS symbol,
 				  quote,
 			exchange,
 			type
 			  FROM (
 				  SELECT
 					  AVG(price) AS Close,
 					  base,
 					  quote,
 			  exchange,
 			  type,
 			  AVG(OneD.Volume) AS volume
 				  FROM
 					  api-project-901373404215.digital_assets.nomics_exchange_market_ticker
 				  WHERE
 					  Timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
 					  AND quote IN ('USD',
 						  'USDT',
 						  'USDC')
 					  AND type = "spot"
 					  AND Status = "active"
 			  AND OneD.Volume IS NOT NULL
 				  GROUP BY
 					  base,
 					  quote,
 			  exchange,
 			  type ) AS oneDay ),
 	  sevenDayPrice AS (
 			  SELECT
 				  CAST(Close AS FLOAT64) price_by_pair_7d,
 			CAST(volume AS FLOAT64) volume_by_pair_7d,
 				  base AS symbol,
 				  quote,
 			exchange
 			  FROM (
 				  SELECT
 					  AVG(price) AS Close,
 					  base,
 					  quote,
 			  exchange,
 			  AVG(OneD.Volume) AS volume
 				  FROM
 					  api-project-901373404215.digital_assets.nomics_exchange_market_ticker
 				  WHERE
 					  Timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
 					  AND quote IN ('USD',
 						  'USDT',
 						  'USDC')
 					  AND type = "spot"
 					  AND Status = "active"
 			  AND OneD.Volume IS NOT NULL
 				  GROUP BY
 					  base,
 			  exchange,
 					  quote ) AS sevenDay ),
 	  thirtyDayPrice AS (
 			  SELECT
 				  CAST(Close AS FLOAT64) price_by_pair_30d,
 			CAST(volume AS FLOAT64) volume_by_pair_30d,
 				  base AS symbol,
 			exchange,
 				  quote
 			  FROM (
 				  SELECT
 					  AVG(price) AS Close,
 					  base,
 			  exchange,
 					  quote,
 			  AVG(OneD.Volume) AS volume
 				  FROM
 					  api-project-901373404215.digital_assets.nomics_exchange_market_ticker
 				  WHERE
 					  Timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
 					  AND quote IN ('USD',
 						  'USDT',
 						  'USDC')
 					  AND type = "spot"
 					  AND Status = "active"
 			  AND OneD.Volume IS NOT NULL
 				  GROUP BY
 					  base,
 			  exchange,
 					  quote ) AS thirtyDay ),
 	  oneYearPrice AS (
 			  SELECT
 				  CAST(Close AS FLOAT64) price_by_pair_1y,
 			CAST(volume AS FLOAT64) volume_by_pair_1y,
 				  base AS symbol,
 			exchange,
 				  quote
 			  FROM (
 				  SELECT
 					  AVG(price) AS Close,
 					  base,
 			  exchange,
 					  quote,
 			  AVG(OneD.Volume) AS volume
 				  FROM
 					  api-project-901373404215.digital_assets.nomics_exchange_market_ticker
 				  WHERE
 					  Timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
 					  AND quote IN ('USD',
 						  'USDT',
 						  'USDC')
 					  AND type = "spot"
 					  AND Status = "active"
 			  AND OneD.Volume IS NOT NULL
 				  GROUP BY
 					  base,
 			  exchange,
 					  quote ) AS oneYear )

 	  SELECT 
 		market.Base, 
 		market.Exchange, 
 		market.Quote, 
 		oneDayPrice.type,
 		market.pair,
 		oneDayPrice.volume_by_pair_1d,
 		oneDayPrice.price_by_pair_1d,
 		sevenDayPrice.volume_by_pair_7d,  
 		sevenDayPrice.price_by_pair_7d,  
 		thirtyDayPrice.volume_by_pair_30d,
 		thirtyDayPrice.price_by_pair_30d,
 		oneYearPrice.volume_by_pair_1y,
 		oneYearPrice.price_by_pair_1y

 	  FROM
 		market
 	  INNER JOIN
 		oneDayPrice
 	  ON
 		oneDayPrice.symbol = market.Base
 		AND oneDayPrice.quote = market.Quote
 		AND oneDayPrice.exchange = market.Exchange
 	  INNER JOIN
 		sevenDayPrice
 	  ON
 		sevenDayPrice.symbol = market.Base
 		AND sevenDayPrice.quote = market.Quote
 		AND sevenDayPrice.exchange = market.Exchange
 	  INNER JOIN
 		thirtyDayPrice
 	  ON
 		thirtyDayPrice.symbol = market.Base
 		AND thirtyDayPrice.quote = market.Quote
 		AND thirtyDayPrice.exchange = market.Exchange
 	  INNER JOIN
 		oneYearPrice
 	  ON
 		oneYearPrice.symbol = market.Base
 		AND oneYearPrice.quote = market.Quote
 		AND oneYearPrice.exchange = market.Exchange
 	WHERE 
 		market.Base IS NOT NULL 
 		AND market.Exchange IS NOT NULL  
 		AND market.Quote IS NOT NULL 
 		AND market.pair IS NOT NULL 
 		AND oneDayPrice.type IS NOT NULL
 		AND oneDayPrice.price_by_pair_1d IS NOT NULL
 		AND oneDayPrice.volume_by_pair_1d IS NOT NULL
 		AND sevenDayPrice.price_by_pair_7d IS NOT NULL
 		AND sevenDayPrice.volume_by_pair_7d IS NOT NULL
 		AND thirtyDayPrice.price_by_pair_30d IS NOT NULL
 		AND thirtyDayPrice.volume_by_pair_30d IS NOT NULL
 		AND oneYearPrice.price_by_pair_1y IS NOT NULL
 		AND oneYearPrice.volume_by_pair_1y IS NOT NULL