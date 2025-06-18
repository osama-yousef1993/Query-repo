with YTD as (
			select 
				base, volume, volume_base, volume_base_change, volume_change, trades, trades_change, price_change, price_quote_change, last_updated
			from 
				nomics_exchange_market_ticker_ytd
			where 
				last_updated >= cast(now() - INTERVAL '365 DAYS' as timestamp)
			AND base = any(select id from public.activeassets())

        ),
		YTDPrice AS 
			(
			SELECT 
				base, exchange, market, quote, type, sub_type, aggregated, price_exclude, volume_exclude,
				base_symbol, quote_symbol, price, price_quote, volume_usd, status, weight, first_trade, 
				first_candle, first_order_book, "timestamp", total_trades
			FROM 
				nomics_exchange_market_ticker
			WHERE 
				timestamp >= cast(now() - INTERVAL '365 DAYS' as timestamp)
				AND quote IN ('USD', 'USDT', 'USDC')
				AND base = any(select id from public.activeassets())
				AND price is not null
			)
select 
	distinct YTDPrice.base, YTDPrice.exchange, YTDPrice.market, YTDPrice.quote, YTDPrice.type, YTDPrice.sub_type, YTDPrice.aggregated, YTDPrice.price_exclude, YTDPrice.volume_exclude,
	YTDPrice.base_symbol, YTDPrice.quote_symbol, YTDPrice.price, YTDPrice.price_quote, YTDPrice.volume_usd, YTDPrice.status, YTDPrice.weight, YTDPrice.first_trade, 
	YTDPrice.first_candle, YTDPrice.first_order_book, YTDPrice.timestamp, YTDPrice.total_trades,
	YTD.volume, YTD.volume_base, YTD.volume_base_change, YTD.volume_change, YTD.trades, YTD.trades_change, YTD.price_change, YTD.price_quote_change, YTD.last_updated
from 
	YTDPrice
	LEFT JOIN
	YTD
	using(base)
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
with
		assets as 
			(
				select
					id,
					status, 
					last_updated
				from 
					nomics_assets
				where id = any(select id from public.activeassets())
				group by 
					id

			),
		market as 
			(
				select
					base, 
					exchange, 
					quote , 
					CONCAT(base, quote) as pair
				from 
					nomics_markets
				where base = any(select id from public.activeassets())

				group by
					base,
					exchange, 
					quote
			),

			ticker as 
			(
				SELECT 
					base,   
					price,
					volume,
					type
				from
					(
						SELECT 
							ticker.base, CAST(ticker.price as FLOAT) as price, ticker.type,
							CAST(oneDay.volume AS FLOAT) as volume
						from (
							select 
								base,
								price,
								type,
								row_number() OVER ()
							from 
								nomics_exchange_market_ticker
							where 
								timestamp >= cast(now() - INTERVAL '365 DAYS' as timestamp)
								AND base = any(select id from public.activeassets())
							group by base,
								price,
								type

						) as ticker
							LEFT JOIN(
							select
								base,
								volume,
								row_number() OVER ()
							from 
								nomics_exchange_market_ticker_one_year
							where 
								last_updated >= cast(now() - INTERVAL '365 DAYS' as timestamp)
								AND base = any(select id from public.activeassets())
								group by base,
								volume
						) as oneDay
						USING (row_number)
					) as oneYear
				)

			SELECT
				assets.id as base,
				market.exchange as exchange, 
				market.quote as quote, 
				market.pair as pair, 												 
				assets.status as status, 
				assets.last_updated as last_updated,
				ticker.type as type,
				ticker.price as price,
				ticker.volume as volume
			from
				assets
				LEFT JOIN 
					market
				ON
					market.base = assets.id
				LEFT JOIN 
					ticker
				ON
					ticker.base = assets.id
			where
				market.exchange is not null
				and market.quote  is not null
				and market.pair  is not null
			group by 
				assets.id,
				market.exchange, 
				market.quote , 
				market.pair, 												 
				assets.status, 
				assets.last_updated,
				ticker.type,
				ticker.price,
				ticker.volume






++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
WITH oneYear AS 
 (
    SELECT
       avg(close) as close,
       base AS symbol,
       timestamp 
    FROM
       (
          SELECT
             close AS Close,
             base,
             timestamp 
          FROM
 		 api-project-901373404215.digital_assets.` + candlesTable + ` c  
          WHERE
             timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 365 DAY) 
       )
    GROUP BY
       base,
       timestamp
 )
 ,
 quantiles as 
 (
    SELECT
       approx_quantiles(close, 4) as quantiles,
       approx_quantiles(close, 4) [offset(3) ] + ( 1.5 * ( approx_quantiles(close, 4) [offset(3) ] - approx_quantiles(close, 4) [offset(1) ] ) ) as upperfence,
       approx_quantiles(close, 4) [offset(1) ] - ( 1.5 * ( approx_quantiles(close, 4) [offset(3) ] - approx_quantiles(close, 4) [offset(1) ] ) ) as lowerfence,
       upper(Base) as symbol 
    FROM
       (
          SELECT
             AVG(close) close,
             Base,
             timestamp 
          FROM
 		 api-project-901373404215.digital_assets.` + candlesTable + ` c   
          WHERE
             timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), Interval 365 DAY ) 
          GROUP BY
             base,
             timestamp 
       )
    GROUP BY
       base 
 ),
 ExchangesPrices AS (
 		SELECT 
 		  Base as Base, 
 		  Exchange, 
 		  avg(Price) as Price, 
 		FROM 
 		api-project-901373404215.digital_assets.` + marketTicketTable + ` c 
 		WHERE 
 		  Exchange NOT IN ("bitmex", "hbtc") 
 		  AND Timestamp > DATETIME_SUB(
 			CURRENT_TIMESTAMP(), 
 			INTERVAL 30 MINUTE
 		  ) 
 		  AND Type = "spot" 
 		  AND status = "active" 
 		  AND Quote IN ("USD", "USDT", "USDC") 
 		GROUP BY 
 		  Base, 
 		  Exchange
 	  )
SELECT
 cast(low_24h as float64 ) as low_24h,
 cast(high_24h as float64) as high_24h,
 cast(low_7d as float64) as low_7d,
 cast(high_7d  as float64) as high_7d,
 cast(low_30d as float64) as low_30d,
 cast(high_30d as float64) as high_30d,
 cast(low_1y as float64) as low_1y,
 cast(high_1y as float64) as high_1y,
 ARRAY_AGG(STRUCT(Exchange AS market, Base AS symbol, IFNULL(PRICE, cast(0 as float64)) AS close)) as Exchanges,
 a.symbol as symbol
   from 
   (
   		select 
			CAST(MAX(CASE WHEN timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 365 DAY)  THEN oneYear.Close END) AS FLOAT64) AS high_1y, 
			CAST(MIN( CASE WHEN timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 365 DAY)  THEN oneYear.Close END ) AS FLOAT64) AS low_1y, 
			CAST(MAX( CASE WHEN timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 30 DAY) THEN oneYear.Close END )AS FLOAT64) AS high_30d, 
			CAST(MIN(CASE WHEN timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 30 DAY) THEN oneYear.Close END) AS FLOAT64) AS low_30d, 
			CAST(MAX( CASE WHEN timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 7 DAY)  THEN oneYear.Close END)as FLOAT64) AS high_7d, 
			CAST(MIN( CASE WHEN timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 7 DAY)  THEN oneYear.Close  END) AS FLOAT64) AS low_7d,
			CAST(MAX( CASE WHEN timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 24 hour) THEN oneYear.Close  END)AS FLOAT64) AS high_24h, 
			CAST(MIN( CASE WHEN  timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 24 hour)  THEN oneYear.Close END) AS FLOAT64) AS low_24h, 
			oneYear.symbol 
			
		FROM
			quantiles 
			Inner join
			oneYear 
			on 		/*Only include closes that are within the margin of error*/
			oneYear.symbol = quantiles.symbol 
			and 
			(
				oneYear.Close >= CAST(quantiles.lowerfence AS float64)
			)
			and 
			(
				oneYear.Close <= CAST(quantiles.upperfence AS float64)
			)
		GROUP BY
			oneYear.symbol
	) a 
INNER JOIN ExchangesPrices on (
		a.symbol = ExchangesPrices.Base
	)  
group by a.symbol, a.low_24h, high_24h,low_7d,high_7d,low_30d,high_30d,low_1y,high_1y`)





"(1INCH,abcc,1inchusdt,USDT,spot,true,false,false,1INCH,1INCH,1374491056576,1374491056584,1374491056592,active,0.0041,\"0001-01-01 00:00:00 +0000 UTC\",\"2021-09-07 00:00:00 +0000 UTC\",\"0001-01-01 00:00:00 +0000 UTC\",\"2022-11-07 12:07:50.670962 +0300 +03 m=+33.437860417\",0)"