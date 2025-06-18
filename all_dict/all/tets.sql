with 
		allTime as 
			(
			SELECT lower(base) as symbol 
			FROM 
				nomics_ohlcv_candles
			where base = 'BTC'
			GROUP BY 
				base
			),
		ExchangesPrices AS 
			( 
				SELECT 
					lower(base) as Symbol, 
					exchange as Market
				FROM 
					nomics_exchange_market_ticker
				WHERE 
					exchange NOT IN ('bitmex','hbtc')
					AND base = 'BTC'
					AND type = 'spot'
					AND timestamp >=  cast(now() - INTERVAL '30 MINUTE' as timestamp)
					AND status = 'active'
					AND quote IN ('USD', 'USDT', 'USDC')
				group by 
					base,
					exchange
			),
		exchangeMetadata as (
			select 
				id, 
				name,
				logo_url
			from 
				nomics_exchange_metadata
			where 
				id = 'aax'
		    ),
		exchangeHighLight as (
			select 
				num_markets,
				exchange
			from 
				nomics_exchange_highlight
			where 
				exchange = 'aax'
			order by 
				num_markets desc
			limit 1
		    ),
	    oneDay as (
            SELECT 
                exchange,
                min(volume) as volume
            FROM 
                (
                    select 
                        exchange, 
                        CASE WHEN avg(volume) is null THEN CAST(0 AS FLOAT)
                        ELSE avg(volume)
                        END
                        as volume
                    from 
                        nomics_exchange_market_ticker_one_day
                    where 
                        last_updated >= cast(now() - INTERVAL '24 HOUR' as timestamp)
                        and exchange = 'aax'
                    group by 
                        exchange
                ) as oneDay
            group by 
                exchange
        ),
        sevenDays as (
            SELECT 
                exchange, 
                min(volume) as volume
            FROM 
                (
                    select 
                        exchange, 
                        CASE WHEN avg(volume) is null THEN CAST(0 AS FLOAT)
                        ELSE avg(volume)
                        END
                        as volume
                    from 
                        nomics_exchange_market_ticker_seven_days
                    where 
                        last_updated >= cast(now() - INTERVAL '7 DAYS' as timestamp)
                        and exchange = 'aax'
                    group by 
                        exchange
                ) as sevenDays
            group by 
                exchange
        ),
        thirtyDays as (
            SELECT 
                exchange, 
                min(volume) as volume
            FROM 
                (
                    select 
                        exchange, 
                        CASE WHEN avg(volume) is null THEN CAST(0 AS FLOAT)
                        ELSE avg(volume)
                        END
                        as volume
                    from 
                        nomics_exchange_market_ticker_thirty_days
                    where 
                        last_updated >= cast(now() - INTERVAL '30 DAYS' as timestamp)
                        and exchange = 'aax'
                    group by 
                        exchange
                ) as thirtyDays
            group by 
                exchange
        ),
        oneYear as (
            SELECT 
                exchange, 
                min(volume) as volume
            FROM 
                (
                    select 
                        exchange, 
                        CASE WHEN avg(volume) is null THEN CAST(0 AS FLOAT)
                        ELSE avg(volume)
                        END
                        as volume
                    from 
                        nomics_exchange_market_ticker_one_year
                    where 
                        last_updated >= cast(now() - INTERVAL '365 DAYS' as timestamp)
                        and exchange = 'aax'
                    group by exchange
                ) as oneYear
            group by 
                exchange
        ),
        YTD as (
            SELECT 
                exchange, 
                min(volume) as volume
            FROM 
                (
                    select 
                        exchange, 
                        CASE WHEN avg(volume) is null THEN CAST(0 AS FLOAT)
                        ELSE avg(volume)
                        END
                        as volume
                    from 
                        nomics_exchange_market_ticker_ytd
                    where 
                        last_updated >= cast(date_trunc('year', current_date) as timestamp)
                        and exchange = 'aax'
                    group by exchange
                ) as YTD
            group by 
                exchange
        )
		select 
			array_to_json(ARRAY_AGG(json_build_object('Market', ExchangesPrices.Market, 
													    'Symbol', ExchangesPrices.Symbol,
													    'ID',exchangeMetadata.id, 
                                                        'Name', exchangeMetadata.name, 
                                                        'Logo',exchangeMetadata.logo_url, 
                                                        'NumMarket', cast(exchangeHighLight.num_markets as int),
                                                        'volume_exchange_1d', cast(oneDay.volume as float) as volume_exchange_1d,
                                                        'volume_exchange_7d',cast(sevenDays.volume as float) as volume_exchange_7d,
                                                        'volume_exchange_30d',cast(thirtyDays.volume as float) as volume_exchange_30d,
                                                        'volume_exchange_1y',cast(oneYear.volume as float) as volume_exchange_1y,
                                                        'volume_exchange_ytd',cast(YTD.volume as float) as volume_exchange_ytd
													 ))) as Exchanges,
			allTime.symbol
		from 
			allTime 
			INNER JOIN 
				ExchangesPrices 
			ON 
				ExchangesPrices.Symbol = allTime.symbol
			INNER JOIN 
				exchangeMetadata
			ON
				exchangeMetadata.id = ExchangesPrices.Market
			INNER Join 
				exchangeHighLight
			ON 
				exchangeHighLight.exchange = exchangeMetadata.id
			INNER Join 
				oneDay
			ON 
				oneDay.exchange = exchangeMetadata.id
			INNER Join 
				sevenDays
			ON 
				sevenDays.exchange = exchangeMetadata.id
			INNER Join 
				thirtyDays
			ON 
				thirtyDays.exchange = exchangeMetadata.id
			INNER Join 
				oneYear
			ON 
				oneYear.exchange = exchangeMetadata.id
			INNER Join 
				YTD
			ON 
				YTD.exchange = exchangeMetadata.id
		group by 
			allTime.symbol