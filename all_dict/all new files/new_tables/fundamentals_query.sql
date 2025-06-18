with
	
	high_low as (
		SELECT lower(symbol), high_24h, low_24h, high_7d, low_7d, high_30d, low_30d, high_1y, low_1y, high_ytd, low_ytd, last_updated
		FROM price_high_low
	),
	active_assets as (
		SELECT id, last_updated, price, price_date, price_timestamp, circulating_supply, max_supply, marketcap, 
		transparent_marketcap, marketcap_dominance, num_exchanges, num_pairs, num_pairs_unmapped, first_candle, first_trade,
		first_order_book, first_priced_at, rank, rank_delta, high, high_timestamp, platform_currency 
		from  activeassets()
	),
	exchangesData as (
		select lower(base) as symbol, exchanges
		from exchanges
	),
	marketPairsData as (
		select lower(base) as symbol, market_pairs
		from market_pairs
	)
select
	lower(price_high_low.symbol),
	active_assets.last_updated,
	active_assets.price,
	active_assets.price_date,
	active_assets.price_timestamp,
	active_assets.circulating_supply,
	active_assets.max_supply,
	active_assets.marketcap, 
	active_assets.transparent_marketcap,
	active_assets.marketcap_dominance,
	active_assets.num_exchanges,
	active_assets.num_pairs,
	active_assets.num_pairs_unmapped,
	active_assets.first_candle,
	active_assets.first_trade,
	active_assets.first_order_book,
	active_assets.first_priced_at,
	active_assets.rank,
	active_assets.rank_delta,
	active_assets.high,
	active_assets.high_timestamp,
	active_assets.platform_currency,
	price_high_low.high_24h,
	price_high_low.low_24h,
	price_high_low.high_7d,
	price_high_low.low_7d,
	price_high_low.high_30d,
	price_high_low.low_30d,
	price_high_low.high_1y,
	price_high_low.low_1y,
	price_high_low.high_ytd,
	price_high_low.low_ytd, 
	price_high_low.last_updated,
	exchangesData.exchanges,
	marketPairsData.market_pairs
from 
	price_high_low
	LEFT JOIN
		active_assets
	ON
		active_assets.id = price_high_low.symbol
	LEFT JOIN
		exchangesData
	USING(symbol)
	LEFT JOIN
		marketPairsData
	USING(symbol)