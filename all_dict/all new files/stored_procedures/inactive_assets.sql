create or replace function public.activeAssets()
	returns Table (
		id TEXT, 
		last_updated TIMESTAMPTZ, 
		price FLOAT, 
		status TEXT, 
		price_date TIMESTAMPTZ, 
		price_timestamp TIMESTAMPTZ, 
		circulating_supply FLOAT, 
		max_supply FLOAT, 
		marketcap FLOAT, 
		transparent_marketcap FLOAT, 
		marketcap_dominance FLOAT, 
		num_exchanges NUMERIC, 
		num_pairs NUMERIC, 
		num_pairs_unmapped NUMERIC, 
		first_candle TIMESTAMPTZ, 
		first_trade TIMESTAMPTZ,  
		first_order_book TIMESTAMPTZ, 
		first_priced_at TIMESTAMPTZ, 
		rank NUMERIC, 
		rank_delta NUMERIC, 
		high FLOAT, 
		high_timestamp TIMESTAMPTZ, 
		platform_currency TEXT
	)
as 
$func$
with tickers as(

SELECT id, row_number() OVER(PARTITION BY id ORDER BY last_updated desc) AS row_num, last_updated, price, status, price_date, price_timestamp, circulating_supply, max_supply, marketcap, transparent_marketcap, marketcap_dominance, num_exchanges, num_pairs, num_pairs_unmapped, first_candle, first_trade, first_order_book, first_priced_at, rank, rank_delta, high, high_timestamp, platform_currency
	FROM public.nomics_currencies_tickers

	)
select 
	id, last_updated, price, status, 
	price_date, price_timestamp, circulating_supply, 
	max_supply, marketcap, transparent_marketcap, 
	marketcap_dominance, num_exchanges, num_pairs, 
	num_pairs_unmapped, first_candle, first_trade, 
	first_order_book, first_priced_at, rank, 
	rank_delta, high, high_timestamp, platform_currency
from tickers
where row_num = 1
	and status = 'active'
	and marketcap is not null
order by marketcap desc
	limit 50

$func$
Language sql

++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

SELECT 
	id, 
	last_updated, 
	price, 
	status, 
	price_date, 
	price_timestamp,
	circulating_supply, 
	max_supply, marketcap,
	transparent_marketcap, 
	marketcap_dominance,
	num_exchanges,
	num_pairs, 
	num_pairs_unmapped,
	first_candle,
	first_trade, 
	first_order_book,
	first_priced_at,
	rank, 
	rank_delta,
	high,
	high_timestamp,
	platform_currency 
from  
	public.inActiveAssets()



('BTC',  
'ETH',
'USDT',
'USDC',
'BNB',
'BUSD',
'XRP',
'ADA',
'SOL',
'DOGE',
'DOT',
'MATIC',
'DAI',
'SHIB',
'STETH',
'TRX',
'AVAX',
'WBTC',
'ATOM',
'FTXTOKEN',
'HEX',
'LEO',
'ETC',
'OKB',
'LTC',
'LINK',
'NEAR',
'XLM',
'CRO',
'UNI',
'XMR',
'ALGO',
'BCH',
'LUNA',
'APE7',
'FLOW2',
'VET',
'ICP',
'OP4',
'FIL',
'CHZ',
'QNT',
'CRI',
'EOS',
'FRAX',
'HBAR',
'XTZ',
'MANA',
'SAND2',
'LDO')

"ada"
"algo"
"ape7"
"atom"
"avax"
"bch"
"bnb"
"btc"
"busd"

"chz"

"cri"
"cro"
"dai"
"doge"
"dot"
"eos"
"etc"
"eth"
"fil"
"flow2"
"frax"
"ftxtoken"
"hbar"
"hex"
"icp"
"ldo"
"leo"
"link"
"ltc"
"luna"
"mana"
"matic"
"near"
"okb"
"qnt"
"sand2"
"shib"
"sol"
"steth"
"trx"
"uni"
"usdc"
"usdt"
"vet"
"wbtc"
"xlm"
"xmr"
"xrp"
"xtz"