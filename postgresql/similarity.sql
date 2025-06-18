CREATE EXTENSION IF NOT EXISTS pg_trgm;

select symbol, name, slug, similarity(name, 'scallop') as sim
from fundamentalslatest
WHERE similarity(name, 'scallop') > 0.8;

select symbol, name, slug, similarity(name, 'anon') as sim
from fundamentalslatest
WHERE similarity(name, 'anon') > 0.8;




SELECT SYMBOL,
	DISPLAY_SYMBOL,
	SLUG,
	STATUS,
	MARKET_CAP,
	PRICE_24H,
	NUMBER_OF_ACTIVE_MARKET_PAIRS,
	DESCRIPTION,
	NAME,
	WEBSITE_URL,
	BLOG_URL,
	DISCORD_URL,
	FACEBOOK_URL,
	GITHUB_URL,
	MEDIUM_URL,
	REDDIT_URL,
	TELEGRAM_URL,
	TWITTER_URL,
	WHITEPAPER_URL,
	YOUTUBE_URL,
	BITCOINTALK_URL,
	BLOCKEXPLORER_URL,
	LOGO_URL,
	b.forbesMetaDataDescription
FROM
	(SELECT SYMBOL,
			DISPLAY_SYMBOL,
			SLUG,
			STATUS,
			MARKET_CAP,
			PRICE_24H,
			NUMBER_OF_ACTIVE_MARKET_PAIRS
		FROM FUNDAMENTALSLATEST
		WHERE  similarity(name, 'scallop') > 0.8) A
LEFT JOIN
	(SELECT ID,
			DESCRIPTION,
			NAME,
			WEBSITE_URL,
			BLOG_URL,
			DISCORD_URL,
			FACEBOOK_URL,
			GITHUB_URL,
			MEDIUM_URL,
			REDDIT_URL,
			TELEGRAM_URL,
			TWITTER_URL,
			WHITEPAPER_URL,
			YOUTUBE_URL,
			BITCOINTALK_URL,
			BLOCKEXPLORER_URL,
			LOGO_URL,
			"forbesMetaDataDescription" as forbesMetaDataDescription
		FROM PUBLIC.COINGECKO_ASSET_METADATA) B ON A.SYMBOL = B.ID 