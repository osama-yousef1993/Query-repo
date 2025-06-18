CREATE
OR REPLACE FUNCTION getcryptocontentbysource(slg text) RETURNS Table (
    symbol Text,
    display_symbol Text,
    slug Text,
    status Text,
    market_cap float,
    price_24h float,
    number_of_active_market_pairs int,
    description Text,
    name Text,
    website_url Text,
    blog_url Text,
    discord_url Text,
    facebook_url Text,
    github_url Text,
    medium_url Text,
    reddit_url Text,
    telegram_url Text,
    twitter_url Text,
    whitepaper_url Text,
    youtube_url Text,
    bitcointalk_url Text,
    blockexplorer_url Text,
    logo_url Text,
    forbesMetaDataDescription Text
) AS $ $
SELECT
    symbol,
    display_symbol,
    slug,
    status,
    market_cap,
    price_24h,
    number_of_active_market_pairs,
    COALESCE(description, '') as description,
    COALESCE(name, '') as name,
    COALESCE(website_url, '') as website_url,
    COALESCE(blog_url, '') as blog_url,
    COALESCE(discord_url, '') as discord_url,
    COALESCE(facebook_url, '') as facebook_url,
    COALESCE(github_url, '') as github_url,
    COALESCE(medium_url, '') as medium_url,
    COALESCE(reddit_url, '') as reddit_url,
    COALESCE(telegram_url, '') as telegram_url,
    COALESCE(twitter_url, '') as twitter_url,
    COALESCE(whitepaper_url, '') as whitepaper_url,
    COALESCE(youtube_url, '') as youtube_url,
    COALESCE(bitcointalk_url, '') as bitcointalk_url,
    COALESCE(blockexplorer_url, '') as blockexplorer_url,
    COALESCE(logo_url, '') as logo_url,
    COALESCE(forbesMetaDataDescription, '') as forbesMetaDataDescription
FROM
    (
        SELECT
            symbol,
            display_symbol,
            slug,
            status,
            market_cap,
            price_24h,
            number_of_active_market_pairs,
        FROM
            fundamentalslatest
        WHERE
            slug = slg
    ) A
    LEFT JOIN (
        SELECT
            id,
            coinpaprika_id,
            coingecko_id
        FROM
            forbes_assets
        WHERE
            coinpaprika_id IS NOT NULL
    ) B ON A.slug = B.id
    LEFT JOIN (
        SELECT
            id,
            description,
            name,
            (LINKS :: JSON ->> 'website') :: text AS website_url,
            (LINKS :: JSON ->> 'blog') :: text AS blog_url,
            (LINKS :: JSON ->> 'discord') :: text AS discord_url,
            (LINKS :: JSON ->> 'facebook') :: text AS facebook_url,
            (LINKS :: JSON ->> 'source_code') :: text AS github_url,
            (LINKS :: JSON ->> 'medium') :: text AS medium_url,
            (LINKS :: JSON ->> 'reddit') :: text AS reddit_url,
            (LINKS :: JSON ->> 'telegram') :: text AS telegram_url,
            (LINKS :: JSON ->> 'twitter') :: text AS twitter_url,
            (WHITEPAPER :: JSON ->> 'link') :: text AS whitepaper_url,
            (LINKS :: JSON ->> 'youtube') :: text AS youtube_url,
            (LINKS :: JSON ->> 'message_board') :: text AS bitcointalk_url,
            (LINKS :: JSON ->> 'explorer') :: JSON ->> 0 AS blockexplorer_url,
            '' AS logo_url,
            '' AS forbesMetaDataDescription
        FROM
            coinpaprika_asset_metadata
    ) META ON B.coinpaprika_id = META.id;

$ $ LANGUAGE SQL;