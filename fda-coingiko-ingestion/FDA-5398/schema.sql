create table "coingecko_assets"(
	"id" text,
	"symbol" text,
    "name" text,
	"platforms" text,
    "last_updated" TIMESTAMPTZ DEFAULT (Now()),
	primary key ("id")
);

create table "coingecko_asset_metadata"(
	"id" text,
	"original_symbol" text,
	"description" text,
	"name" text,
	"website_url" text,
	"logo_url" text,
	"blog_url" text,
	"slack_url" text,
	"discord_url" text,
	"facebook_url" text,
	"github_url" text,
	"bitbucket_url" text,
	"medium_url" text,
	"reddit_url" text,
	"telegram_url" text,
	"twitter_url" text,
	"youtube_url" text,
	"whitepaper_url" text,
	"blockexplorer_url" text,
	"bitcointalk_url" text,
	"platform_currency_id" text,
	"platform_contract_address" text,
	"ico_start_date" TIMESTAMPTZ,
	"ico_end_date" TIMESTAMPTZ,
	"ico_total_raised" text,
	"ico_total_raised_currency" text,
	"alexa_rank" integer,
	"facebook_likes" integer,
	"twitter_followers" integer,
	"reddit_average_posts_48h" float,
	"reddit_average_comments_48h" float,
	"reddit_subscribers" integer,
	"reddit_accounts_active_48h" integer,
	"telegram_channel_user_count" integer,
	"repo_forks" integer,
	"repo_stars" integer,
	"repo_subscribers" integer,
	"repo_total_issues" integer,
	"repo_closed_issues" integer,
	"repo_pull_requests_merged" integer,
	"repo_pull_request_contributors" integer,
	"repo_code_additions_4_weeks" integer,
	"repo_code_deletions_4_weeks" integer,
	"repo_commit_count_4_weeks" integer,
	"genesis_date" DATE,
	"last_updated" TIMESTAMPTZ DEFAULT (Now()),
	primary key ("id")
);


create table "coingecko_exchanges" (
	"id" text,
	"name" text,
	primary key("id")
)

CREATE Table "coingecko_exchanges_tickers" (
	"name" text,
	"tickers" json,
	primary key("name")
)


create table "coingecko_exchange_metadata"(
	"id" text,
	"name" TEXT,
	"year" INTEGER,
	"description" TEXT,
	"location" TEXT,
	"logo_url" TEXT,
	"website_url" TEXT,
	"twitter_url" TEXT,
	"facebook_url" TEXT,
	"youtube_url" TEXT,
	"linkedin_url" TEXT,
	"reddit_url" TEXT,
	"chat_url" TEXT,
	"slack_url" TEXT,
	"telegram_url" TEXT,
	"blog_url" TEXT,
	"centralized" BOOLEAN,
	"decentralized" BOOLEAN,
	"has_trading_incentive" BOOLEAN,
	"trust_score" INTEGER,
	"trust_score_rank" INTEGER,
	"trade_volume_24h_btc" FLOAT,
	"trade_volume_24h_btc_normalized" FLOAT,
	"last_updated" TIMESTAMPTZ DEFAULT ( Now()),
	primary key ("id")
);

CREATE INDEX ON "coingecko_exchange_metadata" ("id");

CREATE FUNCTION getxexchangeidsbytrust()
RETURNS Table (
	id Text
) AS $$
BEGIN
  RETURN QUERY EXECUTE format('SELECT 
	id
	from coingecko_exchange_metadata 
	where trust_score is not null 
	order by trust_score desc
	limit 5'
							 );
END
$$ LANGUAGE plpgsql;


CREATE TABLE coingecko_categories (
	"id" TEXT,
	"name" TEXT,
	"market_cap" FLOAT,
	"market_cap_change_24h" FLOAT,
	"content" TEXT,
	"top_3_coins" VARCHAR(500)[],
	"volume_24h" FLOAT,
	"markets" JSON,
	"last_updated" TIMESTAMPTZ DEFAULT (Now()),
	primary key ("id")
);
create table NFTDataLatest (
  	"id" TEXT, 
  	"contract_address" TEXT, 
  	"asset_platform_id" TEXT, 
  	"name" TEXT, 
  	"symbol" TEXT, 
  	"image" TEXT, 
  	"description" TEXT, 
  	"native_currency" TEXT, 
  	"floor_price_usd" FLOAT, 
  	"market_cap_usd" FLOAT, 
  	"volume_24h_usd" FLOAT, 
  	"floor_price_native" FLOAT, 
  	"market_cap_native" FLOAT, 
  	"volume_24h_native" FLOAT, 
  	"floor_price_in_usd_24h_percentage_change" FLOAT, 
  	"number_of_unique_addresses" INTEGER, 
  	"number_of_unique_addresses_24h_percentage_change" FLOAT, 
  	"total_supply" FLOAT, 
  	"slug" TEXT, 
  	"website_url" TEXT, 
  	"twitter_url" TEXT, 
  	"discord_url" TEXT, 
  	"last_updated" TIMESTAMPTZ DEFAULT (Now()), 
  	"avg_floor_price_1d" double precision, 
  	"avg_floor_price_7d" double precision, 
  	"avg_floor_price_30d" double precision, 
  	"avg_floor_price_90d" double precision, 
  	"avg_sale_price_1d" double precision, 
  	"avg_sale_price_7d" double precision, 
  	"avg_sale_price_30d" double precision, 
  	"avg_sale_price_90d" double precision, 
  	"avg_total_sales_pct_change_1d" double precision, 
  	"avg_total_sales_pct_change_7d" double precision, 
  	"avg_total_sales_pct_change_30d" double precision, 
  	"avg_total_sales_pct_change_90d" double precision, 
  	"total_sales_1d" double precision, 
  	"total_sales_7d" double precision, 
  	"total_sales_30d" double precision, 
  	"total_sales_90d" double precision, 
  	"avg_sales_price_change_1d" double precision, 
  	"avg_sales_price_change_7d" double precision, 
  	"avg_sales_price_change_30d" double precision, 
  	"avg_sales_price_change_90d" double precision, 
  	primary key("id")
);
-- Add indexes to those fields because we use them to filter the data from the table. 
CREATE INDEX ON "nftdatalatest" ("name");
CREATE INDEX ON "nftdatalatest" ("market_cap_usd");

-- we need to modify the NFTDataLatest and add those field
ALTER TABLE "nftdatalatest" ADD COLUMN "native_currency_symbol" TEXT;
ALTER TABLE "nftdatalatest" ADD COLUMN "market_cap_24h_percentage_change_usd" FLOAT;
ALTER TABLE "nftdatalatest" ADD COLUMN "market_cap_24h_percentage_change_native" FLOAT;
ALTER TABLE "nftdatalatest" ADD COLUMN "volume_24h_percentage_change_usd" FLOAT;
ALTER TABLE "nftdatalatest" ADD COLUMN "volume_24h_percentage_change_native" FLOAT;

Alter table "nftdatalatest" Add COLUMN "avg_floor_price_ytd" FLOAT;
Alter table "nftdatalatest" Add COLUMN "avg_sale_price_ytd" FLOAT;
Alter table "nftdatalatest" Add COLUMN "avg_total_sales_pct_change_ytd" FLOAT;
Alter table "nftdatalatest" Add COLUMN "total_sales_ytd" FLOAT;
Alter table "nftdatalatest" Add COLUMN "avg_sales_price_change_ytd" FLOAT;
Alter table "nftdatalatest" Add COLUMN "explorers" json;


-- we need to modify the global_description and add this field
-- So we can use it for Assets and NFTs
ALTER TABLE global_description ADD COLUMN "type" TEXT;


-- create NFTPagination So we can use it to build the Trending for NFTs Dynamic Description
CREATE OR REPLACE FUNCTION NFTPagination(lim int,pageNum int, sortBy Text ,  direction Text)
RETURNS Table (
	name Text,
	slug Text,
	volume_24h_percentage_change_usd FLOAT
) AS $$

BEGIN
  RETURN QUERY EXECUTE format(' 
	SELECT 
		name,
		slug,
		volume_24h_percentage_change_usd
	FROM 
		public.nftdatalatest
	where 
		market_cap_usd is not null
	order by %s %s NULLS LAST limit %s offset %s',
							  sortBy,
							  direction,
							  lim,
							  lim*pageNum
							 ) USING sortBy,lim,pageNum,direction;
END
$$ LANGUAGE plpgsql;


