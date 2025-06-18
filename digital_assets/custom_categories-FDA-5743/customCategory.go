package datastruct

import (
	"fmt"
	"os"
	"time"
)

var CustomCategoryTable = fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "test_custom_category")

type CustomCategoryRequest struct {
	CategoryName   string   `json:"categoryName,omitempty" firestore:"categoryName"`
	PlatformId     string   `json:"platformId,omitempty" firestore:"platformId"`
	Assets         []string `json:"assets,omitempty" firestore:"assets"`
	TableName      string   `json:"tableName,omitempty" firestore:"tableName"`
	Column         string   `json:"column,omitempty" firestore:"column"`
	Sort           string   `json:"sort,omitempty" firestore:"sort"`
	Limit          int      `json:"limit,omitempty" firestore:"limit"`
	Condition      string   `json:"condition,omitempty" firestore:"condition"`
	ConditionValue string   `json:"conditionValue,omitempty" firestore:"conditionValue"`
}

type CustomCategory struct {
	CategoryName   string      `json:"categoryName" postgresql:"category_name"`
	CategoryFields interface{} `json:"categoryFields" postgresql:"category_fields"`
	CategoryType   string      `json:"categoryType" postgresql:"category_type"`
	LastUpdated    time.Time   `json:"lastUpdated" postgresql:"last_updated"`
}

type Fundamentals struct {
	Symbol                    string    `json:"symbol" firestore:"symbol" postgres:"symbol"`
	Name                      string    `json:"name" firestore:"name" postgres:"name"`
	Slug                      string    `json:"slug" firestore:"slug" postgres:"slug"`
	Logo                      string    `json:"logo" firestore:"logo" postgres:"logo"`
	DisplaySymbol             string    `json:"display_symbol" firestore:"displaySymbol" postgres:"display_symbol"`
	Source                    string    `json:"source" firestore:"source" postgres:"source"`
	TemporaryDataDelay        bool      `json:"temporary_data_delay" firestore:"temporaryDataDelay" postgres:"temporary_data_delay"`
	Price24h                  *float64  `json:"price_24h" firestore:"price24h" postgres:"price_24h"`
	Percentage24h             *float64  `json:"percentage_24h" firestore:"percentage24h" postgres:"percentage_24h"`
	Date                      time.Time `json:"date" firestore:"date" postgres:"date"`
	ChangeValue24h            *float64  `json:"change_value_24h" firestore:"changeValue24h" postgres:"change_value_24h"`
	MarketCap                 *float64  `json:"market_cap" firestore:"marketCap" postgres:"market_cap"`
	OriginalSymbol            string    `json:"original_symbol" firestore:"originalSymbol" postgres:"original_symbol"`
	NumberOfActiveMarketPairs *int64    `json:"number_of_active_market_pairs" firestore:"numberOfActiveMarketPairs" postgres:"number_of_active_market_pairs"`
	Price7D                   *float64  `json:"price_7d" firestore:"price7d" postgres:"price_7d"`
	Price30D                  *float64  `json:"price_30d" firestore:"price30d" postgres:"price_30d"`
	Price1Y                   *float64  `json:"price_1Y" firestore:"price1Y" postgres:"price_1Y"`
	PriceYTD                  *float64  `json:"price_ytd" firestore:"priceYtd" postgres:"price_ytd"`
	Percentage1H              *float64  `json:"percentage_1h" firestore:"percentage_1h" postgres:"percentage_1h"`
	Percentage7D              *float64  `json:"percentage_7d" firestore:"percentage_7d" postgres:"percentage_7d"`
	Percentage30D             *float64  `json:"percentage_30d" firestore:"percentage_30d" postgres:"percentage_30d"`
	Percentage1Y              *float64  `json:"percentage_1y" firestore:"percentage_1y" postgres:"percentage_1y"`
	PercentageYTD             *float64  `json:"percentage_ytd" firestore:"percentage_ytd" postgres:"percentage_ytd"`
	CirculatingSupply         *float64  `json:"circulating_supply" firestore:"circulatingSupply" postgres:"circulating_supply"`
	LastUpdated               time.Time `json:"last_updated" firestore:"last_updated" postgres:"last_updated"`
}

type NFTsTable struct {
	ID                                         string  `json:"id" postgres:"id"`                                                                                             // It presents NFT Unique ID
	ContractAddress                            string  `json:"contract_address" postgres:"contract_address"`                                                                 // It presents NFT Contract Address
	AssetPlatformId                            string  `json:"asset_platform_id" postgres:"asset_platform_id"`                                                               // It presents the Chain ID that NFT is related to.
	Name                                       string  `json:"name" postgres:"name"`                                                                                         // It presents the NFT Name
	Symbol                                     string  `json:"symbol" postgres:"symbol"`                                                                                     // It presents the NFT Symbol
	DisplaySymbol                              string  `json:"display_symbol" postgres:"display_symbol"`                                                                     // It presents the NFT Symbol
	Image                                      string  `json:"image" postgres:"image"`                                                                                       // It presents the NFT Image
	Description                                string  `json:"description" postgres:"description"`                                                                           // It presents the NFT Description
	NativeCurrency                             string  `json:"native_currency" postgres:"native_currency"`                                                                   // It presents the NFT currency that NFT use to specify the currency like ethereum.
	FloorPriceUsd                              float64 `json:"floor_price_usd" postgres:"floor_price_usd"`                                                                   // It presents min price for the NFT in USD.
	MarketCapUsd                               float64 `json:"market_cap_usd" postgres:"market_cap_usd"`                                                                     // It presents the market cap for NFT in USD.
	Volume24hUsd                               float64 `json:"volume_24h_usd" postgres:"volume_24h_usd"`                                                                     // It presents volume for NFT in USD.
	FloorPriceNative                           float64 `json:"floor_price_native" postgres:"floor_price_native"`                                                             // It presents min price for NFT in native currency
	MarketCapNative                            float64 `json:"market_cap_native" postgres:"market_cap_native"`                                                               // It presents market cap for NFT in native currency
	Volume24hNative                            float64 `json:"volume_24h_native" postgres:"volume_24h_native"`                                                               // It presents volume for NFT in native currency
	FloorPriceInUsd24hPercentageChange         float64 `json:"floor_price_in_usd_24h_percentage_change" postgres:"floor_price_in_usd_24h_percentage_change"`                 // It presents the percentage change in floor price for 24 hours for NFT
	Volume24hPercentageChangeUsd               float64 `json:"volume_24h_percentage_change_usd" postgres:"volume_24h_percentage_change_usd"`                                 // It presents the percentage change in floor price for 24 hours for NFT
	NumberOfUniqueAddresses                    int     `json:"number_of_unique_addresses" postgres:"number_of_unique_addresses"`                                             // It presents the number of owners for the NFT
	NumberOfUniqueAddresses24hPercentageChange float64 `json:"number_of_unique_addresses_24h_percentage_change" postgres:"number_of_unique_addresses_24h_percentage_change"` // It presents the percentage change in the number of owners for 24 hours for NFTs.
	Slug                                       string  `json:"slug" postgres:"slug"`                                                                                         // It presents the slug for NFT
	TotalSupply                                float64 `json:"total_supply" postgres:"total_supply"`                                                                         // It presents total supply the NFT provide in there collection
	LastUpdated                                string  `json:"last_updated" postgres:"last_updated"`                                                                         // It presents last time NFT Data updated.
}

type CategoryFundamentalTable struct {
	ID                        string              `json:"id" bigquery:"id" postgres:"id"` //id from
	Name                      string              `json:"name" bigquery:"name" postgres:"name"`
	TotalTokens               int                 `json:"total_tokens" bigquery:"total_tokens" postgres:"total_tokens"`
	AveragePercentage24H      float64             `json:"average_percentage_24h" bigquery:"average_percentage_24h" postgres:"average_percentage_24h"`
	MarketCapPercentageChange float64             `json:"market_cap_percentage_24h" bigquery:"market_cap_percentage_24h" postgres:"market_cap_percentage_24h"`
	Volume24H                 float64             `json:"volume_24h" bigquery:"volume_24h" postgres:"volume_24h"`
	Price24H                  float64             `json:"price_24h" bigquery:"price_24h" postgres:"price_24h"`
	AveragePrice              float64             `json:"average_price" bigquery:"average_price" postgres:"average_price"`
	MarketCap                 float64             `json:"market_cap" bigquery:"market_cap" postgres:"market_cap"`    // Market cap of the category
	TopGainers                []CategoryTopGainer `json:"top_gainers" bigquery:"top_gainers" postgres:"top_gainers"` //Top Gainers are the top assets by marketcap percentage
	TopMovers                 []CategoryTopGainer `json:"top_movers" bigquery:"top_movers" postgres:"top_movers"`    //Top Movers are the top assets by the absolute value of the  marketcap percentage
	LastUpdated               time.Time           `json:"last_updated" bigquery:"last_updated" postgres:"last_updated"`
	ForbesID                  string              `json:"forbesID" bigquery:"forbesid" postgres:"forbesID"`       //Id suggested by forbes seo team
	ForbesName                string              `json:"forbesName" bigquery:"forbesName" postgres:"forbesName"` //Data that populates the categories description H1 tag
	Slug                      string              `json:"slug" bigquery:"slug" postgres:"slug"`
	IsHighlighted             bool                `json:"is_highlighted" bigquery:"is_highlighted" postgres:"is_highlighted"`
}

var FundamentalsColumns string = `
SELECT 
	symbol,
	name,
	slug,
	logo,
	display_symbol,
	source,
	temporary_data_delay,
	price_24h,
	percentage_24h,
	date,
	change_value_24h,
	market_cap,
	original_symbol,
	number_of_active_market_pairs,
	price_7d,
	price_30d,
	price_1Y,
	price_ytd,
	percentage_1h,
	percentage_7d,
	percentage_30d,
	percentage_1y,
	percentage_ytd,
	circulating_supply,
	last_updated

`
var NFTColumns string = `
SELECT 
	id,
	contract_address,
	asset_platform_id,
	name,
	symbol,
	symbol as display_symbol,
	image,
	description,
	native_currency,
	floor_price_usd,
	market_cap_usd,
	volume_24h_usd,
	floor_price_native,
	market_cap_native,
	volume_24h_native,
	floor_price_in_usd_24h_percentage_change,
	volume_24h_percentage_change_usd,
	number_of_unique_addresses,
	number_of_unique_addresses_24h_percentage_change,
	slug,
	total_supply,
	last_updated
`
var CategoryColumns string = `
SELECT 
	id,
	name,
	total_tokens,
	average_percentage_24h,
	market_cap_percentage_24h,
	volume_24h,
	price_24h,
	average_price,
	market_cap,
	top_gainers,
	top_movers,
	last_updated,
	"forbesID",
	"forbesName",
	slug,
	is_highlighted
`
