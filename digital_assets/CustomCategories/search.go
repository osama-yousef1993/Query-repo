package datastruct

import (
	"time"
)

// NFTPrices Struct Will use to map the data that will retrieve from postgresql Table
type NFTPrices struct {
	ID                                         string   `json:"id" postgres:"id"`                                                                                             // It presents NFT Unique ID
	ContractAddress                            string   `json:"contract_address" postgres:"contract_address"`                                                                 // It presents NFT Contract Address
	AssetPlatformId                            string   `json:"asset_platform_id" postgres:"asset_platform_id"`                                                               // It presents the Chain ID that NFT is related to.
	Name                                       string   `json:"name" postgres:"name"`                                                                                         // It presents the NFT Name
	Symbol                                     string   `json:"symbol" postgres:"symbol"`                                                                                     // It presents the NFT Symbol
	DisplaySymbol                              string   `json:"displaySymbol" postgres:"display_symbol"`                                                                      // It presents the NFT Symbol
	Image                                      string   `json:"logo" postgres:"image"`                                                                                        // It presents the NFT Image
	Description                                string   `json:"description" postgres:"description"`                                                                           // It presents the NFT Description
	NativeCurrency                             string   `json:"native_currency" postgres:"native_currency"`                                                                   // It presents the NFT currency that NFT use to specify the currency like ethereum.
	FloorPriceUsd                              float64  `json:"floor_price_usd" postgres:"floor_price_usd"`                                                                   // It presents min price for the NFT in USD.
	MarketCapUsd                               float64  `json:"market_cap_usd" postgres:"market_cap_usd"`                                                                     // It presents the market cap for NFT in USD.
	Volume24hUsd                               float64  `json:"volume_24h_usd" postgres:"volume_24h_usd"`                                                                     // It presents volume for NFT in USD.
	FloorPriceNative                           float64  `json:"floor_price_native" postgres:"floor_price_native"`                                                             // It presents min price for NFT in native currency
	MarketCapNative                            float64  `json:"market_cap_native" postgres:"market_cap_native"`                                                               // It presents market cap for NFT in native currency
	Volume24hNative                            float64  `json:"volume_24h_native" postgres:"volume_24h_native"`                                                               // It presents volume for NFT in native currency
	FloorPriceInUsd24hPercentageChange         float64  `json:"floor_price_in_usd_24h_percentage_change" postgres:"floor_price_in_usd_24h_percentage_change"`                 // It presents the percentage change in floor price for 24 hours for NFT
	Volume24hPercentageChangeUsd               float64  `json:"volume_24h_percentage_change_usd" postgres:"volume_24h_percentage_change_usd"`                                 // It presents the percentage change in floor price for 24 hours for NFT
	NumberOfUniqueAddresses                    int      `json:"number_of_unique_addresses" postgres:"number_of_unique_addresses"`                                             // It presents the number of owners for the NFT
	NumberOfUniqueAddresses24hPercentageChange float64  `json:"number_of_unique_addresses_24h_percentage_change" postgres:"number_of_unique_addresses_24h_percentage_change"` // It presents the percentage change in the number of owners for 24 hours for NFTs.
	Slug                                       string   `json:"slug" postgres:"slug"`                                                                                         // It presents the slug for NFT
	TotalSupply                                float64  `json:"total_supply" postgres:"total_supply"`                                                                         // It presents total supply the NFT provide in there collection
	LastUpdated                                string   `json:"last_updated" postgres:"last_updated"`                                                                         // It presents last time NFT Data updated.
	FullCount                                  *int     `postgres:"full_count"`                                                                                               // It presents the number of NFTs that we have in Postgres.
	UUID                                       string   `json:"uuid"`                                                                                                         // It presents the number of NFTs that we have in Postgres.
	ForbesAssetPlatformId                      []string `json:"forbes_asset_platform_id" postgres:"forbes_asset_platform_id"`                                                 // It presents the number of NFTs that we have in Postgres.
}
type NFTPlatform struct {
	ID                    string   `json:"id" postgres:"id"`                                             // It presents NFT Unique ID                                                                                                        // It presents the number of NFTs that we have in Postgres.
	ForbesAssetPlatformId []string `json:"forbes_asset_platform_id" postgres:"forbes_asset_platform_id"` // It presents the number of NFTs that we have in Postgres.
}

type TradedAssetsTable struct {
	Symbol                string   `json:"symbol" firestore:"symbol" postgres:"symbol"`
	DisplaySymbol         string   `json:"displaySymbol" firestore:"displaySymbol" postgres:"displaySymbol"`
	Name                  string   `json:"name" firestore:"name" postgres:"name"`
	Slug                  string   `json:"slug" firestore:"slug" postgres:"slug"`
	Logo                  string   `json:"logo" firestore:"logo" postgres:"logo"`
	TemporaryDataDelay    bool     `json:"temporaryDataDelay" firestore:"temporaryDataDelay" postgres:"temporary_data_delay"`
	Price                 *float64 `json:"price" firestore:"price" postgres:"price_24h"`
	Percentage            *float64 `json:"percentage" firestore:"percentage" postgres:"percentage_24h"`
	Percentage1H          *float64 `json:"percentage_1h" firestore:"percentage_1h" postgres:"percentage_1h"`
	Percentage7D          *float64 `json:"percentage_7d" firestore:"percentage_7d" postgres:"percentage_7d"`
	ChangeValue           *float64 `json:"changeValue" firestore:"changeValue" postgres:"change_value_24h"`
	MarketCap             *float64 `json:"marketCap" firestore:"marketCap" postgres:"market_cap"`
	Volume                *float64 `json:"volume" firestore:"volume" postgres:"volume_1d"`
	FullCount             *int     `postgres:"full_count"`
	Rank                  *int     `json:"rank" firestore:"rank" postgres:"rank"`
	Status                string   `postgres:"status"`
	MarketCapPercentage1d *float64 `json:"market_cap_percent_change_1d" firestore:"market_cap_percent_change_1d" postgres:"market_cap_percent_change_1d"`
}

type CategoryFundamental struct {
	ForbesID                  string              `json:"-" bigquery:"forbesid" postgres:"forbesid"`              //Id suggested by forbes seo team
	ForbesName                string              `json:"forbesName" bigquery:"forbesName" postgres:"forbesName"` //Data that populates the categories description H1 tag
	ID                        string              `json:"id" bigquery:"id" postgres:"id"`                         //id from
	Name                      string              `json:"name" bigquery:"name" postgres:"name"`
	TotalTokens               int                 `json:"total_tokens" bigquery:"total_tokens" postgres:"total_tokens"`
	AveragePercentage24H      float64             `json:"average_percentage_24h" bigquery:"average_percentage_24h" postgres:"average_percentage_24h"`
	MarketCapPercentageChange float64             `json:"market_cap_percentage_change" bigquery:"market_cap_percentage_change" postgres:"market_cap_percentage_change"`
	Volume24H                 float64             `json:"volume_24h" bigquery:"volume_24h" postgres:"volume_24h"`
	Price24H                  float64             `json:"price_24h" bigquery:"price_24h" postgres:"price_24h"`
	AveragePrice              float64             `json:"average_price" bigquery:"average_price" postgres:"average_price"`
	MarketCap                 float64             `json:"market_cap" bigquery:"market_cap" postgres:"market_cap"`              // Market cap of the category
	TopGainers                []CategoryTopGainer `json:"top_gainers,omitempty" bigquery:"top_gainers" postgres:"top_gainers"` //Top Gainers are the top assets by marketcap percentage
	TopMovers                 []CategoryTopGainer `json:"top_movers,omitempty" bigquery:"top_movers" postgres:"top_movers"`    //Top Movers are the top assets by the absolute value of the  marketcap percentage
	LastUpdated               time.Time           `json:"last_updated" bigquery:"last_updated" postgres:"last_updated"`
	Slug                      string              `json:"slug" bigquery:"slug" postgres:"slug"`
}

type CategoryTopGainer struct {
	Slug                string  `json:"slug" bigquery:"slug" postgres:"slug"`
	Logo                string  `json:"logo" bigquery:"logo" postgres:"logo"`
	Symbol              string  `json:"symbol" bigquery:"symbol" postgres:"symbol"`
	Name                string  `json:"name" bigquery:"name" postgres:"name"`
	MarketCap           float64 `json:"market_cap" bigquery:"market_cap" postgres:"market_cap"`                                  // Market cap of the category
	MarketCapPercentage float64 `json:"market_cap_percentage" bigquery:"market_cap_percentage" postgres:"market_cap_percentage"` // Market cap percentage of the category
	Volume              float64 `json:"volume_24h" bigquery:"volume_24h" postgres:"volume_24h"`                                  // Market cap of the category
	VolumePercentage    float64 `json:"volume_percentage" bigquery:"volume_percentage" postgres:"volume_percentage"`             // Market cap percentage of the category
}

/*
This documentation explains the use of this Struct  https://docs.google.com/document/d/1gjEG6fDHklE6xsVx-DpaAcmqJXNpevv7SBYmpUawlVs/edit
The datasouce of this object is Coingecko
*/
type CategoriesData struct {
	ID                 string              `json:"id" postgres:"id"`                                       // ID of the category
	Name               string              `json:"name" postgres:"name"`                                   // Name of the category
	MarketCap          float64             `json:"market_cap" postgres:"market_cap"`                       // Market cap of the category
	MarketCapChange24H float64             `json:"market_cap_change_24h" postgres:"market_cap_change_24h"` // Market cap change in the last 24 hours
	Content            string              `json:"content" postgres:"content"`                             // Description of the category
	Top3Coins          []string            `json:"top_3_coins" postgres:"top_3_coins"`                     // Top 3 coins in the category
	Volume24H          float64             `json:"volume_24h" postgres:"volume_24h"`                       // Volume in the last 24 hours
	UpdatedAt          time.Time           `json:"updated_at" postgres:"updated_at"`                       // Last updated time
	Markets            []CoinsMarketResult `json:"markets" postgres:"markets"`                             // List of all the assets in the category
	ForbesName         string              `json:"forbesName" postgres:"forbesName"`                       // List of all the assets in the category
	Slug               string              `json:"slug" postgres:"slug"`                                   // List of all the assets in the category

}

type CoinsMarketResultResult []CoinsMarketResult

type SearchResultsTable string

const (
	FTSearch            SearchResultsTable = "Coin_Search"            // Fungible Token dictionary Kind - Search by name, symbol of the fungible token
	NftSearch           SearchResultsTable = "NFT_Search_Data"        // Non-Fungible Token dictionary Kind - Search by name, symbol of the NFT
	CategoryTableSearch SearchResultsTable = "CategoriesTable_Search" // Category dictionary Kind - Search directly by category
)

// Configurations used in FDA.
type FDAConfig_Categories struct {
	DocId                string
	CategoriesExclusions []string `json:"categoriesExclusions" firestore:"categoriesExclusions"`
}
