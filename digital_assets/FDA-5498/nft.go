package datastruct

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

// Collection name of where a users watchlist information is stored
var NftCollectionName = fmt.Sprintf("%s%s", "nft", os.Getenv("DATA_NAMESPACE"))

// Collection name of NFT Chains exist in FS
var NFTChainCollectionName = fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "nft_chains")

// NFTCollection Struct Will use to map the data that will retrieve from postgresql Table for each nft collection
type NftCollection struct {
	ID                                         string         `json:"id" postgres:"id"`                                                                                             // It presents NFT Unique ID
	ContractAddress                            string         `json:"contract_address" postgres:"contract_address"`                                                                 // It presents NFT Contract Address
	AssetPlatformId                            string         `json:"asset_platform_id" postgres:"asset_platform_id"`                                                               // It presents the Chain ID that NFT is related to.
	Name                                       string         `json:"name" postgres:"name"`                                                                                         // It presents the NFT Name
	Symbol                                     string         `json:"symbol" postgres:"symbol"`                                                                                     // It presents the NFT Symbol
	Rank                                       int            `json:"rank" postgres:"rank,omitempty"`                                                                               // It presents the NFT Rank. It is calculated by sorting an asset by volume_24h_usd field. We use slug as tiebreakers. And its calculated through the stored procedure
	PrevRankedSlug                             string         `json:"prev_ranked_slug" postgres:"prev_ranked_slug,omitempty"`                                                       // Previously ranked asset's slug
	NextRankedSlug                             string         `json:"next_ranked_slug" postgres:"next_ranked_slug,omitempty"`                                                       // Next ranked asset's slug
	DisplaySymbol                              string         `json:"displaySymbol" postgres:"display_symbol"`                                                                      // It presents the NFT Symbol
	Image                                      Image          `json:"image" postgres:"image"`                                                                                       // It presents the NFT Image
	Description                                string         `json:"description" postgres:"description"`                                                                           // It presents the NFT Description
	NativeCurrency                             string         `json:"native_currency" postgres:"native_currency"`                                                                   // It presents the NFT currency that NFT use to specify the currency like ethereum.
	FloorPriceUsd                              float64        `json:"floor_price_usd" postgres:"floor_price_usd"`                                                                   // It presents min price for the NFT in USD.
	MarketCapUsd                               float64        `json:"market_cap_usd" postgres:"market_cap_usd"`                                                                     // It presents the market cap for NFT in USD.
	Volume24hUsd                               float64        `json:"volume_24h_usd" postgres:"volume_24h_usd"`                                                                     // It presents volume for NFT in USD.
	FloorPriceNative                           float64        `json:"floor_price_native" postgres:"floor_price_native"`                                                             // It presents min price for NFT in native currency
	MarketCapNative                            float64        `json:"market_cap_native" postgres:"market_cap_native"`                                                               // It presents market cap for NFT in native currency
	Volume24hNative                            float64        `json:"volume_24h_native" postgres:"volume_24h_native"`                                                               // It presents volume for NFT in native currency
	FloorPriceInUsd24hPercentageChange         float64        `json:"floor_price_in_usd_24h_percentage_change" postgres:"floor_price_in_usd_24h_percentage_change"`                 // It presents the percentage change in floor price for 24 hours for NFT
	Volume24hPercentageChangeUsd               float64        `json:"volume_24h_percentage_change_usd" postgres:"volume_24h_percentage_change_usd"`                                 // It presents the percentage change in floor price for 24 hours for NFT
	NumberOfUniqueAddresses                    int            `json:"number_of_unique_addresses" postgres:"number_of_unique_addresses"`                                             // It presents the number of owners for the NFT
	NumberOfUniqueAddresses24hPercentageChange float64        `json:"number_of_unique_addresses_24h_percentage_change" postgres:"number_of_unique_addresses_24h_percentage_change"` // It presents the percentage change in the number of owners for 24 hours for NFTs.
	Slug                                       string         `json:"slug" postgres:"slug"`                                                                                         // It presents the slug for NFT
	TotalSupply                                float64        `json:"total_supply" postgres:"total_supply"`                                                                         // It presents total supply the NFT provide in there collection
	WebsiteUrl                                 string         `json:"website_url" postgres:"website_url"`                                                                           // It presents the website_url for NFT
	TwitterUrl                                 string         `json:"twitter_url" postgres:"twitter_url"`                                                                           // It presents the twitter_url for NFT
	DiscordUrl                                 string         `json:"discord_url" postgres:"discord_url"`                                                                           // It presents the discord_url for NFT
	Explorers                                  []Explorers    `json:"explorers" postgres:"explorers"`                                                                               // It presents the explorers for NFT
	LastUpdated                                string         `json:"last_updated" postgres:"last_updated"`                                                                         // It presents last time NFT Data updated.
	AvgSalePrice                               NftTimeframes  `json:"avg_sale_price" postgres:"avg_sale_price"`                                                                     // It presents the average sale price for NFT in different timeframes.
	AvgTotalSalesPercentChange                 NftTimeframes  `json:"avg_total_sales_pct_change" postgres:"avg_total_sales_pct_change"`                                             // It presents the average sale price for NFT in different timeframes.
	TotalSales                                 NftTimeframes  `json:"total_sales" postgres:"total_sales"`                                                                           // It presents the average sale price for NFT in different timeframes.
	AvgSalesPriceChange                        NftTimeframes  `json:"avg_sales_price_change" postgres:"avg_sales_price_change"`                                                     // It presents the average sale price for NFT in different timeframes.
	VolumeUSD                                  NftTimeframes  `json:"volume_usd" postgres:"volume_usd"`                                                                             // It presents the volume usd for NFT in different timeframes.
	VolumeNative                               NftTimeframes  `json:"volume_native" postgres:"volume_native"`                                                                       // It presents the volume native for NFT in different timeframes.
	VolumePercentChangeUSD                     NftTimeframes  `json:"volume_percent_change_usd" postgres:"volume_percent_change_usd"`                                               // It presents the average volume usd for NFT in different timeframes.
	VolumePercentChangeNative                  NftTimeframes  `json:"volume_percent_change_native" postgres:"volume_percent_change_native"`                                         // It presents the average volume native for NFT in different timeframes.
	LowestFloorPriceUSD                        NftTimeframes  `json:"lowest_floor_price_usd" postgres:"lowest_floor_price_usd"`                                                     // It presents the lowest price usd for NFT in different timeframes.
	HighestFloorPriceUSD                       NftTimeframes  `json:"highest_floor_price_usd" postgres:"highest_floor_price_usd"`                                                   // It presents the highest price usd for NFT in different timeframes.
	LowestFloorPriceNative                     NftTimeframes  `json:"lowest_floor_price_native" postgres:"lowest_floor_price_native"`                                               // It presents the lowest price native for NFT in different timeframes.
	HighestFloorPriceNative                    NftTimeframes  `json:"highest_floor_price_native" postgres:"highest_floor_price_native"`                                             // It presents the highest price native for NFT in different timeframes.
	FloorPricePercentChangeUSD                 NftTimeframes  `json:"floor_price_percent_change_usd" postgres:"floor_price_percent_change_usd"`                                     // It presents the floor price percent change usd for NFT in different timeframes.
	FloorPricePercentChangeNative              NftTimeframes  `json:"floor_price_percent_change_native" postgres:"floor_price_percent_change_native"`                               // It It presents the floor price percent change native for NFT in different timeframes.
	LowestFloorPricePercentChangeUSD           NftTimeframes  `json:"lowest_floor_price_percentage_change_usd" postgres:"lowest_floor_price_percentage_change_usd"`                 // It It presents the lowest floor price percent change usd for NFT in different timeframes.
	HighestFloorPricePercentChangeUSD          NftTimeframes  `json:"highest_floor_price_percentage_change_usd" postgres:"highest_floor_price_percentage_change_usd"`               // It It presents the highest floor price percent change use for NFT in different timeframes.
	LowestFloorPricePercentChangeNative        NftTimeframes  `json:"lowest_floor_price_percentage_change_native" postgres:"lowest_floor_price_percentage_change_native"`           // It It presents the lowest floor price percent change native for NFT in different timeframes.
	HighestFloorPricePercentChangeNative       NftTimeframes  `json:"highest_floor_price_percentage_change_native" postgres:"highest_floor_price_percentage_change_native"`         // It It presents the highest floor price percent change native for NFT in different timeframes.
	NativeCurrencySymbol                       string         `json:"native_currency_symbol" postgres:"native_currency_symbol"`                                                     // It presents the native_currency_symbol for NFT
	MarketCap24hPercentageChangeUSD            float64        `json:"market_cap_24h_percentage_change_usd" postgres:"market_cap_24h_percentage_change_usd"`                         // It presents the market_cap_24h_percentage_change_usd for NFT
	MarketCap24hPercentageChangeNative         float64        `json:"market_cap_24h_percentage_change_native" postgres:"market_cap_24h_percentage_change_native"`                   // It presents the market_cap_24h_percentage_change_native for NFT
	Volume24hPercentageChangeNative            float64        `json:"volume_24h_percentage_change_native" postgres:"volume_24h_percentage_change_native"`                           // It presents the volume_24h_percentage_change_native for NFT
	LowestFloorPrice24hUsd                     float64        `json:"lowest_floor_price_24h_usd,omitempty" postgres:"lowest_floor_price_24h_usd"`                                   // It presents the lowest_floor_price_24h_usd for NFT
	HighestFloorPrice24hUsd                    float64        `json:"highest_floor_price_24h_usd,omitempty" postgres:"highest_floor_price_24h_usd"`                                 // It presents the highest_floor_price_24h_usd for NFT
	LowestFloorPrice24hNative                  float64        `json:"lowest_floor_price_24h_native,omitempty" postgres:"lowest_floor_price_24h_native"`                             // It presents the lowest_floor_price_24h_native for NFT
	HighestFloorPrice24hNative                 float64        `json:"highest_floor_price_24h_native,omitempty" postgres:"highest_floor_price_24h_native"`                           // It presents the highest_floor_price_24h_native for NFT
	FloorPrice24hPercentageChangeUsd           float64        `json:"floor_price_24h_percentage_change_usd,omitempty" postgres:"floor_price_24h_percentage_change_usd"`             // It presents the floor_price_24h_percentage_change_usd for NFT
	FloorPrice24hPercentageChangeNative        float64        `json:"floor_price_24h_percentage_change_native,omitempty" postgres:"floor_price_24h_percentage_change_native"`       // It presents the floor_price_24h_percentage_change_native for NFT
	NFTQuestion                                []NFTQuestion  `json:"questions,omitempty" postgres:"questions"`                                                                     // It presents the questions for NFT
	NextUp                                     []NextUpSlices `json:"next_up,omitempty" postgres:"next_up"`
}

// The 4 next ranked assets array, required for the recirc component.
type NextUpSlices struct {
	Slug  string `json:"slug,omitempty" postgres:"slug"`   // Slug of each nft asset
	Rank  int    `json:"rank,omitempty" postgres:"rank"`   // Rank of each nft asset, sorted by volume_24h.
	Name  string `json:"name,omitempty" postgres:"name"`   // It presents the NFT Name
	Image Image  `json:"image,omitempty" postgres:"image"` // It presents the NFT Image
}
type NftTimeframes struct {
	OneDay    float64 `json:"1d,omitempty"`  // One day timeframe
	SevenDay  float64 `json:"7d,omitempty"`  // Seven day timeframe
	ThirtyDay float64 `json:"30d,omitempty"` // Thirty day timeframe
	NinetyDay float64 `json:"90d,omitempty"` // Ninety day timeframe
	Ytd       float64 `json:"ytd,omitempty"` // Ninety day timeframe
}

type NFTChain struct {
	ID   string `json:"id" firestore:"id"`     // Id of chain and it will present the assets platform id from the NFT endpoint. We will use it to filter NFTs by chains.
	Name string `json:"name" firestore:"name"` // Name for Chain, it will be used to display in the NFT prices Page.
}
type Explorers struct {
	Name string `json:"name" postgres:"name"` // Name for NFT Explorers ex: Etherscan, Ethplorer.
	Link string `json:"link" postgres:"link"` // Link for Explorers.
}

type NFTPricesResp struct {
	NFT                   []NFTPrices `json:"nft"`    // Array of NFTs result
	Total                 int         `json:"total"`  // The NFTs total exist in response that return from Postgres.
	Source                string      `json:"source"` // The source that provides NFTs data.
	HasTemporaryDataDelay bool        `json:"hasTemporaryDataDelay"`
}

type Image struct {
	Small string `json:"small" postgres:"image"`       // Small image version for NFT
	Large string `json:"large" postgres:"large_image"` // Large image version for NFT
}

type NFTQuestion struct {
	Question string `json:"question"` // NFT Question
	Answer   string `json:"answer"`   // NFT Answer
}

// Will use this type to map the ExplorersResult Array from PG
type ExplorersResult []Explorers

func (c ExplorersResult) Value() (driver.Value, error) {
	return json.Marshal(c)
}

func (c *ExplorersResult) Scan(value interface{}) error {
	var b []byte
	switch t := value.(type) {
	case []byte:
		b = t
	case string:
		b = []byte(t)
	default:
		return errors.New("unknown type")
	}
	return json.Unmarshal(b, c)
}

// Will use this type to map the ExplorersResult Array from PG
type NFTQuestionResult []NFTQuestion

func (c NFTQuestionResult) Value() (driver.Value, error) {
	return json.Marshal(c)
}

func (c *NFTQuestionResult) Scan(value interface{}) error {
	var b []byte
	switch t := value.(type) {
	case []byte:
		b = t
	case string:
		b = []byte(t)
	default:
		return errors.New("unknown type")
	}
	return json.Unmarshal(b, c)
}

// Will use this type to map the NextUpSlices Array from PG
type NextUpResult []NextUpSlices

func (c NextUpResult) Value() (driver.Value, error) {
	return json.Marshal(c)
}

func (c *NextUpResult) Scan(value interface{}) error {
	var b []byte
	switch t := value.(type) {
	case []byte:
		b = t
	case string:
		b = []byte(t)
	default:
		return errors.New("unknown type")
	}
	return json.Unmarshal(b, c)
}
