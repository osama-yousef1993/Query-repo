package dto

import "time"

// TokenSubmission is the struct for the token submission
// This struct is used to submit token data to Forbes
type AssetSubmission struct {
	ID                string    `json:"id" firestore:"id" postgres:"id"`                                                 // ID is the unique identifier of the token
	Name              string    `json:"name" firestore:"name" postgres:"name"`                                           // Name is the name of the token
	Symbol            string    `json:"symbol" firestore:"symbol" postgres:"symbol"`                                     // Symbol is the symbol of the token
	Price             float64   `json:"price" firestore:"price" postgres:"price"`                                        // Price is the price of the token
	CirculatingSupply float64   `json:"circulating_supply" firestore:"circulating_supply" postgres:"circulating_supply"` // CirculatingSupply is the circulating supply of the token
	MarketCap         float64   `json:"market_cap" firestore:"market_cap" postgres:"market_cap"`                         // MarketCap is the market cap of the token
	Volume            float64   `json:"volume" firestore:"volume" postgres:"volume"`                                     // Volume is the volume of the token
	QuoteCurrency     string    `json:"quote_currency" firestore:"quote_currency" postgres:"quote_currency"`             // QuoteCurrency is the quote currency of the token
	Source            string    `json:"source" firestore:"source" postgres:"source"`                                     // Source is the source of the token
	OccuranceTime     time.Time `json:"occurance_time" firestore:"occurance_time" postgres:"occurance_time"`             // OccuranceTime is the time of the occurance
}

// ForbesToken is the struct for the token data in Forbes
// This struct is used to store token data in Forbes
type ForbesAsset struct {
	ID              *string    `json:"id" postgres:"id"`                             // ForbesID is the unique identifier of the token
	Name            *string    `json:"name" postgres:"name"`                         // Name is the name of the token
	Symbol          *string    `json:"symbol" postgres:"symbol"`                     // Symbol is the symbol of the token
	CoingeckoID     *string    `json:"coingecko_id" postgres:"coingecko_id"`         // CoingeckoID is the Coingecko ID of the token
	CoinpaprikaID   *string    `json:"coinpaprika_id" postgres:"coinpaprika_id"`     // CoinpaprikaID is the Coinpaprika ID of the token
	ContractAddress *string    `json:"contract_address" postgres:"contract_address"` // ContractAddress is the contract address of the token
	LastUpdated     *time.Time `json:"last_updated" postgres:"last_updated"`         // LastUpdated is the time of the last update
}
