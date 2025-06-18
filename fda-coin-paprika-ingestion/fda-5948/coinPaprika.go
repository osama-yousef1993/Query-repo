package datastruct

import "time"

// Coin represents the structure used to map the results returned by CoinPaprika's API,
// providing basic information about cryptocurrencies.
type Coins struct {
	ID          string    `json:"id,omitempty" postgresql:"id,omitempty"`                     // Unique identifier of the coin on CoinPaprika
	Name        string    `json:"name,omitempty" postgresql:"name,omitempty"`                 // Name of the coin as listed on CoinPaprika
	Symbol      string    `json:"symbol,omitempty" postgresql:"symbol,omitempty"`             // Symbol or ticker of the coin on CoinPaprika
	Rank        int64     `json:"rank,omitempty" postgresql:"rank,omitempty"`                 // Current rank of the coin on CoinPaprika; depends on 'is_active' being true; otherwise, it will be 0
	IsNew       bool      `json:"is_new,omitempty" postgresql:"is_new,omitempty"`             // Flag indicating if the coin was added within the last 5 days
	IsActive    bool      `json:"is_active,omitempty" postgresql:"is_active,omitempty"`       // Flag indicating if the coin is active; active coins have calculable price and volume
	LastUpdated time.Time `json:"last_updated,omitempty" postgresql:"last_updated,omitempty"` // Timestamp of the last update to this record in the database
}

// Coin represents the detailed structure used to map the cryptocurrency data from CoinPaprika.
type Coin struct {
	ID                *string     `json:"id,omitempty" postgresql:"id,omitempty"`                                 // Unique identifier of the cryptocurrency on CoinPaprika
	Name              *string     `json:"name,omitempty" postgresql:"name,omitempty"`                             // Name of the cryptocurrency as listed on CoinPaprika
	Symbol            *string     `json:"symbol,omitempty" postgresql:"symbol,omitempty"`                         // Symbol or ticker of the cryptocurrency on CoinPaprika
	Rank              *int64      `json:"rank,omitempty" postgresql:"rank,omitempty"`                             // Current rank of the cryptocurrency on CoinPaprika; rank is 0 if 'is_active' is false
	IsNew             *bool       `json:"is_new,omitempty" postgresql:"is_new,omitempty"`                         // Indicates if the cryptocurrency was added within the last 5 days
	IsActive          *bool       `json:"is_active,omitempty" postgresql:"is_active,omitempty"`                   // Indicates if the cryptocurrency is active; active coins have calculable price and volume
	Logo              *string     `json:"logo,omitempty" postgresql:"logo,omitempty"`                             // URL of the cryptocurrency's logo image
	Tags              []Tag       `json:"tags,omitempty" postgresql:"tags,omitempty"`                             // List of tags associated with the cryptocurrency on CoinPaprika
	Team              []Person    `json:"team,omitempty" postgresql:"team,omitempty"`                             // List of team members involved in the development of the cryptocurrency
	Parent            Parent      `json:"parent,omitempty" postgresql:"parent,omitempty"`                         // cryptocurrency's Parent
	Description       *string     `json:"description,omitempty" postgresql:"description,omitempty"`               // Text description of the cryptocurrency
	Message           *string     `json:"message,omitempty" postgresql:"message,omitempty"`                       // Important status message or announcement about the cryptocurrency
	OpenSource        *bool       `json:"open_source,omitempty" postgresql:"open_source,omitempty"`               // Indicates if the cryptocurrency is an open-source project
	HardwareWallet    *bool       `json:"hardware_wallet,omitempty" postgresql:"hardware_wallet,omitempty"`       // Indicates if the cryptocurrency is supported by hardware wallets
	StartedAt         *string     `json:"started_at,omitempty" postgresql:"started_at,omitempty"`                 // Launch date of the cryptocurrency (in RFC3339/ISO-8601 format)
	DevelopmentStatus *string     `json:"development_status,omitempty" postgresql:"development_status,omitempty"` // Development status (e.g., working product, beta, concept)
	ProofType         *string     `json:"proof_type,omitempty" postgresql:"proof_type,omitempty"`                 // Type of consensus mechanism used (e.g., Proof of Work, Proof of Stake)
	OrgStructure      *string     `json:"org_structure,omitempty" postgresql:"org_structure,omitempty"`           // Organizational structure of the project (e.g., centralized, decentralized)
	HashAlgorithm     *string     `json:"hash_algorithm,omitempty" postgresql:"hash_algorithm,omitempty"`         // Name of the hash algorithm used by the cryptocurrency
	Contracts         []Contracts `json:"contracts,omitempty" postgresql:"contracts,omitempty"`                   // List of contract information (e.g., smart contract addresses and platforms)
	Links             Links       `json:"links,omitempty" postgresql:"links,omitempty"`                           // List of general links (e.g., website, explorer, social media)
	LinksExtended     []CoinLink  `json:"links_extended,omitempty" postgresql:"links_extended,omitempty"`         // Extended link details including stats (e.g., followers, members)
	Whitepaper        Whitepaper  `json:"whitepaper,omitempty" postgresql:"whitepaper,omitempty"`                 // Information about the whitepaper, including link and thumbnail
	FirstDataAt       *string     `json:"first_data_at,omitempty" postgresql:"first_data_at,omitempty"`           // Date of the first available data for the cryptocurrency (in RFC3339/ISO-8601 format)
	LastUpdated       time.Time   `json:"last_updated,omitempty" postgresql:"last_updated,omitempty"`             // Timestamp of the last update in the database
}

// Person represents an individual team member involved in the cryptocurrency project.
type Person struct {
	ID       string `json:"id,omitempty" postgresql:"id,omitempty"`             // Unique identifier of the team member
	Name     string `json:"name,omitempty" postgresql:"name,omitempty"`         // Name of the team member
	Position string `json:"position,omitempty" postgresql:"position,omitempty"` // Position or role of the team member in the project
}

// Parent represents the parent for this coin ex Base will be parent for usdc-usd-coin
type Parent struct {
	ID     string `json:"id,omitempty" postgresql:"id,omitempty"`         // Unique identifier of the Parent ex : base-base
	Name   string `json:"name,omitempty" postgresql:"name,omitempty"`     // Name of the parent ex : Base
	Symbol string `json:"symbol,omitempty" postgresql:"symbol,omitempty"` // Symbol or Parent ex : BASE
}

// Contracts represents smart contract details for a cryptocurrency.
type Contracts struct {
	Contract string `json:"contract,omitempty" postgresql:"contract,omitempty"` // Contract identifier, typically the smart contract address
	Platform string `json:"platform,omitempty" postgresql:"platform,omitempty"` // Platform hosting the contract (e.g., Ethereum, Tron)
	Type     string `json:"type,omitempty" postgresql:"type,omitempty"`         // Contract type (e.g., ERC20, BEP2, TRC10, TRC20)
}

// Links represents various URLs related to the cryptocurrency.
type Links struct {
	Explorer     []string `json:"explorer,omitempty" postgresql:"explorer,omitempty"`           // List of blockchain explorer links
	Facebook     string   `json:"facebook,omitempty" postgresql:"facebook,omitempty"`           // List of Facebook page links
	Reddit       string   `json:"reddit,omitempty" postgresql:"reddit,omitempty"`               // List of Reddit community or profile links
	SourceCode   string   `json:"source_code,omitempty" postgresql:"source_code,omitempty"`     // List of source code repository links
	Website      string   `json:"website,omitempty" postgresql:"website,omitempty"`             // List of official website links
	Youtube      string   `json:"youtube,omitempty" postgresql:"youtube,omitempty"`             // List of YouTube channel links
	Medium       string   `json:"medium,omitempty" postgresql:"medium,omitempty"`               // List of Medium blog/profile links
	Announcement string   `json:"announcement,omitempty" postgresql:"announcement,omitempty"`   // List of Announcement channel this will be added from links extended
	Telegram     string   `json:"telegram,omitempty" postgresql:"telegram,omitempty"`           // List of telegram channel this will be added from links extended
	Twitter      string   `json:"twitter,omitempty" postgresql:"twitter,omitempty"`             // List of twitter channel this will be added from links extended
	MessageBoard string   `json:"message_board,omitempty" postgresql:"message_board,omitempty"` // List of message_board channel this will be added from links extended
	Wallet       string   `json:"wallet,omitempty" postgresql:"wallet,omitempty"`               // List of wallet channel this will be added from links extended
	Blog         string   `json:"blog,omitempty" postgresql:"blog,omitempty"`                   // List of blog channel this will be added from links extended
	Chat         string   `json:"chat,omitempty" postgresql:"chat,omitempty"`                   // List of chat channel this will be added from links extended
	Slack        string   `json:"slack,omitempty" postgresql:"slack,omitempty"`                 // List of slack channel this will be added from links extended
	Discord      string   `json:"discord,omitempty" postgresql:"discord,omitempty"`             // List of discord channel this will be added from links extended
}

// LinksExtended contains detailed link information along with statistics for certain links.
type CoinLink struct {
	URL   *string            `json:"url,omitempty" postgresql:"url,omitempty"`     // URL of the link
	Type  *string            `json:"type,omitempty" postgresql:"type,omitempty"`   // Type of the link (e.g., website, explorer, social media)
	Stats map[string]float64 `json:"stats,omitempty" postgresql:"stats,omitempty"` // Statistics associated with the link (e.g., followers, stars, subscribers)
}

// Whitepaper represents the cryptocurrency whitepaper details.
type Whitepaper struct {
	Link      string `json:"link,omitempty" postgresql:"link,omitempty"`           // URL of the whitepaper
	Thumbnail string `json:"thumbnail,omitempty" postgresql:"thumbnail,omitempty"` // Thumbnail image of the whitepaper
}

// CoinOHLCV represents the Open, High, Low, Close, Volume, and Market Cap data for a cryptocurrency over a specific time period.
// It is commonly used in financial analysis and trading to track price movements and market activity.
type CoinOHLCV struct {
	ID        string    `json:"id" postgresql:"id"`                 // Unique identifier of the cryptocurrency.
	TimeOpen  time.Time `json:"time_open" postgresql:"time_open"`   // The timestamp when the time period begins.
	TimeClose time.Time `json:"time_close" postgresql:"time_close"` // The timestamp when the time period ends
	Open      float64   `json:"open" postgresql:"open"`             // The opening price of the cryptocurrency at the start of the time period.
	High      float64   `json:"high" postgresql:"high"`             // The highest price of the cryptocurrency during the time period.
	Low       float64   `json:"low" postgresql:"low"`               // The lowest price of the cryptocurrency during the time period.
	Close     float64   `json:"close" postgresql:"close"`           // The closing price of the cryptocurrency at the end of the time period.
	Volume    int64     `json:"volume" postgresql:"volume"`         // The total trading volume of the cryptocurrency during the time period.
	MarketCap int64     `json:"market_cap" postgresql:"market_cap"` // The market capitalization of the cryptocurrency at the end of the time period.
}

// This query will get the top 60 coin from coinpaprika_assets
// We take only 60 because this is the total request we can hit with free endpoint from CoinPaprika
// This will be removed after we update our plan with CoinPaprika to get full Coins from Our data base
const CoinPaprikaAssets = `
SELECT 
	id,
	name,
	symbol,
	rank,
	is_new,
	is_active,
	last_updated
FROM 
	public.coinpaprika_assets
where 
	rank != 0
Order by 
	rank asc
LIMIT 10;`
