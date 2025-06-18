package model

import "time"

type CoinGeckoAsset struct {
	ID        string      `json:"id"`                  //coin id
	Symbol    string      `json:"symbol"`              //coin symbol
	Name      string      `json:"name"`                //coin name
	Platforms interface{} `json:"platforms,omitempty"` //platforms (blockchains) mapped to their address

}

type CoingeckoExchangeMetadata struct {
	ForbesID                    string    `json:"forbes_id"`
	ID                          string    `json:"id"`
	Name                        string    `json:"name"`
	Slug                        string    `json:"slug"`
	Year                        int       `json:"year"`
	Description                 string    `json:"description"`
	Location                    string    `json:"location"`
	LogoURL                     string    `json:"logo_url"`
	Logo                        string    `json:"logo"` //only used for sending the logo to frontend
	WebsiteURL                  string    `json:"website_url"`
	TwitterURL                  string    `json:"twitter_url"`
	FacebookURL                 string    `json:"facebook_url"`
	YoutubeURL                  string    `json:"youtube_url"`
	LinkedinURL                 string    `json:"linkedin_url"`
	RedditURL                   string    `json:"reddit_url"`
	ChatURL                     string    `json:"chat_url"`
	SlackURL                    string    `json:"slack_url"`
	TelegramURL                 string    `json:"telegram_url"`
	BlogURL                     string    `json:"blog_url"`
	Centralized                 bool      `json:"centralized"`
	Decentralized               bool      `json:"decentralized"`
	HasTradingIncentive         bool      `json:"has_trading_incentive"`
	TrustScore                  int       `json:"trust_score"`
	TrustScoreRank              int       `json:"trust_score_rank"`
	TradeVolume24HBTC           float64   `json:"trade_volume_24h_btc"`
	TradeVolume24HBTCNormalized float64   `json:"trade_volume_24h_btc_normalized"`
	LastUpdated                 time.Time `json:"last_updated"`
}
