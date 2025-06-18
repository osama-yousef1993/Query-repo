package dto

import (
	"time"

	"cloud.google.com/go/bigquery"
)

type BQExchangesTickers struct {
	Name    string     `bigquery:"name" json:"name"`       //name of exchange
	Tickers []BQTicker `bigquery:"tickers" json:"tickers"` //list of tickers
	Source  string     `bigquery:"source" json:"source"`   //source of the data
}

type BQTicker struct {
	Base                   string                 `bigquery:"base" json:"base,omitempty"`     //Ticker's coin
	Target                 string                 `bigquery:"target" json:"target,omitempty"` //Ticker's target coin
	Market                 BQMarketSimple         `bigquery:"market" json:"market,omitempty"` //Ticker's simple market
	Last                   bigquery.NullFloat64   `bigquery:"last" json:"last,omitempty"`     //Ticker's last price against the target token
	Volume                 bigquery.NullFloat64   `bigquery:"volume" json:"volume,omitempty"` //Ticker's volume
	ConvertedLast          BQConvertedLast        `bigquery:"convertedLast" json:"converted_last,omitempty"`
	ConvertedVolume        BQConvertedVolume      `bigquery:"convertedVolume" json:"converted_volume,omitempty"`                 //Ticker's converted volume
	CostToMoveUpUsd        bigquery.NullFloat64   `bigquery:"costToMoveUpUsd" json:"cost_to_move_up_usd,omitempty"`              //Cost to move up in USD
	CostToMoveDownUsd      bigquery.NullFloat64   `bigquery:"costToMoveDownUsd" json:"cost_to_move_down_usd,omitempty"`          //Cost to move down in USD
	TrustScore             string                 `bigquery:"trustScore" json:"trust_score,omitempty"`                           //Trust score
	BidAskSpreadPercentage bigquery.NullFloat64   `bigquery:"bidAskSpreadPercentage" json:"bid_ask_spread_percentage,omitempty"` //Bid & Ask spread's percentage
	Timestamp              bigquery.NullTimestamp `bigquery:"timestamp" json:"timestamp,omitempty"`                              //Timestamp
	LastTradedAt           bigquery.NullTimestamp `bigquery:"lastTradedAt" json:"last_traded_at,omitempty"`                      //Last traded at timestamp
	LastFetchAt            bigquery.NullTimestamp `bigquery:"lastFetchAt" json:"last_fetch_at,omitempty"`                        //Last data fetched at timestamp
	IsAnomaly              bool                   `bigquery:"isAnomaly" json:"is_anomaly,omitempty"`                             //Whether the ticker is an anomaly
	IsStale                bool                   `bigquery:"isStale" json:"is_stale,omitempty"`                                 //Whether the ticker is stale!
	TradeURL               string                 `bigquery:"tradeUrl" json:"trade_url,omitempty"`                               //Trade URL
	TokenInfoURL           string                 `bigquery:"tokenInfoUrl" json:"token_info_url,omitempty"`                      //URL of the ticker
	CoinID                 string                 `bigquery:"coinId" json:"coin_id,omitempty"`                                   //Coin ID
	TargetCoinID           string                 `bigquery:"targetCoinId" json:"target_coin_id,omitempty"`                      // Target coin's id
}

type BQMarketSimple struct {
	Name                string `bigquery:"name" json:"name,omitempty"`                                 //market's name
	Identifier          string `bigquery:"identifier" json:"identifier,omitempty"`                     //market's id
	HasTradingIncentive bool   `bigquery:"hasTradingIncentive" json:"has_trading_incentive,omitempty"` //Whether the market has trading incentives
}

// Converted volume
type BQConvertedVolume struct {
	Btc bigquery.NullFloat64 `bigquery:"btc" json:"btc,omitempty"` //in BTC
	Eth bigquery.NullFloat64 `bigquery:"eth" json:"eth,omitempty"` //in ETH
	Usd bigquery.NullFloat64 `bigquery:"usd" json:"usd,omitempty"` //in USD
}

type BQConvertedLast struct {
	Btc bigquery.NullFloat64 `bigquery:"btc" json:"btc,omitempty"` //in BTC
	Eth bigquery.NullFloat64 `bigquery:"eth" json:"eth,omitempty"` //in ETH
	Usd bigquery.NullFloat64 `bigquery:"usd" json:"usd,omitempty"` //in USD
}

type ForbesExchange struct {
	ForbesID      *string    `json:"forbes_id"`
	Name          *string    `json:"name"`
	CoingeckoID   *string    `json:"coingecko_id"`
	CoinpaprikaID *string    `json:"coinpaprika_id"`
	LastUpdated   *time.Time `json:"last_updated"`
}

type ExchangeSubmission struct {
	ID     string `json:"id" firestore:"id" postgres:"id"`
	Name   string `json:"name" firestore:"name" postgres:"name"`
	Source string `json:"source" firestore:"source" postgres:"source"`
}
