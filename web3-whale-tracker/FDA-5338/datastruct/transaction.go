package datastruct

import (
	"fmt"
	"math/big"
	"os"
	"time"

	"cloud.google.com/go/bigquery"
)

var TransactionTableName = "Digital_Asset_Transactions_data"           // BQ table name
var SubscriptionID = "colorful-notion-ethereum_transactions_raw"       // PubSub Subscription ID
var TopicID = "projects/awesome-web3/topics/ethereum_transactions_raw" // PubSub Topic ID

var CollectionName = fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "whale_tracker_alert") // WhaleTracker Setting Collection Name

const (
	EthereumTXURL = "https://etherscan.io/tx/"
	EthereumScan  = "etherscan"
)

// BQTransaction This we will use it to map transaction data from BQ
type BQTransaction struct {
	ChainId                  bigquery.NullInt64     `json:"chain_id,omitempty" bigquery:"chain_id"`
	Id                       string                 `json:"id,omitempty" bigquery:"id"`
	LogIndex                 bigquery.NullInt64     `json:"log_index,omitempty" bigquery:"log_index"`
	TransactionHash          string                 `json:"transaction_hash,omitempty" bigquery:"transaction_hash"`
	TransactionIndex         bigquery.NullInt64     `json:"transaction_index,omitempty" bigquery:"transaction_index"`
	Address                  string                 `json:"address,omitempty" bigquery:"address"`
	CreatorAddress           string                 `json:"creator_address,omitempty" bigquery:"creator_address"`
	TxFromAddress            string                 `json:"tx_from_address,omitempty" bigquery:"tx_from_address"`
	Data                     string                 `json:"data,omitempty" bigquery:"data"`
	Topics                   []string               `json:"topics,omitempty" bigquery:"topics"`
	BlockTimestamp           bigquery.NullTimestamp `json:"block_timestamp,omitempty" bigquery:"block_timestamp"`
	BlockNumber              bigquery.NullInt64     `json:"block_number,omitempty" bigquery:"block_number"`
	BlockHash                string                 `json:"block_hash,omitempty" bigquery:"block_hash"`
	Signature                string                 `json:"signature,omitempty" bigquery:"signature"`
	FromAddress              string                 `json:"from_address,omitempty" bigquery:"from_address"`
	ToAddress                string                 `json:"to_address,omitempty" bigquery:"to_address"`
	ToCreatorAddress         string                 `json:"to_creator_address,omitempty" bigquery:"to_creator_address"`
	ToTxFromAddress          string                 `json:"to_tx_from_address,omitempty" bigquery:"to_tx_from_address"`
	Decoded                  []DecodedList          `json:"decoded,omitempty" bigquery:"decoded"`
	DecodedError             string                 `json:"decoded_error,omitempty" bigquery:"decoded_error"`
	IsDecoded                bigquery.NullBool      `json:"is_decoded,omitempty" bigquery:"is_decoded"`
	ValueLossless            bigquery.NullInt64     `json:"value_lossless,omitempty" bigquery:"value_lossless"`
	Value                    bigquery.NullFloat64   `json:"value,omitempty" bigquery:"value"`
	Nonce                    bigquery.NullInt64     `json:"nonce,omitempty" bigquery:"nonce"`
	Gas                      bigquery.NullInt64     `json:"gas,omitempty" bigquery:"gas"`
	GasPrice                 bigquery.NullInt64     `json:"gas_price,omitempty" bigquery:"gas_price"`
	Input                    string                 `json:"input,omitempty" bigquery:"input"`
	ReceiptCumulativeGasUsed bigquery.NullInt64     `json:"receipt_cumulative_gas_used,omitempty" bigquery:"receipt_cumulative_gas_used"`
	ReceiptGasUsed           bigquery.NullInt64     `json:"receipt_gas_used,omitempty" bigquery:"receipt_gas_used"`
	ReceiptContractAddress   string                 `json:"receipt_contract_address,omitempty" bigquery:"receipt_contract_address"`
	ReceiptRoot              string                 `json:"receipt_root,omitempty" bigquery:"receipt_root"`
	ReceiptStatus            bigquery.NullInt64     `json:"receipt_status,omitempty" bigquery:"receipt_status"`
	MaxFeePerGas             bigquery.NullInt64     `json:"max_fee_per_gas,omitempty" bigquery:"max_fee_per_gas"`
	MaxPriorityFeePerGas     bigquery.NullInt64     `json:"max_priority_fee_per_gas,omitempty" bigquery:"max_priority_fee_per_gas"`
	TransactionType          bigquery.NullInt64     `json:"transaction_type,omitempty" bigquery:"transaction_type"`
	ReceiptEffectiveGasPrice bigquery.NullInt64     `json:"receipt_effective_gas_price,omitempty" bigquery:"receipt_effective_gas_price"`
	Fee                      bigquery.NullFloat64   `json:"fee,omitempty" bigquery:"fee"`
	TxnSaving                bigquery.NullInt64     `json:"txn_saving,omitempty" bigquery:"txn_saving"`
	BurnedFee                bigquery.NullFloat64   `json:"burned_fee,omitempty" bigquery:"burned_fee"`
	MethodId                 string                 `json:"method_id,omitempty" bigquery:"method_id"`
	R                        string                 `json:"r,omitempty" bigquery:"r"`
	S                        string                 `json:"s,omitempty" bigquery:"s"`
	V                        string                 `json:"v,omitempty" bigquery:"v"`
	AccessList               []AccessList           `json:"access_list,omitempty" bigquery:"access_list"`
	ValueUsd                 bigquery.NullFloat64   `json:"value_usd,omitempty" bigquery:"value_usd"`
	FeeUsd                   bigquery.NullFloat64   `json:"fee_usd,omitempty" bigquery:"fee_usd"`
	RowLastUpdated           time.Time              `json:"row_last_updated,omitempty" bigquery:"row_last_updated"`
}

// We will use this to map the Transaction data from PubSub
type Transaction struct {
	ChainId                  int          `json:"chain_id"`
	Id                       string       `json:"id"`
	LogIndex                 int          `json:"log_index"`
	TransactionHash          string       `json:"transaction_hash"`
	TransactionIndex         int          `json:"transaction_index"`
	Address                  string       `json:"address"`
	CreatorAddress           string       `json:"creator_address"`
	TxFromAddress            string       `json:"tx_from_address"`
	Data                     string       `json:"data"`
	Topics                   []string     `json:"topics"`
	BlockTimestamp           int64        `json:"block_timestamp"`
	BlockNumber              int          `json:"block_number"`
	BlockHash                string       `json:"block_hash"`
	Signature                string       `json:"signature"`
	FromAddress              string       `json:"from_address"`
	ToAddress                string       `json:"to_address"`
	ToCreatorAddress         string       `json:"to_creator_address"`
	ToTxFromAddress          string       `json:"to_tx_from_address"`
	Decoded                  string       `json:"decoded"`
	DecodedError             string       `json:"decoded_error"`
	IsDecoded                bool         `json:"is_decoded"`
	ValueLossless            *big.Int     `json:"value_lossless"`
	Value                    float64      `json:"value"`
	Nonce                    int          `json:"nonce"`
	Gas                      int          `json:"gas"`
	GasPrice                 int          `json:"gas_price"`
	Input                    string       `json:"input"`
	ReceiptCumulativeGasUsed int          `json:"receipt_cumulative_gas_used"`
	ReceiptGasUsed           int          `json:"receipt_gas_used"`
	ReceiptContractAddress   string       `json:"receipt_contract_address"`
	ReceiptRoot              string       `json:"receipt_root"`
	ReceiptStatus            int          `json:"receipt_status"`
	MaxFeePerGas             int          `json:"max_fee_per_gas"`
	MaxPriorityFeePerGas     int          `json:"max_priority_fee_per_gas"`
	TransactionType          int          `json:"transaction_type"`
	ReceiptEffectiveGasPrice int          `json:"receipt_effective_gas_price"`
	Fee                      float64      `json:"fee"`
	TxnSaving                int          `json:"txn_saving"`
	BurnedFee                float64      `json:"burned_fee"`
	MethodId                 string       `json:"method_id"`
	R                        string       `json:"r"`
	S                        string       `json:"s"`
	V                        string       `json:"v"`
	AccessList               []AccessList `json:"access_list"`
	ValueUsd                 float64      `json:"value_usd"`
	FeeUsd                   float64      `json:"fee_usd"`
}

type AccessList struct {
	Address     string   `json:"address"`
	StorageKeys []string `json:"storage_keys"`
}

type DecodedList struct {
	Name         string  `json:"name,omitempty"`
	Type         string  `json:"type,omitempty"`
	Value        string  `json:"value,omitempty"`
	ValueFloat   float64 `json:"value_float,omitempty"`
	PriceUsd     float64 `json:"price_usd,omitempty"`
	ValueUsd     float64 `json:"value_usd,omitempty"`
	TokenAddress string  `json:"token_address,omitempty"`
	TokenSymbol  string  `json:"token_symbol,omitempty"`
}

// Get Wallets Query
var Query = `
SELECT
  DISTINCT from_address
FROM
  api-project-901373404215.digital_assets.Digital_Asset_Transactions_data_test
GROUP BY
  from_address
HAVING
  COUNT(from_address) = 1
`

type WhaleTrackerAlertRules struct {
	Entity          []WhaleTrackerRulesEntity `json:"entity" firestore:"entity"`                   // WhaleTracker Entity: ex ftx,binance etc. If this entity is receiving or sending the amount within our threshold we will send an alert
	MinUSDThreshold string                    `json:"minUsdThreshold" firestore:"minUsdThreshold"` // Should be treated as float. Is a string due to rowy limitations
	MaxUSDThreshold string                    `json:"maxUsdThreshold" firestore:"maxUsdThreshold"` // Should be treated float. Is a string due to rowy limitations
	Token           []WhaleTrackerRulesToken  `json:"token" firestore:"token"`                     // token Ex: BUSD,USDT,BTC,ETH
	Chain           []WhaleTrackerRulesChain  `json:"chain" firestore:"chain"`                     // chain Ex: arbitrum,ethereum
	IsActive        bool                      `json:"isActive" firestore:"isActive"`               // If true we will Build the alert. If not we will ignore it.
	Color           firestoreColors           `json:"colors" firestore:"colors"`                   // colorcode alerts.
}

type WhaleTrackerRulesEntity struct {
	Name string `json:"name" firestore:"name"`
}

type WhaleTrackerRulesToken struct {
	Token string `json:"token" firestore:"token_symbol"`
}

type WhaleTrackerRulesChain struct {
	Chain string `json:"chain" firestore:"chain"`
}

type firestoreColors struct {
	Hex string       `json:"hex" firestore:"hex"`
	RGB firestoreRGB `json:"rgb" firestore:"rgb"`
	HSV firestoreHSV `json:"hsv" firestore:"hsv"`
}

type firestoreRGB struct {
	R float64 `json:"r" firestore:"r"`
	G float64 `json:"g" firestore:"g"`
	B float64 `json:"b" firestore:"b"`
}

type firestoreHSV struct {
	H float64 `json:"h" firestore:"h"`
	S float64 `json:"s" firestore:"s"`
	V float64 `json:"v" firestore:"v"`
}
