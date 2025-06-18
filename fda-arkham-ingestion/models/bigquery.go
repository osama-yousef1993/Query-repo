package models

import (
	"cloud.google.com/go/bigquery"
)

/*
A big query object that represents transfer records returned from the arkham transfer endpoint
*/
type BQArkhamTransferRecord struct {
	ArkhamID        string                 `json:"arkham_id" bigquery:"arkham_id" firestore:"arkham_id"`                      // uid provided by arkham
	TransactionHash string                 `json:"transaction_hash" bigquery:"transaction_hash" firestore:"transaction_hash"` // Unique Hash of the transaction
	ToAddress       string                 `json:"to_addr" bigquery:"to_addr" firestore:"to_addr"`                            // public address where funds were transferred to
	ToEntity        string                 `json:"to_entity" bigquery:"to_entity" firestore:"to_entity"`                      // entity name provided by arkahm. Usually an organization associated with a wallet address
	FromAddress     string                 `json:"from_addr" bigquery:"from_addr" firestore:"from_addr"`                      // public address where funds were transferred from. (sender of funds)
	FromEntity      string                 `json:"from_entity" bigquery:"from_entity" firestore:"from_entity"`                // entity name provided by arkahm. Usually an organization associated with a wallet address. (sender of funds)
	TokenSymbol     string                 `json:"token_symbol" bigquery:"token_symbol" firestore:"token_symbol"`             // symbol of token
	TokenQuantity   bigquery.NullFloat64   `json:"token_quantity" bigquery:"token_quantity" firestore:"token_quantity"`       // quantity of tokens in transaction
	Chain           string                 `json:"chain" bigquery:"chain" firestore:"chain"`                                  // the chain that the transaction occurred on
	TotalPrice      bigquery.NullFloat64   `json:"total_price" bigquery:"total_price" firestore:"total_price"`                // total value that was transfered in USD
	BlockTimestamp  bigquery.NullTimestamp `json:"block_timestamp" bigquery:"block_timestamp" firestore:"block_timestamp"`    // time the block was created
	BlockHash       string                 `json:"block_hash" bigquery:"block_hash" firestore:"block_hash"`                   // hash of the block
	InsertTimestamp bigquery.NullTimestamp `json:"insert_timestamp" bigquery:"insert_timestamp" firestore:"insert_timestamp"` // time the block was created
}

type BQGetChainResult struct {
	Chain string `json:"chain" bigquery:"chain"`
}

type BQGetTokenResult struct {
	Token string `json:"token_symbol" bigquery:"token_symbol"`
	Count int    `json:"count" bigquery:"count"`
}

type BQArkhamPortfolioEntry struct {
	Arkham_Entity   string                 `json:"arkham_entity" bigquery:"arkham_entity" firestore:"arkham_entity"`
	Chain           string                 `json:"chain" bigquery:"chain" firestore:"chain"`
	TokenId         string                 `json:"token_id" bigquery:"token_id" firestore:"token_id"`
	TokenName       string                 `json:"token_name" bigquery:"token_name" firestore:"token_name"`
	TokenSymbol     string                 `json:"token_symbol" bigquery:"token_symbol" firestore:"token_symbol"`
	Price           bigquery.NullFloat64   `json:"price" bigquery:"price" firestore:"price"`
	TotalUSD        bigquery.NullFloat64   `json:"total_usd" bigquery:"total_usd" firestore:"total_usd"`
	InsertTimestamp bigquery.NullTimestamp `json:"insert_time" bigquery:"insert_time" firestore:"insert_time"` // time the block was created

}

type BQTransaction struct {
	ChainId                  bigquery.NullInt64     `json:"chain_id" bigquery:"chain_id"`
	Id                       string                 `json:"id" bigquery:"id"`
	LogIndex                 bigquery.NullInt64     `json:"log_index" bigquery:"log_index"`
	TransactionHash          string                 `json:"transaction_hash" bigquery:"transaction_hash"`
	TransactionIndex         bigquery.NullInt64     `json:"transaction_index" bigquery:"transaction_index"`
	Address                  string                 `json:"address" bigquery:"address"`
	CreatorAddress           string                 `json:"creator_address" bigquery:"creator_address"`
	TxFromAddress            string                 `json:"tx_from_address" bigquery:"tx_from_address"`
	Data                     string                 `json:"data" bigquery:"data"`
	Topics                   []string               `json:"topics" bigquery:"topics"`
	BlockTimestamp           bigquery.NullTimestamp `json:"block_timestamp" bigquery:"block_timestamp"`
	BlockNumber              bigquery.NullInt64     `json:"block_number" bigquery:"block_number"`
	BlockHash                string                 `json:"block_hash" bigquery:"block_hash"`
	Signature                string                 `json:"signature" bigquery:"signature"`
	FromAddress              string                 `json:"from_address" bigquery:"from_address"`
	ToAddress                string                 `json:"to_address" bigquery:"to_address"`
	ToCreatorAddress         string                 `json:"to_creator_address" bigquery:"to_creator_address"`
	ToTxFromAddress          string                 `json:"to_tx_from_address" bigquery:"to_tx_from_address"`
	Decoded                  []DecodedList          `json:"decoded" bigquery:"decoded"`
	DecodedError             string                 `json:"decoded_error" bigquery:"decoded_error"`
	IsDecoded                bigquery.NullBool      `json:"is_decoded" bigquery:"is_decoded"`
	ValueLossless            bigquery.NullInt64     `json:"value_lossless" bigquery:"value_lossless"`
	Value                    bigquery.NullFloat64   `json:"value" bigquery:"value"`
	Nonce                    bigquery.NullInt64     `json:"nonce" bigquery:"nonce"`
	Gas                      bigquery.NullInt64     `json:"gas" bigquery:"gas"`
	GasPrice                 bigquery.NullInt64     `json:"gas_price" bigquery:"gas_price"`
	Input                    string                 `json:"input" bigquery:"input"`
	ReceiptCumulativeGasUsed bigquery.NullInt64     `json:"receipt_cumulative_gas_used" bigquery:"receipt_cumulative_gas_used"`
	ReceiptGasUsed           bigquery.NullInt64     `json:"receipt_gas_used" bigquery:"receipt_gas_used"`
	ReceiptContractAddress   string                 `json:"receipt_contract_address" bigquery:"receipt_contract_address"`
	ReceiptRoot              string                 `json:"receipt_root" bigquery:"receipt_root"`
	ReceiptStatus            bigquery.NullInt64     `json:"receipt_status" bigquery:"receipt_status"`
	MaxFeePerGas             bigquery.NullInt64     `json:"max_fee_per_gas" bigquery:"max_fee_per_gas"`
	MaxPriorityFeePerGas     bigquery.NullInt64     `json:"max_priority_fee_per_gas" bigquery:"max_priority_fee_per_gas"`
	TransactionType          bigquery.NullInt64     `json:"transaction_type" bigquery:"transaction_type"`
	ReceiptEffectiveGasPrice bigquery.NullInt64     `json:"receipt_effective_gas_price" bigquery:"receipt_effective_gas_price"`
	Fee                      bigquery.NullFloat64   `json:"fee" bigquery:"fee"`
	TxnSaving                bigquery.NullInt64     `json:"txn_saving" bigquery:"txn_saving"`
	BurnedFee                bigquery.NullFloat64   `json:"burned_fee" bigquery:"burned_fee"`
	MethodId                 string                 `json:"method_id" bigquery:"method_id"`
	R                        string                 `json:"r" bigquery:"r"`
	S                        string                 `json:"s" bigquery:"s"`
	V                        string                 `json:"v" bigquery:"v"`
	AccessList               []AccessList           `json:"access_list" bigquery:"access_list"`
	ValueUsd                 bigquery.NullFloat64   `json:"value_usd" bigquery:"value_usd"`
	FeeUsd                   bigquery.NullFloat64   `json:"fee_usd" bigquery:"fee_usd"`
}
