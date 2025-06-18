package models

import (
	"math/big"
	"time"
)

type Attribute struct {
	InsertId        string    `json:"insert_id"`
	ReceiptStatus   string    `json:"receipt_status"`
	MethodId        string    `json:"method_id"`
	Finalized       string    `json:"finalized"`
	MsgType         string    `json:"msg_type"`
	ToAddress       string    `json:"to_address"`
	BlockHash       string    `json:"block_hash"`
	FromAddress     string    `json:"from_address"`
	Timestamp       time.Time `json:"timestamp"`
	TransactionHash string    `json:"transaction_hash"`
	Signature       string    `json:"signature"`
	Chain           string    `json:"chain"`
}

type MessagesSchemas struct {
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
