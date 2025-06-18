package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"
	"github.com/Forbes-Media/go-tools/log"
	"github.com/Forbes-Media/web3-whale-tracker/common"
	"github.com/Forbes-Media/web3-whale-tracker/datastruct"
	"go.opentelemetry.io/otel/codes"
	"google.golang.org/api/iterator"
)

type TransactionsQuery interface {
	InsertTransaction(ctx context.Context, transactions *[]datastruct.BQTransaction) error
	GetTransactions(ctx context.Context, threshold float64, wallets []string) []datastruct.BQTransaction
	GetTransactionsWallets(ctx context.Context) []string
	GetTransactionHistory(ctx context.Context, wallet string) ([]datastruct.MapBQTransaction, error)
}

type transactionsQuery struct{}

func (t *transactionsQuery) InsertTransaction(ctx context.Context, transactions *[]datastruct.BQTransaction) error {
	span, labels := common.GenerateSpan("InsertTransaction", ctx)
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "InsertTransaction"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "InsertTransaction"))

	client, err := bqUtils.GetBigQueryClient()
	if err != nil {
		log.ErrorL(labels, "")
	}

	bqInserter := client.Dataset("digital_assets").Table(datastruct.TransactionTableName).Inserter()
	bqInserter.IgnoreUnknownValues = true
	inserterErr := bqInserter.Put(ctx, *transactions)
	var retryError error
	if inserterErr != nil {
		if strings.Contains(inserterErr.Error(), "413") {
			log.Info("413 Error. Breaking up tickers and retrying insert")
			l := len(*transactions)
			var ticks []datastruct.BQTransaction
			ticks = append(ticks, *transactions...)
			for y := (l / 3); y < l; y += (l / 3) {
				a := ticks[y-(l/3) : y]
				er := t.InsertTransaction(ctx, &a)
				if er != nil {
					retryError = er
				}
			}
			//If we couldnt recover return the error
			return retryError
		}
		//if not a 413 error return the error
		return inserterErr
	}
	log.EndTimeL(labels, "InsertTransaction", startTime, nil)
	span.SetStatus(codes.Ok, "InsertTransaction")
	return nil
}

func (t *transactionsQuery) GetTransactions(ctx context.Context, threshold float64, wallets []string) []datastruct.BQTransaction {
	span, labels := common.GenerateSpan("GetTransactions", ctx)
	defer span.End()

	var (
		transactions      []datastruct.BQTransaction
		newWallets        []string
		transactionSended []string
	)

	span.AddEvent(fmt.Sprintf("Starting %s", "GetTransactions"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "GetTransactions"))
	client, err := pubsubUtils.GetPubSubClient()
	if err != nil {
		log.ErrorL(labels, "")
		return transactions
	}
	subscription := client.Subscription(datastruct.SubscriptionID)
	messageHandler := func(ctx context.Context, msg *pubsub.Message) {
		var transaction datastruct.Transaction

		if err := json.Unmarshal(msg.Data, &transaction); err != nil {
			log.ErrorL(labels, "Failed to decode message: %s", err)
			msg.Nack()
			return
		}
		transactionRes := t.ConvertTransactionToBQ(ctx, transaction)
		if transaction.Value >= threshold {
			transactions = append(transactions, *transactionRes)
			newWallets = append(newWallets, transaction.FromAddress)
			if !slices.Contains(transactionSended, transaction.TransactionHash) {
				transactionSended = append(transactionSended, transaction.TransactionHash)
				// common.SendSlack(transaction)
			}
		} else if slices.Contains(wallets, transaction.FromAddress) { // we don't send it to slack
			log.Info("send message to slack")
			transactions = append(transactions, *transactionRes)
			// common.SendSlack(transaction)
			// send to slack
		}
		msg.Ack()
	}

	err = subscription.Receive(ctx, messageHandler)
	if err != nil {
		log.ErrorL(labels, "Failed to decode message: %s", err)
		return transactions
	}

	log.EndTimeL(labels, "GetTransactions", startTime, nil)
	span.SetStatus(codes.Ok, "GetTransactions")

	return transactions
}

func (t *transactionsQuery) ConvertTransactionToBQ(ctx context.Context, tran datastruct.Transaction) *datastruct.BQTransaction {
	var decoded []datastruct.DecodedList
	json.Unmarshal([]byte(tran.Decoded), &decoded)
	transactionResult := datastruct.BQTransaction{
		ChainId:                  bigquery.NullInt64{Int64: int64(tran.ChainId), Valid: true},
		Id:                       tran.Id,
		LogIndex:                 bigquery.NullInt64{Int64: int64(tran.LogIndex), Valid: true},
		TransactionHash:          tran.TransactionHash,
		TransactionIndex:         bigquery.NullInt64{Int64: int64(tran.TransactionIndex), Valid: true},
		Address:                  tran.Address,
		CreatorAddress:           tran.CreatorAddress,
		TxFromAddress:            tran.TxFromAddress,
		Data:                     tran.Data,
		Topics:                   tran.Topics,
		BlockTimestamp:           bigquery.NullTimestamp{Timestamp: time.Unix(tran.BlockTimestamp, 0).UTC(), Valid: true},
		BlockNumber:              bigquery.NullInt64{Int64: int64(tran.BlockNumber), Valid: true},
		BlockHash:                tran.BlockHash,
		Signature:                tran.Signature,
		FromAddress:              tran.FromAddress,
		ToAddress:                tran.ToAddress,
		ToCreatorAddress:         tran.ToCreatorAddress,
		ToTxFromAddress:          tran.ToTxFromAddress,
		Decoded:                  decoded,
		DecodedError:             tran.DecodedError,
		IsDecoded:                bigquery.NullBool{Bool: tran.IsDecoded, Valid: true},
		ValueLossless:            bigquery.NullInt64{Int64: int64(tran.ValueLossless.Int64()), Valid: true},
		Value:                    bigquery.NullFloat64{Float64: tran.Value, Valid: true},
		Nonce:                    bigquery.NullInt64{Int64: int64(tran.Nonce), Valid: true},
		Gas:                      bigquery.NullInt64{Int64: int64(tran.Gas), Valid: true},
		GasPrice:                 bigquery.NullInt64{Int64: int64(tran.GasPrice), Valid: true},
		Input:                    tran.Input,
		ReceiptCumulativeGasUsed: bigquery.NullInt64{Int64: int64(tran.ReceiptCumulativeGasUsed), Valid: true},
		ReceiptGasUsed:           bigquery.NullInt64{Int64: int64(tran.ReceiptGasUsed), Valid: true},
		ReceiptContractAddress:   tran.ReceiptContractAddress,
		ReceiptRoot:              tran.ReceiptRoot,
		ReceiptStatus:            bigquery.NullInt64{Int64: int64(tran.ReceiptStatus), Valid: true},
		MaxFeePerGas:             bigquery.NullInt64{Int64: int64(tran.MaxFeePerGas), Valid: true},
		MaxPriorityFeePerGas:     bigquery.NullInt64{Int64: int64(tran.MaxPriorityFeePerGas), Valid: true},
		TransactionType:          bigquery.NullInt64{Int64: int64(tran.TransactionType), Valid: true},
		ReceiptEffectiveGasPrice: bigquery.NullInt64{Int64: int64(tran.ReceiptEffectiveGasPrice), Valid: true},
		Fee:                      bigquery.NullFloat64{Float64: tran.Fee, Valid: true},
		TxnSaving:                bigquery.NullInt64{Int64: int64(tran.TxnSaving), Valid: true},
		BurnedFee:                bigquery.NullFloat64{Float64: tran.BurnedFee, Valid: true},
		MethodId:                 tran.MethodId,
		R:                        tran.R,
		S:                        tran.S,
		V:                        tran.V,
		AccessList:               tran.AccessList,
		ValueUsd:                 bigquery.NullFloat64{Float64: tran.ValueUsd, Valid: true},
		FeeUsd:                   bigquery.NullFloat64{Float64: tran.FeeUsd, Valid: true},
	}

	return &transactionResult
}

func (t *transactionsQuery) GetTransactionsWallets(ctx context.Context) []string {
	span, labels := common.GenerateSpan("GetTransactionsWallets", ctx)
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "GetTransactionsWallets"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "GetTransactionsWallets"))

	client, err := bqUtils.GetBigQueryClient()
	if err != nil {
		log.ErrorL(labels, "GetTransactionsWallets Error %s", err)
		span.SetStatus(codes.Error, err.Error())
		return nil
	}

	queryResult := client.Query(datastruct.Query)

	it, err := queryResult.Read(ctx)
	if err != nil {
		log.ErrorL(labels, "GetTransactionsWallets Error %s", err)
		span.SetStatus(codes.Error, err.Error())
		return nil
	}
	var wallets []string
	for {
		var transaction datastruct.BQTransaction
		err := it.Next(&transaction)
		if err == iterator.Done {
			break
		}

		if err != nil {
			log.ErrorL(labels, "GetTransactionsWallets Error %s", err)
			span.SetStatus(codes.Error, err.Error())
			return nil
		}
		wallets = append(wallets, transaction.FromAddress)

	}

	log.EndTimeL(labels, "GetTransactionsWallets", startTime, nil)
	span.SetStatus(codes.Ok, "GetTransactionsWallets")
	return wallets
}

func (t *transactionsQuery) GetTransactionHistory(ctx context.Context, wallet string) ([]datastruct.MapBQTransaction, error) {
	span, labels := common.GenerateSpan("GetTransactionHistory", ctx)
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "GetTransactionHistory"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "GetTransactionHistory"))

	client, err := bqUtils.GetBigQueryClient()
	if err != nil {
		log.ErrorL(labels, "GetTransactionHistory Error %s", err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	query := fmt.Sprintf(datastruct.TransactionQuery, strings.ToLower(wallet))
	queryResult := client.Query(query)

	it, err := queryResult.Read(ctx)
	if err != nil {
		log.ErrorL(labels, "GetTransactionHistory Error %s", err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	var transactions []datastruct.MapBQTransaction

	for {
		var tran map[string]bigquery.Value
		err := it.Next(&tran)
		if err == iterator.Done {
			break
		}

		if err != nil {
			log.ErrorL(labels, "GetTransactionHistory Error %s", err)
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}
		txn := Map(tran)
		transactions = append(transactions, txn)
	}

	log.EndTimeL(labels, "GetTransactionHistory", startTime, nil)
	span.SetStatus(codes.Ok, "GetTransactionHistory")
	return transactions, nil
}

func Map(tran map[string]bigquery.Value) datastruct.MapBQTransaction {
	var txn datastruct.MapBQTransaction
	txn.ChainId = tran["chain_id"].(bigquery.NullInt64)
	txn.Id = tran["id"].(string)
	txn.LogIndex = tran["log_index"].(bigquery.NullInt64)
	txn.TransactionHash = tran["transaction_hash"].(string)
	txn.TransactionIndex = tran["transaction_index"].(bigquery.NullInt64)
	txn.Address = tran["address"].(string)
	txn.CreatorAddress = tran["creator_address"].(string)
	txn.TxFromAddress = tran["tx_from_address"].(string)
	txn.Data = tran["data"].(string)
	txn.Topics = tran["topics"].([]string)
	txn.BlockTimestamp = tran["block_timestamp"].(bigquery.NullTimestamp)
	txn.BlockNumber = tran["block_number"].(bigquery.NullInt64)
	txn.BlockHash = tran["block_hash"].(string)
	txn.Signature = tran["signature"].(string)
	txn.FromAddress = tran["from_address"].(string)
	txn.ToAddress = tran["to_address"].(string)
	txn.ToCreatorAddress = tran["to_creator_address"].(string)
	txn.ToTxFromAddress = tran["to_tx_from_address"].(string)
	// Assuming that tran["decoded"] is of type []DecodedList and needs to be handled similarly
	// txn.Decoded = tran["decoded"].([]DecodedList)
	txn.DecodedError = tran["decoded_error"].(string)
	txn.IsDecoded = tran["is_decoded"].(bigquery.NullBool)
	// Handle BigQueryDecimal fields
	txn.ValueLossless = tran["value_lossless"].(bigquery.NullInt64)
	txn.Value.Scan(tran["value"])
	txn.Nonce = tran["nonce"].(bigquery.NullInt64)
	txn.Gas = tran["gas"].(bigquery.NullInt64)
	txn.GasPrice = tran["gas_price"].(bigquery.NullInt64)
	txn.Input = tran["input"].(string)
	txn.ReceiptCumulativeGasUsed = tran["receipt_cumulative_gas_used"].(bigquery.NullInt64)
	txn.ReceiptGasUsed = tran["receipt_gas_used"].(bigquery.NullInt64)
	txn.ReceiptContractAddress = tran["receipt_contract_address"].(string)
	txn.ReceiptRoot = tran["receipt_root"].(string)
	txn.ReceiptStatus = tran["receipt_status"].(bigquery.NullInt64)
	txn.TransactionType = tran["transaction_type"].(bigquery.NullInt64)
	txn.Fee = tran["fee"].(bigquery.NullFloat64)
	txn.BurnedFee = tran["burned_fee"].(bigquery.NullFloat64)
	txn.MethodId = tran["method_id"].(string)
	txn.R = tran["r"].(string)
	txn.S = tran["s"].(string)
	txn.V = tran["v"].(string)
	// Assuming that tran["access_list"] is of type []AccessList and needs to be handled similarly
	// txn.AccessList = tran["access_list"].([]AccessList)
	txn.ValueUsd = tran["value_usd"].(bigquery.NullFloat64)
	txn.FeeUsd = tran["fee_usd"].(bigquery.NullFloat64)

	return txn
}
