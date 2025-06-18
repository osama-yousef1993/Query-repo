package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
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
	InsertTransaction(ctx context.Context, transactions []datastruct.BQTransaction) error // Insert Transactions to BQ
	GetTransactions(ctx context.Context, wallets []string) []datastruct.BQTransaction     // Get Transactions from PubSub
	GetTransactionsWallets(ctx context.Context) []string                                  // Get Wallet addresses from BQ
	GetAlertRules(ctx context.Context) ([]datastruct.WhaleTrackerAlertRules, error)       // Get Alert Rules from FS
}

type transactionsQuery struct{}

// InsertTransaction
// Takes (ctx context.Context, transactions *[]datastruct.BQTransaction)
// Returns error
//
// Takes Context and array of Transactions then Insert it to BQ
// Returns error if the process doesn't build successfully.
func (t *transactionsQuery) InsertTransaction(ctx context.Context, transactions []datastruct.BQTransaction) error {
	span, labels := common.GenerateSpan("transactionsQuery.InsertTransaction", ctx)
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "transactionsQuery.InsertTransaction"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "transactionsQuery.InsertTransaction"))

	client, err := bqUtils.GetBigQueryClient()
	if err != nil {
		log.ErrorL(labels, "Error transactionsQuery.InsertTransaction BQ connection %s", err)
	}
	defer client.Close()

	table := t.GetTableName(datastruct.TransactionTableName)

	bqInserter := client.Dataset("digital_assets").Table(table).Inserter()
	bqInserter.IgnoreUnknownValues = true
	inserterErr := bqInserter.Put(ctx, transactions)
	var retryError error
	if inserterErr != nil {
		if strings.Contains(inserterErr.Error(), "413") {
			log.Info("413 Error. Breaking up tickers and retrying insert")
			l := len(transactions)
			var ticks []datastruct.BQTransaction
			ticks = append(ticks, transactions...)
			for y := (l / 3); y < l; y += (l / 3) {
				a := ticks[y-(l/3) : y]
				er := t.InsertTransaction(ctx, a)
				if er != nil {
					retryError = er
				}
			}
			//If we couldn't recover return the error
			return retryError
		}
		//if not a 413 error return the error
		return inserterErr
	}
	log.EndTimeL(labels, "transactionsQuery.InsertTransaction", startTime, nil)
	span.SetStatus(codes.Ok, "transactionsQuery.InsertTransaction")
	return nil
}

// GetTransactions
// Takes (ctx context.Context, threshold float64, wallets []string)
// - threshold : Determine the Amount for Transaction we need to check
// - wallets : array of string that contains all wallets stores in BQ
// Returns []datastruct.BQTransaction
//
// GetTransactions It will fetch the messages from PubSub then check the threshold if it meets the requirement the Transaction will added to Transaction array and send the Transaction to Slack channel.
// Returns []datastruct.BQTransaction
func (t *transactionsQuery) GetTransactions(ctx context.Context, wallets []string) []datastruct.BQTransaction {
	span, labels := common.GenerateSpan("transactionsQuery.GetTransactions", ctx)
	defer span.End()

	var (
		transactions      []datastruct.BQTransaction
		walletTransaction []datastruct.BQTransaction
		uniqueAddress     = make(map[string]bool)
		mu                sync.Mutex
	)

	span.AddEvent(fmt.Sprintf("Starting %s", "transactionsQuery.GetTransactions"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "transactionsQuery.GetTransactions"))
	client, err := pubsubUtils.GetPubSubClient()
	if err != nil {
		log.ErrorL(labels, "")
		return transactions
	}

	rules, err := t.GetAlertRules(ctx)
	if err != nil {
		log.ErrorL(labels, "")
		span.SetStatus(codes.Error, "Error getting rules from FS")
	}

	subscription := client.Subscription(datastruct.SubscriptionID)
	messageHandler := func(ctx context.Context, msg *pubsub.Message) {
		var transaction datastruct.EthereumTransactionRow

		if err := json.Unmarshal(msg.Data, &transaction); err != nil {
			log.ErrorL(labels, "Failed to decode message: %s", err)
			msg.Nack()
			return
		}

		transactionRes := t.ConvertTransactionToBQ(ctx, transaction)
		var (
			min float64
			max float64
		)
		for _, rule := range rules {
			if rule.Entity != nil {
				if rule.Entity[0] == transaction.FromAddress {
					min, max = t.GetMinMaxValue(ctx, rule.MinUSDThreshold, rule.MaxUSDThreshold)
					goto Process
				}
			} else {
				min, max = t.GetMinMaxValue(ctx, rule.MinUSDThreshold, rule.MaxUSDThreshold)
				goto Process
			}
		Process:
			if transaction.ValueUsd >= min && transaction.ValueUsd <= max {
				// Send the transaction to slack channel
				// todo add queue to add messages and send it to slack to build the slack table
				common.SendSlack(transaction, rule.Color.Hex)
				mu.Lock()
				exist := uniqueAddress[transactionRes.TransactionHash]
				if !exist {
					uniqueAddress[transactionRes.TransactionHash] = true
					transactions = append(transactions, *transactionRes)
					if len(transactions) >= 1000 {
						err := t.InsertTransaction(ctx, transactions)
						if err != nil {
							log.ErrorL(labels, "Failed to Insert Transactions To BQ: %s", err)
						}
						transactions = []datastruct.BQTransaction{}
					}
					mu.Unlock()
				} else {
					mu.Unlock()
				}
			} else if slices.Contains(wallets, transactionRes.FromAddress) {
				// This process for other transactions that exist in our list and doesn't match the Threshold
				mu.Lock()
				exist := uniqueAddress[transactionRes.TransactionHash]
				if !exist {
					uniqueAddress[transactionRes.TransactionHash] = true
					walletTransaction = append(walletTransaction, *transactionRes)
					if len(walletTransaction) >= 1000 {
						err := t.InsertTransaction(ctx, walletTransaction)
						if err != nil {
							log.ErrorL(labels, "Failed to Insert Transactions To BQ: %s", err)
						}
						walletTransaction = []datastruct.BQTransaction{}
					}
					mu.Unlock()
				} else {
					mu.Unlock()
				}
			}
		}
		msg.Ack()
	}

	err = subscription.Receive(ctx, messageHandler)
	if err != nil {
		log.ErrorL(labels, "Failed to decode message: %s", err)
		return transactions
	}

	log.EndTimeL(labels, "transactionsQuery.GetTransactions", startTime, nil)
	span.SetStatus(codes.Ok, "transactionsQuery.GetTransactions")

	return transactions
}

// ConvertTransactionToBQ
// Takes (ctx context.Context, tran datastruct.Transaction)
// Returns *datastruct.BQTransaction
//
// Map Transaction object to BQ object so the can Inserted to BQ table.
// Returns datastruct.BQTransaction
func (t *transactionsQuery) ConvertTransactionToBQ(ctx context.Context, tran datastruct.EthereumTransactionRow) *datastruct.BQTransaction {
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
		RowLastUpdated:           time.Now().UTC(),
	}

	return &transactionResult
}

// GetTransactionsWallets
// Takes Context
// Returns array of Wallets
//
// Get Wallets value from BQ and build the Array
// Returns Array of wallet and nil if error occur.
func (t *transactionsQuery) GetTransactionsWallets(ctx context.Context) []string {
	span, labels := common.GenerateSpan("transactionsQuery.GetTransactionsWallets", ctx)
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "transactionsQuery.GetTransactionsWallets"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "transactionsQuery.GetTransactionsWallets"))

	client, err := bqUtils.GetBigQueryClient()
	if err != nil {
		log.ErrorL(labels, "transactionsQuery.GetTransactionsWallets Error %s", err)
		span.SetStatus(codes.Error, err.Error())
		return nil
	}

	queryResult := client.Query(datastruct.Query)

	it, err := queryResult.Read(ctx)
	if err != nil {
		log.ErrorL(labels, "transactionsQuery.GetTransactionsWallets Error %s", err)
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
			log.ErrorL(labels, "transactionsQuery.GetTransactionsWallets Error %s", err)
			span.SetStatus(codes.Error, err.Error())
			return nil
		}
		wallets = append(wallets, transaction.FromAddress)

	}

	log.EndTimeL(labels, "transactionsQuery.GetTransactionsWallets", startTime, nil)
	span.SetStatus(codes.Ok, "transactionsQuery.GetTransactionsWallets")
	return wallets
}

// GetTableName
// Takes table name
// Returns Table name with DATA_NAMESPACE to get the correct location for data
//
// It will take table name and concat it with DATA_NAMESPACE to determine the data source from dev/prd
// Returns the source to insert the data into it.
func (t *transactionsQuery) GetTableName(tableName string) string {
	if os.Getenv("DATA_NAMESPACE") == "_dev" {
		return fmt.Sprintf("%s%s", tableName, os.Getenv("DATA_NAMESPACE"))
	}
	return tableName
}

// Get the alert rules
// GetAlertRules
// Takes Context
// Returns ([]datastruct.WhaleTrackerAlertRules, error)
//
// Get Alert Rules value from FS
// Returns Alert Rules and no err if successfully.
func (t *transactionsQuery) GetAlertRules(ctx context.Context) ([]datastruct.WhaleTrackerAlertRules, error) {
	span, labels := common.GenerateSpan("transactionsQuery.GetAlertRules", ctx)
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "transactionsQuery.GetAlertRules"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "transactionsQuery.GetAlertRules"))
	fs, err := fsUtils.GetFirestoreClient()
	if err != nil {
		log.ErrorL(labels, "transactionsQuery.GetAlertRules Error %s", err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	var rules []datastruct.WhaleTrackerAlertRules

	iter := fs.Collection(datastruct.CollectionName).Documents(ctx)

	for {
		doc, err := iter.Next()
		var rule datastruct.WhaleTrackerAlertRules

		if err == iterator.Done {
			break
		} else if err != nil {
			log.ErrorL(labels, "transactionsQuery.GetAlertRules Error %s", err)
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		} else {
			err = doc.DataTo(&rule)
			if err != nil {
				log.ErrorL(labels, "transactionsQuery.GetAlertRules Error %s", err)
				span.SetStatus(codes.Error, err.Error())
				return nil, err
			}
			if rule.IsActive {
				rules = append(rules, rule)
			}
		}
	}

	log.EndTimeL(labels, "transactionsQuery.GetAlertRules", startTime, nil)
	span.SetStatus(codes.Ok, "transactionsQuery.GetAlertRules")
	return rules, nil
}

// Get the Min and Max threshold value
// GetMinMaxValue
// Takes (ctx context.Context, MinUSDThreshold, MaxUSDThreshold string)
// Returns (float64, float64)
//
// Extract the Min and Max threshold value from Rule
// Returns Min and Max value After convert it from string to float64.
func (t *transactionsQuery) GetMinMaxValue(ctx context.Context, MinUSDThreshold, MaxUSDThreshold string) (float64, float64) {
	span, labels := common.GenerateSpan("transactionsQuery.GetMinMaxValue", ctx)
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "transactionsQuery.GetMinMaxValue"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "transactionsQuery.GetMinMaxValue"))

	var (
		min float64 = 1.0
		max float64 = 10000000.0
		err error
	)

	min, err = strconv.ParseFloat(strings.ReplaceAll(MinUSDThreshold, ",", ""), 64)
	if err != nil {
		log.Error("Error transactionsQuery.GetMinMaxValue Could Not parse min value setting defaulting the value to 1.0 %s", err)
		span.SetStatus(codes.Error, err.Error())
	}
	max, err = strconv.ParseFloat(strings.ReplaceAll(MaxUSDThreshold, ",", ""), 64)
	if err != nil {
		log.Error("Error transactionsQuery.GetMinMaxValue Could Not parse max value setting defaulting the value to 10000000.0 %s", err)
		span.SetStatus(codes.Error, err.Error())
	}

	log.EndTimeL(labels, "transactionsQuery.GetMinMaxValue", startTime, nil)
	span.SetStatus(codes.Ok, "transactionsQuery.GetMinMaxValue")
	return min, max
}
