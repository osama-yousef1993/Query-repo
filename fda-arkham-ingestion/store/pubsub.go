package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"
	"github.com/Forbes-Media/fda-arkham-ingestion/models"
	"github.com/Forbes-Media/go-tools/log"
)

const (
	projectId      = "digital-assets-301018"
	subscriptionID = "colorful-notion-ethereum_transactions_raw"
	topicID        = "projects/awesome-web3/topics/ethereum_transactions_raw"
)

var (
	pubsubClientOnce sync.Once
	pubsubClient     *PubsubClient
)

type PubsubClient struct {
	*pubsub.Client
}

func NewPubSubClient() (*PubsubClient, error) {
	if pubsubClient == nil {
		pubsubClientOnce.Do(func() {
			client, err := pubsub.NewClient(context.Background(), projectId)
			if err != nil {
				log.Error("%s", err)
			}
			var pubsubC PubsubClient
			pubsubC.Client = client
			pubsubClient = &pubsubC
		})
	}
	return pubsubClient, nil
}

// todo old code
// func (p *PubsubClient) GetPubSubMessages(ctx context.Context, threshold float64) []models.MessagesSchemas {
// 	endTime := time.Now().UTC()
// 	startTime := endTime.Add(-24 * time.Hour).UTC()
// 	// filter := fmt.Sprintf("attributes.timestamp >= '%s' && attributes.timestamp <= '%s'", startTime.Format(time.RFC3339), endTime.Format(time.RFC3339))
// 	// subscription, err := p.CreateSubscription(ctx, subscriptionID, pubsub.SubscriptionConfig{
// 	// 	Filter: filter,
// 	// })
// 	// if err != nil {
// 	// 	log.Error("%s", err)
// 	// }
// 	subscription := p.Subscription(subscriptionID)
// 	var messageSchemas []models.MessagesSchemas

// 	messageHandler := func(ctx context.Context, msg *pubsub.Message) {

// 		if msg.Attributes["chain"] == "ethereum" {

// 			timestampStr, ok := msg.Attributes["timestamp"]
// 			if !ok {
// 				fmt.Println("No timestamp attribute found in message")
// 				msg.Nack()
// 				return
// 			}

// 			timestampUnix, _ := strconv.ParseInt(timestampStr, 10, 64)
// 			timestamp := time.Unix(timestampUnix, 0)
// 			fmt.Println("Timestamp:", timestamp)

// 			// Filter messages within the specified time range
// 			if timestampUnix > startTime.Unix() && timestampUnix < endTime.Unix() {
// 				var messageSchema models.MessagesSchemas
// 				if err := json.Unmarshal(msg.Data, &messageSchema); err != nil {
// 					fmt.Printf("Failed to decode message: %v", err)
// 					msg.Nack()
// 					return
// 				}
// 				if messageSchema.ValueUsd >= threshold {
// 					messageSchemas = append(messageSchemas, messageSchema)
// 				}
// 				msg.Ack()
// 			}
// 		}
// 		msg.Ack()
// 	}
// 	subscription.ReceiveSettings.MaxExtension = 30 * time.Minute
// 	err := subscription.Receive(ctx, messageHandler)
// 	if err != nil {
// 		log.Error("%s", err)
// 	}

// 	return messageSchemas

// }

func (p *PubsubClient) GetPubSubMessages(ctx context.Context, threshold float64) []models.BQTransaction {
	// endTime := time.Now().UTC()
	// startTime := endTime.Add(-24 * time.Hour).UTC()
	// Assuming p.Subscription returns a *pubsub.Subscription
	subscription := p.Subscription(subscriptionID)
	var messageSchemas []models.BQTransaction
	bqs, err := NewBQStore()
	if err != nil {
		fmt.Printf("%s \n", err)
	}
	// pg := PGConnect()

	messageHandler := func(ctx context.Context, msg *pubsub.Message) {

		// if msg.Attributes["chain"] == "ethereum" {
		timestampStr, ok := msg.Attributes["timestamp"]
		if !ok {
			fmt.Println("No timestamp attribute found in message")
			msg.Nack()
			return
		}

		timestampUnix, _ := strconv.ParseInt(timestampStr, 10, 64)
		timestamp := time.Unix(timestampUnix, 0)
		fmt.Println("Timestamp:", timestamp)

		var messageSchema models.MessagesSchemas
		if err := json.Unmarshal(msg.Data, &messageSchema); err != nil {
			fmt.Printf("Failed to decode message: %v", err)
			msg.Nack()
			return
		}
		// err := pg.InsertTransactions(ctx, &messageSchema)
		// if err != nil {
		// 	fmt.Printf("Failed to insert Transaction: %v", err)
		// 	msg.Nack()
		// 	return
		// }
		// if messageSchema.ValueUsd >= threshold {
		messageTran := MapTransaction(ctx, messageSchema)
		messageSchemas = append(messageSchemas, *messageTran)
		if len(messageSchemas) >= 1000 {

			err = bqs.InsertTransactionData(ctx, messageSchemas)
			if err != nil {
				fmt.Printf("%s \n", err)
			}
			messageSchemas = []models.BQTransaction{}
		}
		// }
		msg.Ack()
		// } else {
		// 	msg.Nack()
		// }
	}
	err = subscription.Receive(ctx, messageHandler)
	if err != nil {
		log.Error("Failed to receive messages: %v", err)
	}
	return messageSchemas
}

func MapTransaction(ctx context.Context, tran models.MessagesSchemas) *models.BQTransaction {
	var decoded []models.DecodedList
	json.Unmarshal([]byte(tran.Decoded), &decoded)
	transactionResult := models.BQTransaction{
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
