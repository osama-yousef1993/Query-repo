package services

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Forbes-Media/go-tools/log"
	"github.com/Forbes-Media/web3-whale-tracker/common"
	"github.com/Forbes-Media/web3-whale-tracker/datastruct"
	"github.com/Forbes-Media/web3-whale-tracker/repository"
	"go.opentelemetry.io/otel/codes"
)

type TransactionsService interface {
	ReceiveTransactions(context.Context, []byte) error
}

// Object for the TransactionsService
type transactionsService struct {
	dao repository.DAO
}

// NewTransactionsService attempts to get access to all Transaction services functions
// Takes repository.DAO to access for all our query function
// Returns TransactionsService interface
//
// Takes dao and return the TransactionsService with dao to access all our Query function to get the data from BQ and PubSub.
// Returns a TransactionsService interface
func NewTransactionsService(dao repository.DAO) TransactionsService {
	return &transactionsService{dao: dao}
}

// BuildTransaction
// Takes (ctx context.Context, threshold float64)
// Return error
//
// Takes Context and threshold
// - threshold : This value we will use it to filter the transactions to see which one meets our requirement.
// Returns error if the process doesn't run successfully
func (t *transactionsService) ReceiveTransactions(ctx context.Context, pubsubMessages []byte) error {
	span, labels := common.GenerateSpan("transactionsService.ReceiveTransactions", ctx)
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "transactionsService.ReceiveTransactions"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "transactionsService.ReceiveTransactions"))
	transactionMGR := t.dao.NewTransactionsProcess()
	var message datastruct.PubSubMessage

	if err := json.Unmarshal(pubsubMessages, &message); err != nil {
		log.EndTime("transactionsService.BuildTransaction Error :", startTime, err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	// Build the transaction
	err := transactionMGR.SendTransaction(ctx, &message)

	if err != nil {
		log.EndTime("transactionsService.BuildTransaction Error :", startTime, err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	log.EndTimeL(labels, "transactionsService.ReceiveTransactions Finished Successfully", startTime, nil)
	span.SetStatus(codes.Ok, "transactionsService.ReceiveTransactions")
	return nil

}
