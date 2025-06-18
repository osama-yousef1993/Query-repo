package services

import (
	"context"
	"fmt"

	"github.com/Forbes-Media/go-tools/log"
	"github.com/Forbes-Media/web3-whale-tracker/common"
	"github.com/Forbes-Media/web3-whale-tracker/datastruct"
	"github.com/Forbes-Media/web3-whale-tracker/repository"
	"go.opentelemetry.io/otel/codes"
)

type TransactionsService interface {
	BuildTransaction(ctx context.Context) error      // Build transaction from PubSub
	GetWalletsEntities(ctx context.Context) []string // Get All Wallets entities
	GetAlertsRules(ctx context.Context) []datastruct.WhaleTrackerAlertRules
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
func (t *transactionsService) BuildTransaction(ctx context.Context) error {
	span, labels := common.GenerateSpan("transactionsService.BuildTransaction", ctx)
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "transactionsService.BuildTransaction"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "transactionsService.BuildTransaction"))
	queryMGR := t.dao.NewTransactionsQuery()

	// Get All stored Wallets from BQ
	wallets := queryMGR.GetTransactionsWallets(ctx)

	// Build the transaction
	transactions := queryMGR.GetTransactions(ctx, wallets)

	// Insert the Transaction to BQ
	err := queryMGR.InsertTransaction(ctx, transactions)

	if err != nil {
		log.EndTime("transactionsService.BuildTransaction Error :", startTime, err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	log.EndTimeL(labels, "transactionsService.BuildTransaction Finished Successfully", startTime, nil)
	span.SetStatus(codes.Ok, "transactionsService.BuildTransaction")
	return nil

}

// GetWalletsEntities
// Takes ctx context.Context
// Return []string
//
// Takes Context and returns array of wallets entities
// Returns array of string if the process run successfully
func (t *transactionsService) GetWalletsEntities(ctx context.Context) []string {
	span, labels := common.GenerateSpan("transactionsService.GetWalletsEntities", ctx)
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "transactionsService.GetWalletsEntities"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "transactionsService.GetWalletsEntities"))
	queryMGR := t.dao.NewTransactionsQuery()

	// Get All stored Wallets from BQ
	wallets := queryMGR.GetTransactionsWallets(ctx)

	log.EndTimeL(labels, "transactionsService.GetWalletsEntities Finished Successfully", startTime, nil)
	span.SetStatus(codes.Ok, "transactionsService.GetWalletsEntities")
	return wallets

}

// GetWalletsEntities
// Takes ctx context.Context
// Return []string
//
// Takes Context and returns array of wallets entities
// Returns array of string if the process run successfully
func (t *transactionsService) GetAlertsRules(ctx context.Context) []datastruct.WhaleTrackerAlertRules {
	span, labels := common.GenerateSpan("transactionsService.GetWalletsEntities", ctx)
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "transactionsService.GetWalletsEntities"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "transactionsService.GetWalletsEntities"))
	queryMGR := t.dao.NewTransactionsQuery()

	// Get All stored Wallets from BQ
	wallets, _ := queryMGR.GetAlertRules(ctx)

	log.EndTimeL(labels, "transactionsService.GetWalletsEntities Finished Successfully", startTime, nil)
	span.SetStatus(codes.Ok, "transactionsService.GetWalletsEntities")
	return wallets

}
