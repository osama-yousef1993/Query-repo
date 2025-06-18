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
	BuildTransaction(ctx context.Context, threshold float64) error
	GetTransactionHistory(ctx context.Context, wallet string) ([]datastruct.MapBQTransaction, error) 
}

type transactionsService struct {
	dao repository.DAO
}

func NewTransactionsService(dao repository.DAO) TransactionsService {
	return &transactionsService{dao: dao}
}

func (t *transactionsService) BuildTransaction(ctx context.Context, threshold float64) error {
	span, labels := common.GenerateSpan("BuildTransaction", ctx)
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "BuildTransaction"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "BuildTransaction"))
	queryMGR := t.dao.NewTransactionsQuery()

	wallets := queryMGR.GetTransactionsWallets(ctx)

	transactions := queryMGR.GetTransactions(ctx, threshold, wallets)

	err := queryMGR.InsertTransaction(ctx, &transactions)

	if err != nil {
		log.EndTime("transactionsService.GetCommunityMembersInfo", startTime, err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	log.EndTimeL(labels, "BuildTransaction", startTime, nil)
	span.SetStatus(codes.Ok, "BuildTransaction")
	return nil

}

func (t *transactionsService) GetTransactionHistory(ctx context.Context, wallet string) ([]datastruct.MapBQTransaction, error) {
	span, labels := common.GenerateSpan("", ctx)
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "GetTransactionHistory"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "GetTransactionHistory"))

	transactions, err := t.dao.NewTransactionsQuery().GetTransactionHistory(ctx, wallet)
	if err != nil {
		log.EndTime("transactionsService.GetTransactionHistory", startTime, err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	log.EndTimeL(labels, "GetTransactionHistory", startTime, nil)
	span.SetStatus(codes.Ok, "GetTransactionHistory")
	return transactions, nil
}
