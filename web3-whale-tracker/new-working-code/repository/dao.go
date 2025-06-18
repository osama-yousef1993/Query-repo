package repository

import (
	"time"

	"github.com/Forbes-Media/web3-whale-tracker/common/cloudUtils"
	"github.com/patrickmn/go-cache"
)

type DAO interface {
	NewTransactionsProcess() TransactionsProcess // Queries for Transaction functionality
}

var (
	fsUtils = cloudUtils.NewFirestoreUtils("digital-assets-301018") // Firestore Client
)

type dao struct{}

// Return DAO interface
func NewDao() DAO {
	return &dao{}
}

// Return Transaction Query Interface
func (d *dao) NewTransactionsProcess() TransactionsProcess {
	// cache takes two values
	//  - defaultExpiration
	//  - cleanupInterval
	return &transactionsProcess{cache: cache.New(5*time.Minute, 10*time.Minute)}
}
