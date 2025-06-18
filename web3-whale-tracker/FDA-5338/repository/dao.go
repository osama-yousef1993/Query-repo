package repository

import "github.com/Forbes-Media/web3-whale-tracker/common/cloudUtils"

type DAO interface {
	NewTransactionsQuery() TransactionsQuery // Queries for Transaction functionality
}

var (
	bqUtils     = cloudUtils.NewBigQueryUtils("api-project-901373404215") // BQ client
	pubsubUtils = cloudUtils.NewPubSubUtils("digital-assets-301018")      // PubSub Client
	fsUtils     = cloudUtils.NewFirestoreUtils("digital-assets-301018")   // Firestore Client
)

type dao struct{}

// Return DAO interface
func NewDao() DAO {
	return &dao{}
}

// Return Transaction Query Interface
func (d *dao) NewTransactionsQuery() TransactionsQuery {
	return &transactionsQuery{}
}
