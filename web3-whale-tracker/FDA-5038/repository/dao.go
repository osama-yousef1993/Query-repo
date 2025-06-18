package repository

import "github.com/Forbes-Media/web3-whale-tracker/common/cloudUtils"

type DAO interface {
	NewTransactionsQuery() TransactionsQuery
}

var (
	bqUtils     = cloudUtils.NewBigQueryUtils("api-project-901373404215")
	pubsubUtils = cloudUtils.NewPubSubUtils("digital-assets-301018")
)

type dao struct {
}

func NewDao() DAO {
	return &dao{}
}

func (d *dao) NewTransactionsQuery() TransactionsQuery {
	return &transactionsQuery{}
}
