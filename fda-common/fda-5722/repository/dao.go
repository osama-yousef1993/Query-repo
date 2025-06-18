package repository

import (
	"os"

	"github.com/Forbes-Media/fda-common/cloudUtils"
)

var (
	pg = cloudUtils.NewPostgresUtils()
	fs = cloudUtils.NewFirestoreUtils("digital-assets-301018")
)

type Dao interface {
	NewExchangeQuery() ExchangeQuery
	NewAssetsQuery() AssetsQuery
}

type dao struct {
}

func NewDao() Dao {
	var (
		pgHost       = os.Getenv("DB_HOST")
		pgDBPort     = os.Getenv("DB_PORT")
		pgDBUser     = os.Getenv("DB_USER")
		pgDBPassword = os.Getenv("DB_PASSWORD")
		pgDBName     = os.Getenv("DB_NAME")
		pgDBSSLMode  = os.Getenv("DB_SSLMODE")
	)
	pg.InitClient(pgHost, pgDBPort, pgDBUser, pgDBPassword, pgDBName, pgDBSSLMode)
	return &dao{}
}

func (d *dao) NewExchangeQuery() ExchangeQuery {
	return &exchangeQuery{}
}

func (d *dao) NewAssetsQuery() AssetsQuery {
	return &assetsQuery{}
}
