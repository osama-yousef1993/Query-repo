package repository

import (
	"os"

	"github.com/Forbes-Media/fda-common/cloudUtils"
)

var ()

type Dao interface {
	NewExchangeQuery() ExchangeQuery
	NewAssetsQuery() AssetsQuery
}

type dao struct {
	data_namespace string
	rowy_prefix    string
	pg             cloudUtils.PostgresUtils
	fs             cloudUtils.FirestoreUtils
	bq             cloudUtils.BigqueryUtils
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
	pg := cloudUtils.NewPostgresUtils()
	fs := cloudUtils.NewFirestoreUtils("digital-assets-301018")
	bq := cloudUtils.NewBigqueryUtils("api-project-901373404215")
	pg.InitClient(pgHost, pgDBPort, pgDBUser, pgDBPassword, pgDBName, pgDBSSLMode)
	return &dao{data_namespace: os.Getenv("DATA_NAMESPACE"), rowy_prefix: os.Getenv("ROWY_PREFIX"), pg: pg, fs: fs, bq: bq}
}

func (d *dao) NewExchangeQuery() ExchangeQuery {
	return &exchangeQuery{data_namespace: d.data_namespace, rowy_prefix: d.rowy_prefix, pg: d.pg, fs: d.fs, bq: d.bq}
}

func (d *dao) NewAssetsQuery() AssetsQuery {
	return &assetsQuery{data_namespace: d.data_namespace, rowy_prefix: d.rowy_prefix, pg: d.pg, fs: d.fs, bq: d.bq}
}
