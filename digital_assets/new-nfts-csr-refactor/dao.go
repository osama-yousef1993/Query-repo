// This package is responsible for exposing access to stored daata
//
// Repository is responsible for eposing functions that interact with the following
//   - Databases
//   - Third Party APIS
package repository

/*
	DAO - DATA ACCESS OBJECT
	Responsible for Providing access to Database Queries

*/
import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/firestore"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/common/bigqueryutils"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/common/firestoreutils"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/go-tools/log"
	"go.nhat.io/otelsql"
	"go.opentelemetry.io/otel"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

type DAO interface {
	// All Functions should end with Query
	NewWatchlistQuery() WatchlistQuery                       //Queries for WatchList functionality
	NewCommunityPageQuery() CommunityPageQuery               //Queries for CommunityPageQuery functionality
	NewPortfolioQuery() PortfolioQuery                       // Queries for Portfolio Functionality
	NewLandingPageQuery() LandingPageQuery                   // Queries for LandingPage Functionality
	NewCryptoPriceQuery() CryptoPriceQuery                   // Queries for CryptoPrice Functionality
	NewEducationQuery() EducationQuery                       // Queries for Education Functionality
	NewVideoQuery() VideoQuery                               //Queries for all Video Functionality
	NewSearchQuery() SearchQuery                             //Queries for FDA Search
	NewChartQuery() ChartQuery                               //Queries for FDA Chart
	GetPortfolioConfigurations() *datastruct.PortfolioConfig // fetches cached portfolio configurations
	NewNFTsQuery() NFTsQuery
}

var (
	firestoreClient    *firestore.Client
	firstoreClientOnce sync.Once
	BQProjectID        = "api-project-901373404215"
	mu                 sync.Mutex
	tracer             = otel.Tracer("github.com/Forbes-Media/forbes-digital-assets/store")
	fsUtils            = firestoreutils.NewFirestoreUtils("digital-assets-301018")
	bqUtils            = bigqueryutils.NewBigqueryUtils("api-project-901373404215")
)

var (
	pg           *sql.DB
	DBClientOnce sync.Once
	data_source  = os.Getenv("DATASOURCE")
)

var (
	pgHost       = os.Getenv("DB_HOST")
	pgDBPort     = os.Getenv("DB_PORT")
	pgDBUser     = os.Getenv("DB_USER")
	pgDBPassword = os.Getenv("DB_PASSWORD")
	pgDBName     = os.Getenv("DB_NAME")
	pgDBSSLMode  = os.Getenv("DB_SSLMODE")
)

// checks to verify postgres dependencies are not empty
func arePGDependenciesLoaded() bool {

	if pgHost == "" || pgDBPort == "" || pgDBUser == "" || pgDBPassword == "" || pgDBName == "" || pgDBSSLMode == "" {
		return false
	}
	return true
}

var (
	once    sync.Once
	bqStore *bigquery.Client
)

func PGConnect() *sql.DB {
	if pg == nil {

		DBClientOnce.Do(func() {

			if arePGDependenciesLoaded() == false {
				log.Critical("could not load postgres dependencies")
				return
			}
			connectionString := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s", pgHost, pgDBPort, pgDBUser, pgDBPassword, pgDBName, pgDBSSLMode)

			driverName, err := otelsql.Register("postgres",
				otelsql.TraceAll(),
				otelsql.WithDatabaseName(os.Getenv("DB_NAME")),
				otelsql.WithSystem(semconv.DBSystemPostgreSQL),
			)
			if err != nil {
				log.Error("%s", err)
				return
			}

			pg, err = sql.Open(driverName, connectionString)

			if err != nil {
				log.Error("%s", err)
				return
			}

			if err := otelsql.RecordStats(pg); err != nil {
				return
			}
			maxLifetime := 5 * time.Minute

			pg.SetConnMaxLifetime(maxLifetime)
			//pg.SetConnMaxIdleTime(maxLifetime)
			connectionError := pg.Ping()

			if connectionError != nil {
				log.Error("%s", connectionError)
				return
			}
		})
	}
	return pg

}

func PGClose() {
	if pg != nil {
		pg.Close()
	}
}

type dao struct {
	PortfolioConfigCache datastruct.PortfolioConfig // a local cache for portfolio configurations
}

// returns a dao interface
func NewDao() DAO {

	// load a local cache used during the life time of db for portfolio queries
	portfolioQuery := &portfolioQuery{}
	porfolioConfig, err := portfolioQuery.GetPortfolioConfigurations(context.Background())
	if err != nil {
		panic("could not load portfolio config")
	}
	bqStore, err = bqUtils.GetBigQueryClient()
	if err != nil {
		panic("could not load bq client")
	}

	return &dao{PortfolioConfigCache: *porfolioConfig}
}

// returns a watchlist interface
func (d *dao) NewWatchlistQuery() WatchlistQuery {
	return &watchlistQuery{}
}

// returns a community Page interface
func (d *dao) NewCommunityPageQuery() CommunityPageQuery {
	return &communityPageQuery{}
}

// returns a portfolio Query Object
func (d *dao) NewPortfolioQuery() PortfolioQuery {
	return &portfolioQuery{}
}

// returns a Landing Page Query Object
func (d *dao) NewLandingPageQuery() LandingPageQuery {
	return &landingPageQuery{}
}

// returns a Landing Page Query Object
func (d *dao) NewCryptoPriceQuery() CryptoPriceQuery {
	return &cryptoPriceQuery{}
}

// returns a Education Query Object
func (d *dao) NewEducationQuery() EducationQuery {
	return &educationQuery{}
}

// returns the local portfolio configuration cache
func (d *dao) GetPortfolioConfigurations() *datastruct.PortfolioConfig {
	return &d.PortfolioConfigCache
}

// returns the local portfolio configuration cache
func (d *dao) NewVideoQuery() VideoQuery {
	return &videoQuery{}
}

// returns a new search service object
func (d *dao) NewSearchQuery() SearchQuery {
	return &searchQuery{}
}

// returns a new Chart service object
func (d *dao) NewChartQuery() ChartQuery {
	return &chartQuery{}
}

// returns the local portfolio configuration cache
func (d *dao) NewNFTsQuery() NFTsQuery {
	return &nftsQuery{}
}
