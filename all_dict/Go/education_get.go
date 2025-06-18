//The App Package contains all of the controller / Delivery Logic for the application.
// Each function will utilize functions from various services.

package app

import (
	"github.com/Forbes-Media/forbes-digital-assets/refactored/services"
)

/*
Microservices is responsible for an entry point to all available services
*/
type Microservices struct {
	watchListService     services.WatchListService     // Provides all services that power a watchlist
	communityPageService services.CommunityPageService // Provides all services that power the Community Page
	portfolioService     services.PortfolioService     // Provides all functionality to power the portfolio page
	educationService     services.EducationService     // Provides all functionality to power the Education page
}

// Instantiates a new microservice objet, which currently only takes one microservice
// takes a watchlistService and returns a new microservice object.
// Add more services here
func NewMicroservices(
	watchListService services.WatchListService,
	communityPageService services.CommunityPageService,
	portfolioService services.PortfolioService,
	educationService services.EducationService,
) (*Microservices, error) {

	ms := Microservices{
		watchListService:     watchListService,
		communityPageService: communityPageService,
		portfolioService:     portfolioService,
		educationService:     educationService,
	}

	return &ms, nil

}






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
	"github.com/Forbes-Media/forbes-digital-assets/refactored/common/firestoreutils"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/go-tools/log"
	"go.nhat.io/otelsql"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

type DAO interface {
	// All Functions should end with Query
	NewWatchlistQuery() WatchlistQuery         //Queries for WatchList functionality
	NewCommunityPageQuery() CommunityPageQuery //Queries for CommunityPageQuery functionality
	NewPortfolioQuery() PortfolioQuery         // Queries for Portfolio Functionality
	NewEducationQuery() EducationQuery         // Queries for Education Functionality

	GetPortfolioConfigurations() *datastruct.PortfolioConfig // fetches cached portfolio configurations
}

var (
	BQProjectID = "api-project-901373404215"
	fsUtils     = firestoreutils.NewFirestoreUtils("digital-assets-301018")
)

var (
	pg           *sql.DB
	DBClientOnce sync.Once
	BQClientOnce sync.Once
	bqStore      *BQStore
)

var (
	pgHost       = os.Getenv("DB_HOST")
	pgDBPort     = os.Getenv("DB_PORT")
	pgDBUser     = os.Getenv("DB_USER")
	pgDBPassword = os.Getenv("DB_PASSWORD")
	pgDBName     = os.Getenv("DB_NAME")
	pgDBSSLMode  = os.Getenv("DB_SSLMODE")
)

type BQStore struct {
	*bigquery.Client
}

// checks to verify postgres dependencies are not empty
func arePGDependenciesLoaded() bool {

	if pgHost == "" || pgDBPort == "" || pgDBUser == "" || pgDBPassword == "" || pgDBName == "" || pgDBSSLMode == "" {
		return false
	}
	return true
}

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

// creating BQ client and sync it using sync.Once instead of creating it everyTime we call the function
func NewBQStore() (*BQStore, error) {
	if bqStore == nil {
		BQClientOnce.Do(func() {
			client, err := bigquery.NewClient(context.Background(), "api-project-901373404215")
			if err != nil {
				log.Error("%s", err)
			}
			var bqs BQStore
			bqs.Client = client
			bqStore = &bqs
		})
	}
	return bqStore, nil
}

func BQClose() {
	if bqStore != nil {
		bqStore.Close()
	}
}

type dao struct {
	PortfolioConfigCache datastruct.PortfolioConfig // a local cache for portfolio configurations
}

// returns a dao interface
func NewDao() DAO {

	// load a local cache used during the life time of db for portfolio queries
	portfolioQuery := &portfolioQuery{}
	portfolioConfig, err := portfolioQuery.GetPortfolioConfigurations(context.Background())
	if err != nil {
		panic("could not load portfolio config")
	}

	return &dao{PortfolioConfigCache: *portfolioConfig}
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

// returns a Education Query Object
func (d *dao) NewEducationQuery() EducationQuery {
	return &educationQuery{}
}

// returns the local portfolio configuration cache
func (d *dao) GetPortfolioConfigurations() *datastruct.PortfolioConfig {
	return &d.PortfolioConfigCache
}



educationService     = rfServices.NewEducationService(db)

r.Handle("/build-learn-education", oidcAuthMiddleware.Middleware(http.HandlerFunc(microservices.BuildEducation))).Methods(http.MethodPost)


// V2 Education Learn Tap
education := v2.PathPrefix("/education").Subrouter()
education.HandleFunc("/learn/", microservices.GetEducation).Methods(http.MethodGet, http.MethodOptions)
