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

	"github.com/Forbes-Media/forbes-digital-assets/refactored/common/cloudUtils"
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
	NftQuery() NftQuery                                      //Queries for getting NFT asset details
	NewChartQuery() ChartQuery                               //Queries for FDA Chart
	NewNewsFeedQuery() NewsFeedQuery                         //Queries for FDA NewsFeed
	NewEditorsPickQuery() EditorsPickQuery                   //Queries for FDA EditorsPick
	NewTopicsQuery() TopicsQuery                             //Queries for FDA Topics
	GetPortfolioConfigurations() *datastruct.PortfolioConfig // fetches cached portfolio configurations
	NewEventsQuery() EventsQuery                             //Queries for FDA Events
	NewCarouselQuery() CarouselQuery                         //Queries for FDA Carousel
	NewResearchQuery() ResearchQuery                         //Queries for FDA Research
	NewDynamicDescriptionQuery() DynamicDescriptionQuery     //Queries for FDA DynamicDescription
	NewProfileQuery() ProfileQuery                           // Queries for FDA Profiles
	NewAssetsQuery() AssetsQuery                             //Queries for FDA Traded Assets
	NewTwitterQuery() TwitterQuery                           // Queries for twitter API
}

var (
	tracer  = otel.Tracer("github.com/Forbes-Media/forbes-digital-assets/store") // otel tracer
	fsUtils = cloudUtils.NewFirestoreUtils("digital-assets-301018")              // FireStore client
	bqUtils = cloudUtils.NewBigqueryUtils("api-project-901373404215")            // Bigquery client
	cSUtils = cloudUtils.NewStorageUtils()                                       // cloud Storage client
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

// returns a new search Query object
func (d *dao) NewSearchQuery() SearchQuery {
	return &searchQuery{}
}

// returns a new NFT Collection service object
func (d *dao) NftQuery() NftQuery {
	return &nftQuery{}
}

// returns a new Chart service object
func (d *dao) NewChartQuery() ChartQuery {
	return &chartQuery{}
}

// returns a new NewsFeed Query object
func (d *dao) NewNewsFeedQuery() NewsFeedQuery {
	return &newsFeedQuery{}
}

// returns a new Editors Pick Query object
func (d *dao) NewEditorsPickQuery() EditorsPickQuery {
	return &editorsPickQuery{}
}

// returns a new Topics service object
func (d *dao) NewTopicsQuery() TopicsQuery {
	return &topicsQuery{}
}

// returns a new Events Query object
func (d *dao) NewEventsQuery() EventsQuery {
	return &eventsQuery{}
}

// returns a new Editors Pick Query object
func (d *dao) NewCarouselQuery() CarouselQuery {
	return &carouselQuery{}
}

// returns a new Profile Query object
func (d *dao) NewProfileQuery() ProfileQuery {
	return &profileQuery{}
}

// returns a new DynamicDescription Query object
func (d *dao) NewDynamicDescriptionQuery() DynamicDescriptionQuery {
	return &dynamicDescriptionQuery{}
}

// returns a new research Query object
func (d *dao) NewResearchQuery() ResearchQuery {
	return &researchQuery{}
}

// returns a new Traded Assets Query object
func (d *dao) NewAssetsQuery() AssetsQuery {
	return &assetsQuery{}
}

// returns a new Traded Assets Query object
func (d *dao) NewTwitterQuery() TwitterQuery {
	return &twitterQuery{}
}
