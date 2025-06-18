package main

import (
	"context"
	"net/http"
	"os"

	"github.com/Forbes-Media/fda-coin-paprika-ingestion/app"
	"github.com/Forbes-Media/fda-coin-paprika-ingestion/common/auth"
	"github.com/Forbes-Media/fda-coin-paprika-ingestion/repository"
	"github.com/Forbes-Media/fda-coin-paprika-ingestion/services"
	"github.com/Forbes-Media/go-tools/log"
	"github.com/gorilla/mux"
	"github.com/joho/godotenv"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gorilla/mux/otelmux"
)

var (
	err                  error
	db                   repository.DAO
	microServices        *app.Microservices
	coinListService      services.CoinListService
	exchangesListService services.ExchangesListService
	otelService          OtelService
	tagsListService      services.TagsListService
)

func init() {}

func CORSMethodMiddleware(r *mux.Router) mux.MiddlewareFunc {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.Header().Set("Access-Control-Allow-Methods", "*")
			w.Header().Set("Access-Control-Allow-Headers", "*")
			w.Header().Set("Access-Control-Allow-Origin", "*")
			if req.Method == http.MethodOptions {
				return
			}
			next.ServeHTTP(w, req)
		})
	}
}

func main() {

	err = godotenv.Load("vault/secrets/app")
	if err != nil {
		log.Emergency("Error loading .env file")
		panic(1)
	}

	GenerateServices()

	var oidcAuthMiddleware auth.OidcAuthMiddleware

	r := mux.NewRouter()

	if otelService.IsOtelEnabled() {

		tp, err := otelService.InitTracer(context.Background())
		if err != nil {
			log.Alert("%s", err)
		}

		defer func() {
			tp.ForceFlush(context.Background())

			if err := tp.Shutdown(context.Background()); err != nil {
				log.Critical("Error shutting down tracer provider: %v", err)
			}
		}()

		go otelService.InitMetrics()

		r.Use(otelmux.Middleware("github.com/Forbes-Media/forbes-digital-assets/main"))

	}
	r.Handle("/consume-coin-paprika-coins-list", oidcAuthMiddleware.Middleware(http.HandlerFunc(microServices.BuildCoinList))).Methods(http.MethodPost)
	r.Handle("/consume-coin-paprika-coins-metadata", oidcAuthMiddleware.Middleware(http.HandlerFunc(microServices.BuildCoinMetaDataList))).Methods(http.MethodPost)
	r.Handle("/consume-coin-paprika-exchanges-list", oidcAuthMiddleware.Middleware(http.HandlerFunc(microServices.BuildExchangesList))).Methods(http.MethodPost)
	r.Handle("/consume-coin-paprika-exchanges-markets", oidcAuthMiddleware.Middleware(http.HandlerFunc(microServices.BuildExchangeMarkets))).Methods(http.MethodPost)
	r.Handle("/consume-coin-paprika-tags-list", oidcAuthMiddleware.Middleware(http.HandlerFunc(microServices.BuildTagsList))).Methods(http.MethodPost)
	r.Handle("/consume-coin-paprika-ohlcv", oidcAuthMiddleware.Middleware(http.HandlerFunc(microServices.BuildCoinHistoricalOHLV))).Methods(http.MethodPost)
	r.Handle("/healthcheck", http.HandlerFunc(HealthCheck)).Methods(http.MethodGet)
	r.Use(CORSMethodMiddleware(r))

	initServer(r)

	os.Exit(0)
}

// GenerateServices initializes the necessary services for the application.
// It creates a new database connection, initializes the coin list and exchanges list services,
// and then sets up the microservices. If there is an error during the setup of the microservices,
// the function logs a critical error and exits the application.
func GenerateServices() {

	db = repository.NewDap()
	coinListService = services.NewCoinListService(db)
	exchangesListService = services.NewExchangesListService(db)
	tagsListService = services.NewTagsListService(db)
	otelService = NewOtelService()

	microServices, err = app.NewMicroservices(coinListService, exchangesListService, tagsListService)
	if err != nil {
		log.Emergency("could not load micro services")
		os.Exit(1)
	}

}

// HealthCheck is an HTTP handler function that responds with a status code of 200 (OK)
// and a plain text message "OK". It can be used to verify that the server is running
// and healthy.
func HealthCheck(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}
