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
	"go.opentelemetry.io/contrib/instrumentation/github.com/gorilla/mux/otelmux"
)

var (
	db                   = repository.NewDap()
	microServices        *app.Microservices
	coinListService      = services.NewCoinListService(db)
	exchangesListService = services.NewExchangesListService(db)
)

func init() {
	var err error
	microServices, err = app.NewMicroservices(coinListService, exchangesListService)
	if err != nil {
		log.Critical("could not load micro services")
	}
}

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
	var oidcAuthMiddleware auth.OidcAuthMiddleware

	r := mux.NewRouter()

	if otelEnabled {

		tp, err := initTracer(context.Background())
		if err != nil {
			log.Alert("%s", err)
		}

		defer func() {
			tp.ForceFlush(context.Background())

			if err := tp.Shutdown(context.Background()); err != nil {
				log.Critical("Error shutting down tracer provider: %v", err)
			}
		}()

		go initMetrics()

		r.Use(otelmux.Middleware("github.com/Forbes-Media/forbes-digital-assets/main"))

	}
	r.Handle("/consume-coin-paprika-coins-list", oidcAuthMiddleware.Middleware(http.HandlerFunc(microServices.BuildCoinList))).Methods(http.MethodPost)
	r.Handle("/consume-coin-paprika-coins-metadata", oidcAuthMiddleware.Middleware(http.HandlerFunc(microServices.BuildCoinMetaDataList))).Methods(http.MethodPost)
	r.Handle("/consume-coin-paprika-exchanges-list", oidcAuthMiddleware.Middleware(http.HandlerFunc(microServices.BuildExchangesList))).Methods(http.MethodPost)
	r.Handle("/consume-coin-paprika-exchanges-markets", oidcAuthMiddleware.Middleware(http.HandlerFunc(microServices.BuildExchangeMarkets))).Methods(http.MethodPost)
	r.Handle("/consume-coin-paprika-change-log", oidcAuthMiddleware.Middleware(http.HandlerFunc(microServices.BuildExchangeMarkets))).Methods(http.MethodPost)
	r.Use(CORSMethodMiddleware(r))

	initServer(r)

	os.Exit(0)
}
