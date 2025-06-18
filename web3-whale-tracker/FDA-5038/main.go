package main

import (
	"context"
	"net/http"

	"github.com/Forbes-Media/go-tools/log"
	"github.com/Forbes-Media/web3-whale-tracker/app"
	"github.com/Forbes-Media/web3-whale-tracker/repository"
	"github.com/Forbes-Media/web3-whale-tracker/services"

	"github.com/gorilla/mux"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gorilla/mux/otelmux"
)

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

var (
	db                      = repository.NewDao()
	microservices           *app.Microservices
	transactionTransactions = services.NewTransactionsService(db)
)

func main() {
	var err error
	microservices, err = app.NewMicroservices(transactionTransactions)
	if err != nil {
		log.Critical("could not load microservices")
	}
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

		r.Use(otelmux.Middleware("github.com/Forbes-Media/web3-whale-tracker/main"))
	}
	r.Use(CORSMethodMiddleware(r))
	r.HandleFunc("/consume-pubsub-messages", microservices.BuildWhaleTrackerData).Methods(http.MethodGet, http.MethodOptions)
	r.HandleFunc("/wallet-transactions", microservices.GetTransactionHistory).Methods(http.MethodGet, http.MethodOptions)

	initServer(r)

}
