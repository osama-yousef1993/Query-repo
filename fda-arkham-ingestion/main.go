package main

import (
	"context"
	"net/http"
	"os"

	"github.com/Forbes-Media/fda-arkham-ingestion/internal"
	"github.com/Forbes-Media/go-tools/log"
	"github.com/gorilla/mux"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gorilla/mux/otelmux"
	"go.opentelemetry.io/otel"
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

// tracer is only used when adding new spans to the trace in this main package
// Used only for not HTTP starts
var tracer = otel.Tracer("github.com/Forbes-Media/fda-arkham-ingestion/main")

func main() {
	//var oidcAuthMiddleware auth.OidcAuthMiddleware
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

		r.Use(otelmux.Middleware("github.com/Forbes-Media/fda-arkham-ingestion/main"))
	}
	r.Handle("/consume-arkham-transfers", http.HandlerFunc(internal.RunConsumeArkhamTransferEvents)).Methods(http.MethodPost)
	r.Handle("/consume-arkham-profiles", http.HandlerFunc(internal.RunConsumeArkhamProfiles)).Methods(http.MethodPost)

	r.Handle("/getArkhamEntities", http.HandlerFunc(internal.GetEntitiesList)).Methods(http.MethodPost, http.MethodOptions)
	r.Handle("/getArkhamChains", http.HandlerFunc(internal.GetChainsList)).Methods(http.MethodPost, http.MethodOptions)
	r.Handle("/getTopTokens", http.HandlerFunc(internal.GetTokensList)).Methods(http.MethodPost, http.MethodOptions)
	r.Handle("/ConsumePubsub", http.HandlerFunc(internal.ConsumePubsub)).Methods(http.MethodGet, http.MethodOptions)

	initServer(r)

	os.Exit(0)

}
