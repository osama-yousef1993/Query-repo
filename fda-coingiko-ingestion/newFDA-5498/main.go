package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Forbes-Media/fda-coingecko-ingestion/internal"
	"github.com/Forbes-Media/fda-nomics-ingestion/auth"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gorilla/mux/otelmux"
	"go.opentelemetry.io/otel"

	"github.com/gorilla/mux"
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
var tracer = otel.Tracer("github.com/Forbes-Media/fda-coingekco-ingestion/main")

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

	r.Handle("/consume-assetlist", oidcAuthMiddleware.Middleware(http.HandlerFunc(internal.ConsumeAssetList))).Methods(http.MethodPost)
	r.Handle("/consume-exchanges-list", oidcAuthMiddleware.Middleware(http.HandlerFunc(internal.ConsumeExchangesList))).Methods(http.MethodPost)
	r.Handle("/consume-assetmarkets", oidcAuthMiddleware.Middleware(http.HandlerFunc(internal.ConsumeCoinGeckoMarkets))).Methods(http.MethodPost)
	r.Handle("/consume-asset-metadata", oidcAuthMiddleware.Middleware(http.HandlerFunc(internal.ConsumeAssetMetadata))).Methods(http.MethodPost)
	r.Handle("/consume-exchange-metadata", oidcAuthMiddleware.Middleware(http.HandlerFunc(internal.ConsumeExchangeMetadata))).Methods(http.MethodPost)
	r.Handle("/consume-exchanges-tickers", oidcAuthMiddleware.Middleware(http.HandlerFunc(internal.ConsumeExchangesTickers))).Methods(http.MethodPost)
	r.Handle("/consume-top-exchanges", oidcAuthMiddleware.Middleware(http.HandlerFunc(internal.ConsumeTopNumExchanges))).Methods(http.MethodPost)
	r.Handle("/consume-global-description", oidcAuthMiddleware.Middleware(http.HandlerFunc(internal.ConsumeGlobalData))).Methods(http.MethodPost)
	r.Handle("/consume-categories", oidcAuthMiddleware.Middleware(http.HandlerFunc(internal.ConsumeCategories))).Methods(http.MethodPost)
	r.Handle("/consume-nft-markets", oidcAuthMiddleware.Middleware(http.HandlerFunc(internal.ConsumeNFTsList))).Methods(http.MethodPost)
	r.Handle("/consume-nft-tickers", oidcAuthMiddleware.Middleware(http.HandlerFunc(internal.ConsumeNFTsTickers))).Methods(http.MethodPost)
	r.Handle("/consume-nft-metadata", oidcAuthMiddleware.Middleware(http.HandlerFunc(internal.ConsumeNFTMetaData))).Methods(http.MethodPost)
	r.Handle("/consume-nft-global-description", oidcAuthMiddleware.Middleware(http.HandlerFunc(internal.ConsumeNFTGlobalData))).Methods(http.MethodPost)

	port := os.Getenv("PORT")
	if port == "" {
		port = "3000"
	}
	// Server Settings
	svr := &http.Server{
		Handler:      r,
		Addr:         ":" + port,
		WriteTimeout: 3500 * time.Second,
		ReadTimeout:  3500 * time.Second,
	}

	go func() {
		// start the web server on port and accept requests
		log.Info("Server listening on port %s", port)
		if err := svr.ListenAndServe(); err != nil {
			log.Error("%s", err)
		}
	}()

	var wait time.Duration

	gracefulStop := make(chan os.Signal, 1)

	// We'll accept graceful shutdowns when quit via SIGINT, SIGTERM, SIGKILL, or Interrupt
	signal.Notify(gracefulStop, syscall.SIGTERM, syscall.SIGINT, os.Interrupt)

	// Block until we receive our signal.
	<-gracefulStop

	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()

	svr.Shutdown(ctx)

	log.Info("Web server shut down")

	close(gracefulStop)

	os.Exit(0)

}
