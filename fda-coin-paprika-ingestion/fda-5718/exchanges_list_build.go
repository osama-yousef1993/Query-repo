package app

import (
	"net/http"

	"github.com/Forbes-Media/fda-coin-paprika-ingestion/common"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
)

func (m *Microservices) BuildExchangesList(w http.ResponseWriter, r *http.Request) {
	// Initialize tracing span and logging labels.
	span, labels := common.GenerateSpan("BuildExchangesList", r.Context())
	defer span.End()

	span.AddEvent("Starting BuildExchangesList")
	startTime := log.StartTimeL(labels, "Starting BuildExchangesList")

	// Trigger the exchanges list build process.
	err := m.exchangesListService.BuildExchangesList(r.Context())
	if err != nil {
		log.ErrorL(labels, "Error in BuildExchangesList: %s", err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	// Log success and respond.
	log.EndTimeL(labels, "BuildExchangesList Completed", startTime, nil)
	span.SetStatus(codes.Ok, "BuildExchangesList Successful")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Ok"))
}

func (m *Microservices) BuildExchangeMarkets(w http.ResponseWriter, r *http.Request) {
	// Initialize tracing span and logging labels.
	span, labels := common.GenerateSpan("BuildExchangeMarkets", r.Context())
	defer span.End()

	span.AddEvent("Starting BuildExchangeMarkets")
	startTime := log.StartTimeL(labels, "Starting BuildExchangeMarkets")

	// Trigger the exchange metadata list build process.
	err := m.exchangesListService.BuildExchangeMarkets(r.Context())
	if err != nil {
		log.ErrorL(labels, "Error in BuildExchangeMarkets: %s", err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	// Log success and respond.
	log.EndTimeL(labels, "BuildExchangeMarkets Completed", startTime, nil)
	span.SetStatus(codes.Ok, "BuildExchangeMarkets Successful")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Ok"))
}
