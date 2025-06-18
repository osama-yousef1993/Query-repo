package app

import (
	"net/http"

	"github.com/Forbes-Media/fda-coin-paprika-ingestion/common"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
)

// BuildCoinList handles the request to build the coin list.
// It triggers the coin list build process using the CoinListService.
// Parameters:
//   - w: The response writer.
//   - r: The request.
//
// Returns:
//   - The response writer with the status code and response body.
//   - The request.
func (m *Microservices) BuildCoinList(w http.ResponseWriter, r *http.Request) {
	// Initialize tracing span and logging labels.
	span, labels := common.GenerateSpan("BuildCoinList", r.Context())
	defer span.End()

	span.AddEvent("Starting BuildCoinList")
	startTime := log.StartTimeL(labels, "Starting BuildCoinList")

	// Trigger the coin list build process.
	err := m.coinListService.BuildCoinList(r.Context())
	if err != nil {
		log.ErrorL(labels, "Error in BuildCoinList: %s", err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	// Log success and respond.
	log.EndTimeL(labels, "BuildCoinList Completed", startTime, nil)
	span.SetStatus(codes.Ok, "BuildCoinList Successful")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Ok"))
}

// BuildCoinMetaDataList handles the request to build the coin metadata list.
// It triggers the coin metadata list build process using the CoinListService.
// Parameters:
//   - w: The response writer.
//   - r: The request.
//
// Returns:
//   - The response writer with the status code and response body.
//   - The request.
func (m *Microservices) BuildCoinMetaDataList(w http.ResponseWriter, r *http.Request) {
	// Initialize tracing span and logging labels.
	span, labels := common.GenerateSpan("BuildCoinMetaDataList", r.Context())
	defer span.End()

	span.AddEvent("Starting BuildCoinMetaDataList")
	startTime := log.StartTimeL(labels, "Starting BuildCoinMetaDataList")

	// Trigger the coin metadata list build process.
	err := m.coinListService.BuildCoinMetaDataList(r.Context())
	if err != nil {
		log.ErrorL(labels, "Error in BuildCoinMetaDataList: %s", err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	// Log success and respond.
	log.EndTimeL(labels, "BuildCoinMetaDataList Completed", startTime, nil)
	span.SetStatus(codes.Ok, "BuildCoinMetaDataList Successful")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Ok"))
}

// BuildCoinMetaDataList handles the request to build the coin metadata list.
// It triggers the coin metadata list build process using the CoinListService.
// Parameters:
//   - w: The response writer.
//   - r: The request.
//
// Returns:
//   - The response writer with the status code and response body.
//   - The request.
func (m *Microservices) BuildCoinHistoricalOHLV(w http.ResponseWriter, r *http.Request) {
	// Initialize tracing span and logging labels.
	span, labels := common.GenerateSpan("BuildCoinHistoricalOHLV", r.Context())
	defer span.End()

	span.AddEvent("Starting BuildCoinHistoricalOHLV")
	startTime := log.StartTimeL(labels, "Starting BuildCoinHistoricalOHLV")

	// Trigger the coin metadata list build process.
	err := m.coinListService.BuildCoinsHistoricalOHLCVData(r.Context())
	if err != nil {
		log.ErrorL(labels, "Error in BuildCoinHistoricalOHLV: %s", err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	// Log success and respond.
	log.EndTimeL(labels, "BuildCoinHistoricalOHLV Completed", startTime, nil)
	span.SetStatus(codes.Ok, "BuildCoinHistoricalOHLV Successful")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Ok"))
}
