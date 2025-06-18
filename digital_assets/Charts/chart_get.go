package app

import (
	"encoding/json"
	"fmt"
	"html"
	"net/http"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/go-tools/log"
	"github.com/gorilla/mux"
	"go.opentelemetry.io/otel/codes"
)

// Get Chart Data from PG
// GetChartData Returns Chart Data for any asset from FT, NFT and CATEGORY
// Returns the output of the call as Json
func (m *Microservices) GetChartData(w http.ResponseWriter, r *http.Request) {
	common.SetResponseHeaders(w, 300)
	span, labels := common.GenerateSpan("V2 GetChartData", r.Context())
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "GetChartData"))
	vars := mux.Vars(r)

	symbol := vars["symbol"]
	period := vars["period"]
	assetsType := html.EscapeString(r.URL.Query().Get("assetsType"))
	interval := fmt.Sprintf("%s_%s", symbol, period)

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "GetChartData"))
	var (
		err             error
		categoriesChart *datastruct.TimeSeriesResultPG
		jsonB           []byte
	)
	categoriesChart, err = m.chartService.GetCategoriesChart(r.Context(), interval, symbol, period, assetsType)
	if err != nil {
		goto ERR
	}

	jsonB, err = json.Marshal(categoriesChart)
	if err != nil {
		goto ERR
	}

	log.EndTimeL(labels, "V2 GetChartData", startTime, nil)
	span.SetStatus(codes.Ok, "V2 GetChartData")
	w.Write(jsonB)
	return

ERR:
	log.ErrorL(labels, "%s", err)
	span.SetStatus(codes.Error, err.Error())
	w.WriteHeader(http.StatusInternalServerError)
	return
}
