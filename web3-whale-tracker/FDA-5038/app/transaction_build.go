package app

import (
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"strconv"

	"github.com/Forbes-Media/go-tools/log"
	"github.com/Forbes-Media/web3-whale-tracker/common"
	"go.opentelemetry.io/otel/codes"
)

func (m *Microservices) BuildWhaleTrackerData(w http.ResponseWriter, r *http.Request) {
	span, labels := common.GenerateSpan("BuildWhaleTrackerData", r.Context())

	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "BuildWhaleTrackerData"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "BuildWhaleTrackerData"))
	var (
		threshold float64 = 0
		err       error
	)
	x := html.EscapeString(r.URL.Query().Get("threshold"))
	threshold, err = strconv.ParseFloat(x, 64)
	if err != nil {
		log.ErrorL(labels, "BuildWhaleTrackerData Threshold Value not exist %s", err)
	}

	err = m.transactions.BuildTransaction(r.Context(), threshold)

	if err != nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
	}

	log.EndTimeL(labels, "BuildWhaleTrackerData", startTime, nil)
	span.SetStatus(codes.Ok, "BuildWhaleTrackerData")
}

func (m *Microservices) GetTransactionHistory(w http.ResponseWriter, r *http.Request) {
	span, labels := common.GenerateSpan("GetTransactionHistory", r.Context())

	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "GetTransactionHistory"))

	startTime := log.StartTimeL(labels, "GetTransactionHistory")
	wallet := html.EscapeString(r.URL.Query().Get("wallet_address"))

	transactions, err := m.transactions.GetTransactionHistory(r.Context(), wallet)
	if err != nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(500)
		return
	}

	res, err := json.Marshal(transactions)
	if err != nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(500)
		return
	}

	log.EndTimeL(labels, "GetTransactionHistory", startTime, nil)
	span.SetStatus(codes.Ok, "GetTransactionHistory")
	w.Write(res)
}
