package app

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/Forbes-Media/go-tools/log"
	"github.com/Forbes-Media/web3-whale-tracker/common"
	"go.opentelemetry.io/otel/codes"
)

// BuildWhaleTrackerData
// Get All messages from PubSub and store it to BQ
// If any Transaction meets Threshold send it to Slack
func (m *Microservices) GetWalletsEntities(w http.ResponseWriter, r *http.Request) {
	common.SetResponseHeaders(w, 300)
	span, labels := common.GenerateSpan("BuildWhaleTrackerData", r.Context())

	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "BuildWhaleTrackerData"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "BuildWhaleTrackerData"))
	wallets := m.transactions.GetWalletsEntities(r.Context())

	res, err := json.Marshal(wallets)
	if err != nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
	}

	log.EndTimeL(labels, "BuildWhaleTrackerData End Successfully", startTime, nil)
	span.SetStatus(codes.Ok, "BuildWhaleTrackerData")
	w.Write(res)
}

// BuildWhaleTrackerData
// Get All messages from PubSub and store it to BQ
// If any Transaction meets Threshold send it to Slack
func (m *Microservices) GetAlertsRules(w http.ResponseWriter, r *http.Request) {
	common.SetResponseHeaders(w, 300)
	span, labels := common.GenerateSpan("BuildWhaleTrackerData", r.Context())

	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "BuildWhaleTrackerData"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "BuildWhaleTrackerData"))
	wallets := m.transactions.GetAlertsRules(r.Context())

	res, err := json.Marshal(wallets)
	if err != nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
	}

	log.EndTimeL(labels, "BuildWhaleTrackerData End Successfully", startTime, nil)
	span.SetStatus(codes.Ok, "BuildWhaleTrackerData")
	w.Write(res)
}
