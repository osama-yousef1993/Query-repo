package app

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/Forbes-Media/go-tools/log"
	"github.com/Forbes-Media/web3-whale-tracker/common"
	"go.opentelemetry.io/otel/codes"
)

// GetWalletsEntities
// Get All Wallets Entities from BQ
// If There are no Wallets it will return empty response
func (m *Microservices) GetWalletsEntities(w http.ResponseWriter, r *http.Request) {
	common.SetResponseHeaders(w, 300)
	span, labels := common.GenerateSpan("GetWalletsEntities", r.Context())

	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "GetWalletsEntities"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "GetWalletsEntities"))
	wallets := m.transactions.GetWalletsEntities(r.Context())

	res, err := json.Marshal(wallets)
	if err != nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
	}

	log.EndTimeL(labels, "GetWalletsEntities End Successfully", startTime, nil)
	span.SetStatus(codes.Ok, "GetWalletsEntities")
	w.Write(res)
}
