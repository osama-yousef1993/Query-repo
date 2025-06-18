package app

import (
	"fmt"
	"net/http"

	"github.com/Forbes-Media/go-tools/log"
	"github.com/Forbes-Media/web3-whale-tracker/common"
	"go.opentelemetry.io/otel/codes"
)

// BuildWhaleTrackerData
// Get All messages from PubSub and store it to BQ
// If any Transaction meets Threshold send it to Slack
func (m *Microservices) BuildWhaleTrackerData(w http.ResponseWriter, r *http.Request) {
	span, labels := common.GenerateSpan("BuildWhaleTrackerData", r.Context())

	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "BuildWhaleTrackerData"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "BuildWhaleTrackerData"))
	// var (
	// 	threshold float64 = 0
	// 	err       error
	// )
	// x := html.EscapeString(r.URL.Query().Get("threshold"))
	// threshold, err = strconv.ParseFloat(x, 64)
	// if err != nil {
	// 	log.ErrorL(labels, "BuildWhaleTrackerData Threshold Value not exist %s", err)
	// }

	err := m.transactions.BuildTransaction(r.Context())

	if err != nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
	}

	log.EndTimeL(labels, "BuildWhaleTrackerData End Successfully", startTime, nil)
	span.SetStatus(codes.Ok, "BuildWhaleTrackerData")
}
