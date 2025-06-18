package app

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/Forbes-Media/go-tools/log"
	"github.com/Forbes-Media/web3-whale-tracker/common"
	"go.opentelemetry.io/otel/codes"
)

// ReceiveTransactions
// Receive messages from PubSub push process
// If any Transaction meets Threshold send it to Slack
func (m *Microservices) ReceiveTransactions(w http.ResponseWriter, r *http.Request) {
	span, labels := common.GenerateSpan("ReceiveTransactions", r.Context())

	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "ReceiveTransactions"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "ReceiveTransactions"))

	message, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.ErrorL(labels, "Error Reading message from pubsub : Error: %s", err)
		span.SetStatus(codes.Error, err.Error())
	}

	err = m.transactions.ReceiveTransactions(r.Context(), message)

	if err != nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
	}

	log.EndTimeL(labels, "ReceiveTransactions End Successfully", startTime, nil)
	span.SetStatus(codes.Ok, "ReceiveTransactions")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Ok"))
}
