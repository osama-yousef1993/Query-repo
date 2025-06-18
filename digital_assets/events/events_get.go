package app

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
)

// Get GetEventsData  from FS
// GetEventsData Returns All Data for an Events Data
// Returns the output of the call
func (m *Microservices) GetEventsData(w http.ResponseWriter, r *http.Request) {
	// updated each 2 hours
	common.SetResponseHeaders(w, 7200)
	span, labels := common.GenerateSpan("V2 GetEventsData", r.Context())
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "GetEventsData"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "GetEventsData"))
	var (
		err    error
		events *datastruct.Events
		jsonB  []byte
	)
	events, err = m.eventsService.GetEventsData(r.Context())
	if err != nil {
		goto ERR
	}

	jsonB, err = json.Marshal(events)
	if err != nil {
		goto ERR
	}

	log.EndTimeL(labels, "V2 GetEventsData", startTime, nil)
	span.SetStatus(codes.Ok, "V2 GetEventsData")
	w.Write(jsonB)
	return

ERR:
	log.ErrorL(labels, "%s", err)
	span.SetStatus(codes.Error, err.Error())
	w.WriteHeader(http.StatusInternalServerError)
	return

}
