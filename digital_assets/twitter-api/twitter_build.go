package app

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
)

func (m *Microservices) PublishTwitterPost(w http.ResponseWriter, r *http.Request) {
	common.SetResponseHeaders(w, 0)
	span, labels := common.GenerateSpan("PublishTwitterPOst", r.Context())
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "PublishTwitterPOst"))

	startTime := log.StartTimeL(labels, "PublishTwitterPOst")

	var data datastruct.RequestBody

	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.ErrorL(labels, "PublishTwitterPOst %s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(500)
		return
	}

	err = json.Unmarshal(body, &data)
	if err != nil {
		log.ErrorL(labels, "PublishTwitterPOst %s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(500)
		return
	}

	result, err := m.twitterService.PublishTwitterPost(r.Context(), data)
	if err != nil {
		log.ErrorL(labels, "PublishTwitterPOst %s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(500)
		return
	}

	log.EndTimeL(labels, "PublishTwitterPOst", startTime, nil)
	span.SetStatus(codes.Ok, "PublishTwitterPOst")
	w.WriteHeader(200)
	w.Write(result)
}
