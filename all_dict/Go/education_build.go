package app

import (
	"fmt"
	"net/http"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
)

// BuildEducation Build education data
// BuildEducation Returns Error if the build failed
// Returns the output of the call
func (m *Microservices) BuildEducation(w http.ResponseWriter, r *http.Request) {

	span, labels := common.GenerateSpan("V2 BuildEducation", r.Context())
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "BuildEducation"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "BuildEducation"))
	err := m.educationService.BuildLearnEducation(r.Context())

	if err != nil {
		goto ERR
	}

	log.EndTimeL(labels, "V2 BuildEducation", startTime, nil)
	span.SetStatus(codes.Ok, "V2 BuildEducation")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
	return
ERR:
	log.ErrorL(labels, "%s", err)
	span.SetStatus(codes.Error, err.Error())
	w.WriteHeader(http.StatusInternalServerError)
	return

}
