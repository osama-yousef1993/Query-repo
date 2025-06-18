package app

import (
	"fmt"
	"net/http"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
)

// BuildEditorsPickData Data from ForbesAPI And BQ
// BuildEditorsPickData Build Data for an EditorsPick from ForbesAPI or BQ if ForbesAPI returns nil
// Returns the output of the call
func (m *Microservices) BuildEditorsPickData(w http.ResponseWriter, r *http.Request) {
	span, labels := common.GenerateSpan("V2 BuildEditorsPickData", r.Context())
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "BuildEditorsPickData"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "BuildEditorsPickData"))

	err := m.editorsPickService.BuildEditorsPick(r.Context())
	if err != nil {
		goto ERR
	}

	log.EndTimeL(labels, "V2 BuildEditorsPickData", startTime, nil)
	span.SetStatus(codes.Ok, "V2 BuildEditorsPickData")
	w.Write([]byte("Ok"))
	return

ERR:
	log.ErrorL(labels, "%s", err)
	span.SetStatus(codes.Error, err.Error())
	w.WriteHeader(http.StatusInternalServerError)
	return

}
