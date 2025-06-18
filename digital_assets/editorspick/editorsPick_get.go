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

// Get GetEditorsPickData Data from FS
// GetEditorsPickData Returns All Data for an EditorsPick Data
// Returns the output of the call
func (m *Microservices) GetEditorsPickData(w http.ResponseWriter, r *http.Request) {
	// updated each 2 hours
	common.SetResponseHeaders(w, 7200)
	span, labels := common.GenerateSpan("V2 GetEditorsPickData", r.Context())
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "GetEditorsPickData"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "GetEditorsPickData"))
	var (
		err         error
		editorsPick *datastruct.EditorsPick
		jsonB       []byte
	)
	editorsPick, err = m.editorsPickService.GetEditorsPick(r.Context())
	if err != nil {
		goto ERR
	}

	jsonB, err = json.Marshal(editorsPick)
	if err != nil {
		goto ERR
	}

	log.EndTimeL(labels, "V2 GetEditorsPickData", startTime, nil)
	span.SetStatus(codes.Ok, "V2 GetEditorsPickData")
	w.Write(jsonB)
	return

ERR:
	log.ErrorL(labels, "%s", err)
	span.SetStatus(codes.Error, err.Error())
	w.WriteHeader(http.StatusInternalServerError)
	return

}
