package app

import (
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"strings"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
)

// GetEducation Returns All section With Articles for Learn Tab from FS
// Expects a Query params (categories) to get the data from FS by section name
// Returns the output of the call
func (m *Microservices) GetEducation(w http.ResponseWriter, r *http.Request) {

	span, labels := common.GenerateSpan("V2 GetEducation", r.Context())
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "GetEducation"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "GetEducation"))
	// selected Categories from Learn Section
	categories := html.EscapeString(r.URL.Query().Get("categories"))

	categoriesList := strings.Split(categories, ",")
	for index, ele := range categoriesList {
		categoriesList[index] = strings.TrimSpace(ele)
	}

	var (
		result *datastruct.LandingPageEducation
		err    error
		jsonB  []byte
	)
	if len(categoriesList) > 0 && categories != "" {
		result, err = m.educationService.GetEducation(r.Context(), categoriesList)
	} else {
		result, err = m.educationService.GetEducation(r.Context(), nil)
	}

	if err != nil {
		goto ERR
	}

	jsonB, err = json.Marshal(result)
	if err != nil {
		goto ERR
	}

	log.EndTimeL(labels, "V2 Education", startTime, nil)
	span.SetStatus(codes.Ok, "V2 Education")
	w.Write(jsonB)
	return
ERR:
	log.ErrorL(labels, "%s", err)
	span.SetStatus(codes.Error, err.Error())
	w.WriteHeader(http.StatusInternalServerError)
	return

}
