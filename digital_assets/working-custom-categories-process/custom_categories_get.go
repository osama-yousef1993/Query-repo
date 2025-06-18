package app

import (
	"encoding/json"
	"fmt"
	"html"
	"net/http"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
)

// GetCustomCategories Get custom categories by type
func (m *Microservices) GetCustomCategories(w http.ResponseWriter, r *http.Request) {
	// updated each 5 min
	common.SetResponseHeaders(w, 300)
	span, labels := common.GenerateSpan("V2 GetCustomCategoriesData", r.Context())
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 GetCustomCategoriesData"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 GetCustomCategoriesData"))

	customCategoryType := html.EscapeString(r.URL.Query().Get("type"))
	var (
		err        error
		categories []datastruct.CustomCategory
		jsonB      []byte
	)
	categories, err = m.customCategoryService.GetCustomCategories(r.Context(), customCategoryType)
	if err != nil {
		goto ERR
	}

	jsonB, err = json.Marshal(categories)
	if err != nil {
		goto ERR
	}

	log.EndTimeL(labels, "V2 GetCustomCategoriesData", startTime, nil)
	span.SetStatus(codes.Ok, "V2 GetCustomCategoriesData")
	w.Write(jsonB)
	return

ERR:
	log.ErrorL(labels, "%s", err)
	span.SetStatus(codes.Error, err.Error())
	w.WriteHeader(http.StatusInternalServerError)
	return
}
