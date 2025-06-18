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

func (m *Microservices) BuildCustomCategoriesData(w http.ResponseWriter, r *http.Request) {
	common.SetResponseHeaders(w, 300)
	span, labels := common.GenerateSpan("V2 BuildCustomCategoriesData", r.Context())
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "BuildCustomCategoriesData"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "BuildCustomCategoriesData"))

	var (
		customCategoryRequest datastruct.CustomCategoryRequest
		err                   error
		bodyBytes             []byte
	)

	bodyBytes, err = io.ReadAll(r.Body)
	if err != nil {
		goto ERR
	}
	if err = json.Unmarshal(bodyBytes, &customCategoryRequest); err != nil {
		goto ERR
	}

	_, err = m.customCategoryService.GetCustomFieldFromFS(r.Context(), customCategoryRequest)

	if err != nil {
		goto ERR
	}

	log.EndTimeL(labels, fmt.Sprintf("Starting %s", "BuildCustomCategoriesData"), startTime, nil)
	span.SetStatus(codes.Ok, "V2 BuildCustomCategoriesData")
	w.Write([]byte("OK"))
	return
ERR:
	log.ErrorL(labels, "%s", err)
	span.SetStatus(codes.Error, err.Error())
	w.WriteHeader(http.StatusInternalServerError)
	return
}
