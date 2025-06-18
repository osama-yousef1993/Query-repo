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

// BuildCustomCategoriesData
// It will build the Custom Categories Data with dynamic query.
func (m *Microservices) BuildCustomCategoriesData(w http.ResponseWriter, r *http.Request) {
	span, labels := common.GenerateSpan("V2 BuildCustomCategoriesData", r.Context())
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 BuildCustomCategoriesData"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 BuildCustomCategoriesData"))

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

	err = m.customCategoryService.BuildCustomCategoriesData(r.Context(), customCategoryRequest)

	if err != nil {
		goto ERR
	}

	log.EndTimeL(labels, "V2 BuildCustomCategoriesData", startTime, nil)
	span.SetStatus(codes.Ok, "V2 BuildCustomCategoriesData")
	w.Write([]byte("OK"))
	return
ERR:
	log.ErrorL(labels, "%s", err)
	span.SetStatus(codes.Error, err.Error())
	w.WriteHeader(http.StatusInternalServerError)
	return
}

// BuildCustomCategoriesDataFS
// It will build the Custom Categories Data with dynamic query from FS.
func (m *Microservices) BuildCustomCategoriesDataFS(w http.ResponseWriter, r *http.Request) {
	span, labels := common.GenerateSpan("V2 BuildCustomCategoriesDataFS", r.Context())
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 BuildCustomCategoriesDataFS"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 BuildCustomCategoriesDataFS"))

	err := m.customCategoryService.BuildCustomCategoriesDataFS(r.Context())

	if err != nil {
		goto ERR
	}

	log.EndTimeL(labels, "V2 BuildCustomCategoriesDataFS", startTime, nil)
	span.SetStatus(codes.Ok, "V2 BuildCustomCategoriesDataFS")
	w.Write([]byte("OK"))
	return
ERR:
	log.ErrorL(labels, "%s", err)
	span.SetStatus(codes.Error, err.Error())
	w.WriteHeader(http.StatusInternalServerError)
	return
}
