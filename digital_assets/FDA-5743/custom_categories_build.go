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
		data                  string
		res                   []byte
	)

	bodyBytes, err = io.ReadAll(r.Body)
	if err != nil {
		goto ERR
	}
	if err = json.Unmarshal(bodyBytes, &customCategoryRequest); err != nil {
		goto ERR
	}

	data, err = m.customCategoryService.GetCustomFieldFromFS(r.Context(), customCategoryRequest)

	if err != nil {
		goto ERR
	}

	res, _ = json.Marshal(data)

	log.EndTimeL(labels, fmt.Sprintf("Starting %s", "BuildCustomCategoriesData"), startTime, nil)
	span.SetStatus(codes.Ok, "V2 BuildCustomCategoriesData")
	w.Write(res)
	return
ERR:
	log.ErrorL(labels, "%s", err)
	span.SetStatus(codes.Error, err.Error())
	w.WriteHeader(http.StatusInternalServerError)
	return
}

// BuildCustomCategoriesDataFS
// It will build the Custom Categories Data with dynamic query from FS.
// func (m *Microservices) BuildCustomCategoriesDataFS(w http.ResponseWriter, r *http.Request) {
// 	span, labels := common.GenerateSpan("V2 BuildCustomCategoriesDataFS", r.Context())
// 	defer span.End()
// 	span.AddEvent(fmt.Sprintf("Starting %s", "V2 BuildCustomCategoriesDataFS"))
// 	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 BuildCustomCategoriesDataFS"))

// 	var (
// 		err            error
// 		categoriesData map[string]datastruct.CategoriesData
// 		jsonB          []byte
// 	)
// 	categoriesData, err = m.customCategoryService.BuildCustomCategoriesDataFS(r.Context())
// 	if err != nil {
// 		goto ERR
// 	}

// 	jsonB, err = json.Marshal(categoriesData)
// 	if err != nil {
// 		goto ERR
// 	}

// 	log.EndTimeL(labels, "V2 BuildCustomCategoriesDataFS", startTime, nil)
// 	span.SetStatus(codes.Ok, "V2 BuildCustomCategoriesDataFS")
// 	w.Write(jsonB)
// 	return
// ERR:
// 	log.ErrorL(labels, "%s", err)
// 	span.SetStatus(codes.Error, err.Error())
// 	w.WriteHeader(http.StatusInternalServerError)
// 	return
// }

SELECT symbol,name,slug,logo,display_symbol,source,temporary_data_delay,price_24h,percentage_24h,date,change_value_24h,market_cap,original_symbol,number_of_active_market_pairs,price_7d,price_30d,price_1Y,price_ytd,percentage_1h,percentage_7d,percentage_30d,percentage_1y,percentage_ytd,circulating_supply,last_updated
 FROM fundamentalslatest 
 WHERE name != 'null' 
 ORDER BY name desc 
 LIMIT 100