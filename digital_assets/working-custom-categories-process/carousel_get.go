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

// Get CarouselData Data from PG and FS
// GetCarouselData Returns All Data for a Carousel Data
// Returns the output of the call
func (m *Microservices) GetCarouselData(w http.ResponseWriter, r *http.Request) {
	// updated each 1 min
	common.SetResponseHeaders(w, 60)
	span, labels := common.GenerateSpan("V2 GetCarouselData", r.Context())
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "GetCarouselData"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "GetCarouselData"))
	var (
		err          error
		carouselData *datastruct.TradedAssetsResp
		jsonB        []byte
	)
	carouselData, err = m.carouselService.GetCarouselData(r.Context())
	if err != nil {
		goto ERR
	}

	jsonB, err = json.Marshal(carouselData)
	if err != nil {
		goto ERR
	}

	log.EndTimeL(labels, "V2 GetCarouselData", startTime, nil)
	span.SetStatus(codes.Ok, "V2 GetCarouselData")
	w.Write(jsonB)
	return

ERR:
	log.ErrorL(labels, "%s", err)
	span.SetStatus(codes.Error, err.Error())
	w.WriteHeader(http.StatusInternalServerError)
	return

}

// Get GetAssetsData Data from PG
// GetAssetsData Returns All Data for a Fundamentals Data
// Returns the output of the call
func (m *Microservices) GetAssetsData(w http.ResponseWriter, r *http.Request) {
	// updated each 1 min
	common.SetResponseHeaders(w, 60)
	span, labels := common.GenerateSpan("V2 GetAssetsData", r.Context())
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "GetAssetsData"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "GetAssetsData"))
	var (
		err          error
		carouselData *datastruct.AssetsData
		jsonB        []byte
	)
	carouselData, err = m.carouselService.GetAssetsData(r.Context())
	if err != nil {
		goto ERR
	}

	jsonB, err = json.Marshal(carouselData)
	if err != nil {
		goto ERR
	}

	log.EndTimeL(labels, "V2 GetAssetsData", startTime, nil)
	span.SetStatus(codes.Ok, "V2 GetAssetsData")
	w.Write(jsonB)
	return

ERR:
	log.ErrorL(labels, "%s", err)
	span.SetStatus(codes.Error, err.Error())
	w.WriteHeader(http.StatusInternalServerError)
	return

}
