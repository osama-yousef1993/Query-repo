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

// BuildNFTCustomCategoriesData Data from PG and FS
// BuildNFTCustomCategoriesData Build Data for an NftChain from FS and NFT Latest data from PG
// Returns the output of the call
func (m *Microservices) BuildNFTCustomCategoriesData(w http.ResponseWriter, r *http.Request) {
	span, labels := common.GenerateSpan("V2 BuildNFTCustomCategoriesData", r.Context())
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "BuildNFTCustomCategoriesData"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "BuildNFTCustomCategoriesData"))

	err := m.nftService.BuildNFTCustomCategoriesData(r.Context())
	if err != nil {
		goto ERR
	}

	log.EndTimeL(labels, "V2 BuildNFTCustomCategoriesData", startTime, nil)
	span.SetStatus(codes.Ok, "V2 BuildNFTCustomCategoriesData")
	w.Write([]byte("Ok"))
	return

ERR:
	log.ErrorL(labels, "%s", err)
	span.SetStatus(codes.Error, err.Error())
	w.WriteHeader(http.StatusInternalServerError)
	return

}

// BuildForbesNFTCustomCategoriesData Data from PG and FS
// BuildForbesNFTCustomCategoriesData Build Data for an NftChain from FS and NFT Latest data from PG
// Returns the output of the call
func (m *Microservices) BuildForbesNFTCustomCategoriesData(w http.ResponseWriter, r *http.Request) {
	span, labels := common.GenerateSpan("V2 BuildForbesNFTCustomCategoriesData", r.Context())
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "BuildForbesNFTCustomCategoriesData"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "BuildForbesNFTCustomCategoriesData"))

	data, err := io.ReadAll(r.Body)

	if err != nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var customNFT datastruct.CustomNFT

	// json.Unmarshal(data, &customNFT)
	if err := json.Unmarshal(data, &customNFT); err != nil {
		log.Error("%s", err)
		return
	}

	err = m.nftService.BuildForbesNFTCustomCategoriesData(r.Context(), customNFT)
	if err != nil {
		goto ERR
	}

	log.EndTimeL(labels, "V2 BuildForbesNFTCustomCategoriesData", startTime, nil)
	span.SetStatus(codes.Ok, "V2 BuildForbesNFTCustomCategoriesData")
	w.Write([]byte("Ok"))
	return

ERR:
	log.ErrorL(labels, "%s", err)
	span.SetStatus(codes.Error, err.Error())
	w.WriteHeader(http.StatusInternalServerError)
	return

}
