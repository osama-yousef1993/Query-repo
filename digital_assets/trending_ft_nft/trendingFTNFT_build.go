package app

import (
	"fmt"
	"net/http"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
)

// BuildTrendingFTNFTData handles the request to build trending data for FT and NFT.
func (m *Microservices) BuildTrendingFTNFTData(w http.ResponseWriter, r *http.Request) {
	span, labels := common.GenerateSpan("V2 BuildTrendingFTNFTData", r.Context())
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "BuildTrendingFTNFTData"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "BuildTrendingFTNFTData"))

	err := m.trendingFTNFTService.BuildTrendingFTNFTData(r.Context())
	if err != nil {
		goto ERR
	}

	log.EndTimeL(labels, "V2 BuildTrendingFTNFTData", startTime, nil)
	span.SetStatus(codes.Ok, "V2 BuildTrendingFTNFTData")
	w.Write([]byte("Ok"))
	return
ERR:
	log.ErrorL(labels, "%s", err)
	span.SetStatus(codes.Error, err.Error())
	w.WriteHeader(http.StatusInternalServerError)
	return
}
