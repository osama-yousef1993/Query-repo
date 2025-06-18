package app

import (
	"encoding/json"
	"net/http"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/go-tools/log"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// Get NFT Chains Data from FS
// GetNFTChains Returns All Data for NFTs Chains
// Returns the output of the call
func (m *Microservices) GetNFTChains(w http.ResponseWriter, r *http.Request) {
	common.SetResponseHeaders(w, 60)

	span, labels := common.GenerateSpan("V2 GetNFTChains", r.Context())

	defer span.End()

	labels["UUID"] = uuid.New().String()
	labels["function"] = "GetNFTChains"
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTime("GetNFTChains: Get NFT Chains Data")

	// Get All NFT Chains List
	chains, err := m.nftsPriceService.GetChainsList(r.Context())
	if chains == nil && err == nil {
		log.ErrorL(labels, "GetNFTChains: Error Getting NFTs Chains List:  %s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusNotFound)

	}

	data, err := json.Marshal(chains)
	if err != nil {
		log.ErrorL(labels, "GetNFTChains: Error Getting NFTs Chains List:  %s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusNotFound)
	}

	span.SetStatus(codes.Ok, "GetNFTChains")
	log.EndTimeL(labels, "GetNFTChains: Successfully Finished", startTime, nil)
	w.WriteHeader(200)
	w.Write(data)

}
