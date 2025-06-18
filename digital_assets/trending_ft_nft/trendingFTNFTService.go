package services

import (
	"context"
	"fmt"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/repository"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
)

type TrendingFTNFTService interface {
	// BuildTrendingFTNFTData orchestrates the process of fetching trending assets data
	BuildTrendingFTNFTData(ctx context.Context) error
}

type trendingFTNFTService struct {
	dao repository.DAO
}

// NewTrendingAssetsService creates and returns a new instance of TrendingFTNFTService.
func NewTrendingAssetsService(dao repository.DAO) TrendingFTNFTService {
	return &trendingFTNFTService{dao: dao}
}

// BuildTrendingFTNFTData orchestrates the process of fetching and storing trending assets data.
// It retrieves both cryptocurrency and NFT trending data from BigQuery and stores them in Firestore.
//
// Parameters:
//   - ctx: Context for handling cancellation and timeouts
//
// Returns:
//   - error: Error if any occurs during the data retrieval or storage process
//
// The function performs the following steps:
//  1. Creates a new query manager instance
//  2. Fetches trending cryptocurrency data from BigQuery
//  3. Fetches trending NFT data from BigQuery
//  4. Stores both datasets in Firestore
//
// If any error occurs during data retrieval, the function returns immediately
// without proceeding to the storage step.
func (t *trendingFTNFTService) BuildTrendingFTNFTData(ctx context.Context) error {
	// Initialize tracing span and logging labels
	span, labels := common.GenerateSpan("V2 BuildTrendingFTNFTData", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 BuildTrendingFTNFTData"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 BuildTrendingFTNFTData"))

	// Initialize query manager
	queryMGR := t.dao.NewTrendingAssetsQuery()

	// Fetch trending cryptocurrency data
	trendingCryptos, err := queryMGR.GetTrendingFTDataBQ(ctx)
	if err != nil {
		log.ErrorL(labels, "Error V2 BuildTrendingFTNFTData Reading Trending Cryptos %s", err)
	}

	// Fetch trending NFT data
	trendingNFTs, err := queryMGR.GetTrendingNFTsDataBQ(ctx)
	if err != nil {
		log.ErrorL(labels, "Error V2 BuildTrendingFTNFTData Reading Trending NFTs %s", err)
		return err
	}

	// Store both datasets in Firestore
	queryMGR.InsertTrendingFTNFT(ctx, trendingCryptos, trendingNFTs)

	// Log completion and set successful status
	log.EndTimeL(labels, "V2 BuildTrendingFTNFTData", startTime, nil)
	span.SetStatus(codes.Ok, "V2 BuildTrendingFTNFTData")
	return nil
}
