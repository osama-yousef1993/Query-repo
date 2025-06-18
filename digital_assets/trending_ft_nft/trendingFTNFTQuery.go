package repository

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/firestore"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
	"google.golang.org/api/iterator"
)

type TrendingFTNFTQuery interface {
	// GetTrendingFTDataBQ retrieves trending cryptocurrency data from BigQuery.
	GetTrendingFTDataBQ(ctx context.Context) ([]datastruct.TrendingAssets, error)
	// GetTrendingNFTsDataBQ retrieves trending NFT data from BigQuery.
	GetTrendingNFTsDataBQ(ctx context.Context) ([]datastruct.TrendingAssets, error)
	// Insert Trending Assets data to FS
	InsertTrendingFTNFT(ctx context.Context, trendingData []datastruct.TrendingAssets, collectionName string)
}

type trendingFTNFTQuery struct{}

// GetTrendingFTDataBQ retrieves trending cryptocurrency data from BigQuery.
// It executes a predefined query and returns a slice of TrendingAssets.
//
// Parameters:
//   - ctx: Context for handling cancellation and timeouts
//
// Returns:
//   - []datastruct.TrendingAssets: Slice containing trending crypto assets data
//   - error: Error if any occurs during the process
//
// The function performs the following steps:
//  1. Initializes tracing and logging
//  2. Establishes connection to BigQuery
//  3. Executes the predefined TrendingCryptoQuery
//  4. Iterates through results and maps them to TrendingAssets struct
//  5. Returns the collected trending crypto data
func (t *trendingFTNFTQuery) GetTrendingFTDataBQ(ctx context.Context) ([]datastruct.TrendingAssets, error) {
	// Initialize tracing span and logging labels
	span, labels := common.GenerateSpan("V2 TrendingFTNFTQuery.GetTrendingFTDataBQ", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 TrendingFTNFTQuery.GetTrendingFTDataBQ"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 TrendingFTNFTQuery.GetTrendingFTDataBQ"))

	// Get BigQuery client
	bqs, err := bqUtils.GetBigQueryClient()
	if err != nil {
		log.ErrorL(labels, "Error V2 TrendingFTNFTQuery.GetTrendingFTDataBQ Connecting to BigQuery: %s", err)
		return nil, err
	}

	// Execute the predefined query
	queryResult := bqs.Query(datastruct.TrendingCryptoQuery)
	it, err := queryResult.Read(ctx)
	if err != nil {
		log.ErrorL(labels, "Error V2 TrendingFTNFTQuery.GetTrendingFTDataBQ Executing Crypto Query: %s", err)
		return nil, err
	}

	// Initialize slice to store results
	var trendingCryptos []datastruct.TrendingAssets

	// Iterate through query results and map to TrendingAssets struct
	for {
		var trendingCrypto datastruct.TrendingAssets
		err := it.Next(&trendingCrypto)
		if err == iterator.Done {
			break
		}

		if err != nil {
			log.ErrorL(labels, "Error V2 TrendingFTNFTQuery.GetTrendingFTDataBQ Mapping Crypto Data: %s", err)
			return nil, err
		}
		trendingCryptos = append(trendingCryptos, trendingCrypto)
	}

	// Log completion and set successful status
	log.EndTimeL(labels, "V2 TrendingFTNFTQuery.GetTrendingFTDataBQ", startTime, nil)
	span.SetStatus(codes.Ok, "V2 TrendingFTNFTQuery.GetTrendingFTDataBQ")
	return trendingCryptos, nil
}

// GetTrendingNFTsDataBQ retrieves trending nfts data from BigQuery.
// It executes a predefined query and returns a slice of TrendingAssets.
//
// Parameters:
//   - ctx: Context for handling cancellation and timeouts
//
// Returns:
//   - []datastruct.TrendingAssets: Slice containing trending crypto nfts data
//   - error: Error if any occurs during the process
//
// The function performs the following steps:
//  1. Initializes tracing and logging
//  2. Establishes connection to BigQuery
//  3. Executes the predefined TrendingCryptoQuery
//  4. Iterates through results and maps them to TrendingAssets struct
//  5. Returns the collected trending crypto data
func (t *trendingFTNFTQuery) GetTrendingNFTsDataBQ(ctx context.Context) ([]datastruct.TrendingAssets, error) {
	// Initialize tracing span and logging labels
	span, labels := common.GenerateSpan("V2 TrendingFTNFTQuery.GetTrendingNFTsDataBQ", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 TrendingFTNFTQuery.GetTrendingNFTsDataBQ"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 TrendingFTNFTQuery.GetTrendingNFTsDataBQ"))

	// Get BigQuery client
	bqs, err := bqUtils.GetBigQueryClient()
	if err != nil {
		log.ErrorL(labels, "Error V2 TrendingFTNFTQuery.GetTrendingNFTsDataBQ Connecting to BigQuery: %s", err)
		return nil, err
	}

	// Execute the predefined query
	queryResult := bqs.Query(datastruct.TrendingNFTQuery)
	it, err := queryResult.Read(ctx)
	if err != nil {
		log.ErrorL(labels, "Error V2 TrendingFTNFTQuery.GetTrendingNFTsDataBQ Executing NFT Query: %s", err)
		return nil, err
	}

	// Initialize slice to store results
	var trendingNFTs []datastruct.TrendingAssets

	// Iterate through query results and map to TrendingAssets struct
	for {
		var trendingNFT datastruct.TrendingAssets
		err := it.Next(&trendingNFT)
		if err == iterator.Done {
			break
		}

		if err != nil {
			log.ErrorL(labels, "Error V2 TrendingFTNFTQuery.GetTrendingNFTsDataBQ Mapping NFT Data: %s", err)
			return nil, err
		}
		trendingNFTs = append(trendingNFTs, trendingNFT)
	}

	trendingNFTs, err = GetNFTsSlugFromPG(ctx, trendingNFTs)
	if err != nil {
		log.ErrorL(labels, "Error V2 TrendingFTNFTQuery.GetTrendingNFTsDataBQ Getting NFT Slug from PG: %s", err)
		return nil, err
	}

	// Log completion and set successful status
	log.EndTimeL(labels, "V2 TrendingFTNFTQuery.GetTrendingNFTsDataBQ", startTime, nil)
	span.SetStatus(codes.Ok, "V2 TrendingFTNFTQuery.GetTrendingNFTsDataBQ")
	return trendingNFTs, nil
}

// InsertTrendingFTNFT inserts or updates trending cryptocurrency and NFT data into Firestore.
// It processes two separate slices for crypto and NFT assets, storing them in their respective collections.
//
// Parameters:
//   - ctx: Context for handling cancellation and timeouts
//   - trendingData: Slice of TrendingAssets containing cryptocurrency data to be inserted or NFT data to be inserted
//   - collectionName: String that hold the FS table name where we need to add the data
//
// The function:
//  1. Stores crypto data in TrendingCryptoCollectionName collection
//  2. Stores NFT data in TrendingNFTCollectionName collection
//  3. Uses the asset's slug as the document ID
//  4. Merges new data with existing documents if they exist
func (t *trendingFTNFTQuery) InsertTrendingFTNFT(ctx context.Context, trendingData []datastruct.TrendingAssets, collectionName string) {
	// Initialize tracing span and logging labels
	span, labels := common.GenerateSpan("V2 TrendingFTNFTQuery.InsertTrendingFTNFT", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 TrendingFTNFTQuery.InsertTrendingFTNFT"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 TrendingFTNFTQuery.InsertTrendingFTNFT"))

	// Get Firestore client
	fs := fsUtils.GetFirestoreClient()

	// Insert trending cryptocurrency data
	for _, data := range trendingData {
		fs.Collection(collectionName).Doc(data.Slug).Set(ctx, map[string]interface{}{
			"slug":  data.Slug,
			"order": data.Order,
		}, firestore.MergeAll)
	}

	// Log completion and set successful status
	log.EndTimeL(labels, "V2 TrendingFTNFTQuery.InsertTrendingFTNFT", startTime, nil)
	span.SetStatus(codes.Ok, "V2 TrendingFTNFTQuery.InsertTrendingFTNFT")
}

// GetNFTsSlugFromPG retrieves and updates NFT slugs from PostgreSQL database for a given list of trending NFTs.
// For each NFT in the input slice, it queries the PostgreSQL database to get the corresponding slug
// from the nftdatalatest table using the NFT's ID.
//
// Parameters:
//   - ctx: Context for handling cancellation and timeouts
//   - trendingNFTs: Slice of TrendingAssets containing NFT data to be updated with slugs
//
// Returns:
//   - []datastruct.TrendingAssets: Updated slice of TrendingAssets with correct slugs
//   - error: Error if any occurs during the database operations
//
// The function:
//  1. Connects to PostgreSQL database
//  2. For each NFT, queries nftdatalatest table using NFT's ID
//  3. Updates the NFT's slug with the retrieved value
//  4. Handles any errors during query execution or data scanning
func GetNFTsSlugFromPG(ctx context.Context, trendingNFTs []datastruct.TrendingAssets) ([]datastruct.TrendingAssets, error) {
	// Initialize tracing span and logging labels
	span, labels := common.GenerateSpan("V2 TrendingFTNFTQuery.GetNFTsSlugFromPG", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 TrendingFTNFTQuery.GetNFTsSlugFromPG"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 TrendingFTNFTQuery.GetNFTsSlugFromPG"))

	// Get PostgreSQL connection
	pg := PGConnect()
	var newTrendingNFTs []datastruct.TrendingAssets

	// Iterate through NFTs and update their slugs
	var nfts []string
	for _, nft := range trendingNFTs {
		// SQL query to get slug from nftdatalatest table
		nfts = append(nfts, fmt.Sprintf("'%s'", nft.Slug))
	}
	query := `
			select 
				id,
				slug 
			from 
				nftdatalatest
			where 
				id in (` + strings.Join(nfts, ",") + `)
		`
	// Execute query
	queryResult, err := pg.QueryContext(ctx, query)
	if err != nil {
		log.ErrorL(labels, "Error V2 TrendingFTNFTQuery.GetNFTsSlugFromPG Query Error From PG : %s", err)
	}

	// Scan and process query results
	for queryResult.Next() {
		var nftData datastruct.NFT
		err := queryResult.Scan(&nftData.Id, &nftData.Slug)
		if err != nil {
			log.ErrorL(labels, "Error V2 TrendingFTNFTQuery.GetNFTsSlugFromPG Scanning Slug Error From PG : %s", err)
		}
		for _, nft := range trendingNFTs {
			if nft.Slug == nftData.Id {
				newTrendingNFTs = append(newTrendingNFTs, datastruct.TrendingAssets{Slug: nftData.Slug, Order: nft.Order})
			}
		}
	}

	// Log completion and set successful status
	log.EndTimeL(labels, "V2 TrendingFTNFTQuery.GetNFTsSlugFromPG", startTime, nil)
	span.SetStatus(codes.Ok, "V2 TrendingFTNFTQuery.GetNFTsSlugFromPG")
	return newTrendingNFTs, nil
}
