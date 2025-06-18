package cryptofilterprotocol

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/Forbes-Media/go-tools/log"
	"github.com/Forbes-Media/web3-common/crypto-filter-protocol/common"
	"github.com/Forbes-Media/web3-common/crypto-filter-protocol/dto"
	"github.com/Forbes-Media/web3-common/crypto-filter-protocol/repository"
	"go.opentelemetry.io/otel/codes"
)

// AssetsIntake defines the interface for ingesting and managing market data.
// It provides methods for processing incoming market data and performing backfill operations.
type AssetsIntake interface {
	// IntakeMarketData processes and ingests market data into the system.
	IntakeMarketData(ctx context.Context, assetsMap []dto.MarketData, source common.DataSource) (err error)
	// BackFillMarketData performs a backfill operation for market data.
	BackFillMarketData(ctx context.Context)
	// UpdateSourceID updates the source ID for an asset in the database based on the provided old and new IDs.
	UpdateSourceID(ctx context.Context, oldID, newID string, sourceID common.DataSource) error
}

// assetsIntake is the implementation of the AssetsIntake interface.
// It provides the functionality for ingesting and managing market data.
type assetsIntake struct {
	dao repository.Dao
}

// NewAssetsIntake creates and returns a new instance of the AssetsIntake interface.
// It initializes the assetsIntake struct with the provided DAO (Data Access Object) for database interactions.
//
// Parameters:
// - dao: A repository.Dao object that provides access to database operations.
//
// Returns:
// - AssetsIntake: An implementation of the AssetsIntake interface, initialized with the provided DAO.
func NewAssetsIntake(dao repository.Dao) AssetsIntake {
	return &assetsIntake{dao: dao}
}

// mapMarketData maps a MarketData object to a BQMarketData object for insertion into BigQuery.
// It performs the following steps:
// 1. Generates a span for tracing and logs the start of the mapping process.
// 2. Maps fields from the input MarketData object to the corresponding fields in the BQMarketData object.
// 3. Handles null or missing values for certain fields (e.g., OccuranceTime) by providing default values.
// 4. Logs the result of the operation and sets the span status accordingly.
//
// Parameters:
// - ctx: The context for managing the request lifecycle, including cancellation and timeouts.
// - asset: A MarketData object containing the market data to be mapped.
//
// Returns:
// - mappedAsset: A BQMarketData object containing the mapped data, ready for insertion into BigQuery.
func mapMarketData(ctx context.Context, asset *dto.MarketData) dto.BQMarketData {
	span, labels := common.GenerateSpan(ctx, "mapMarketData")
	defer span.End()

	span.AddEvent("Starting: mapMarketData")

	var mappedAsset dto.BQMarketData

	mappedAsset.ID = asset.ID
	mappedAsset.Name = asset.Name
	mappedAsset.Symbol = asset.Symbol
	mappedAsset.Price = bigquery.NullFloat64{Float64: asset.Price, Valid: true}
	mappedAsset.CirculatingSupply = bigquery.NullFloat64{Float64: asset.CirculatingSupply, Valid: true}
	mappedAsset.MaxSupply = bigquery.NullFloat64{Float64: asset.MaxSupply, Valid: true}
	mappedAsset.MarketCap = bigquery.NullFloat64{Float64: asset.MarketCap, Valid: true}
	mappedAsset.Volume = bigquery.NullFloat64{Float64: asset.Volume, Valid: true}
	mappedAsset.QuoteCurrency = asset.QuoteCurrency
	mappedAsset.Source = asset.Source

	if asset.OccuranceTime.IsZero() {
		mappedAsset.OccuranceTime = bigquery.NullTimestamp{Timestamp: time.Now(), Valid: true}
	} else {
		mappedAsset.OccuranceTime = bigquery.NullTimestamp{Timestamp: asset.OccuranceTime, Valid: true}
	}

	span.SetStatus(codes.Ok, "Success")
	log.DebugL(labels, "Successfully Map MarketData")

	return mappedAsset
}

// IntakeMarketData processes and ingests market data into the system.
// It performs the following steps:
// 1. Generates a span for tracing and logs the start of the operation.
// 2. Determines the column to use for asset lookup based on the data source (e.g., CoinGecko or CoinPaprika).
// 3. Retrieves Forbes assets from the database using the specified column and contract address.
// 4. Maps and categorizes the incoming market data into successful, new, and failed entries.
// 5. Upserts new Forbes assets into the database.
// 6. Inserts successful market data into BigQuery.
// 7. Sends failed market data to a remediation collection for further processing.
// 8. Logs the result of the operation and sets the span status accordingly.
//
// Parameters:
// - ctx: The context for managing the request lifecycle, including cancellation and timeouts.
// - assetsArray: A slice of MarketData containing the market data to be processed.
// - source: The data source (e.g., CoinGecko or CoinPaprika) for the market data.
//
// Returns:
// - error: An error if any issues occur during the process, otherwise nil.
func (a *assetsIntake) IntakeMarketData(ctx context.Context, assetsArray []dto.MarketData, source common.DataSource) (err error) {
	span, labels := common.GenerateSpan(ctx, "IntakeMarketData")
	defer span.End()

	span.AddEvent("Starting: IntakeMarketData")

	startTime := log.StartTimeL(labels, "Starting: IntakeMarketData")
	db := a.dao.NewAssetsQuery()
	col := ""

	switch source {
	case common.SrcCoinGecko:
		col = fmt.Sprintf("%s_id", common.SrcCoinGecko)
	case common.SrcCoinPaprika:
		col = fmt.Sprintf("%s_id", common.SrcCoinPaprika)
	default:
		log.Error("Invalid source: %v", source)
		return fmt.Errorf("invalid source: %v", source)
	}

	forbesAssets, err := db.GetForbesAssets(ctx, col)

	if err != nil {
		log.Error("Error getting assets: %v", err)
		return err
	}

	forbesAssetsByContractAddress, err := db.GetForbesAssets(ctx, "contract_address")
	if err != nil {
		log.Error("Error getting assets: %v", err)
		return err
	}

	var (
		successful      []dto.BQMarketData
		newForbesAssets []dto.ForbesAsset
	)

	var failed = make(map[string]dto.MarketData)

	for i := range assetsArray {
		asset := assetsArray[i]
		coin, ok := forbesAssets[asset.ID]

		if ok {
			mappedAsset := mapMarketData(ctx, &asset)
			mappedAsset.ForbesID = *coin.ForbesID
			successful = append(successful, mappedAsset)

			continue
		}

		if asset.ContractAddress != "" {
			coin, ok := forbesAssetsByContractAddress[asset.ContractAddress]

			if ok {
				newForbesAssets = append(newForbesAssets, dto.ForbesAsset{ForbesID: coin.ForbesID, Name: &asset.Name, Symbol: &asset.Symbol})

				continue
			}

			failed[asset.Name] = dto.MarketData{ID: asset.ID, Name: asset.Name, Symbol: asset.Symbol, Source: asset.Source}
		}

		failed[asset.Name] = dto.MarketData{ID: asset.ID, Name: asset.Name, Symbol: asset.Symbol, Source: asset.Source}
	}

	err = db.UpsertForbesAssets(ctx, &newForbesAssets)
	if err != nil {
		log.ErrorL(labels, "Error upserting exchanges: %v", err)
		span.SetStatus(codes.Error, err.Error())

		return err
	}

	err = db.InsertMarketData(ctx, &successful)
	if err != nil {
		log.Error("Error writing to assets remediation collection: %v", err)
		span.SetStatus(codes.Error, err.Error())

		return err
	}

	if len(failed) > 0 {
		err = db.InsertToRemediationCollection(ctx, failed)
		if err != nil {
			log.Error("Error writing to assets remediation collection: %v", err)
			span.SetStatus(codes.Error, err.Error())

			return err
		}
	}

	span.SetStatus(codes.Ok, "IntakeMarketData")
	log.EndTimeL(labels, "IntakeMarketData", startTime, nil)

	return nil
}

// BackFillMarketData performs a backfill operation for market data by retrieving and upserting Forbes assets into the database.
// It performs the following steps:
// 1. Generates a span for tracing and initializes logging labels.
// 2. Retrieves existing Forbes assets from the database to check if a backfill is necessary.
// 3. If assets already exist, logs an error and aborts the backfill process.
// 4. Retrieves distinct fundamentals data to gather a list of assets for backfilling.
// 5. Converts the retrieved data into a slice of ForbesAsset objects.
// 6. Upserts the assets into the database.
// 7. Logs the result of the operation and sets the span status accordingly.
//
// Parameters:
// - ctx: The context for managing the request lifecycle, including cancellation and timeouts.
func (a *assetsIntake) BackFillMarketData(ctx context.Context) {
	span, labels := common.GenerateSpan(ctx, "assets_Intake.BackFillMarketData")

	db := repository.NewDao()

	assets, err := db.NewAssetsQuery().GetForbesAssets(ctx, "forbes_id")
	if err != nil {
		log.ErrorL(labels, "Error getting assets: %v", err)
		span.SetStatus(codes.Error, err.Error())

		return
	}

	if len(assets) > 0 {
		log.ErrorL(labels, "cannot backfill assets are already defined")
		span.SetStatus(codes.Error, "Assets are already defined")

		return
	}

	assetsMap, err := db.NewAssetsQuery().GetFundamentalsDistinct(ctx)
	if err != nil {
		log.ErrorL(labels, "Error closing client: %v", err)
		span.SetStatus(codes.Error, err.Error())

		return
	}

	err = db.NewAssetsQuery().UpsertForbesAssets(ctx, assetsMap)
	if err != nil {
		log.ErrorL(labels, "Error upserting Assets: %v", err)
		span.SetStatus(codes.Error, err.Error())

		return
	}

	span.SetStatus(codes.Ok, "Success")
	log.InfoL(labels, "Successfully backfilled Assets")
}

// UpdateSourceID updates the source ID for an asset in the database based on the provided old and new IDs.
// It performs the following steps:
// 1. Generates a span for tracing and logs the start of the operation.
// 2. Validates that the old ID, new ID, and data source are not empty.
// 3. Determines the database column to update based on the data source (e.g., CoinGecko or CoinPaprika).
// 4. Retrieves the asset from the database using the old source ID to ensure it exists.
// 5. Updates the source ID in the database with the new ID.
// 6. Logs the result of the operation and sets the span status accordingly.
//
// Parameters:
// - ctx: The context for managing the request lifecycle, including cancellation and timeouts.
// - oldID: A pointer to the old source ID that needs to be updated.
// - newID: A pointer to the new source ID that will replace the old ID.
// - sourceID: The data source (e.g., CoinGecko or CoinPaprika) for which the ID is being updated.
//
// Returns:
// - error: An error if any issues occur during the process, otherwise nil.
func (a *assetsIntake) UpdateSourceID(ctx context.Context, oldID, newID string, sourceID common.DataSource) error {
	span, labels := common.GenerateSpan(ctx, "UpdateSourceID")
	defer span.End()

	span.AddEvent("Starting UpdateSourceID")

	startTime := log.StartTimeL(labels, "Starting UpdateSourceID")

	queryMGR := a.dao.NewAssetsQuery()

	if oldID == "" || newID == "" || sourceID == "" {
		log.EndTimeL(labels, "UpdateSourceID Error one of the values are empty", startTime, errors.New("all values should be exist"))
		span.SetStatus(codes.Error, "UpdateSourceID Error one of the values are empty")

		return errors.New("empty values can'r proceed")
	}

	var col string

	switch sourceID {
	case common.SrcCoinGecko:
		col = "coingecko_id"
	case common.SrcCoinPaprika:
		col = "coinpaprika_id"
	default:
		log.Error("Invalid source: %v", sourceID)
		return fmt.Errorf("invalid source: %v", sourceID)
	}

	// asset, err := queryMGR.GetAssetBySourceID(ctx, oldID, col)
	// if err != nil {
	// 	log.Error("UpdateSourceID Error Getting asset with source ID from PG: %v", err)
	// 	span.SetStatus(codes.Error, "UpdateSourceID Error Getting asset with source ID from PG")

	// 	return err
	// }

	// if asset.ForbesID == nil && *asset.ForbesID == "" {
	// 	log.Error("UpdateSourceID Error asset with source ID not exist: %v", err)
	// 	span.SetStatus(codes.Error, "UpdateSourceID Error asset with source ID not exist")

	// 	return err
	// }

	err := queryMGR.UpdateSourceID(ctx, oldID, newID, col)

	if err != nil {
		log.Error("UpdateSourceID Error Updated source ID: %v", err)
		span.SetStatus(codes.Error, "UpdateSourceID Error Updated source ID")

		return err
	}

	log.EndTimeL(labels, "UpdateSourceID Completed", startTime, nil)
	span.SetStatus(codes.Ok, "UpdateSourceID Completed")

	return nil
}
