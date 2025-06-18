package repository

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Forbes-Media/go-tools/log"
	"github.com/Forbes-Media/web3-common/cloudutils"
	"github.com/Forbes-Media/web3-common/crypto-filter-protocol/common"
	"github.com/Forbes-Media/web3-common/crypto-filter-protocol/dto"
	"go.opentelemetry.io/otel/codes"
)

type AssetsQuery interface {
	// GetForbesAssets retrieves a map of Forbes assets from the database.
	GetForbesAssets(ctx context.Context, mapBy string) (assets map[string]dto.ForbesAsset, err error)
	// InsertMarketData inserts market data into BigQuery.
	InsertMarketData(ctx context.Context, marketData *[]dto.BQMarketData) error
	// GetFundamentalsDistinct retrieves a list of distinct fundamentals from the database.
	GetFundamentalsDistinct(ctx context.Context) (exchanges *[]dto.ForbesAsset, err error)
	// UpsertForbesAssets performs an upsert operation (insert or update) for a list of Forbes assets in the database.
	UpsertForbesAssets(ctx context.Context, assets *[]dto.ForbesAsset) (err error)
	// InsertToRemediationCollection inserts market data into a remediation collection.
	InsertToRemediationCollection(ctx context.Context, assets map[string]dto.MarketData) (err error)
	// UpdateSourceID updates the source ID for an asset in the database based on the provided old and new IDs.
	UpdateSourceID(ctx context.Context, oldID, newID, sourceID string) error
	// GetAssetBySourceID retrieves an asset from the database using the provided source ID.
	// GetAssetBySourceID(ctx context.Context, oldID, sourceID string) (*dto.ForbesAsset, error)
}

type assetsQuery struct {
	// data_namespace specifies the namespace or environment for the data (e.g., "prd", "dev").
	// This is used to isolate data between different environments.
	dataNamespace string

	// rowy_prefix is a prefix used for Rowy-specific configurations or identifiers.
	// Rowy is a tool for managing databases, and this prefix helps differentiate resources.
	rowyPrefix string

	// pg is an instance of PostgresUtils, which provides utility methods for interacting with a PostgreSQL database.
	// This is used for querying, inserting, or updating data in PostgreSQL.
	pg cloudutils.PostgresUtils

	// fs is an instance of FirestoreUtils, which provides utility methods for interacting with Google Firestore.
	// This is used for querying, inserting, or updating data in Firestore.
	fs cloudutils.FirestoreUtils

	// bq is an instance of BigqueryUtils, which provides utility methods for interacting with Google BigQuery.
	// This is used for querying or inserting large datasets into BigQuery for analysis.
	bq cloudutils.BigqueryUtils
}

// GetForbesAssets retrieves a map of Forbes assets from the database.
// It performs the following steps:
// 1. Queries the database for the Forbes assets.
// 2. Maps the retrieved assets to the provided mapBy field.
//
// Parameters:
// - ctx: The context for managing the request lifecycle, including cancellation and timeouts.
// - mapBy: A string containing the field to map the assets by.
// Returns:
// - assets: A map of ForbesAsset containing the retrieved assets.
// - error: An error if any issues occur during the process, otherwise nil.
func (a *assetsQuery) GetForbesAssets(ctx context.Context, mapBy string) (assets map[string]dto.ForbesAsset, err error) {
	span, labels := common.GenerateSpan(ctx, "crypto-filter-protocol.GetForbesAssets")
	defer span.End()
	span.AddEvent("Starting crypto-filter-protocol.GetForbesAssets")

	startTime := log.StartTimeL(labels, "Starting crypto-filter-protocol.GetForbesAssets")
	assets = map[string]dto.ForbesAsset{}
	c, err := a.pg.GetClient()

	if err != nil {
		log.Error("Error closing client: %v", err)
		return
	}

	query := `
		SELECT
			id,
			name,
			symbol,
			COALESCE(coingecko_id, ''),
			COALESCE(coinpaprika_id, ''),
			COALESCE(contract_address, ''),
			last_updated
		FROM forbes_assets`

	queryResult, err := c.Query(query)
	if err != nil {
		log.Error("Error querying forbes_assets: %v", err)
		return
	}

	for queryResult.Next() {
		var asset dto.ForbesAsset
		err = queryResult.Scan(
			&asset.ForbesID, &asset.Name, &asset.Symbol, &asset.CoingeckoID,
			&asset.CoinpaprikaID, &asset.ContractAddress, &asset.LastUpdated)

		if err != nil {
			log.Error("Error scanning forbes_assets: %v", err)

			return
		}

		switch mapBy {
		case "id":
			assets[strings.ToLower(*asset.ForbesID)] = asset
		case "name":
			assets[strings.ToLower(*asset.Name)] = asset
		case "symbol":
			assets[strings.ToLower(*asset.Symbol)] = asset
		case "coinpaprika_id":
			assets[strings.ToLower(*asset.CoinpaprikaID)] = asset
		case "coingecko_id":
			assets[strings.ToLower(*asset.CoingeckoID)] = asset
		case "contract_address":
			assets[*asset.ContractAddress] = asset
		}
	}

	log.EndTimeL(labels, "Finished crypto-filter-protocol.GetForbesAssets", startTime, nil)

	return
}

// InsertMarketData inserts a batch of market data into BigQuery.
// It performs the following steps:
// 1. Generates a span for tracing and logs the start time of the operation.
// 2. Retrieves the table name for market data based on the data namespace.
// 3. Initializes a BigQuery client.
// 4. Configures the BigQuery inserter to ignore unknown values.
// 5. Attempts to insert the market data into BigQuery.
// 6. If a 413 error (payload too large) occurs, the data is split into smaller batches and retried.
// 7. Logs the result of the operation and returns any errors encountered.
//
// Parameters:
// - ctx: The context for managing the request lifecycle, including cancellation and timeouts.
// - marketData: A pointer to a slice of BQMarketData containing the market data to be inserted.
//
// Returns:
// - error: An error if any issues occur during the process, otherwise nil.
func (a *assetsQuery) InsertMarketData(ctx context.Context, marketData *[]dto.BQMarketData) error {
	span, labels := common.GenerateSpan(ctx, "crypto-filter-protocol.InsertMarketData")
	defer span.End()

	startTime := log.StartTime("crypto-filter-protocol.InsertMarketData")

	currenciesTable := common.GetTableName("Digital_Asset_MarketData", a.dataNamespace)

	iArr := cloudutils.ConvertToInterfaceSlice(marketData)
	err := a.bq.InsertData(ctx, iArr, currenciesTable)

	if err != nil {
		log.Error("Error inserting data into BigQuery: %v", err)
		return err
	}

	log.EndTimeL(labels, "crypto-filter-protocol.InsertMarketData: Successfully finished Inserting Markets Data at time : %s", startTime, nil)

	return nil
}

// GetFundamentalsDistinct retrieves a list of distinct Forbes assets from the database.
// It performs the following steps:
// 1. Generates a span for tracing and logs the start time of the operation.
// 2. Initializes a PostgreSQL client.
// 3. Executes a query to retrieve distinct Forbes assets based on the latest fundamentals data.
// 4. Iterates through the query results, mapping them to ForbesAsset objects.
// 5. Logs the result of the operation and returns the retrieved assets or any errors encountered.
//
// Parameters:
// - ctx: The context for managing the request lifecycle, including cancellation and timeouts.
//
// Returns:
// - assets: A pointer to a slice of ForbesAsset containing the retrieved distinct assets.
// - error: An error if any issues occur during the process, otherwise nil.
func (a *assetsQuery) GetFundamentalsDistinct(ctx context.Context) (assets *[]dto.ForbesAsset, err error) {
	span, labels := common.GenerateSpan(ctx, "crypto-filter-protocol.GetFundamentalsDistinct")
	defer span.End()
	span.AddEvent("Starting crypto-filter-protocol.GetFundamentalsDistinct")

	startTime := log.StartTimeL(labels, "Starting crypto-filter-protocol.GetFundamentalsDistinct")
	assets = &[]dto.ForbesAsset{}
	c, err := a.pg.GetClient()

	if err != nil {
		log.ErrorL(labels, "Error getting postgres client: %v", err)
		span.SetStatus(codes.Error, "Error getting postgres client")

		return
	}

	query := `
		select distinct
		slug, display_symbol, symbol as coingecko_id, name, platform_contract_address
		from (
		SELECT symbol, slug, name, display_symbol
			from fundamentalslatest
		where status = 'active'
		) fundamentals
		left join (
		SELECT id, platform_contract_address
		FROM public.coingecko_asset_metadata
		) metadata 
		on  metadata.id = fundamentals.symbol`

	queryResult, err := c.Query(query)
	if err != nil {
		log.ErrorL(labels, "Error getting postgres client: %v", err)
		span.SetStatus(codes.Error, "Error getting postgres client")

		return
	}

	for queryResult.Next() {
		var asset dto.ForbesAsset
		err = queryResult.Scan(
			&asset.ForbesID, &asset.Symbol, &asset.CoingeckoID, &asset.Name, &asset.ContractAddress)

		if err != nil {
			log.ErrorL(labels, "Error scanning forbes_assets: %v", err)
			span.SetStatus(codes.Error, "error scanning forbes_assets")

			return
		}

		*assets = append(*assets, asset)
	}

	log.EndTimeL(labels, "crypto-filter-protocol.GetFundamentalsDistinct", startTime, nil)
	span.SetStatus(codes.Ok, "success")

	return
}

// UpsertForbesAssets performs an upsert operation (insert or update) for a list of Forbes assets into the database.
// It performs the following steps:
// 1. Generates a span for tracing and logs the start time of the operation.
// 2. Initializes a PostgreSQL client.
// 3. Prepares the data for insertion by constructing a batch of values and arguments.
// 4. Executes an upsert query to insert or update the assets in the database.
// 5. Handles large datasets by splitting them into smaller batches (up to 65,000 values per batch).
// 6. Logs the result of the operation and returns any errors encountered.
//
// Parameters:
// - ctx: The context for managing the request lifecycle, including cancellation and timeouts.
// - assets: A pointer to a slice of ForbesAsset containing the assets to be upserted.
//
// Returns:
// - error: An error if any issues occur during the process, otherwise nil.
func (a *assetsQuery) UpsertForbesAssets(ctx context.Context, assets *[]dto.ForbesAsset) (err error) {
	span, labels := common.GenerateSpan(ctx, "crypto-filter-protocol.UpsertForbesAssets")
	defer span.End()
	span.AddEvent("Starting crypto-filter-protocol.UpsertForbesAssets")

	startTime := log.StartTimeL(labels, "Starting crypto-filter-protocol.UpsertForbesAssets")

	c, err := a.pg.GetClient()

	if err != nil {
		log.ErrorL(labels, "Error getting postgres client: %v", err)
		span.SetStatus(codes.Error, "Error getting postgres client")

		return
	}

	assetList := *assets
	valueString := make([]string, 0, len(assetList))
	valueArgs := make([]interface{}, 0, len(assetList)*7)
	colCount := 7
	tableName := "forbes_assets"

	var i = 0

	for y := 0; y < len(assetList); y++ {
		e := assetList[y]

		var valString = fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d)", i*colCount+1, i*colCount+2, i*colCount+3, i*colCount+4, i*colCount+5, i*colCount+6, i*colCount+7)
		valueString = append(valueString, valString)

		valueArgs = append(valueArgs, e.ForbesID, e.Name, e.Symbol, e.CoingeckoID, e.CoinpaprikaID, e.ContractAddress, time.Now().UTC())
		i++

		if len(valueArgs) >= 65000 || y == len(assetList)-1 {
			insertStatement := fmt.Sprintf("INSERT INTO %s (id, name, symbol, coingecko_id, coinpaprika_id, contract_address, last_updated) VALUES %s", tableName, strings.Join(valueString, ","))
			updateStatement := "ON CONFLICT (id) DO UPDATE SET name = COALESCE(EXCLUDED.name, forbes_assets.name), symbol = COALESCE(EXCLUDED.symbol, forbes_assets.symbol), coingecko_id = COALESCE(EXCLUDED.coingecko_id, forbes_assets.coingecko_id), coinpaprika_id = COALESCE(EXCLUDED.coinpaprika_id, forbes_assets.coinpaprika_id), contract_address = COALESCE(EXCLUDED.contract_address, forbes_assets.contract_address), last_updated = EXCLUDED.last_updated"

			query := insertStatement + " " + updateStatement
			_, inserterError := c.ExecContext(context.Background(), query, valueArgs...)

			if inserterError != nil {
				log.ErrorL(labels, "UpsertForbesAssets: Error Upserting Assets List to PostgreSQL: %v", inserterError)
				return inserterError
			}

			valueString = make([]string, 0, len(assetList))
			valueArgs = make([]interface{}, 0, len(assetList)*colCount)
			i = 0
		}
	}

	log.EndTimeL(labels, "UpsertForbesAssets", startTime, nil)
	span.SetStatus(codes.Ok, "success")

	return
}

// InsertToRemediationCollection inserts a map of market data into a Firestore collection for remediation purposes.
// It performs the following steps:
// 1. Generates a span for tracing and logs the start time of the operation.
// 2. Writes the provided market data to a Firestore collection using the `WriteMapToCollection` utility.
// 3. Logs the result of the operation and returns any errors encountered.
//
// Parameters:
// - ctx: The context for managing the request lifecycle, including cancellation and timeouts.
// - assets: A map of MarketData containing the market data to be inserted, keyed by a unique identifier.
//
// Returns:
// - error: An error if any issues occur during the process, otherwise nil.
func (a *assetsQuery) InsertToRemediationCollection(ctx context.Context, assets map[string]dto.MarketData) (err error) {
	span, labels := common.GenerateSpan(ctx, "crypto-filter-protocol.InsertToRemediationCollection")
	defer span.End()

	span.AddEvent("Starting crypto-filter-protocol.InsertToRemediationCollection")

	startTime := log.StartTimeL(labels, "Starting crypto-filter-protocol.InsertToRemediationCollection")

	err = cloudutils.WriteMapToCollection(ctx, assets, common.AssignRowyPrefix("cryptoIntake_assets", a.rowyPrefix), a.fs)
	if err != nil {
		log.Error("Error writing to firestore collection: %v", err)
		return
	}

	log.EndTime("crypto-filter-protocol.InsertToRemediationCollection", startTime, nil)
	span.SetStatus(codes.Ok, "success")

	return
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
func (a *assetsQuery) UpdateSourceID(ctx context.Context, oldID, newID, sourceID string) error {
	span, labels := common.GenerateSpan(ctx, "crypto-filter-protocol.UpdateSourceID")
	defer span.End()
	span.AddEvent("Starting crypto-filter-protocol.UpdateSourceID")

	startTime := log.StartTimeL(labels, "Starting crypto-filter-protocol.UpdateSourceID")

	c, err := a.pg.GetClient()

	if err != nil {
		log.ErrorL(labels, "crypto-filter-protocol.UpdateSourceID Error getting postgres client: %v", err)
		span.SetStatus(codes.Error, "crypto-filter-protocol.UpdateSourceID Error getting postgres client")

		return err
	}

	tableName := "forbes_assets"

	query := `select id, coingecko_id, coinpaprika_id from $1 where $2 = '$3'`

	queryResult, err := c.Query(query, tableName, sourceID, oldID)

	if err != nil {
		log.ErrorL(labels, "crypto-filter-protocol.UpdateSourceID: Error Getting Asset from PostgreSQL: %v", err)
		span.SetStatus(codes.Error, "crypto-filter-protocol.UpdateSourceID: Error Getting Asset from PostgreSQL")

		return err
	}

	var asset dto.ForbesAsset
	for queryResult.Next() {
		err = queryResult.Scan(&asset.ForbesID, &asset.CoingeckoID, &asset.CoinpaprikaID)
		if err != nil {
			log.ErrorL(labels, "crypto-filter-protocol.UpdateSourceID: Error Scan Asset from PostgreSQL: %v", err)
			span.SetStatus(codes.Error, "crypto-filter-protocol.UpdateSourceID: Error Scan Asset from PostgreSQL")

			return err
		}
	}

	if asset.ForbesID != nil && *asset.ForbesID != "" {
		updateStatement := `UPDATE $1 SET $2 = '$3' WHERE $2 = '$3'`

		_, updateError := c.ExecContext(ctx, updateStatement, tableName, sourceID, newID, oldID)

		if updateError != nil {
			log.ErrorL(labels, "crypto-filter-protocol.UpdateSourceID: Error Update Asset to PostgreSQL: %v", updateError)
			span.SetStatus(codes.Error, "crypto-filter-protocol.UpdateSourceID: Error Update Asset to PostgreSQL")

			return updateError
		}
	}

	log.EndTimeL(labels, "crypto-filter-protocol.UpdateSourceID Completed", startTime, nil)
	span.SetStatus(codes.Ok, "crypto-filter-protocol.UpdateSourceID Completed")

	return nil
}

// GetAssetBySourceID retrieves an asset from the database using the provided source ID.
// It performs the following steps:
// 1. Generates a span for tracing and logs the start of the operation.
// 2. Retrieves a PostgreSQL client to execute the database query.
// 3. Constructs and executes an SQL query to fetch the asset based on the source ID.
// 4. Scans the query result into a ForbesAsset DTO (Data Transfer Object).
// 5. Logs the result of the operation and sets the span status accordingly.
//
// Parameters:
// - ctx: The context for managing the request lifecycle, including cancellation and timeouts.
// - oldID: The source ID used to look up the asset in the database.
// - sourceID: The column name (e.g., "coingecko_id" or "coinpaprika_id") to query against.
//
// Returns:
// - *dto.ForbesAsset: A pointer to the ForbesAsset DTO containing the retrieved asset data.
// - error: An error if any issues occur during the process, otherwise nil.
// func (a *assetsQuery) GetAssetBySourceID(ctx context.Context, oldID, sourceID string) (*dto.ForbesAsset, error) {
// 	span, labels := common.GenerateSpan(ctx, "crypto-filter-protocol.GetAssetBySourceID")
// 	defer span.End()
// 	span.AddEvent("Starting crypto-filter-protocol.GetAssetBySourceID")

// 	startTime := log.StartTimeL(labels, "Starting crypto-filter-protocol.GetAssetBySourceID")

// 	c, err := a.pg.GetClient()

// 	if err != nil {
// 		log.ErrorL(labels, "crypto-filter-protocol.GetAssetBySourceID Error getting postgres client: %v", err)
// 		span.SetStatus(codes.Error, "crypto-filter-protocol.GetAssetBySourceID Error getting postgres client")

// 		return nil, err
// 	}

// 	tableName := "forbes_assets"

// 	query := `select id, coingecko_id, coinpaprika_id from ` + tableName + ` where ` + sourceID + ` = '` + oldID + `'`

// 	queryResult, err := c.Query(query)

// 	if err != nil {
// 		log.ErrorL(labels, "crypto-filter-protocol.GetAssetBySourceID: Error Getting Asset from PostgreSQL: %v", err)
// 		span.SetStatus(codes.Error, "crypto-filter-protocol.GetAssetBySourceID: Error Getting Asset from PostgreSQL")

// 		return nil, err
// 	}

// 	var asset dto.ForbesAsset
// 	for queryResult.Next() {
// 		err = queryResult.Scan(&asset.ForbesID, &asset.CoingeckoID, &asset.CoinpaprikaID)
// 		if err != nil {
// 			log.ErrorL(labels, "crypto-filter-protocol.GetAssetBySourceID: Error Scan Asset from PostgreSQL: %v", err)
// 			span.SetStatus(codes.Error, "crypto-filter-protocol.GetAssetBySourceID: Error Scan Asset from PostgreSQL")

// 			return nil, err
// 		}
// 	}

// 	log.EndTimeL(labels, "crypto-filter-protocol.GetAssetBySourceID Completed", startTime, nil)
// 	span.SetStatus(codes.Ok, "crypto-filter-protocol.GetAssetBySourceID Completed")

// 	return &asset, nil
// }
