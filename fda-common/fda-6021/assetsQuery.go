package repository

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
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

	GetAssetsSEOData(ctx context.Context) ([]dto.TickerSEOData, error)
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

func (a *assetsQuery) GetAssetsSEOData(ctx context.Context) ([]dto.TickerSEOData, error) {
	span, labels := common.GenerateSpan(ctx, "GetAssetsSEOData")
	defer span.End()

	span.AddEvent("Starting GetAssetsSEOData")

	startTime := log.StartTimeL(labels, "Starting GetAssetsSEOData")

	var assets []dto.TickerSEOData

	c, err := a.pg.GetClient()

	if err != nil {
		log.ErrorL(labels, "Error getting postgres client: %v", err)
		span.SetStatus(codes.Error, "Error getting postgres client")

		return nil, err
	}

	query := `	select 
		name, 
		symbol,
		display_symbol,
		slug,
		slug_override
	from 
		fundamentalslatest`

	queryResult, err := c.Query(query)

	if err != nil {
		log.ErrorL(labels, "Error getting AssetsSEO Data From PG: %v", err)
		span.SetStatus(codes.Error, "Error getting AssetsSEO Data From PG")

		return nil, err
	}

	for queryResult.Next() {
		var asset dto.TickerSEOData
		err = queryResult.Scan(&asset.Name, &asset.Symbol, &asset.DisplaySymbol, &asset.Slug, &asset.SlugOverride)

		if err != nil {
			log.ErrorL(labels, "Error Scanning AssetsSEO Data From PG: %v", err)
			span.SetStatus(codes.Error, "Error Scanning AssetsSEO Data From PG")

			return nil, err
		}

		assets = append(assets, asset)
	}

	log.EndTimeL(labels, "GetAssetsSEOData Completed Successfully", startTime, nil)
	span.SetStatus(codes.Ok, "GetAssetsSEOData Completed Successfully")

	return assets, nil
}

// BuildSlugMaps generates 2 important maps, which are used for generating unique slugs:
// 1. allSlugToSymbolMap : This is map[string]string, which maps a slug => symbol.
// 2. oldFundamentalsMap : This maps a symbol => oldFundamental.
func BuildSlugMaps(ctx context.Context, oldFundamentals *[]dto.TickerSEOData) (allSlugToSymbolMap common.ConcurrentMap[string, string], oldFundamentalsMap common.ConcurrentMap[string, dto.TickerSEOData]) {
	span, labels := common.GenerateSpan(ctx, "BuildSlugMaps")
	defer span.End()

	startTime := log.StartTimeL(labels, "build Slug Maps")

	allSlugToSymbolMap = common.NewConcurrentMap[string, string]()
	oldFundamentalsMap = common.NewConcurrentMap[string, dto.TickerSEOData]()

	for _, fundamental := range *oldFundamentals {
		oldFundamentalsMap.AddValue(fundamental.Symbol, fundamental)

		if value := allSlugToSymbolMap.GetValue(fundamental.Slug); value == nil || *value == "" {
			allSlugToSymbolMap.AddValue(fundamental.Slug, fundamental.Symbol)
		}
	}

	log.EndTimeL(labels, "build Slug Maps", startTime, nil)
	span.SetStatus(codes.Ok, "Success")

	return
}

// buildFundamentalSlug generates a unique and standardized slug for a financial asset based on its name and symbol.
// A slug is a string that uniquely identifies an asset. The function ensures the slug is unique across
// all assets by checking against a concurrent map (`allSlugToSymbolMap`) that stores existing slugs and their corresponding symbols.
//
// If the asset already has a valid slug (from previous data) and its name/symbol hasn't changed, the function retains
// the old slug. If the asset is new or its name/symbol has changed, the function generates a new unique slug by appending
// a numeric suffix (e.g., "bitcoin-btc-2") if necessary.
//
// Parameters:
//   - ctxO: A context object used for tracing and managing the function's execution lifecycle.
//   - allSlugToSymbolMap: A thread-safe map that stores the mapping between slugs and their corresponding asset symbols.
//     This map is used to ensure slugs are unique across all assets.
//   - oldFundamental: An object representing the existing fundamental data of the asset, including its previous slug and symbol.
//   - ticker: An object representing the current market data of the asset, including its name and symbol.
//
// Returns:
//   - string: A unique slug for the asset. If the asset already has a valid slug that matches the new slug, the old slug
//     is returned. Otherwise, a new unique slug is generated and returned.
//
// Process:
//  1. Generates a new slug by combining the asset's name and symbol (e.g., "bitcoin-btc").
//  2. Checks if the old slug exists and matches the new slug. If so, and the associated symbol matches, the old slug is retained.
//  3. If the asset is new or its name/symbol has changed, a new unique slug is generated by appending a numeric suffix
//     (e.g., "bitcoin-btc-2") and ensuring it doesn't already exist in the `allSlugToSymbolMap`.
//  4. The new slug is added to the `allSlugToSymbolMap` with the asset's ID as the corresponding value.
//  5. Returns the generated or retained slug.
//
// Example:
//   - For an asset with name "Bitcoin" and symbol "BTC", the slug "bitcoin-btc" is generated.
//   - If "bitcoin-btc" already exists, the function generates "bitcoin-btc-2", "bitcoin-btc-3", etc., until a unique slug is found.
//
// Edge Cases:
//   - New Asset: Generates a new slug and ensures its uniqueness.
//   - Changed Name/Symbol: Generates a new slug to reflect the updated information.
//   - Duplicate Slugs: Appends a numeric suffix to ensure uniqueness.
//
// Dependencies:
//   - `tracer`: Used for tracing the function's execution.
//   - `rfCommon.ConcurrentMap`: A thread-safe map for storing slug-to-symbol mappings.
//   - `codes`: Contains status codes for OpenTelemetry tracing.
//   - `cleanString`: A helper function to sanitize and clean strings (e.g., removing invalid characters).
//
// Thread Safety:
//   - The function is thread-safe due to the use of a concurrent map (`allSlugToSymbolMap`).
//
// Performance Considerations:
//   - The function uses a loop to generate unique slugs, which is efficient due to the concurrent map's fast lookups.
//   - Suitable for use in concurrent environments.
func buildFundamentalSlug(ctx context.Context, allSlugToSymbolMap common.ConcurrentMap[string, string], oldFundamental dto.TickerSEOData, ticker dto.BQMarketData) string {
	span, _ := common.GenerateSpan(ctx, "buildFundamentalSlug")
	defer span.End()

	if oldFundamental.SlugOverride != "" {
		span.SetStatus(codes.Ok, "Success")

		return oldFundamental.SlugOverride
	}

	oldSlug := oldFundamental.Slug
	newSlug := strings.ToLower(strings.ReplaceAll(fmt.Sprintf("%s-%s", ticker.Name, ticker.Symbol), " ", "-"))

	oldSlugSymbol := allSlugToSymbolMap.GetValue(oldSlug)

	// If the asset already has a valid previous slug, and its = to the new slug.retain the value
	// unique only to this asset's symbol.
	if oldSlug != "" && newSlug == oldSlug {
		if oldSlugSymbol != nil && *oldSlugSymbol == oldFundamental.Symbol {
			span.SetStatus(codes.Ok, "Success")

			oldSlug = cleanString(oldSlug)

			allSlugToSymbolMap.AddValue(oldSlug, oldFundamental.Symbol)
		}
	}

	// Only for newly added asset.
	if newSlug != oldSlug || (newSlug == oldSlug && *oldSlugSymbol != oldFundamental.Symbol) {
		var (
			iterator = 2
			nextSlug = newSlug + "-" + strconv.Itoa(iterator)
		)

		nextSlug = cleanString(nextSlug)

		for {
			if allSlugToSymbolMap.GetValue(nextSlug) == nil || *allSlugToSymbolMap.GetValue(nextSlug) == "" {
				break
			}

			iterator++
			nextSlug = newSlug + "-" + strconv.Itoa(iterator)
		}

		allSlugToSymbolMap.AddValue(nextSlug, ticker.ID)
		span.SetStatus(codes.Ok, "Success")

		return nextSlug
	}

	return oldSlug
}

// cleanString
// Takes string
// Returns string
// It will take a string and clean it from any character that will cause an issue with FE.
func cleanString(s string) string {
	// Step 1: Convert to lowercase
	s = strings.ToLower(s)

	// Step 2: Remove leading and trailing slashes
	s = strings.Trim(s, "/")

	// Step 3: Replace spaces and underscores with hyphens
	s = regexp.MustCompile(`[\s_]+`).ReplaceAllString(s, "-")

	// Step 4: Remove parentheses but keep their content
	s = regexp.MustCompile(`\(([^)]+)\)`).ReplaceAllString(s, "$1")

	// Step 5: Replace apostrophes, forward slashes, periods, and other non-alphanumeric characters (except hyphens) with hyphens
	s = regexp.MustCompile(`['./']+`).ReplaceAllString(s, "-")
	s = regexp.MustCompile(`[^a-z0-9-]+`).ReplaceAllString(s, "-")

	// Step 6: Replace multiple consecutive hyphens with a single hyphen
	s = regexp.MustCompile(`-+`).ReplaceAllString(s, "-")

	// Step 7: Trim leading and trailing hyphens
	s = strings.Trim(s, "-")

	return s
}
