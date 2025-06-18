package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Forbes-Media/fda-coin-paprika-ingestion/common"
	"github.com/Forbes-Media/fda-coin-paprika-ingestion/datastruct"
	"github.com/Forbes-Media/go-tools/log"
	"github.com/coinpaprika/coinpaprika-api-go-client/coinpaprika"
	"go.opentelemetry.io/otel/codes"
)

// CoinListQuery defines the interface for querying and managing coin list data.
// This interface includes methods to interact with an external API and a PostgreSQL database for retrieving
// and storing coin-related information.
type CoinListQuery interface {
	// GetCoinList retrieves a list of coins from the CoinPaprika API.
	GetCoinList(ctx context.Context) ([]*coinpaprika.Coin, error)

	// UpsertCoinList upsert a list of coins into the PostgreSQL database.
	UpsertCoinList(ctx context.Context, coins []*coinpaprika.Coin) error

	// GetCoinsIDFromPG retrieves a list of coin IDs from the PostgreSQL database.
	GetCoinsIDFromPG(ctx context.Context) ([]string, error)

	// GetCoinDataByID retrieves metadata for specific coins based on their IDs.
	GetCoinDataByID(ctx context.Context, coinsID []string) ([]*coinpaprika.Coin, map[string]string, error)

	// UpsertCoinsMetaData upsert coin metadata into the PostgreSQL database.
	UpsertCoinsMetaData(ctx context.Context, coins []datastruct.Coin, coinMap map[string]string) error

	GetHistoricalOHLCVData(ctx context.Context, coins []string) (map[string][]*coinpaprika.OHLCVEntry, error)

	UpsertHistoricalOHLCVData(ctx context.Context, coins []datastruct.CoinOHLCV) error
}

// coinListQuery provides a concrete implementation of the CoinListQuery interface.
// It interacts with the CoinPaprika API and PostgreSQL database for managing coin data.
type coinListQuery struct{}

// GetCoinList retrieves a list of coins from the CoinPaprika API and logs the process.
// Parameters:
//   - ctx: Context to manage the lifecycle of the request and enable logging/tracing.
//
// Returns:
//   - A slice of pointers to coinpaprika.Coin representing the list of coins retrieved.
//   - An error if the operation fails.
func (c *coinListQuery) GetCoinList(ctx context.Context) ([]*coinpaprika.Coin, error) {
	// Start tracing and logging for the GetCoinList function.
	span, labels := common.GenerateSpan("GetCoinList", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "GetCoinList"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "GetCoinList"))

	// Fetch the list of coins from the CoinPaprika API using a utility.
	coins, err := cPaprikaUtils.GetCoinList(ctx)
	if err != nil {
		// Log and set the span status in case of an error.
		span.SetStatus(codes.Error, "GetCoinList Error Getting Coins from CoinPaprika")
		log.EndTimeL(labels, "GetCoinList Error Getting Coins from CoinPaprika", startTime, err)
		return nil, err
	}

	// Mark the span as successful and log the completion.
	span.SetStatus(codes.Ok, "GetCoinList Finished")
	log.EndTimeL(labels, "GetCoinList Finished", startTime, nil)
	return coins, nil
}

// UpsertCoinList upsert a list of coins into the PostgreSQL database.
// Parameters:
//   - ctx: Context to manage the lifecycle of the request and enable logging/tracing.
//   - coins: A slice of pointers to coinpaprika.Coin representing the coins to be inserted.
//
// Functionality:
//   - Constructs and executes batched UPSERT queries to insert or update coin data into the PostgreSQL table.
//   - Uses ON CONFLICT to handle duplicate entries by updating existing records.
//
// Returns:
//   - An error if the operation fails; otherwise, returns nil.
func (c *coinListQuery) UpsertCoinList(ctx context.Context, coins []*coinpaprika.Coin) error {
	// Start tracing and logging for the UpsertCoinList function.
	span, labels := common.GenerateSpan("UpsertCoinList", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "UpsertCoinList"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "UpsertCoinList"))

	// Get PostgreSQL client.
	pg := pgUtils.GetPGClient()

	// Prepare variables for batch insertion.
	valueString := make([]string, 0, len(coins))                // Stores the placeholder value strings for each row.
	totalFields := 7                                            // Total number of columns in the PostgreSQL table.
	valueArgs := make([]interface{}, 0, len(coins)*totalFields) // Stores the actual values to be inserted.
	tableName := "coinpaprika_assets"                           // Target table name.
	var i = 0                                                   // Counter for placeholders.

	// Iterate through the coins to prepare the batch insert.
	for y := 0; y < len(coins); y++ {
		mult := i * totalFields
		var coin = coins[y]

		// Construct a value string for the current coin using placeholders.
		var valSting = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d)", mult+1, mult+2, mult+3, mult+4, mult+5, mult+6, mult+7)
		valueString = append(valueString, valSting)

		// Add coin details to the value arguments.
		valueArgs = append(valueArgs, coin.ID)       // ID of the coin.
		valueArgs = append(valueArgs, coin.Name)     // Name of the coin.
		valueArgs = append(valueArgs, coin.Symbol)   // Symbol of the coin.
		valueArgs = append(valueArgs, coin.Rank)     // Rank of the coin.
		valueArgs = append(valueArgs, coin.IsNew)    // Whether the coin is new.
		valueArgs = append(valueArgs, coin.IsActive) // Whether the coin is active.
		valueArgs = append(valueArgs, time.Now())    // Timestamp for last updated.
		i++

		// Check if the batch size exceeds PostgreSQL limit or if this is the last coin.
		if len(valueArgs) >= 65000 || y == len(coins)-1 {
			// Construct the final INSERT statement with ON CONFLICT handling.
			insertStatement := fmt.Sprintf(
				`INSERT INTO %s (id, name, symbol, rank, is_new, is_active, last_updated) VALUES %s`,
				tableName, strings.Join(valueString, ","),
			)
			updateStatement := `ON CONFLICT (id) DO UPDATE SET 
				name = EXCLUDED.name, 
				symbol = EXCLUDED.symbol, 
				rank = EXCLUDED.rank, 
				is_new = EXCLUDED.is_new, 
				is_active = EXCLUDED.is_active, 
				last_updated = EXCLUDED.last_updated`

			// Combine INSERT and ON CONFLICT statements.
			query := insertStatement + " " + updateStatement

			// Execute the batch insert query.
			_, inserterError := pg.ExecContext(ctx, query, valueArgs...)
			if inserterError != nil {
				// Log error if insertion fails.
				log.Error("UpsertCoinList: Error inserting coins into PostgreSQL: %s", inserterError)
				span.SetStatus(codes.Error, "UpsertCoinList: Error inserting coins into PostgreSQL")
				log.EndTimeL(labels, "UpsertCoinList: Error inserting coins into PostgreSQL", startTime, inserterError)
				return inserterError
			}

			// Reset batch variables for the next batch.
			valueString = make([]string, 0, len(coins))
			valueArgs = make([]interface{}, 0, len(coins)*totalFields)
			i = 0
		}
	}

	// Mark the span as successful and log the completion.
	span.SetStatus(codes.Ok, "UpsertCoinList Finished")
	log.EndTimeL(labels, "UpsertCoinList Finished", startTime, nil)
	return nil
}

// UpsertCoinsMetaData upsert a list of coins with metadata into the PostgreSQL database.
// Parameters:
//   - ctx: Context to manage the lifecycle of the request and enable logging/tracing.
//   - coins: A slice of datastruct.Coin representing the coins with metadata to be inserted.
//   - coinMap: A map of coin ID to CoinPaprika ID.
//
// Functionality:
//   - Constructs and executes batched UPSERT queries to insert or update coin metadata in the PostgreSQL table.
//   - Handles JSON marshalling for fields like Tags, Team, Parent, Links, and Whitepaper.
//   - Uses ON CONFLICT to handle duplicate entries by updating existing records.
//
// Returns:
//   - An error if the operation fails; otherwise, returns nil.
func (c *coinListQuery) UpsertCoinsMetaData(ctx context.Context, coins []datastruct.Coin, coinMap map[string]string) error {
	// Start tracing and logging for the UpsertCoinsMetaData function.
	span, labels := common.GenerateSpan("UpsertCoinsMetaData", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "UpsertCoinsMetaData"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "UpsertCoinsMetaData"))

	// Get PostgreSQL client.
	pg := pgUtils.GetPGClient()

	// Prepare variables for batch insertion.
	valueString := make([]string, 0, len(coins))                // Stores the placeholder value strings for each row.
	totalFields := 24                                           // Total number of columns in the PostgreSQL table.
	valueArgs := make([]interface{}, 0, len(coins)*totalFields) // Stores the actual values to be inserted.
	tableName := "coinpaprika_asset_metadata"                   // Target table name.
	var i = 0                                                   // Counter for placeholders.

	// Iterate through the coins to prepare the batch insert.
	for y := 0; y < len(coins); y++ {
		mult := i * totalFields
		var coin = coins[y]
		var coinpaprikaID = coinMap[*coin.ID]

		// Construct a value string for the current coin using placeholders.
		var valSting = fmt.Sprintf(
			"($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			mult+1, mult+2, mult+3, mult+4, mult+5, mult+6, mult+7, mult+8, mult+9, mult+10,
			mult+11, mult+12, mult+13, mult+14, mult+15, mult+16, mult+17, mult+18, mult+19, mult+20,
			mult+21, mult+22, mult+23, mult+24,
		)
		valueString = append(valueString, valSting)

		// Add coin details to the value arguments.
		valueArgs = append(valueArgs, coin.ID)       // ID of the coin.
		valueArgs = append(valueArgs, coinpaprikaID) // ID of the coin as FK for coinpaprika_assets table.
		valueArgs = append(valueArgs, coin.Name)     // Name of the coin.
		valueArgs = append(valueArgs, coin.Symbol)   // Symbol of the coin.
		valueArgs = append(valueArgs, coin.Rank)     // Rank of the coin.
		valueArgs = append(valueArgs, coin.IsNew)    // Whether the coin is new.
		valueArgs = append(valueArgs, coin.IsActive) // Whether the coin is active.
		valueArgs = append(valueArgs, coin.Logo)     // Logo URL of the coin.
		tags, _ := json.Marshal(coin.Tags)           // JSON-encoded tags.
		valueArgs = append(valueArgs, tags)
		team, _ := json.Marshal(coin.Team) // JSON-encoded team information.
		valueArgs = append(valueArgs, team)
		parent, _ := json.Marshal(coin.Parent) // JSON-encoded parent data.
		valueArgs = append(valueArgs, parent)
		valueArgs = append(valueArgs, coin.Description)       // Description of the coin.
		valueArgs = append(valueArgs, coin.Message)           // Message associated with the coin.
		valueArgs = append(valueArgs, coin.OpenSource)        // Open-source status.
		valueArgs = append(valueArgs, coin.StartedAt)         // Start date of the coin.
		valueArgs = append(valueArgs, coin.DevelopmentStatus) // Development status.
		valueArgs = append(valueArgs, coin.HardwareWallet)    // Hardware wallet compatibility.
		valueArgs = append(valueArgs, coin.ProofType)         // Proof type of the coin.
		valueArgs = append(valueArgs, coin.OrgStructure)      // Organizational structure.
		valueArgs = append(valueArgs, coin.HashAlgorithm)     // Hashing algorithm used.
		links, _ := json.Marshal(coin.Links)                  // JSON-encoded links.
		valueArgs = append(valueArgs, links)
		whitepaper, _ := json.Marshal(coin.Whitepaper) // JSON-encoded whitepaper.
		valueArgs = append(valueArgs, whitepaper)
		valueArgs = append(valueArgs, coin.FirstDataAt) // First data availability date.
		valueArgs = append(valueArgs, time.Now())       // Timestamp for last updated.
		i++

		// Check if the batch size exceeds PostgreSQL limit or if this is the last coin.
		if len(valueArgs) >= 65000 || y == len(coins)-1 {
			// Construct the final INSERT statement with ON CONFLICT handling.
			insertStatement := fmt.Sprintf(
				`INSERT INTO %s (id, coinpaprika_id, name, symbol, rank, is_new, is_active, logo, tags, team, parent, description, message, open_source, started_at, development_status, hardware_wallet, proof_type, org_structure, hash_algorithm, links, whitepaper, first_data_at, last_updated) VALUES %s`,
				tableName, strings.Join(valueString, ","),
			)
			updateStatement := `ON CONFLICT (id) DO UPDATE SET 
				name = EXCLUDED.name, 
				symbol = EXCLUDED.symbol, 
				rank = EXCLUDED.rank, 
				is_new = EXCLUDED.is_new, 
				is_active = EXCLUDED.is_active, 
				logo = EXCLUDED.logo, 
				tags = EXCLUDED.tags, 
				team = EXCLUDED.team, 
				parent = EXCLUDED.parent, 
				description = EXCLUDED.description, 
				message = EXCLUDED.message, 
				open_source = EXCLUDED.open_source, 
				started_at = EXCLUDED.started_at, 
				development_status = EXCLUDED.development_status, 
				hardware_wallet = EXCLUDED.hardware_wallet, 
				proof_type = EXCLUDED.proof_type, 
				org_structure = EXCLUDED.org_structure, 
				hash_algorithm = EXCLUDED.hash_algorithm, 
				links = EXCLUDED.links, 
				whitepaper = EXCLUDED.whitepaper, 
				first_data_at = EXCLUDED.first_data_at, 
				last_updated = EXCLUDED.last_updated`

			// Combine INSERT and ON CONFLICT statements.
			query := insertStatement + " " + updateStatement

			// Execute the batch insert query.
			_, inserterError := pg.ExecContext(ctx, query, valueArgs...)
			if inserterError != nil {
				// Log error if insertion fails.
				log.Error("UpsertCoinsMetaData: Error inserting coin metadata into PostgreSQL: %s", inserterError)
				span.SetStatus(codes.Error, "UpsertCoinsMetaData: Error inserting coin metadata into PostgreSQL")
				log.EndTimeL(labels, "UpsertCoinsMetaData: Error inserting coin metadata into PostgreSQL", startTime, inserterError)
				return inserterError
			}

			// Reset batch variables for the next batch.
			valueString = make([]string, 0, len(coins))
			valueArgs = make([]interface{}, 0, len(coins)*totalFields)
			i = 0
		}
	}

	// Mark the span as successful and log the completion.
	span.SetStatus(codes.Ok, "UpsertCoinsMetaData Finished")
	log.EndTimeL(labels, "UpsertCoinsMetaData Finished", startTime, nil)
	return nil
}

// GetCoinsIDFromPG retrieves a list of coin IDs from the PostgreSQL database.
// Parameters:
//   - ctx: Context to manage the lifecycle of the request and enable logging/tracing.
//
// Returns:
//   - A slice of coin IDs ([]string) if the operation is successful.
//   - An error if the operation fails.
func (c *coinListQuery) GetCoinsIDFromPG(ctx context.Context) ([]string, error) {
	// Start tracing and logging for the GetCoinsIDFromPG function.
	span, labels := common.GenerateSpan("GetCoinsIDFromPG", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "GetCoinsIDFromPG"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "GetCoinsIDFromPG"))

	// Get PostgreSQL client.
	pg := pgUtils.GetPGClient()

	// Define the query to retrieve data.
	query := datastruct.CoinPaprikaAssets

	// Execute the query.
	queryResult, err := pg.QueryContext(ctx, query)
	if err != nil {
		// Handle query execution error.
		span.SetStatus(codes.Error, "Error executing query in GetCoinsIDFromPG")
		log.EndTimeL(labels, "Error executing query in GetCoinsIDFromPG", startTime, err)
		return nil, err
	}
	defer queryResult.Close() // Ensure the query result is closed after processing.

	// Initialize a slice to store coin IDs.
	var coinsID []string

	// Iterate through the query results.
	for queryResult.Next() {
		var coin datastruct.Coins
		// Scan the current row into a datastruct.Coins object.
		err := queryResult.Scan(&coin.ID, &coin.Name, &coin.Symbol, &coin.Rank, &coin.IsNew, &coin.IsActive, &coin.LastUpdated)
		if err != nil {
			// Handle scan error.
			span.SetStatus(codes.Error, "Error scanning query result in GetCoinsIDFromPG")
			log.EndTimeL(labels, "Error scanning query result in GetCoinsIDFromPG", startTime, err)
			return nil, err
		}
		// Append the coin ID to the result slice.
		coinsID = append(coinsID, coin.ID)
	}

	// Check if there were any errors during iteration.
	if err = queryResult.Err(); err != nil {
		span.SetStatus(codes.Error, "Error iterating through query result in GetCoinsIDFromPG")
		log.EndTimeL(labels, "Error iterating through query result in GetCoinsIDFromPG", startTime, err)
		return nil, err
	}

	// Mark the span as successful and log the completion.
	span.SetStatus(codes.Ok, "GetCoinsIDFromPG Finished")
	log.EndTimeL(labels, "GetCoinsIDFromPG Finished", startTime, nil)
	return coinsID, nil
}

// GetCoinDataByID retrieves detailed coin data for the specified coin IDs from the CoinPaprika API.
// Parameters:
//   - ctx: Context to manage the lifecycle of the request and enable logging/tracing.
//   - coinsID: A slice of strings containing the IDs of the coins to retrieve.
//
// Returns:
//   - A slice of `datastruct.Coin` containing detailed coin data if successful.
//   - A map of coin ID to CoinPaprika ID.
//   - An error if the operation fails.
func (c *coinListQuery) GetCoinDataByID(ctx context.Context, coinsID []string) ([]*coinpaprika.Coin, map[string]string, error) {
	// Start tracing and logging for the GetCoinDataByID function.
	span, labels := common.GenerateSpan("GetCoinDataByID", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "GetCoinDataByID"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "GetCoinDataByID"))

	// Fetch coin data from the CoinPaprika API using the provided coin IDs.
	paprikaCoins, coinMap, err := cPaprikaUtils.GetCoinByID(ctx, coinsID)
	if err != nil {
		// Log and trace any error that occurs while fetching coin data.
		span.SetStatus(codes.Error, "GetCoinDataByID Error Getting Coins from CoinPaprika")
		log.EndTimeL(labels, "GetCoinDataByID Error Getting Coins from CoinPaprika", startTime, err)
		return nil, nil, err
	}

	// Mark the span as successful and log the completion.
	span.SetStatus(codes.Ok, "GetCoinDataByID Finished")
	log.EndTimeL(labels, "GetCoinDataByID Finished", startTime, nil)
	return paprikaCoins, coinMap, nil
}

func (c *coinListQuery) GetHistoricalOHLCVData(ctx context.Context, coins []string) (map[string][]*coinpaprika.OHLCVEntry, error) {
	span, labels := common.GenerateSpan("GetHistoricalOHLCVData", ctx)
	defer span.End()

	span.AddEvent("Starting GetHistoricalOHLCVData")
	startTime := log.StartTimeL(labels, "Starting GetHistoricalOHLCVData")

	historicalOHLCV, err := cPaprikaUtils.GetHistoricalOHLV(ctx, coins)
	if err != nil {
		span.SetStatus(codes.Error, "GetHistoricalOHLCVData Error Getting Coins Historical OHLCV from CoinPaprika")
		log.EndTimeL(labels, "GetHistoricalOHLCVData Error Getting Coins Historical OHLCV from CoinPaprika", startTime, err)
		return nil, err
	}

	log.EndTimeL(labels, "GetHistoricalOHLCVData Completed", startTime, nil)
	span.SetStatus(codes.Ok, "GetHistoricalOHLCVData Completed")
	return historicalOHLCV, nil
}

func (c *coinListQuery) UpsertHistoricalOHLCVData(ctx context.Context, coins []datastruct.CoinOHLCV) error {
	// Start tracing and logging for the UpsertHistoricalOHLCVData function.
	span, labels := common.GenerateSpan("UpsertHistoricalOHLCVData", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "UpsertHistoricalOHLCVData"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "UpsertHistoricalOHLCVData"))

	// Get PostgreSQL client.
	pg := pgUtils.GetPGClient()

	// Prepare variables for batch insertion.
	valueString := make([]string, 0, len(coins))                // Stores the placeholder value strings for each row.
	totalFields := 10                                           // Total number of columns in the PostgreSQL table.
	valueArgs := make([]interface{}, 0, len(coins)*totalFields) // Stores the actual values to be inserted.
	tableName := "coinpaprika_coins_historical_ohlcv"           // Target table name.
	var i = 0                                                   // Counter for placeholders.

	// Iterate through the coins to prepare the batch insert.
	for y := 0; y < len(coins); y++ {
		mult := i * totalFields
		var coin = coins[y]

		// Construct a value string for the current coin using placeholders.
		var valSting = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)", mult+1, mult+2, mult+3, mult+4, mult+5, mult+6, mult+7, mult+8, mult+9, mult+10)
		valueString = append(valueString, valSting)

		// Add coin OHLCV details to the value arguments.
		valueArgs = append(valueArgs, coin.ID)        // ID of the coin.
		valueArgs = append(valueArgs, coin.TimeOpen)  // Time open.
		valueArgs = append(valueArgs, coin.TimeClose) // Time close.
		valueArgs = append(valueArgs, coin.Open)      // Open price.
		valueArgs = append(valueArgs, coin.High)      // High price.
		valueArgs = append(valueArgs, coin.Low)       // Low price.
		valueArgs = append(valueArgs, coin.Close)     // Close price.
		valueArgs = append(valueArgs, coin.Volume)    // Volume.
		valueArgs = append(valueArgs, coin.MarketCap) // Market cap.
		valueArgs = append(valueArgs, time.Now())     // Timestamp for last updated.
		i++

		// Check if the batch size exceeds PostgreSQL limit or if this is the last coin.
		if len(valueArgs) >= 65000 || y == len(coins)-1 {
			// Construct the final INSERT statement with ON CONFLICT handling.
			insertStatement := fmt.Sprintf(
				`INSERT INTO %s (id, time_open, time_close, open, high, low, close, volume, market_cap, last_updated) VALUES %s`,
				tableName, strings.Join(valueString, ","),
			)
			// updateStatement := `ON CONFLICT (id) DO UPDATE SET
			// 	name = EXCLUDED.name,
			// 	symbol = EXCLUDED.symbol,
			// 	rank = EXCLUDED.rank,
			// 	is_new = EXCLUDED.is_new,
			// 	is_active = EXCLUDED.is_active,
			// 	last_updated = EXCLUDED.last_updated`

			// Combine INSERT and ON CONFLICT statements.
			// query := insertStatement +

			// Execute the batch insert query.
			_, inserterError := pg.ExecContext(ctx, insertStatement, valueArgs...)
			if inserterError != nil {
				// Log error if insertion fails.
				log.Error("UpsertHistoricalOHLCVData: Error inserting coins HistoricalOHLCV into PostgreSQL: %s", inserterError)
				span.SetStatus(codes.Error, "UpsertHistoricalOHLCVData: Error inserting coins HistoricalOHLCV into PostgreSQL")
				log.EndTimeL(labels, "UpsertHistoricalOHLCVData: Error inserting coins HistoricalOHLCV into PostgreSQL", startTime, inserterError)
				return inserterError
			}

			// Reset batch variables for the next batch.
			valueString = make([]string, 0, len(coins))
			valueArgs = make([]interface{}, 0, len(coins)*totalFields)
			i = 0
		}
	}

	// Mark the span as successful and log the completion.
	span.SetStatus(codes.Ok, "UpsertHistoricalOHLCVData Finished")
	log.EndTimeL(labels, "UpsertHistoricalOHLCVData Finished", startTime, nil)
	return nil
}
