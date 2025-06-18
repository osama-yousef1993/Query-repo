package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
	"google.golang.org/api/iterator"
)

type CustomCategoryQuery interface {
	// BuildDynamicQuery generates a SQL query dynamically based on the fields in the provided CustomCategoryRequest.
	BuildDynamicQuery(ctx context.Context, customCategoryFields *datastruct.CustomCategoryRequest) string

	// FetchDataByTableName executes the provided SQL query against the database and returns the resulting data.
	FetchDataByTableName(ctx context.Context, query string, customCategoryFields datastruct.CustomCategoryRequest) (interface{}, error)

	// InsertCustomCategories inserts a new CustomCategory record into the database.
	InsertCustomCategories(ctx context.Context, data *datastruct.CustomCategory) error

	// GetCustomCategories retrieves all CustomCategory records from the database that match the specified customCategoryType.
	GetCustomCategories(ctx context.Context, customCategoryType string) ([]datastruct.CustomCategory, error)

	// GetCustomCategories retrieves all CustomCategory records from the FS to use it for rebuild the data.
	GetCustomCategoriesDataFS(ctx context.Context) ([]datastruct.CustomCategoryRequest, error)
}

type customCategoryQuery struct{}

// BuildDynamicQuery constructs and returns a SQL query string dynamically based on the fields provided in the CustomCategoryRequest.
// Parameters:
//   - ctx: Context to manage the lifecycle and logging of the request.
//   - customCategoryFields: Pointer to a CustomCategoryRequest struct, which contains fields such as
//     Assets, TableName, ConditionColumn, ConditionSymbol, ConditionValue, OrderColumn, Sort, and Limit for query generation.
//
// Returns:
//   - A string representing the dynamically constructed SQL query.
func (c *customCategoryQuery) BuildDynamicQuery(ctx context.Context, customCategoryFields *datastruct.CustomCategoryRequest) string {
	span, labels := common.GenerateSpan("V2 CustomCategoryQuery.BuildDynamicQuery", ctx)
	defer span.End()
	span.AddEvent("Starting V2 CustomCategoryQuery.BuildDynamicQuery")
	startTime := log.StartTimeL(labels, "Starting V2 CustomCategoryQuery.BuildDynamicQuery")

	var (
		columns     string
		column      string
		orderColumn string
		query       string
	)

	// Check if specific assets are included in the request to tailor the query accordingly
	if len(customCategoryFields.Assets) > 0 {
		// Set columns and basic query for specific assets
		columns = datastruct.FundamentalsColumns
		query = fmt.Sprintf("%s FROM %s", columns, customCategoryFields.TableName)

		// Build the asset filter in the WHERE clause
		assetFilters := make([]string, len(customCategoryFields.Assets))
		for i, asset := range customCategoryFields.Assets {
			assetFilters[i] = fmt.Sprintf("'%s'", asset.Symbol)
		}
		query += fmt.Sprintf(" WHERE symbol IN (%s)", strings.Join(assetFilters, ","))

	} else {
		// If no specific assets are provided, dynamically select columns and conditions based on the table name
		columns, column, orderColumn = GetSelectedColumnsByTableName(ctx, customCategoryFields.TableName, customCategoryFields.ConditionColumn, customCategoryFields.OrderColumn)
		query = fmt.Sprintf("%s FROM %s", columns, customCategoryFields.TableName)

		// Build a conditional WHERE clause if provided in the request
		if customCategoryFields.ConditionSymbol != "" && customCategoryFields.ConditionValue != "" {
			query += fmt.Sprintf(" WHERE %s %s %s", column, customCategoryFields.ConditionSymbol, convertConditionValue(customCategoryFields.ConditionValue))
		}

		// Add ORDER BY and LIMIT clauses if specified
		if orderColumn != "" && customCategoryFields.Sort != "" && customCategoryFields.Limit > 0 {
			query += fmt.Sprintf(" ORDER BY %s %s LIMIT %d", orderColumn, customCategoryFields.Sort, customCategoryFields.Limit)
		}
	}

	log.EndTimeL(labels, "V2 CustomCategoryQuery.BuildDynamicQuery Finished", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return query
}

// convertConditionValue takes a string value and attempts to convert it to its appropriate type.
// It first tries to convert the string to an integer using strconv.Atoi.
// If integer conversion fails, it attempts to convert to a float64 using strconv.ParseFloat.
// If both numeric conversions fail, it returns the original string value.
// Returns interface{} which can be of type int, float64, or string based on successful conversion.
func convertConditionValue(value string) interface{} {
	// Try parse as integer
	if intVal, err := strconv.Atoi(value); err == nil {
		return intVal
	}

	// Try parse as float
	if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
		return floatVal
	}

	// If not numeric, return as string
	return fmt.Sprintf("'%s'", value)
}

// GetSelectedColumnsByTableName constructs the SELECT clause based on table-specific configuration mappings.
// This function uses the TableColumnMapper to look up the appropriate column names based on the provided table name.
//
// Parameters:
//   - ctx: Context for managing the request lifecycle.
//   - tableName: A string representing the name of the table to be queried.
//   - column: The name of the condition column as a string.
//   - orderColumn: The name of the order column as a string.
//
// Returns:
//   - A string representing the SELECT clause for the table’s columns.
//   - The mapped condition column as a string.
//   - The mapped order column as a string.
func GetSelectedColumnsByTableName(ctx context.Context, tableName string, column string, orderColumn string) (string, string, string) {
	var (
		tableColumns    string
		columnName      string
		orderColumnName string
	)

	// Fetch the table configuration from the TableColumnMapper
	tableConfig := TableColumnMapper(ctx)

	// Determine the columns, condition column, and order column based on the table name
	switch tableName {
	case "fundamentalslatest", "nftdatalatest", "categories_fundamentals":
		// Retrieve the table configuration for the given table name
		table, exists := tableConfig[tableName]
		if !exists {
			fmt.Printf("No configuration found for table name: %s\n", tableName)
			return "", "", ""
		}

		// Set the SELECT clause columns based on the table configuration
		tableColumns = table.Columns

		// Map the specified column to the configured column name or default if not found
		columnName = table.ColumnMap[column]
		if columnName == "" {
			columnName = table.DefaultValue
		}

		// Map the specified order column to the configured column name or default if not found
		orderColumnName = table.ColumnMap[orderColumn]
		if orderColumnName == "" {
			orderColumnName = table.DefaultValue
		}

	default:
		// Handle case where table name does not match any known configuration
		fmt.Printf("Unsupported table name: %s\n", tableName)
		return "", "", ""
	}

	return tableColumns, columnName, orderColumnName
}

// TableColumnMapper constructs and returns a map of table configurations for different tables.
// Each table configuration includes the list of columns, a map of column names to their corresponding database field names,
// and a default value to use when a specific column is not provided.
// It uses the context parameter to maintain any contextual data that may be required during the function's execution.
// This function helps in dynamically generating queries by providing column mappings and defaults for various tables.
//
// Returns:
//   - A map where the key is the table name (e.g., "fundamentalslatest", "nftdatalatest")
//     and the value is a TableConfig struct that holds the table's column names, column mappings, and default value.
func TableColumnMapper(ctx context.Context) map[string]datastruct.TableConfig {
	var tableConfig = make(map[string]datastruct.TableConfig)

	// Define the table configurations
	tableConfig["fundamentalslatest"] = datastruct.TableConfig{
		Columns: datastruct.FundamentalsColumns,
		ColumnMap: map[string]string{
			"name":       "name",
			"slug":       "slug",
			"symbol":     "symbol",
			"price":      "price_24h",
			"percentage": "percentage_24h",
			"marketCap":  "market_cap",
		},
		DefaultValue: "market_cap",
	}

	tableConfig["nftdatalatest"] = datastruct.TableConfig{
		Columns: datastruct.NFTColumns,
		ColumnMap: map[string]string{
			"name":       "name",
			"slug":       "slug",
			"symbol":     "symbol",
			"volume":     "volume_24h_usd",
			"total":      "total_sales_1d",
			"percentage": "pct_change_volume_usd_1d",
			"marketCap":  "market_cap_usd",
		},
		DefaultValue: "market_cap_usd",
	}

	tableConfig["categories_fundamentals"] = datastruct.TableConfig{
		Columns: datastruct.CategoryColumns,
		ColumnMap: map[string]string{
			"id":         "id",
			"name":       "name",
			"slug":       "slug",
			"volume":     "volume_24h",
			"price":      "price_24h",
			"percentage": "market_cap_percentage_24h",
			"marketCap":  "market_cap",
			"tokens":     "total_tokens",
		},
		DefaultValue: "market_cap",
	}

	return tableConfig
}

// FetchDataByTableName executes a SQL query against the database and returns the result.
// It takes the context, the SQL query, and the CustomCategoryRequest as input parameters.
// It uses the provided table name to determine the correct data structure for the result.
// The function returns the fetched data as an interface{} and any error that occurred during the process.
//
// Parameters:
//   - ctx: The context for tracking the request lifecycle and span for tracing.
//   - query: The SQL query to be executed against the database.
//   - customCategoryFields: The request parameters used to determine the table name and other details.
//
// Returns:
//   - The result of the query, which is returned as an interface{} to handle different types of data.
//   - An error if one occurred during the process; otherwise, it returns nil.
func (c *customCategoryQuery) FetchDataByTableName(ctx context.Context, query string, customCategoryFields datastruct.CustomCategoryRequest) (interface{}, error) {
	span, labels := common.GenerateSpan("V2 CustomCategoryQuery.FetchData", ctx)
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "V2 CustomCategoryQuery.FetchData"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 FetchData"))

	var results interface{}
	var err error

	// Determine the appropriate data structure based on the table name and fetch the data accordingly
	switch customCategoryFields.TableName {
	case "fundamentalslatest":
		// Fetch data for the 'fundamentalslatest' table
		results, err = FetchData[datastruct.Fundamentals](ctx, query, customCategoryFields)
	case "nftdatalatest":
		// Fetch data for the 'nftdatalatest' table
		results, err = FetchData[datastruct.NFTsTable](ctx, query, customCategoryFields)
	case "categories_fundamentals":
		// Fetch data for the 'categories_fundamentals' table
		results, err = FetchData[datastruct.CategoryFundamentalTable](ctx, query, customCategoryFields)
	default:
		// Return nil if the table name is not recognized
		log.EndTimeL(labels, fmt.Sprintf("Starting %s", "V2 CustomCategoryQuery.FetchData"), startTime, nil)
		span.SetStatus(codes.Ok, "success")
		return nil, nil
	}

	// Handle any errors that occurred during the data fetch
	if err != nil {
		span.SetStatus(codes.Error, "V2 CustomCategoryQuery.FetchData()")
		log.EndTime("Fetch Data Query", startTime, err)
		return nil, err
	}

	// Successfully finished the query execution
	log.EndTimeL(labels, fmt.Sprintf("%s Finished", "V2 CustomCategoryQuery.FetchData"), startTime, nil)
	span.SetStatus(codes.Ok, "success")

	return results, nil
}

// FetchData executes the provided SQL query against the database and maps the results to a slice of the specified struct type.
// It dynamically maps query results to the provided struct type and handles nullable fields appropriately.
// This function is generic, allowing it to fetch data for various custom category types, as specified by the table configuration.
//
// Parameters:
//   - ctx: Context for tracing and controlling the request lifecycle.
//   - query: SQL query string to execute.
//   - customCategoryFields: Input parameters used to construct the query and specify the custom category request.
//
// Returns:
//   - A slice of results mapped to the specified struct type T.
//   - An error, if one occurs during the process.
func FetchData[T any](ctx context.Context, query string, customCategoryFields datastruct.CustomCategoryRequest) ([]T, error) {
	span, labels := common.GenerateSpan("V2 FetchData", ctx)
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "V2 FetchData"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 FetchData"))

	// Establish a connection to the PostgreSQL database
	pg := PGConnect()

	// Execute the query and fetch the result set
	queryResult, err := pg.QueryContext(ctx, query)
	if err != nil {
		// Log and return error if query execution fails
		span.SetStatus(codes.Error, "V2 FetchData Query Execution Error")
		log.EndTime("V2 FetchData Query Execution Error", startTime, err)
		return nil, err
	}
	defer queryResult.Close()

	var results []T

	// Iterate over rows in the result set, mapping each to the specified struct type
	for queryResult.Next() {
		var res T

		columns, err := queryResult.Columns()
		if err != nil {
			// Log and return error if column retrieval fails
			span.SetStatus(codes.Error, "V2 FetchData Column Retrieval Error")
			log.EndTime("V2 FetchData Column Retrieval Error", startTime, err)
			return nil, err
		}

		values := make([]interface{}, len(columns))
		scanArgs := make([]interface{}, len(columns))
		for i := range values {
			values[i] = new(interface{})
			scanArgs[i] = values[i]
		}

		if err := queryResult.Scan(scanArgs...); err != nil {
			// Log and return error if scanning the row fails
			span.SetStatus(codes.Error, "V2 FetchData Row Scan Error")
			log.EndTime("V2 FetchData Row Scan Error", startTime, err)
			return nil, err
		}

		// Map each column's value and handle nullable fields
		rowMap := make(map[string]interface{})
		for i, col := range columns {
			val := *(values[i].(*interface{}))
			handleNullableValue(col, val, rowMap)
		}

		// Convert the row map to JSON and unmarshal it into the specified struct type
		jsonData, err := json.Marshal(rowMap)
		if err != nil {
			span.SetStatus(codes.Error, "V2 FetchData JSON Marshalling Error")
			log.EndTime("V2 FetchData JSON Marshalling Error", startTime, err)
			return nil, err
		}

		if err := json.Unmarshal(jsonData, &res); err != nil {
			span.SetStatus(codes.Error, "V2 FetchData JSON Unmarshalling Error")
			log.EndTime("V2 FetchData JSON Unmarshalling Error", startTime, err)
			return nil, err
		}

		results = append(results, res)
	}

	log.EndTimeL(labels, fmt.Sprintf("%s Finished", "V2 FetchData"), startTime, nil)
	span.SetStatus(codes.Ok, "success")

	return results, nil
}

// handleNullableValue converts nullable database values to their appropriate Go types and populates a map.
// This function processes each column in the result set, ensuring compatibility with JSON marshalling.
//
// Parameters:
//   - column: The column name from the result set.
//   - value: The raw value retrieved from the database.
//   - rowMap: A map for storing processed column data for each row.
func handleNullableValue(column string, value interface{}, rowMap map[string]interface{}) {
	switch v := value.(type) {
	case nil:
		rowMap[column] = nil
	case float64:
		rowMap[column] = &v
	case float32:
		f64 := float64(v)
		rowMap[column] = &f64
	case int:
		f64 := float64(v)
		rowMap[column] = &f64
	case int64:
		f64 := float64(v)
		rowMap[column] = &f64
	case int32:
		f64 := float64(v)
		rowMap[column] = &f64
	case uint64:
		f64 := float64(v)
		rowMap[column] = &f64
	case uint32:
		f64 := float64(v)
		rowMap[column] = &f64
	case string:
		rowMap[column] = v
	case []byte:
		var jsonVal interface{}
		if err := json.Unmarshal(v, &jsonVal); err == nil {
			rowMap[column] = jsonVal
		} else {
			rowMap[column] = string(v)
		}
	case time.Time:
		rowMap[column] = v
	default:
		fmt.Printf("Unhandled type for column %s: %T\n", column, v)
		rowMap[column] = v
	}
}

// InsertCustomCategories inserts or upserts a new CustomCategory record into the database.
// If a category with the same name already exists, it updates the existing record's fields, type, and timestamp.
// This function uses an upsert operation to ensure that duplicate category names do not create additional records.
//
// Parameters:
//   - ctx: Context for managing the request lifecycle.
//   - data: Pointer to a CustomCategory struct containing the data to insert or update.
//
// Returns:
//   - An error, if one occurs during the insertion or upsert operation.
func (c *customCategoryQuery) InsertCustomCategories(ctx context.Context, data *datastruct.CustomCategory) error {
	span, labels := common.GenerateSpan("V2 CustomCategoryQuery.InsertCustomCategories", ctx)
	defer span.End()

	span.AddEvent("Starting V2 CustomCategoryQuery.InsertCustomCategories")
	startTime := log.StartTimeL(labels, "Starting V2 CustomCategoryQuery.InsertCustomCategories")

	// Establish a connection to the PostgreSQL database
	pg := PGConnect()
	tableName := "custom_categories"

	// Prepare SQL statement and parameter placeholders for the insert/upsert operation
	valueString := make([]string, 0, 1)
	valueArgs := make([]interface{}, 0, 4)
	i := 0

	valString := fmt.Sprintf("($%d,$%d,$%d,$%d)", i*4+1, i*4+2, i*4+3, i*4+4)
	valueString = append(valueString, valString)

	valueArgs = append(valueArgs, data.CategoryName)
	categoryFields, _ := json.Marshal(data.CategoryFields)
	valueArgs = append(valueArgs, categoryFields)
	valueArgs = append(valueArgs, data.CategoryType)
	valueArgs = append(valueArgs, time.Now())

	upsertStatement := `
		ON CONFLICT (category_name)
		DO UPDATE SET 
			category_fields = EXCLUDED.category_fields,
			category_type = EXCLUDED.category_type,
			last_updated = EXCLUDED.last_updated
	`
	insertStatement := fmt.Sprintf(
		"INSERT INTO %s VALUES %s %s",
		tableName,
		strings.Join(valueString, ","),
		upsertStatement,
	)

	// Execute the insert or upsert SQL statement
	_, err := pg.ExecContext(ctx, insertStatement, valueArgs...)
	if err != nil {
		// Log and return error if the upsert operation fails
		log.ErrorL(labels, fmt.Sprintf("V2 CustomCategoryQuery.InsertCustomCategories Error: %s", err))
		log.EndTime("Upsert CustomCategories", startTime, err)
		return err
	}

	log.EndTimeL(labels, "V2 CustomCategoryQuery.InsertCustomCategories Finished", startTime, nil)
	span.SetStatus(codes.Ok, "V2 CustomCategoryQuery.InsertCustomCategories")

	return nil
}

// GetCustomCategories retrieves all CustomCategory records from the database that match the specified category type.
// This function fetches data based on the given `customCategoryType` and maps the result to a slice of CustomCategory structs.
//
// Parameters:
//   - ctx: Context for managing the request lifecycle.
//   - customCategoryType: A string representing the type of custom categories to retrieve.
//
// Returns:
//   - A slice of CustomCategory structs containing the retrieved data, or an error if one occurred.
func (c *customCategoryQuery) GetCustomCategories(ctx context.Context, customCategoryType string) ([]datastruct.CustomCategory, error) {
	span, labels := common.GenerateSpan("GetCustomCategories", ctx)
	defer span.End()

	span.AddEvent("Starting GetCustomCategories")
	startTime := log.StartTimeL(labels, "Starting GetCustomCategories")

	// Establish a connection to the PostgreSQL database
	pg := PGConnect()

	// Construct the SQL query to fetch the custom categories
	query := `
		SELECT 
			category_name,
			category_fields,
			category_type,
			last_updated
		FROM 
			public.custom_categories 
		WHERE 
			category_type = $1
	`

	// Execute the query and fetch the result set
	queryResult, err := pg.QueryContext(ctx, query, customCategoryType)
	if err != nil {
		// Log and return error if the query execution fails
		span.SetStatus(codes.Error, "Error executing query in GetCustomCategories")
		log.EndTime("GetCustomCategories", startTime, err)
		return nil, err
	}
	defer queryResult.Close()

	var customCategories []datastruct.CustomCategory

	// Process the result set and map each record to a CustomCategory struct
	for queryResult.Next() {
		var customCategory datastruct.CustomCategory
		var categoryFieldsJSON []byte

		// Scan the query result into the struct fields
		err := queryResult.Scan(&customCategory.CategoryName, &categoryFieldsJSON, &customCategory.CategoryType, &customCategory.LastUpdated)
		if err != nil {
			// Log and return error if scanning fails
			span.SetStatus(codes.Error, "Error scanning results in GetCustomCategories")
			log.EndTime("GetCustomCategories", startTime, err)
			return nil, err
		}

		// Unmarshal JSON data into CategoryFields within the CustomCategory struct
		err = json.Unmarshal(categoryFieldsJSON, &customCategory.CategoryFields)
		if err != nil {
			// Log and return error if JSON unmarshaling fails
			span.SetStatus(codes.Error, "Error unmarshaling JSON in GetCustomCategories")
			log.EndTime("GetCustomCategories", startTime, err)
			return nil, err
		}

		customCategories = append(customCategories, customCategory)
	}

	// Complete the logging and tracing before returning results
	log.EndTimeL(labels, "GetCustomCategories finished successfully", startTime, nil)
	span.SetStatus(codes.Ok, "GetCustomCategories completed successfully")

	return customCategories, nil
}

// GetCustomCategoriesDataFS retrieves all CustomCategory records from the FS.
// This function fetches data from FS and maps the result to a slice of CustomCategoryRequest structs.
//
// Parameters:
//   - ctx: Context for managing the request lifecycle.
//
// Returns:
//   - A slice of CustomCategoryRequest structs containing the retrieved data from FS, or an error if one occurred.
func (c *customCategoryQuery) GetCustomCategoriesDataFS(ctx context.Context) ([]datastruct.CustomCategoryRequest, error) {
	span, labels := common.GenerateSpan("V2 CustomCategoryQuery.GetCustomCategoriesDataFS", ctx)
	defer span.End()

	span.AddEvent("Starting V2 CustomCategoryQuery.GetCustomCategoriesDataFS")
	startTime := log.StartTimeL(labels, "Starting V2 CustomCategoryQuery.GetCustomCategoriesDataFS")

	var customCategoryRequests []datastruct.CustomCategoryRequest
	fs := fsUtils.GetFirestoreClient()

	dbSnap := fs.Collection(datastruct.CustomCategoryTable).Documents(ctx)

	for {
		var customCategoryRequest datastruct.CustomCategoryRequest
		doc, err := dbSnap.Next()

		if err == iterator.Done {
			break
		}
		if err := doc.DataTo(&customCategoryRequest); err != nil {
			log.Error("Error V2 EducationQuery.GetEducation from FS: %s", err)
			return nil, err
		}
		customCategoryRequests = append(customCategoryRequests, customCategoryRequest)
	}

	// Complete the logging and tracing before returning results
	log.EndTimeL(labels, "GetCustomCategoriesDataFS finished successfully", startTime, nil)
	span.SetStatus(codes.Ok, "GetCustomCategoriesDataFS completed successfully")

	return customCategoryRequests, nil
}
