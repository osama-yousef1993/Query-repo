package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
)

type CustomCategoryQuery interface {
	BuildDynamicQuery(ctx context.Context, customCategoryFields *datastruct.CustomCategoryRequest) string
	FetchDataByTableName(ctx context.Context, query string, customCategoryFields datastruct.CustomCategoryRequest) (interface{}, error)
	InsertCustomCategories(ctx context.Context, data *datastruct.CustomCategory) error
	GetCustomCategories(ctx context.Context, customCategoryType string) ([]datastruct.CustomCategory, error)
}

type customCategoryQuery struct{}

// todo function to build dynamic query
func (c *customCategoryQuery) BuildDynamicQuery(ctx context.Context, customCategoryFields *datastruct.CustomCategoryRequest) string {
	span, labels := common.GenerateSpan("V2 CustomCategoryQuery.BuildDynamicQuery", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 CustomCategoryQuery.BuildDynamicQuery"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 CustomCategoryQuery.BuildDynamicQuery"))
	var (
		columns string
		column  string
		query   string
	)
	if len(customCategoryFields.Assets) > 0 {
		columns = datastruct.FundamentalsColumns
		query = fmt.Sprintf(`%s From %s`, columns, customCategoryFields.TableName)
		assets := make([]string, len(customCategoryFields.Assets))
		for index, value := range customCategoryFields.Assets {
			assets[index] = fmt.Sprintf("'%s'", value)
		}
		w := fmt.Sprintf(" Where symbol in (%s)", strings.Join(assets, ","))
		query = fmt.Sprintf("%s%s", query, w)
	} else {
		columns, column = GetSelectedColumnsByTableName(ctx, customCategoryFields.TableName, customCategoryFields.Column)
		query = fmt.Sprintf(`%s From %s`, columns, customCategoryFields.TableName)

		if customCategoryFields.Condition != "" && customCategoryFields.ConditionValue != "" {
			w := fmt.Sprintf(" Where %s %s '%s'", column, customCategoryFields.Condition, customCategoryFields.ConditionValue)
			query = query + w
		}
		query = query + fmt.Sprintf(" Order by %s %s Limit %d", column, customCategoryFields.Sort, customCategoryFields.Limit)

	}
	log.EndTimeL(labels, fmt.Sprintf("Starting %s", "V2 CustomCategoryQuery.BuildDynamicQuery"), startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return query
}

func GetSelectedColumnsByTableName(ctx context.Context, tableName string, column string) (string, string) {
	var columns string
	var columnName string
	switch tableName {
	case "fundamentalslatest":
		columns = datastruct.FundamentalsColumns
		switch column {
		case "name":
			columnName = "name"
		case "slug":
			columnName = "slug"
		case "symbol":
			columnName = "symbol"
		case "price":
			columnName = "price_24h"
		case "percentage":
			columnName = "percentage_24h"
		case "marketCap":
			columnName = "market_cap"
		default:
			columnName = "market_cap"
		}
	case "nftdatalatest":
		columns = datastruct.NFTColumns
		switch column {
		case "name":
			columnName = "name"
		case "slug":
			columnName = "slug"
		case "symbol":
			columnName = "symbol"
		case "volume":
			columnName = "volume_24h_usd"
		case "total":
			columnName = "total_sales_1d"
		case "percentage":
			columnName = "pct_change_volume_usd_1d"
		case "marketCap":
			columnName = "market_cap_usd"
		default:
			columnName = "market_cap_usd"
		}
	case "categories_fundamentals":
		columns = datastruct.CategoryColumns
		switch column {
		case "name":
			columnName = "name"
		case "slug":
			columnName = "slug"
		case "volume":
			columnName = "volume_24h"
		case "price":
			columnName = "price_24h"
		case "percentage":
			columnName = "market_cap_percentage_24h"
		case "marketCap":
			columnName = "market_cap"
		case "tokens":
			columnName = "total_tokens"
		default:
			columnName = "market_cap"
		}
	}
	return columns, columnName
}

// todo build function to execute the query and return the result
func (c *customCategoryQuery) FetchDataByTableName(ctx context.Context, query string, customCategoryFields datastruct.CustomCategoryRequest) (interface{}, error) {
	span, labels := common.GenerateSpan("V2 FetchData", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 FetchData"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 FetchData"))

	var results interface{}
	var err error

	switch customCategoryFields.TableName {
	case "fundamentalslatest":
		results, err = FetchData[datastruct.Fundamentals](ctx, query, customCategoryFields)
	case "nftdatalatest":
		results, err = FetchData[datastruct.NFTsTable](ctx, query, customCategoryFields)
	case "categories_fundamentals":
		results, err = FetchData[datastruct.CategoryFundamentalTable](ctx, query, customCategoryFields)
	default:
		log.EndTimeL(labels, fmt.Sprintf("Starting %s", "V2 CustomCategoryQuery.FetchData"), startTime, nil)
		span.SetStatus(codes.Ok, "success")
		return nil, nil
	}

	if err != nil {
		span.SetStatus(codes.Error, "V2 CryptoPriceQuery.GetCryptoCategories")
		log.EndTime("Get Crypto Price Categories Query", startTime, err)
		return nil, err
	}

	log.EndTimeL(labels, fmt.Sprintf("Starting %s", "V2 CustomCategoryQuery.FetchData"), startTime, nil)
	span.SetStatus(codes.Ok, "success")

	return results, nil
}

func FetchData[T any](ctx context.Context, query string, customCategoryFields datastruct.CustomCategoryRequest) ([]T, error) {
	span, labels := common.GenerateSpan("V2 FetchData", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 FetchData"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 FetchData"))

	pg := PGConnect()

	queryResult, err := pg.QueryContext(ctx, query)
	if err != nil {
		span.SetStatus(codes.Error, "V2 CryptoPriceQuery.GetCryptoCategories")
		log.EndTime("Get Crypto Price Categories Query", startTime, err)
		return nil, err
	}

	defer queryResult.Close()

	var results []T

	for queryResult.Next() {
		var res T
		columns, err := queryResult.Columns()
		if err != nil {
			span.SetStatus(codes.Error, "V2 CryptoPriceQuery.GetCryptoCategories scan error")
			log.EndTime("Get Crypto Price Categories Query", startTime, err)
			return nil, err
		}

		// Create a slice of interface{} to hold the row values
		values := make([]interface{}, len(columns))
		// Create a slice of pointers to interface{} for scanning
		scanArgs := make([]interface{}, len(columns))
		for i := range values {
			values[i] = new(interface{})
			scanArgs[i] = values[i]
		}

		if err := queryResult.Scan(scanArgs...); err != nil {
			span.SetStatus(codes.Error, "V2 CryptoPriceQuery.GetCryptoCategories scan error")
			log.EndTime("Get Crypto Price Categories Query", startTime, err)
			return nil, err
		}
		// Create a map of column name to value
		// rowMap := make(map[string]interface{})
		// for i, col := range columns {
		// 	val := *(values[i].(*interface{}))
		// 	rowMap[col] = val

		// }
		// Create a map and handle nullable values
		rowMap := make(map[string]interface{})
		for i, col := range columns {
			val := *(values[i].(*interface{}))

			switch v := val.(type) {
			case nil:
				rowMap[col] = nil
			case float64:
				rowMap[col] = &v
			case float32:
				f64 := float64(v)
				rowMap[col] = &f64
			case int:
				f64 := float64(v)
				rowMap[col] = &f64
			case int64:
				f64 := float64(v)
				rowMap[col] = &f64
			case int32:
				f64 := float64(v)
				rowMap[col] = &f64
			case uint64:
				f64 := float64(v)
				rowMap[col] = &f64
			case uint32:
				f64 := float64(v)
				rowMap[col] = &f64
			case string:
				// Try to convert string to float64 if it's numeric
				// if f64, err := strconv.ParseFloat(v, 64); err == nil {
				// 	rowMap[col] = &f64
				// } else {
				rowMap[col] = v
				// }
			case []byte:
				// Try to convert []byte to float64 if it's numeric
				var jsonVal interface{}
				if err := json.Unmarshal(v, &jsonVal); err == nil {
					rowMap[col] = jsonVal
				} else {
					rowMap[col] = string(v)
				}
			case time.Time:
				rowMap[col] = v
			default:
				// For debugging purposes
				fmt.Printf("Unhandled type for column %s: %T\n", col, v)
				rowMap[col] = v
			}
		}
		// Convert the map to JSON
		jsonData, err := json.Marshal(rowMap)
		if err != nil {
			span.SetStatus(codes.Error, "CustomCategoryQuery.FetchData marshaling row to JSON")
			log.EndTime("CustomCategoryQuery.FetchData marshaling row to JSON", startTime, err)
			return nil, err
		}

		// Unmarshal JSON into the struct
		if err := json.Unmarshal(jsonData, &res); err != nil {
			span.SetStatus(codes.Error, "CustomCategoryQuery.FetchData Error unmarshaling JSON to struct")
			log.EndTime("CustomCategoryQuery.FetchData unmarshaling JSON to struct", startTime, err)
			return nil, err
		}

		results = append(results, res)
	}

	log.EndTimeL(labels, fmt.Sprintf("Starting %s", "V2 CustomCategoryQuery.FetchData"), startTime, nil)
	span.SetStatus(codes.Ok, "success")

	return results, nil
}

// todo function to Insert data to categories table
func FetchDataTest[T any](ctx context.Context, query string, customCategoryFields datastruct.CustomCategoryRequest) ([]T, error) {
	span, labels := common.GenerateSpan("V2 FetchData", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 FetchData"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 FetchData"))

	pg := PGConnect()
	queryResult, err := pg.QueryContext(ctx, query)
	if err != nil {
		span.SetStatus(codes.Error, "V2 CryptoPriceQuery.GetCryptoCategories")
		log.EndTime("Get Crypto Price Categories Query", startTime, err)
		return nil, err
	}
	defer queryResult.Close()

	var results []T
	for queryResult.Next() {
		var res T

		columns, err := queryResult.Columns()
		if err != nil {
			span.SetStatus(codes.Error, "V2 CryptoPriceQuery.GetCryptoCategories scan error")
			log.EndTime("Get Crypto Price Categories Query", startTime, err)
			return nil, err
		}

		// Create a slice of interface{} to hold the row values
		values := make([]interface{}, len(columns))
		scanArgs := make([]interface{}, len(columns))
		for i := range values {
			values[i] = new(interface{})
			scanArgs[i] = values[i]
		}

		if err := queryResult.Scan(scanArgs...); err != nil {
			span.SetStatus(codes.Error, "V2 CryptoPriceQuery.GetCryptoCategories scan error")
			log.EndTime("Get Crypto Price Categories Query", startTime, err)
			return nil, err
		}

		// Create a map and handle nullable values
		rowMap := make(map[string]interface{})
		for i, col := range columns {
			val := *(values[i].(*interface{}))

			// Handle different types of values
			switch v := val.(type) {
			case nil:
				rowMap[col] = nil
			case float64:
				rowMap[col] = &v
			case string:
				rowMap[col] = v
			case []byte:
				// Try to convert []byte to string
				rowMap[col] = string(v)
			case time.Time:
				rowMap[col] = v
			default:
				// For any other types, store as is
				rowMap[col] = v
			}
		}

		// Convert the map to JSON
		jsonData, err := json.Marshal(rowMap)
		if err != nil {
			span.SetStatus(codes.Error, "Error marshaling row to JSON")
			log.EndTime("Get Crypto Price Categories Query", startTime, err)
			return nil, err
		}

		// Use a custom decoder to handle null values
		decoder := json.NewDecoder(bytes.NewReader(jsonData))
		decoder.UseNumber()

		if err := decoder.Decode(&res); err != nil {
			span.SetStatus(codes.Error, "Error unmarshaling JSON to struct")
			log.EndTime("Get Crypto Price Categories Query", startTime, err)
			return nil, fmt.Errorf("decode error: %v, data: %s", err, string(jsonData))
		}

		results = append(results, res)
	}

	log.EndTimeL(labels, fmt.Sprintf("Starting %s", "V2 CustomCategoryQuery.FetchData"), startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return results, nil
}

func (c *customCategoryQuery) InsertCustomCategories(ctx context.Context, data *datastruct.CustomCategory) error {
	span, labels := common.GenerateSpan("InsertCustomCategories", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "InsertCustomCategories"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "InsertCustomCategories"))

	pg := PGConnect()
	tableName := "custom_categories"

	valueString := make([]string, 0, 1)
	valueArgs := make([]interface{}, 0, 4)
	i := 0

	var valString = fmt.Sprintf("($%d,$%d,$%d,$%d)", i*4+1, i*4+2, i*4+3, i*4+4)
	valueString = append(valueString, valString)

	valueArgs = append(valueArgs, data.CategoryName)
	categoryFields, _ := json.Marshal(data.CategoryFields)
	valueArgs = append(valueArgs, categoryFields)
	valueArgs = append(valueArgs, data.CategoryType)
	valueArgs = append(valueArgs, time.Now())

	upsertStatement := ` ON CONFLICT (category_name) DO UPDATE SET category_fields = EXCLUDED.category_fields, category_type = EXCLUDED.category_type, last_updated = EXCLUDED.last_updated `
	insertStatement := fmt.Sprintf(`Insert INTO %s Values %s %s`, tableName, strings.Join(valueString, ","), upsertStatement)
	_, inserterError := pg.ExecContext(ctx, insertStatement, valueArgs...)

	if inserterError != nil {
		log.ErrorL(labels, fmt.Sprintf("UpsertCategoryFundamentals TimeElapsed %s", inserterError))
		log.EndTime("Upsert Category Fundamentals", startTime, inserterError)
		return inserterError
	}

	log.EndTimeL(labels, fmt.Sprintf("Finished %s", "InsertCustomCategories"), startTime, nil)
	span.SetStatus(codes.Ok, "InsertCustomCategories")
	return nil
}

func (c *customCategoryQuery) GetCustomCategories(ctx context.Context, customCategoryType string) ([]datastruct.CustomCategory, error) {
	span, labels := common.GenerateSpan("GetCustomCategories ", ctx)
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "GetCustomCategories"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "GetCustomCategories"))

	pg := PGConnect()

	query := fmt.Sprintf(`SELECT category_name, category_fields, category_type, last_updated
	FROM public.custom_categories where category_type = '%s'`, customCategoryType)

	queryResult, err := pg.QueryContext(ctx, query)

	if err != nil {
		span.SetStatus(codes.Error, "GetCustomCategories()")
		log.EndTime("GetCustomCategories", startTime, err)
		return nil, err
	}

	defer queryResult.Close()

	var customCategories []datastruct.CustomCategory

	for queryResult.Next() {
		var customCategory datastruct.CustomCategory
		var CategoryFields []byte
		err := queryResult.Scan(&customCategory.CategoryName, &CategoryFields, &customCategory.CategoryType, &customCategory.LastUpdated)
		if err != nil {
			span.SetStatus(codes.Error, "GetCustomCategories()")
			log.EndTime("GetCustomCategories", startTime, err)
			return nil, err
		}

		err = json.Unmarshal(CategoryFields, &customCategory.CategoryFields)
		if err != nil {
			span.SetStatus(codes.Error, "GetCustomCategories()")
			log.EndTime("GetCustomCategories", startTime, err)
			return nil, err
		}

		customCategories = append(customCategories, customCategory)
	}

	log.EndTimeL(labels, fmt.Sprintf("finished %s", "GetCustomCategories"), startTime, nil)
	span.SetStatus(codes.Ok, fmt.Sprintf("Starting %s", "GetCustomCategories"))
	return customCategories, nil

}
