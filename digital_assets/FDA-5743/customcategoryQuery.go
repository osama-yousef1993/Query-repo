package repository

import (
	"context"
	"fmt"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
)

type CustomCategoryQuery interface {
	BuildDynamicQuery(ctx context.Context, customCategoryFields *datastruct.CustomCategoryRequest) string
	FetchDataByTableName(ctx context.Context, query string, customCategoryFields datastruct.CustomCategoryRequest) (interface{}, string, error)
}

type customCategoryQuery struct{}

// todo function to build dynamic query
func (c *customCategoryQuery) BuildDynamicQuery(ctx context.Context, customCategoryFields *datastruct.CustomCategoryRequest) string {
	span, labels := common.GenerateSpan("V2 CustomCategoryQuery.BuildDynamicQuery", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 CustomCategoryQuery.BuildDynamicQuery"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 CustomCategoryQuery.BuildDynamicQuery"))
	columns, column := GetSelectedColumnsByTableName(ctx, customCategoryFields.TableName, customCategoryFields.Column)
	query := fmt.Sprintf(`%s From %s`, columns, customCategoryFields.TableName)

	if customCategoryFields.Condition != "" && customCategoryFields.ConditionValue != "" {
		w := fmt.Sprintf(" Where %s %s '%s'", column, customCategoryFields.Condition, customCategoryFields.ConditionValue)
		query = query + w
	}
	query = query + fmt.Sprintf(" Order by %s %s Limit %d", column, customCategoryFields.Sort, customCategoryFields.Limit)
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
		}
	}
	return columns, columnName
}

// todo build function to execute the query and return the result
func (c *customCategoryQuery) FetchDataByTableName(ctx context.Context, query string, customCategoryFields datastruct.CustomCategoryRequest) (interface{}, string, error) {
	span, labels := common.GenerateSpan("V2 FetchData", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 FetchData"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 FetchData"))

	var results interface{}
	var err error

	switch customCategoryFields.TableName {
	case "fundamentalslatest":
		results, err = FetchData[datastruct.TradedAssetsTable](ctx, query, customCategoryFields)
	case "nftdatalatest":
		results, err = FetchData[datastruct.NFTPrices](ctx, query, customCategoryFields)
	case "categories_fundamentals":
		results, err = FetchData[datastruct.CategoryFundamental](ctx, query, customCategoryFields)
	default:
		log.EndTimeL(labels, fmt.Sprintf("Starting %s", "V2 CustomCategoryQuery.FetchData"), startTime, nil)
		span.SetStatus(codes.Ok, "success")
		return nil, "", nil
	}

	if err != nil {
		span.SetStatus(codes.Error, "V2 CryptoPriceQuery.GetCryptoCategories")
		log.EndTime("Get Crypto Price Categories Query", startTime, err)
		return nil, customCategoryFields.TableName, err
	}

	log.EndTimeL(labels, fmt.Sprintf("Starting %s", "V2 CustomCategoryQuery.FetchData"), startTime, nil)
	span.SetStatus(codes.Ok, "success")

	return results, customCategoryFields.TableName, nil
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

		values := make([]interface{}, len(columns))
		for i := range values {
			values[i] = &res
		}

		if err := queryResult.Scan(values...); err != nil {
			span.SetStatus(codes.Error, "V2 CryptoPriceQuery.GetCryptoCategories scan error")
			log.EndTime("Get Crypto Price Categories Query", startTime, err)
			return nil, err
		}

		results = append(results, res)
	}

	log.EndTimeL(labels, fmt.Sprintf("Starting %s", "V2 CustomCategoryQuery.FetchData"), startTime, nil)
	span.SetStatus(codes.Ok, "success")

	return results, nil
}

// todo function to Insert data to categories table
