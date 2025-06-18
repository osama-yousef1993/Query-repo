package repository

import (
	"context"
	"fmt"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
)

type ChartQuery interface {
	GetCategoriesCharts(ctx context.Context, interval string, symbol string, period string, assetsType string) ([]datastruct.TimeSeriesResultPG, error)
}

type chartQuery struct{}

// GetCategoriesCharts Gets all Categories Charts for Categories price Page
// Takes a (context, interval, symbol, period, assetsType)
// Returns ([]datastruct.TimeSeriesResultPG, Error)
//
// Gets all Categories Chart data from Postgresql
// Returns Array of Categories Chart data for a Category.
func (c *chartQuery) GetCategoriesCharts(ctx context.Context, interval string, symbol string, period string, assetsType string) ([]datastruct.TimeSeriesResultPG, error) {

	span, labels := common.GenerateSpan("V2 CryptoPriceQuery.GetCryptoCategories", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 CryptoPriceQuery.GetCryptoCategories"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 CryptoPriceQuery.GetCryptoCategories"))

	var timeSeriesResults []datastruct.TimeSeriesResultPG

	pg := PGConnect()

	query := c.BuildChartQuery(ctx, assetsType)

	queryResult, err := pg.QueryContext(ctx, query, interval, symbol, assetsType)

	if err != nil {
		span.SetStatus(codes.Error, "V2 CryptoPriceQuery.GetCryptoCategories")
		log.EndTime("Get Crypto Price Categories Query", startTime, err)
		return nil, err
	}
	defer queryResult.Close()

	for queryResult.Next() {
		var timeSeriesResult datastruct.TimeSeriesResultPG
		err := queryResult.Scan(&timeSeriesResult.IsIndex, &timeSeriesResult.Source, &timeSeriesResult.TargetResolutionSeconds, (*datastruct.SlicePGResult)(&timeSeriesResult.Slice), &timeSeriesResult.Interval, &timeSeriesResult.Symbol, &timeSeriesResult.Status)
		if err != nil {
			span.SetStatus(codes.Error, "V2 CryptoPriceQuery.GetCryptoCategories scan error")
			log.EndTime("Get Crypto Price Categories Query", startTime, err)
			return nil, err
		}
		var newSlice []datastruct.FESlicePG
		for _, sliceObject := range timeSeriesResult.Slice {
			var slice datastruct.FESlicePG

			slice.Time = sliceObject.Time
			if assetsType == "CATEGORY" {
				slice.AvgClose = sliceObject.MarketCapUSD
			} else {
				slice.AvgClose = sliceObject.AvgClose
			}
			newSlice = append(newSlice, slice)
		}
		timeSeriesResult.Slice = nil
		timeSeriesResult.FESlice = newSlice
		timeSeriesResult.Source = data_source
		timeSeriesResults = append(timeSeriesResults, timeSeriesResult)
	}

	log.EndTime("Get Crypto Price Categories Query", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return timeSeriesResults, nil
}

// BuildChartQuery Build the Query we need to get the Chart for (FT chart, NFT chart and CATEGORY chart)
// Takes a (context, assetsType)
// Returns string
//
// Build the query we will use to get the chart data for the datatype we send ex FT, NFT, CATEGORY
// Returns string as query with the correct Stored Procedure to get the Chart Data from the type we need.
func (c *chartQuery) BuildChartQuery(ctx context.Context, assetType string) string {
	span, labels := common.GenerateSpan("V2 ChartQuery.BuildChartQuery", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 ChartQuery.BuildChartQuery"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 ChartQuery.BuildChartQuery"))
	var result string
	// this switch will determine what is the correct Stored Procedure we will use in our Query depends on assetType
	// Todo we will separate this Stored Procedure getFTNFTChartData to be two Stored Procedure one for FT and one for NFT
	switch assetType {
	case "CATEGORY":
		result = "public.getCategoriesChartData($1, $2, $3)"
	case "NFT":
		result = "public.getFTNFTChartData($1, $2, $3)"
	case "FT":
		result = "public.getFTNFTChartData($1, $2, $3)"
	default:
		result = ""
	}
	query := fmt.Sprintf(`
	select
		is_index, 
		source, 
		target_resolution_seconds, 
		prices,
		tm_interval,
		symbol,
		status  
	from %s`, result)

	log.EndTime("Build Chart Categories Query", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return query
}
