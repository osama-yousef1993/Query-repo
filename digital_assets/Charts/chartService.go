package services

import (
	"context"
	"time"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/repository"
	"github.com/Forbes-Media/forbes-digital-assets/services"
	"github.com/Forbes-Media/go-tools/log"
)

type ChartService interface {
	GetCategoriesChart(ctx context.Context, interval string, symbol string, period string, assetsType string) (*datastruct.TimeSeriesResultPG, error)
}

type chartService struct {
	dao repository.DAO
}

func NewChartServices(dao repository.DAO) ChartService {
	return &chartService{dao: dao}
}

// GetCategoriesChart Attempts to Get Categories Charts from PG
// Takes a (context, interval, symbol, period, assetsType)
// Returns ([]datastruct.TimeSeriesResultPG, error)
//
// Takes the
// - context
// - interval for the chart
// - symbol that we need the chart for
// - period for the chart ex 24h
// - assetsType for the chart type ex: FT, NFT CATEGORY
// Returns a []datastruct.TimeSeriesResultPG with all of the Categories Chart data from PG
func (c *chartService) GetCategoriesChart(ctx context.Context, interval string, symbol string, period string, assetsType string) (*datastruct.TimeSeriesResultPG, error) {
	categoriesChart, err := c.dao.NewChartQuery().GetCategoriesCharts(ctx, interval, symbol, period, assetsType)
	if err != nil {
		log.Error("%s", err)
		return nil, err
	}
	var result datastruct.TimeSeriesResultPG
	//if data is returned from the query run it through the filter
	if len(categoriesChart) > 0 {
		result = c.FilterChartData(ctx, categoriesChart, period, interval)
	}
	return &result, nil
}

// iterate through all results. If the status is not active return the max chart
// if there has been < 3 trades in the last 24 hours return trades for the last 2 days
func (c *chartService) FilterChartData(ctx context.Context, timeSeriesResults []datastruct.TimeSeriesResultPG, period string, interval string) datastruct.TimeSeriesResultPG {
	_, span := tracer.Start(ctx, "FilterChartData")
	defer span.End()
	defer services.LogPanics()

	var (
		notice         = ""
		result         datastruct.TimeSeriesResultPG
		responsePeriod = period
	)

	var (
		TS_24h datastruct.TimeSeriesResultPG
		TS_7d  datastruct.TimeSeriesResultPG
		TS_max datastruct.TimeSeriesResultPG
	)

	//if we dont have more that 24 hours worth of data amke 24 hr and max assignment
	//else make the 7 day assignment as well. This is to fic nil pointer assignment issue
	if len(timeSeriesResults) == 1 {
		TS_24h = timeSeriesResults[0]
		TS_max = timeSeriesResults[len(timeSeriesResults)-1]
	} else {
		TS_24h = timeSeriesResults[0]
		TS_7d = timeSeriesResults[1]
		TS_max = timeSeriesResults[len(timeSeriesResults)-1]
	}

	// Default the chart based on interval
	for _, cd := range timeSeriesResults {
		if cd.Interval == interval {
			result = cd
			break
		}
	}

	// if the status is not active return the max chart.
	if TS_24h.Status != "active" {
		result = TS_max
		responsePeriod = "max"
		notice = "The maximum trade history available is shown since the token is no longer actively traded."
	} else if len(TS_24h.FESlice) <= 3 && period == "24h" { // if there are less than or = to 3 candles, append more candles to 24 hour chart
		notice = "Due to low trade activity additional information is being provided."
		var includedDates []datastruct.FESlicePG

		for _, cd := range TS_7d.FESlice {
			//if the time is before the first time in 24hr chart data
			//and not older than 2 days include it in the 24 hour chart
			if cd.Time.Before(TS_24h.FESlice[0].Time) && cd.Time.After(TS_24h.FESlice[0].Time.Add(-time.Hour*24)) {
				includedDates = append(includedDates, cd)
			}
		}
		includedDates = append(includedDates, TS_24h.FESlice...)
		TS_24h.FESlice = includedDates
		result = TS_24h
	}

	//Catch all: If the chart being returned has <= 3 candles dont display data
	if len(result.FESlice) <= 3 {
		result.FESlice = nil
		notice = "Trade data is not currently available for this asset."
	}

	result.Notice = notice
	result.Period = responsePeriod
	return result

}
