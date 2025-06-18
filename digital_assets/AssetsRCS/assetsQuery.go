package repository

import (
	"context"
	"fmt"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
)

type AssetsQuery interface {
	GetTradedAssets(ctx context.Context) ([]datastruct.TradedAssetsTable, error) // Get Traded Assets from PG
}

type assetsQuery struct {
}

// GetTradedAssets Gets all traded Assets Info from PG
// Takes a context
// Returns ([]datastruct.TradedAssetsTable, error)
//
// Get All traded Assets Info from PG
// Returns []datastruct.TradedAssetsTable and no error if successfully
func (a *assetsQuery) GetTradedAssets(ctx context.Context) ([]datastruct.TradedAssetsTable, error) {
	span, labels := common.GenerateSpan("V2 AssetsQuery.GetTradedAssets", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 AssetsQuery.GetTradedAssets"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 AssetsQuery.GetTradedAssets"))

	var assets []datastruct.TradedAssetsTable

	pg := PGConnect()
	query := fmt.Sprintf(`
		SELECT 
			symbol,
			display_symbol,						  
			name,
			slug,
			logo,
			temporary_data_delay,
			price_24h,
			percentage_1h,
			percentage_24h,
			percentage_7d,
			change_value_24h,						  
			market_cap,
			volume_1d,
			status,
			market_cap_percent_change_1d,
			(case when market_cap = 0 then null when market_cap != 0 then rank_number end) as rank_number
		from public.searchTradedAssetsBySource('%s')
	`, data_source)

	queryResult, err := pg.QueryContext(ctx, query)

	if err != nil {
		log.ErrorL(labels, "Error V2 AssetsQuery.GetTradedAssets Executing Query error from PG")
		return nil, err
	}

	for queryResult.Next() {
		var tradedAsset datastruct.TradedAssetsTable
		err := queryResult.Scan(&tradedAsset.Symbol, &tradedAsset.DisplaySymbol, &tradedAsset.Name, &tradedAsset.Slug, &tradedAsset.Logo, &tradedAsset.TemporaryDataDelay, &tradedAsset.Price, &tradedAsset.Percentage1H, &tradedAsset.Percentage, &tradedAsset.Percentage7D, &tradedAsset.ChangeValue, &tradedAsset.MarketCap, &tradedAsset.Volume, &tradedAsset.Status, &tradedAsset.MarketCapPercentage1d, &tradedAsset.Rank)
		if err != nil {
			log.ErrorL(labels, "Error V2 AssetsQuery.GetTradedAssets Scanning Data from PG")
			return nil, err
		}
		assets = append(assets, tradedAsset)
	}

	log.EndTimeL(labels, "V2 AssetsQuery.GetTradedAssets", startTime, nil)
	span.SetStatus(codes.Ok, "V2 AssetsQuery.GetTradedAssets")
	return assets, nil
}
