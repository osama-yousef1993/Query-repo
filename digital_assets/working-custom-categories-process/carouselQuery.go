package repository

import (
	"context"
	"fmt"
	"slices"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
	"google.golang.org/api/iterator"
)

type CarouselQuery interface {
	GetPGGetTradedAssets(ctx context.Context, dataFilters datastruct.TradedAssetsFilters) (*datastruct.TradedAssetsResp, error)             // Get the treaded Assets from PG
	GetFDAConfig_Carousel(ctx context.Context) (*datastruct.FDAConfig_Carousel, error)                                                      // Get the config Carousel data from FS
	GetCarouselData(ctx context.Context, assets datastruct.TradedAssetsResp, excludedAssets []string) (*datastruct.TradedAssetsResp, error) // Build the Carousel response
	GetPGAssetsData(ctx context.Context) (*datastruct.AssetsData, error)
}

type carouselQuery struct{}

// GetPGGetTradedAssets Gets all Treaded Assets Data from PG for Carousel
// Takes a (ctx context.Context, lim int, pageNum int, sortBy string, direction string)
// - lim the response limit the number of record we need to return from the response
// - pageNum the page number we need to fetch data from
// - sortBy the value we need to use to sort the data.
// - direction how the data will be sorted asc or desc
// Returns (*datastruct.TradedAssetsResp, Error)
//
// Gets the Treaded Assets Data from PG
// Returns the *datastruct.TradedAssetsResp and no error if successful
func (c *carouselQuery) GetPGGetTradedAssets(ctx context.Context, dataFilters datastruct.TradedAssetsFilters) (*datastruct.TradedAssetsResp, error) {
	span, labels := common.GenerateSpan("V2 CarouselQuery.GetPGGetTradedAssets", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 CarouselQuery.GetPGGetTradedAssets"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 CarouselQuery.GetPGGetTradedAssets"))

	pg := PGConnect()
	var assets []datastruct.TradedAssetsTable

	query := `select 
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
				full_count from public.tradedAssetsPagination_BySource_1($1, $2, $3, $4, $5)` //The frontend starts at 1, while the query will consider the pagenum always has to subtract 1

	queryResult, err := pg.QueryContext(ctx, query, dataFilters.Limit, dataFilters.PageNum-1, dataFilters.SortBy, dataFilters.Direction, data_source)

	var nomics datastruct.TradedAssetsTable

	if err != nil {
		log.ErrorL(labels, "Error V2 CarouselQuery.GetPGGetTradedAssets Getting Carousel Data from PG: %s", err)
		return nil, err
	}
	defer queryResult.Close()

	for queryResult.Next() {
		err := queryResult.Scan(&nomics.Symbol, &nomics.DisplaySymbol, &nomics.Name, &nomics.Slug, &nomics.Logo, &nomics.TemporaryDataDelay, &nomics.Price, &nomics.Percentage1H, &nomics.Percentage, &nomics.Percentage7D, &nomics.ChangeValue, &nomics.MarketCap, &nomics.Volume, &nomics.FullCount)
		if err != nil {
			log.ErrorL(labels, "Error V2 CarouselQuery.GetPGGetTradedAssets Mapping Carousel Data from PG: %s", err)
			return nil, err
		}
		assets = append(assets, nomics)
	}

	var resp = datastruct.TradedAssetsResp{Source: data_source, Total: *assets[0].FullCount, Assets: assets}
	log.EndTimeL(labels, "V2 CarouselQuery.GetPGGetTradedAssets", startTime, nil)
	span.SetStatus(codes.Ok, "V2 CarouselQuery.GetPGGetTradedAssets")
	return &resp, nil
}

// GetPGAssetsData Gets all Treaded Assets Data from PG
// Takes a (ctx context.Context)
// Returns ([]datastruct.TradedAssetsTable, Error)
//
// Gets the Treaded Assets Data from PG
// Returns the []datastruct.TradedAssetsTable and no error if successful
func (c *carouselQuery) GetPGAssetsData(ctx context.Context) (*datastruct.AssetsData, error) {
	span, labels := common.GenerateSpan("V2 CarouselQuery.GetPGAssetsData", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 CarouselQuery.GetPGAssetsData"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 CarouselQuery.GetPGAssetsData"))

	pg := PGConnect()
	var assetsData datastruct.AssetsData
	
	var assets []datastruct.Asset
	query := `select 
				symbol
			from 
				fundamentalslatest
			where status = 'active'
			order by symbol asc`

	queryResult, err := pg.QueryContext(ctx, query)

	var nomics datastruct.Asset

	if err != nil {
		log.ErrorL(labels, "Error V2 CarouselQuery.GetPGAssetsData Getting Carousel Data from PG: %s", err)
		return nil, err
	}
	defer queryResult.Close()

	for queryResult.Next() {
		err := queryResult.Scan(&nomics.Symbol)
		if err != nil {
			log.ErrorL(labels, "Error V2 CarouselQuery.GetPGAssetsData Mapping Carousel Data from PG: %s", err)
			return nil, err
		}
		assets = append(assets, nomics)
	}
	assetsData = datastruct.AssetsData{Asset: assets}

	log.EndTimeL(labels, "V2 CarouselQuery.GetPGAssetsData", startTime, nil)
	span.SetStatus(codes.Ok, "V2 CarouselQuery.GetPGAssetsData")
	return &assetsData, nil
}

// GetFDAConfig_Carousel Gets Config data for Carousel
// Takes a context
// Returns (*datastruct.FDAConfig_Carousel, Error)
//
// Gets the Config Carousel data from firestore
// Returns the *datastruct.FDAConfig_Carousel and no error if successful
func (c *carouselQuery) GetFDAConfig_Carousel(ctx context.Context) (*datastruct.FDAConfig_Carousel, error) {
	span, labels := common.GenerateSpan("V2 CarouselQuery.GetFDAConfig_Carousel", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 CarouselQuery.GetFDAConfig_Carousel"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 CarouselQuery.GetFDAConfig_Carousel"))
	fs := fsUtils.GetFirestoreClient()

	dbSnap := fs.Collection(datastruct.ConfigCollectionName).Documents(ctx)
	var config datastruct.FDAConfig_Carousel
	for {

		doc, err := dbSnap.Next()

		if err == iterator.Done {
			break
		}

		if err := doc.DataTo(&config); err != nil {
			log.ErrorL(labels, "Error V2 CarouselQuery.GetFDAConfig_Carousel Mapping Carousel Data from FS: %s", err)
			return nil, err
		}

		config.DocId = doc.Ref.ID

	}
	log.EndTimeL(labels, "V2 CarouselQuery.GetFDAConfig_Carousel", startTime, nil)
	span.SetStatus(codes.Ok, "V2 CarouselQuery.GetFDAConfig_Carousel")
	return &config, nil
}

// GetCarouselData Gets Carousel Data
// Takes a (ctx context.Context, assets datastruct.TradedAssetsResp, excludedAssets []string)
// - TradedAssetsResp the Asset we will use to build the Carousel data
// - excludedAssets the assets we will remove from the Carousel response
// Returns (*datastruct.TradedAssetsResp, Error)
//
// Gets the Carousel Data
// Returns the *datastruct.TradedAssetsResp and no error if successful
func (c *carouselQuery) GetCarouselData(ctx context.Context, assets datastruct.TradedAssetsResp, excludedAssets []string) (*datastruct.TradedAssetsResp, error) {
	span, labels := common.GenerateSpan("V2 CarouselQuery.GetFDAConfig_Carousel", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 CarouselQuery.GetFDAConfig_Carousel"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 CarouselQuery.GetFDAConfig_Carousel"))

	var assetsResult []datastruct.TradedAssetsTable
	for _, asset := range assets.Assets {
		//Only get top 10 assets
		if len(assetsResult) < 10 {
			//if the asset is not in the exclusion list include the asset
			if !slices.Contains(excludedAssets, asset.Symbol) {
				assetsResult = append(assetsResult, asset)
			}
		} else {
			break
		}
	}

	assets.Assets = assetsResult

	log.EndTimeL(labels, "V2 CarouselQuery.GetFDAConfig_Carousel", startTime, nil)
	span.SetStatus(codes.Ok, "V2 CarouselQuery.GetFDAConfig_Carousel")
	return &assets, nil

}
