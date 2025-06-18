package services

import (
	"context"
	"fmt"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/repository"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
)

type CarouselService interface {
	GetCarouselData(ctx context.Context) (*datastruct.TradedAssetsResp, error) // Get Carousel Data
	GetAssetsData(ctx context.Context) (*datastruct.AssetsData, error)
}

type carouselService struct {
	dao repository.DAO
}

func NewCarouselService(dao repository.DAO) CarouselService {
	return &carouselService{dao: dao}
}

// GetCarouselData Attempts to Get Carousel information
// Takes a context
// Returns (*datastruct.TradedAssetsResp, error)
//
// Takes the context and get the Carousel data
// Returns a *datastruct.TradedAssetsResp with all of the Carousel info
func (c *carouselService) GetCarouselData(ctx context.Context) (*datastruct.TradedAssetsResp, error) {
	span, labels := common.GenerateSpan("V2 CarouselService.GetCarouselData", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 CarouselService.GetCarouselData"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 CarouselService.GetCarouselData"))
	queryMGR := c.dao.NewCarouselQuery()

	dataFilters := datastruct.TradedAssetsFilters{Limit: 20, PageNum: 1, SortBy: "market_cap", Direction: "desc"}

	treadedAssetsResult, err := queryMGR.GetPGGetTradedAssets(ctx, dataFilters)
	if err != nil {
		log.ErrorL(labels, "Error V2 CarouselService.GetCarouselData Getting Treaded Assets Data from PG: %s", err)
		return nil, err
	}

	excludedAssets, err := queryMGR.GetFDAConfig_Carousel(ctx)
	if err != nil {
		log.ErrorL(labels, "Error V2 CarouselService.GetCarouselData Getting Config Carousel Data from FS: %s", err)
		return nil, err
	}

	carouselResult, err := queryMGR.GetCarouselData(ctx, *treadedAssetsResult, excludedAssets.CarouselExclusions)
	if err != nil {
		log.ErrorL(labels, "Error V2 CarouselService.GetCarouselData Getting Carousel Data: %s", err)
		return nil, err
	}

	log.EndTimeL(labels, "V2 CarouselService.GetCarouselData", startTime, nil)
	span.SetStatus(codes.Ok, "V2 CarouselService.GetCarouselData")

	return carouselResult, nil
}

// GetAssetsData Attempts to Get assets information
// Takes a context
// Returns ([]datastruct.TradedAssetsTable, error)
//
// Takes the context and get the assets data
// Returns a []datastruct.TradedAssetsTable with all of the assets info
func (c *carouselService) GetAssetsData(ctx context.Context) (*datastruct.AssetsData, error) {
	span, labels := common.GenerateSpan("V2 CarouselService.GetAssetsData", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 CarouselService.GetAssetsData"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 CarouselService.GetAssetsData"))
	queryMGR := c.dao.NewCarouselQuery()

	treadedAssetsResult, err := queryMGR.GetPGAssetsData(ctx)
	if err != nil {
		log.ErrorL(labels, "Error V2 CarouselService.GetAssetsData Getting Treaded Assets Data from PG: %s", err)
		return nil, err
	}
	log.EndTimeL(labels, "V2 CarouselService.GetAssetsData", startTime, nil)
	span.SetStatus(codes.Ok, "V2 CarouselService.GetAssetsData")

	return treadedAssetsResult, nil
}
