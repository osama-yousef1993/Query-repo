package services

import (
	"context"
	"errors"
	"fmt"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/dto"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/repository"
	"github.com/Forbes-Media/go-tools/log"
)

// PortfolioService is an interface that defines functions that execute business logic around portfolios
type SearchService interface {
	SearchCache(context.Context, dto.SearchRequest) (*[]byte, error)
	InitializeCaches(ctx context.Context)
	ReportSearchTerm(ctx context.Context, searchTerm string, resultsCount int, targetTable datastruct.SearchResultsTable)
	RebuildCaches(ctx context.Context)
}

// portfolioService implements the PortfolioServiceInterface
type searchService struct {
	dao              repository.DAO
	cachedCategories CacheService     // Data that is logically cached in memmory. This is to power oue search bar.
	cachedNFTs       NFTsPriceService // Data that is logically cached in memory. This is to power oue search bar.
}

// NewWatchlistService Attempts to Get Access to all WatchList functions
// Takes a repository.DAO so we can use our Query functions
// Returns (WatchListService)
//
// Takes the dao and return WatchListService with dao to access all our functions in WatchList to get data from our Storage
// Returns a WatchListService interface for WatchList
func NewSearchService(dao repository.DAO) SearchService {
	return &searchService{dao: dao}
}

func (s *searchService) SearchCache(ctx context.Context, req dto.SearchRequest) (*[]byte, error) {

	var (
		searchResults     *[]byte
		err               error
		searchResultTable datastruct.SearchResultsTable
		resCount          int
	)

	span, labels := common.GenerateSpan("V2 SearchService.SearchCache", ctx)
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 SearchService.SearchCache"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 SearchService.SearchCache"))
	defer span.End()

	switch req.Category {
	case dto.CategoriesTable:
		if s.cachedCategories == nil {
			s.cachedCategories = NewCategoriesCacheService(s.dao)
			s.cachedCategories.BuildCache(ctx)
		}
		searchResultTable = datastruct.CategoryTableSearch
		searchResults, resCount, err = s.cachedCategories.SearchTerm(ctx, req.Query, req.Paginate)
	// todo build ntf NewNFTCacheService/BuildCache/NFTTableSearch/SearchTermNFT
	case dto.Nft:
		if s.cachedNFTs == nil {
			s.cachedNFTs = NewNFTsService(s.dao)
			s.cachedNFTs.BuildCache(ctx, "")
		}
		searchResultTable = datastruct.NftSearch
		searchResults, resCount, err = s.cachedNFTs.SearchTerm(ctx, req.Query, req.Paginate)
	// todo ntfChains NewNFTChainsCacheService/BuildCache/NFTTableSearch/SearchTermNFT
	case dto.NFTChains:
		if s.cachedNFTs == nil {
			s.cachedNFTs = NewNFTsService(s.dao)
			s.cachedNFTs.BuildCache(ctx, "chains")
		}
		searchResultTable = datastruct.NftSearch
		searchResults, resCount, err = s.cachedNFTs.SearchChainsTerm(ctx, req.Query, req.Paginate)
	default:
		err = errors.New("invalid query")
	}
	if err != nil {
		log.ErrorL(labels, err.Error())
		log.EndTimeL(labels, "V2 SearchService.SearchCache", startTime, err)
		return nil, err
	}

	// send and forget the search term and count of results to big query
	go func(searchTerm string, resultsCount int, targetTable datastruct.SearchResultsTable) {
		s.ReportSearchTerm(context.Background(), searchTerm, resultsCount, targetTable)
	}(req.Query, resCount, searchResultTable)

	log.EndTimeL(labels, "V2 SearchService.SearchCache", startTime, err)
	return searchResults, nil
}

// InitializeCache Fetches data from databases, and builds out cached data objects
// Takes context
// Returns error
func (s *searchService) InitializeCaches(ctx context.Context) {
	go func() {
		s.cachedCategories = NewCategoriesCacheService(s.dao)
		s.cachedCategories.BuildCache(ctx)
	}()
}

// InitializeCache Only Builds Caches no initilaization
// Takes context
// Returns error
func (s *searchService) RebuildCaches(ctx context.Context) {
	if s.cachedCategories == nil {
		s.cachedCategories = NewCategoriesCacheService(s.dao)
	}
	go func() {
		s.cachedCategories.BuildCache(ctx)
	}()
}

// ReportSearchTerm:  Stores a search query to big query
func (s *searchService) ReportSearchTerm(ctx context.Context, searchTerm string, resultsCount int, targetTable datastruct.SearchResultsTable) {
	searchQuery := s.dao.NewSearchQuery()
	searchQuery.InsertSearchData(ctx, searchTerm, resultsCount, targetTable)
}
