package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/dto"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
)

type AssetsService interface {
	BuildCache(ctx context.Context) error
	SearchTerm(ctx context.Context, searchTerm string, paginate dto.Paginate) (*[]byte, int, error)
	SearchCategoryTerm(ctx context.Context, searchTerm string, paginate dto.Paginate) (*[]byte, int, error)
}

type assetsService struct {
	dao                     repository.DAO
	SearchType              dto.SearchType                            // For asset, the type would be exact match. Otherwise we're keeping it Fuzzy.
	AssetsTable             map[string][]datastruct.TradedAssetsTable // Key = Search Assets Term. Value = Array of Assets related to the search term.
	AssetsByCategoryIDTable map[string][]datastruct.TradedAssetsTable // Key = Search Assets Term. Value = Array of Assets related to the search term.
	Words                   []string                                  //Array of all search terms in the dictionary
	CategoryWords           []string                                  //Array of all search terms in the dictionary
	Category                dto.DictionaryDataSet                     //Category of the dictionary
	Lock                    *sync.Mutex                               // A lock to be used when interacting with this object
	DefaultFuzzySearchLimit int                                       //Limit of how many objects can be returned from a fuzzy search
	Datasource              string                                    //Data Source is the source of the provider, or defined as calculated
}

func NewAssetsService(dao repository.DAO) AssetsService {
	return &assetsService{dao: dao, Category: dto.Ft, Lock: &sync.Mutex{}, DefaultFuzzySearchLimit: 100, Datasource: "calculated", SearchType: dto.Fuzzy}
}

// BuildAssetsCache Attempts to Build FT cached data
// Takes a (ctx0 context.Context)
// Returns error
//
// Takes
// - context
// Build FT cache data and array of string (Words).
// Returns an error if the build failed
func (a *assetsService) BuildCache(ctx context.Context) error {
	span, labels := common.GenerateSpan("V2 AssetsService.BuildAssetsCache", ctx)
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 AssetsService.BuildAssetsCache"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 AssetsService.BuildAssetsCache"))
	defer span.End()

	assets, err := a.dao.NewAssetsQuery().GetTradedAssets(ctx)
	if err != nil {
		log.ErrorL(labels, "Error V2 AssetsService.BuildAssetsCache Getting Assets data from PG %s", err.Error())
		return err
	}

	categories, err := a.dao.NewCryptoPriceQuery().GetCryptoCategories(ctx)
	if err != nil {
		log.ErrorL(labels, "Error V2 AssetsService.BuildCategoryAssetsCache Getting Categories data from PG %s", err.Error())
		return err
	}

	var (
		words                      = []string{}
		categoryWords              = []string{}
		dictionaryAssets           = make(map[string][]datastruct.TradedAssetsTable)
		dictionaryCategoriesAssets = make(map[string][]datastruct.TradedAssetsTable)
	)
	a.Lock.Lock()
	defer a.Lock.Unlock()
	dictionaryAssets, words = a.BuildAssetsCache(ctx, assets)
	dictionaryCategoriesAssets, categoryWords = a.BuildCategoryAssetsCache(ctx, categories, assets)

	a.Words = words
	a.CategoryWords = categoryWords
	a.AssetsTable = dictionaryAssets
	a.AssetsByCategoryIDTable = dictionaryCategoriesAssets

	log.EndTimeL(labels, "V2 AssetsService.BuildAssetsCache", startTime, err)
	span.SetStatus(codes.Ok, "success")
	return nil
}

// BuildAssetsCache Attempts to Build FT cached data
// Takes a (ctx0 context.Context)
// Returns error
//
// Takes
// - context
// Build FT cache data and array of string (Words).
// Returns an error if the build failed
func (a *assetsService) BuildAssetsCache(ctx context.Context, assets []datastruct.TradedAssetsTable) (map[string][]datastruct.TradedAssetsTable, []string) {
	span, labels := common.GenerateSpan("V2 AssetsService.BuildAssetsCache", ctx)
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 AssetsService.BuildAssetsCache"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 AssetsService.BuildAssetsCache"))
	defer span.End()
	var (
		words            = []string{}
		dictionaryAssets = make(map[string][]datastruct.TradedAssetsTable)
	)
	for _, asset := range assets {
		if asset.Name != "" {
			field := strings.ToLower(asset.Name)
			if !slices.Contains(words, field) {
				words = append(words, field)
			}
			dictionaryAssets[field] = append(dictionaryAssets[field], asset)
		}
		if asset.DisplaySymbol != "" {
			field := strings.ToLower(asset.DisplaySymbol)
			if !slices.Contains(words, field) {
				words = append(words, field)
			}
			dictionaryAssets[field] = append(dictionaryAssets[field], asset)
		}
	}

	log.EndTimeL(labels, "V2 AssetsService.BuildAssetsCache", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return dictionaryAssets, words
}

// BuildCategoryAssetsCache Attempts to Build FT Category cached data
// Takes a (ctx0 context.Context)
// Returns error
//
// Takes
// - context
// Build FT cache data and array of string (Words).
// Returns an error if the build failed
func (a *assetsService) BuildCategoryAssetsCache(ctx context.Context, categories []datastruct.CryptoCategories, assets []datastruct.TradedAssetsTable) (map[string][]datastruct.TradedAssetsTable, []string) {
	span, labels := common.GenerateSpan("V2 AssetsService.BuildCategoryAssetsCache", ctx)
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 AssetsService.BuildCategoryAssetsCache"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 AssetsService.BuildCategoryAssetsCache"))
	defer span.End()

	var (
		words            = []string{}
		dictionaryAssets = make(map[string][]datastruct.TradedAssetsTable)
	)
	// assign all the assets to the dictionary
	for _, category := range categories {
		if category.ID != "" {
			categoryKey := strings.ToLower(category.ID)
			words = append(words, categoryKey)
			assetSymbols := make(map[string]bool)

			// get all the symbols for the category
			for _, marketData := range category.Markets {
				assetSymbols[marketData.ID] = true
			}

			// append the fundamentals of an asset if it exists in the category.
			for _, asset := range assets {
				if asset.Symbol != "" && assetSymbols[asset.Symbol] {
					dictionaryAssets[categoryKey] = append(dictionaryAssets[categoryKey], asset)
				}
			}

			//Sort the assets by market cap since we're keeping this our default sort order.
			sort.Slice(dictionaryAssets[categoryKey], func(i, j int) bool {
				return compareFloat(dictionaryAssets[categoryKey][i].MarketCap, dictionaryAssets[categoryKey][j].MarketCap, "desc")
			})
		}
	}

	log.EndTimeL(labels, "V2 AssetsService.BuildCategoryAssetsCache", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return dictionaryAssets, words
}

// PaginateSortAssets Attempts to Build the result after it paginate and sort the NFTs data
// Takes a (ctx context.Context, allNFTs interface{}, paginate dto.Paginate, ignoreInitialAssets int)
// Returns interface
//
// Takes
// - context
// - Array of NFTs
// - paginate:  object that contains the limit for the data, sort Direction for the data and what we need to SortBy
// - ignoreInitialAssets : the number of result that will be ignored
// Returns interface{} this will the result that we have for Assets
func (a *assetsService) PaginateSortAssets(ctx context.Context, tradedAssets interface{}, paginate dto.Paginate, ignoreInitialAssets int) interface{} {
	span, labels := common.GenerateSpan("V2 AssetsService.PaginateSortAssets", ctx)
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 AssetsService.PaginateSortAssets"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 AssetsService.PaginateSortAssets"))
	defer span.End()

	validatePaginate(ctx, &paginate)

	allAssets, ok := tradedAssets.(*[]datastruct.TradedAssetsTable)
	if !ok {
		err := errors.New("V2 AssetsService.PaginateSortAssets Failed to Assert Type []datastruct.TradedAssetsTable ")
		span.SetStatus(codes.Error, err.Error())
		log.ErrorL(labels, err.Error())
		log.EndTimeL(labels, "V2 AssetsService.PaginateSortAssets", startTime, err)
		return nil
	}

	if len(*allAssets) == 0 {
		return allAssets
	}

	initialAssets := (*allAssets)[:ignoreInitialAssets]
	assets := (*allAssets)[ignoreInitialAssets:]
	if paginate.SortBy == "name" {
		assets = paginateSortCryptoByNames[datastruct.TradedAssetsTable](ctx, paginate, assets)
	} else {
		sort.Slice(assets, func(i, j int) bool { // sort the assets
			var result = j > i //defaults to sort by relevance.
			switch paginate.SortBy {
			case "volume":
				result = compareFloat(assets[i].Volume, assets[j].Volume, paginate.Direction)
			case "price":
				result = compareFloat(assets[i].Price, assets[j].Price, paginate.Direction)
			case "marketCap":
				result = compareFloat(assets[i].MarketCap, assets[j].MarketCap, paginate.Direction)
			case "rank":
				result = compareFloat(assets[i].MarketCap, assets[j].MarketCap, paginate.Direction)
			case "percentage":
				result = compareFloat(assets[i].Percentage, assets[j].Percentage, paginate.Direction)
			case "percentage_1h":
				result = compareFloat(assets[i].Percentage1H, assets[j].Percentage1H, paginate.Direction)
			case "percentage_7d":
				result = compareFloat(assets[i].Percentage7D, assets[j].Percentage7D, paginate.Direction)
			case "change":
				result = compareFloat(assets[i].ChangeValue, assets[j].ChangeValue, paginate.Direction)
			}
			return result
		})
	}

	assets = append(initialAssets, assets...)
	start := (paginate.PageNum - 1) * paginate.Limit // paginate the nfts
	end := start + paginate.Limit
	if start > len(assets) {
		log.ErrorL(labels, "V2 AssetsService.PaginateSortAssets Start limit more than Nfts data length")
		log.EndTimeL(labels, "V2 AssetsService.PaginateSortAssets", startTime, nil)
		return []datastruct.TradedAssetsTable{}
	}
	if end > len(assets) {
		end = len(assets)
	}
	log.EndTimeL(labels, "V2 AssetsService.PaginateSortAssets", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return assets[start:end]
}

// RemoveDuplicateAssets Attempts to Build an Array of unique Assets
// Takes a (ctx0 context.Context, nfts []datastruct.TradedAssetsTable)
// Returns []datastruct.TradedAssetsTable
//
// Takes
// - context
// - Array of NFTs
// Returns []datastruct.TradedAssetsTable it's an array on unique NFTs data.
func (a *assetsService) RemoveDuplicateAssets(ctx context.Context, assets []datastruct.TradedAssetsTable) []datastruct.TradedAssetsTable {
	span, labels := common.GenerateSpan("V2 AssetsService.RemoveDuplicateAssets", ctx)
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 AssetsService.RemoveDuplicateAssets"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 AssetsService.RemoveDuplicateAssets"))
	defer span.End()

	seen := make(map[string]bool)
	result := []datastruct.TradedAssetsTable{}

	for _, asset := range assets {
		_, seenAsset := seen[asset.Slug]
		if !seenAsset && asset.Status == "active" {
			seen[asset.Slug] = true
			result = append(result, asset)
		}
	}
	log.EndTimeL(labels, "V2 AssetsService.RemoveDuplicateAssets", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return result
}

// SearchTerm Attempts to Search in NFTs data and returns the data if the search term exist if not it returns the data
// Takes a (ctx context.Context, searchTerm string, paginate dto.Paginate)
// Returns (*[]byte, int, error)
//
// Takes
// - context
// - searchTerm for what we need to found in NFTs data.
// - paginate for what we need to found in NFTs data.
// Returns a Nfts response with all data we need.
func (a *assetsService) SearchTerm(ctx context.Context, searchTerm string, paginate dto.Paginate) (*[]byte, int, error) {

	var returnData = dto.SearchResponse{}

	span, labels := common.GenerateSpan("V2 AssetsService.SearchTerm", ctx)
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 AssetsService.SearchTerm"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 AssetsService.SearchTerm"))
	defer span.End()

	a.Lock.Lock()
	defer a.Lock.Unlock()

	if len(a.AssetsTable) == 0 {
		err := errors.New("no cache detected")
		log.ErrorL(labels, "V2 AssetsService.SearchTerm Error with cache: %s", err)
		log.EndTimeL(labels, "V2 AssetsService.SearchTerm", startTime, err)
		return nil, 0, err
	}

	searchTerm = strings.ToLower(searchTerm)

	var assets []datastruct.TradedAssetsTable
	exactMatchAssets, isID := a.AssetsTable[searchTerm]

	if isID {
		assets = append(exactMatchAssets, assets...)
	} else {
		exactMatchAssets, isExact := a.AssetsTable[searchTerm]
		if isExact {
			assets = append(exactMatchAssets, assets...)
		} else if searchTerm == "" {
			for _, assetList := range a.AssetsTable {
				assets = append(assets, assetList...)
			}
		} else if a.SearchType == dto.Fuzzy {
			assets = fuzzySearch(ctx, searchTerm, &a.Words, &a.AssetsTable, a.DefaultFuzzySearchLimit)
		}
	}

	totalAssets := len(assets)
	//if no data was found return
	if totalAssets <= 0 {
		jsonData, err := json.Marshal(returnData)

		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			log.ErrorL(labels, "V2 AssetsService.SearchTerm Converting empty data : %s", err.Error())
			log.EndTimeL(labels, "V2 AssetsService.SearchTerm", startTime, err)
			return nil, 0, err
		}
		return &jsonData, totalAssets, nil
	}

	sortedResult := a.PaginateSortAssets(ctx, &assets, paginate, 0)

	assetsFundamentals, ok := sortedResult.([]datastruct.TradedAssetsTable)
	assetsFundamentals = a.RemoveDuplicateAssets(ctx, assetsFundamentals)

	if !ok {
		err := errors.New("V2 AssetsService.SearchTerm Failed to Assert Type []datastruct.NFTPrices ")
		span.SetStatus(codes.Error, err.Error())
		log.ErrorL(labels, "V2 AssetsService.SearchTerm Data Sorted Error : %s", err.Error())
		log.EndTimeL(labels, "V2 AssetsService.SearchTerm", startTime, err)
		return nil, 0, err
	}

	/*
		We need the response from nft/Prices, exactly the same response from Traded Assets endpoint.
		- Source : for the data (Coingecko)
		- Total : the number of Assets that return after the search term
		- NFTs : the data to be displayED on the page.
	*/
	returnData = dto.SearchResponse{Source: a.Datasource, Total: totalAssets, Assets: &assetsFundamentals}

	jsonData, err := json.Marshal(returnData)

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		log.ErrorL(labels, "V2 AssetsService.SearchTerm Converting data Error : %s", err.Error())
		log.EndTimeL(labels, "V2 AssetsService.SearchTerm", startTime, err)
		return nil, 0, err
	}
	log.EndTimeL(labels, "V2 AssetsService.SearchTerm", startTime, err)
	span.SetStatus(codes.Ok, "success")
	return &jsonData, len(assetsFundamentals), nil
}

// SearchTerm Attempts to Search in NFTs data and returns the data if the search term exist if not it returns the data
// Takes a (ctx context.Context, searchTerm string, paginate dto.Paginate)
// Returns (*[]byte, int, error)
//
// Takes
// - context
// - searchTerm for what we need to found in NFTs data.
// - paginate for what we need to found in NFTs data.
// Returns a Nfts response with all data we need.
func (a *assetsService) SearchCategoryTerm(ctx context.Context, searchTerm string, paginate dto.Paginate) (*[]byte, int, error) {

	var returnData = dto.SearchResponse{}

	span, labels := common.GenerateSpan("V2 AssetsService.SearchTerm", ctx)
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 AssetsService.SearchTerm"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 AssetsService.SearchTerm"))
	defer span.End()

	a.Lock.Lock()
	defer a.Lock.Unlock()

	if len(a.AssetsByCategoryIDTable) == 0 {
		err := errors.New("no cache detected")
		log.ErrorL(labels, "V2 AssetsService.SearchTerm Error with cache: %s", err)
		log.EndTimeL(labels, "V2 AssetsService.SearchTerm", startTime, err)
		return nil, 0, err
	}
	var assets []datastruct.TradedAssetsTable

	categoryAssets, exactMatchFound := a.AssetsByCategoryIDTable[paginate.CategoryID]
	if !exactMatchFound {
		err := errors.New("V2 AssetsService.SearchChainsTerm Can't found the Category ID")
		log.ErrorL(labels, err.Error())
		log.EndTimeL(labels, "V2 AssetsService.SearchChainsTerm", startTime, err)
		return nil, 0, err
	}

	searchTerm = strings.ToLower(searchTerm)
	assetsResult, words := a.BuildAssetsCache(ctx, categoryAssets)

	exactMatchAssets, isID := assetsResult[searchTerm]

	if isID {
		assets = append(exactMatchAssets, assets...)
	} else {
		exactMatchAssets, isExact := assetsResult[searchTerm]
		if isExact {
			assets = append(exactMatchAssets, assets...)
		} else if searchTerm == "" {
			for _, assetList := range assetsResult {
				assets = append(assets, assetList...)
			}
		} else if a.SearchType == dto.Fuzzy {
			assets = fuzzySearch(ctx, searchTerm, &words, &assetsResult, a.DefaultFuzzySearchLimit)
		}
	}

	totalAssets := len(assets)
	//if no data was found return
	if totalAssets <= 0 {
		jsonData, err := json.Marshal(returnData)

		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			log.ErrorL(labels, "V2 AssetsService.SearchTerm Converting empty data : %s", err.Error())
			log.EndTimeL(labels, "V2 AssetsService.SearchTerm", startTime, err)
			return nil, 0, err
		}
		return &jsonData, totalAssets, nil
	}

	sortedResult := a.PaginateSortAssets(ctx, &assets, paginate, 0)

	assetsFundamentals, ok := sortedResult.([]datastruct.TradedAssetsTable)
	assetsFundamentals = a.RemoveDuplicateAssets(ctx, assetsFundamentals)

	if !ok {
		err := errors.New("V2 AssetsService.SearchTerm Failed to Assert Type []datastruct.NFTPrices ")
		span.SetStatus(codes.Error, err.Error())
		log.ErrorL(labels, "V2 AssetsService.SearchTerm Data Sorted Error : %s", err.Error())
		log.EndTimeL(labels, "V2 AssetsService.SearchTerm", startTime, err)
		return nil, 0, err
	}

	/*
		We need the response from nft/Prices, exactly the same response from Traded Assets endpoint.
		- Source : for the data (Coingecko)
		- Total : the number of Assets that return after the search term
		- NFTs : the data to be displayED on the page.
	*/
	returnData = dto.SearchResponse{Source: a.Datasource, Total: totalAssets, Assets: &assetsFundamentals}

	jsonData, err := json.Marshal(returnData)

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		log.ErrorL(labels, "V2 AssetsService.SearchTerm Converting data Error : %s", err.Error())
		log.EndTimeL(labels, "V2 AssetsService.SearchTerm", startTime, err)
		return nil, 0, err
	}
	log.EndTimeL(labels, "V2 AssetsService.SearchTerm", startTime, err)
	span.SetStatus(codes.Ok, "success")
	return &jsonData, len(assetsFundamentals), nil
}
