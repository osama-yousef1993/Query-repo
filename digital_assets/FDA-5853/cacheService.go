package services

//This is a sub service used to banage cached data.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/dto"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/repository"
	"github.com/Forbes-Media/go-tools/log"
	"github.com/lithammer/fuzzysearch/fuzzy"
	"go.opentelemetry.io/otel/codes"
)

// CacheService
//
// A cache service must have the following functionality/
// Build Cache: A function that builds out some sort of cached data
// SearchTerm: A function that takes a string and a pagination object. This should search for the string(The query Term) and sort the data based on a pagination object
// PaginateSortAssets: A function that should implement sorting logic based on the type of the cached data, and return the sorted list
type CacheService interface {
	BuildCache(context.Context) error                                                                                                    //A function that builds out the cached data
	SearchTerm(context.Context, string, dto.SearchRequest) (*[]byte, int, error)                                                         // A function that searches the cached data and calls paginateSortAssets to sort the data
	PaginateSortAssets(ctx context.Context, arrayOfData interface{}, sortInstructions dto.Paginate, ignoreInitialAssets int) interface{} //This function should take an arry of any type and sort that array based on buiness logic and rturn it.
}

// CacheFTsByCategoryService
//
// A cache service must have the following functionality/
// Build Cache: A function that builds out some sort of cached data
// SearchTerm: A function that takes a string and a pagination object. This should search for the string(The query Term) and sort the data based on a pagination object
// PaginateSortAssets: A function that should implement sorting logic based on the type of the cached data, and return the sorted list
// SearchCategoryTerm : A function that takes a string and a pagination object. This should search for the string(The query Term) and sort the data based on a pagination object for Category Assets
// We need this interface because there is new function we need to use for  assetsService and we don't need it for categoriesTableCacheService
// So we need this interface to include it in our CacheService in this case we can use it with out break any part from our code
type CacheFTsByCategoryService interface {
	CacheService                                                                                            // An interface that serve multiple functions
	SearchCategoryTerm(ctx context.Context, searchTerm string, paginate dto.Paginate) (*[]byte, int, error) // A function that searches the category assets cached data and calls paginateSortAssets to sort the data
}

// A caching service used for our categoris fundamentals
type categoriesTableCacheService struct {
	dao                     repository.DAO
	SearchType              dto.SearchType                              // For category, the type would be exact match. Otherwise we're keeping it Fuzzy.
	CategoriesTable         map[string][]datastruct.CategoryFundamental // Key = Search Term. Value = Array of Assets related to the search term.
	Words                   []string                                    // Array of all search terms in the dictionary
	CategoriesById          map[string][]datastruct.CategoryFundamental // A map of category data indexed by id.
	AssetsTable             map[string][]datastruct.TradedAssetsTable   // Key = Search Assets Term. Value = Array of Assets related to the search term.
	AssetsByCategoryIDTable map[string][]datastruct.TradedAssetsTable   // Key = Search category Assets Term. Value = Array of Assets related to the search term.
	Category                dto.DictionaryDataSet                       // Category of the dictionary
	Lock                    *sync.Mutex                                 // A lock to be used when interacting with this object
	DefaultFuzzySearchLimit int                                         // Limit of how many objects can be returned from a fuzzy search
	Datasource              string                                      // Data Source is the source of the provider, or defined as calculated
	ExcludedCategories      []string                                    // A list of excluded categories by id
}

func NewCategoriesCacheService(dao repository.DAO) CacheService {

	query := dao.NewSearchQuery()
	excludedCategories, err := query.GetFDAConfig_Categories()
	if err != nil {
		log.Warning("%s", err)
	}

	return &categoriesTableCacheService{dao: dao, Category: dto.CategoriesTable, Lock: &sync.Mutex{}, DefaultFuzzySearchLimit: 100, Datasource: "calculated", SearchType: dto.Fuzzy, ExcludedCategories: excludedCategories.CategoriesExclusions}
}

// highlightsCategoryFilter filters out categories based on the isHilighted flag.
// returns a list of datastruct.CategoryFundamentals
func (c *categoriesTableCacheService) highlightsCategoryFilter(isHighlighted *bool, categories []datastruct.CategoryFundamental) []datastruct.CategoryFundamental {

	var catgories []datastruct.CategoryFundamental
	for _, category := range categories {

		if isHighlighted == nil {
			catgories = append(catgories, category)
		} else if !*isHighlighted && !category.IsHighlighted {
			catgories = append(catgories, category)
		} else if *isHighlighted && category.IsHighlighted {
			catgories = append(catgories, category)
		}
	}
	return catgories
}

// Search a dictionary for the given term. Name of dictionary is provided by the dictionaryCategory. Returns the assets related to the search term.
func (c *categoriesTableCacheService) SearchTerm(ctx context.Context, searchTerm string, params dto.SearchRequest) (*[]byte, int, error) {

	var returnData = dto.SearchResponse{}

	span, labels := common.GenerateSpan("V2 categoriesCache.SearchTerm", ctx)
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 categoriesCache.SearchTerm"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 categoriesCache.SearchTerm"))
	defer span.End()
	c.Lock.Lock()
	defer c.Lock.Unlock()
	// RebuildCache if the dictionary is empty. This happens when the server is restarted.
	if len(c.CategoriesTable) == 0 {
		err := errors.New("no cache detected")
		span.SetStatus(codes.Error, err.Error())
		log.AlertL(labels, err.Error())
		log.EndTimeL(labels, "V2 categoriesCache.SearchTerm", startTime, err)
		return nil, 0, err
	}

	searchTerm = strings.ToLower(searchTerm)
	var assets []datastruct.CategoryFundamental
	exactMatchAssets, isID := c.CategoriesById[searchTerm] // Check the categoriesId map to see if the search term passed in from the user is is the unique id of eligible categories.
	if isID {                                              // if the searchTerm is a match we extract the value of the key and continue
		assets = append(exactMatchAssets, assets...)
	} else {
		//if we don't have a match by id we check to see if there is an exact match by asset name ex: if searchTerm = "layer 1 (l1)" returns the category info layer-1
		exactMatchAssets, isExact := c.CategoriesTable[searchTerm]
		if isExact { //if we have an exact match return the matched data
			assets = append(exactMatchAssets, assets...)
		} else if searchTerm == "" { //if empty string passed while searching, we return all the assets.
			for _, assetList := range c.CategoriesTable {
				assets = append(assets, assetList...)
			}
		} else if c.SearchType == dto.Fuzzy { // If we don't have an exact match do a fuzzy search
			assets = fuzzySearch[datastruct.CategoryFundamental](ctx, searchTerm, &c.Words, &c.CategoriesTable, c.DefaultFuzzySearchLimit)
		}
	}

	if params.IsHighlighted != nil {
		filteredCategories := c.highlightsCategoryFilter(params.IsHighlighted, assets)
		assets = filteredCategories

	}

	totalAssets := len(assets)
	//if no data was found return
	if totalAssets <= 0 {
		jsonData, err := json.Marshal(returnData)

		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			log.ErrorL(labels, err.Error())
			log.EndTimeL(labels, "V2 categoriesCache.SearchTerm", startTime, err)
			return nil, 0, err
		}
		return &jsonData, totalAssets, nil
	}
	sortedResults := c.PaginateSortAssets(ctx, &assets, params.Paginate, 0)

	categoryFundamentals, ok := sortedResults.([]datastruct.CategoryFundamental)
	if !ok {
		err := errors.New("V2 categoriesCache.SearchTerm Failed to Assert Type []datastruct.CategoryFundamental ")
		span.SetStatus(codes.Error, err.Error())
		log.ErrorL(labels, err.Error())
		log.EndTimeL(labels, "V2 categoriesCache.SearchTerm", startTime, err)
		return nil, 0, err
	}

	/*
		We need the response from search, exactly the same response from Traded Assets endpoint.
		- Source : for the data (Coingecko)
		- Total : the number of Assets that return after the search term
		- Assets : the data to be displayED on the page.
	*/
	returnData = dto.SearchResponse{Source: c.Datasource, Total: totalAssets, Categories: &categoryFundamentals}

	jsonData, err := json.Marshal(returnData)

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		log.ErrorL(labels, err.Error())
		log.EndTimeL(labels, "V2 categoriesCache.SearchTerm", startTime, err)
		return nil, 0, err
	}
	span.SetStatus(codes.Ok, "success")
	return &jsonData, len(categoryFundamentals), nil
}

// Fuzzy search for the given searchTerm in the given words. Returns the assets/nfts/categories related to the search term.
func fuzzySearch[T dto.SearchTable](ctx context.Context, searchTerm string, words *[]string, assets *map[string][]T, DefaultFuzzySearchLimit int) []T {
	_, span := tracer.Start(ctx, "fuzzySearch")
	defer span.End()

	var result []T
	ranks := fuzzy.RankFindNormalized(searchTerm, *words) // case-insensitive & unicode-normalized fuzzy search.
	sort.Sort(ranks)                                      // sorts by the Levenshtein distance
	for rankIdx, rank := range ranks {
		if rankIdx >= DefaultFuzzySearchLimit {
			break
		}
		result = append(result, (*assets)[rank.Target]...)
	}

	span.SetStatus(codes.Ok, "success")
	return result
}

// paginate and sort the assets
func (c *categoriesTableCacheService) PaginateSortAssets(ctx context.Context, allAssets interface{}, paginate dto.Paginate, ignoreInitialAssets int) interface{} {
	span, labels := common.GenerateSpan("V2 categoriesCache.SearchTerm", ctx)
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 categoriesCache.SearchTerm"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 categoriesCache.SearchTerm"))
	defer span.End()

	validatePaginate(ctx, &paginate) // validate the paginate object

	categoryFundamentals, ok := allAssets.(*[]datastruct.CategoryFundamental)
	if !ok {
		err := errors.New("V2 categoriesCache.PaginateSortAssets Failed to Assert Type []datastruct.CategoryFundamental ")
		span.SetStatus(codes.Error, err.Error())
		log.AlertL(labels, err.Error())
		log.EndTimeL(labels, "V2 categoriesCache.PaginateSortAssets", startTime, err)
		return nil
	}

	if len(*categoryFundamentals) == 0 {
		return categoryFundamentals
	}
	initialAssets := (*categoryFundamentals)[:ignoreInitialAssets]
	assets := (*categoryFundamentals)[ignoreInitialAssets:]

	if paginate.SortBy == "name" {
		assets = paginateSortCryptoByNames(ctx, paginate, assets)
	} else {

		sort.Slice(assets, func(i, j int) bool { // sort the assets
			var result = j > i //defaults to sort by relevance.
			switch paginate.SortBy {
			case "volume_24h":
				result = compareFloat(&assets[i].Volume24H, &assets[j].Volume24H, paginate.Direction)
			case "market_cap":
				result = compareFloat(&assets[i].MarketCap, &assets[j].MarketCap, paginate.Direction)
			case "market_cap_percentage_change":
				result = compareFloat(&assets[i].MarketCapPercentageChange, &assets[j].MarketCapPercentageChange, paginate.Direction)
			case "average_percentage_24h":
				result = compareFloat(&assets[i].AveragePercentage24H, &assets[j].AveragePercentage24H, paginate.Direction)
			case "average_price":
				result = compareFloat(&assets[i].AveragePrice, &assets[j].AveragePrice, paginate.Direction)
			case "total_tokens":
				result = compareInt(&assets[i].TotalTokens, &assets[j].TotalTokens, paginate.Direction)
			}

			return result
		})
	}
	assets = append(initialAssets, assets...)
	start := (paginate.PageNum - 1) * paginate.Limit // paginate the assets
	end := start + paginate.Limit
	if start > len(assets) {
		log.EndTimeL(labels, "V2 categoriesCache.PaginateSortAssets", startTime, nil)
		return []datastruct.CategoryFundamental{}
	}
	if end > len(assets) {
		end = len(assets)
	}
	log.EndTimeL(labels, "V2 categoriesCache.PaginateSortAssets", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return assets[start:end]
}

// compare float values for the sort.Slices function
// compare float values for the sort.Slices function
func compareFloat(val1 *float64, val2 *float64, direction string) bool {
	if val1 == nil && val2 == nil {
		return false
	}
	if direction == "desc" {
		// If val1 is nil and val2 is not, val1 comes first
		if val1 == nil && val2 != nil {
			return true
		}
		// If val1 is not nil and val2 is nil, val2 comes first
		if val1 != nil && val2 == nil {
			return false
		}
		return *val1 > *val2
	}
	// If val1 is nil and val2 is not, val1 comes last
	if val1 == nil && val2 != nil {
		return false
	}
	// If val1 is not nil and val2 is nil, val1 comes first
	if val1 != nil && val2 == nil {
		return true
	}
	return *val1 < *val2
}

// compare int values for the sort.Slices function
func compareInt(val1 *int, val2 *int, direction string) bool {
	if val1 == nil || val2 == nil {
		return false
	}
	if direction == "desc" {
		return *val1 > *val2
	}
	return *val1 < *val2
}

// Validate the paginate's limit, Page number and direction values.
func validatePaginate(ctx context.Context, paginate *dto.Paginate) {
	_, span := tracer.Start(ctx, "validatePaginate")
	defer span.End()

	if paginate.Direction != "asc" && paginate.Direction != "desc" {
		paginate.Direction = "desc"
	}
	if paginate.PageNum < 1 {
		paginate.PageNum = 1
	}
	if paginate.Limit < 1 {
		paginate.Limit = 1
	}
	span.SetStatus(codes.Ok, "success")
}

// the sort function will sort the data after we filter it with regex
// it will sort data by ASC order for all data
func cryptoSortFunctionality[T dto.SearchTable](assets []T) {
	sort.Slice(assets, func(i, j int) bool {
		var res = j > i
		assetNameI, assetNameJ := GETFieldDataString(assets[i], assets[j], "Name")
		res = strings.ToLower(assetNameI) < strings.ToLower(assetNameJ)
		return res
	})
}

// This function will returns the field and it's value from the Type we need to check the value for
// The values maybe be from Assets, NFTs or Categories
// Takes (fieldIndexI T, fieldIndexJ T, fieldName string)
// - fieldIndexI the I index from the type we need to check
// - fieldIndexI the J index from the same type we need to check
// - fieldName the field name we need to get from the SearchTable
// Returns the values from fieldIndexI and fieldIndexI
func GETFieldDataString[T dto.SearchTable](fieldIndexI T, fieldIndexJ T, fieldName string) (string, string) {
	fieldTypeI := reflect.TypeOf(fieldIndexI)
	fieldValueI := reflect.ValueOf(fieldIndexI)
	volumeI, _ := fieldTypeI.FieldByName(fieldName)
	indexI := volumeI.Index[0]
	valueI := fieldValueI.Field(indexI).Interface().(string)
	fieldTypeJ := reflect.TypeOf(fieldIndexJ)
	fieldValueJ := reflect.ValueOf(fieldIndexJ)
	volume1, _ := fieldTypeJ.FieldByName(fieldName)
	indexJ := volume1.Index[0]
	valueJ := fieldValueJ.Field(indexJ).Interface().(string)
	return valueI, valueJ
}

// Special Sort Function we will use it only if we need to sort by Crypto Names
func paginateSortCryptoByNames[T dto.SearchTable](ctx context.Context, paginate dto.Paginate, assets []T) []T {
	span, labels := common.GenerateSpan("V2 categoriesCache.paginateSortCryptoByNames", ctx)
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 categoriesCache.paginateSortCryptoByNames"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 categoriesCache.paginateSortCryptoByNames"))
	defer span.End()

	// build array of all cases that exist in Crypto name
	specialNames, numberNames, normalNames := buildCryptoDataUsingRegexFilter(assets)

	// Sort Names Start with Special Characters
	cryptoSortFunctionality[T](specialNames)
	// Sort Names start with numeric Characters
	cryptoSortFunctionality[T](numberNames)
	// Sort Names start with Alphabetic Characters
	cryptoSortFunctionality[T](normalNames)

	// Build Crypto response Sorted by name
	// If the sort ASC the result will return in this order --> normalName --> numericName --> specialName
	// If the sort DESC the result will return in this order --> numericName --> normalName --> specialName
	if paginate.Direction == "asc" {
		// It will sort data using ASC like: a-z --> 0-9 --> special
		assets = buildOrderedCryptoPrices[T](normalNames, numberNames, specialNames)
	} else if paginate.Direction == "desc" {
		// It will sort data using DESC like: 0-9 --> a-z --> special
		assets = buildOrderedCryptoPrices[T](numberNames, normalNames, specialNames)
	}
	log.EndTimeL(labels, "V2 categoriesCache.PaginateSortAssets", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return assets
}

// Use this function to build the Crypto for all names after the filter and the sort functions
// it will return an array of Crypto that ordered as we need
// We will use this function for all SearchTable Types (Assets, NFTs and Categories)
// Asc order will return ==> normalName --> numericName --> specialName
// Desc order will return ==> numericName --> normalName --> specialName
func buildOrderedCryptoPrices[T dto.SearchTable](normalNames []T, numericNames []T, specialNames []T) []T {
	var combinedPrices []T
	combinedPrices = append(combinedPrices, normalNames...)
	combinedPrices = append(combinedPrices, numericNames...)
	combinedPrices = append(combinedPrices, specialNames...)
	return combinedPrices
}

// Build the array of names that match the regex
// use regex to build the data array that match each filter.
func buildCryptoDataUsingRegexFilter[T dto.SearchTable](assets []T) ([]T, []T, []T) {
	var specialNames []T
	var numberNames []T
	var normalNames []T
	specialPattern := `/[^\s\w ]/`
	numberPattern := `^[0-9]`
	charPattern := `^[a-zA-Z]`

	// Create a regular expression object
	specialRegex, err := regexp.Compile(specialPattern)
	if err != nil {
		fmt.Println("Error compiling regex for Special Characters:", err)
		return nil, nil, nil
	}
	numberRegex, err := regexp.Compile(numberPattern)
	if err != nil {
		fmt.Println("Error compiling regex for numeric Characters:", err)
		return nil, nil, nil
	}
	charRegex, err := regexp.Compile(charPattern)
	if err != nil {
		fmt.Println("Error compiling regex for Normal Characters:", err)
		return nil, nil, nil
	}

	// check if the NFT name match the regex condition then append it to Crypto Price Array.
	for _, asset := range assets {
		// because we have different types to deal with we use the reflect
		// with reflect we can get the field that wwe need to use with MatchString
		assetType := reflect.TypeOf(asset)
		assetValue := reflect.ValueOf(asset)
		fieldName, _ := assetType.FieldByName("Name")
		index := fieldName.Index[0]
		name := assetValue.Field(index).Interface().(string)
		if specialRegex.MatchString(name) {
			specialNames = append(specialNames, asset)
		} else if numberRegex.MatchString(name) {
			numberNames = append(numberNames, asset)
		} else if charRegex.MatchString(name) {
			normalNames = append(normalNames, asset)
		} else {
			specialNames = append(specialNames, asset)
		}
	}

	return specialNames, numberNames, normalNames
}

// Builds name & symbol dictionary from the assets list.
func (c *categoriesTableCacheService) BuildCache(ctx0 context.Context) error {
	_, span := tracer.Start(ctx0, "buildFTDictionary")
	defer span.End()
	searchQuery := c.dao.NewSearchQuery()
	assets, err := searchQuery.GetCategoriesFundamentals(ctx0)
	if err != nil {
		log.Alert("failed to load cache: %v", err)
		return err
	}
	words := []string{}
	dictionaryAssets := make(map[string][]datastruct.CategoryFundamental)     //resetting the map
	dictionaryAssetsById := make(map[string][]datastruct.CategoryFundamental) //resetting the map
	// assign all the assets to the dictionary
	for _, asset := range *assets {
		if asset.Name != "" && !slices.Contains(c.ExcludedCategories, asset.ID) {
			field := strings.ToLower(asset.Name)
			if !slices.Contains(words, field) {
				words = append(words, field)
			}

			dictionaryAssets[field] = append(dictionaryAssets[field], asset)

			fieldid := strings.ToLower(asset.Slug)

			dictionaryAssetsById[fieldid] = append(dictionaryAssetsById[fieldid], asset)
		}
	}
	c.Lock.Lock()
	defer c.Lock.Unlock()
	c.CategoriesTable = dictionaryAssets
	c.Words = words
	c.CategoriesById = dictionaryAssetsById

	span.SetStatus(codes.Ok, "success")
	return nil

}

// SearchTerm Attempts to Search in FT data and returns the data if the search term exist if not it returns the data
// Takes a (ctx context.Context, searchTerm string, paginate dto.Paginate)
// Returns (*[]byte, int, error)
//
// Takes
// - context
// - searchTerm for what we need to found in FT data.
// - paginate for what we need to found in FT data.
// Returns a Ft response with all data we need.
func (a *assetsService) SearchTerm(ctx context.Context, searchTerm string, params dto.SearchRequest) (*[]byte, int, error) {

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

	sortedResult := a.PaginateSortAssets(ctx, &assets, params.Paginate, 0)

	assetsFundamentals, ok := sortedResult.([]datastruct.TradedAssetsTable)
	assetsFundamentals = a.RemoveDuplicateAssets(ctx, assetsFundamentals)

	if !ok {
		err := errors.New("V2 AssetsService.SearchTerm Failed to Assert Type []datastruct.TradedAssetsTable ")
		span.SetStatus(codes.Error, err.Error())
		log.ErrorL(labels, "V2 AssetsService.SearchTerm Data Sorted Error : %s", err.Error())
		log.EndTimeL(labels, "V2 AssetsService.SearchTerm", startTime, err)
		return nil, 0, err
	}

	/*
		We need the response from assets, exactly the same response from Traded Assets endpoint.
		- Source : for the data (Coingecko)
		- Total : the number of Assets that return after the search term
		- Assets : the data to be displayed on the page.
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
