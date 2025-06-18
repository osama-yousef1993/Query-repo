package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/lithammer/fuzzysearch/fuzzy"
	"go.opentelemetry.io/otel/codes"
	"golang.org/x/exp/slices"
)

var dictionaryLock = &sync.Mutex{}  //Lock for dictionary. Used only while updating the dictionary.
var defaultFuzzySearchLimit = 10000 //How many search results from FuzzySearch we'll consider.

type Dictionary struct {
	searchType SearchType                     `json:"seach_type"` //For category, the type would be exact match. Otherwise we're keeping it Fuzzy.
	assets     map[string][]TradedAssetsTable `json:"assets"`     //Key = Search Term. Value = Array of Assets related to the search term.
	words      []string                       `json:"words"`      //Array of all search terms in the dictionary
	category   DictionaryCategory             `json:"category"`   //Category of the dictionary
	nfts       map[string][]NFTPrices         `json:"nfts"`       //NFTs of the dictionary
}

type Paginate struct {
	SortBy     string `json:"sort_by"`     // sort by field
	Direction  string `json:"direction"`   // sort direction : asc or desc
	PageNum    int    `json:"page_num"`    // page number. Default 1
	Limit      int    `json:"limit"`       // limit per page.
	CategoryID string `json:"category_id"` // category id for featured categories
	ChainID    string `json:"chain_id"`    // chain id for featured categories
}

type DictionaryCategory int

const (
	Ft         DictionaryCategory = 0 // Fungible Token dictionary Kind - Search by name, symbol of the fungible token
	Nft        DictionaryCategory = 1 // Non-Fungible Token dictionary Kind - Search by name, symbol of the NFT
	Category   DictionaryCategory = 2 // Category dictionary Kind - Search directly by category
	FTCategory DictionaryCategory = 3 // Fungible Token Category dictionary Kind - Search  Fungible Token directly by category
	NFTChains  DictionaryCategory = 4 // Non-Fungible Token Category dictionary Kind - Search  Non-Fungible Token directly by chains
)

type SearchType int

const (
	Fuzzy SearchType = 0 //If the dictionary requires fuzzy search
	Exact SearchType = 1 //If the dictionary requires exact match
)

// Category Dictionary. We keep searchType as Exact because we'll be provided by the exact category's ID while searching.
var categoryCache Dictionary = Dictionary{
	searchType: Exact,
	assets:     make(map[string][]TradedAssetsTable),
	category:   Category,
}

// FTCategory (Fungible Token with in Category) Dictionary. Wee Keep searchType as Fuzzy because we will search by name or symbol of the fungible token that exist in the provided category
var ftCategoryCache Dictionary = Dictionary{
	searchType: Fuzzy,
	assets:     make(map[string][]TradedAssetsTable),
	category:   FTCategory,
}

// Fungible Assets Dictionary. Used when the user searches by name or symbol of the fungible token.
var ftCache Dictionary = Dictionary{
	searchType: Fuzzy,
	assets:     make(map[string][]TradedAssetsTable),
	category:   Ft,
}

// NFT Dictionary.
var nftCache Dictionary = Dictionary{
	searchType: Fuzzy,
	nfts:       make(map[string][]NFTPrices),
	category:   Nft,
}

// NFT Chains Dictionary.
var nftChainsCache Dictionary = Dictionary{
	searchType: Fuzzy,
	nfts:       make(map[string][]NFTPrices),
	category:   NFTChains,
}

// Rebuilds the cache for all the dictionaries. Usually this is called when the fundamentals are rebuilt. IgnoreLock is used to ignore the dictionaryLock when the lock is already acquired by the function that is calling RebuildCache( this ).
func RebuildCache(ctx context.Context, ignoreLock bool) error {
	ctx0, span := tracer.Start(ctx, "PGGetSearchAssets")
	defer span.End()

	assets, err := PGGetSearchAssets(ctx0)
	if err != nil {
		return err
	}
	categories, err := PGGetCategories(ctx0)
	if err != nil {
		return err
	}

	if !ignoreLock {
		dictionaryLock.Lock()
		defer dictionaryLock.Unlock()
	}
	buildFTDictionary(ctx0, assets)
	buildCategoriesDictionary(ctx0, assets, categories)
	span.SetStatus(codes.Ok, "success")
	return nil
}

// Search a dictionary for the given term. Name of dictionary is provided by the dictionaryCategory. Returns the assets related to the search term.
func SearchTerm(ctx context.Context, searchTerm string, dictionaryCategory DictionaryCategory, paginate Paginate) ([]byte, error) {
	ctx0, span := tracer.Start(ctx, "SearchTerm")
	defer span.End()

	dictionaryLock.Lock()
	defer dictionaryLock.Unlock()

	dictionary := getDictionaryByCategory(ctx0, dictionaryCategory)

	// RebuildCache if the dictionary is empty. This happens when the server is restarted.
	if len(dictionary.words) == 0 {
		RebuildCache(ctx0, true)
		dictionary = getDictionaryByCategory(ctx0, dictionaryCategory)
	}

	searchTerm = strings.ToLower(searchTerm)
	var assets []TradedAssetsTable

	if searchTerm == "" { //if empty string passed while searching, we return all the assets.
		for _, assetList := range dictionary.assets {
			assets = append(assets, assetList...)
		}
	} else if dictionary.searchType == Fuzzy { // For dictionaries that need a fuzzy match. Category dictionary doesn't need this.
		assets = fuzzySearch(ctx0, searchTerm, &dictionary.words, &dictionary.assets)
	}

	exactMatchAssets, isExact := dictionary.assets[searchTerm] //Assets that directly match the search term.
	exactMatchAssets = RemoveDuplicateInactiveAssets(ctx0, exactMatchAssets)
	if isExact {
		assets = append(exactMatchAssets, assets...) // append the direct match assets to the assets array in the front.
	}
	assets = RemoveDuplicateInactiveAssets(ctx0, assets)
	totalAssets := len(assets)
	assets = PaginateSortAssets(ctx0, &assets, paginate, len(exactMatchAssets))

	/*
		We need the response from search, exactly the same response from Traded Assets endpoint.
		- Source : for the data (Coingecko)
		- Total : the number of Assets that return after the search term
		- Assets : the data to be displayED on the page.
	*/
	var resp = TradedAssetsResp{Source: data_source, Total: totalAssets, Assets: assets}

	jsonData, err := json.Marshal(resp)
	if err == nil {
		bqs, _ := NewBQStore()
		err = bqs.InsertSearchData(ctx0, searchTerm, totalAssets, "asset")
	}
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	span.SetStatus(codes.Ok, "success")
	return jsonData, nil
}

// Search a dictionary for the given Category. Name of dictionary is provided by the dictionaryCategory. Returns the assets related to the search term in this Category.
func SearchTermByCategory(ctx context.Context, searchTerm string, dictionaryCategory DictionaryCategory, paginate Paginate) ([]byte, error) {
	ctx0, span := tracer.Start(ctx, "SearchTermByCategory")
	defer span.End()

	dictionaryLock.Lock()
	defer dictionaryLock.Unlock()

	// return the Fungible Token Category cache type
	// will use the category id from paginate object
	dictionary := getDictionaryByCategory(ctx0, dictionaryCategory)

	// RebuildCache if the dictionary is empty. This happens when the server is restarted.
	if len(dictionary.words) == 0 {
		RebuildCache(ctx0, true)
		dictionary = getDictionaryByCategory(ctx0, dictionaryCategory)
	}
	exactMatchFound := false
	var assetsResult []TradedAssetsTable
	// Get all assets that related to the category
	categoryAssets, exactMatchFound := dictionary.assets[paginate.CategoryID]
	/*
		The built FTCategoryWords function will take the assets for the category and
		return assets for this category as a map and the words array of asset names to be searched in.
	*/
	assets, words := buildFTCategoryWords(ctx0, categoryAssets)
	dictionary.words = words
	searchTerm = strings.ToLower(searchTerm)
	// If there's exact match for the category ID in the dictionary, we will use fuzzy search for the search term in this category, and return the assets.
	if exactMatchFound {
		assetsResult = fuzzySearch(ctx0, searchTerm, &dictionary.words, &assets)
	}

	exactMatchAssets, isExact := assets[searchTerm] //Assets that directly match the search term.
	exactMatchAssets = RemoveDuplicateInactiveAssets(ctx0, exactMatchAssets)
	if isExact {
		assetsResult = append(exactMatchAssets, assetsResult...) // append the direct match assets to the assets array in the front.
	}
	assetsResult = RemoveDuplicateInactiveAssets(ctx0, assetsResult)
	totalAssets := len(assetsResult)
	assetsResult = PaginateSortAssets(ctx0, &assetsResult, paginate, len(exactMatchAssets))

	/*
		We need the response from search, exactly the same response from Traded Assets endpoint.
		- Source : for the data (Coingecko)
		- Total : the number of Assets that return after the search term
		- Assets : the data to be displayED on the page.
	*/
	var resp = TradedAssetsResp{Source: data_source, Total: totalAssets, Assets: assetsResult}

	jsonData, err := json.Marshal(resp)
	if err == nil {
		bqs, _ := NewBQStore()
		err = bqs.InsertSearchData(ctx0, searchTerm, totalAssets, "asset")
	}
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	span.SetStatus(codes.Ok, "success")
	return jsonData, nil
}

// Fuzzy search for the given searchTerm in the given words. Returns the assets related to the search term.
func fuzzySearch(ctx0 context.Context, searchTerm string, words *[]string, assets *map[string][]TradedAssetsTable) []TradedAssetsTable {
	_, span := tracer.Start(ctx0, "fuzzySearch")
	defer span.End()

	var result []TradedAssetsTable
	ranks := fuzzy.RankFindNormalized(searchTerm, *words) // case-insensitive & unicode-normalized fuzzy search.
	sort.Sort(ranks)                                      // sorts by the Levenshtein distance
	for rankIdx, rank := range ranks {
		if rankIdx >= defaultFuzzySearchLimit {
			break
		}
		result = append(result, (*assets)[rank.Target]...)
	}

	span.SetStatus(codes.Ok, "success")
	return result
}

// Gives the dictionary based on the category provided.
func getDictionaryByCategory(ctx0 context.Context, dictionaryCategory DictionaryCategory) Dictionary {
	_, span := tracer.Start(ctx0, "getDictionaryByCategory")
	defer span.End()

	var dictionary Dictionary
	var allDictionaries = []Dictionary{categoryCache, ftCache, nftCache, ftCategoryCache, nftChainsCache}
	for _, dict := range allDictionaries {
		if dict.category == dictionaryCategory {
			dictionary = dict
			break
		}
	}
	span.SetStatus(codes.Ok, "success")
	return dictionary
}

// Gives the dictionaryCategory based on the category string provided.
func GetDictionaryCategoryByString(ctx0 context.Context, category string) (DictionaryCategory, error) {
	_, span := tracer.Start(ctx0, "GetDictionaryCategoryByString")
	defer span.End()

	var dictionaryCategory DictionaryCategory //dictionary category is an enum derived from the query params of the API endpoint.
	if category == "ft" {
		dictionaryCategory = Ft
	} else if category == "nft" {
		dictionaryCategory = Nft
	} else if category == "category" {
		dictionaryCategory = Category
	} else if category == "ftCategory" {
		dictionaryCategory = FTCategory
	} else if category == "nftChains" {
		dictionaryCategory = NFTChains
	} else {
		span.SetStatus(codes.Error, "Invalid category")
		return dictionaryCategory, errors.New("Invalid category")
	}

	span.SetStatus(codes.Ok, "success")
	return dictionaryCategory, nil
}

// Builds name & symbol dictionary from the assets list.
func buildFTDictionary(ctx0 context.Context, assets []TradedAssetsTable) {
	_, span := tracer.Start(ctx0, "buildFTDictionary")
	defer span.End()

	words := []string{}
	dictionaryAssets := make(map[string][]TradedAssetsTable) //resetting the map

	// assign all the assets to the dictionary
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
	ftCache.words = words             //resetting the array
	ftCache.assets = dictionaryAssets //resetting the map

	span.SetStatus(codes.Ok, "success")
}

// Builds Categories Dictionary from the categories & assets table
func buildCategoriesDictionary(ctx0 context.Context, assets []TradedAssetsTable, categories []CategoriesData) {
	_, span := tracer.Start(ctx0, "buildCategoriesDictionary")
	defer span.End()

	words := []string{}
	dictionaryAssets := make(map[string][]TradedAssetsTable) //resetting the map

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

	categoryCache.words = words               //resetting the array
	categoryCache.assets = dictionaryAssets   //resetting the map
	ftCategoryCache.assets = dictionaryAssets //resetting the map

	span.SetStatus(codes.Ok, "success")
}

// compress assets when there are duplicate assets (due to matching names & symbols). Also filters out comatokens.
func RemoveDuplicateInactiveAssets(ctx0 context.Context, assets []TradedAssetsTable) []TradedAssetsTable {
	_, span := tracer.Start(ctx0, "RemoveDuplicateInactiveAssets")
	defer span.End()

	seen := make(map[string]bool)
	result := []TradedAssetsTable{}

	for _, asset := range assets {
		_, seenAsset := seen[asset.Slug]
		if !seenAsset && asset.Status == "active" {
			seen[asset.Slug] = true
			result = append(result, asset)
		}
	}
	span.SetStatus(codes.Ok, "success")
	return result
}

// paginate and sort the assets
func PaginateSortAssets(ctx0 context.Context, allAssets *[]TradedAssetsTable, paginate Paginate, ignoreInitialAssets int) []TradedAssetsTable {
	_, span := tracer.Start(ctx0, "PaginateSortAssets")
	defer span.End()

	validatePaginate(ctx0, &paginate) // validate the paginate object
	if len(*allAssets) == 0 {
		return *allAssets
	}
	initialAssets := (*allAssets)[:ignoreInitialAssets]
	assets := (*allAssets)[ignoreInitialAssets:]
	if paginate.SortBy == "name" {
		assets = PaginateSortCryptoByNames(ctx0, paginate, assets)
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
	start := (paginate.PageNum - 1) * paginate.Limit // paginate the assets
	end := start + paginate.Limit
	if start > len(assets) {
		return []TradedAssetsTable{}
	}
	if end > len(assets) {
		end = len(assets)
	}
	span.SetStatus(codes.Ok, "success")
	return assets[start:end]
}

// Validate the paginate's limit, Page number and direction values.
func validatePaginate(ctx0 context.Context, paginate *Paginate) {
	_, span := tracer.Start(ctx0, "validatePaginate")
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

// compare float values for the sort.Slices function
func compareFloat(val1 *float64, val2 *float64, direction string) bool {
	if val1 == nil || val2 == nil {
		return false
	}
	if direction == "desc" {
		return *val1 > *val2
	}
	return *val1 < *val2
}

// It will take the assets by category and build the assets map and the words array
func buildFTCategoryWords(ctx0 context.Context, categoryAssets []TradedAssetsTable) (map[string][]TradedAssetsTable, []string) {
	_, span := tracer.Start(ctx0, "buildFTCategoryWords")
	defer span.End()

	words := []string{}
	dictionaryAssets := make(map[string][]TradedAssetsTable)

	// Assign all the assets to the dictionary by category id
	// The assets will be related to category id that provided.
	for _, asset := range categoryAssets {
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
	span.SetStatus(codes.Ok, "success")
	return dictionaryAssets, words
}

// Special Sort Function we will use it only if we need to sort by Crypto Names
func PaginateSortCryptoByNames(ctx0 context.Context, paginate Paginate, assets []TradedAssetsTable) []TradedAssetsTable {
	_, span := tracer.Start(ctx0, "PaginateSortCryptoByNames")
	defer span.End()

	// build array of all cases that exist in Crypto name
	specialNames, numberNames, normalNames := BuildCryptoDataUsingRegexFilter(assets)

	// Sort Names Start with Special Characters
	CryptoSortFunctionality(specialNames)
	// Sort Names start with numeric Characters
	CryptoSortFunctionality(numberNames)
	// Sort Names start with Alphabetic Characters
	CryptoSortFunctionality(normalNames)

	// Build Crypto response Sorted by name
	// If the sort ASC the result will return in this order --> normalName --> numericName --> specialName
	// If the sort DESC the result will return in this order --> numericName --> normalName --> specialName
	if paginate.Direction == "asc" {
		// It will sort data using ASC like: a-z --> 0-9 --> special
		assets = BuildOrderedCryptoPrices(normalNames, numberNames, specialNames)
	} else if paginate.Direction == "desc" {
		// It will sort data using DESC like: 0-9 --> a-z --> special
		assets = BuildOrderedCryptoPrices(numberNames, normalNames, specialNames)
	}
	span.SetStatus(codes.Ok, "success")
	return assets
}

// the sort function will sort the data after we filter it with regex
// it will sort data by ASC order for all data
func CryptoSortFunctionality(assets []TradedAssetsTable) {
	sort.Slice(assets, func(i, j int) bool {
		var res = j > i
		res = strings.ToLower(assets[i].Name) < strings.ToLower(assets[j].Name)
		return res
	})
}

// Use this function to build the Crypto for all names after the filter and the sort functions
// it will return an array of Crypto that ordered as we need
// Asc order will return ==> normalName --> numericName --> specialName
// Desc order will return ==> numericName --> normalName --> specialName
func BuildOrderedCryptoPrices(normalNames []TradedAssetsTable, numericNames []TradedAssetsTable, specialNames []TradedAssetsTable) []TradedAssetsTable {
	var combinedPrices []TradedAssetsTable
	combinedPrices = append(combinedPrices, normalNames...)
	combinedPrices = append(combinedPrices, numericNames...)
	combinedPrices = append(combinedPrices, specialNames...)
	return combinedPrices
}

// Build the array of names that match the regex
// use regex to build the data array that match each filter.
func BuildCryptoDataUsingRegexFilter(assets []TradedAssetsTable) ([]TradedAssetsTable, []TradedAssetsTable, []TradedAssetsTable) {
	var specialNames []TradedAssetsTable
	var numberNames []TradedAssetsTable
	var normalNames []TradedAssetsTable
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
		name := asset.Name
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









// build new trending topics
func BuildTrendingTopicArray(ctx context.Context, trendingTopics []Topic, notTrendingTopics []Topic, topicIndex int) []Topic {
	_, span := tracer.Start(ctx, "BuildTrendingTopicArray")
	defer span.End()
	span.AddEvent("Start Build Trending Topic Array")

	var topicResult []Topic
	trendingTopicCount := 20
	trendingTopicsLen := len(trendingTopics)
	notTrendingTopicsLen := len(notTrendingTopics)
	totalIndex := (trendingTopicsLen + notTrendingTopicsLen)
	res := totalIndex - topicIndex
	if topicIndex-1 == 0 {
		topicIndex = trendingTopicCount
	}

	// if the result for topic equals to 20 then return the topic with in the range
	// if it's not equals to 20 we need to get the last part from topics and append the rest of them to reach 20 topics
	if res >= 20 {
		topicResult = append(topicResult, notTrendingTopics[topicIndex-trendingTopicCount:topicIndex]...)
	} else {
		firstIndex := topicIndex - trendingTopicCount
		lastIndex := res + firstIndex
		if firstIndex > len(notTrendingTopics) {
			firstIndex = firstIndex - len(notTrendingTopics)
		}
		if lastIndex > len(notTrendingTopics) {
			lastIndex = len(notTrendingTopics)
		}
		topicResult = append(topicResult, notTrendingTopics[firstIndex:lastIndex]...)
		if len(topicResult) < trendingTopicCount {
			t := trendingTopicCount - len(topicResult)
			topicResult = append(topicResult, notTrendingTopics[0:t]...)
		} else if len(topicResult) > trendingTopicCount {
			topicResult = topicResult[0: 20]
		}
	}
	span.SetStatus(otelCodes.Ok, "Success")
	return topicResult
}