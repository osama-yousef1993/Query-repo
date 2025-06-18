




package store

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/Forbes-Media/go-tools/log"
	"github.com/lithammer/fuzzysearch/fuzzy"
	"go.opentelemetry.io/otel/codes"
	"golang.org/x/exp/slices"
)

type SearchInterface interface {
	NFTPrices | TradedAssetsTable // union of types
}

type DictionaryTest[T SearchInterface] struct {
	searchType SearchType         `json:"seach_type"` //For category, the type would be exact match. Otherwise we're keeping it Fuzzy.
	assets     map[string][]T     `json:"assets"`     //Key = Search Term. Value = Array of Assets related to the search term.
	words      []string           `json:"words"`      //Array of all search terms in the dictionary
	category   DictionaryCategory `json:"category"`   //Category of the dictionary
}


func categoryCacheTest[T SearchInterface]() DictionaryTest[T] {
	var categoryCacheTest DictionaryTest[T] = DictionaryTest[T]{
		searchType: Exact,
		assets:     make(map[string][]T),
		category:   Category,
	}
	return categoryCacheTest
}

func ftCategoryCacheTest[T SearchInterface]() DictionaryTest[T] {
	var ftCategoryCacheTest DictionaryTest[T] = DictionaryTest[T]{
		searchType: Exact,
		assets:     make(map[string][]T),
		category:   FTCategory,
	}
	return ftCategoryCacheTest
}
func ftCacheTest[T SearchInterface]() DictionaryTest[T] {
	var ftCacheTest DictionaryTest[T] = DictionaryTest[T]{
		searchType: Exact,
		assets:     make(map[string][]T),
		category:   Ft,
	}
	return ftCacheTest
}
func nftCacheTest[T SearchInterface]() DictionaryTest[T] {
	var nftCacheTest DictionaryTest[T] = DictionaryTest[T]{
		searchType: Fuzzy,
		assets:     make(map[string][]T),
		category:   Nft,
	}
	return nftCacheTest
}
func nftChainsCacheTest[T SearchInterface]() DictionaryTest[T] {
	var nftChainsCacheTest DictionaryTest[T] = DictionaryTest[T]{
		searchType: Exact,
		assets:     make(map[string][]T),
		category:   NFTChains,
	}
	return nftChainsCacheTest
}

type TradedAssetsRespTest[T SearchInterface] struct {
	Assets                []T    `json:"assets"`
	Total                 int    `json:"total"`
	HasTemporaryDataDelay bool   `json:"hasTemporaryDataDelay"`
	Source                string `json:"source"`
}

func typeofobject(x interface{}) string {
	s := fmt.Sprintf("%T", x)
	log.Error("%s", s)
	return fmt.Sprintf("%T", x)
}

// Rebuilds the cache for all the dictionaries. Usually this is called when the fundamentals are rebuilt. IgnoreLock is used to ignore the dictionaryLock when the lock is already acquired by the function that is calling RebuildCache( this ).
func RebuildCacheTest[T SearchInterface](ctx context.Context, ignoreLock bool) error {
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
	buildFTDictionaryTest(ctx0, assets)
	buildCategoriesDictionaryTest(ctx0, assets, categories)
	span.SetStatus(codes.Ok, "success")
	return nil
}

// Fuzzy search for the given searchTerm in the given words. Returns the assets related to the search term.
func fuzzySearchTest[T SearchInterface](ctx0 context.Context, searchTerm string, words *[]string, assets *map[string][]T) []T {
	_, span := tracer.Start(ctx0, "fuzzySearch")
	defer span.End()

	var result []T
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

// Search a dictionary for the given term. Name of dictionary is provided by the dictionaryCategory. Returns the assets related to the search term.
func SearchTermTest[T SearchInterface](ctx context.Context, searchTerm string, dictionaryCategory DictionaryCategory, paginate Paginate) ([]byte, error) {
	ctx0, span := tracer.Start(ctx, "SearchTerm")
	defer span.End()

	dictionaryLock.Lock()
	defer dictionaryLock.Unlock()

	dictionary := getDictionaryByCategoryTest[T](ctx0, dictionaryCategory)

	// RebuildCache if the dictionary is empty. This happens when the server is restarted.
	if len(dictionary.words) == 0 {
		// RebuildCacheTest[T](ctx0, true)
		dictionary, _ = RebuildNFTCacheTest[T](ctx0, true, dictionary)
		dictionary = getDictionaryByCategoryTest[T](ctx0, dictionaryCategory)
	}
	searchTerm = strings.ToLower(searchTerm)

	var assets []T

	if searchTerm == "" { //if empty string passed while searching, we return all the assets.
		for _, assetList := range dictionary.assets {
			assets = append(assets, assetList...)
		}
	} else if dictionary.searchType == Fuzzy { // For dictionaries that need a fuzzy match. Category dictionary doesn't need this.
		assets = fuzzySearchTest[T](ctx0, searchTerm, &dictionary.words, &dictionary.assets)
	}

	exactMatchAssets, isExact := dictionary.assets[searchTerm] //Assets that directly match the search term.
	exactMatchAssets = RemoveDuplicateInactiveAssetsTest[T](ctx0, exactMatchAssets)
	if isExact {
		assets = append(exactMatchAssets, assets...) // append the direct match assets to the assets array in the front.
	}
	assets = RemoveDuplicateInactiveAssetsTest[T](ctx0, assets)
	totalAssets := len(assets)
	assets = PaginateSortAssetsTest[T](ctx0, assets, paginate, len(exactMatchAssets))

	/*
		We need the response from search, exactly the same response from Traded Assets endpoint.
		- Source : for the data (Coingecko)
		- Total : the number of Assets that return after the search term
		- Assets : the data to be displayED on the page.
	*/
	resp := TradedAssetsRespTest[T]{Source: data_source, Total: totalAssets, Assets: assets}

	jsonData, err := json.Marshal(resp)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	span.SetStatus(codes.Ok, "success")
	return jsonData, nil
}

// Gives the dictionary based on the category provided.
func getDictionaryByCategoryTest[T SearchInterface](ctx0 context.Context, dictionaryCategory DictionaryCategory) DictionaryTest[T] {
	_, span := tracer.Start(ctx0, "getDictionaryByCategory")
	defer span.End()
	ftCacheTest := ftCacheTest[T]()
	nftCacheTest := nftCacheTest[T]()
	nftChainsCacheTest := nftChainsCacheTest[T]()
	categoryCacheTest := categoryCacheTest[T]()
	ftCategoryCacheTest := ftCategoryCacheTest[T]()
	var dictionary DictionaryTest[T]
	var allDictionaries = &[]DictionaryTest[T]{categoryCacheTest, ftCacheTest, nftCacheTest, ftCategoryCacheTest, nftChainsCacheTest}
	for _, dict := range *allDictionaries {
		if dict.category == dictionaryCategory {
			dictionary = dict
			break
		}
	}
	//map[string][]github.com/Forbes-Media/forbes-digital-assets/store.NFTPrices []
	//
	fmt.Println("%s", typeofobject(dictionary.assets))
	span.SetStatus(codes.Ok, "success")
	return dictionary
}

// compress assets when there are duplicate assets (due to matching names & symbols). Also filters out comatokens.
func RemoveDuplicateInactiveAssetsTest[T SearchInterface](ctx0 context.Context, assets []T) []T {
	_, span := tracer.Start(ctx0, "RemoveDuplicateInactiveAssets")
	defer span.End()

	seen := make(map[string]bool)
	result := []T{}

	if typeofobject(assets) == "store.TradedAssetsTable" {
		for _, asset := range assets {
			slug := GETFieldString(asset, "Slug")
			status := GETFieldString(asset, "Status")
			_, seenAsset := seen[slug]
			if !seenAsset && status == "active" {
				seen[slug] = true
				result = append(result, asset)
			}
		}
	}
	span.SetStatus(codes.Ok, "success")
	return result
}

// paginate and sort the assets
func PaginateSortAssetsTest[T SearchInterface](ctx0 context.Context, allAssets []T, paginate Paginate, ignoreInitialAssets int) []T {
	_, span := tracer.Start(ctx0, "PaginateSortAssets")
	defer span.End()

	validatePaginate(ctx0, &paginate) // validate the paginate object
	if len(allAssets) == 0 {
		return allAssets
	}
	initialAssets := (allAssets)[:ignoreInitialAssets]
	assets := (allAssets)[ignoreInitialAssets:]
	if typeofobject(allAssets) == "store.TradedAssetsTable" {
		sort.Slice(assets, func(i, j int) bool { // sort the assets
			var result = j > i //defaults to sort by relevance.

			switch paginate.SortBy {
			case "volume":
				value, value1 := GETFieldDataFloat[T](&assets[i], &assets[j], "Volume")
				result = compareFloat(value, value1, paginate.Direction)
			case "price":
				value, value1 := GETFieldDataFloat[T](&assets[i], &assets[j], "Price")
				result = compareFloat(value, value1, paginate.Direction)
			case "marketCap":
				value, value1 := GETFieldDataFloat[T](&assets[i], &assets[j], "MarketCap")
				result = compareFloat(value, value1, paginate.Direction)
			case "rank":
				value, value1 := GETFieldDataFloat[T](&assets[i], &assets[j], "MarketCap")
				result = compareFloat(value, value1, paginate.Direction)
			case "percentage":
				value, value1 := GETFieldDataFloat[T](&assets[i], &assets[j], "Percentage")
				result = compareFloat(value, value1, paginate.Direction)
			case "percentage_1h":
				value, value1 := GETFieldDataFloat[T](&assets[i], &assets[j], "Percentage1H")
				result = compareFloat(value, value1, paginate.Direction)
			case "percentage_7d":
				value, value1 := GETFieldDataFloat[T](&assets[i], &assets[j], "Percentage7D")
				result = compareFloat(value, value1, paginate.Direction)
			case "change":
				value, value1 := GETFieldDataFloat[T](&assets[i], &assets[j], "ChangeValue")
				result = compareFloat(value, value1, paginate.Direction)
			case "name":
				value, value1 := GETFieldDataString[T](&assets[i], &assets[j], "Name")
				if paginate.Direction == "asc" {
					result = strings.ToLower(value) < strings.ToLower(value1)
				} else {
					result = strings.ToLower(value) > strings.ToLower(value1)
				}
			}
			return result
		})
	} else if typeofobject(allAssets) == "store.NFTPrices" {
		sort.Slice(assets, func(i, j int) bool { // sort the nfts
			var result = j > i //defaults to sort by relevance.
			switch paginate.SortBy {
			case "volume":
				value, value1 := GETFieldDataFloat[T](&assets[i], &assets[j], "Volume24hUsd")
				result = compareFloat(value, value1, paginate.Direction)
			case "price":
				value, value1 := GETFieldDataFloat[T](&assets[i], &assets[j], "FloorPriceUsd")
				result = compareFloat(value, value1, paginate.Direction)
			case "marketCap":
				value, value1 := GETFieldDataFloat[T](&assets[i], &assets[j], "MarketCapUsd")
				result = compareFloat(value, value1, paginate.Direction)
			case "percentage":
				value, value1 := GETFieldDataFloat[T](&assets[i], &assets[j], "FloorPriceInUsd24hPercentageChange")
				result = compareFloat(value, value1, paginate.Direction)
			case "owners":
				value, value1 := GETFieldDataInt[T](&assets[i], &assets[j], "NumberOfUniqueAddresses")
				result = compareInt(value, value1, paginate.Direction)
			case "name":
				value, value1 := GETFieldDataString[T](&assets[i], &assets[j], "Name")
				if paginate.Direction == "asc" {
					result = strings.ToLower(value) < strings.ToLower(value1)
				} else {
					result = strings.ToLower(value) > strings.ToLower(value1)
				}
			}
			return result
		})
	}
	assets = append(initialAssets, assets...)
	start := (paginate.PageNum - 1) * paginate.Limit // paginate the assets
	end := start + paginate.Limit
	if start > len(assets) {
		return []T{}
	}
	if end > len(assets) {
		end = len(assets)
	}
	span.SetStatus(codes.Ok, "success")
	return assets[start:end]
}

func GETFieldDataFloat[T SearchInterface](fieldOne *T, fieldTwo *T, fieldName string) (*float64, *float64) {
	t := reflect.TypeOf(fieldOne)
	v := reflect.ValueOf(fieldOne)
	volume, _ := t.FieldByName(fieldName)
	index := volume.Index[0]
	value := v.Field(index).Interface().(float64)
	t1 := reflect.TypeOf(fieldTwo)
	v1 := reflect.ValueOf(fieldTwo)
	volume1, _ := t1.FieldByName(fieldName)
	index1 := volume1.Index[0]
	value1 := v1.Field(index1).Interface().(float64)
	return &value, &value1

}
func GETFieldDataInt[T SearchInterface](fieldOne *T, fieldTwo *T, fieldName string) (*int, *int) {
	t := reflect.TypeOf(fieldOne)
	v := reflect.ValueOf(fieldOne)
	volume, _ := t.FieldByName(fieldName)
	index := volume.Index[0]
	value := v.Field(index).Interface().(int)
	t1 := reflect.TypeOf(fieldTwo)
	v1 := reflect.ValueOf(fieldTwo)
	volume1, _ := t1.FieldByName(fieldName)
	index1 := volume1.Index[0]
	value1 := v1.Field(index1).Interface().(int)
	return &value, &value1

}
func GETFieldDataString[T SearchInterface](fieldOne *T, fieldTwo *T, fieldName string) (string, string) {
	t := reflect.TypeOf(fieldOne)
	v := reflect.ValueOf(fieldOne)
	volume, _ := t.FieldByName(fieldName)
	index := volume.Index[0]
	value := v.Field(index).Interface().(string)
	t1 := reflect.TypeOf(fieldTwo)
	v1 := reflect.ValueOf(fieldTwo)
	volume1, _ := t1.FieldByName(fieldName)
	index1 := volume1.Index[0]
	value1 := v1.Field(index1).Interface().(string)
	return value, value1
}

func GETFieldString[T SearchInterface](fieldOne T, fieldName string) string {
	t := reflect.TypeOf(fieldOne)
	v := reflect.ValueOf(fieldOne)
	volume, _ := t.FieldByName(fieldName)
	index := volume.Index[0]
	value := v.Field(index).Interface().(string)
	return value
}

// Rebuilds the cache for NFTs the dictionaries. Usually this is called when the NFT fundamentals are rebuilt. IgnoreLock is used to ignore the dictionaryLock when the lock is already acquired by the function that is calling RebuildNFTCache( this ).
func RebuildNFTCacheTest[T SearchInterface](ctx context.Context, ignoreLock bool, dictionary DictionaryTest[T]) (DictionaryTest[T], error) {
	ctx0, span := tracer.Start(ctx, "RebuildNFTCache")
	defer span.End()

	nfts, err := PGGetNFTPrices(ctx0)
	if err != nil {
		return err
	}

	if !ignoreLock {
		dictionaryLock.Lock()
		defer dictionaryLock.Unlock()
	}
	if dictionary.category == Nft {
		dictionary = buildNFTDictionaryTest[T](ctx0, nfts, dictionary)
	} else if dictionary.category == NFTChains {
		dictionary = buildNFTChainsDictionaryTest[T](ctx0, nfts, dictionary)
	}
	span.SetStatus(codes.Ok, "success")
	return dictionary, nil
}

// Builds name & symbol dictionary from the assets list.
func buildFTDictionaryTest[T SearchInterface](ctx0 context.Context, assets []T) {
	_, span := tracer.Start(ctx0, "buildFTDictionary")
	defer span.End()

	words := []string{}
	dictionaryAssets := make(map[string][]T) //resetting the map

	// assign all the assets to the dictionary
	for _, asset := range assets {
		name := GETFieldString(asset, "Name")
		displaySymbol := GETFieldString(asset, "DisplaySymbol")
		if name != "" {
			field := strings.ToLower(name)
			if !slices.Contains(words, field) {
				words = append(words, field)
			}
			dictionaryAssets[field] = append(dictionaryAssets[field], asset)
		}
		if displaySymbol != "" {
			field := strings.ToLower(displaySymbol)
			if !slices.Contains(words, field) {
				words = append(words, field)
			}
			dictionaryAssets[field] = append(dictionaryAssets[field], asset)
		}
	}
	ftCache := ftCacheTest[T]()
	ftCache.words = words             //resetting the array
	ftCache.assets = dictionaryAssets //resetting the map

	span.SetStatus(codes.Ok, "success")
}

// Builds Categories Dictionary from the categories & assets table
func buildCategoriesDictionaryTest[T SearchInterface](ctx0 context.Context, assets []T, categories []CategoriesData) {
	_, span := tracer.Start(ctx0, "buildCategoriesDictionary")
	defer span.End()

	words := []string{}
	dictionaryAssets := make(map[string][]T) //resetting the map

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
				symbol := GETFieldString(asset, "Symbol")
				if symbol != "" && assetSymbols[symbol] {
					dictionaryAssets[categoryKey] = append(dictionaryAssets[categoryKey], asset)
				}
			}

			//Sort the assets by market cap since we're keeping this our default sort order.
			sort.Slice(dictionaryAssets[categoryKey], func(i, j int) bool {
				value, value1 := GETFieldDataFloat(&dictionaryAssets[categoryKey][i], &dictionaryAssets[categoryKey][j], "MarketCap")
				return compareFloat(value, value1, "desc")
			})
		}
	}
	categoryCache := categoryCacheTest[T]()
	ftCategoryCache := ftCategoryCacheTest[T]()
	categoryCache.words = words               //resetting the array
	categoryCache.assets = dictionaryAssets   //resetting the map
	ftCategoryCache.assets = dictionaryAssets //resetting the map

	span.SetStatus(codes.Ok, "success")
}

// Builds NFT Chain Dictionary from the chains & nfts table
func buildNFTChainsDictionaryTest[T SearchInterface](ctx0 context.Context, nfts []NFTPrices, dictionary DictionaryTest[T]) DictionaryTest[T] {
	_, span := tracer.Start(ctx0, "buildNFTChainsDictionary")
	defer span.End()

	words := []string{}
	dictionaryNft := make(map[string][]T) //resetting the map

	// assign all the nfts to the each chain id
	for _, nft := range nfts {
		assetPlatformId := GETFieldString(nft, "AssetPlatformId")
		if assetPlatformId != "" {
			field := strings.ToLower(assetPlatformId)
			if !slices.Contains(words, field) {
				words = append(words, field)
			}
			dictionaryNft[field] = append(dictionaryNft[field], nft)
		}
	}
	dictionary.assets = dictionaryNft //resetting the map
	span.SetStatus(codes.Ok, "success")

	return dictionary
}

// It will build NFT data it will build the word and the NFTs arrays
func buildNFTDictionaryTest[T SearchInterface](ctx0 context.Context, nfts []NFTPrices, dictionary DictionaryTest[T]) DictionaryTest[T] {
	_, span := tracer.Start(ctx0, "buildNFTDictionary")
	defer span.End()

	words := []string{}
	dictionaryNft := make(map[string][]T) //resetting the map

	// assign all the nfts to the dictionary
	t := new(T)
	for _, nft := range nfts {
		name := GETFieldString(nft, "Name")
		symbol := GETFieldString(nft, "Symbol")
		if name != "" {
			field := strings.ToLower(name)
			if !slices.Contains(words, field) {
				words = append(words, field)
			}
			dictionaryNft[field] = append(dictionaryNft[field], (any(t)).nft)
		}
		if symbol != "" {
			field := strings.ToLower(symbol)
			if !slices.Contains(words, field) {
				words = append(words, field)
			}
			dictionaryNft[field] = append(dictionaryNft[field], nft)
		}
	}
	dictionary.words = words          //resetting the array
	dictionary.assets = dictionaryNft //resetting the map

	span.SetStatus(codes.Ok, "success")
	return dictionary
}


