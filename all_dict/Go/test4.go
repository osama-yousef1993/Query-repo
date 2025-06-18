// main
v1.HandleFunc("/nft-chains", GetNFTChains).Methods(http.MethodGet, http.MethodOptions)
v1.HandleFunc("/nft-prices", GetNFTPrices).Methods(http.MethodGet, http.MethodOptions)



func GetNFTChains(w http.ResponseWriter, r *http.Request) {
	setResponseHeaders(w, 60)
	labels := make(map[string]string)

	ctx, span := tracer.Start(r.Context(), "GetNFTChains")

	defer span.End()

	labels["UUID"] = uuid.New().String()
	labels["function"] = "BuildVideos"
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTime("Get Assets Calculator Data")

	data, err := store.GetNFTChains(ctx)
	if data == nil && err == nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusNotFound)

	}
	span.SetStatus(codes.Ok, "GetNFTChains")
	log.EndTimeL(labels, "BuildVideos ", startTime, nil)
	w.WriteHeader(200)
	w.Write(data)

}

func GetNFTPrices(w http.ResponseWriter, r *http.Request) {
	setResponseHeaders(w, 300)
	// updated each 5 minute
	labels := make(map[string]string)
	span := trace.SpanFromContext(r.Context())
	defer span.End()

	labels["function"] = "GetSearchAssets"
	labels["UUID"] = uuid.New().String()
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()
	span.SetAttributes(attribute.String("UUID", labels["UUID"]))
	startTime := log.StartTimeL(labels, "NFT Price Table")
	paginate := store.Paginate{} //captures the pagination params.
	limit := html.EscapeString(r.URL.Query().Get("limit"))
	pageNum := html.EscapeString(r.URL.Query().Get("pageNum"))
	paginate.SortBy = html.EscapeString(r.URL.Query().Get("sortBy"))
	paginate.Direction = html.EscapeString(r.URL.Query().Get("direction"))
	category := html.EscapeString(r.URL.Query().Get("category"))
	query := html.EscapeString(r.URL.Query().Get("query"))
	// Will use chainID if we need to search about specific NFT using Chains
	chainID := html.EscapeString(r.URL.Query().Get("chain_id"))
	var limitError error
	var pageError error
	paginate.Limit, limitError = strconv.Atoi(limit)
	paginate.PageNum, pageError = strconv.Atoi(pageNum)
	dictionaryCategory, dictionaryErr := store.GetDictionaryCategoryByString(r.Context(), category)

	if limitError != nil || pageError != nil || dictionaryErr != nil { //throw an error if pagination args are improper.
		log.ErrorL(labels, "Invalid pagination values")
		span.SetStatus(codes.Error, "Invalid pagination values")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var result []byte
	var err error

	if chainID != "" {
		// If chainID exists in query params, It will be used to searching for a specific chain using NFT query.
		// this means the user needs to search for nfts using a specific chain
		paginate.ChainID = chainID
		// The SearchTermByChains function will build the result using the NFTs that exist in the specified chain
		result, err = store.SearchTermByChains(r.Context(), query, dictionaryCategory, paginate)
	} else {
		result, err = store.SearchNFTTerm(r.Context(), query, dictionaryCategory, paginate)
	}

	if result == nil && err == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if err != nil {
		log.Error("%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	reader := bytes.NewReader(result)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(result)))
	store.ConsumeTime("Get Traded NFT Data", startTime, nil)
	w.WriteHeader(http.StatusOK)
	io.Copy(w, reader)
}
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// firestore
type NFTChain struct {
	ID   string `json:"id" firestore:"id"`
	Name string `json:"name" firestore:"name"`
}

func GetNFTChains(ctx0 context.Context) ([]byte, error) {
	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "GetNFTChains")
	defer span.End()

	var nftChains []NFTChain

	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "nft_chains")
	// Get the Global Description and the Lists Section from firestore
	iter := fs.Collection(collectionName).Documents(ctx)
	span.AddEvent("Start Getting NFT Chains Data from FS")

	for {
		var nftChain NFTChain
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			log.Error("Error Getting NFT Chains Data from FS: %s", err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}

		err = doc.DataTo(&nftChain)
		if err != nil {
			log.Error("Error Getting NFT Chains Data from FS: %s", err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}
		nftChains = append(nftChains, nftChain)
	}

	jsonData, err := BuildJsonResponse(ctx, nftChains, "NFT Chains Data")

	if err != nil {
		log.Error("Error Converting NFT Chains to Json: %s", err)
		span.SetStatus(otelCodes.Error, err.Error())
		return nil, err
	}

	span.SetStatus(otelCodes.Ok, "Success")
	return jsonData, nil
}

// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// postgresql
type NFTPrices struct {
	ID                                         string  `json:"id" postgres:"id"`
	ContractAddress                            string  `json:"contract_address" postgres:"contract_address"`
	AssetPlatformId                            string  `json:"asset_platform_id" postgres:"asset_platform_id"`
	Name                                       string  `json:"name" postgres:"name"`
	Symbol                                     string  `json:"symbol" postgres:"symbol"`
	Image                                      string  `json:"image" postgres:"image"`
	Description                                string  `json:"description" postgres:"description"`
	NativeCurrency                             string  `json:"native_currency" postgres:"native_currency"`
	FloorPriceUsd                              float64 `json:"floor_price_usd" postgres:"floor_price_usd"`
	MarketCapUsd                               float64 `json:"market_cap_usd" postgres:"market_cap_usd"`
	Volume24hUsd                               float64 `json:"volume_24h_usd" postgres:"volume_24h_usd"`
	FloorPriceNative                           float64 `json:"floor_price_native" postgres:"floor_price_native"`
	MarketCapNative                            float64 `json:"market_cap_native" postgres:"market_cap_native"`
	Volume24hNative                            float64 `json:"volume_24h_native" postgres:"volume_24h_native"`
	FloorPriceInUsd24hPercentageChange         float64 `json:"floor_price_in_usd_24h_percentage_change" postgres:"floor_price_in_usd_24h_percentage_change"`
	NumberOfUniqueAddresses                    int     `json:"number_of_unique_addresses" postgres:"number_of_unique_addresses"`
	NumberOfUniqueAddresses24hPercentageChange float64 `json:"number_of_unique_addresses_24h_percentage_change" postgres:"number_of_unique_addresses_24h_percentage_change"`
	Slug                                       string  `json:"slug" postgres:"slug"`
	LastUpdated                                string  `json:"last_updated" postgres:"last_updated"`
	FullCount                                  *int    `postgres:"full_count"`
}

type NFTPricesResp struct {
	NFT                   []NFTPrices `json:"nft"`
	Total                 int         `json:"total"`
	HasTemporaryDataDelay bool        `json:"hasTemporaryDataDelay"`
	Source                string      `json:"source"`
}

func PGGetNFTPrices(ctx0 context.Context) ([]NFTPrices, error) {

	ctx, span := tracer.Start(ctx0, "PGGetTradedAssets")
	defer span.End()

	startTime := log.StartTime("Pagination Query")
	var nfts []NFTPrices
	pg := PGConnect()
	query := `SELECT 
					id,
					contract_address,
					asset_platform_id,
					name,
					symbol,
					image,
					description,
					native_currency,
					floor_price_usd,
					market_cap_usd,
					volume_24h_usd,
					floor_price_native,
					market_cap_native,
					volume_24h_native,
					floor_price_in_usd_24h_percentage_change,
					number_of_unique_addresses,
					number_of_unique_addresses_24h_percentage_change,
					slug,
					last_updated,
					count(id) OVER() AS full_count
				FROM 
					public.nftdatalatest`
	queryResult, err := pg.QueryContext(ctx, query)

	if err != nil {
		log.EndTime("Pagination Query", startTime, err)
		return nil, err
	}
	defer queryResult.Close()

	for queryResult.Next() {
		var nft NFTPrices
		err := queryResult.Scan(&nft.ID, &nft.ContractAddress, &nft.AssetPlatformId, &nft.Name, &nft.Symbol, &nft.Image, &nft.Description, &nft.NativeCurrency, &nft.FloorPriceUsd, &nft.MarketCapUsd, &nft.Volume24hUsd, &nft.FloorPriceNative, &nft.MarketCapNative, &nft.Volume24hNative, &nft.FloorPriceInUsd24hPercentageChange, &nft.NumberOfUniqueAddresses, &nft.NumberOfUniqueAddresses24hPercentageChange, &nft.Slug, &nft.LastUpdated, &nft.FullCount)
		if err != nil {
			log.EndTime("Pagination Query", startTime, err)
			return nil, err
		}
		nfts = append(nfts, nft)
	}
	return nfts, nil
}


// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// search dictionary
package store

import (
	"context"
	"encoding/json"
	"errors"
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
	NFTChains  DictionaryCategory = 4 // Fungible Token Category dictionary Kind - Search  Fungible Token directly by category
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
		case "name":
			if paginate.Direction == "asc" {
				result = strings.ToLower(assets[i].Name) < strings.ToLower(assets[j].Name)
			} else {
				result = strings.ToLower(assets[i].Name) > strings.ToLower(assets[j].Name)
			}
		}
		return result
	})
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


// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// search nft dictionary

package store

import (
	"context"
	"encoding/json"
	"sort"
	"strings"

	"github.com/lithammer/fuzzysearch/fuzzy"
	"go.opentelemetry.io/otel/codes"
	"golang.org/x/exp/slices"
)


// Rebuilds the cache for NFTs the dictionaries. Usually this is called when the NFT fundamentals are rebuilt. IgnoreLock is used to ignore the dictionaryLock when the lock is already acquired by the function that is calling RebuildNFTCache( this ).
func RebuildNFTCache(ctx context.Context, ignoreLock bool) error {
	ctx0, span := tracer.Start(ctx, "PGGetSearchAssets")
	defer span.End()

	nfts, err := PGGetNFTPrices(ctx0)
	if err != nil {
		return err
	}

	if !ignoreLock {
		dictionaryLock.Lock()
		defer dictionaryLock.Unlock()
	}
	buildNFTDictionary(ctx0, nfts)
	buildNFTChainsDictionary(ctx0, nfts)
	span.SetStatus(codes.Ok, "success")
	return nil
}

// compare int values for the sort.Slices function for the Owners
func compareInt(val1 *int, val2 *int, direction string) bool {
	if val1 == nil || val2 == nil {
		return false
	}
	if direction == "desc" {
		return *val1 > *val2
	}
	return *val1 < *val2
}

// It will build NFT data it will build the word and the NFTs arrays
func buildNFTDictionary(ctx0 context.Context, nfts []NFTPrices) {
	_, span := tracer.Start(ctx0, "buildFTDictionary")
	defer span.End()

	words := []string{}
	dictionaryNft := make(map[string][]NFTPrices) //resetting the map

	// assign all the nfts to the dictionary
	for _, nft := range nfts {
		if nft.Name != "" {
			field := strings.ToLower(nft.Name)
			if !slices.Contains(words, field) {
				words = append(words, field)
			}
			dictionaryNft[field] = append(dictionaryNft[field], nft)
		}
		if nft.Symbol != "" {
			field := strings.ToLower(nft.Symbol)
			if !slices.Contains(words, field) {
				words = append(words, field)
			}
			dictionaryNft[field] = append(dictionaryNft[field], nft)
		}
	}
	nftCache.words = words        //resetting the array
	nftCache.nfts = dictionaryNft //resetting the map

	span.SetStatus(codes.Ok, "success")
}

// Search a dictionary for the given term. Name of dictionary is provided by the dictionaryCategory. Returns the nfts related to the search term.
func SearchNFTTerm(ctx context.Context, searchTerm string, dictionaryCategory DictionaryCategory, paginate Paginate) ([]byte, error) {
	ctx0, span := tracer.Start(ctx, "SearchTerm")
	defer span.End()

	dictionaryLock.Lock()
	defer dictionaryLock.Unlock()

	dictionary := getDictionaryByCategory(ctx0, dictionaryCategory)

	// RebuildNFTCache if the dictionary is empty. This happens when the server is restarted.
	if len(dictionary.words) == 0 {
		RebuildNFTCache(ctx0, true)
		dictionary = getDictionaryByCategory(ctx0, dictionaryCategory)
	}

	searchTerm = strings.ToLower(searchTerm)
	var nfts []NFTPrices

	if searchTerm == "" { //if empty string passed while searching, we return all the nfts.
		for _, nftList := range dictionary.nfts {
			nfts = append(nfts, nftList...)
		}
	} else if dictionary.searchType == Fuzzy { // For dictionaries that need a fuzzy match.
		nfts = fuzzyNFTSearch(ctx0, searchTerm, &dictionary.words, &dictionary.nfts)
	}

	exactMatchNFTs, isExact := dictionary.nfts[searchTerm] //NFTs that directly match the search term.
	exactMatchNFTs = RemoveDuplicateNFTs(ctx0, exactMatchNFTs)
	if isExact {
		nfts = append(exactMatchNFTs, nfts...) // append the direct match assets to the assets array in the front.
	}
	nfts = RemoveDuplicateNFTs(ctx0, nfts)
	totalAssets := len(nfts)
	nfts = PaginateSortNFTs(ctx0, &nfts, paginate, len(exactMatchNFTs))

	/*
		We need the response from search, exactly the same response from Traded Assets endpoint.
		- Source : for the data (Coingecko)
		- Total : the number of Assets that return after the search term
		- Assets : the data to be displayED on the page.
	*/
	var resp = NFTPricesResp{Source: data_source, Total: totalAssets, NFT: nfts}

	jsonData, err := json.Marshal(resp)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	span.SetStatus(codes.Ok, "success")
	return jsonData, nil
}

func fuzzyNFTSearch(ctx0 context.Context, searchTerm string, words *[]string, nfts *map[string][]NFTPrices) []NFTPrices {
	_, span := tracer.Start(ctx0, "fuzzyNFTSearch")
	defer span.End()

	var result []NFTPrices
	ranks := fuzzy.RankFindNormalized(searchTerm, *words) // case-insensitive & unicode-normalized fuzzy search.
	sort.Sort(ranks)                                      // sorts by the Levenshtein distance
	for rankIdx, rank := range ranks {
		if rankIdx >= defaultFuzzySearchLimit {
			break
		}
		result = append(result, (*nfts)[rank.Target]...)
	}

	span.SetStatus(codes.Ok, "success")
	return result
}

func RemoveDuplicateNFTs(ctx0 context.Context, nfts []NFTPrices) []NFTPrices {
	_, span := tracer.Start(ctx0, "RemoveDuplicateInactiveNFTs")
	defer span.End()

	seen := make(map[string]bool)
	result := []NFTPrices{}

	for _, nft := range nfts {
		_, seenAsset := seen[nft.ID]
		if !seenAsset {
			seen[nft.ID] = true
			result = append(result, nft)
		}
	}
	span.SetStatus(codes.Ok, "success")
	return result
}

func PaginateSortNFTs(ctx0 context.Context, allAssets *[]NFTPrices, paginate Paginate, ignoreInitialAssets int) []NFTPrices {
	_, span := tracer.Start(ctx0, "PaginateSortAssets")
	defer span.End()

	validatePaginate(ctx0, &paginate) // validate the paginate object
	if len(*allAssets) == 0 {
		return *allAssets
	}
	initialAssets := (*allAssets)[:ignoreInitialAssets]
	assets := (*allAssets)[ignoreInitialAssets:]
	sort.Slice(assets, func(i, j int) bool { // sort the nfts
		var result = j > i //defaults to sort by relevance.
		switch paginate.SortBy {
		case "volume":
			result = compareFloat(&assets[i].Volume24hUsd, &assets[j].Volume24hUsd, paginate.Direction)
		case "price":
			result = compareFloat(&assets[i].FloorPriceUsd, &assets[j].FloorPriceUsd, paginate.Direction)
		case "marketCap":
			result = compareFloat(&assets[i].MarketCapUsd, &assets[j].MarketCapUsd, paginate.Direction)
		case "percentage":
			result = compareFloat(&assets[i].FloorPriceInUsd24hPercentageChange, &assets[j].FloorPriceInUsd24hPercentageChange, paginate.Direction)
		case "owners":
			result = compareInt(&assets[i].NumberOfUniqueAddresses, &assets[j].NumberOfUniqueAddresses, paginate.Direction)
		case "name":
			if paginate.Direction == "asc" {
				result = strings.ToLower(assets[i].Name) < strings.ToLower(assets[j].Name)
			} else {
				result = strings.ToLower(assets[i].Name) > strings.ToLower(assets[j].Name)
			}
		}
		return result
	})
	assets = append(initialAssets, assets...)
	start := (paginate.PageNum - 1) * paginate.Limit // paginate the nfts
	end := start + paginate.Limit
	if start > len(assets) {
		return []NFTPrices{}
	}
	if end > len(assets) {
		end = len(assets)
	}
	span.SetStatus(codes.Ok, "success")
	return assets[start:end]
}

// Builds NFT Chain Dictionary from the chains & nfts table
func buildNFTChainsDictionary(ctx0 context.Context, nfts []NFTPrices) {
	_, span := tracer.Start(ctx0, "buildNFTChainsDictionary")
	defer span.End()

	words := []string{}
	dictionaryNft := make(map[string][]NFTPrices) //resetting the map

	// assign all the nfts to the each chain id
	for _, nft := range nfts {
		if nft.AssetPlatformId != "" {
			field := strings.ToLower(nft.AssetPlatformId)
			if !slices.Contains(words, field) {
				words = append(words, field)
			}
			dictionaryNft[field] = append(dictionaryNft[field], nft)
		}
	}
	nftChainsCache.nfts = dictionaryNft //resetting the map
	span.SetStatus(codes.Ok, "success")
}

// Search a dictionary for the given Chains. Name of dictionary is provided by the dictionaryCategory. Returns the NFTs related to the search term in this Chains.
func SearchTermByChains(ctx context.Context, searchTerm string, dictionaryCategory DictionaryCategory, paginate Paginate) ([]byte, error) {
	ctx0, span := tracer.Start(ctx, "SearchTermByChains")
	defer span.End()

	dictionaryLock.Lock()
	defer dictionaryLock.Unlock()

	// return the Non Fungible Token Category cache type
	// will use the chain id from paginate object
	dictionary := getDictionaryByCategory(ctx0, dictionaryCategory)

	// RebuildCache if the dictionary is empty. This happens when the server is restarted.
	if len(dictionary.words) == 0 {
		RebuildNFTCache(ctx0, true)
		dictionary = getDictionaryByCategory(ctx0, dictionaryCategory)
	}
	exactMatchFound := false
	var nftsResult []NFTPrices
	// Get all nfts that related to the chain
	nftsChains, exactMatchFound := dictionary.nfts[paginate.ChainID]
	/*
		The built NFTChainsWords function will take the nfts for the chain and
		return nfts for this chain as a map and the words array of nfts names to be searched in.
	*/
	nfts, words := buildNFTChainsWords(ctx0, nftsChains)
	dictionary.words = words
	searchTerm = strings.ToLower(searchTerm)
	// If there's exact match for the chain ID in the dictionary, we will use fuzzy search for the search term in this chain, and return the nfts.
	if exactMatchFound {
		nftsResult = fuzzyNFTSearch(ctx0, searchTerm, &dictionary.words, &nfts)
	}

	exactMatchAssets, isExact := nfts[searchTerm] //nfts that directly match the search term.
	exactMatchAssets = RemoveDuplicateNFTs(ctx0, exactMatchAssets)
	if isExact {
		nftsResult = append(exactMatchAssets, nftsResult...) // append the direct match nfts to the nfts array in the front.
	}
	nftsResult = RemoveDuplicateNFTs(ctx0, nftsResult)
	totalAssets := len(nftsResult)
	nftsResult = PaginateSortNFTs(ctx0, &nftsResult, paginate, len(exactMatchAssets))

	/*
		We need the response from search, exactly the same response from Traded Assets endpoint.
		- Source : for the data (Coingecko)
		- Total : the number of NFTs that return after the search term
		- Assets : the data to be displayed on the page.
	*/
	var resp = NFTPricesResp{Source: data_source, Total: totalAssets, NFT: nftsResult}

	jsonData, err := json.Marshal(resp)

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	span.SetStatus(codes.Ok, "success")
	return jsonData, nil
}

// It will take the NFTs by Chains and build the NFTs map and the words array
func buildNFTChainsWords(ctx0 context.Context, nftChains []NFTPrices) (map[string][]NFTPrices, []string) {
	_, span := tracer.Start(ctx0, "buildNFTChainsWords")
	defer span.End()

	words := []string{}
	dictionaryAssets := make(map[string][]NFTPrices)

	// Assign all the nfts to the dictionary by chain id
	// The nfts will be related to chain id that provided.
	for _, nft := range nftChains {
		if nft.Name != "" {
			field := strings.ToLower(nft.Name)
			if !slices.Contains(words, field) {
				words = append(words, field)
			}
			dictionaryAssets[field] = append(dictionaryAssets[field], nft)
		}
		if nft.Symbol != "" {
			field := strings.ToLower(nft.Symbol)
			if !slices.Contains(words, field) {
				words = append(words, field)
			}
			dictionaryAssets[field] = append(dictionaryAssets[field], nft)
		}
	}
	span.SetStatus(codes.Ok, "success")
	return dictionaryAssets, words
}



// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


/// test code to use genreics
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++



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

func categoryCache[T SearchInterface]() DictionaryTest[T] {
	var categoryCacheTest DictionaryTest[T] = DictionaryTest[T]{
		searchType: Exact,
		assets:     make(map[string][]T),
		category:   Category,
	}
	return categoryCacheTest
}

func ftCategoryCacheTest[T TableRow]() DictionaryTest[T] {
	var ftCategoryCacheTest DictionaryTest[T] = DictionaryTest[T]{
		searchType: Exact,
		assets:     make(map[string][]T),
		category:   FTCategory,
	}
	return ftCategoryCacheTest
}
func ftCacheTest[T TableRow]() DictionaryTest[T] {
	var ftCacheTest DictionaryTest[T] = DictionaryTest[T]{
		searchType: Exact,
		assets:     make(map[string][]T),
		category:   Ft,
	}
	return ftCacheTest
}
func nftCacheTest[T TableRow]() DictionaryTest[T] {
	var nftCacheTest DictionaryTest[T] = DictionaryTest[T]{
		searchType: Fuzzy,
		assets:     make(map[string][]T),
		category:   Nft,
	}
	return nftCacheTest
}
func nftChainsCacheTest[T TableRow]() DictionaryTest[T] {
	var nftChainsCacheTest DictionaryTest[T] = DictionaryTest[T]{
		searchType: Exact,
		assets:     make(map[string][]T),
		category:   NFTChains,
	}
	return nftChainsCacheTest
}

type TradedAssetsRespTest[T TableRow] struct {
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
func RebuildCacheTest[T TableRow](ctx context.Context, ignoreLock bool) error {
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
func fuzzySearchTest[T TableRow](ctx0 context.Context, searchTerm string, words *[]string, assets *map[string][]T) []T {
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
func SearchTermTest[T TableRow](ctx context.Context, searchTerm string, dictionaryCategory DictionaryCategory, paginate Paginate) ([]byte, error) {
	ctx0, span := tracer.Start(ctx, "SearchTerm")
	defer span.End()
	_ = nftCacheTest[T]()
	_ = nftChainsCacheTest[T]()
	_ = categoryCacheTest[T]()
	_ = ftCategoryCacheTest[T]()

	dictionaryLock.Lock()
	defer dictionaryLock.Unlock()

	dictionary := getDictionaryByCategoryTest[T](ctx0, dictionaryCategory)

	// RebuildCache if the dictionary is empty. This happens when the server is restarted.
	if len(dictionary.words) == 0 {
		// RebuildCacheTest[T](ctx0, true)
		RebuildNFTCacheTest[T](ctx0, true)
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
func getDictionaryByCategoryTest[T TableRow](ctx0 context.Context, dictionaryCategory DictionaryCategory) DictionaryTest[T] {
	_, span := tracer.Start(ctx0, "getDictionaryByCategory")
	defer span.End()

	var dictionary DictionaryTest[T]
	var allDictionaries = &[]DictionaryTest[T]{categoryCacheTest[T](), ftCacheTest[T](), nftCacheTest[T](), ftCategoryCacheTest[T](), nftChainsCacheTest[T]()}
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
func RemoveDuplicateInactiveAssetsTest[T TableRow](ctx0 context.Context, assets []T) []T {
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
func PaginateSortAssetsTest[T TableRow](ctx0 context.Context, allAssets []T, paginate Paginate, ignoreInitialAssets int) []T {
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

func GETFieldDataFloat[T TableRow](fieldOne *T, fieldTwo *T, fieldName string) (*float64, *float64) {
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
func GETFieldDataInt[T TableRow](fieldOne *T, fieldTwo *T, fieldName string) (*int, *int) {
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
func GETFieldDataString[T TableRow](fieldOne *T, fieldTwo *T, fieldName string) (string, string) {
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

func GETFieldString[T TableRow](fieldOne T, fieldName string) string {
	t := reflect.TypeOf(fieldOne)
	v := reflect.ValueOf(fieldOne)
	volume, _ := t.FieldByName(fieldName)
	index := volume.Index[0]
	value := v.Field(index).Interface().(string)
	return value
}

// Rebuilds the cache for NFTs the dictionaries. Usually this is called when the NFT fundamentals are rebuilt. IgnoreLock is used to ignore the dictionaryLock when the lock is already acquired by the function that is calling RebuildNFTCache( this ).
func RebuildNFTCacheTest[T TableRow](ctx context.Context, ignoreLock bool) error {
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
	buildNFTDictionaryTest(ctx0, nfts)
	buildNFTChainsDictionaryTest(ctx0, nfts)
	span.SetStatus(codes.Ok, "success")
	return nil
}

// Builds name & symbol dictionary from the assets list.
func buildFTDictionaryTest[T TableRow](ctx0 context.Context, assets []T) {
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
func buildCategoriesDictionaryTest[T TableRow](ctx0 context.Context, assets []T, categories []CategoriesData) {
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
func buildNFTChainsDictionaryTest[T TableRow](ctx0 context.Context, nfts []T) {
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
	nftChainsCache := nftChainsCacheTest[T]()
	nftChainsCache.assets = dictionaryNft //resetting the map
	span.SetStatus(codes.Ok, "success")
}

// It will build NFT data it will build the word and the NFTs arrays
func buildNFTDictionaryTest[T TableRow](ctx0 context.Context, nfts []T) {
	_, span := tracer.Start(ctx0, "buildNFTDictionary")
	defer span.End()

	words := []string{}
	dictionaryNft := make(map[string][]T) //resetting the map

	// assign all the nfts to the dictionary
	for _, nft := range nfts {
		name := GETFieldString(nft, "Name")
		symbol := GETFieldString(nft, "Symbol")
		if name != "" {
			field := strings.ToLower(name)
			if !slices.Contains(words, field) {
				words = append(words, field)
			}
			dictionaryNft[field] = append(dictionaryNft[field], nft)
		}
		if symbol != "" {
			field := strings.ToLower(symbol)
			if !slices.Contains(words, field) {
				words = append(words, field)
			}
			dictionaryNft[field] = append(dictionaryNft[field], nft)
		}
	}
	nftCache := nftCacheTest[T]()
	nftCache.words = words          //resetting the array
	nftCache.assets = dictionaryNft //resetting the map

	span.SetStatus(codes.Ok, "success")
}









// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


/// test code to use genreics
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++









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


