package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

type NFTsService interface {
	GetChainsList(ctx context.Context) ([]datastruct.NFTChain, error)
	BuildCache(ctx0 context.Context, buildType string) error
	SearchTerm(ctx context.Context, searchTerm string, paginate dto.Paginate) (*[]byte, int, error)
}

type nftsService struct {
	dao                     repository.DAO
	SearchType              dto.SearchType                    //For category, the type would be exact match. Otherwise we're keeping it Fuzzy.
	NFTTable                map[string][]datastruct.NFTPrices //Key = Search Term. Value = Array of Assets related to the search term.
	Words                   []string                          //Array of all search terms in the dictionary
	Category                dto.DictionaryDataSet             //Category of the dictionary
	Lock                    *sync.Mutex                       // A lock to be used when interacting with this object
	DefaultFuzzySearchLimit int                               //Limit of how many objects can be returned from a fuzzy search
	Datasource              string                            //Data Source is the source of the provider, or defined as calculated
}

func NewNFTsService(dao repository.DAO) NFTsService {
	return &nftsService{dao: dao, Category: dto.Nft, Lock: &sync.Mutex{}, DefaultFuzzySearchLimit: 100, Datasource: "calculated", SearchType: dto.Fuzzy}
}

// GetChainsList Attempts to Get NFTs Chain List from FS
// Takes a context
// Returns ([]datastruct.NFTChain, error)
//
// Takes the context
// Returns a []datastruct.NFTChain with all of the fields that we need from FS.
func (n *nftsService) GetChainsList(ctx context.Context) ([]datastruct.NFTChain, error) {

	nfts, err := n.dao.NewNFTsQuery().GetChainsList(ctx)
	if err != nil {
		log.Error("GetChainsList: Error Getting NFTs Chains from FS: %s", err)
		return nil, err
	}
	return nfts, nil
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
func (n *nftsService) SearchTerm(ctx context.Context, searchTerm string, paginate dto.Paginate) (*[]byte, int, error) {

	var returnData = dto.SearchResponse{}

	span, labels := common.GenerateSpan("V2 NFTsCache.SearchTerm", ctx)
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 NFTsCache.SearchTerm"))

	defer span.End()

	n.Lock.Lock()
	defer n.Lock.Unlock()

	if len(n.NFTTable) == 0 {
		err := errors.New("no cache detected")
		span.SetStatus(codes.Error, err.Error())
		log.AlertL(labels, err.Error())
		log.EndTimeL(labels, "V2 NFTsCache.SearchTerm", startTime, err)
		return nil, 0, err
	}
	var nfts []datastruct.NFTPrices
	var exactMatchAssets []datastruct.NFTPrices
	var nftsChains map[string][]datastruct.NFTPrices
	var isID bool

	if paginate.ChainID != "" {
		nftsChain, isChain := n.NFTTable[paginate.ChainID]
		if !isChain {
			err := errors.New("V2 NFTsCache.SearchTerm Failed to Assert Type []datastruct.NFTPrices ")
			span.SetStatus(codes.Error, err.Error())
			log.ErrorL(labels, err.Error())
			log.EndTimeL(labels, "V2 NFTsCache.SearchTerm", startTime, err)
			return nil, 0, err
		}

		nftsChains, n.Words = n.BuildNFTs(ctx, nftsChain)
		searchTerm = strings.ToLower(searchTerm)

		exactMatchAssets, isID = nftsChains[searchTerm]
	} else {
		searchTerm = strings.ToLower(searchTerm)
		exactMatchAssets, isID = n.NFTTable[searchTerm]
	}

	if isID {
		nfts = append(exactMatchAssets, nfts...)
	} else {
		exactMatchAssets, isExact := n.NFTTable[searchTerm]
		if isExact {
			nfts = append(exactMatchAssets, nfts...)
		} else if searchTerm == "" {
			for _, assetList := range n.NFTTable {
				nfts = append(nfts, assetList...)
			}
		} else if n.SearchType == dto.Fuzzy {
			nfts = n.fuzzySearch(ctx, searchTerm, &n.Words, &n.NFTTable)
		}
	}

	totalAssets := len(nfts)
	//if no data was found return
	if totalAssets <= 0 {
		jsonData, err := json.Marshal(returnData)

		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			log.ErrorL(labels, err.Error())
			log.EndTimeL(labels, "V2 NFTsCache.SearchTerm", startTime, err)
			return nil, 0, err
		}
		return &jsonData, totalAssets, nil
	}

	sortedResult := n.PaginateSortNFTs(ctx, &nfts, paginate, 0)

	nftsFundamentals, ok := sortedResult.([]datastruct.NFTPrices)
	nftsFundamentals = n.RemoveDuplicateNFTs(ctx, nftsFundamentals)

	if !ok {
		err := errors.New("V2 NFTsCache.SearchTerm Failed to Assert Type []datastruct.NFTPrices ")
		span.SetStatus(codes.Error, err.Error())
		log.ErrorL(labels, err.Error())
		log.EndTimeL(labels, "V2 NFTsCache.SearchTerm", startTime, err)
		return nil, 0, err
	}

	/*
		We need the response from nft/Prices, exactly the same response from Traded Assets endpoint.
		- Source : for the data (Coingecko)
		- Total : the number of Assets that return after the search term
		- NFTs : the data to be displayED on the page.
	*/
	returnData = dto.SearchResponse{Source: n.Datasource, Total: totalAssets, NFT: &nftsFundamentals}

	jsonData, err := json.Marshal(returnData)

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		log.ErrorL(labels, err.Error())
		log.EndTimeL(labels, "V2 NFTsCache.SearchTerm", startTime, err)
		return nil, 0, err
	}
	span.SetStatus(codes.Ok, "success")
	return &jsonData, len(nftsFundamentals), nil
}

// BuildCache Attempts to Build NFTs cached data
// Takes a (ctx0 context.Context, buildType string)
// Returns error
//
// Takes
// - context
// - buildType to determine what type of cache we need to build (NFTs cache or NFTs Chains cache).
// Returns an error if the build failed
func (n *nftsService) BuildCache(ctx0 context.Context, buildType string) error {
	_, span := tracer.Start(ctx0, "buildFTDictionary")
	defer span.End()
	searchQuery := n.dao.NewNFTsQuery()
	nfts, err := searchQuery.GetNFTPricesFundamentals(ctx0)
	if err != nil {
		log.Alert("failed to load cache: %v", err)
		return err
	}
	var words = []string{}
	var dictionaryNFTs = make(map[string][]datastruct.NFTPrices) //resetting the map
	// Build the dictionary for NFTChains or NFTs
	if buildType == "chains" {
		dictionaryNFTs, words = n.BuildNFTsChains(ctx0, nfts)
	} else {
		dictionaryNFTs, words = n.BuildNFTs(ctx0, nfts)
	}
	n.Lock.Lock()
	defer n.Lock.Unlock()

	n.NFTTable = dictionaryNFTs
	n.Words = words

	span.SetStatus(codes.Ok, "success")
	return nil

}

// BuildNFTs Attempts to Build NFTs cached data
// Takes a (ctx0 context.Context, nfts []datastruct.NFTPrices)
// Returns error
//
// Takes
// - context
// - Array of NFTs
// Returns (map[string][]datastruct.NFTPrices, []string)
// - map[string][]datastruct.NFTPrices this is a map of all NFTs value that we have.
// - []string this an array of words we will use to in fuzzy search.
func (n *nftsService) BuildNFTs(ctx0 context.Context, nfts []datastruct.NFTPrices) (map[string][]datastruct.NFTPrices, []string) {
	_, span := tracer.Start(ctx0, "buildFTDictionary")
	defer span.End()

	words := []string{}
	dictionaryAssets := make(map[string][]datastruct.NFTPrices)
	// assign all the nfts to the dictionary
	for _, nft := range nfts {
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

// BuildNFTsChains Attempts to Build NFTsChains cached data
// Takes a (ctx0 context.Context, nfts []datastruct.NFTPrices)
// Returns error
//
// Takes
// - context
// - Array of NFTs
// Returns (map[string][]datastruct.NFTPrices, []string)
// - map[string][]datastruct.NFTPrices this is a map of all NFTs value that we have.
// - []string this an array of words we will use to in fuzzy search.
func (n *nftsService) BuildNFTsChains(ctx0 context.Context, nfts []datastruct.NFTPrices) (map[string][]datastruct.NFTPrices, []string) {
	_, span := tracer.Start(ctx0, "BuildNFTsChains")
	defer span.End()
	words := []string{}
	dictionaryAssets := make(map[string][]datastruct.NFTPrices)
	for _, nft := range nfts {
		if nft.AssetPlatformId != "" {
			field := strings.ToLower(nft.AssetPlatformId)
			if !slices.Contains(words, field) {
				words = append(words, field)
			}
			dictionaryAssets[field] = append(dictionaryAssets[field], nft)
		}
	}
	span.SetStatus(codes.Ok, "success")
	return dictionaryAssets, words
}

// todo try to build with categories using generics
func (n *nftsService) fuzzySearch(ctx context.Context, searchTerm string, words *[]string, assets *map[string][]datastruct.NFTPrices) []datastruct.NFTPrices {
	_, span := tracer.Start(ctx, "fuzzySearch")
	defer span.End()

	var result []datastruct.NFTPrices
	ranks := fuzzy.RankFindNormalized(searchTerm, *words) // case-insensitive & unicode-normalized fuzzy search.
	sort.Sort(ranks)                                      // sorts by the Levenshtein distance
	for rankIdx, rank := range ranks {
		if rankIdx >= n.DefaultFuzzySearchLimit {
			break
		}
		result = append(result, (*assets)[rank.Target]...)
	}

	span.SetStatus(codes.Ok, "success")
	return result
}

// PaginateSortNFTs Attempts to Build the result after it paginate and sort the NFTs data
// Takes a (ctx context.Context, allNFTs interface{}, paginate dto.Paginate, ignoreInitialAssets int)
// Returns interface
//
// Takes
// - context
// - Array of NFTs
// - paginate:  object that contains the limit for the data, sort Direction for the data and what we need to SortBy
// - ignoreInitialAssets : the number of result that will be ignored
// Returns interface{} this will the result that we have for NFTs
func (n *nftsService) PaginateSortNFTs(ctx context.Context, allNFTs interface{}, paginate dto.Paginate, ignoreInitialAssets int) interface{} {
	span, labels := common.GenerateSpan("V2 NFTsCache.SearchTerm", ctx)
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 NFTsCache.SearchTerm"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 NFTsCache.SearchTerm"))
	defer span.End()

	validatePaginate(ctx, &paginate) // validate the paginate object

	nftsFundamentals, ok := allNFTs.(*[]datastruct.NFTPrices)
	if !ok {
		err := errors.New("V2 NFTsCache.PaginateSortAssets Failed to Assert Type []datastruct.NFTPrices ")
		span.SetStatus(codes.Error, err.Error())
		log.AlertL(labels, err.Error())
		log.EndTimeL(labels, "V2 NFTsCache.PaginateSortAssets", startTime, err)
		return nil
	}

	if len(*nftsFundamentals) == 0 {
		return nftsFundamentals
	}

	initialAssets := (*nftsFundamentals)[:ignoreInitialAssets]
	nfts := (*nftsFundamentals)[ignoreInitialAssets:]

	// if we need to sort by name we will use the new sort function to make sure ti will sort the data as we need
	if paginate.SortBy == "name" {
		nfts = paginateSortNFTsByNames(ctx, paginate, nfts)
	} else {
		sort.Slice(nfts, func(i, j int) bool { // sort the nfts
			var result = j > i //defaults to sort by relevance.
			switch paginate.SortBy {
			case "volume":
				result = compareFloat(&nfts[i].Volume24hUsd, &nfts[j].Volume24hUsd, paginate.Direction)
			case "price":
				result = compareFloat(&nfts[i].FloorPriceUsd, &nfts[j].FloorPriceUsd, paginate.Direction)
			case "marketCap":
				result = compareFloat(&nfts[i].MarketCapUsd, &nfts[j].MarketCapUsd, paginate.Direction)
			case "percentage":
				result = compareFloat(&nfts[i].Volume24hPercentageChangeUsd, &nfts[j].Volume24hPercentageChangeUsd, paginate.Direction)
			case "transactions":
				result = compareFloat(&nfts[i].NumberOfUniqueAddresses24hPercentageChange, &nfts[j].NumberOfUniqueAddresses24hPercentageChange, paginate.Direction)
			case "total":
				result = compareFloat(&nfts[i].TotalSupply, &nfts[j].TotalSupply, paginate.Direction)
			case "owners":
				result = compareInt(&nfts[i].NumberOfUniqueAddresses, &nfts[j].NumberOfUniqueAddresses, paginate.Direction)
			}
			return result
		})
	}

	nfts = append(initialAssets, nfts...)
	start := (paginate.PageNum - 1) * paginate.Limit // paginate the nfts
	end := start + paginate.Limit
	if start > len(nfts) {
		log.EndTimeL(labels, "V2 NFTsCache.PaginateSortAssets", startTime, nil)
		return []datastruct.NFTPrices{}
	}
	if end > len(nfts) {
		end = len(nfts)
	}
	log.EndTimeL(labels, "V2 NFTsCache.PaginateSortAssets", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return nfts[start:end]
}

// the sort function will sort the data after we filter it with regex
// it will sort data by ASC order for all data
func NFTsSortFunctionality(assets []datastruct.NFTPrices) {
	sort.Slice(assets, func(i, j int) bool {
		var res = j > i
		res = strings.ToLower(assets[i].Name) < strings.ToLower(assets[j].Name)
		return res
	})
}

// Special Sort Function we will use it only if we need to sort by NFTs Names
func paginateSortNFTsByNames(ctx context.Context, paginate dto.Paginate, assets []datastruct.NFTPrices) []datastruct.NFTPrices {
	span, labels := common.GenerateSpan("V2 NFTsCache.paginateSortNFTsByNames", ctx)
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 NFTsCache.paginateSortNFTsByNames"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 NFTsCache.paginateSortNFTsByNames"))
	defer span.End()

	// build array of all cases that exist in NFTs name
	specialNames, numberNames, normalNames := buildNFTsDataUsingRegexFilter(assets)

	// Sort Names Start with Special Characters
	NFTsSortFunctionality(specialNames)
	// Sort Names start with numeric Characters
	NFTsSortFunctionality(numberNames)
	// Sort Names start with Alphabetic Characters
	NFTsSortFunctionality(normalNames)

	// Build NFTs response Sorted by name
	// If the sort ASC the result will return in this order --> normalName --> numericName --> specialName
	// If the sort DESC the result will return in this order --> numericName --> normalName --> specialName
	if paginate.Direction == "asc" {
		// It will sort data using ASC like: a-z --> 0-9 --> special
		assets = buildOrderedNFTsPrices(normalNames, numberNames, specialNames)
	} else if paginate.Direction == "desc" {
		// It will sort data using DESC like: 0-9 --> a-z --> special
		assets = buildOrderedNFTsPrices(numberNames, normalNames, specialNames)
	}
	log.EndTimeL(labels, "V2 NFTsCache.PaginateSortAssets", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return assets
}

// Use this function to build the NFTs for all names after the filter and the sort functions
// it will return an array of NFTs that ordered as we need
// Asc order will return ==> normalName --> numericName --> specialName
// Desc order will return ==> numericName --> normalName --> specialName
func buildOrderedNFTsPrices(normalNames []datastruct.NFTPrices, numericNames []datastruct.NFTPrices, specialNames []datastruct.NFTPrices) []datastruct.NFTPrices {
	var combinedPrices []datastruct.NFTPrices
	combinedPrices = append(combinedPrices, normalNames...)
	combinedPrices = append(combinedPrices, numericNames...)
	combinedPrices = append(combinedPrices, specialNames...)
	return combinedPrices
}

// Build the array of names that match the regex
// use regex to build the data array that match each filter.
func buildNFTsDataUsingRegexFilter(assets []datastruct.NFTPrices) ([]datastruct.NFTPrices, []datastruct.NFTPrices, []datastruct.NFTPrices) {
	var specialNames []datastruct.NFTPrices
	var numberNames []datastruct.NFTPrices
	var normalNames []datastruct.NFTPrices
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

	// check if the NFT name match the regex condition then append it to NFTs Price Array.
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

// RemoveDuplicateNFTs Attempts to Build an Array of unique NFTs
// Takes a (ctx0 context.Context, nfts []datastruct.NFTPrices)
// Returns []datastruct.NFTPrices
//
// Takes
// - context
// - Array of NFTs
// Returns []datastruct.NFTPrices it's an array on unique NFTs data.
func (n *nftsService) RemoveDuplicateNFTs(ctx0 context.Context, nfts []datastruct.NFTPrices) []datastruct.NFTPrices {
	_, span := tracer.Start(ctx0, "RemoveDuplicateNFTs")
	defer span.End()

	seen := make(map[string]bool)
	result := []datastruct.NFTPrices{}

	for _, nft := range nfts {
		_, seenNFT := seen[nft.ID]
		if !seenNFT {
			seen[nft.ID] = true
			// If the ContractAddress is empty for any NFT existing in any Chains,
			// we need to create a unique value for it. So we concatenate the ID, Name, and Floor price Usd to ensure we have the unique value for this NFT.
			if nft.ContractAddress == "" {
				uuid := fmt.Sprintf("%s_%s_%v", nft.ID, strings.ReplaceAll(nft.Name, " ", ""), nft.FloorPriceUsd)
				nft.UUID = uuid
			}
			result = append(result, nft)
		}
	}
	span.SetStatus(codes.Ok, "success")
	return result
}
