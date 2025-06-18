// This package is responsible exposing functions that contains core business logic
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
	"github.com/Forbes-Media/forbes-digital-assets/refactored/repository"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
)

/*
This Service contains all of the business logic that is required to power the nft on the FDA homepage
*/
type NftService interface {
	GetNftCollection(context.Context, dto.NftCollectionRequest) (*datastruct.NftCollection, error)           // Returns all asset information in regards to an nft collection
	GetChainsList(ctx context.Context) ([]datastruct.NFTChain, error)                                        // get All NFT Chains from FS
	BuildCache(ctx0 context.Context) error                                                                   // Build NFT data from PG
	SearchNftsTerm(ctx context.Context, searchTerm string, paginate dto.Paginate) (*[]byte, int, error)      // Search for NFT by Term
	SearchNFTChainsTerm(ctx context.Context, searchTerm string, paginate dto.Paginate) (*[]byte, int, error) // Search NFT by Chain
	BuildNFTCustomCategoriesData(ctx context.Context) error                                                  // Build NFT custom categories to FS
	BuildForbesNFTCustomCategoriesData(ctx context.Context, customNFT datastruct.CustomNFT) error            // Build NFT custom categories to FS
}

// Create object for the service that contains a repository.nft interface
type nftService struct {
	dao                     repository.DAO
	SearchType              dto.SearchType                    //For category, the type would be exact match. Otherwise we're keeping it Fuzzy.
	NFTTable                map[string][]datastruct.NFTPrices //Key = Search NFT Term. Value = Array of Assets related to the search term.
	NFTChainsTable          map[string][]datastruct.NFTPrices //Key = Search NFT Chains Term. Value = Array of Assets related to the search term.
	Words                   []string                          //Array of all search terms in the dictionary
	Category                dto.DictionaryDataSet             //Category of the dictionary
	Lock                    *sync.Mutex                       // A lock to be used when interacting with this object
	DefaultFuzzySearchLimit int                               //Limit of how many objects can be returned from a fuzzy search
	Datasource              string                            //Data Source is the source of the provider, or defined as calculated
}

// NewNftService Attempts to Get Access to all Nft functions
// Takes a repository.DAO so we can use our Query functions
// Returns (NftService)
//
// Takes the dao and return NftService with dao to access all our functions in Nft to get data from our Storage
// Returns a NftService interface for Nft
func NewNftService(dao repository.DAO) NftService {
	return &nftService{dao: dao, Category: dto.Nft, Lock: &sync.Mutex{}, DefaultFuzzySearchLimit: 100, Datasource: "calculated", SearchType: dto.Fuzzy}
}

// GetNft Attempts to Get asset information from a users nft
// Takes a context and a dto.NftRequest object
// Returns (*datastruct.Nft, error)
//
// Takes the ID form the nftRequest and then
// Then it calls  w.dao.NftQuery().GetNftCollection for the collection in Postgres to retrieve info about the Collection
// Returns a *datastruct.NftCollection with all of the populated info
// If the error is nill and the collection is also nil, then the collection is not found
func (w *nftService) GetNftCollection(ctx context.Context, req dto.NftCollectionRequest) (*datastruct.NftCollection, error) {
	var (
		err error
		nft *datastruct.NftCollection
	)

	span, labels := common.GenerateSpan("V2 NftService.GetNftCollection", ctx)
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 NftService.GetNftCollection"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 NftService.GetNftCollection"))
	defer span.End()

	nft, err = w.dao.NftQuery().GetNftCollection(ctx, req.Slug)
	if err != nil {
		log.ErrorL(labels, "%s", err)
		log.EndTimeL(labels, "V2 NftService.GetNftCollection", startTime, err)
		return nil, err
	}
	log.EndTimeL(labels, "V2 NftService.GetNftCollection", startTime, err)
	return nft, nil
}

// GetChainsList Attempts to Get NFTs Chain List from FS
// Takes a context
// Returns ([]datastruct.NFTChain, error)
//
// Takes the context
// Returns a []datastruct.NFTChain with all of the fields that we need from FS.
func (n *nftService) GetChainsList(ctx context.Context) ([]datastruct.NFTChain, error) {

	span, labels := common.GenerateSpan("V2 NftService.GetChainsList", ctx)
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 NftService.GetChainsList"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 NftService.GetChainsList"))
	defer span.End()
	nfts, err := n.dao.NftQuery().GetChainsList(ctx)
	if err != nil {
		log.ErrorL(labels, "V2 NftService.GetChainsList Error Getting NFTs Chains from FS: %s", err)
		return nil, err
	}
	log.EndTimeL(labels, "V2 NftService.GetChainsList", startTime, err)
	span.SetStatus(codes.Ok, "success")
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
func (n *nftService) SearchNftsTerm(ctx context.Context, searchTerm string, paginate dto.Paginate) (*[]byte, int, error) {

	var returnData = dto.SearchResponse{}

	span, labels := common.GenerateSpan("V2 NftService.SearchTerm", ctx)
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 NftService.SearchTerm"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 NftService.SearchTerm"))
	defer span.End()

	n.Lock.Lock()
	defer n.Lock.Unlock()

	if len(n.NFTTable) == 0 {
		err := errors.New("no cache detected")
		log.ErrorL(labels, "V2 NftService.SearchTerm Error with cache: %s", err)
		log.EndTimeL(labels, "V2 NftService.SearchTerm", startTime, err)
		return nil, 0, err
	}

	searchTerm = strings.ToLower(searchTerm)

	var nfts []datastruct.NFTPrices
	exactMatchAssets, isID := n.NFTTable[searchTerm]

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
			nfts = fuzzySearch(ctx, searchTerm, &n.Words, &n.NFTTable, n.DefaultFuzzySearchLimit)
		}
	}

	totalAssets := len(nfts)
	//if no data was found return
	if totalAssets <= 0 {
		jsonData, err := json.Marshal(returnData)

		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			log.ErrorL(labels, "V2 NftService.SearchTerm Converting empty data : %s", err.Error())
			log.EndTimeL(labels, "V2 NftService.SearchTerm", startTime, err)
			return nil, 0, err
		}
		return &jsonData, totalAssets, nil
	}

	sortedResult := n.PaginateSortNFTs(ctx, &nfts, paginate, 0)

	nftsFundamentals, ok := sortedResult.([]datastruct.NFTPrices)
	nftsFundamentals = n.RemoveDuplicateNFTs(ctx, nftsFundamentals)

	if !ok {
		err := errors.New("V2 NftService.SearchTerm Failed to Assert Type []datastruct.NFTPrices ")
		span.SetStatus(codes.Error, err.Error())
		log.ErrorL(labels, "V2 NftService.SearchTerm Data Sorted Error : %s", err.Error())
		log.EndTimeL(labels, "V2 NftService.SearchTerm", startTime, err)
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
		log.ErrorL(labels, "V2 NftService.SearchTerm Converting data Error : %s", err.Error())
		log.EndTimeL(labels, "V2 NftService.SearchTerm", startTime, err)
		return nil, 0, err
	}
	log.EndTimeL(labels, "V2 NftService.SearchTerm", startTime, err)
	span.SetStatus(codes.Ok, "success")
	return &jsonData, len(nftsFundamentals), nil
}

// SearchChainsTerm Attempts to Search in NFTs Chains data and returns the data if the search term exist if not it returns the data
// Takes a (ctx context.Context, searchTerm string, paginate dto.Paginate)
// Returns (*[]byte, int, error)
//
// Takes
// - context
// - searchTerm for what we need to found in NFTsChains data.
// - paginate for what we need to found in NFTs data.
// Returns a Nfts response with all data we need.
func (n *nftService) SearchNFTChainsTerm(ctx context.Context, searchTerm string, paginate dto.Paginate) (*[]byte, int, error) {

	var returnData = dto.SearchResponse{}

	span, labels := common.GenerateSpan("V2 NftService.SearchChainsTerm", ctx)
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 NftService.SearchChainsTerm"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 NftService.SearchChainsTerm"))
	defer span.End()

	n.Lock.Lock()
	defer n.Lock.Unlock()

	if len(n.NFTChainsTable) == 0 {
		err := errors.New("no cache detected")
		span.SetStatus(codes.Error, err.Error())
		log.AlertL(labels, err.Error())
		log.EndTimeL(labels, "V2 NftService.SearchChainsTerm", startTime, err)
		return nil, 0, err
	}
	nftsChain, isChain := n.NFTChainsTable[paginate.ChainID]
	if !isChain {
		err := errors.New("V2 NftService.SearchChainsTerm Can't found the Chain")
		log.ErrorL(labels, err.Error())
		log.EndTimeL(labels, "V2 NftService.SearchChainsTerm", startTime, err)
		return nil, 0, err
	}

	nftsChains, _, words := n.BuildNFTs(ctx, nftsChain)
	searchTerm = strings.ToLower(searchTerm)

	var nfts []datastruct.NFTPrices
	exactMatchAssets, isID := nftsChains[searchTerm]

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
			nfts = fuzzySearch(ctx, searchTerm, &words, &nftsChains, n.DefaultFuzzySearchLimit)
		}
	}

	totalAssets := len(nfts)
	//if no data was found return
	if totalAssets <= 0 {
		jsonData, err := json.Marshal(returnData)

		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			log.ErrorL(labels, err.Error())
			log.EndTimeL(labels, "V2 NftService.SearchChainsTerm Converting Empty Data", startTime, err)
			return nil, 0, err
		}
		return &jsonData, totalAssets, nil
	}

	sortedResult := n.PaginateSortNFTs(ctx, &nfts, paginate, 0)

	nftsFundamentals, ok := sortedResult.([]datastruct.NFTPrices)
	nftsFundamentals = n.RemoveDuplicateNFTs(ctx, nftsFundamentals)

	if !ok {
		err := errors.New("V2 NftService.SearchChainsTerm Failed to Sorted Type []datastruct.NFTPrices")
		span.SetStatus(codes.Error, err.Error())
		log.ErrorL(labels, err.Error())
		log.EndTimeL(labels, "V2 NftService.SearchChainsTerm", startTime, err)
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
		log.EndTimeL(labels, "V2 NftService.SearchChainsTerm Converting Data Error", startTime, err)
		return nil, 0, err
	}
	log.EndTimeL(labels, "V2 NftService.SearchTerm", startTime, err)
	span.SetStatus(codes.Ok, "success")
	return &jsonData, len(nftsFundamentals), nil
}

// BuildCache Attempts to Build NFTs cached data
// Takes a (ctx0 context.Context)
// Returns error
//
// Takes
// - context
// Build NFTs cache amd NFTs Chains cache.
// Returns an error if the build failed
func (n *nftService) BuildCache(ctx context.Context) error {
	span, labels := common.GenerateSpan("V2 NftService.BuildCache", ctx)
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 NftService.BuildCache"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 NftService.BuildCache"))
	defer span.End()
	searchQuery := n.dao.NftQuery()
	nfts, err := searchQuery.GetNFTPricesFundamentals(ctx)
	if err != nil {
		log.ErrorL(labels, "Error V2 NftService.BuildCache Getting NFTs data fro PG %s", err.Error())
		return err
	}
	var words = []string{}
	var dictionaryNFTs = make(map[string][]datastruct.NFTPrices)
	var dictionaryNFTChains = make(map[string][]datastruct.NFTPrices)
	n.Lock.Lock()
	defer n.Lock.Unlock()
	dictionaryNFTs, dictionaryNFTChains, words = n.BuildNFTs(ctx, nfts)
	n.NFTTable = dictionaryNFTs
	n.NFTChainsTable = dictionaryNFTChains

	n.Words = words

	log.EndTimeL(labels, "V2 NftService.BuildCache", startTime, err)
	span.SetStatus(codes.Ok, "success")
	return nil

}

// BuildNFTs Attempts to Build NFTs cached data and NFTChains cache data
// Takes a (ctx0 context.Context, nfts []datastruct.NFTPrices)
// Returns error
//
// Takes
// - context
// - Array of NFTs
// Returns (map[string][]datastruct.NFTPrices, []string)
// - map[string][]datastruct.NFTPrices this is a map of all NFTs value that we have.
// - []string this an array of words we will use to in fuzzy search.
func (n *nftService) BuildNFTs(ctx context.Context, nfts []datastruct.NFTPrices) (map[string][]datastruct.NFTPrices, map[string][]datastruct.NFTPrices, []string) {
	span, labels := common.GenerateSpan("V2 NftService.BuildNFTs", ctx)
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 NftService.BuildNFTs"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 NftService.BuildNFTs"))
	defer span.End()

	words := []string{}
	dictionaryNFT := make(map[string][]datastruct.NFTPrices)
	dictionaryNFTChains := make(map[string][]datastruct.NFTPrices)
	// assign all the nfts to the dictionary
	for _, nft := range nfts {
		if nft.Name != "" {
			field := strings.ToLower(nft.Name)
			if !slices.Contains(words, field) {
				words = append(words, field)
			}
			dictionaryNFT[field] = append(dictionaryNFT[field], nft)
		}
		if nft.Symbol != "" {
			field := strings.ToLower(nft.Symbol)
			if !slices.Contains(words, field) {
				words = append(words, field)
			}
			dictionaryNFT[field] = append(dictionaryNFT[field], nft)
		}
		if nft.AssetPlatformId != "" {
			field := strings.ToLower(nft.AssetPlatformId)
			dictionaryNFTChains[field] = append(dictionaryNFTChains[field], nft)
		}
	}
	log.EndTimeL(labels, "V2 NftService.BuildNFTs", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return dictionaryNFT, dictionaryNFTChains, words
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
func (n *nftService) PaginateSortNFTs(ctx context.Context, allNFTs interface{}, paginate dto.Paginate, ignoreInitialAssets int) interface{} {
	span, labels := common.GenerateSpan("V2 NftService.PaginateSortNFTs", ctx)
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 NftService.PaginateSortNFTs"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 NftService.PaginateSortNFTs"))
	defer span.End()

	validatePaginate(ctx, &paginate) // validate the paginate object

	nftsFundamentals, ok := allNFTs.(*[]datastruct.NFTPrices)
	if !ok {
		err := errors.New("V2 NftService.PaginateSortNFTs Failed to Assert Type []datastruct.NFTPrices ")
		span.SetStatus(codes.Error, err.Error())
		log.ErrorL(labels, err.Error())
		log.EndTimeL(labels, "V2 NftService.PaginateSortNFTs", startTime, err)
		return nil
	}

	if len(*nftsFundamentals) == 0 {
		return nftsFundamentals
	}

	initialAssets := (*nftsFundamentals)[:ignoreInitialAssets]
	nfts := (*nftsFundamentals)[ignoreInitialAssets:]

	// if we need to sort by name we will use the new sort function to make sure ti will sort the data as we need
	if paginate.SortBy == "name" {
		nfts = paginateSortCryptoByNames[datastruct.NFTPrices](ctx, paginate, nfts)
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
		log.ErrorL(labels, "V2 NftService.PaginateSortNFTs Start limit more than Nfts data length")
		log.EndTimeL(labels, "V2 NftService.PaginateSortNFTs", startTime, nil)
		return []datastruct.NFTPrices{}
	}
	if end > len(nfts) {
		end = len(nfts)
	}
	log.EndTimeL(labels, "V2 NftService.PaginateSortNFTs", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return nfts[start:end]
}

// RemoveDuplicateNFTs Attempts to Build an Array of unique NFTs
// Takes a (ctx0 context.Context, nfts []datastruct.NFTPrices)
// Returns []datastruct.NFTPrices
//
// Takes
// - context
// - Array of NFTs
// Returns []datastruct.NFTPrices it's an array on unique NFTs data.
func (n *nftService) RemoveDuplicateNFTs(ctx context.Context, nfts []datastruct.NFTPrices) []datastruct.NFTPrices {
	span, labels := common.GenerateSpan("V2 NftService.RemoveDuplicateNFTs", ctx)
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 NFTsCache.RemoveDuplicateNFTs"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 NFTsCache.RemoveDuplicateNFTs"))
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
	log.EndTimeL(labels, "V2 NftService.RemoveDuplicateNFTs", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return result
}

// BuildNFTCustomCategoriesData Attempts to Build NFTs Custom Categories from FS and PG
// Takes a (ctx context.Context)
// Returns error
//
// Returns nil if the process build successfully.
func (n *nftService) BuildNFTCustomCategoriesData(ctx context.Context) error {
	span, labels := common.GenerateSpan("V2 NftService.BuildNFTCustomCategoriesData", ctx)
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 NFTsCache.BuildNFTCustomCategoriesData"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 NFTsCache.BuildNFTCustomCategoriesData"))
	defer span.End()
	queryMag := n.dao.NftQuery()
	assetsPlatformIds, err := queryMag.GetNFTAssetsPlatformID(ctx)
	if err != nil {
		err := errors.New("V2 NftService.BuildNFTCustomCategoriesData Failed to fetch NFT assetsPlatformIds from PG ")
		span.SetStatus(codes.Error, err.Error())
		log.ErrorL(labels, err.Error())
		log.EndTimeL(labels, "V2 NftService.BuildNFTCustomCategoriesData", startTime, err)
		return nil
	}

	nftChains, err := queryMag.GetChainsList(ctx)

	if err != nil {
		err := errors.New("V2 NftService.BuildNFTCustomCategoriesData Failed to Fetch NFT Chains from FS")
		span.SetStatus(codes.Error, err.Error())
		log.ErrorL(labels, err.Error())
		log.EndTimeL(labels, "V2 NftService.BuildNFTCustomCategoriesData", startTime, err)
		return nil
	}
	chainResult := n.MapNFTsChains(ctx, assetsPlatformIds, nftChains)

	err = queryMag.SaveNFTCategories(ctx, chainResult)

	if err != nil {
		err := errors.New("V2 NftService.BuildNFTCustomCategoriesData Failed To save the NFT Categories to FS")
		span.SetStatus(codes.Error, err.Error())
		log.ErrorL(labels, err.Error())
		log.EndTimeL(labels, "V2 NftService.BuildNFTCustomCategoriesData", startTime, err)
		return nil
	}

	log.EndTimeL(labels, "V2 NftService.BuildNFTCustomCategoriesData", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return nil
}

// MapNFTsChains Attempts to Map the Categories to Chains
// Takes a (ctx context.Context, assetsPlatformIds []string, chains []datastruct.NFTChain)
// Returns []datastruct.NFTChain
// Takes:
//   - assetsPlatformIds all new Categories.
//   - chains exists chains in FS
//
// Returns []datastruct.NFTChain after Map all new categories to our exist chains.
func (n *nftService) MapNFTsChains(ctx context.Context, assetsPlatformIds []string, chains []datastruct.NFTChain) []datastruct.NFTChain {
	span, labels := common.GenerateSpan("V2 NftService.BuildNFTCustomCategoriesData", ctx)
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 NFTsCache.BuildNFTCustomCategoriesData"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 NFTsCache.BuildNFTCustomCategoriesData"))
	defer span.End()
	var nftChains []datastruct.NFTChain
	chainsMap := n.ConvertNFTsChains(ctx, chains)

	for _, category := range assetsPlatformIds {
		value, ok := chainsMap[category]
		if ok {
			nftChains = append(nftChains, value)
		} else {
			c := datastruct.NFTChain{ID: category, Name: strings.Title(category)}
			nftChains = append(nftChains, c)
		}
	}

	log.EndTimeL(labels, "V2 NftService.BuildNFTCustomCategoriesData", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return nftChains
}

// ConvertNFTsChains Attempts to Convert NFT Chains to Map object
// Takes a (ctx context.Context, chains []datastruct.NFTChain)
// Returns map[string]datastruct.NFTChain
//
// Returns map[string]datastruct.NFTChain after map all Id to each NFTChain object to make the access to it by Id.
func (n *nftService) ConvertNFTsChains(ctx context.Context, chains []datastruct.NFTChain) map[string]datastruct.NFTChain {
	span, labels := common.GenerateSpan("V2 NftService.ConvertNFTsChains", ctx)
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 NFTsCache.ConvertNFTsChains"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 NFTsCache.ConvertNFTsChains"))
	defer span.End()
	nftChains := make(map[string]datastruct.NFTChain)

	for _, chain := range chains {
		nftChains[chain.ID] = chain
	}

	log.EndTimeL(labels, "V2 NftService.ConvertNFTsChains", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return nftChains
}

// BuildForbesNFTCustomCategoriesData Attempts to Build NFTs Custom Categories from FS and PG
// Takes a (ctx context.Context)
// Returns error
//
// Returns nil if the process build successfully.
func (n *nftService) BuildForbesNFTCustomCategoriesData(ctx context.Context, customNFT datastruct.CustomNFT) error {
	span, labels := common.GenerateSpan("V2 NftService.BuildForbesNFTCustomCategoriesData", ctx)
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 NFTsCache.BuildForbesNFTCustomCategoriesData"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 NFTsCache.BuildForbesNFTCustomCategoriesData"))
	defer span.End()
	queryMag := n.dao.NftQuery()
	assetsPlatformIds, err := queryMag.GetNFTAssetsPlatformID(ctx)
	if err != nil {
		err := errors.New("V2 NftService.BuildForbesNFTCustomCategoriesData Failed to fetch NFT assetsPlatformIds from PG ")
		span.SetStatus(codes.Error, err.Error())
		log.ErrorL(labels, err.Error())
		log.EndTimeL(labels, "V2 NftService.BuildForbesNFTCustomCategoriesData", startTime, err)
		return nil
	}

	nftChains, err := queryMag.GetChainsList(ctx)

	if err != nil {
		err := errors.New("V2 NftService.BuildForbesNFTCustomCategoriesData Failed to Fetch NFT Chains from FS")
		span.SetStatus(codes.Error, err.Error())
		log.ErrorL(labels, err.Error())
		log.EndTimeL(labels, "V2 NftService.BuildForbesNFTCustomCategoriesData", startTime, err)
		return nil
	}

	forbesNFTCategories, err := queryMag.GetFOrbesNFTCategoriesList(ctx)

	if err != nil {
		err := errors.New("V2 NftService.BuildNFTCustomCategoriesData Failed to Fetch NFT Chains from FS")
		span.SetStatus(codes.Error, err.Error())
		log.ErrorL(labels, err.Error())
		log.EndTimeL(labels, "V2 NftService.BuildNFTCustomCategoriesData", startTime, err)
		return nil
	}

	chainResult := n.MapNFTsChains(ctx, assetsPlatformIds, nftChains)

	nftPlatformResult := n.MapForbesNFTsPlatforms(ctx, forbesNFTCategories, customNFT)

	err = queryMag.SaveNFTCategories(ctx, chainResult)

	if err != nil {
		err := errors.New("V2 NftService.BuildForbesNFTCustomCategoriesData Failed To save the NFT Categories to FS")
		span.SetStatus(codes.Error, err.Error())
		log.ErrorL(labels, err.Error())
		log.EndTimeL(labels, "V2 NftService.BuildForbesNFTCustomCategoriesData", startTime, err)
		return nil
	}

	err = queryMag.UpsertNFTCategories(ctx, nftPlatformResult)

	if err != nil {
		err := errors.New("V2 NftService.BuildForbesNFTCustomCategoriesData Failed To save the NFT Categories to FS")
		span.SetStatus(codes.Error, err.Error())
		log.ErrorL(labels, err.Error())
		log.EndTimeL(labels, "V2 NftService.BuildForbesNFTCustomCategoriesData", startTime, err)
		return nil
	}

	log.EndTimeL(labels, "V2 NftService.BuildForbesNFTCustomCategoriesData", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return nil
}

// MapNFTsChains Attempts to Map the Categories to Chains
// Takes a (ctx context.Context, assetsPlatformIds []string, chains []datastruct.NFTChain)
// Returns []datastruct.NFTChain
// Takes:
//   - assetsPlatformIds all new Categories.
//   - chains exists chains in FS
//
// Returns []datastruct.NFTChain after Map all new categories to our exist chains.
func (n *nftService) MapForbesNFTsPlatforms(ctx context.Context, forbesNFTCategories map[string]datastruct.NFTPlatform, customNFT datastruct.CustomNFT) []datastruct.NFTPlatform {
	span, labels := common.GenerateSpan("V2 NftService.BuildNFTCustomCategoriesData", ctx)
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 NFTsCache.BuildNFTCustomCategoriesData"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 NFTsCache.BuildNFTCustomCategoriesData"))
	defer span.End()
	var nfts []datastruct.NFTPlatform

	for _, nftPlatform := range customNFT.NFTIds {
		value, ok := forbesNFTCategories[nftPlatform]
		if ok {
			if !slices.Contains(value.ForbesAssetPlatformId, customNFT.PlatformId) {
				value.ForbesAssetPlatformId = append(value.ForbesAssetPlatformId, customNFT.PlatformId)
				nfts = append(nfts, value)
			}
		}
	}

	log.EndTimeL(labels, "V2 NftService.BuildNFTCustomCategoriesData", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return nfts
}
