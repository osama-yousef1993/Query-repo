package dto

import (
	"sync"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
)

type DictionaryDataSet string

const (
	Ft              DictionaryDataSet = "ft"         // Fungible Token dictionary Kind - Search by name, symbol of the fungible token
	Nft             DictionaryDataSet = "nft"        // Non-Fungible Token dictionary Kind - Search by name, symbol of the NFT
	Category        DictionaryDataSet = "category"   // Category dictionary Kind - Search directly by category
	FTCategory      DictionaryDataSet = "ftCategory" // Fungible Token Category dictionary Kind - Search  Fungible Token directly by category
	NFTChains       DictionaryDataSet = "nftChains"  // Non-Fungible Token Category dictionary Kind - Search  Non-Fungible Token directly by chains
	CategoriesTable DictionaryDataSet = "categories" // Powers the data shown on the categories table (Uses the DategoriesFundamentals DataSet).
)

type SearchType int

const (
	Fuzzy SearchType = 0 //If the dictionary requires fuzzy search
	Exact SearchType = 1 //If the dictionary requires exact match
)

// A cached object that can be accessed concurrently  Used to power search bar feature

type SearchTable interface {
	datastruct.TradedAssetsTable | datastruct.NFTPrices | datastruct.CategoryFundamental
}

type Dictionary interface {
}

type dictionary struct {
	SearchType              SearchType                                `json:"seach_type"` //For category, the type would be exact match. Otherwise we're keeping it Fuzzy.
	Assets                  map[string][]datastruct.TradedAssetsTable `json:"assets"`     //Key = Search Term. Value = Array of Assets related to the search term.
	Words                   []string                                  `json:"words"`      //Array of all search terms in the dictionary
	Category                DictionaryDataSet                         `json:"category"`   //Category of the dictionary
	Nfts                    map[string][]datastruct.NFTPrices         `json:"nfts"`       //NFTs of the dictionary
	Lock                    *sync.Mutex
	defaultFuzzySearchLimit int32
}

type SearchRequest struct {
	Paginate   Paginate          `json:"-"`          //information used on how to sort search reults
	Query      string            `json:"query"`      //used when searching against terms in a cache ex(look for all tokens that contain "bi")
	PageNumber int               `json:"pageNumber"` // the page of data we are looking for
	CategoryID string            `json:"categoryID"` // Used to search for objects that belong to a certain category (can be used in fungible tokens search or categories search)
	Category   DictionaryDataSet `json:"category"`   //Used to search for items belonging to a certain dataset ex:(fungible tokens,NFTS,category fundamentals)
}

type Paginate struct {
	SortBy     string `json:"sort_by"`     // sort by field
	Direction  string `json:"direction"`   // sort direction : asc or desc
	PageNum    int    `json:"page_num"`    // page number. Default 1
	Limit      int    `json:"limit"`       // limit per page.
	CategoryID string `json:"category_id"` // category id for featured categories
	ChainID    string `json:"chain_id"`    // chain id for featured categories
}

type NFTPricesResp struct {
	NFT                   []datastruct.NFTPrices `json:"nft"`   // Array of NFTs result
	Total                 int                    `json:"total"` // The NFTs total exist in response that return from Postgres.
	HasTemporaryDataDelay bool                   `json:"hasTemporaryDataDelay"`
	Source                string                 `json:"source"` // The source that provides NFTs data.
}

type SearchResponse struct {
	Assets                *[]datastruct.TradedAssetsTable   `json:"assets,omitempty"`
	Categories            *[]datastruct.CategoryFundamental `json:"categories,omitempty"`
	NFT                   *[]datastruct.NFTPrices           `json:"nft"` // Array of NFTs result
	Total                 int                               `json:"total"`
	HasTemporaryDataDelay bool                              `json:"hasTemporaryDataDelay"`
	Source                string                            `json:"source"`
}
