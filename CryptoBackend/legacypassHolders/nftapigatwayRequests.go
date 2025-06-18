package nftapigateway

// Request used by the update v2 function
type NFTAPIGateway_UpdateTokenRequest struct {
	Profiles        []Profiles `json:"profiles"`        // a list of NFT Profiles
	ContractAddress string     `json:"contractAddress"` // The contract address that the profiles belong to
	Chain           string     `json:"chain"`           // the chain the contract is deployed to
}

// attributes of an NFT
type Attributes struct {
	TraitType string `json:"trait_type,omitempty"`
	Value     string `json:"value,omitempty"`
}

// A profile of the an NFT.
type Profiles struct {
	TokenID             *string       `json:"tokenID"`
	TokenName           *string       `json:"tokenName,omitempty"`
	PersonNaturalID     *string       `json:"personNaturalID,omitempty"`
	ListNaturalID       *string       `json:"listNaturalID,omitempty"`
	Attributes          *[]Attributes `json:"attributes,omitempty"`
	ExplorerURI         *string       `json:"explorerUri,omitempty"`
	OpenseaURI          *string       `json:"openseaUri,omitempty"`
	TokenDescription    *string       `json:"tokenDescription,omitempty"`
	TokenImage          *string       `json:"tokenImage,omitempty"`
	MetadataURI         *string       `json:"metadataUri,omitempty"`
	Category            *string       `json:"category,omitempty"`
	ParentListURI       *string       `json:"parentListUri,omitempty"`
	ListURI             *string       `json:"listUri,omitempty"`
	AltListURI          *string       `json:"altListUri,omitempty"`
	ContractAddress     *string       `json:"contractAddress,omitempty"`
	Claimed             *bool         `json:"claimed,omitempty"`
	ClaimedDate         *string       `json:"claimDate,omitempty"`
	Voucher             *[]Voucher    `json:"voucher,omitempty"`
	TokenURI            *string       `json:"tokenURI,omitempty"`
	OwnerWallet         *string       `json:"ownerWallet,omitempty"`
	Chain               *string       `json:"chain,omitempty"`
	MagicEdenURI        *string       `json:"magicEdenUri,omitempty"`
	TokenAnimationImage *string       `json:"tokenAnimationImage,omitempty"`
}

// Voucher is an object that contains information about products
// that the user can claim
type Voucher struct {
	MonthsUntilExpiration int    `json:"monthsUntilExpiration,omitempty"`
	GrantID               string `json:"grantId,omitempty"`
}

// This is a request object used to claim NFT data from the
// NFT API Gateway Getcollection info V1 request
type NFTAPIGateway_V1GetCollectionInfoRequest struct {
	Result             int                `json:"result,omitempty"`
	CollectionEditInfo CollectionEditInfo `json:"collectionEditInfo,omitempty"`
	NftInfo            []NftInfo          `json:"nftInfo,omitempty"`
}

// Information about when the collection was last updated
type CollectionEditInfo struct {
	CollectionName     string `json:"collectionName,omitempty"`
	LastUpdatedPrd     string `json:"lastUpdatedPrd,omitempty"`
	LastUpdatedDev     string `json:"lastUpdatedDev,omitempty"`
	LastUpdateEventPrd int    `json:"lastUpdateEventPrd,omitempty"`
	LastUpdateEventDev int    `json:"lastUpdateEventDev,omitempty"`
}

// NFT Info is returned in the getcollection v1 response.
type NftInfo struct {
	Image             string     `json:"image,omitempty"`
	TokenID           string     `json:"tokenID,omitempty"`
	BlockExpl         string     `json:"blockExpl,omitempty"`
	FtxID             string     `json:"ftxID,omitempty"`
	CurrencySymbol    string     `json:"currencySymbol,omitempty"`
	ContractAddress   string     `json:"contractAddress,omitempty"`
	Attributes        Attributes `json:"attributes,omitempty"`
	OwnerName         string     `json:"ownerName,omitempty"`
	TransactionSource string     `json:"transactionSource,omitempty"`
	BlockExpOwner     string     `json:"blockExpOwner,omitempty"`
	OwnerWallet       string     `json:"ownerWallet,omitempty"`
	LastTransferDate  string     `json:"lastTransferDate,omitempty"`
	LastSoldDate      string     `json:"lastSoldDate,omitempty"`
	SalePrice         string     `json:"salePrice,omitempty"`
	UsdSalePrice      string     `json:"usdSalePrice,omitempty"`
	TokenURI          string     `json:"tokenURI,omitempty"`
	ID                string     `json:"id,omitempty"`
}

type NFTProfilesResult struct {
	Result       int           `json:"result"`
	ProfileCount int           `json:"profileCount"`
	Profiles     []NFTProfiles `json:"profiles"`
	StatusCode   int           `json:"statusCode"`
}

type NFTProfiles struct {
	ContractAddress     string    `json:"contractAddress"`
	ExplorerUri         string    `json:"explorerUri"`
	TokenImage          string    `json:"tokenImage"`
	TokenName           string    `json:"tokenName"`
	TokenID             string    `json:"tokenID"`
	Claimed             bool      `json:"claimed"`
	Voucher             []Voucher `json:"voucher"`
	TokenURI            string    `json:"tokenURI"`
	OwnerWallet         string    `json:"ownerWallet"`
	Chain               string    `json:"chain"`
	TokenAnimationImage string    `json:"tokenAnimationImage"`
}

type NFTProfilesContract struct {
	Contracts string `json:"contracts"`
}

type QueryParams struct {
	Query string `url:"contracts"` // filters the response to our need data ex: we need the data for our channel_115
}
