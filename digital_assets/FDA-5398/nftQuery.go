package repository

import (
	"context"
	"fmt"

	"cloud.google.com/go/firestore"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
	otelCodes "go.opentelemetry.io/otel/codes"
	"google.golang.org/api/iterator"
)

type NftQuery interface {
	GetNftCollection(context.Context, string) (*datastruct.NftCollection, error)   //Get the NFT collection data from Postgres.
	GetChainsList(ctx context.Context) ([]datastruct.NFTChain, error)              // Get NFT Chains
	GetNFTPricesFundamentals(ctx0 context.Context) ([]datastruct.NFTPrices, error) // Get NFT Prices
	GetNFTAssetsPlatformID(ctx context.Context) ([]string, error)                  // Get NFT Assets Platform IDs
	SaveNFTCategories(ctx context.Context, chains []datastruct.NFTChain) error     // save all new NFT chains
}

type nftQuery struct{}

// GetNftCollection Gets all information associated with an NFT collection, to show on the NFT page
// Takes a context and the id of the NFT collection
// Returns (*datastruct.NftCollection, error)
//
// Takes the collection id and then queries postgres for information associated with it
// Returns the a populated NftCollection object with all required data. and no error if successful.
// Returns nil value if the NFT collection is not found
func (w *nftQuery) GetNftCollection(ctx context.Context, collectionId string) (*datastruct.NftCollection, error) {
	span, labels := common.GenerateSpan("V2 nftQuery.NftCollection", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 nftQuery.NftCollection"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 nftQuery.NftCollection"))

	var NftResult datastruct.NftCollection

	pg := PGConnect()

	query := `
		SELECT 
			id,
			contract_address,
			asset_platform_id,
			name,
			symbol,
			display_symbol,
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
			volume_24h_percentage_change_usd,
			number_of_unique_addresses,
			number_of_unique_addresses_24h_percentage_change,
			slug,
			total_supply,
			website_url, 
			twitter_url, 
			discord_url,
			explorers,
			last_updated,
			avg_sale_price_1d,
			avg_sale_price_7d,
			avg_sale_price_30d,
			avg_sale_price_90d,
			avg_sale_price_ytd,
			avg_total_sales_pct_change_1d,
			avg_total_sales_pct_change_7d,
			avg_total_sales_pct_change_30d,
			avg_total_sales_pct_change_90d,
			avg_total_sales_pct_change_ytd,
			total_sales_1d,
			total_sales_7d,
			total_sales_30d,
			total_sales_90d,
			total_sales_ytd,
			avg_sales_price_change_1d,
			avg_sales_price_change_7d,
			avg_sales_price_change_30d,
			avg_sales_price_change_90d,
			avg_sales_price_change_ytd,
			native_currency_symbol,
			market_cap_24h_percentage_change_usd,
			market_cap_24h_percentage_change_native,
			volume_24h_percentage_change_native
		FROM 
			public.getnftcollection(
				'` + collectionId + `'
			)
		`

	queryResult, err := pg.QueryContext(ctx, query)

	if err != nil {
		return &NftResult, err
	}

	for queryResult.Next() {
		err := queryResult.Scan(&NftResult.ID, &NftResult.ContractAddress, &NftResult.AssetPlatformId, &NftResult.Name, &NftResult.Symbol, &NftResult.DisplaySymbol, &NftResult.Image, &NftResult.Description, &NftResult.NativeCurrency, &NftResult.FloorPriceUsd, &NftResult.MarketCapUsd, &NftResult.Volume24hUsd, &NftResult.FloorPriceNative, &NftResult.MarketCapNative, &NftResult.Volume24hNative, &NftResult.FloorPriceInUsd24hPercentageChange, &NftResult.Volume24hPercentageChangeUsd, &NftResult.NumberOfUniqueAddresses, &NftResult.NumberOfUniqueAddresses24hPercentageChange, &NftResult.Slug, &NftResult.TotalSupply, &NftResult.WebsiteUrl, &NftResult.TwitterUrl, &NftResult.DiscordUrl, (*datastruct.ExplorersResult)(&NftResult.Explorers), &NftResult.LastUpdated, &NftResult.AvgSalePrice.OneDay, &NftResult.AvgSalePrice.SevenDay, &NftResult.AvgSalePrice.ThirtyDay, &NftResult.AvgSalePrice.NinetyDay, &NftResult.AvgSalePrice.Ytd, &NftResult.AvgTotalSalesPercentChange.OneDay, &NftResult.AvgTotalSalesPercentChange.SevenDay, &NftResult.AvgTotalSalesPercentChange.ThirtyDay, &NftResult.AvgTotalSalesPercentChange.NinetyDay, &NftResult.AvgTotalSalesPercentChange.Ytd, &NftResult.TotalSales.OneDay, &NftResult.TotalSales.SevenDay, &NftResult.TotalSales.ThirtyDay, &NftResult.TotalSales.NinetyDay, &NftResult.TotalSales.Ytd, &NftResult.AvgSalesPriceChange.OneDay, &NftResult.AvgSalesPriceChange.SevenDay, &NftResult.AvgSalesPriceChange.ThirtyDay, &NftResult.AvgSalesPriceChange.NinetyDay, &NftResult.AvgSalesPriceChange.Ytd, &NftResult.NativeCurrencySymbol, &NftResult.MarketCap24hPercentageChangeUSD, &NftResult.MarketCap24hPercentageChangeNative, &NftResult.Volume24hPercentageChangeNative)
		if err != nil {
			span.SetStatus(codes.Error, "V2 NftCollection data scan error")
			log.EndTime("Get NftCollection Query", startTime, err)
			return nil, err
		}
	}

	// If the NFT collection is not found, return nil object.
	if NftResult.ID == "" {
		log.EndTimeL(labels, "V2 nftQuery.NftCollection Not found NFT collection", startTime, nil)
		span.SetStatus(codes.Ok, "V2 nftQuery.NftCollection Not found NFT collection")
		return nil, nil
	}

	log.EndTimeL(labels, "V2 nftQuery.NftCollection", startTime, nil)
	span.SetStatus(codes.Ok, "V2 nftQuery.NftCollection")

	return &NftResult, nil
}

// GetChainsList Gets all NFT Chains Data from FS
// Takes a context
// Returns ([]datastruct.NFTChain, error)
//
// Returns the NFTs Chains and no error if successful.
func (n *nftQuery) GetChainsList(ctx context.Context) ([]datastruct.NFTChain, error) {
	fs := fsUtils.GetFirestoreClient()
	span, labels := common.GenerateSpan("V2 nftQuery.GetChainsList", ctx)
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "V2 nftQuery.GetChainsList"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 nftQuery.GetChainsList"))

	var nftChains []datastruct.NFTChain

	// Get the NFT chains from Firestore
	iter := fs.Collection(datastruct.NFTChainCollectionName).Documents(ctx)
	span.AddEvent(" nftQuery.GetChainsList: Start Getting NFT Chains Data from FS")

	for {
		var nftChain datastruct.NFTChain
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			log.ErrorL(labels, " nftQuery.GetChainsList: Error Getting NFT Chains Data from FS: %s", err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}

		err = doc.DataTo(&nftChain)
		if err != nil {
			log.ErrorL(labels, " nftQuery.GetChainsList: Error Getting NFT Chains Data from FS: %s", err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}
		nftChains = append(nftChains, nftChain)
	}

	log.EndTimeL(labels, "V2 nftQuery.GetChainsList: Finished Successfully ", startTime, nil)
	span.SetStatus(codes.Ok, "V2 nftQuery.GetChainsList")

	return nftChains, nil
}

// GetNFTPricesFundamentals Gets all NFT Data from PG
// Takes a context
// Returns ([]datastruct.NFTPrices, error)
//
// Returns the NFTs Prices data from PG and no error if successful.
func (n *nftQuery) GetNFTPricesFundamentals(ctx context.Context) ([]datastruct.NFTPrices, error) {

	span, labels := common.GenerateSpan("V2 nftQuery.GetNFTPricesFundamentals", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 nftQuery.GetNFTPricesFundamentals"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 nftQuery.GetNFTPricesFundamentals"))
	var nfts []datastruct.NFTPrices
	pg := PGConnect()
	query := `SELECT 
			id,
			contract_address,
			asset_platform_id,
			name,
			symbol,
			symbol as display_symbol,
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
			volume_24h_percentage_change_usd,
			number_of_unique_addresses,
			number_of_unique_addresses_24h_percentage_change,
			slug,
			total_supply,
			last_updated,
			count(id) OVER() AS full_count
		FROM 
			public.nftdatalatest
		where volume_24h_percentage_change_usd is not null`
	queryResult, err := pg.QueryContext(ctx, query)

	if err != nil {
		log.EndTimeL(labels, " V2 nftQuery.GetNFTPricesFundamentals NFT Prices Query Execution", startTime, err)
		return nil, err
	}
	defer queryResult.Close()

	for queryResult.Next() {
		var nft datastruct.NFTPrices
		err := queryResult.Scan(&nft.ID, &nft.ContractAddress, &nft.AssetPlatformId, &nft.Name, &nft.Symbol, &nft.DisplaySymbol, &nft.Image, &nft.Description, &nft.NativeCurrency, &nft.FloorPriceUsd, &nft.MarketCapUsd, &nft.Volume24hUsd, &nft.FloorPriceNative, &nft.MarketCapNative, &nft.Volume24hNative, &nft.FloorPriceInUsd24hPercentageChange, &nft.Volume24hPercentageChangeUsd, &nft.NumberOfUniqueAddresses, &nft.NumberOfUniqueAddresses24hPercentageChange, &nft.Slug, &nft.TotalSupply, &nft.LastUpdated, &nft.FullCount)
		if err != nil {
			log.EndTimeL(labels, "V2 nftQuery.GetNFTPricesFundamentals NFT Prices Query Scan", startTime, err)
			return nil, err
		}
		nfts = append(nfts, nft)
	}
	log.EndTimeL(labels, "V2 nftQuery.GetNFTPricesFundamentals", startTime, nil)
	span.SetStatus(codes.Ok, "V2 nftQuery.GetNFTPricesFundamentals")
	return nfts, nil
}

// GetNFTAssetsPlatformID Gets all NFT Platforms Assets ID Data from PG
// Takes a context
// Returns ([]string, error)
//
// Returns the Array of NFTs  Platforms Assets ID data from PG and no error if successful.
func (n *nftQuery) GetNFTAssetsPlatformID(ctx context.Context) ([]string, error) {
	span, labels := common.GenerateSpan("V2 nftQuery.GetNFTAssetsPlatformID", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 nftQuery.GetNFTAssetsPlatformID"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 nftQuery.GetNFTAssetsPlatformID"))
	var platformAssetsId []string
	pg := PGConnect()
	query := `SELECT distinct  
					asset_platform_id
				FROM 
					public.nftdatalatest
				group by 
					asset_platform_id;`
	queryResult, err := pg.QueryContext(ctx, query)

	if err != nil {
		log.EndTimeL(labels, " V2 nftQuery.GetNFTAssetsPlatformID NFT Assets Platform Query Execution", startTime, err)
		return nil, err
	}
	defer queryResult.Close()

	for queryResult.Next() {
		var nft datastruct.NFTPrices
		err := queryResult.Scan(&nft.AssetPlatformId)
		if err != nil {
			log.EndTimeL(labels, "V2 nftQuery.GetNFTAssetsPlatformID NFT Assets Platform Query Scan", startTime, err)
			return nil, err
		}
		platformAssetsId = append(platformAssetsId, nft.AssetPlatformId)
	}
	log.EndTimeL(labels, "V2 nftQuery.GetNFTAssetsPlatformID", startTime, nil)
	span.SetStatus(codes.Ok, "V2 nftQuery.GetNFTAssetsPlatformID")
	return platformAssetsId, nil
}

// SaveNFTCategories Store new NFT Categories Data to FS
// Takes a context and array of new Chains (Categories) with Id and Name
// Returns error
//
// Returns no error if successful.
func (n *nftQuery) SaveNFTCategories(ctx context.Context, chains []datastruct.NFTChain) error {
	fs := fsUtils.GetFirestoreClient()
	span, labels := common.GenerateSpan("V2 nftQuery.SaveNFTCategories", ctx)
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "V2 nftQuery.SaveNFTCategories"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 nftQuery.SaveNFTCategories"))

	for _, chain := range chains {
		fs.Collection(datastruct.NFTChainCollectionName).Doc(chain.ID).Set(ctx, map[string]interface{}{
			"id":   chain.ID,
			"name": chain.Name,
		}, firestore.MergeAll)
	}

	log.EndTimeL(labels, "V2 nftQuery.SaveNFTCategories: Finished Successfully ", startTime, nil)
	span.SetStatus(codes.Ok, "V2 nftQuery.SaveNFTCategories")

	return nil
}
