package repository

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/go-tools/log"
	"github.com/lib/pq"
	"go.opentelemetry.io/otel/codes"
	otelCodes "go.opentelemetry.io/otel/codes"
	"google.golang.org/api/iterator"
)

type NftQuery interface {
	GetNftCollection(context.Context, string) (*datastruct.NftCollection, error)               //Get the NFT collection data from Postgres.
	GetChainsList(ctx context.Context) ([]datastruct.NFTChain, error)                          // Get NFT Chains
	GetFOrbesNFTCategoriesList(ctx context.Context) (map[string]datastruct.NFTPlatform, error) // Get NFT Chains
	GetNFTPricesFundamentals(ctx0 context.Context) ([]datastruct.NFTPrices, error)             // Get NFT Prices
	GetNFTAssetsPlatformID(ctx context.Context) ([]string, error)                              // Get NFT Assets Platform IDs
	SaveNFTCategories(ctx context.Context, chains []datastruct.NFTChain) error                 // save all new NFT chains
	UpsertNFTCategories(ctx context.Context, nft []datastruct.NFTPlatform) error
}

type nftQuery struct{}

// GetNftCollection Gets all information associated with an NFT collection, to show on the NFT page
// Takes a context and the id of the NFT collection
// Returns (*datastruct.NftCollection, error)
//
// Takes the collection id and then queries postgres for information associated with it
// Returns the a populated NftCollection object with all required data. and no error if successful.
// Returns nil value if the NFT collection is not found
func (w *nftQuery) GetNftCollection(ctx context.Context, slug string) (*datastruct.NftCollection, error) {
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
			rank,
			prev_ranked_slug,
			next_ranked_slug,
			display_symbol,
			image,
 			large_image,
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
			volume_24h_percentage_change_native,
			volume_usd_1d,
			volume_usd_7d,
			volume_usd_30d,
			volume_usd_90d,
			volume_usd_ytd,
			volume_native_1d,
			volume_native_7d,
			volume_native_30d,
			volume_native_90d,
			volume_native_ytd,
			pct_change_volume_usd_1d,
			pct_change_volume_usd_7d,
			pct_change_volume_usd_30d,
			pct_change_volume_usd_90d,
			pct_change_volume_usd_ytd,
			pct_change_volume_native_1d,
			pct_change_volume_native_7d,
			pct_change_volume_native_30d,
			pct_change_volume_native_90d,
			pct_change_volume_native_ytd,
			lowest_floor_price_24h_usd,
			lowest_floor_price_7d_usd,
			lowest_floor_price_30d_usd,
			lowest_floor_price_90d_usd,
			lowest_floor_price_ytd_usd,
			highest_floor_price_24h_usd,
			highest_floor_price_7d_usd,
			highest_floor_price_30d_usd,
			highest_floor_price_90d_usd,
			highest_floor_price_ytd_usd,
			lowest_floor_price_24h_native,
			lowest_floor_price_7d_native,
			lowest_floor_price_30d_native,
			lowest_floor_price_90d_native,
			lowest_floor_price_ytd_native,
			highest_floor_price_24h_native,
			highest_floor_price_7d_native,
			highest_floor_price_30d_native,
			highest_floor_price_90d_native,
			highest_floor_price_ytd_native,
			floor_price_24h_percentage_change_usd,
			floor_price_7d_percentage_change_usd,
			floor_price_30d_percentage_change_usd,
			floor_price_90d_percentage_change_usd,
			floor_price_ytd_percentage_change_usd,
			floor_price_24h_percentage_change_native,
			floor_price_24h_percentage_change_native as floor_price_native_24h_percentage_change,
			floor_price_7d_percentage_change_native,
			floor_price_30d_percentage_change_native,
			floor_price_90d_percentage_change_native,
			floor_price_ytd_percentage_change_native,
			lowest_floor_price_24h_percentage_change_usd,
			lowest_floor_price_7d_percentage_change_usd,
			lowest_floor_price_30d_percentage_change_usd,
			lowest_floor_price_90d_percentage_change_usd,
			lowest_floor_price_ytd_percentage_change_usd,
			highest_floor_price_24h_percentage_change_usd,
			highest_floor_price_7d_percentage_change_usd,
			highest_floor_price_30d_percentage_change_usd,
			highest_floor_price_90d_percentage_change_usd,
			highest_floor_price_ytd_percentage_change_usd,
			lowest_floor_price_24h_percentage_change_native,
			lowest_floor_price_7d_percentage_change_native,
			lowest_floor_price_30d_percentage_change_native,
			lowest_floor_price_90d_percentage_change_native,
			lowest_floor_price_ytd_percentage_change_native,
			highest_floor_price_24h_percentage_change_native,
			highest_floor_price_7d_percentage_change_native,
			highest_floor_price_30d_percentage_change_native,
			highest_floor_price_90d_percentage_change_native,
			highest_floor_price_ytd_percentage_change_native,
			questions,
			next_up
		FROM 
			public.GetNFTCollectionWithRank(
				'` + slug + `'
			)
		`

	queryResult, err := pg.QueryContext(ctx, query)

	if err != nil {
		return &NftResult, err
	}

	for queryResult.Next() {
		nextUpSlices := []datastruct.NextUpSlices{}
		err := queryResult.Scan(&NftResult.ID, &NftResult.ContractAddress, &NftResult.AssetPlatformId, &NftResult.Name, &NftResult.Symbol, &NftResult.Rank, &NftResult.PrevRankedSlug, &NftResult.NextRankedSlug, &NftResult.DisplaySymbol, &NftResult.Image.Small, &NftResult.Image.Large, &NftResult.Description, &NftResult.NativeCurrency, &NftResult.FloorPriceUsd, &NftResult.MarketCapUsd, &NftResult.Volume24hUsd, &NftResult.FloorPriceNative, &NftResult.MarketCapNative, &NftResult.Volume24hNative, &NftResult.FloorPriceInUsd24hPercentageChange, &NftResult.Volume24hPercentageChangeUsd, &NftResult.NumberOfUniqueAddresses, &NftResult.NumberOfUniqueAddresses24hPercentageChange, &NftResult.Slug, &NftResult.TotalSupply, &NftResult.WebsiteUrl, &NftResult.TwitterUrl, &NftResult.DiscordUrl, (*datastruct.ExplorersResult)(&NftResult.Explorers), &NftResult.LastUpdated, &NftResult.AvgSalePrice.OneDay, &NftResult.AvgSalePrice.SevenDay, &NftResult.AvgSalePrice.ThirtyDay, &NftResult.AvgSalePrice.NinetyDay, &NftResult.AvgSalePrice.Ytd, &NftResult.AvgTotalSalesPercentChange.OneDay, &NftResult.AvgTotalSalesPercentChange.SevenDay, &NftResult.AvgTotalSalesPercentChange.ThirtyDay, &NftResult.AvgTotalSalesPercentChange.NinetyDay, &NftResult.AvgTotalSalesPercentChange.Ytd, &NftResult.TotalSales.OneDay, &NftResult.TotalSales.SevenDay, &NftResult.TotalSales.ThirtyDay, &NftResult.TotalSales.NinetyDay, &NftResult.TotalSales.Ytd, &NftResult.AvgSalesPriceChange.OneDay, &NftResult.AvgSalesPriceChange.SevenDay, &NftResult.AvgSalesPriceChange.ThirtyDay, &NftResult.AvgSalesPriceChange.NinetyDay, &NftResult.AvgSalesPriceChange.Ytd, &NftResult.NativeCurrencySymbol, &NftResult.MarketCap24hPercentageChangeUSD, &NftResult.MarketCap24hPercentageChangeNative, &NftResult.Volume24hPercentageChangeNative, &NftResult.VolumeUSD.OneDay, &NftResult.VolumeUSD.SevenDay, &NftResult.VolumeUSD.ThirtyDay, &NftResult.VolumeUSD.NinetyDay, &NftResult.VolumeUSD.Ytd, &NftResult.VolumeNative.OneDay, &NftResult.VolumeNative.SevenDay, &NftResult.VolumeNative.ThirtyDay, &NftResult.VolumeNative.NinetyDay, &NftResult.VolumeNative.Ytd, &NftResult.VolumePercentChangeUSD.OneDay, &NftResult.VolumePercentChangeUSD.SevenDay, &NftResult.VolumePercentChangeUSD.ThirtyDay, &NftResult.VolumePercentChangeUSD.NinetyDay, &NftResult.VolumePercentChangeUSD.Ytd, &NftResult.VolumePercentChangeNative.OneDay, &NftResult.VolumePercentChangeNative.SevenDay, &NftResult.VolumePercentChangeNative.ThirtyDay, &NftResult.VolumePercentChangeNative.NinetyDay, &NftResult.VolumePercentChangeNative.Ytd, &NftResult.LowestFloorPriceUSD.OneDay, &NftResult.LowestFloorPriceUSD.SevenDay, &NftResult.LowestFloorPriceUSD.ThirtyDay, &NftResult.LowestFloorPriceUSD.NinetyDay, &NftResult.LowestFloorPriceUSD.Ytd, &NftResult.HighestFloorPriceUSD.OneDay, &NftResult.HighestFloorPriceUSD.SevenDay, &NftResult.HighestFloorPriceUSD.ThirtyDay, &NftResult.HighestFloorPriceUSD.NinetyDay, &NftResult.HighestFloorPriceUSD.Ytd, &NftResult.LowestFloorPriceNative.OneDay, &NftResult.LowestFloorPriceNative.SevenDay, &NftResult.LowestFloorPriceNative.ThirtyDay, &NftResult.LowestFloorPriceNative.NinetyDay, &NftResult.LowestFloorPriceNative.Ytd, &NftResult.HighestFloorPriceNative.OneDay, &NftResult.HighestFloorPriceNative.SevenDay, &NftResult.HighestFloorPriceNative.ThirtyDay, &NftResult.HighestFloorPriceNative.NinetyDay, &NftResult.HighestFloorPriceNative.Ytd, &NftResult.FloorPricePercentChangeUSD.OneDay, &NftResult.FloorPricePercentChangeUSD.SevenDay, &NftResult.FloorPricePercentChangeUSD.ThirtyDay, &NftResult.FloorPricePercentChangeUSD.NinetyDay, &NftResult.FloorPricePercentChangeUSD.Ytd, &NftResult.FloorPricePercentChangeNative.OneDay, &NftResult.FloorPrice24hPercentageChangeNative, &NftResult.FloorPricePercentChangeNative.SevenDay, &NftResult.FloorPricePercentChangeNative.ThirtyDay, &NftResult.FloorPricePercentChangeNative.NinetyDay, &NftResult.FloorPricePercentChangeNative.Ytd, &NftResult.LowestFloorPricePercentChangeUSD.OneDay, &NftResult.LowestFloorPricePercentChangeUSD.SevenDay, &NftResult.LowestFloorPricePercentChangeUSD.ThirtyDay, &NftResult.LowestFloorPricePercentChangeUSD.NinetyDay, &NftResult.LowestFloorPricePercentChangeUSD.Ytd, &NftResult.HighestFloorPricePercentChangeUSD.OneDay, &NftResult.HighestFloorPricePercentChangeUSD.SevenDay, &NftResult.HighestFloorPricePercentChangeUSD.ThirtyDay, &NftResult.HighestFloorPricePercentChangeUSD.NinetyDay, &NftResult.HighestFloorPricePercentChangeUSD.Ytd, &NftResult.LowestFloorPricePercentChangeNative.OneDay, &NftResult.LowestFloorPricePercentChangeNative.SevenDay, &NftResult.LowestFloorPricePercentChangeNative.ThirtyDay, &NftResult.LowestFloorPricePercentChangeNative.NinetyDay, &NftResult.LowestFloorPricePercentChangeNative.Ytd, &NftResult.HighestFloorPricePercentChangeNative.OneDay, &NftResult.HighestFloorPricePercentChangeNative.SevenDay, &NftResult.HighestFloorPricePercentChangeNative.ThirtyDay, &NftResult.HighestFloorPricePercentChangeNative.NinetyDay, &NftResult.HighestFloorPricePercentChangeNative.Ytd, (*datastruct.NFTQuestionResult)(&NftResult.NFTQuestion), (*datastruct.NextUpResult)(&nextUpSlices))
		if err != nil {
			span.SetStatus(codes.Error, "V2 NftCollection data scan error")
			log.EndTime("Get NftCollection Query", startTime, err)
			return nil, err
		}
		// Removing empty next_up assets. When the nft collection is ranked last, this will keep the next_up array empty instead of sending over empty objects back.
		for _, elem := range nextUpSlices {
			if elem.Slug != "" {
				NftResult.NextUp = append(NftResult.NextUp, elem)
			}
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
		where volume_24h_percentage_change_usd is not null
		And is_active = true`

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
	query := `
			SELECT 
				DISTINCT asset_platform_id
			FROM 
				public.nftdatalatest
			UNION
			SELECT 
				DISTINCT unnest(forbes_asset_platform_id)
			FROM 
				public.nftdatalatest`
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

// GetFOrbesNFTCategoriesList Gets all NFT Platforms Assets ID Data from PG
// Takes a context
// Returns ([]string, error)
//
// Returns the Array of NFTs  Platforms Assets ID data from PG and no error if successful.
func (n *nftQuery) GetFOrbesNFTCategoriesList(ctx context.Context) (map[string]datastruct.NFTPlatform, error) {
	span, labels := common.GenerateSpan("V2 nftQuery.GetFOrbesNFTCategoriesList", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 nftQuery.GetFOrbesNFTCategoriesList"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 nftQuery.GetFOrbesNFTCategoriesList"))
	var forbesNFT = make(map[string]datastruct.NFTPlatform)
	pg := PGConnect()
	query := `
			SELECT 
				id, forbes_asset_platform_id
			FROM 
				public.nftdatalatest_test1`
	queryResult, err := pg.QueryContext(ctx, query)

	if err != nil {
		log.EndTimeL(labels, " V2 nftQuery.GetFOrbesNFTCategoriesList NFT Assets Platform Query Execution", startTime, err)
		return nil, err
	}
	defer queryResult.Close()

	for queryResult.Next() {
		var nft datastruct.NFTPlatform
		err := queryResult.Scan(&nft.ID, pq.Array(&nft.ForbesAssetPlatformId))
		if err != nil {
			log.EndTimeL(labels, "V2 nftQuery.GetFOrbesNFTCategoriesList NFT Assets Platform Query Scan", startTime, err)
			return nil, err
		}
		forbesNFT[nft.ID] = nft
	}
	log.EndTimeL(labels, "V2 nftQuery.GetFOrbesNFTCategoriesList", startTime, nil)
	span.SetStatus(codes.Ok, "V2 nftQuery.GetFOrbesNFTCategoriesList")
	return forbesNFT, nil
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

// UpsertNFTCategories Store new NFT Categories Data to FS
// Takes a context and array of new Chains (Categories) with Id and Name
// Returns error
//
// Returns no error if successful.
func (n *nftQuery) UpsertNFTCategories(ctx context.Context, nfts []datastruct.NFTPlatform) error {
	span, labels := common.GenerateSpan("V2 nftQuery.UpsertNFTCategories", ctx)
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "V2 nftQuery.UpsertNFTCategories"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 nftQuery.UpsertNFTCategories"))

	pg := PGConnect()
	valueString := make([]string, 0, len(nfts))
	valueNFTs := make([]interface{}, 0, len(nfts)*2)
	var i = 0

	tableName := "nftdatalatest_test1"

	totalFields := 2

	for y := 0; y < len(nfts); y++ {
		var nft = nfts[y]

		var valString = fmt.Sprintf("($%d,$%d)", i*totalFields+1, i*totalFields+2)
		valueString = append(valueString, valString)
		valueNFTs = append(valueNFTs, nft.ID)
		valueNFTs = append(valueNFTs, pq.Array(nft.ForbesAssetPlatformId))
		i++
		if len(valueNFTs) >= 65000 || y == len(nfts)-1 {
			upsertStatement := " ON CONFLICT (id) DO UPDATE SET forbes_asset_platform_id = EXCLUDED.forbes_asset_platform_id;"
			insertCharts := fmt.Sprintf("INSERT INTO %s (id, forbes_asset_platform_id) VALUES %s %s", tableName, strings.Join(valueString, ","), upsertStatement)
			latencyTimeStart := time.Now()
			_, inserterError := pg.ExecContext(ctx, insertCharts, valueNFTs...)
			latency := time.Since(latencyTimeStart)

			log.InfoL(labels, fmt.Sprintf("Upsert Fundamentals : time to insert %dms", latency.Milliseconds()))

			if inserterError != nil {
				log.ErrorL(labels, fmt.Sprintf("UpsertFundamentals TimeElapsed: %fs", latency.Seconds()), inserterError)
				log.EndTime("Upsert Fundamentals Latest", startTime, inserterError)
				return inserterError
			}
			valueString = make([]string, 0, len(nfts))
			valueNFTs = make([]interface{}, 0, len(nfts)*2)
			i = 0

		}

	}

	log.EndTimeL(labels, "V2 nftQuery.UpsertNFTCategories: Finished Successfully ", startTime, nil)
	span.SetStatus(codes.Ok, "V2 nftQuery.UpsertNFTCategories")

	return nil
}
