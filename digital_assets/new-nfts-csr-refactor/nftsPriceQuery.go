package repository

import (
	"context"
	"fmt"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
	otelCodes "go.opentelemetry.io/otel/codes"
	"google.golang.org/api/iterator"
)

type NFTsQuery interface {
	GetChainsList(ctx context.Context) ([]datastruct.NFTChain, error)
	GetNFTPricesFundamentals(ctx0 context.Context) ([]datastruct.NFTPrices, error)
}

type nftsQuery struct{}

func (n *nftsQuery) GetChainsList(ctx context.Context) ([]datastruct.NFTChain, error) {
	fs := fsUtils.GetFirestoreClient()
	span, labels := common.GenerateSpan("V2 NFTsQuery.GetChainsList", ctx)
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "V2 NFTsQuery.GetChainsList"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 NFTsQuery.GetChainsList"))

	var nftChains []datastruct.NFTChain

	// Get the NFT chains from Firestore
	iter := fs.Collection(datastruct.NFTsCollectionName).Documents(ctx)
	span.AddEvent(" NFTsQuery.GetChainsList: Start Getting NFT Chains Data from FS")

	for {
		var nftChain datastruct.NFTChain
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			log.Error(" NFTsQuery.GetChainsList: Error Getting NFT Chains Data from FS: %s", err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}

		err = doc.DataTo(&nftChain)
		if err != nil {
			log.Error(" NFTsQuery.GetChainsList: Error Getting NFT Chains Data from FS: %s", err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}
		nftChains = append(nftChains, nftChain)
	}

	log.EndTimeL(labels, "V2 NFTsQuery.GetChainsList: Finished Successfully ", startTime, nil)
	span.SetStatus(codes.Ok, "V2 NFTsQuery.GetChainsList")

	return nftChains, nil
}

// It will retrieve all NFTs data from postgres
func (n *nftsQuery) GetNFTPricesFundamentals(ctx0 context.Context) ([]datastruct.NFTPrices, error) {

	ctx, span := tracer.Start(ctx0, "PGGetNFTPrices")
	defer span.End()

	startTime := log.StartTime("NFT Prices Query")
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
		log.EndTime("NFT Prices Query", startTime, err)
		return nil, err
	}
	defer queryResult.Close()

	for queryResult.Next() {
		var nft datastruct.NFTPrices
		err := queryResult.Scan(&nft.ID, &nft.ContractAddress, &nft.AssetPlatformId, &nft.Name, &nft.Symbol, &nft.DisplaySymbol, &nft.Image, &nft.Description, &nft.NativeCurrency, &nft.FloorPriceUsd, &nft.MarketCapUsd, &nft.Volume24hUsd, &nft.FloorPriceNative, &nft.MarketCapNative, &nft.Volume24hNative, &nft.FloorPriceInUsd24hPercentageChange, &nft.Volume24hPercentageChangeUsd, &nft.NumberOfUniqueAddresses, &nft.NumberOfUniqueAddresses24hPercentageChange, &nft.Slug, &nft.TotalSupply, &nft.LastUpdated, &nft.FullCount)
		if err != nil {
			log.EndTime("NFT Prices Query", startTime, err)
			return nil, err
		}
		nfts = append(nfts, nft)
	}
	return nfts, nil
}
