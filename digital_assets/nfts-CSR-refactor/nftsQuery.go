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
}

type nftsQuery struct {}

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
