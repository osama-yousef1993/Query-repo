package datastruct

import (
	"fmt"
	"os"
)

var NFTsCollectionName = fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "nft_chains")

type NFTChain struct {
	ID   string `json:"id" firestore:"id"`     // Id of chain and it will present the assets platform id from the NFT endpoint. We will use it to filter NFTs by chains.
	Name string `json:"name" firestore:"name"` // Name for Chain, it will be used to display in the NFT prices Page.
}

type NFTPricesResp struct {
	NFT                   []NFTPrices `json:"nft"`   // Array of NFTs result
	Total                 int         `json:"total"` // The NFTs total exist in response that return from Postgres.
	HasTemporaryDataDelay bool        `json:"hasTemporaryDataDelay"`
	Source                string      `json:"source"` // The source that provides NFTs data.
}
