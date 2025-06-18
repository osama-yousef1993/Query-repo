package repository

import (
	"context"
	"fmt"
	"strings"

	"github.com/Forbes-Media/fda-common/crypto-filter-protocol/common"
	"github.com/Forbes-Media/fda-common/crypto-filter-protocol/dto"
	"github.com/Forbes-Media/go-tools/log"
)

type AssetsQuery interface {
	// GetForbesAssets retrieves a map of Forbes assets from the database.
	GetForbesAssets(mapBy string) (assets *map[string]dto.ForbesAsset, err error)
}

type assetsQuery struct{}

// GetForbesAssets retrieves a map of Forbes assets from the database.
// It performs the following steps:
// 1. Queries the database for the Forbes assets.
// 2. Maps the retrieved assets to the provided mapBy field.
//
// Parameters:
// - mapBy: A string containing the field to map the assets by.
// Returns:
// - assets: A map of ForbesAsset containing the retrieved assets.
// - error: An error if any issues occur during the process, otherwise nil.
func (r *assetsQuery) GetForbesAssets(mapBy string) (assets *map[string]dto.ForbesAsset, err error) {
	span, labels := common.GenerateSpan("crypto-filter-protocol.GetForbesAssets", context.Background())
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "crypto-filter-protocol.GetForbesAssets"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "crypto-filter-protocol.GetForbesAssets"))
	assets = &map[string]dto.ForbesAsset{}
	c, err := pg.GetClient()

	if err != nil {
		log.Error("Error closing client: %v", err)
		return
	}

	query := fmt.Sprintf(`
		SELECT
			id,
			name,
			symbol,
			coingecko_id,
			coinpaprika_id,
			contract_address,
			last_updated
		FROM %s`, "forbes_assets")

	queryResult, err := c.Query(query)
	if err != nil {
		log.Error("Error querying forbes_assets: %v", err)
		return
	}

	for queryResult.Next() {
		var asset dto.ForbesAsset
		err = queryResult.Scan(
			&asset.ID, &asset.Name, &asset.Symbol, &asset.CoingeckoID,
			&asset.CoinpaprikaID, &asset.ContractAddress, &asset.LastUpdated)
		if err != nil {
			log.Error("Error scanning forbes_assets: %v", err)
			return
		}
		switch mapBy {
		case "id":
			(*assets)[strings.ToLower(*asset.ID)] = asset
		case "name":
			(*assets)[strings.ToLower(*asset.Name)] = asset
		case "symbol":
			(*assets)[strings.ToLower(*asset.Symbol)] = asset
		case "CoinpaprikaID":
			(*assets)[strings.ToLower(*asset.CoinpaprikaID)] = asset
		case "CoingeckoID":
			(*assets)[strings.ToLower(*asset.CoingeckoID)] = asset
		case "ContractAddress":
			(*assets)[*asset.ContractAddress] = asset
		}
	}
	log.EndTimeL(labels, fmt.Sprintf("Finished %s", "crypto-filter-protocol.GetForbesAssets"), startTime, nil)
	return
}
