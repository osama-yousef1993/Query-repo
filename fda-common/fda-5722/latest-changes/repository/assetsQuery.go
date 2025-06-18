package repository

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Forbes-Media/fda-common/cloudUtils"
	"github.com/Forbes-Media/fda-common/crypto-filter-protocol/common"
	"github.com/Forbes-Media/fda-common/crypto-filter-protocol/dto"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
)

type AssetsQuery interface {
	// GetForbesAssets retrieves a map of Forbes assets from the database.
	GetForbesAssets(mapBy string) (assets *map[string]dto.ForbesAsset, err error)
	// InsertMarketData inserts market data into BigQuery.
	InsertMarketData(ctx context.Context, marketData *[]dto.BQMarketData) error
	// GetFundamentalsDistinct retrieves a list of distinct fundamentals from the database.
	GetFundamentalsDistinct(ctx context.Context) (exchanges *[]dto.ForbesAsset, err error)
	// UpsertForbesAssets upserts a list of Forbes assets into the database.
	UpsertForbesAssets(ctx context.Context, assets *[]dto.ForbesAsset) (err error)
	// GetAssetsDataFS retrieves a map of asset data from Firestore.
	// GetAssetsDataFS(ctx context.Context) (map[string]dto.AssetData, error)
	//
	InsertToRemediationCollection(ctx context.Context, assets map[string]dto.MarketData) (err error)
}

type assetsQuery struct {
	data_namespace string
	rowy_prefix    string
	pg             cloudUtils.PostgresUtils
	fs             cloudUtils.FirestoreUtils
	bq             cloudUtils.BigqueryUtils
}

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
func (a *assetsQuery) GetForbesAssets(mapBy string) (assets *map[string]dto.ForbesAsset, err error) {
	span, labels := common.GenerateSpan("crypto-filter-protocol.GetForbesAssets", context.Background())
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "crypto-filter-protocol.GetForbesAssets"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "crypto-filter-protocol.GetForbesAssets"))
	assets = &map[string]dto.ForbesAsset{}
	c, err := a.pg.GetClient()

	if err != nil {
		log.Error("Error closing client: %v", err)
		return
	}

	query := fmt.Sprintf(`
		SELECT
			id,
			name,
			symbol,
			COALESCE(coingecko_id, ''),
			COALESCE(coinpaprika_id, ''),
			COALESCE(contract_address, ''),
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
			&asset.ForbesID, &asset.Name, &asset.Symbol, &asset.CoingeckoID,
			&asset.CoinpaprikaID, &asset.ContractAddress, &asset.LastUpdated)
		if err != nil {
			log.Error("Error scanning forbes_assets: %v", err)
			return
		}
		switch mapBy {
		case "id":
			(*assets)[strings.ToLower(*asset.ForbesID)] = asset
		case "name":
			(*assets)[strings.ToLower(*asset.Name)] = asset
		case "symbol":
			(*assets)[strings.ToLower(*asset.Symbol)] = asset
		case "coinpaprika_id":
			(*assets)[strings.ToLower(*asset.CoinpaprikaID)] = asset
		case "coingecko_id":
			(*assets)[strings.ToLower(*asset.CoingeckoID)] = asset
		case "contract_address":
			(*assets)[*asset.ContractAddress] = asset
		}
	}
	log.EndTimeL(labels, fmt.Sprintf("Finished %s", "crypto-filter-protocol.GetForbesAssets"), startTime, nil)
	return
}

func (a *assetsQuery) InsertMarketData(ctx context.Context, marketData *[]dto.BQMarketData) error {
	span, labels := common.GenerateSpan("crypto-filter-protocol.InsertMarketData", ctx)
	defer span.End()
	startTime := log.StartTime("crypto-filter-protocol.InsertMarketData")
	currenciesTable := common.GetTableName("Digital_Asset_MarketData", a.data_namespace)

	c, err := a.bq.GetBigQueryClient()
	if err != nil {
		log.Error("Error getting BigQuery client: %v", err)
		return err

	}

	bqInserter := c.Dataset("digital_assets").Table(currenciesTable).Inserter()
	bqInserter.IgnoreUnknownValues = true

	inserterErr := bqInserter.Put(ctx, *marketData)
	var retryError error
	if inserterErr != nil {
		if strings.Contains(inserterErr.Error(), "413") {
			log.Info("413 Error. Breaking up tickers and retrying insert")
			l := len(*marketData)
			var ticks []dto.BQMarketData
			ticks = append(ticks, *marketData...)
			for y := (l / 3); y < l; y += (l / 3) {
				t := ticks[y-(l/3) : y]
				er := a.InsertMarketData(ctx, &t)
				if er != nil {
					retryError = er
				}
			}
			//If we couldnt recover return the error
			log.EndTimeL(labels, "crypto-filter-protocol.InsertMarketData: Error Inserting Markets Data to BigQuery : %s", startTime, inserterErr)
			return retryError
		}
		log.EndTimeL(labels, "crypto-filter-protocol.InsertMarketData: Error Inserting Markets Data to BigQuery : %s", startTime, inserterErr)
		return inserterErr
	}
	log.EndTimeL(labels, "crypto-filter-protocol.InsertMarketData: Successfully finished Inserting Markets Data at time : %s", startTime, nil)
	return nil
}

func (a *assetsQuery) GetFundamentalsDistinct(ctx context.Context) (assets *[]dto.ForbesAsset, err error) {
	span, labels := common.GenerateSpan("crypto-filter-protocol.GetFundamentalsDistinct", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "crypto-filter-protocol.GetFundamentalsDistinct"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "crypto-filter-protocol.GetFundamentalsDistinct"))
	assets = &[]dto.ForbesAsset{}
	c, err := a.pg.GetClient()

	if err != nil {
		log.ErrorL(labels, "Error getting postgres client: %v", err)
		span.SetStatus(codes.Error, "Error getting postgres client")
		return
	}

	query := fmt.Sprintf(`
		SELECT ef.slug,ef.symbol, ef.name
		FROM fundamentalslatest ef
		INNER JOIN (
			SELECT symbol, MAX(last_updated) as max_last_updated
			FROM %s
			GROUP BY symbol
		) ef1
		ON ef.symbol = ef1.symbol AND ef.last_updated = ef1.max_last_updated
		where slug != ''
		and status = 'active'
		ORDER BY ef.name`, "fundamentalslatest")

	queryResult, err := c.Query(query)
	if err != nil {
		log.ErrorL(labels, "Error getting postgres client: %v", err)
		span.SetStatus(codes.Error, "Error getting postgres client")
		return
	}

	for queryResult.Next() {
		var asset dto.ForbesAsset
		err = queryResult.Scan(
			&asset.ForbesID, &asset.CoingeckoID, &asset.Name)
		if err != nil {
			log.ErrorL(labels, "Error scanning forbes_assets: %v", err)
			span.SetStatus(codes.Error, "error scanning forbes_assets")
			return
		}
		*assets = append(*assets, asset)
	}

	log.EndTimeL(labels, "crypto-filter-protocol.GetFundamentalsDistinct", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return

}

func (a *assetsQuery) UpsertForbesAssets(ctx context.Context, assets *[]dto.ForbesAsset) (err error) {
	span, labels := common.GenerateSpan("crypto-filter-protocol.UpsertForbesAssets", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "crypto-filter-protocol.UpsertForbesAssets"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "crypto-filter-protocol.UpsertForbesAssets"))

	c, err := a.pg.GetClient()

	if err != nil {
		log.ErrorL(labels, "Error getting postgres client: %v", err)
		span.SetStatus(codes.Error, "Error getting postgres client")
		return
	}
	assetList := *assets
	valueString := make([]string, 0, len(assetList))
	valueArgs := make([]interface{}, 0, len(assetList)*5)
	colCount := 5
	tableName := "forbes_assets"
	var i = 0
	for y := 0; y < len(assetList); y++ {
		e := assetList[y]

		var valString = fmt.Sprintf("($%d, $%d, $%d, $%d, $%d)", i*colCount+1, i*colCount+2, i*colCount+3, i*colCount+4, i*colCount+5)
		valueString = append(valueString, valString)

		valueArgs = append(valueArgs, e.ForbesID)
		valueArgs = append(valueArgs, e.Name)
		valueArgs = append(valueArgs, e.CoingeckoID)
		valueArgs = append(valueArgs, e.CoinpaprikaID)
		valueArgs = append(valueArgs, time.Now().UTC())
		i++

		if len(valueString) >= 65000 || y == len(assetList)-1 {
			insertStatement := fmt.Sprintf("INSERT INTO %s (forbes_id, name, coingecko_id, coinpaprika_id, last_updated) VALUES %s", tableName, strings.Join(valueString, ","))
			updateStatement := "ON CONFLICT (forbes_id) DO UPDATE SET name = EXCLUDED.name, coingecko_id = COALESCE(EXCLUDED.coingecko_id, coingecko_id), coinpaprika_id = COALESCE(EXCLUDED.coinpaprika_id, coinpaprika_id), last_updated = EXCLUDED.last_updated"

			query := insertStatement + " " + updateStatement
			_, inserterError := c.ExecContext(context.Background(), query, valueArgs...)
			if inserterError != nil {
				log.ErrorL(labels, "UpsertForbesAssets: Error Upserting Assets List to PostgreSQL: %v", inserterError)
				return inserterError
			}
			valueString = make([]string, 0, len(assetList))
			valueArgs = make([]interface{}, 0, len(assetList)*colCount)
			i = 0
		}
	}
	log.EndTimeL(labels, "Search assets Query", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return

}

// todo remove it
// func (a *assetsQuery) GetAssetsDataFS(ctx context.Context) (map[string]dto.AssetData, error) {
// 	span, labels := common.GenerateSpan("GetAssetsDataFS", ctx)
// 	defer span.End()

// 	span.AddEvent(fmt.Sprintf("Starting: %s", "GetAssetsDataFS"))
// 	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting: %s", "GetAssetsDataFS"))

// 	fs := a.fs.GetFirestoreClient()

// 	var assets = make(map[string]dto.AssetData)

// 	it := fs.Collection(dto.AssetsCollectionName).Documents(ctx)

// 	for {
// 		var asset dto.AssetData
// 		doc, done := it.Next()
// 		if done == iterator.Done {
// 			break
// 		}
// 		if err := doc.DataTo(&asset); err != nil {
// 			log.Error("Error getting asset data: %v", err)
// 			return nil, err
// 		}
// 		assets[asset.Symbol] = asset
// 	}

// 	span.SetStatus(codes.Ok, "GetAssetsDataFS")
// 	log.EndTimeL(labels, "GetAssetsDataFS", startTime, nil)
// 	return assets, nil
// }

func (a *assetsQuery) InsertToRemediationCollection(ctx context.Context, assets map[string]dto.MarketData) (err error) {
	span, labels := common.GenerateSpan("crypto-filter-protocol.InsertToRemediationCollection", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "crypto-filter-protocol.InsertToRemediationCollection"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "crypto-filter-protocol.InsertToRemediationCollection"))

	err = cloudUtils.WriteMapToCollection(assets, common.AssignRowyPrefix("cryptoIntake_assets", a.rowy_prefix), a.fs, ctx)
	if err != nil {
		log.Error("Error writing to firestore collection: %v", err)
		return
	}

	log.EndTime("crypto-filter-protocol.InsertToRemediationCollection", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return

}
