package cryptofilterprotocol

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/Forbes-Media/fda-common/crypto-filter-protocol/common"
	"github.com/Forbes-Media/fda-common/crypto-filter-protocol/dto"
	"github.com/Forbes-Media/fda-common/crypto-filter-protocol/repository"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
)

type AssetsIntake interface {
	InTakeMarketData(ctx context.Context, assetsMap []dto.MarketData, source common.DataSource) (err error)
	BackFillMarketData(ctx context.Context)
}

type assetsIntake struct {
	dao repository.Dao
}

func NewAssetsIntake(dao repository.Dao) AssetsIntake {
	return &assetsIntake{dao: dao}
}

// func (a *assetsIntake) SubmitAssetsToForbes(ctx context.Context, submission []dto.MarketData, source common.DataSource) (err error) {
// 	span, labels := common.GenerateSpan("SubmitAssetsToForbes", ctx)
// 	defer span.End()

// 	db := a.dao.NewAssetsQuery()
// 	col := ""
// 	if source == common.Src_CoinGecko {
// 		col = "coingecko_id"
// 	} else if source == common.Src_CoinPaprika {
// 		col = "coinpaprika_id"
// 	} else {
// 		log.Error("Invalid source: %v", source)
// 		return fmt.Errorf("Invalid source: %v", source)
// 	}

// 	forbesAssets, err := db.GetForbesAssets(col)
// 	if err != nil {
// 		log.Error("Error getting assets: %v", err)
// 		return err
// 	}

// 	updates, failed, err := mapAssetsToForbes(submission, forbesAssets)
// 	if err != nil {
// 		log.Error("Error mapping assets: %v", err)
// 		return err
// 	}

// 	err = db.UpsertForbesAssets(ctx, updates)
// 	if err != nil {
// 		log.ErrorL(labels, "Error upserting exchanges: %v", err)
// 		span.SetStatus(codes.Error, err.Error())
// 		return err
// 	}

// 	err = db.InsertToRemediationCollection(ctx, failed)
// 	if err != nil {
// 		log.Error("Error writing to exchange remediation collection: %v", err)
// 		span.SetStatus(codes.Error, err.Error())
// 		return err
// 	}

// 	return nil
// }

// func mapAssetsToForbes(submissions []dto.MarketData, assets *map[string]dto.ForbesAsset) (updates *[]dto.ForbesAsset, failed map[string]dto.MarketData, err error) {
// 	updates = &[]dto.ForbesAsset{}
// 	failed = map[string]dto.MarketData{}
// 	for _, submission := range submissions {
// 		if submission.Source != "coinpaprika" && submission.Source != "coingecko" {
// 			log.Error("Invalid source: %v", submission.Source)
// 			err = fmt.Errorf("Invalid source: %v", submission.Source)
// 			return
// 		}

// 		if submission.ContractAddress != "" {
// 			val, ok := (*assets)[submission.ID]
// 			if ok {
// 				switch submission.Source {
// 				case "coinpaprika":
// 					val.CoinpaprikaID = &submission.ID
// 				case "coingecko":
// 					val.CoingeckoID = &submission.ID
// 				}
// 				*updates = append(*updates, val)

// 			} else {
// 				failed[submission.Name] = dto.MarketData{ID: submission.ID, Name: submission.Name, Symbol: submission.Symbol, Price: submission.Price, CirculatingSupply: submission.CirculatingSupply, MarketCap: submission.MarketCap, Volume: submission.Volume, QuoteCurrency: submission.QuoteCurrency, Source: submission.Source}
// 			}

// 		}

// 	}
// 	return
// }

// func (a *assetsIntake) InsertMarketData(ctx context.Context, assetsMap []dto.MarketData, source common.DataSource) (err error) {
// 	span, labels := common.GenerateSpan("InTakeAssets", ctx)
// 	defer span.End()

// 	span.AddEvent(fmt.Sprintf("Starting: %s", "InTakeAssets"))
// 	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting: %s", "InTakeAssets"))
// 	db := a.dao.NewAssetsQuery()
// 	col := ""
// 	if source == common.Src_CoinGecko {
// 		col = "coingecko_id"
// 	} else if source == common.Src_CoinPaprika {
// 		col = "coinpaprika_id"
// 	} else {
// 		log.Error("Invalid source: %v", source)
// 		return fmt.Errorf("Invalid source: %v", source)
// 	}

// 	forbesAssets, err := db.GetForbesAssets(col)
// 	if err != nil {
// 		log.Error("Error getting assets: %v", err)
// 		return err
// 	}
// 	var (
// 		// inTakeAssets []dto.MarketData
// 		assets []dto.BQMarketData
// 		failed = map[string]dto.MarketData{}
// 	)
// 	for _, asset := range assetsMap {
// 		register, ok := (*forbesAssets)[string(asset.ID)]
// 		if ok {
// 			mappedAsset := mapMarketData(ctx, asset)
// 			mappedAsset.ForbesID = *register.ForbesID
// 			assets = append(assets, mappedAsset)
// 		} else {
// 			failed[asset.Name] = dto.MarketData{ID: asset.ID, Name: asset.Name, Symbol: asset.Symbol, Source: asset.Source}
// 		}

// 	}
// 	db.InsertMarketData(ctx, &assets)

// 	// err = db.InsertToRemediationCollection(ctx, failed)
// 	// if err != nil {
// 	// 	log.Error("Error writing to exchange remediation collection: %v", err)
// 	// 	span.SetStatus(codes.Error, err.Error())
// 	// 	return err
// 	// }

// 	// if inTakeAssets != nil && len(inTakeAssets) > 0 {
// 	// 	err = a.InTakeMarketData(ctx, inTakeAssets, source)
// 	// 	if err != nil {
// 	// 		log.ErrorL(labels, "Error upserting Assets: %v", err)
// 	// 		span.SetStatus(codes.Error, err.Error())
// 	// 		return
// 	// 	}
// 	// }
// 	span.SetStatus(codes.Ok, "InTakeAssets")
// 	log.EndTimeL(labels, "InTakeAssets", startTime, nil)
// 	return nil
// }

func mapMarketData(ctx context.Context, asset dto.MarketData) dto.BQMarketData {
	span, labels := common.GenerateSpan("InTakeMarketData", ctx)
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting: %s", "InTakeMarketData"))
	var mappedAsset dto.BQMarketData

	mappedAsset.ID = asset.ID
	mappedAsset.Name = asset.Name
	mappedAsset.Symbol = asset.Symbol
	mappedAsset.Price = bigquery.NullFloat64{Float64: asset.Price, Valid: true}
	mappedAsset.CirculatingSupply = asset.CirculatingSupply
	mappedAsset.MarketCap = bigquery.NullFloat64{Float64: asset.MarketCap, Valid: true}
	mappedAsset.Volume = bigquery.NullFloat64{Float64: asset.Volume, Valid: true}
	mappedAsset.QuoteCurrency = asset.QuoteCurrency
	mappedAsset.Source = asset.Source
	if asset.OccuranceTime.IsZero() {
		mappedAsset.OccuranceTime = bigquery.NullTimestamp{Timestamp: time.Now(), Valid: true}
	}

	span.SetStatus(codes.Ok, "Success")
	log.DebugL(labels, "Successfully validated exchange tickers")
	return mappedAsset
}

func (a *assetsIntake) InTakeMarketData(ctx context.Context, assetsArray []dto.MarketData, source common.DataSource) (err error) {
	span, labels := common.GenerateSpan("InTakeMarketData", ctx)
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting: %s", "InTakeMarketData"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting: %s", "InTakeMarketData"))
	db := a.dao.NewAssetsQuery()
	col := ""
	if source == common.Src_CoinGecko {
		col = "coingecko_id"
	} else if source == common.Src_CoinPaprika {
		col = "coinpaprika_id"
	} else {
		log.Error("Invalid source: %v", source)
		return fmt.Errorf("Invalid source: %v", source)
	}

	forbesAssets, err := db.GetForbesAssets(col)
	if err != nil {
		log.Error("Error getting assets: %v", err)
		return err
	}

	forbesAssetsByContractAddress, err := db.GetForbesAssets("contract_address")
	if err != nil {
		log.Error("Error getting assets: %v", err)
		return err
	}

	var successful []dto.BQMarketData
	var newForbesAssets []dto.ForbesAsset
	var failed = make(map[string]dto.MarketData)

	for _, asset := range assetsArray {
		coin, ok := (*forbesAssets)[string(asset.ID)]
		if ok {
			mappedAsset := mapMarketData(ctx, asset)
			mappedAsset.ForbesID = *coin.ForbesID
			successful = append(successful, mappedAsset)
		} else {
			if asset.ContractAddress != "" {
				coin, ok := (*forbesAssetsByContractAddress)[string(asset.ContractAddress)]
				if ok {
					newForbesAssets = append(newForbesAssets, dto.ForbesAsset{ForbesID: coin.ForbesID, Name: &asset.Name, Symbol: &asset.Symbol})
				} else {
					failed[asset.Name] = dto.MarketData{ID: asset.ID, Name: asset.Name, Symbol: asset.Symbol, Source: asset.Source}
				}

			}
			failed[asset.Name] = dto.MarketData{ID: asset.ID, Name: asset.Name, Symbol: asset.Symbol, Price: asset.Price, Source: asset.Source}
		}

	}

	err = db.UpsertForbesAssets(ctx, &newForbesAssets)
	if err != nil {
		log.ErrorL(labels, "Error upserting exchanges: %v", err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	db.InsertMarketData(ctx, &successful)

	if len(failed) > 0 {
		err = db.InsertToRemediationCollection(ctx, failed)
		if err != nil {
			log.Error("Error writing to assets remediation collection: %v", err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
	}
	span.SetStatus(codes.Ok, "InTakeMarketData")
	log.EndTimeL(labels, "InTakeMarketData", startTime, nil)
	return nil
}

// // todo remove it
// func CheckNameSymbolAssets(ctx context.Context, assetsBySymbol, assetsByName *map[string]dto.ForbesAsset, asset dto.MarketData) *dto.BQMarketData {
// 	span, labels := common.GenerateSpan("InTakeMarketData", ctx)
// 	defer span.End()

// 	span.AddEvent(fmt.Sprintf("Starting: %s", "InTakeMarketData"))
// 	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting: %s", "InTakeMarketData"))

// 	var mappedAsset dto.BQMarketData
// 	registerSymbol, ok := (*assetsBySymbol)[string(asset.Symbol)]
// 	if !ok {
// 		registerName, ok := (*assetsByName)[string(asset.Symbol)]
// 		if !ok {
// 			return nil
// 		}
// 		mappedAsset := mapMarketData(ctx, asset)
// 		mappedAsset.ForbesID = *registerName.ForbesID
// 		return &mappedAsset
// 	}

// 	mappedAsset = mapMarketData(ctx, asset)
// 	mappedAsset.ForbesID = *registerSymbol.ForbesID

// 	span.SetStatus(codes.Ok, "InTakeMarketData")
// 	log.EndTimeL(labels, "InTakeMarketData", startTime, nil)
// 	return &mappedAsset
// }

func (e *assetsIntake) BackFillMarketData(ctx context.Context) {
	span, labels := common.GenerateSpan("assets_intake.BackFillMarketData", ctx)

	db := repository.NewDao()

	assets, err := db.NewAssetsQuery().GetForbesAssets("forbes_id")
	if err != nil {
		log.ErrorL(labels, "Error getting assets: %v", err)
		span.SetStatus(codes.Error, err.Error())
		return
	}
	if assets != nil && len(*assets) > 0 {
		log.ErrorL(labels, "cannot backfill assets are already defined")
		span.SetStatus(codes.Error, "Assets are already defined")
		return
	}

	assetsMap, err := db.NewAssetsQuery().GetFundamentalsDistinct(ctx)
	if err != nil {
		log.ErrorL(labels, "Error closing client: %v", err)
		span.SetStatus(codes.Error, err.Error())
		return
	}

	assetsArr := &[]dto.ForbesAsset{}
	for _, asset := range *assetsMap {
		*assetsArr = append(*assetsArr, asset)
	}

	err = db.NewAssetsQuery().UpsertForbesAssets(ctx, assetsArr)
	if err != nil {
		log.ErrorL(labels, "Error upserting Assets: %v", err)
		span.SetStatus(codes.Error, err.Error())
		return
	}
	span.SetStatus(codes.Ok, "Success")
	log.InfoL(labels, "Successfully backfilled Assets")
	return

}
