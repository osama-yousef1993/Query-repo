package services

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/Forbes-Media/fda-common/crypto-filter-protocol/dto"
	"github.com/Forbes-Media/fda-common/crypto-filter-protocol/repository"
	"github.com/Forbes-Media/go-tools/log"
)

type AssetsIntake interface {
	SubmitAssetsToForbes(submission []dto.AssetSubmission) (err error)
}

type assetsIntake struct {
	dao repository.Dao
}

func NewAssetsIntake(dao repository.Dao) AssetsIntake {
	return &assetsIntake{dao: dao}
}

func (a *assetsIntake) SubmitAssetsToForbes(submission []dto.AssetSubmission) (err error) {
	db := a.dao.NewAssetsQuery()
	assets, err := db.GetForbesAssets("name")
	if err != nil {
		log.Error("Error getting assets: %v", err)
		return err
	}

	updates, failed, err := mapAssetsToForbes(submission, assets)
	if err != nil {
		log.Error("Error mapping assets: %v", err)
		return err
	}

	file, err := json.MarshalIndent(failed, "", " ")
	_ = os.WriteFile("failed_assets.json", file, 0644)
	file1, err := json.MarshalIndent(updates, "", " ")
	_ = os.WriteFile("updates_assets.json", file1, 0644)

	return nil
}

func mapAssetsToForbes(submissions []dto.AssetSubmission, assets *map[string]dto.ForbesAsset) (updates *[]dto.ForbesAsset, failed map[string]dto.AssetSubmission, err error) {
	updates = &[]dto.ForbesAsset{}
	failed = map[string]dto.AssetSubmission{}
	for _, submission := range submissions {
		if submission.Source != "coinpaprika" && submission.Source != "coingecko" {
			log.Error("Invalid source: %v", submission.Source)
			err = fmt.Errorf("Invalid source: %v", submission.Source)
			return
		}

		val, ok := (*assets)[strings.ToLower(submission.Name)]
		if ok {
			switch submission.Source {
			case "coinpaprika":
				val.CoinpaprikaID = &submission.ID
			case "coingecko":
				val.CoingeckoID = &submission.ID
			}
			*updates = append(*updates, val)
		} else {
			failed[submission.Name] = dto.AssetSubmission{ID: submission.ID, Name: submission.Name, Symbol: submission.Symbol, Price: submission.Price, CirculatingSupply: submission.CirculatingSupply, MarketCap: submission.MarketCap, Volume: submission.Volume, QuoteCurrency: submission.QuoteCurrency, Source: submission.Source}
		}
	}
	return
}
