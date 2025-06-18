package main

import (
	"fmt"

	"github.com/Forbes-Media/fda-common/crypto-filter-protocol/dto"
	"github.com/Forbes-Media/fda-common/crypto-filter-protocol/repository"
	"github.com/Forbes-Media/fda-common/crypto-filter-protocol/services"
)

func main() {
	fmt.Println("Hello World")
	db := repository.NewDao()
	ai := services.NewAssetsIntake(db)
	// ei := services.NewExchangeIntake(db)
	//ei.BackFillExchangeTickers()
	// submissions := []dto.ExchangeSubmission{}
	// submissions = append(submissions, dto.ExchangeSubmission{ID: "binance", Name: "Binance", Source: "coinpaprika"})
	// submissions = append(submissions, dto.ExchangeSubmission{ID: "hyperliquid", Name: "HyperLiquid", Source: "coinpaprika"})
	// submissions = append(submissions, dto.ExchangeSubmission{ID: "binance-us", Name: "Binance US", Source: "coinpaprika"})
	// submissions = append(submissions, dto.ExchangeSubmission{ID: "bibox", Name: "Bibox", Source: "coingecko"})
	// ei.SubmitExchangesToForbes(submissions)

	submissions := []dto.AssetSubmission{}
	submissions = append(submissions, dto.AssetSubmission{ID: "bitcoin", Name: "Bitcoin", Source: "coinpaprika"})
	submissions = append(submissions, dto.AssetSubmission{ID: "ethereum", Name: "Ethereum", Source: "coinpaprika"})
	submissions = append(submissions, dto.AssetSubmission{ID: "tether", Name: "Tether", Source: "coingecko"})
	submissions = append(submissions, dto.AssetSubmission{ID: "binancecoin", Name: "Binance Coin", Source: "coingecko"})
	ai.SubmitAssetsToForbes(submissions)
}
