package main

import (
	"context"
	"time"

	cryptofilterprotocol "github.com/Forbes-Media/web3-common/crypto-filter-protocol"
	"github.com/Forbes-Media/web3-common/crypto-filter-protocol/common"
	"github.com/Forbes-Media/web3-common/crypto-filter-protocol/dto"
	"github.com/Forbes-Media/web3-common/crypto-filter-protocol/repository"
)

func main() {

	// SAMPLE CODE Implemantation

	/*
		db := repository.NewDao()
		ei := cryptofilterprotocol.NewExchangeIntake(db)
		//ei.BackFillExchangeTickers()
		/subisssions := []dto.ExchangeSubmission{}
		subisssions = append(subisssions, dto.ExchangeSubmission{ID: "binance", Name: "Binance", Source: "coinpaprika"})
		subisssions = append(subisssions, dto.ExchangeSubmission{ID: "hyperliquid", Name: "HyperLiquid", Source: "coinpaprika"})
		subisssions = append(subisssions, dto.ExchangeSubmission{ID: "binance-us", Name: "Binance US", Source: "coinpaprika"})
		subisssions = append(subisssions, dto.ExchangeSubmission{ID: "bibox", Name: "Bibox", Source: "coingecko"})
		ei.SubmitExchangesToForbes(context.Background(), subisssions)

		tickers := map[string]dto.BQExchangesTickers{
			"binance": {
				ID:     "binance",
				Name:   "Binance",
				Source: common.SrcCoinGecko,
				Tickers: []dto.BQTicker{
					{
						Base:   "BTC",
						Target: "USDT",
						Market: dto.BQMarketSimple{
							Name:       "Binance",
							Identifier: "binance",
						},
						Last:                   bigquery.NullFloat64{Float64: 102019.58, Valid: true},
						Volume:                 bigquery.NullFloat64{Float64: 23978.88849, Valid: true},
						ConvertedLast:          dto.BQConvertedLast{Btc: bigquery.NullFloat64{Float64: 0.99989502, Valid: true}, Eth: bigquery.NullFloat64{Float64: 27.626118, Valid: true}, Usd: bigquery.NullFloat64{Float64: 102078, Valid: true}},
						ConvertedVolume:        dto.BQConvertedVolume{Btc: bigquery.NullFloat64{Float64: 23567, Valid: true}, Eth: bigquery.NullFloat64{Float64: 651139, Valid: true}, Usd: bigquery.NullFloat64{Float64: 2405940710, Valid: true}},
						TrustScore:             "green",
						BidAskSpreadPercentage: bigquery.NullFloat64{Float64: 0.01001, Valid: true},
						Timestamp:              bigquery.NullTimestamp{Timestamp: time.Date(2025, time.January, 6, 20, 31, 32, 0, time.UTC), Valid: true},
						LastTradedAt:           bigquery.NullTimestamp{Timestamp: time.Date(2025, time.January, 6, 20, 31, 32, 0, time.UTC), Valid: true},
						LastFetchAt:            bigquery.NullTimestamp{Timestamp: time.Date(2025, time.January, 6, 20, 32, 31, 0, time.UTC), Valid: true},
						TradeURL:               "https://www.binance.com/en/trade/BTC_USDT?ref=37754157",
						CoinID:                 "bitcoin",
						TargetCoinID:           "tether",
					},
				},
			},
		}

		ei.IntakeExchangeTickers(context.Background(), tickers, common.SrcCoinGecko)
		//fmt.Println(tickers)


		// Assets Intake example
		// 1- CoinPaprika

		var markets []dto.MarketData

		markets = append(markets, dto.MarketData{
			ID:                "btc-bitcoin",
			Name:              "Bitcoin",
			Symbol:            "BTC",
			Price:             95345.804764692,
			CirculatingSupply: 0,
			MarketCap:         1830382948603,
			Volume:            59082376791,
			QuoteCurrency:     "",
			Source:            "coinpaprika",
			OccuranceTime:     time.Now().UTC(),
		})
		markets = append(markets, dto.MarketData{
			ID:                "eth-ethereum",
			Name:              "Ethereum",
			Symbol:            "ETH",
			Price:             3356.06,
			CirculatingSupply: 120479619,
			MarketCap:         390691420333,
			Volume:            30289250611,
			QuoteCurrency:     "",
			Source:            "coinpaprika",
			OccuranceTime:     time.Now().UTC(),
		})

		//db := repository.NewDao()
		ei := cryptofilterprotocol.NewAssetsIntake(db)
		ei.IntakeMarketData(context.Background(), markets, common.SrcCoinPaprika)

		// 2- CoinGecko

		markets = []dto.MarketData{}
		markets = append(markets, dto.MarketData{
			ID:                "bitcoin",
			Name:              "Bitcoin",
			Symbol:            "btc",
			Price:             69840,
			CirculatingSupply: 0,
			MarketCap:         1373546629363,
			Volume:            18867210007,
			QuoteCurrency:     "",
			Source:            "coingecko",
			OccuranceTime:     time.Now().UTC(),
		})
		markets = append(markets, dto.MarketData{
			ID:                "ethereum",
			Name:              "Ethereum",
			Symbol:            "eth",
			Price:             3234.14,
			CirculatingSupply: 120479619,
			MarketCap:         389329198376,
			Volume:            33850862412,
			QuoteCurrency:     "",
			Source:            "coingecko",
			OccuranceTime:     time.Now().UTC(),
		})
		db := repository.NewDao()
		ei = cryptofilterprotocol.NewAssetsIntake(db)
		ei.IntakeMarketData(context.Background(), markets, common.SrcCoinGecko)

		// Update SourceId with new value
		var x string = "hxro1"
		var y string = "hxro"

		err := ei.UpdateSourceID(context.Background(), x, y, common.SrcCoinGecko)
		if err != nil {
			fmt.Printf("Error update the source id %s", err.Error())
		}
	*/

	markets := []dto.MarketData{}
	markets = append(markets, dto.MarketData{
		ID:                "bitcoin",
		Name:              "Bitcoin",
		Symbol:            "btc",
		Price:             69840,
		CirculatingSupply: 0,
		MarketCap:         1373546629363,
		Volume:            18867210007,
		QuoteCurrency:     "",
		Source:            "coingecko",
		OccuranceTime:     time.Now().UTC(),
	})
	markets = append(markets, dto.MarketData{
		ID:                "bitcoin",
		Name:              "Bitcoin",
		Symbol:            "btc",
		Price:             69840,
		CirculatingSupply: 0,
		MarketCap:         1373546629363,
		Volume:            18867210007,
		QuoteCurrency:     "",
		Source:            "coingecko",
		OccuranceTime:     time.Now().UTC(),
	})
	markets = append(markets, dto.MarketData{
		ID:                "rock",
		Name:              "ROCK",
		Symbol:            "rock",
		Price:             69840,
		CirculatingSupply: 0,
		MarketCap:         1373546629363,
		Volume:            18867210007,
		QuoteCurrency:     "",
		Source:            "coingecko",
		OccuranceTime:     time.Now().UTC(),
	})
	markets = append(markets, dto.MarketData{
		ID:                "rock",
		Name:              "ROCK",
		Symbol:            "rock-4",
		Price:             69840,
		CirculatingSupply: 0,
		MarketCap:         1373546629363,
		Volume:            18867210007,
		QuoteCurrency:     "",
		Source:            "coingecko",
		OccuranceTime:     time.Now().UTC(),
	})
	markets = append(markets, dto.MarketData{
		ID:                "rock",
		Name:              "ROCK",
		Symbol:            "rock-3",
		Price:             69840,
		CirculatingSupply: 0,
		MarketCap:         1373546629363,
		Volume:            18867210007,
		QuoteCurrency:     "",
		Source:            "coingecko",
		OccuranceTime:     time.Now().UTC(),
	})
	markets = append(markets, dto.MarketData{
		ID:                "pumpkin",
		Name:              "Pumpkin",
		Symbol:            "pump",
		Price:             69840,
		CirculatingSupply: 0,
		MarketCap:         1373546629363,
		Volume:            18867210007,
		QuoteCurrency:     "",
		Source:            "coingecko",
		OccuranceTime:     time.Now().UTC(),
	})
	markets = append(markets, dto.MarketData{
		ID:                "pumpkin-3",
		Name:              "Pumpkin",
		Symbol:            "pkin",
		Price:             69840,
		CirculatingSupply: 0,
		MarketCap:         1373546629363,
		Volume:            18867210007,
		QuoteCurrency:     "",
		Source:            "coingecko",
		OccuranceTime:     time.Now().UTC(),
	})
	markets = append(markets, dto.MarketData{
		ID:                "pumpkin-3",
		Name:              "Pumpkin",
		Symbol:            "pkin",
		Price:             69840,
		CirculatingSupply: 0,
		MarketCap:         1373546629363,
		Volume:            18867210007,
		QuoteCurrency:     "",
		Source:            "coingecko",
		OccuranceTime:     time.Now().UTC(),
	})
	markets = append(markets, dto.MarketData{
		ID:                "pumpkin-2",
		Name:              "Pumpkin",
		Symbol:            "pumpkin",
		Price:             69840,
		CirculatingSupply: 0,
		MarketCap:         1373546629363,
		Volume:            18867210007,
		QuoteCurrency:     "",
		Source:            "coingecko",
		OccuranceTime:     time.Now().UTC(),
	})
	markets = append(markets, dto.MarketData{
		ID:                "ethereum",
		Name:              "Ethereum",
		Symbol:            "eth",
		Price:             3234.14,
		CirculatingSupply: 120479619,
		MarketCap:         389329198376,
		Volume:            33850862412,
		QuoteCurrency:     "",
		Source:            "coingecko",
		OccuranceTime:     time.Now().UTC(),
	})
	markets = append(markets, dto.MarketData{
		ID:                "ethereum",
		Name:              "Ethereum",
		Symbol:            "eth",
		Price:             3234.14,
		CirculatingSupply: 120479619,
		MarketCap:         389329198376,
		Volume:            33850862412,
		QuoteCurrency:     "",
		Source:            "coingecko",
		OccuranceTime:     time.Now().UTC(),
	})
	db := repository.NewDao()
	ei := cryptofilterprotocol.NewAssetsIntake(db)
	ei.IntakeMarketData(context.Background(), markets, common.SrcCoinGecko)
}
