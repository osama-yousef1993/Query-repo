
// Converts NFT Market Data from coingecko to our BigQuery Structure
func CGNFTTickersDataBQNFTTickersData(ctx0 context.Context, cgData []coingecko.NFTTickers) *[]models.BQNFTTickers {

	_, span := tracer.Start(ctx0, "CGNFTMarketDataBQNFTMarketHistory")
	defer span.End()
	startTime := log.StartTime("CGNFTMarketDataBQNFTMarketHistory")
	var bqTickerData []models.BQNFTTickers

	log.Debug("CGNFTMarketDataBQNFTMarketHistory: Start Mapping BQ NFT Market Data")
	for _, nft := range cgData {
		var tickers []models.BQNFTTicker

		for _, ticker := range nft.Tickers {
			tickers = append(tickers, models.BQNFTTicker{
				FloorPriceInNativeCurrency: bigquery.NullFloat64{Float64: ticker.FloorPriceInNativeCurrency, Valid: true},
				H24VolumeInNativeCurrency:  bigquery.NullFloat64{Float64: ticker.H24VolumeInNativeCurrency, Valid: true},
				NativeCurrency:             ticker.NativeCurrency,
				NativeCurrencySymbol:       ticker.NativeCurrencySymbol,
				UpdatedAt:                  bigquery.NullTimestamp{Timestamp: ticker.UpdatedAt, Valid: true},
				NFTMarketplaceId:           ticker.NFTMarketplaceId,
				Name:                       ticker.Name,
				Image:                      ticker.Image,
				NFTCollectionUrl:           ticker.NFTCollectionUrl,
			})
		}
		bqTickerData = append(bqTickerData, models.BQNFTTickers{
			ID:             nft.ID,
			Tickers:        tickers,
			RowLastUpdated: bigquery.NullTimestamp{Timestamp: time.Now(), Valid: true},
		})

	}
	log.EndTime("CGNFTMarketDataBQNFTMarketHistory: Successfully finished Mapping NFT Market Data To BQ NFT Market History at time : %s", startTime, nil)
	return &bqTickerData
}
