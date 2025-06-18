
type BQNFTTickers struct {
	ID             string                 `json:"id"`
	Tickers        []BQNFTTicker          `json:"tickers"`
	RowLastUpdated bigquery.NullTimestamp `json:"row_last_updated" bigquery:"row_last_updated"`
}
type BQNFTTicker struct {
	FloorPriceInNativeCurrency bigquery.NullFloat64   `json:"floor_price_in_native_currency" bigquery:"floor_price_in_native_currency"`
	H24VolumeInNativeCurrency  bigquery.NullFloat64   `json:"h24_volume_in_native_currency" bigquery:"h24_volume_in_native_currency"`
	NativeCurrency             string                 `json:"native_currency" bigquery:"native_currency"`
	NativeCurrencySymbol       string                 `json:"native_currency_symbol" bigquery:"native_currency_symbol"`
	UpdatedAt                  bigquery.NullTimestamp `json:"updated_at" bigquery:"updated_at"`
	NFTMarketplaceId           string                 `json:"nft_marketplace_id" bigquery:"nft_marketplace_id"`
	Name                       string                 `json:"name" bigquery:"name"`
	Image                      string                 `json:"image" bigquery:"image"`
	NFTCollectionUrl           string                 `json:"nft_collection_url" bigquery:"nft_collection_url"`
}


/*
Inserts NFT Data into bigquery
*/
func (bq *BQStore) InsertNFTTickersData(ctx0 context.Context, tickers *[]models.BQNFTTickers) error {
	ctx, span := tracer.Start(ctx0, "InsertNFTTickersData")
	defer span.End()
	startTime := log.StartTime("InsertNFTTickersData")

	currenciesTable := GetTableName("Digital_Assets_NFT_TickerData")

	bqInserter := bq.Dataset("digital_assets").Table(currenciesTable).Inserter()
	bqInserter.IgnoreUnknownValues = true

	inserterErr := bqInserter.Put(ctx, *tickers)
	var retryError error
	if inserterErr != nil {
		if strings.Contains(inserterErr.Error(), "413") {
			log.Info("413 Error. Breaking up tickers and retrying insert")
			l := len(*tickers)
			var ticks []models.BQNFTTickers
			ticks = append(ticks, *tickers...)
			for y := (l / 3); y < l; y += (l / 3) {
				a := ticks[y-(l/3) : y]
				er := bq.InsertNFTTickersData(ctx, &a)
				if er != nil {
					retryError = er
				}
			}
			//If we couldnt recover return the error
			log.EndTime("InsertNFTTickersData: Error Inserting NFT Data to BigQuery : %s", startTime, inserterErr)
			return retryError
		}
		//if not a 413 error return the error
		log.EndTime("InsertNFTTickersData: Error Inserting NFT Data to BigQuery : %s", startTime, inserterErr)
		return inserterErr

	}
	log.EndTime("InsertNFTTickersData: Successfully finished Inserting NFT Data at time : %s", startTime, nil)
	return nil
}