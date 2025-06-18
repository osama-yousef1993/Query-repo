func (bq *BQStore) InsertExchangesTickersData(ctx0 context.Context, exchangesTickers *[]models.BQExchangesTickers) error {
	ctx, span := tracer.Start(ctx0, "InsertExchangesTickersData")
	defer span.End()
	startTime := log.StartTime("InsertExchangesTickersData")
	currenciesTable := GetTableName("Digital_Asset_Exchanges_Tickers_Data")

	bqInserter := bq.Dataset("digital_assets").Table(currenciesTable).Inserter()
	bqInserter.IgnoreUnknownValues = true

	inserterErr := bqInserter.Put(ctx, *exchangesTickers)
	var retryError error
	if inserterErr != nil {
		if strings.Contains(inserterErr.Error(), "413") {
			log.Info("413 Error. Breaking up tickers and retrying insert")
			l := len(*exchangesTickers)
			var ticks []models.BQExchangesTickers
			ticks = append(ticks, *exchangesTickers...)
			for y := (l / 3); y < l; y += (l / 3) {
				a := ticks[y-(l/3) : y]
				er := bq.InsertExchangesTickersData(ctx, &a)
				if er != nil {
					retryError = er
				}
			}
			//If we couldnt recover return the error
			log.EndTime("InsertNFTData: Error Inserting NFT Data to BigQuery : %s", startTime, inserterErr)
			return retryError
		}
		log.EndTime("InsertExchangesTickersData: Error Inserting Exchanges Ticker Data to BigQuery : %s", startTime, inserterErr)
		return inserterErr
	}
	log.EndTime("InsertExchangesTickersData: Successfully finished Inserting Exchanges Ticker Data at time : %s", startTime, nil)
	return nil
}