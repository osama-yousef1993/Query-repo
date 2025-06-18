
func UpsertNFTTickersData(ctx0 context.Context, nftdata *[]coingecko.NFTTickers) error {

	ctx, span := tracer.Start(ctx0, "UpsertNFTTickersData")
	defer span.End()
	startTime := log.StartTime("UpsertNFTTickersData")
	pg := PGConnect()

	exchangeListTMP := *nftdata
	valueString := make([]string, 0, len(exchangeListTMP))
	totalFields := 2 //total number of columns in the postgres collection
	valueArgs := make([]interface{}, 0, len(exchangeListTMP)*totalFields)
	var idsInserted []string
	tableName := "nftdatalatest_test1"
	var i = 0 //used for argument positions
	for y := 0; y < len(exchangeListTMP); y++ {
		mult := i * totalFields
		var nftData = exchangeListTMP[y]
		// if nftData.ID == "" {
		// 	nftData.ID = nftData.Name
		// }
		idsInserted = append(idsInserted, nftData.ID)

		/**
		* We're generating the insert value string for the postgres query.
		*
		* e.g. Let's say a collection in postgres has 5 columns. Then this looks something like this
		* ($1,$2,$3,$4,$5),($6,$7,$8,$9,$10),(..)...
		*
		* and so on. We use these variables in the postgres query builder. In our case, we currently have 46 columns in the collection.
		 */
		var valString = fmt.Sprintf("($%d,$%d)", mult+1, mult+2)
		valueString = append(valueString, valString)

		// Please note that the order of the following appending values matter. We map the following values to the 46 variables defined in the couple of lines defined above.
		valueArgs = append(valueArgs, nftData.ID)
		tickers, _ := json.Marshal(nftData.Tickers)
		valueArgs = append(valueArgs, tickers) // Explorers urls
		i++

		if len(valueArgs) >= 65000 || y == len(exchangeListTMP)-1 {
			log.Debug("UpsertNFTTickersData: Start Upserting NFT MetaData")
			insertStatementCoins := fmt.Sprintf("INSERT INTO %s (id,tickers) VALUES %s", tableName, strings.Join(valueString, ","))

			//only update urls(metadata)
			updateStatement := "ON CONFLICT (id) DO UPDATE SET  tickers = EXCLUDED.tickers"
			query := insertStatementCoins + updateStatement
			_, inserterError := pg.ExecContext(ctx, query, valueArgs...)

			if inserterError != nil {
				log.Error("UpsertNFTTickersData: Error Upserting NFT MetaData to PostgreSQL : %s", inserterError)
			}

			valueString = make([]string, 0, len(exchangeListTMP))
			valueArgs = make([]interface{}, 0, len(exchangeListTMP)*totalFields)

			i = 0
		}
	}
	log.EndTime("UpsertNFTTickersData: Successfully finished Upserting NFT MetaData at time : %s", startTime, nil)
	return nil
}