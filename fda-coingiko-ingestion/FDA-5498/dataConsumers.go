
/*
- Retrieves a list of Exchanges from coingecko and runs until no resposes are left.
- Upserts data to postgres
- Inserts current price info into postgres
- Gets market history if we are pulling for the first time
- Inserts current market data to bigquery for historical chart
*/
func ConsumeNFTsTickers(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "ConsumeNFTsTickers")
	defer span.End()
	labels := generateLabelFromContext(ctx)
	startTime := log.StartTimeL(labels, "ConsumeNFTsTickers")
	var (
		cgNFTList    []coingecko.NFTTickers
		throttleChan = make(chan bool, 20)
		wg           sync.WaitGroup
	)
	nftIDs, err := store.GetIDNFTList(ctx)
	if err != nil {
		log.EndTimeL(labels, "ConsumeNFTsTickers: Error getting NFT ID Data PostgreSQL: %s", startTime, err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
	}
	//we will make requests until the response is empty. This is due to a lack of recieving a TOTAL header
	for _, id := range nftIDs[0:50] {
		throttleChan <- true
		wg.Add(1)
		go func(id string) {
			var maxRetries = 3
		RETRY:
			cgRateLimiter.Wait(limiterContext)
			data, _, err := c.GetNFTTickers(ctx, id)
			addToTotalCalls(ctx)
			if err != nil {
				log.EndTimeL(labels, "ConsumeNFTsTickers: Error getting NFTsTIckers from CoinGecko API: %s", startTime, err)
				w.WriteHeader(http.StatusInternalServerError)
				if maxRetries > 0 {
					maxRetries--
					goto RETRY
				}
				// return
			}
			if data != nil {
				data.ID = id
				cgNFTList = append(cgNFTList, *data)
			}
			<-throttleChan
			wg.Done()

		}(id)
	}
	// Get the market chart data if it is the first time we recieve the colletion in our list
	//Store the new NFT Trade data to the historical table
	log.DebugL(labels, "ConsumeNFTsTickers: Start Converting NFTMarket to BQNFTMarket")
	tickers := store.CGNFTTickersDataBQNFTTickersData(ctx, cgNFTList)
	bq, err := store.NewBQStore()
	if err != nil {
		log.EndTimeL(labels, "ConsumeNFTsTickers: Error connecting to BigQuery Client: %s", startTime, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	log.DebugL(labels, "ConsumeNFTsTickers: Start Inserting NFTTickersData to BigQuery")
	bq.InsertNFTTickersData(ctx, tickers)

	//Store the data to postgres
	log.DebugL(labels, "ConsumeNFTsTickers: Start Inserting NFTMarket to PostgreSQL")
	store.UpsertNFTTickersData(ctx, &cgNFTList)

	saveCount(ctx)
	log.EndTimeL(labels, "ConsumeNFTsTickers: Successfully finished consuming NFTsList from Coingecko API", startTime, nil)
	span.SetStatus(codes.Ok, "OK")
	w.WriteHeader(200)
	w.Write([]byte("OK"))

}