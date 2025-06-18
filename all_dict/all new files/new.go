//schem.sql

CREATE TABLE exchange_fundamentalslatest
(
    name TEXT,
    slug TEXT,
    id TEXT,
    logo TEXT,
    exchange_active_market_pairs NUMERIC,
    nomics JSON,
    forbes JSON,
    last_updated TIMESTAMPTZ DEFAULT Now(),
    PRIMARY KEY (id)
);

CREATE PROCEDURE upsert_exchange_fundamentalslatest(name TEXT,slug TEXT,id TEXT,logo TEXT,exchange_active_market_pairs NUMERIC,nomics JSON,forbes JSON, last_updated timestamp)
LANGUAGE SQL
AS $BODY$
  INSERT INTO exchange_fundamentalslatest 
	VALUES (name, slug, id, logo, exchange_active_market_pairs, nomics, forbes, last_updated) 
  ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id, name = EXCLUDED.name, slug = EXCLUDED.slug, 
  logo = EXCLUDED.logo, exchange_active_market_pairs = EXCLUDED.exchange_active_market_pairs,
  nomics = EXCLUDED.nomics, forbes = EXCLUDED.forbes, last_updated = EXCLUDED.last_updated;
$BODY$;


//bigquery 
// line 108 
type ExchangeResults struct {
	Name                      string  `json:"name" postgres:"name" bigquery:"name"`
	Slug                      string  `json:"slug" postgres:"slug" bigquery:"slug"`
	Id                        string  `json:"id" postgres:"id" bigquery:"id"`
	Logo                      string  `json:"logo" postgres:"logo" bigquery:"logo"`
	ExchangeActiveMarketPairs int     `json:"exchange_active_market_pairs" postgres:"exchange_active_market_pairs" bigquery:"exchange_active_market_pairs"`
	VolumeByExchange1D        float64 `json:"volume_by_exchange_1d" postgres:"volume_by_exchange_1d" bigquery:"volume_by_exchange_1d"`
}

//2297
func (bq *BQStore) ExchangeFundamentalsCG(ctxO context.Context, uuid string) (map[string]ExchangeResults, error) {
	ctx, span := tracer.Start(ctxO, "ExchangeFundamentalsCG")
	defer span.End()

	labels := make(map[string]string)

	labels["uuid"] = uuid
	span.SetAttributes(attribute.String("uuid", uuid))
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	labels["bigquery"] = "true"
	span.SetAttributes(attribute.Bool("bigquery", true))
	labels["subFunction"] = "ExchangeFundamentalsCG"

	exchangeTable := GetTableName("Digital_Asset_Exchanges_Tickers_Data")

	log.DebugL(labels, "Building Exchange Fundamentals")
	query := bq.Query(`
	SELECT
		market.name AS name,
		market.Identifier AS id,
		AVG(CAST(ticker.volume AS float64)) AS volume_by_exchange_1d,
		COALESCE(COUNT(CONCAT(ticker.CoinID, '-', ticker.Target)), 0) AS exchange_active_market_pairs
	FROM
		api-project-901373404215.digital_assets.Digital_Asset_Exchanges_Tickers_Data_dev,
		UNNEST(tickers) AS ticker
		JOIN (
			SELECT
				market.name AS name,
				market.Identifier AS id,
				ARRAY_AGG(ticker
					ORDER BY timestamp DESC
					LIMIT 1)[OFFSET(0)] AS ticker
			FROM
				api-project-901373404215.digital_assets.` + exchangeTable + `,
				UNNEST(tickers) AS ticker
			WHERE
				ticker.volume IS NOT NULL 
				AND
				ticker.Target IN ('USD', 'USDC', 'USDT')
			GROUP BY
				market.name,
				market.Identifier
		) AS latest_tickers ON
			latest_tickers.name = market.name AND
			latest_tickers.id = market.Identifier AND
			latest_tickers.ticker.CoinID = ticker.CoinID AND
			latest_tickers.ticker.Target = ticker.Target
	GROUP BY
		market.name,
		market.Identifier

	`)

	job, err := query.Run(ctx)
	if err != nil {
		return nil, err
	}

	labels["exchange_fundamentals_job_id"] = job.ID()
	span.SetAttributes(attribute.String("exchange_fundamentals_job_id", job.ID()))

	log.DebugL(labels, "Exchange Fundamentals Query Job ID: %s", job.ID())

	status, err := job.Wait(ctx)
	if err != nil {
		return nil, err
	}

	span.AddEvent("Exchange Fundamentals Query Job Completed")

	if err := status.Err(); err != nil {
		return nil, err
	}

	it, err := job.Read(ctx)
	if err != nil {
		return nil, err
	}

	exchangesResults := make(map[string]ExchangeResults)
	for {

		var exchangesResult ExchangeResults
		err := it.Next(&exchangesResult)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		exchangesResults[exchangesResult.Id] = exchangesResult

	}

	log.InfoL(labels, "Exchange Fundamentals for %d symbols built", len(exchangesResults))

	span.SetStatus(codes.Ok, "Exchange Fundamentals Query Job Completed")

	return exchangesResults, nil

}



// firstore 
// line 255
type ExchangeFundamentals struct {
	Name                      string         `json:"name" postgres:"name" bigquery:"name"`
	Slug                      string         `json:"slug" postgres:"slug" bigquery:"slug"`
	Id                        string         `json:"id" postgres:"id" bigquery:"id"`
	Logo                      string         `json:"logo" postgres:"logo" bigquery:"logo"`
	ExchangeActiveMarketPairs int            `json:"exchange_active_market_pairs" postgres:"exchange_active_market_pairs" bigquery:"exchange_active_market_pairs"`
	Nomics                    ExchangeVolume `json:"nomics" postgres:"nomics" bigquery:"nomics"`
	Forbes                    ExchangeVolume `json:"forbes" postgres:"forbes" bigquery:"forbes"`
	LastUpdated               time.Time      `json:"last_updated" postgres:"last_updated" bigquery:"last_updated"`
}

// fundamentals 
// line 481
func CombineExchanges(exchangeMetadata model.CoingeckoExchangeMetadata, exchangeData ExchangeResults) (ExchangeFundamentals, error) {
	var exchange ExchangeFundamentals
	exchange.Name = exchangeData.Name
	exchange.Slug = strings.ToLower(fmt.Sprintf("%s-%s", strings.ReplaceAll(exchangeData.Name, " ", "-"), exchangeData.Id))
	exchange.Logo = exchangeMetadata.LogoURL
	exchange.Id = exchangeData.Id
	exchange.ExchangeActiveMarketPairs = exchangeData.ExchangeActiveMarketPairs
	exchange.Nomics.VolumeByExchange1D = exchangeData.VolumeByExchange1D
	exchange.LastUpdated = time.Now()

	return exchange, nil
}

// postgresql
// line 3587
func GetExchangeMetaDataWithoutLimit(ctxO context.Context) ([]model.CoingeckoExchangeMetadata, error) {

	_, span := tracer.Start(ctxO, "GetExchangeMetaDataWithoutLimit")
	defer span.End()
	startTime := StartTime("Exchange Metadata Data Query")

	var exchangesMetadata []model.CoingeckoExchangeMetadata
	pg := PGConnect()

	query := `
	SELECT 
		id,
        name, 
        year, 
        description, 
        location, 
        logo_url, 
		website_url, 
        twitter_url, 
        facebook_url, 
        youtube_url, 
		linkedin_url, 
        reddit_url, 
        chat_url, 
        slack_url, 
		telegram_url, 
        blog_url, 
        centralized, 
        decentralized, 
		has_trading_incentive, 
        trust_score, 
        trust_score_rank, 
		trade_volume_24h_btc, 
        trade_volume_24h_btc_normalized, 
        last_updated
	FROM 
		public.coingecko_exchange_metadata
	where 
		trust_score is not null 
 	order by trust_score desc
	`

	queryResult, err := pg.Query(query)

	if err != nil {
		ConsumeTime("Exchange Metadata Data Query", startTime, err)
		span.SetStatus(otelCodes.Error, err.Error())
		return nil, err
	}

	for queryResult.Next() {
		var exchangeMetadata model.CoingeckoExchangeMetadata

		err := queryResult.Scan(&exchangeMetadata.ID, &exchangeMetadata.Name, &exchangeMetadata.Year, &exchangeMetadata.Description, &exchangeMetadata.Location, &exchangeMetadata.LogoURL, &exchangeMetadata.WebsiteURL, &exchangeMetadata.TwitterURL, &exchangeMetadata.FacebookURL, &exchangeMetadata.YoutubeURL, &exchangeMetadata.LinkedinURL, &exchangeMetadata.RedditURL, &exchangeMetadata.ChatURL, &exchangeMetadata.SlackURL, &exchangeMetadata.TelegramURL, &exchangeMetadata.BlogURL, &exchangeMetadata.Centralized, &exchangeMetadata.Decentralized, &exchangeMetadata.HasTradingIncentive, &exchangeMetadata.TrustScore, &exchangeMetadata.TrustScoreRank, &exchangeMetadata.TradeVolume24HBTC, &exchangeMetadata.TradeVolume24HBTCNormalized, &exchangeMetadata.LastUpdated)
		if err != nil {
			span.SetStatus(otelCodes.Error, err.Error())
			ConsumeTime("Exchange Metadata Data Query Scan", startTime, err)
			return nil, err
		}
		exchangesMetadata = append(exchangesMetadata, exchangeMetadata) 

	}
	ConsumeTime("Exchange Metadata Data Query", startTime, nil)
	span.SetStatus(otelCodes.Ok, "Success")
	return exchangesMetadata, nil
}

func InsertExchangeFundamentals(exchange ExchangeFundamentals, labels map[string]string) error {

	startTime := StartTimeL(labels, "Exchange Fundamental Insert")

	pg := PGConnect()

	// TODO: [FDA-1077] Change to Stored Procedure that preformes an upsert
	insertStatementsExchange := "INSERT INTO exchange_fundamentals(name, slug, id, logo, exchange_active_market_pairs, nomics, forbes) VALUES ($1, $2, $3, $4, $5, $6, $7)"
	// convert Exchanges[] and Nomics into json type to make it easy to store in PG table
	nomics, _ := json.Marshal(exchange.Nomics)
	forbes, _ := json.Marshal(exchange.Forbes)

	_, insertError := pg.Exec(insertStatementsExchange, exchange.Name, exchange.Slug, exchange.Id, exchange.Logo, exchange.ExchangeActiveMarketPairs, nomics, forbes)
	if insertError != nil {
		ConsumeTimeL(labels, "Exchange Fundamental Insert", startTime, insertError)
		return insertError
	}

	ConsumeTimeL(labels, "Exchange Fundamental Insert", startTime, nil)

	return nil
}

func InsertExchangeFundamentalsLatest(exchange ExchangeFundamentals, labels map[string]string) error {

	startTime := log.StartTimeL(labels, "Exchange Fundamental Insert")

	pg := PGConnect()

	insertStatementsFundamentals := "CALL upsert_exchange_fundamentalslatest ($1, $2, $3, $4, $5, $6, $7, $8)"

	query := insertStatementsFundamentals
	// convert Exchanges[] and Nomics into json type to make it easy to store in PG table
	nomics, _ := json.Marshal(exchange.Nomics)
	forbes, _ := json.Marshal(exchange.Forbes)

	_, insertError := pg.Exec(query, exchange.Name, exchange.Slug, exchange.Id, exchange.Logo, exchange.ExchangeActiveMarketPairs, nomics, forbes, exchange.LastUpdated)

	if insertError != nil {
		log.EndTimeL(labels, "Exchange Fundamental Insert", startTime, insertError)
		return insertError
	}

	log.EndTimeL(labels, "Exchange Fundamental Insert", startTime, nil)

	return nil
}

// main
// line 700
func BuildExchangeFundamentalsHandler(w http.ResponseWriter, r *http.Request) {
	setResponseHeaders(w, 0)
	vars := mux.Vars(r)
	period := vars["period"]
	labels := make(map[string]string)

	span := trace.SpanFromContext(r.Context())
	defer span.End()

	labels["function"] = "BuildExchangeFundamentalsHandler"
	labels["UUID"] = uuid.New().String()
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()
	span.SetAttributes(attribute.String("period", period))
	span.SetAttributes(attribute.String("UUID", labels["UUID"]))
	startTime := log.StartTimeL(labels, "Build Exchange Fundamentals Data ")

	g, ctx := errgroup.WithContext(r.Context())

	bqs, err := store.NewBQStore()
	if err != nil {
		// If an error occurs, the full context is canceled. Which causes all the first two Go Routines to cancel.
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		span.SetStatus(codes.Error, err.Error())
		return
	}

	// Go Routine 1
	// Get The Exchange Metadata elements needed for the Exchanges Fundamentals
	// this will get all exchanges metadata
	var exchangesMetaData []model.CoingeckoExchangeMetadata
	g.Go(func() error {
		results, err := store.GetExchangeMetaDataWithoutLimit(ctx)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return errors.New("error getting Exchanges Metadata CG from PG: " + err.Error())
		}
		log.DebugL(labels, "Received Exchanges Metadata CG %d results from PG", len(results))

		exchangesMetaData = results
		fmt.Println(len(exchangesMetaData))
		return nil

	})

	// Go Routine 2
	// Get The Exchanges Tickers needed for the Exchanges Fundamentals
	exchangeResults := make(map[string]store.ExchangeResults)
	g.Go(func() error {
		results, err := bqs.ExchangeFundamentalsCG(ctx, labels["UUID"])
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return errors.New("error getting Exchanges Tickers Fundamentals CG from BQ: " + err.Error())
		}
		log.DebugL(labels, "Received Exchanges Tickers Fundamentals CG %d results from BQ", len(results))

		exchangeResults = results
		fmt.Println(len(exchangeResults))

		return nil

	})

	span.AddEvent("Waiting for Go Routines to finish")
	err = g.Wait()  // Blocks till all go routines are done
	if err != nil { // If any of the go routines return an error
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal Server Error"))
		span.SetStatus(codes.Error, err.Error())
		return
	}

	// Starts new WaitGroup for the next set of go routines - which don't need to return an error
	var (
		wg sync.WaitGroup
		// Max 10 concurrent requests
		throttleChan = make(chan bool, 10)
	)

	for _, v := range exchangesMetaData {

		throttleChan <- true
		wg.Add(1)
		go func(ctxO context.Context, v model.CoingeckoExchangeMetadata, uuid string) {
			label := make(map[string]string)
			label["symbol"] = v.Name
			span.SetAttributes(attribute.String("exchange", v.Name))
			label["period"] = period
			span.SetAttributes(attribute.String("period", period))
			label["uuid"] = uuid
			span.SetAttributes(attribute.String("uuid", uuid))
			label["spanID"] = span.SpanContext().SpanID().String()
			label["traceID"] = span.SpanContext().TraceID().String()
			// check if the exchange metadata exist in exchange tickers
			if exchangeDataFromCG, ok := exchangeResults[v.ID]; ok {

				// map the exchange metadata to exchanges tickers to build exchange
				e, err := store.CombineExchanges(v, exchangeDataFromCG)

				if err != nil {
					log.ErrorL(label, "Error combining Exchange Fundamentals for %s: %s", v.ID, err.Error())
					goto waitReturn // If there is an error, skip to the end of the go routine
				}

				// Save the Exchanges Fundamentals to PG
				err = store.InsertExchangeFundamentals(e, label)
				if err != nil {
					log.ErrorL(label, "Error saving Exchange Fundamentals %s", err)
				}
				// Save the latest Exchanges Fundamentals to PG
				store.InsertExchangeFundamentalsLatest(e, label)
			}

		waitReturn:
			<-throttleChan // Remove from the throttle channel - allowing another go routine to start
			wg.Done()

		}(r.Context(), v, labels["UUID"])

	}

	wg.Wait()
	log.EndTimeL(labels, "Exchange Fundamentals CG Build ", startTime, nil)
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "ok")
	span.SetStatus(codes.Ok, "Exchange Fundamentals CG Built")

}