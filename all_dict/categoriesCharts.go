// postgrsql
type Categories struct {
	ID    string   `json:"id" postgres:"id"`       // ID of the category
	Coins []string `json:"coins" postgres:"coins"` // Top 3 coins in the category                           // List of all the assets in the category
}

func GetCategories(ctx0 context.Context) ([]Categories, error) {
	pg := PGConnect()

	ctx, span := tracer.Start(ctx0, "GetCategories")

	defer span.End()
	startTime := log.StartTime("Get Categories Query")

	var categories []Categories
	span.AddEvent("Start Getting Categories")
	queryResult, err := pg.QueryContext(ctx, `
		select id, array_agg(markets ->> 'id') as coins
		from (
			select json_array_elements(markets) as markets, id
			from coingecko_categories
		) as foo
		group by id
	`)

	if err != nil {
		span.SetStatus(codes.Error, "PGGetCategories")
		log.EndTime("Get Categories Query", startTime, err)
		return nil, err
	}
	defer queryResult.Close()

	for queryResult.Next() {
		var category Categories
		err := queryResult.Scan(&category.ID, pq.Array(&category.Coins))
		if err != nil {
			span.SetStatus(codes.Error, "PGGetCategories scan error")
			log.EndTime("Get Categories Query", startTime, err)
			return nil, err
		}
		categories = append(categories, category)
	}
	log.EndTime("Get Categories Query", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return categories, nil
}

// bigquery.go
// BuildCategoriesChartQuery queries the chart data by interval
// Inputs:
// - Interva: String (1 DAY, 1 HOUR, 1 MINUTE)
// - TargetResolutionSeconds: Int as String (60, 300, 900, 3600, 14400, 86400)
// - UUID: String `uuid.New().String()`
// - Context: Context
// Returns the chart data
func (bq *BQStore) BuildCategoriesChartQuery(interval BQTimeInterval, stringResolutionSeconds ChartQueryResSeconds) *bigquery.Query {
	var query *bigquery.Query
	candlesTable := fmt.Sprintf("Digital_Asset_MarketData%s", os.Getenv("DATA_NAMESPACE"))
	query = bq.Query(`
		SELECT
			ARRAY_AGG(STRUCT('Time',
				time,
				'Price',
				price)) AS beprices
			FROM (
			SELECT
				symbol,
				time,
				CAST(AVG(price) AS FLOAT64) price
			FROM (
				SELECT
					ID symbol,
					Occurance_Time AS time,
					CAST(AVG(Price) AS FLOAT64) price,
					ROW_NUMBER() OVER (PARTITION BY ID, CAST(FLOOR(UNIX_SECONDS(Occurance_Time)/ (` + string(stringResolutionSeconds) + ` ))AS INT64 )
					ORDER BY
						Occurance_Time) AS row_num
				FROM
					api-project-901373404215.digital_assets.` + candlesTable + ` c
				WHERE
					Occurance_Time >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL ` + string(interval) + `)
				GROUP BY
					Occurance_Time,
					ID
				ORDER BY
					Occurance_Time ) AS test
			WHERE
				row_num = 1
				AND symbol IN  UNNEST(@coins)
			GROUP BY
				symbol,
				time
			ORDER BY
				time ASC ) AS foo
		`)
	return query

}

// QueryChartByInterval queries the chart data by interval
// Inputs:
// - Interva: String (1 DAY, 1 HOUR, 1 MINUTE)
// - TargetResolutionSeconds: Int as String (60, 300, 900, 3600, 14400, 86400)
// - UUID: String `uuid.New().String()`
// - Context: Context
// Returns the chart data
func (bq *BQStore) CategoriesQueryChartByInterval(interval BQTimeInterval, stringResolutionSeconds ChartQueryResSeconds, uuid string, dataType DictionaryCategory, coins []string, categoryId string, ctxO context.Context) (*TimeSeriesResultPG, error) {

	ctx, span := tracer.Start(ctxO, "CategoriesQueryChartByInterval")
	defer span.End()

	labels := make(map[string]string)

	labels["uuid"] = uuid
	span.SetAttributes(attribute.String("uuid", uuid))
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	labels["bigquery"] = "true"
	span.SetAttributes(attribute.Bool("bigquery", true))
	labels["subFunction"] = "CategoriesQueryChartByInterval"

	var (
		tsResult TimeSeriesResultPG
		wg       sync.WaitGroup
		// Max 10 concurrent requests
		throttleChan = make(chan bool, 20)
		qErr         error
	)

	throttleChan <- true
	wg.Add(1)
	go func() (*TimeSeriesResultPG, error) {
		query := bq.BuildCategoriesChartQuery(interval, stringResolutionSeconds)
		query.Parameters = []bigquery.QueryParameter{
			{
				Name:  "coins",
				Value: coins,
			},
		}
		job, err := query.Run(ctx)
		if err != nil {
			return &tsResult, err
		}

		log.DebugL(labels, "Fundamentals Query Job ID: %s", job.ID())
		span.SetAttributes(attribute.String("chart_by_interval_job_id", job.ID()))

		status, err := job.Wait(ctx)
		if err != nil {
			return &tsResult, err
		}

		if err := status.Err(); err != nil {
			return &tsResult, err
		}

		it, err := job.Read(ctx)
		if err != nil {
			return &tsResult, err
		}

		span.AddEvent("Query Chart By Interval BQ Query Complete")

		for {
			// var tsObj = TimeSeriesResultPG{}
			tsResult.TargetResolutionSeconds, _ = strconv.Atoi(string(stringResolutionSeconds))
			tsResult.IsIndex = false
			err := it.Next(&tsResult)
			if err == iterator.Done {
				break
			}
			if err != nil {
				return &tsResult, err
			}
			//SortChartDataPG(tsObj.Slice)
			tsResult.AssetType = "CATEGORY"
			tsResult.Symbol = categoryId

			// tsResult = append(tsResult, tsObj)
		}
		<-throttleChan
		wg.Done()
		return nil, nil
	}()
	wg.Wait()
	if qErr != nil {
		log.Error("%s", qErr)
		return &tsResult, qErr

	}

	span.SetStatus(codes.Ok, "Query Chart By Interval BQ Query Complete")
	return &tsResult, nil
}

// main
v1.HandleFunc("/get-categories-chart/{period}", BuildCategoriesChartHandler).Methods(http.MethodGet, http.MethodOptions)
r.Handle("/build-categories-chart/{period}", oidcAuthMiddleware.Middleware(http.HandlerFunc(BuildCategoriesChartHandler))).Methods(http.MethodPost)

func BuildCategoriesChartHandler(w http.ResponseWriter, r *http.Request) {
	setResponseHeaders(w, 300)
	labels := make(map[string]string)
	ctx, span := tracer.Start(r.Context(), "BuildCategoriesChartHandler")

	defer span.End()
	labels["UUID"] = uuid.New().String()
	labels["function"] = "BuildCategoriesChartHandler"
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))
	startTime := log.StartTime("Get Frequently Asked Questions Data")
	g, ctx := errgroup.WithContext(r.Context())
	var categories []store.Categories

	g.Go(func() error {
		catResult, err := store.GetCategories(ctx)
		if err != nil {
			log.ErrorL(labels, "%s", err)
			w.WriteHeader(http.StatusInternalServerError)
			span.SetStatus(codes.Error, err.Error())
			return errors.New("Error getting tickers from BQ: " + err.Error())
		}
		categories = catResult
		return nil
	})

	err := g.Wait() // Blocks till all go routines are done
	if err != nil { // If any of the go routines return an error
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal Server Error"))
		span.SetStatus(codes.Error, err.Error())
		return
	}

	vars := mux.Vars(r)
	period := vars["period"]

	var (
		result []store.TimeSeriesResultPG
		qErr   error
		wg     sync.WaitGroup
		// Max 10 concurrent requests
		throttleChan = make(chan bool, 20)
	)

	bqs, err := store.NewBQStore()
	if err != nil {
		// If an error occurs, the full context is canceled. Which causes all the first two Go Routines to cancel.
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	for _, category := range categories {
		throttleChan <- true
		wg.Add(1)

		coins := category.Coins
		id := category.ID
		switch period {
		case "24h":
			go func(coins []string, id string) {
				var res *store.TimeSeriesResultPG
				res, qErr = bqs.CategoriesQueryChartByInterval(store.BQ_OneDay, store.ResSeconds_900, labels["UUID"], store.Category, coins, id, r.Context())
				res.Interval = fmt.Sprintf("%s_%s", id, period)
				result = append(result, *res)
				<-throttleChan
				wg.Done()
			}(coins, id)
		case "7d":
			throttleChan <- true
			// wg.Add(1)
			go func(coins []string, id string) {
				var res *store.TimeSeriesResultPG
				res, qErr = bqs.CategoriesQueryChartByInterval(store.BQ_OneDay, store.ResSeconds_14400, labels["UUID"], store.Category, coins, id, r.Context())
				res.Interval = fmt.Sprintf("%s_%s", id, period)
				result = append(result, *res)
				<-throttleChan
				wg.Done()
			}(coins, id)
		case "30d":
			throttleChan <- true
			// wg.Add(1)
			go func(coins []string, id string) {
				var res *store.TimeSeriesResultPG
				res, qErr = bqs.CategoriesQueryChartByInterval(store.BQ_OneDay, store.ResSeconds_43200, labels["UUID"], store.Category, coins, id, r.Context())
				res.Interval = fmt.Sprintf("%s_%s", id, period)
				result = append(result, *res)
				<-throttleChan
				wg.Done()
			}(coins, id)
		case "1y":
			throttleChan <- true
			// wg.Add(1)
			go func(coins []string, id string) {
				var res *store.TimeSeriesResultPG
				res, qErr = bqs.CategoriesQueryChartByInterval(store.BQ_OneDay, store.ResSeconds_432000, labels["UUID"], store.Category, coins, id, r.Context())
				res.Interval = fmt.Sprintf("%s_%s", id, period)
				result = append(result, *res)
				<-throttleChan
				wg.Done()
			}(coins, id)
		case "max":
			throttleChan <- true
			// wg.Add(1)
			go func(coins []string, id string) {
				var res *store.TimeSeriesResultPG
				res, qErr = bqs.CategoriesQueryChartByInterval(store.BQ_OneDay, store.ResSeconds_1296000, labels["UUID"], store.Category, coins, id, r.Context())
				res.Interval = fmt.Sprintf("%s_%s", id, period)
				result = append(result, *res)
				<-throttleChan
				wg.Done()
			}(coins, id)
		default:
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		wg.Wait()
		if qErr != nil {
			log.Error("%s", qErr)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}

	response, err := json.Marshal(result)
	if err != nil {
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		span.SetStatus(codes.Error, err.Error())
		return
	}

	log.EndTimeL(labels, "Frequently Asked Questions Data", startTime, nil)
	span.SetStatus(codes.Ok, "Frequently Asked Questions Data")
	w.WriteHeader(http.StatusOK)
	w.Write(response)

}



// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

// potgrsql

if chart.AssetType == "CATEGORY" {
	valueCharts = append(valueCharts, chart.Interval)
} else {
	interval := fmt.Sprintf("%s_%s", chart.Symbol, period)
	valueCharts = append(valueCharts, interval)
}

type Categories struct {
	ID    string   `json:"id" postgres:"id"`       // ID of the category
	Coins []string `json:"coins" postgres:"coins"` // Top 3 coins in the category
}

func GetCategories(ctx0 context.Context) ([]Categories, error) {
	pg := PGConnect()

	ctx, span := tracer.Start(ctx0, "GetCategories")

	defer span.End()
	startTime := log.StartTime("GetCategories")

	var categories []Categories
	span.AddEvent("Start Getting Categories")
	queryResult, err := pg.QueryContext(ctx, `
		select id, array_agg(markets ->> 'id') as coins
		from (
			select json_array_elements(markets) as markets, id
			from coingecko_categories
		) as foo
		group by id
	`)

	if err != nil {
		log.EndTime("GetCategories: Error Getting Categories Data from PG", startTime, err)
		span.SetStatus(codes.Error, "GetCategories: Error Getting Categories Data from PG")
		return nil, err
	}
	defer queryResult.Close()

	for queryResult.Next() {
		var category Categories
		err := queryResult.Scan(&category.ID, pq.Array(&category.Coins))
		if err != nil {
			log.EndTime("GetCategories: Error Mapping Categories Data from PG", startTime, err)
			span.SetStatus(codes.Error, "GetCategories: Error Mapping Categories Data from PG")
			return nil, err
		}
		categories = append(categories, category)
	}
	log.EndTime("GetCategories: Successfully finished Getting Categories Data", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return categories, nil
}

// bigquery

// BuildCategoriesChartQuery queries the chart data by interval
// Inputs:
// - Interva: String (1 DAY, 1 HOUR, 1 MINUTE)
// - TargetResolutionSeconds: Int as String (60, 300, 900, 3600, 14400, 86400)
// - UUID: String `uuid.New().String()`
// - Context: Context
// Returns the chart data
func (bq *BQStore) BuildCategoriesChartQuery(interval BQTimeInterval, stringResolutionSeconds ChartQueryResSeconds) *bigquery.Query {
	var query *bigquery.Query
	candlesTable := fmt.Sprintf("Digital_Asset_MarketData%s", os.Getenv("DATA_NAMESPACE"))
	query = bq.Query(`
	SELECT
		ARRAY_AGG(STRUCT('Time',
			time,
			'Price',
			price)) AS beprices
	FROM (
		SELECT
			ID symbol,
			Occurance_Time AS time,
			CAST(AVG(Price) AS FLOAT64) price,
			ROW_NUMBER() OVER (PARTITION BY ID, CAST(FLOOR(UNIX_SECONDS(Occurance_Time)/ (` + string(stringResolutionSeconds) + ` ))AS INT64 )
			ORDER BY
			Occurance_Time) AS row_num
		FROM
			api-project-901373404215.digital_assets.` + candlesTable + ` c
		WHERE
			Occurance_Time >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL ` + string(interval) + `)
		GROUP BY
			Occurance_Time,
			ID
		ORDER BY
			Occurance_Time 
	) AS test
	WHERE
		row_num = 1
		AND symbol IN UNNEST(@coins)
		`)
	return query

}

// QueryChartByInterval queries the chart data by interval
// Inputs:
// - Interva: String (1 DAY, 1 HOUR, 1 MINUTE)
// - TargetResolutionSeconds: Int as String (60, 300, 900, 3600, 14400, 86400)
// - UUID: String `uuid.New().String()`
// - Context: Context
// Returns the chart data
func (bq *BQStore) CategoriesQueryChartByInterval(interval BQTimeInterval, stringResolutionSeconds ChartQueryResSeconds, uuid string, dataType DictionaryCategory, categories []Categories, period string, ctxO context.Context) ([]TimeSeriesResultPG, error) {

	ctx, span := tracer.Start(ctxO, "CategoriesQueryChartByInterval")
	defer span.End()

	labels := make(map[string]string)

	labels["uuid"] = uuid
	span.SetAttributes(attribute.String("uuid", uuid))
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	labels["bigquery"] = "true"
	span.SetAttributes(attribute.Bool("bigquery", true))
	labels["subFunction"] = "CategoriesQueryChartByInterval"
	var tsResult []TimeSeriesResultPG

	for _, category := range categories {
		coins := category.Coins
		categoryId := category.ID
		query := bq.BuildCategoriesChartQuery(interval, stringResolutionSeconds)
		query.Parameters = []bigquery.QueryParameter{
			{
				Name:  "coins",
				Value: coins,
			},
		}
		job, err := query.Run(ctx)
		if err != nil {
			return tsResult, err
		}

		log.DebugL(labels, "CategoriesQueryChartByInterval: Categories Chart Query Job ID: %s", job.ID())
		span.SetAttributes(attribute.String("chart_by_interval_job_id", job.ID()))

		status, err := job.Wait(ctx)
		if err != nil {
			return tsResult, err
		}

		if err := status.Err(); err != nil {
			return tsResult, err
		}

		it, err := job.Read(ctx)
		if err != nil {
			return tsResult, err
		}

		span.AddEvent("CategoriesQueryChartByInterval: Query Chart By Interval BQ Query Complete")

		for {
			var tsObj = TimeSeriesResultPG{}
			tsObj.TargetResolutionSeconds, _ = strconv.Atoi(string(stringResolutionSeconds))
			tsObj.IsIndex = false
			err := it.Next(&tsObj)
			if err == iterator.Done {
				break
			}
			if err != nil {
				return tsResult, err
			}
			//SortChartDataPG(tsObj.Slice)
			tsObj.AssetType = "CATEGORY"
			tsObj.Symbol = categoryId
			tsObj.Interval = fmt.Sprintf("%s_%s", categoryId, period)

			tsResult = append(tsResult, tsObj)
		}
	}

	span.SetStatus(codes.Ok, "CategoriesQueryChartByInterval: Categories Chart Query By Interval BQ Completed")
	return tsResult, nil
}


// main

r.Handle("/build-categories-chart/{period}", oidcAuthMiddleware.Middleware(http.HandlerFunc(BuildCategoriesChartHandler))).Methods(http.MethodPost)
v1.HandleFunc("/get-categories-chart/{period}", BuildCategoriesChartHandler).Methods(http.MethodGet, http.MethodOptions)

func BuildChartHandler(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	var wg sync.WaitGroup
	g, ctx := errgroup.WithContext(r.Context())
	period := vars["period"]
	setResponseHeaders(w, 60)
	labels := make(map[string]string)
	ctx, span := tracer.Start(r.Context(), "BuildChartHandler")

	defer span.End()

	labels["period"] = period
	labels["function"] = "BuildChartHandler"
	labels["UUID"] = uuid.New().String()

	startTime := log.StartTimeL(labels, "BuildChartHandler")
	log.DebugL(labels, "BuildChartHandler: Chart Data Build Process Started at :: %s for Period :: %s", startTime, period)
	var categories []store.Categories

	g.Go(func() error {
		catResult, err := store.GetCategories(ctx)
		if err != nil {
			log.ErrorL(labels, "BuildCategoriesChartHandler: Error getting Categories Data from PG: %s", err)
			w.WriteHeader(http.StatusInternalServerError)
			span.SetStatus(codes.Error, err.Error())
			return errors.New("BuildCategoriesChartHandler: Error getting Categories Data from PG: " + err.Error())
		}
		log.EndTimeL(labels, "BuildCategoriesChartHandler: Finished Getting Categories Data from PG", startTime, nil)
		categories = catResult
		return nil
	})

	err := g.Wait() // Blocks till all go routines are done
	if err != nil { // If any of the go routines return an error
		log.ErrorL(labels, "BuildCategoriesChartHandler: WaitGroup Error %s", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal Server Error"))
		span.SetStatus(codes.Error, err.Error())
		return
	}

	var result []store.TimeSeriesResultPG
	var qErr error

	bqs, err := store.NewBQStore()
	if err != nil {
		// If an error occurs, the full context is canceled. Which causes all the first two Go Routines to cancel.
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// get chart data fro BQ by Interval
	switch period {
	case "24h":
		wg.Add(3)
		go func() {
			var res []store.TimeSeriesResultPG
			res, qErr = bqs.QueryChartByInterval(store.BQ_OneDay, store.ResSeconds_900, labels["UUID"], store.Ft, r.Context())
			result = append(result, res...)
			wg.Done()
		}()
		go func() {
			var res []store.TimeSeriesResultPG
			res, qErr = bqs.QueryChartByInterval(store.BQ_OneDay, store.ResSeconds_900, labels["UUID"], store.Nft, r.Context())
			result = append(result, res...)
			wg.Done()
		}()
		go func() {
			var res []store.TimeSeriesResultPG
			res, qErr = bqs.CategoriesQueryChartByInterval(store.BQ_OneDay, store.ResSeconds_900, labels["UUID"], store.Category, categories, period, r.Context())
			result = append(result, res...)
			wg.Done()
		}()
	case "7d":
		wg.Add(3)
		go func() {
			var res []store.TimeSeriesResultPG
			res, qErr = bqs.QueryChartByInterval(store.BQ_SevenDay, store.ResSeconds_14400, labels["UUID"], store.Ft, r.Context())
			result = append(result, res...)
			wg.Done()
		}()
		go func() {
			var res []store.TimeSeriesResultPG
			res, qErr = bqs.QueryChartByInterval(store.BQ_SevenDay, store.ResSeconds_14400, labels["UUID"], store.Nft, r.Context())
			result = append(result, res...)
			wg.Done()
		}()
		go func() {
			var res []store.TimeSeriesResultPG
			res, qErr = bqs.CategoriesQueryChartByInterval(store.BQ_SevenDay, store.ResSeconds_14400, labels["UUID"], store.Category, categories, period, r.Context())
			result = append(result, res...)
			wg.Done()
		}()
	case "30d":
		wg.Add(3)
		go func() {
			var res []store.TimeSeriesResultPG
			res, qErr = bqs.QueryChartByInterval(store.BQ_ThirtyDay, store.ResSeconds_43200, labels["UUID"], store.Ft, r.Context())
			result = append(result, res...)
			wg.Done()
		}()
		go func() {
			var res []store.TimeSeriesResultPG
			res, qErr = bqs.QueryChartByInterval(store.BQ_ThirtyDay, store.ResSeconds_43200, labels["UUID"], store.Nft, r.Context())
			result = append(result, res...)
			wg.Done()
		}()
		go func() {
			var res []store.TimeSeriesResultPG
			res, qErr = bqs.CategoriesQueryChartByInterval(store.BQ_ThirtyDay, store.ResSeconds_43200, labels["UUID"], store.Category, categories, period, r.Context())
			result = append(result, res...)
			wg.Done()
		}()
	case "1y":
		wg.Add(3)
		go func() {
			var res []store.TimeSeriesResultPG
			res, qErr = bqs.QueryChartByInterval(store.BQ_OneYear, store.ResSeconds_432000, labels["UUID"], store.Ft, r.Context())
			result = append(result, res...)
			wg.Done()
		}()
		go func() {
			var res []store.TimeSeriesResultPG
			res, qErr = bqs.QueryChartByInterval(store.BQ_OneYear, store.ResSeconds_432000, labels["UUID"], store.Nft, r.Context())
			result = append(result, res...)
			wg.Done()
		}()
		go func() {
			var res []store.TimeSeriesResultPG
			res, qErr = bqs.CategoriesQueryChartByInterval(store.BQ_OneYear, store.ResSeconds_432000, labels["UUID"], store.Category, categories, period, r.Context())
			result = append(result, res...)
			wg.Done()
		}()
	case "max":
		wg.Add(3)
		go func() {
			var res []store.TimeSeriesResultPG
			res, qErr = bqs.QueryChartByInterval(store.BQ_Max, store.ResSeconds_1296000, labels["UUID"], store.Ft, r.Context())
			result = append(result, res...)
			wg.Done()
		}()
		go func() {
			var res []store.TimeSeriesResultPG
			res, qErr = bqs.QueryChartByInterval(store.BQ_Max, store.ResSeconds_1296000, labels["UUID"], store.Nft, r.Context())
			result = append(result, res...)
			wg.Done()
		}()
		go func() {
			var res []store.TimeSeriesResultPG
			res, qErr = bqs.CategoriesQueryChartByInterval(store.BQ_Max, store.ResSeconds_1296000, labels["UUID"], store.Category, categories, period, r.Context())
			result = append(result, res...)
			wg.Done()
		}()
	default:
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	wg.Wait()
	if qErr != nil {
		log.Error("%s", qErr)
		w.WriteHeader(http.StatusInternalServerError)
	}

	err = store.InsertNomicsChartData(r.Context(), period, result)

	if err != nil {
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	log.EndTimeL(labels, "Chart Data Build ", startTime, nil)

	w.Write([]byte("ok"))

}


if f.Symbol != "" {
	allFundamentals = append(allFundamentals, f)
}


/*
- We need this process to fill the data for Categories Charts.
- The solution to get the data for Categories Charts is
	- We must fetch the data for one year then we need to filter this data by 24 hour.
	- After we get it we need to get the Max price for 24 hour in this case we will insure that the max value only will return in 24 hour interval
	- Then it will be implemented for all 24 hour interval in one year so this will contains 365 prices for entire Year for each assets.
	- We need to loop over this result and do our calculation for each category in this case we will have price for all categories for one year ago.
	- We need to insert the result to new table for the first time and then we can build the Categories chart for 7 days from our historical result.
*/