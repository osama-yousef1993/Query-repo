
// Queries all chart data for a given asset or nft, and the assets status and returns the results.
func GetChartData(ctxO context.Context, interval string, symbol string, period string, assetsType string) ([]byte, error) {

	ctx, span := tracer.Start(ctxO, "PGUpdateChartData")
	defer span.End()
	startTime := StartTime("Charts Data Query")

	pg := PGConnect()

	var timeSeriesResults []TimeSeriesResultPG
	var result TimeSeriesResultPG
	var query string
	// we will remove it when we start use the new endpoint
	if assetsType == "" {
		query = `
		select
			is_index, 
			source, 
			target_resolution_seconds, 
			prices,
			tm_interval,
			symbol,
			status  
			from public.getChartData('` + interval + `','` + symbol + `')`
	} else {
		query = `
		select
			is_index, 
			source, 
			target_resolution_seconds, 
			prices,
			tm_interval,
			symbol,
			status  
			from public.getFTNFTChartData('` + interval + `','` + symbol + `', '` + assetsType + `')`
	}

	queryResult, err := pg.QueryContext(ctx, query)

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		ConsumeTime("Charts Data Query", startTime, err)
		return nil, err
	}
	span.AddEvent("Query Executed")
	defer queryResult.Close()

	for queryResult.Next() {
		var timeSeriesResult TimeSeriesResultPG
		err := queryResult.Scan(&timeSeriesResult.IsIndex, &timeSeriesResult.Source, &timeSeriesResult.TargetResolutionSeconds, (*slicePGResult)(&timeSeriesResult.Slice), &timeSeriesResult.Interval, &timeSeriesResult.Symbol, &timeSeriesResult.Status)

		if err != nil {
			ConsumeTime("Charts Data Query Scan", startTime, err)
			return nil, err
		}
		var newSlice []FESlicePG
		for _, sliceObject := range timeSeriesResult.Slice {
			var slice FESlicePG

			slice.Time = sliceObject.Time
			slice.AvgClose = sliceObject.AvgClose
			newSlice = append(newSlice, slice)
		}
		timeSeriesResult.Slice = nil
		timeSeriesResult.FESlice = newSlice
		timeSeriesResults = append(timeSeriesResults, timeSeriesResult)
	}

	ConsumeTime("Charts Data Query", startTime, nil)

	//if data is returned from the query run it through the filter
	if len(timeSeriesResults) > 0 {
		result = FilterChartData(ctx, timeSeriesResults, period, interval)
	}
	span.SetStatus(codes.Ok, "Charts Data Query")

	result.Source = data_source

	b, err := json.Marshal(result)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		ConsumeTime("Charts Data Query", startTime, err)
		return nil, err
	}
	
	return b, nil
}





func GetChartHandler(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)

	symbol := vars["symbol"]
	period := vars["period"]

	labels := make(map[string]string)
	setResponseHeaders(w, 60)
	span := trace.SpanFromContext(r.Context())

	defer span.End()

	labels["period"] = period
	labels["symbol"] = symbol
	labels["function"] = "GetChartHandler"
	labels["UUID"] = uuid.New().String()
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()
	span.SetAttributes(attribute.String("period", period))
	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTimeL(labels, "GetChartHandler")

	interval := fmt.Sprintf("%s_%s", symbol, period)

	// retrieve chart data by interval from PG table
	result, err := store.GetChartData(r.Context(), interval, symbol, period, "")
	if result == nil && err == nil {
		span.SetStatus(codes.Error, "No Data Found")
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		log.Error("%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	store.ConsumeTime("Get Chart", startTime, nil)
	log.EndTime("GetChartData", startTime, nil)
	span.SetStatus(codes.Ok, "GetChartData")
	w.Write(result)
}


// Get Chart Data for NFT or FT
func GetFTNFTChartHandler(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)

	symbol := vars["symbol"]
	period := vars["period"]
	labels := make(map[string]string)
	setResponseHeaders(w, 60)
	span := trace.SpanFromContext(r.Context())

	defer span.End()

	labels["period"] = period
	labels["symbol"] = symbol
	labels["function"] = "GetFTNFTChartHandler"
	labels["UUID"] = uuid.New().String()
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()
	span.SetAttributes(attribute.String("period", period))
	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTimeL(labels, "GetFTNFTChartHandler")
	// assetType will determine what data type you need to return for chart NFT or FT.
	assetsType := html.EscapeString(r.URL.Query().Get("assetsType"))
	interval := fmt.Sprintf("%s_%s", symbol, period)

	// retrieve chart data by interval and Assets Type from PG table
	result, err := store.GetChartData(r.Context(), interval, symbol, period, assetsType)

	if result == nil && err == nil {
		span.SetStatus(codes.Error, "No Data Found")
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		log.Error("%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	store.ConsumeTime("Get FT NFT Chart Data", startTime, nil)
	log.EndTime("GetFTNFTChartHandler", startTime, nil)
	span.SetStatus(codes.Ok, "GetFTNFTChartHandler")
	w.Write(result)
}