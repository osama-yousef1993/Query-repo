chartService         = rfServices.NewChartServices(db)

chartService)

v2.HandleFunc("/chart/{period}/{symbol}", microservices.GetChartData).Methods(http.MethodGet, http.MethodOptions)



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
	log.EndTime("Get FT NFT Chart Data", startTime, nil)
	log.EndTime("GetFTNFTChartHandler", startTime, nil)
	span.SetStatus(codes.Ok, "GetFTNFTChartHandler")
	w.Write(result)
}
