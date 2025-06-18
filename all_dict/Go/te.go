GetChainsList



// Will fetch all NFT Chains from firestore
func GetNFTChains(w http.ResponseWriter, r *http.Request) {
	setResponseHeaders(w, 60)
	labels := make(map[string]string)

	ctx, span := tracer.Start(r.Context(), "GetNFTChains")

	defer span.End()

	labels["UUID"] = uuid.New().String()
	labels["function"] = "GetNFTChains"
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTime("Get NFTs Chains List")

	// Get All NFTs Chains List from FS
	data, err := store.GetChainsList(ctx)
	if data == nil && err == nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusNotFound)

	}
	span.SetStatus(codes.Ok, "GetNFTChains")
	log.EndTimeL(labels, "GetNFTChains ", startTime, nil)
	w.WriteHeader(200)
	w.Write(data)

}

// Get NFT Prices Data
// Searches the NFT table with the provided query and pagination.
func GetNFTPrices(w http.ResponseWriter, r *http.Request) {
	setResponseHeaders(w, 300)
	// updated each 5 minute
	labels := make(map[string]string)
	ctx, span := tracer.Start(r.Context(), "GetNFTPrices")
	defer span.End()

	labels["function"] = "GetNFTPrices"
	labels["UUID"] = uuid.New().String()
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()
	span.SetAttributes(attribute.String("UUID", labels["UUID"]))
	startTime := log.StartTimeL(labels, "NFT Price Table")
	paginate := store.Paginate{} //captures the pagination params.
	limit := html.EscapeString(r.URL.Query().Get("limit"))
	pageNum := html.EscapeString(r.URL.Query().Get("pageNum"))
	paginate.SortBy = html.EscapeString(r.URL.Query().Get("sortBy"))
	paginate.Direction = html.EscapeString(r.URL.Query().Get("direction"))
	category := html.EscapeString(r.URL.Query().Get("category"))
	// We can use if we need to search for specific NFT
	query := html.EscapeString(r.URL.Query().Get("query"))
	// Will use chainID if we need to search about specific NFT using Chains
	chainID := html.EscapeString(r.URL.Query().Get("chain_id"))
	var limitError error
	var pageError error
	paginate.Limit, limitError = strconv.Atoi(limit)
	paginate.PageNum, pageError = strconv.Atoi(pageNum)
	dictionaryCategory, dictionaryErr := store.GetDictionaryCategoryByString(ctx, category)

	if limitError != nil || pageError != nil || dictionaryErr != nil { //throw an error if pagination args are improper.
		log.ErrorL(labels, "Invalid pagination values")
		span.SetStatus(codes.Error, "Invalid pagination values")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var result []byte
	var err error

	if chainID != "" {
		// If chainID exists in query params, It will be used to searching for a specific chain using NFT query.
		// this means the user needs to search for nfts using a specific chain
		paginate.ChainID = chainID
		// The SearchTermByChains function will build the result using the NFTs that exist in the specified chain
		result, err = store.SearchTermByChains(ctx, query, dictionaryCategory, paginate)
	} else {
		result, err = store.SearchNFTTerm(ctx, query, dictionaryCategory, paginate)
	}

	if result == nil && err == nil {
		log.Error("%s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if err != nil {
		log.Error("%s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	reader := bytes.NewReader(result)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(result)))
	store.ConsumeTime("Get NFT Prices Data", startTime, nil)
	span.SetStatus(codes.Ok, "GetNFTPrices")
	w.WriteHeader(http.StatusOK)
	io.Copy(w, reader)
}
