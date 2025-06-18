v1.HandleFunc("/build-top-75", BuildTop75Assets).Methods(http.MethodGet, http.MethodOptions)


func BuildTop75Assets(w http.ResponseWriter, r *http.Request) {
	setResponseHeaders(w, 300)
	labels := make(map[string]string)

	ctx, span := tracer.Start(r.Context(), "BuildTop75Assets")

	defer span.End()

	labels["UUID"] = uuid.New().String()
	labels["function"] = "BuildTop75Assets"
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTime("Build Top 75 Assets")

	topAssets, err := store.GetTop75Assets(ctx)
	// topics, err := services.GetNewsTopics(ctx)

	// allTopics := services.BuildNewsTopicWithTop75Assets(ctx, topics, topAssets)

	result := services.BuildTop75AssetsArticles(ctx, topAssets)

	if err != nil {
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		span.SetStatus(codes.Error, err.Error())
		return
	}

	res, err := json.Marshal(result)
	if err != nil {
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		span.SetStatus(codes.Error, err.Error())
		return
	}

	log.EndTimeL(labels, "Build Top 75 Assets", startTime, nil)
	span.SetStatus(codes.Ok, "Build Top 75 Assets")
	w.WriteHeader(http.StatusOK)
	w.Write(res)

}
