
v1.HandleFunc("/test", InsertTopics).Methods(http.MethodGet, http.MethodOptions)


func InsertTopics(w http.ResponseWriter, r *http.Request) {
	setResponseHeaders(w, 60)

	labels := make(map[string]string)
	ctx, span := tracer.Start(r.Context(), "InsertTopics")
	defer span.End()

	labels["UUID"] = uuid.New().String()
	labels["function"] = "InsertTopics"
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTime("Insert Topics Data")
	result, err := services.BuildNewsTopicsCategories(ctx)
	if err != nil {
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		span.SetStatus(codes.Error, err.Error())
		return
	}

	store.SaveNewsTopic(ctx, result)

	log.EndTimeL(labels, "InsertTopics ", startTime, nil)
	span.SetStatus(codes.Ok, "InsertTopics")
	w.WriteHeader(200)
	w.Write([]byte("ok"))

}