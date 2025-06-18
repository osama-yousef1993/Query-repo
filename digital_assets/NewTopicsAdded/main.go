v1.HandleFunc("/test", InsertTopics).Methods(http.MethodGet, http.MethodOptions)
v1.HandleFunc("/fix-order", FixNewsTopics).Methods(http.MethodGet, http.MethodOptions)
v1.HandleFunc("/all-topics", GetNewsTopics).Methods(http.MethodGet, http.MethodOptions)


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

	store.SaveNewsTopic(ctx)

	log.EndTimeL(labels, "InsertTopics ", startTime, nil)
	span.SetStatus(codes.Ok, "InsertTopics")
	w.WriteHeader(200)
	w.Write([]byte("ok"))

}

func FixNewsTopics(w http.ResponseWriter, r *http.Request) {
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

	store.FixNewsTopic(ctx)

	log.EndTimeL(labels, "InsertTopics ", startTime, nil)
	span.SetStatus(codes.Ok, "InsertTopics")
	w.WriteHeader(200)
	w.Write([]byte("ok"))

}

// Get All section With Articles for Learn Tab
func GetNewsTopics(w http.ResponseWriter, r *http.Request) {
	// update each 5 min
	setResponseHeaders(w, 300)
	labels := make(map[string]string)

	ctx, span := tracer.Start(r.Context(), "GetNewsTopics")

	defer span.End()

	labels["UUID"] = uuid.New().String()
	labels["function"] = "GetNewsTopics"
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTime("Get News Topics Data")

	result, err := services.GetNewsTopicsData(ctx)

	if err != nil {
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		span.SetStatus(codes.Error, err.Error())
		return
	}

	log.EndTimeL(labels, "News Topics Data", startTime, nil)
	span.SetStatus(codes.Ok, "News Topics Data")
	w.WriteHeader(http.StatusOK)
	w.Write(result)

}
