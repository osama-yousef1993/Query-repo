v1.HandleFunc("/featured-categories-test", GetFeaturedCategoriesTest).Methods(http.MethodGet, http.MethodOptions)

// Will fetch  Featured Categories from FS
func GetFeaturedCategoriesTest(w http.ResponseWriter, r *http.Request) {
	// update each 30 sec
	setResponseHeaders(w, 30)

	labels := make(map[string]string)

	span := trace.SpanFromContext(r.Context())

	defer span.End()

	labels["UUID"] = uuid.New().String()
	labels["function"] = "GetFeaturedCategories"
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTime("Get Featured Categories")

	// Will returns the ID and name for all Featured Categories
	categories, err := store.GetFeaturedCategoriesTest(r.Context())

	if err != nil {
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		span.SetStatus(codes.Error, err.Error())
		return
	}

	store.UpdateIsCategoriesLink(r.Context(), categories)
	log.EndTimeL(labels, "Featured Categories Data", startTime, nil)
	span.SetStatus(codes.Ok, "Featured Categories Data")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(""))
}