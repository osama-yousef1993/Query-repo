
community.HandleFunc("/faq", GetFrequentlyAskedQuestions).Methods(http.MethodGet, http.MethodOptions)


// Get Frequently Asked Questions Data from FS
func GetFrequentlyAskedQuestions(w http.ResponseWriter, r *http.Request) {
	setResponseHeaders(w, 300)
	labels := make(map[string]string)

	ctx, span := tracer.Start(r.Context(), "GetFrequentlyAskedQuestions")

	defer span.End()

	labels["UUID"] = uuid.New().String()
	labels["function"] = "GetFrequentlyAskedQuestions"
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTime("Get Frequently Asked Questions Data")

	result, err := store.GetCommunityPageFrequentlyAskedQuestions(ctx)

	if err != nil {
		log.ErrorL(labels, "%s", err)
		w.WriteHeader(http.StatusInternalServerError)
		span.SetStatus(codes.Error, err.Error())
		return
	}

	log.EndTimeL(labels, "Frequently Asked Questions Data", startTime, nil)
	span.SetStatus(codes.Ok, "Frequently Asked Questions Data")
	w.WriteHeader(http.StatusOK)
	w.Write(result)

}



type FAQ struct {
	Question string `json:"question" firestore:"question"` // It will present the Question for FAQ
	Answer   string `json:"answer" firestore:"answer"`     // It will present the Answer for FAQ Question
}

// Get Community Page FAQ Data from FS 
func GetCommunityPageFrequentlyAskedQuestions(ctx0 context.Context) ([]byte, error) {
	fs := GetFirestoreClient()

	ctx, span := tracer.Start(ctx0, "GetCommunityPageFrequentlyAskedQuestions")

	defer span.End()

	span.AddEvent("Start Getting Frequently Asked Questions")

	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "fqa_section")

	db := fs.Collection(collectionName).Documents(ctx)
	var faqs []FAQ
	for {
		var faq FAQ

		doc, err := db.Next()
		if err == iterator.Done {
			break
		}

		if err := doc.DataTo(&faq); err != nil {
			log.Error("Error Community Page Frequently Asked Questions Data from FS: %s", err)
			span.AddEvent(fmt.Sprintf("Error Community Page Frequently Asked Questions Data from FS: %s", err))
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}
		faqs = append(faqs, faq)
	}
	result, err := BuildJsonResponse(ctx, faqs, "Community Page Frequently Asked Questions Data")
	if err != nil {
		log.Error("Error Community Page Frequently Asked Questions Data to Json: %s", err)
		span.SetStatus(otelCodes.Error, err.Error())
		return nil, err
	}

	return result, nil
}