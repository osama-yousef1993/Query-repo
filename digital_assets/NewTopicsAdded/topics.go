

// get Topic with it's connected articles from FS using slug
func GetNewsTopics(ctx0 context.Context) (map[string]Topic, error) {
	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "GetNewsTopics")
	defer span.End()

	sectionCollection := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "news")

	// get topic data using slug
	dbSnap := fs.Collection(sectionCollection).Documents(ctx)

	span.AddEvent("Start Get News Topics Data from FS")

	var topics = make(map[string]Topic)
	for {
		var topic Topic
		doc, err := dbSnap.Next()

		if err == iterator.Done {
			break
		}

		if err := doc.DataTo(&topic); err != nil {
			log.Error("Error Getting News Topics Data from FS: %s", err)
			span.AddEvent(fmt.Sprintf("Error Getting News Topics Data from FS: %s", err))
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}
		topics[topic.TopicName] = topic

	}

	span.AddEvent("Modify Articles to be only 8 Articles for each Topic")
	span.SetStatus(otelCodes.Ok, "Success")
	return topics, nil
}

// get Topic with it's connected articles from FS using slug
func FixNewsTopics(ctx0 context.Context) ([]Topic, error) {
	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "GetNewsTopics")
	defer span.End()

	sectionCollection := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "news")

	// get topic data using slug
	dbSnap := fs.Collection(sectionCollection).Documents(ctx)

	span.AddEvent("Start Get News Topics Data from FS")

	var topics []Topic
	for {
		var topic Topic
		doc, err := dbSnap.Next()

		if err == iterator.Done {
			break
		}

		if err := doc.DataTo(&topic); err != nil {
			log.Error("Error Getting News Topics Data from FS: %s", err)
			span.AddEvent(fmt.Sprintf("Error Getting News Topics Data from FS: %s", err))
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}
		topics = append(topics, topic)

	}

	span.AddEvent("Modify Articles to be only 8 Articles for each Topic")
	span.SetStatus(otelCodes.Ok, "Success")
	return topics, nil
}






// build Topics data from BQ and config table in Rowy
func GetNewsTopicsData(ctx0 context.Context) ([]byte, error) {
	ctx, span := tracer.Start(ctx0, "GetNewsTopicsData")
	defer span.End()
	span.AddEvent("Start Get News Topics Data")
	// get the topic with all it's articles using slug
	topics, err := GetNewsTopics(ctx)

	if err != nil {
		log.Error("Error Getting News Topics from FS:  %s", err)
		span.AddEvent(fmt.Sprintf("Error Getting News Topics from FS: %s", err))
		span.SetStatus(otelCodes.Error, err.Error())
		return nil, err
	}

	file, _ := json.MarshalIndent(topics, " ", "")
	_ = os.WriteFile("topics.json", file, 0644)

	result, err := json.Marshal(topics)
	if err != nil {
		log.Error("Error : %s", err)
		span.SetStatus(otelCodes.Error, err.Error())
		span.AddEvent(fmt.Sprintf("Error : %s", err))
		return nil, err
	}

	span.SetStatus(otelCodes.Ok, "Success")
	return result, nil

}
