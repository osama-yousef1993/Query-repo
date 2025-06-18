
// firestore
// Add topics with all its data to FS
func SaveNewsTopicsCategories(ctx0 context.Context, topics []services.TopicCategories) {

	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "SaveNewsTopics")
	defer span.End()
	span.AddEvent("Start Insert Topics To FS")
	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "category_news")
	for _, topic := range topics {
		fs.Collection(collectionName).Doc(topic.Name).Set(ctx, map[string]interface{}{
			"categoryName": topic.Name,
		}, firestore.MergeAll)
		for _, content := range topic.ContentProps {
			doc := make(map[string]interface{})
			doc["topicName"] = content.TopicName
			doc["topicUrl"] = content.TopicURL
			doc["isAsset"] = content.IsAsset
			doc["slug"] = content.Slug
			//if there is no natural id dont store the article
			fs.Collection(collectionName).Doc(topic.Name).Collection("topics").Doc(content.TopicName).Set(ctx, doc, firestore.MergeAll)
		}
	}
	span.SetStatus(otelCodes.Ok, "Success")

}

// topic 

// Trending Topic Tags
type TrendingTopics struct {
	TopicName string `json:"topicName" firestore:"topicName"` // Topic NAme will display in FE
	Slug      string `json:"slug" firestore:"slug"`           // Topic Slug Will use for the news topic page
	TopicURL  string `json:"topicUrl" firestore:"topicUrl"`   // Topic Slug Will use for the news topic page
	IsAsset   bool   `json:"isAsset" firestore:"isAsset"`     // IsAsset flag will use it to determine if the Topic is an assets Topic or normal news Topic
}

type Categories struct {
	Category string   `json:"category"`
	Topics   []string `json:"topics"`
}

type TopicCategories struct {
	Name         string           `json:"name" firestore:"categoryName"`
	ContentProps []TrendingTopics `json:"contentProps"`
}

func BuildTopicCategories(ctx0 context.Context) *[]TopicCategories {
	jsonData := `[
		{
			"category": "Stablecoins",
			"topics": [
				"Binance USD",
				"TrueUSD",
				"USDC",
				"Dai",
				"USDT",
				"Tether"
			]
		},
		{
			"category": "Protocol Tokens",
			"topics": [
				"XRP",
				"Cardano",
				"Dogecoin",
				"TRON",
				"Litecoin",
				"Polkadot",
				"Polygon",
				"Dai",
				"Shiba Inu",
				"Avalanche",
				"Uniswap",
				"Chainlink",
				"Cosmos",
				"OKB",
				"Filecoin",
				"Hedera",
				"Aptos",
				"VeChain",
				"Algorand",
				"ApeCoin",
				"Optimism",
				"Tezos",
				"Solana"
			]
		},
		{
			"category": "Industry",
			"topics": [
				"Coinbase",
				"FTX",
				"Binance",
				"Kraken",
				"Digital Currency Group",
				"BlockFi",
				"Circle",
				"Robinhood",
				"PayPal",
				"Gemini"
			]
		},
		{
			"category": "Miners",
			"topics": [
				"Marathon",
				"Blockstream",
				"Riot",
				"Stronghold Digital"
			]
		},
		{
			"category": "Venture",
			"topics": [
				"Seven Seven Six",
				"A16Z",
				"Polychain",
				"Pantera",
				"Sequoia",
				"Union Square Ventures",
				"Multicoin",
				"Dragonfly"
			]
		},
		{
			"category": "Enterprise Blockchain",
			"topics": [
				"JPMorgan",
				"HSBC",
				"Santander",
				"Goldman Sachs",
				"IBM",
				"Microsoft"
			]
		}
	]`

	var items []Categories
	var topicCategories []TopicCategories
	// Unmarshal the JSON data into the slice
	err := json.Unmarshal([]byte(jsonData), &items)
	if err != nil {
		fmt.Println("Error parsing JSON:", err)
		return nil
	}
	for _, item := range items {
		var topicCategory TopicCategories
		topicCategory.Name = item.Category
		for _, topic := range item.Topics {
			res, _ := GetTopicsTagsListByName(ctx0, topic)
			topicCategory.ContentProps = append(topicCategory.ContentProps, *res)
		}
		fmt.Printf("%s%v", item.Category, item.Topics)
		topicCategories = append(topicCategories, topicCategory)
	}
	file, _ := json.MarshalIndent(topicCategories, " ", "")
	_ = os.WriteFile("topicCategories.json", file, 0644)
	return &topicCategories

}

// Get all trending topics from Firestore
func GetTopicsTagsListByName(ctx0 context.Context, name string) (*TrendingTopics, error) {
	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "GetTopicsTagsList")
	defer span.End()

	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "news")
	// Get all Topics from Firestore
	iter := fs.Collection(collectionName).Where("topicName", "==", name).Documents(ctx)
	span.AddEvent("Start Getting Topics Tags List from FS")

	var topicsTag TrendingTopics
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			log.Error("Error Getting Topics Tags List from FS: %s", err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}

		err = doc.DataTo(&topicsTag)
		if err != nil {
			log.Error("Error Getting Topics Tags Lista from FS: %s", err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}
	}

	span.SetStatus(otelCodes.Ok, "Success")
	return &topicsTag, nil
}

// get Topic with it's connected articles from FS using slug
func GetAllNewsTopicCategories(ctx0 context.Context) ([]byte, error) {
	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "GetNewsTopics")
	defer span.End()

	sectionCollection := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "category_news")

	// get topic data using slug
	dbSnap := fs.Collection(sectionCollection).Documents(ctx)

	span.AddEvent("Start Get News Topics Data from FS")

	var topicsCategories []TopicCategories
	for {
		var topicsCategory TopicCategories
		var topics []TrendingTopics
		doc, err := dbSnap.Next()

		if err == iterator.Done {
			break
		}

		if err := doc.DataTo(&topicsCategory); err != nil {
			log.Error("Error Getting News Topics Data from FS: %s", err)
			span.AddEvent(fmt.Sprintf("Error Getting News Topics Data from FS: %s", err))
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}

		// get topic articles
		db := fs.Collection(sectionCollection).Doc(topicsCategory.Name).Collection("topics").Documents(ctx)

		for {
			var topic TrendingTopics
			doc, err := db.Next()

			if err == iterator.Done {
				break
			}

			if err := doc.DataTo(&topic); err != nil {
				log.Error("Error Getting Article Data from FS: %s", err)
				span.AddEvent(fmt.Sprintf("Error Getting Article Data from FS: %s", err))
				span.SetStatus(otelCodes.Error, err.Error())
				return nil, err
			}

			topics = append(topics, topic)
		}

		topicsCategory.ContentProps = topics

		topicsCategories = append(topicsCategories, topicsCategory)
	}

	span.AddEvent("Modify Articles to be only 8 Articles for each Topic")
	span.SetStatus(otelCodes.Ok, "Success")
	result, err := json.Marshal(topicsCategories)
	if err != nil {
		log.Error("Error : %s", err)
		span.SetStatus(otelCodes.Error, err.Error())
		span.AddEvent(fmt.Sprintf("Error : %s", err))
		return nil, err
	}
	return result, nil
}


// main 
v1.HandleFunc("/build-topic-category/", GetNewsTopicCategories).Methods(http.MethodGet, http.MethodOptions)
v1.HandleFunc("/topic-category/", GetAllNewsTopicCategories).Methods(http.MethodGet, http.MethodOptions)



// Get All section With Articles for Learn Tab
func GetAllNewsTopicCategories(w http.ResponseWriter, r *http.Request) {
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

	result, err := services.GetAllNewsTopicCategories(ctx)

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

// Get All categories With related News Topic
func GetNewsTopicCategories(w http.ResponseWriter, r *http.Request) {
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

	res := services.BuildTopicCategories(ctx)

	store.SaveNewsTopicsCategories(ctx, *res)

	log.EndTimeL(labels, "News Topics Data", startTime, nil)
	span.SetStatus(codes.Ok, "News Topics Data")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))

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

//firestore

// Add topics with all its data to FS
func SaveNewsTopicsCategories(ctx0 context.Context, topics []services.TopicCategories) {

	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "SaveNewsTopics")
	defer span.End()
	span.AddEvent("Start Insert Topics To FS")
	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "category_news")
	for _, topic := range topics {
		fs.Collection(collectionName).Doc(topic.CategoryName).Set(ctx, map[string]interface{}{
			"categoryName": topic.CategoryName,
		}, firestore.MergeAll)
		for _, content := range topic.CategoryTopics {
			doc := make(map[string]interface{})
			doc["topicName"] = content.TopicName
			doc["topicUrl"] = content.TopicURL
			doc["isAsset"] = content.IsAsset
			doc["slug"] = content.Slug
			//if there is no natural id dont store the article
			// use NewDoc when we need to add new topic to categories
			fs.Collection(collectionName).Doc(topic.CategoryName).Collection("topics").NewDoc().Set(ctx, doc, firestore.MergeAll)
		}
	}
	span.SetStatus(otelCodes.Ok, "Success")

}


type TopicCategories struct {
	CategoryName   string           `json:"name" firestore:"categoryName"`
	CategoryTopics []TrendingTopics `json:"contentProps" firestore:"topics"`
}

// get All category from FS and build the topics data for it
func BuildCategoriesTopics(ctx0 context.Context) ([]TopicCategories, error) {

	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "BuildNewsTopics")
	defer span.End()

	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "category_news")

	dbSnap := fs.Collection(collectionName).Documents(ctx)

	var topicCategories []TopicCategories
	for {
		var topicCategory TopicCategories
		var topics []TrendingTopics
		doc, err := dbSnap.Next()

		if err == iterator.Done {
			break
		}
		if err := doc.DataTo(&topicCategory); err != nil {
			log.Error("Error Getting News Topics Data from FS: %s", err)
			span.AddEvent(fmt.Sprintf("Error Getting News Topics Data from FS: %s", err))
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}
		// get topic articles
		db := fs.Collection(collectionName).Doc(topicCategory.CategoryName).Collection("topics").Documents(ctx)

		for {
			var topic TrendingTopics
			doc, err := db.Next()

			if err == iterator.Done {
				break
			}

			if err := doc.DataTo(&topic); err != nil {
				log.Error("Error Getting Article Data from FS: %s", err)
				span.AddEvent(fmt.Sprintf("Error Getting Article Data from FS: %s", err))
				span.SetStatus(otelCodes.Error, err.Error())
				return nil, err
			}
			topicResult, _ := GetTopicsTagsByName(ctx0, topic.TopicName)
			topicResult.DocId = doc.Ref.ID
			topics = append(topics, *topicResult)
		}
		topicCategory.CategoryTopics = topics
		topicCategories = append(topicCategories, topicCategory)
	}

	return topicCategories, nil

}

// Get all topics by name topic
func GetTopicsTagsByName(ctx0 context.Context, name string) (*TrendingTopics, error) {
	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "GetTopicsTagsList")
	defer span.End()

	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "news")
	// Get all Topics from Firestore
	iter := fs.Collection(collectionName).Where("topicName", "==", name).Documents(ctx)
	span.AddEvent("Start Getting Topics Tags List from FS")

	var topicsTag TrendingTopics
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			log.Error("Error Getting Topics Tags List from FS: %s", err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}

		err = doc.DataTo(&topicsTag)
		if err != nil {
			log.Error("Error Getting Topics Tags Lista from FS: %s", err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}

	}

	span.SetStatus(otelCodes.Ok, "Success")
	return &topicsTag, nil
}

// get all News Topics Categories
func GetAllNewsTopicCategories(ctx0 context.Context) ([]byte, error) {
	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "GetNewsTopics")
	defer span.End()

	sectionCollection := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "category_news")

	dbSnap := fs.Collection(sectionCollection).Documents(ctx)

	span.AddEvent("Start Get News Topics Data from FS")

	var topicsCategories []TopicCategories
	for {
		var topicsCategory TopicCategories
		var topics []TrendingTopics
		doc, err := dbSnap.Next()

		if err == iterator.Done {
			break
		}

		if err := doc.DataTo(&topicsCategory); err != nil {
			log.Error("Error Getting News Topics Data from FS: %s", err)
			span.AddEvent(fmt.Sprintf("Error Getting News Topics Data from FS: %s", err))
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}

		// get categories topics
		db := fs.Collection(sectionCollection).Doc(topicsCategory.CategoryName).Collection("topics").Documents(ctx)

		for {
			var topic TrendingTopics
			doc, err := db.Next()

			if err == iterator.Done {
				break
			}

			if err := doc.DataTo(&topic); err != nil {
				log.Error("Error Getting Article Data from FS: %s", err)
				span.AddEvent(fmt.Sprintf("Error Getting Article Data from FS: %s", err))
				span.SetStatus(otelCodes.Error, err.Error())
				return nil, err
			}

			topics = append(topics, topic)
		}

		topicsCategory.CategoryTopics = topics

		topicsCategories = append(topicsCategories, topicsCategory)
	}

	span.AddEvent("Modify Articles to be only 8 Articles for each Topic")
	span.SetStatus(otelCodes.Ok, "Success")
	result, err := json.Marshal(topicsCategories)
	if err != nil {
		log.Error("Error : %s", err)
		span.SetStatus(otelCodes.Error, err.Error())
		span.AddEvent(fmt.Sprintf("Error : %s", err))
		return nil, err
	}
	return result, nil
}

type Categories struct {
	Category string   `json:"category"`
	Topics   []string `json:"topics"`
}

func BuildTopicCategories(ctx0 context.Context) (*[]TopicCategories, error) {
	jsonData := `[
		{
			"category": "Stablecoins",
			"topics": [
				"Binance USD",
				"TrueUSD",
				"USDC",
				"Dai",
				"USDT",
				"Tether"
			]
		},
		{
			"category": "Protocol Tokens",
			"topics": [
				"XRP",
				"Cardano",
				"Dogecoin",
				"TRON",
				"Litecoin",
				"Polkadot",
				"Polygon",
				"Dai",
				"Shiba Inu",
				"Avalanche",
				"Uniswap",
				"Chainlink",
				"Cosmos",
				"OKB",
				"Filecoin",
				"Hedera",
				"Aptos",
				"VeChain",
				"Algorand",
				"ApeCoin",
				"Optimism",
				"Tezos",
				"Solana"
			]
		},
		{
			"category": "Industry",
			"topics": [
				"Coinbase",
				"FTX",
				"Binance",
				"Kraken",
				"Digital Currency Group",
				"BlockFi",
				"Circle",
				"Robinhood",
				"PayPal",
				"Gemini"
			]
		},
		{
			"category": "Miners",
			"topics": [
				"Marathon",
				"Blockstream",
				"Riot",
				"Stronghold Digital"
			]
		},
		{
			"category": "Venture",
			"topics": [
				"Seven Seven Six",
				"A16Z",
				"Polychain",
				"Pantera",
				"Sequoia",
				"Union Square Ventures",
				"Multicoin",
				"Dragonfly"
			]
		},
		{
			"category": "Enterprise Blockchain",
			"topics": [
				"JPMorgan",
				"HSBC",
				"Santander",
				"Goldman Sachs",
				"IBM",
				"Microsoft"
			]
		}
	]`

	var items []Categories
	var topicCategories []TopicCategories
	// Unmarshal the JSON data into the slice
	err := json.Unmarshal([]byte(jsonData), &items)
	if err != nil {
		fmt.Println("Error parsing JSON:", err)
		return nil, err
	}
	for _, item := range items {
		var topicCategory TopicCategories
		topicCategory.CategoryName = item.Category
		for _, topic := range item.Topics {
			res, _ := GetTopicsTagsByName(ctx0, topic)
			topicCategory.CategoryTopics = append(topicCategory.CategoryTopics, *res)
		}
		fmt.Printf("%s%v", item.Category, item.Topics)
		topicCategories = append(topicCategories, topicCategory)
	}
	file, _ := json.MarshalIndent(topicCategories, " ", "")
	_ = os.WriteFile("topicCategories.json", file, 0644)
	return &topicCategories, nil

}
