type Topic struct {
	TopicName          string             `json:"topicName" firestore:"topicName"`                   // Topic Name
	BertieTag          string             `json:"bertieTag" firestore:"bertieTag"`                   // Bertie Tag we will use it to fetch all articles related to the topic
	Description        string             `json:"description" firestore:"description"`               // topic Description
	IsTrending         bool               `json:"isTrending" firestore:"isTrending"`                 // Trending Tag for topic
	Slug               string             `json:"slug" firestore:"slug"`                             // topic Slug
	TopicURl           string             `json:"topicUrl" firestore:"topicUrl"`                     // topic url
	TopicOrder         int                `json:"topicOrder" firestore:"topicOrder"`                 // topic order we will use it for updating the trending topic for 24 hour
	TitleTemplate      string             `json:"titleTemplate" firestore:"titleTemplate"`           // topic title
	SummaryDescription string             `json:"summaryDescription" firestore:"summaryDescription"` // topic summary description
	NewsHeader         string             `json:"newsHeader" firestore:"newsHeader"`                 // topic header
	IsAsset            bool               `json:"isAsset" firestore:"isAsset"`                       // topic asset flag
	Articles           []EducationArticle `json:"articles" firestore:"articles"`                     // topic articles
}


// Add topics with all its data to FS
func SaveNewsTopics(ctx0 context.Context, topics []services.Topic) {

	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "SaveNewsTopics")
	defer span.End()
	span.AddEvent("Start Insert Topics To FS")
	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "news")
	for index, topic := range topics {
		slug := strings.ToLower(strings.ReplaceAll(topic.TopicName, " ", "-"))
		isAsset := false
		topicUrl := fmt.Sprintf("/%s/%s", "news", slug)
		fund, err := CheckTopicAssets(ctx, topic.TopicName)
		if err != nil {
			isAsset = false
		}
		if fund.Symbol != "" {
			isAsset = true
			slug = fund.Slug
			topicUrl = fmt.Sprintf("/%s/%s", "assets", slug)
		}
		fs.Collection(collectionName).Doc(topic.TopicName).Set(ctx, map[string]interface{}{
			"topicName":          topic.TopicName,
			"bertieTag":          topic.BertieTag,
			"topicUrl":           topicUrl,
			"topicOrder":         index + 1,
			"description":        topic.Description,
			"isTrending":         topic.IsTrending,
			"titleTemplate":      topic.TitleTemplate,
			"slug":               slug,
			"summaryDescription": topic.SummaryDescription,
			"newsHeader":         topic.NewsHeader,
			"isAsset":            isAsset,
		}, firestore.MergeAll)
		for _, article := range topic.Articles {
			doc := make(map[string]interface{})
			doc["id"] = article.Id
			doc["title"] = article.Title
			doc["image"] = article.Image
			doc["articleURL"] = article.ArticleURL
			doc["author"] = article.Author
			doc["type"] = article.Type
			doc["authorType"] = article.AuthorType
			doc["authorLink"] = article.AuthorLink
			doc["description"] = article.Description
			doc["publishDate"] = article.PublishDate
			doc["disabled"] = article.Disabled
			doc["seniorContributor"] = article.SeniorContributor
			doc["bylineFormat"] = article.BylineFormat
			doc["bertieTag"] = article.BertieTag
			doc["order"] = article.Order
			doc["isFeaturedArticle"] = article.IsFeaturedArticle
			doc["lastUpdated"] = article.LastUpdated
			if article.DocId != "" {
				fs.Collection(collectionName).Doc(topic.TopicName).Collection("articles").Doc(article.DocId).Set(ctx, doc, firestore.MergeAll)
			} else {
				fs.Collection(collectionName).Doc(topic.TopicName).Collection("articles").NewDoc().Set(ctx, doc, firestore.MergeAll)
			}
		}
	}
	span.SetStatus(otelCodes.Ok, "Success")

}

func CheckTopicAssets(ctxO context.Context, name string) (*FundamentalsData, error) {
	// Starts new child span from the parent span in the context.
	ctx, span := tracer.Start(ctxO, "CheckTopicAssets", trace.WithAttributes(attribute.String("name", name)))
	defer span.End()

	startTime := StartTime("Check Topic Assets Query")

	pg := PGConnect()
	query := `
	SELECT 
		symbol,
		name,
		slug
	FROM 
		public.fundamentalslatest
	where 
		name = '` + name + `'
		 `
	var fundamentals FundamentalsData

	queryResult, err := pg.QueryContext(ctx, query)
	span.AddEvent("Query Executed")

	if err != nil {

		ConsumeTime("Check Topic Assets Query", startTime, err)
		span.SetStatus(codes.Error, "unable to get data for name from PG")
		return nil, err

	}

	for queryResult.Next() {
		err := queryResult.Scan(&fundamentals.Symbol, &fundamentals.Name, &fundamentals.Slug)
		if err != nil {
			ConsumeTime("Check Topic Assets Query", startTime, err)
			return nil, err
		}
	}
	ConsumeTime("Check Topic Assets Query", startTime, nil)

	span.SetStatus(codes.Ok, "success")

	return &fundamentals, nil
}



// get Topic with it's connected articles from FS using slug
func BuildNewsTopicFromAPI(ctx0 context.Context, slug string) ([]byte, error) {
	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "GetNewsTopics")
	defer span.End()

	sectionCollection := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "news")

	// get topic data using slug
	dbSnap := fs.Collection(sectionCollection).Where("slug", "==", slug).Documents(ctx)

	span.AddEvent("Start Get News Topics Data from FS")

	var topic Topic
	for {
		var articles []EducationArticle
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

		// get topic articles
		db := fs.Collection(sectionCollection).Doc(topic.TopicName).Collection("articles").OrderBy("order", firestore.Asc).Documents(ctx)

		for {
			var article EducationArticle
			doc, err := db.Next()

			if err == iterator.Done {
				break
			}

			if err := doc.DataTo(&article); err != nil {
				log.Error("Error Getting Article Data from FS: %s", err)
				span.AddEvent(fmt.Sprintf("Error Getting Article Data from FS: %s", err))
				span.SetStatus(otelCodes.Error, err.Error())
				return nil, err
			}

			articles = append(articles, article)
		}
		// name := strings.ToLower(strings.ReplaceAll(topic.TopicName, " ", "-"))
		// apiArticles := GetArticlesFromAPI(ctx, name, topic.BertieTag)
		// topic, err = MapAPIArticlesToFSTopic(ctx, topic, apiArticles)

	}

	span.AddEvent("Modify Articles to be only 8 Articles for each Topic")
	span.SetStatus(otelCodes.Ok, "Success")
	result, err := json.Marshal(topic)
	if err != nil {
		log.Error("Error : %s", err)
		span.SetStatus(codes.Error, err.Error())
		span.AddEvent(fmt.Sprintf("Error : %s", err))
		return nil, err
	}
	return result, nil
	// return &topic, nil
}




// build new trending topics
func BuildTrendingTopicArray(ctx context.Context, trendingTopics []Topic, notTrendingTopics []Topic, topicIndex int) []Topic {
	_, span := tracer.Start(ctx, "BuildTrendingTopicArray")
	defer span.End()
	span.AddEvent("Start Build Trending Topic Array")

	var topicResult []Topic
	trendingTopicCount := 20
	trendingTopicsLen := len(trendingTopics)
	notTrendingTopicsLen := len(notTrendingTopics)
	totalIndex := (trendingTopicsLen + notTrendingTopicsLen)
	res := totalIndex - topicIndex
	if topicIndex-1 == 0 {
		topicIndex = trendingTopicCount
	}

	// if the result for topic equals to 20 then return the topic with in the range
	// if it's not equals to 20 we need to get the last part from topics and append the rest of them to reach 20 topics
	var (
		firstIndex int
		lastIndex  int
	)
	if res >= 20 {
		firstIndex = topicIndex - trendingTopicCount
		lastIndex = topicIndex
		// check it the first index out of notTrendingTopics range
		if firstIndex > len(notTrendingTopics) {
			firstIndex = firstIndex - len(notTrendingTopics)
		}
		// check it the first last out of notTrendingTopics range
		if lastIndex > len(notTrendingTopics) {
			lastIndex = len(notTrendingTopics)
		}
		topicResult = append(topicResult, notTrendingTopics[firstIndex:lastIndex]...)
	} else {
		firstIndex = topicIndex - trendingTopicCount
		lastIndex = res + firstIndex
		// check it the first index out of notTrendingTopics range
		if firstIndex > len(notTrendingTopics) {
			firstIndex = firstIndex - len(notTrendingTopics)
		}
		// check it the last index out of notTrendingTopics range
		if lastIndex > len(notTrendingTopics) {
			lastIndex = len(notTrendingTopics)
		}
		topicResult = append(topicResult, notTrendingTopics[firstIndex:lastIndex]...)

	}
	if len(topicResult) < trendingTopicCount {
		t := trendingTopicCount - len(topicResult)
		topicResult = append(topicResult, notTrendingTopics[0:t]...)
	} else if len(topicResult) > trendingTopicCount { // add this to ensure the updated trending topics will be equal to 20 each time it update it
		topicResult = topicResult[0:20]
	}
	span.SetStatus(otelCodes.Ok, "Success")
	return topicResult
}


s := "
We are excited to announce that, due to our remarkable growth over 
the last 3 of years in Crypto and NFTs market, we are expanding!

In fact, we are opening a new featured in Digital Assets."

sd := "
The new Digital Assets Features is finally here!
What makes the Digital Assets different is That give you the Opportunity to see the latest news about the Crypto and NFTs markets.
 
"