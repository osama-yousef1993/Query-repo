
type TopAsset struct {
	Symbol      string  `json:"symbol" postgres:"symbol"`             // ID of the category
	Name        string  `json:"name" postgres:"name"`                 // Name of the category
	Slug        string  `json:"slug" postgres:"slug"`                 // Total Tokens  of the category
	MarketCap   float64 `json:"market_cap" postgres:"market_cap"`     // Array of coins for the category
	HasArticles bool    `json:"has_articles" postgres:"has_articles"` // Array of coins for the category
}

// Map All articles from Data Product API (DysonSphere)
func BuildTop75AssetsArticles(ctx context.Context, assets []TopAsset) []TopAsset {
	ctx0, span := tracer.Start(ctx, "GetArticlesFromDS")
	defer span.End()
	span.AddEvent("Start Get API Response From Data Product")
	// result := make(map[string][]EducationArticle)
	var newTopAssets []TopAsset
	for _, asset := range assets {
		res, err := GetArticlesFromDysonSphere(ctx0, asset.Name)
		if err != nil {
			fmt.Print(err.Error())
			return nil
		}
		if len(res) > 0 {
			asset.HasArticles = true
		} else {
			asset.HasArticles = false
		}
		// var articles []EducationArticle
		// for _, data := range res {
		// 	var article EducationArticle
		// 	article.Title = data.Title
		// 	article.ArticleURL = data.Uri
		// 	article.Image = data.Image
		// 	article.Description = data.Description
		// 	article.Type = data.PrimaryAuthor.Type
		// 	article.AuthorType = data.PrimaryAuthor.AuthorType
		// 	article.PublishDate = data.Timestamp
		// 	article.Author = data.PrimaryAuthor.Name
		// 	article.AuthorLink = data.PrimaryAuthor.AuthorLink
		// 	article.SeniorContributor = data.PrimaryAuthor.SeniorContributor
		// 	article.BylineFormat = &data.PrimaryAuthor.BylineFormat
		// 	article.Disabled = data.PrimaryAuthor.Disabled
		// 	article.BertieTag = topic.BertieTag
		// 	article.NaturalID = data.NaturalId
		// 	articles = append(articles, article)

		// }
		// topic.Articles = append(topic.Articles, articles...)
		newTopAssets = append(newTopAssets, asset)
	}

	return newTopAssets
}

// get Topic with it's connected articles from FS And Map any new articles from Data Product API (DysonSphere) to each topic.
func GetNewsTopics(ctx0 context.Context) ([]string, error) {
	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "BuildNewsTopics")
	defer span.End()

	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "news")

	// get topic data using slug
	dbSnap := fs.Collection(collectionName).Documents(ctx)

	span.AddEvent("Start Build All News Topic From API")

	var topics []Topic
	var names []string
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
		names = append(names, topic.TopicName)
		topics = append(topics, topic)
	}

	span.SetStatus(otelCodes.Ok, "Success")
	return names, nil
}

func BuildNewsTopicWithTop75Assets(ctx0 context.Context, topics []Topic, assets []TopAsset) []Topic {
	_, span := tracer.Start(ctx0, "BuildNewsTopicWithTop75Assets")
	defer span.End()
	var newTopics []Topic
	uniqueName := make(map[string]bool)

	for _, topic := range topics {
		var newTopic Topic
		for _, asset := range assets {
			if asset.Name == topic.TopicName {
				if _, ok := uniqueName[asset.Name]; !ok {
					uniqueName[asset.Name] = true
					topic.TopicURl = fmt.Sprintf("news/%s", topic.Slug)
					newTopics = append(newTopics, topic)
				}
			} else {
				if _, ok := uniqueName[asset.Name]; !ok {
					uniqueName[asset.Name] = true
					newTopic.TopicName = asset.Name
					newTopic.BertieTag = asset.Name
					newTopic.Description = ""
					newTopic.IsTrending = false
					newTopic.IsAsset = true
					newTopic.IsFeaturedHome = false
					newTopic.Slug = asset.Slug
					newTopic.TopicURl = fmt.Sprintf("news/%s", asset.Slug)
					newTopic.TopicOrder = 0
					newTopic.TitleTemplate = fmt.Sprintf("Latest %s News | Forbes Digital Assets", asset.Name)
					newTopic.TopicPageDescription = ""
					newTopic.NewsHeader = asset.Name
					newTopic.AliasesName = asset.Name
					newTopics = append(newTopics, newTopic)
				}
			}
		}
	}

	span.SetStatus(otelCodes.Ok, "Success")
	return newTopics
}
