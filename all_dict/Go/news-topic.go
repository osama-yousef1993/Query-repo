r.Handle("/build-topics", oidcAuthMiddleware.Middleware(http.HandlerFunc(BuildTopics))).Methods(http.MethodPost)
v1.HandleFunc("/topics", InsertTopics).Methods(http.MethodGet, http.MethodOptions)
v1.HandleFunc("/trending-topics", GetTopicsTags).Methods(http.MethodGet, http.MethodOptions)
v1.HandleFunc("/news-topics/", GetNewsTopics).Methods(http.MethodGet, http.MethodOptions)




func InsertTopics(w http.ResponseWriter, r *http.Request) {
	setResponseHeaders(w, 60)

	labels := make(map[string]string)
	ctx, span := tracer.Start(r.Context(), "BuildVideos")
	defer span.End()

	labels["UUID"] = uuid.New().String()
	labels["function"] = "BuildVideos"
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTime("Get Assets Calculator Data")

	store.SaveNewsTopic(ctx)

	log.EndTimeL(labels, "BuildVideos ", startTime, nil)
	span.SetStatus(codes.Ok, "BuildVideos")
	w.WriteHeader(200)
	w.Write([]byte("ok"))

}

func GetTopicsTags(w http.ResponseWriter, r *http.Request) {
	setResponseHeaders(w, 60)

	labels := make(map[string]string)
	ctx, span := tracer.Start(r.Context(), "GetTopicsTags")
	defer span.End()

	labels["UUID"] = uuid.New().String()
	labels["function"] = "GetTopicsTags"
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTime("Get Topics Tags")

	result, err := store.GetTopicsTagsList(ctx)

	if result == nil && err == nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusNotFound)
	}

	if err != nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusNotFound)
	}

	log.EndTimeL(labels, "GetTopicsTags ", startTime, nil)
	span.SetStatus(codes.Ok, "GetTopicsTags")
	w.WriteHeader(200)
	w.Write(result)

}

func BuildTopics(w http.ResponseWriter, r *http.Request) {
	labels := make(map[string]string)

	span := trace.SpanFromContext(r.Context())

	defer span.End()

	labels["function"] = "BuildTopics"
	labels["UUID"] = uuid.New().String()
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTimeL(labels, "Build Topics Data")

	result, err := services.BuildTopics(r.Context())

	if err != nil {
		log.ErrorL(labels, "%s", err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	store.SaveNewsTopics(r.Context(), result)

	log.EndTimeL(labels, "Build Topics Data ", startTime, nil)
	span.SetStatus(codes.Ok, "Build Topics Data")
	w.WriteHeader(200)
	w.Write([]byte("ok"))

}

// Get All section With Articles for Learn Tab
func GetNewsTopics(w http.ResponseWriter, r *http.Request) {
	// update each 5 min
	setResponseHeaders(w, 300)

	labels := make(map[string]string)

	span := trace.SpanFromContext(r.Context())

	defer span.End()

	labels["UUID"] = uuid.New().String()
	labels["function"] = "GetNewsTopics"
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTime("Get News Topics Data")

	result, err := services.GetNewsTopicsData(r.Context())

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


// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

type TrendingTopics struct {
	TopicName string `json:"topicName" firestore:"topicName"` // Id of chain and it will present the assets platform id from the NFT endpoint. We will use it to filter NFTs by chains.
	TopicURL  string `json:"topicUrl" firestore:"topicUrl"`   // Name for Chain, it will be used to display in the NFT prices Page.
}


// Get NFT chains List from FS
func GetTopicsTagsList(ctx0 context.Context) ([]byte, error) {
	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "GetNFTChains")
	defer span.End()

	var topicsTags []TrendingTopics

	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "news")
	// Get the NFT chains from Firestore
	iter := fs.Collection(collectionName).Where("isTrending", "==", true).Documents(ctx)
	span.AddEvent("Start Getting NFT Chains Data from FS")

	for {
		var topicsTag TrendingTopics
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			log.Error("Error Getting NFT Chains Data from FS: %s", err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}

		err = doc.DataTo(&topicsTag)
		if err != nil {
			log.Error("Error Getting NFT Chains Data from FS: %s", err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}
		topicsTags = append(topicsTags, topicsTag)
	}

	rand.Shuffle(len(topicsTags), func(i, j int) { topicsTags[i], topicsTags[j] = topicsTags[j], topicsTags[i] })

	jsonData, err := BuildJsonResponse(ctx, topicsTags[0:20], "NFT Chains Data")

	if err != nil {
		log.Error("Error Converting NFT Chains to Json: %s", err)
		span.SetStatus(otelCodes.Error, err.Error())
		return nil, err
	}

	span.SetStatus(otelCodes.Ok, "Success")
	return jsonData, nil
}

func SaveNewsTopic(ctx context.Context) {

	fs := GetFirestoreClient()

	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "news")

	topics := []string{"Digital Assets",
		"Bitcoin",
		"Ethereum",
		"Stablecoins",
		"NFT",
		"Miners",
		"Regulation",
		"Gaming",
		"Artificial Intelligence",
		"Web3",
		"Binance",
		"Blockchain",
		"Digital Assets",
		"Cryptocurrency",
		"Binance USD",
		"TrueUSD",
		"USDC",
		"USDT",
		"XRP",
		"Cardano", //20
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
		"Cosmos Hub",
		"OKB",
		"Filecoin",
		"Hedera",
		"Aptos",
		"VeChain",
		"Algorand",
		"ApeCoin",
		"Optimism",
		"Tezos", // 20 -> 40
		"Solana",
		"BNB",
		"Coinbase",
		"FTX",
		"Binance",
		"Kraken",
		"Digital Currency Group",
		"BlockFi",
		"Circle",
		"Tether",
		"Robinhood",
		"PayPal",
		"Gemini",
		"Marathon",
		"Blockstream",
		"Riot",
		"Stronghold Digital",
		"Seven Seven Six",
		"A16Z",
		"Polychain", // 20 -> 60
		"Pantera",
		"Sequoia",
		"Union Square",
		"Multicoin",
		"Dragonfly",
		"JPMorgan",
		"HSBC",
		"Santander",
		"Goldman Sachs",
		"IBM",
		"Microsoft",
		"IMF", // 12 -> 72
	}

	for _, section := range topics {
		fs.Collection(collectionName).Doc(section).Set(ctx, map[string]interface{}{
			"topicName":  section,
			"bertieTag":  section,
			"topicUrl":   fmt.Sprintf("/topics/%s", strings.ToLower(strings.ReplaceAll(section, " ", "-"))),
			"isTrending": true,
		}, firestore.MergeAll)
	}

}

func SaveNewsTopics(ctx context.Context, topics []services.Topic) {

	fs := GetFirestoreClient()

	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "news")
	for _, topic := range topics {
		fs.Collection(collectionName).Doc(topic.TopicName).Set(ctx, map[string]interface{}{
			"topicName":   topic.TopicName,
			"bertieTag":   topic.BertieTag,
			"topicUrl":    topic.TopicURl,
			"description": topic.Description,
			"isTrending":  topic.IsTrending,
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
// topics.go

package services

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
	"google.golang.org/api/iterator"
)

type Topic struct {
	TopicName   string             `json:"topicName" firestore:"topicName"`
	BertieTag   string             `json:"bertieTag" firestore:"bertieTag"`
	Description string             `json:"description" firestore:"description"`
	IsTrending  bool               `json:"isTrending" firestore:"isTrending"`
	TopicURl    string             `json:"topicUrl" firestore:"topicUrl"`
	Articles    []EducationArticle `json:"articles" firestore:"articles"`
}

// get topics from config Rowy table
// get all Articles from BQ depends on Bertie Tag
// Map Articles to each Topic
// return all Topics and it's Articles to by saved on FS Table
func BuildTopics(ctx context.Context) ([]Topic, error) {
	fs := GetFirestoreClient()
	ctxO, span := tracer.Start(ctx, "BuildTopics")
	defer span.End()
	topicCollection := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "news")

	dbSnap := fs.Collection(topicCollection).Documents(ctxO)
	span.AddEvent("Start Get Topics Data from FS")

	var topics []Topic

	var bertieTag []string
	for {
		var topic Topic
		var articles []EducationArticle
		doc, err := dbSnap.Next()

		if err == iterator.Done {
			break
		}

		if err := doc.DataTo(&topic); err != nil {
			log.Error("Error Getting Topics Data from FS: %s", err)
			span.AddEvent(fmt.Sprintf("Error Getting Topics Data from FS: %s", err))
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}
		db := fs.Collection(topicCollection).Doc(doc.Ref.ID).Collection("articles").Documents(ctxO)

		for {
			var article EducationArticle
			doc, err := db.Next()

			if err == iterator.Done {
				break
			}

			if err := doc.DataTo(&article); err != nil {
				log.Error("Error Getting Article Data from FS: %s", err)
				span.AddEvent(fmt.Sprintf("Error Getting Article Data from FS: %s", err))
				span.SetStatus(codes.Error, err.Error())
				return nil, err
			}
			article.DocId = doc.Ref.ID
			if article.UpdatedAt != nil {
				article.LastUpdated = article.UpdatedAt["timestamp"].(time.Time)
			}

			articles = append(articles, article)
		}

		bertieTag = append(bertieTag, topic.BertieTag)

		topic.Articles = articles
		topics = append(topics, topic)

	}

	articles, err := GetEducationContentFromBertie(bertieTag, ctxO, "mv_content_latest")

	if err != nil {
		log.Error("Error Getting Articles from Bertie BQ: %s", err)
		span.SetStatus(codes.Error, err.Error())
		span.AddEvent(fmt.Sprintf("Error Getting Articles from Bertie BQ: %s", err))
		return nil, err
	}
	newsTopics, err := MapArticlesToTopic(topics, articles)
	if err != nil {
		log.Error("Error Map Articles to Sections: %s", err)
		span.SetStatus(codes.Error, err.Error())
		span.AddEvent(fmt.Sprintf("Error Map Articles to Sections: %s", err))
		return nil, err
	}
	return newsTopics, nil

}

// map Articles to each Topic by Bertie Tag
func MapArticlesToTopic(topics []Topic, articles []EducationArticle) ([]Topic, error) {
	var newsTopics []Topic
	for _, topic := range topics {
		var topicArticles []EducationArticle
		for _, article := range articles {
			if topic.BertieTag == article.BertieTag {
				for _, sectionArticle := range topic.Articles {
					// if article exist in section map the new value article to it
					if sectionArticle.Title == article.Title {
						article.DocId = sectionArticle.DocId
						article.Order = sectionArticle.Order
						article.LastUpdated = sectionArticle.LastUpdated
						article.IsFeaturedArticle = sectionArticle.IsFeaturedArticle
						goto ADDArticles
					}
				}
			ADDArticles:
				topicArticles = append(topicArticles, article)
			}
		}
		SortArticles(topicArticles, true)
		topic.Articles = topicArticles
		newsTopics = append(newsTopics, topic)
	}
	return newsTopics, nil
}

// get all Topics with it's connected articles from FS
func GetNewsTopics(ctx context.Context) ([]Topic, error) {
	fs := GetFirestoreClient()
	_, span := tracer.Start(ctx, "GetEducationSectionData")
	defer span.End()

	sectionCollection := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "news")

	var (
		topics []Topic
		dbSnap *firestore.DocumentIterator
	)
	// If categories exist, we need to get the section and its articles to selected categories.
	// Else it will return the top 8 articles from all sections.
	dbSnap = fs.Collection(sectionCollection).Where("isTrending", "==", true).Documents(ctx)

	span.AddEvent("Start Get Section Data from FS")

	for {
		var articles []EducationArticle
		var topic Topic
		doc, err := dbSnap.Next()

		if err == iterator.Done {
			break
		}

		if err := doc.DataTo(&topic); err != nil {
			log.Error("Error Getting Section Data from FS: %s", err)
			span.AddEvent(fmt.Sprintf("Error Getting Section Data from FS: %s", err))
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}

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
				span.SetStatus(codes.Error, err.Error())
				return nil, err
			}

			articles = append(articles, article)
		}
		topic.Articles = articles
		topics = append(topics, topic)

	}

	/*
		- We will return the new object LandingPageEducation with both Sections data and the top Articles from Selected Section.
		- If selected categories exist, it will return the selected section and its top 12 latest articles.
		- If not, it will return all sections and the top 12 articles from all sections.
	*/
	topics = GetTop8ArticlesFromLearnSection(topics)
	return topics, nil
}

// build Topics data from BQ and config table in Rowy
func GetNewsTopicsData(ctx context.Context) ([]byte, error) {
	_, span := tracer.Start(ctx, "GetNewsTopicsData")
	defer span.End()
	span.AddEvent("Start Get News Topics Data")
	topics, err := GetNewsTopics(ctx)

	if err != nil {
		log.Error("Error Getting News Topics from FS:  %s", err)
		span.AddEvent(fmt.Sprintf("Error Getting News Topics from FS: %s", err))
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	result, err := json.Marshal(topics)
	if err != nil {
		log.Error("Error : %s", err)
		span.SetStatus(codes.Error, err.Error())
		span.AddEvent(fmt.Sprintf("Error : %s", err))
		return nil, err
	}

	return result, nil

}

/*
- This will ensure that if the selected topic contains more than 8 articles, it will return only the top 8 articles from the selected topic else it will return all articles.
*/
func GetTop8ArticlesFromLearnSection(topics []Topic) []Topic {
	var newsTopics []Topic
	var length int
	
	for _, topic := range topics {
		var latestArticles []EducationArticle
		SortArticles(topic.Articles, false)
		articlesLength := len(topic.Articles)
		if articlesLength > 8 {
			length = 8
		} else {
			length = articlesLength
		}
		latestArticles = append(latestArticles, topic.Articles[0:length]...)
		topic.Articles = latestArticles
		newsTopics = append(newsTopics, topic)

	}
	return newsTopics
}





/*
- This will ensure that if the selected topic contains more than 8 articles, it will return only the top 8 articles from the selected topic else it will return all articles.
*/
func GetTop8ArticlesFromTopics(ctx context.Context, topic Topic) Topic {
	_, span := tracer.Start(ctx, "GetTop8ArticlesFromTopics")
	defer span.End()
	span.AddEvent("Start Get Top 8 Articles From Topics")

	var length int
	var latestArticles []EducationArticle
	SortArticles(topic.Articles, false)
	articlesLength := len(topic.Articles)
	if articlesLength > 8 {
		length = 8
	} else {
		length = articlesLength
	}
	latestArticles = append(latestArticles, topic.Articles[0:length]...)
	topic.Articles = latestArticles
	span.SetStatus(otelCodes.Ok, "Success")
	return topic
}