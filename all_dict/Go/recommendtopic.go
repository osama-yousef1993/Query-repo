package services

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
	otelCodes "go.opentelemetry.io/otel/codes"
	"google.golang.org/api/iterator"
)

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
	Articles           []EducationArticle `json:"articles" firestore:"articles"`                     // topic articles
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

	// Get All Topics from FS
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
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}
		// Get All Articles the related to each Topic from FS
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
				span.SetStatus(otelCodes.Error, err.Error())
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

	// get All new Articles from BQ using Bertie tag for Topics
	articles, err := GetEducationContentFromBertie(bertieTag, ctxO, "mv_content_latest")

	if err != nil {
		log.Error("Error Getting Articles from Bertie BQ: %s", err)
		span.SetStatus(otelCodes.Error, err.Error())
		span.AddEvent(fmt.Sprintf("Error Getting Articles from Bertie BQ: %s", err))
		return nil, err
	}
	// Map the new Articles to Topics
	newsTopics, err := MapArticlesToTopic(ctxO, topics, articles)
	if err != nil {
		log.Error("Error Map Articles to Sections: %s", err)
		span.SetStatus(otelCodes.Error, err.Error())
		span.AddEvent(fmt.Sprintf("Error Map Articles to Sections: %s", err))
		return nil, err
	}
	return newsTopics, nil

}

// map Articles to each Topic by Bertie Tag
func MapArticlesToTopic(ctxO context.Context, topics []Topic, articles []EducationArticle) ([]Topic, error) {
	_, span := tracer.Start(ctxO, "MapArticlesToTopic")
	defer span.End()

	span.AddEvent("Start Map Articles to each topic")
	var newsTopics []Topic

	for _, topic := range topics {
		var topicArticles []EducationArticle
		for _, article := range articles {
			if topic.BertieTag == article.BertieTag {
				for _, sectionArticle := range topic.Articles {
					// if article exist in topic map the new value article to it
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
	span.SetStatus(otelCodes.Ok, "Success")
	return newsTopics, nil
}

// get Topic with it's connected articles from FS using slug
func GetNewsTopic(ctx0 context.Context, slug string) (*Topic, error) {
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
		topic.Articles = articles

	}

	span.AddEvent("Modify Articles to be only 8 Articles for each Topic")
	// map 8 articles for the topics
	topic = GetTop8ArticlesFromTopics(ctx, topic)
	span.SetStatus(otelCodes.Ok, "Success")
	return &topic, nil
}

// build Topics data from BQ and config table in Rowy
func GetNewsTopicData(ctx0 context.Context, slug string) ([]byte, error) {
	ctx, span := tracer.Start(ctx0, "GetNewsTopicsData")
	defer span.End()
	span.AddEvent("Start Get News Topics Data")
	// get the topic with all it's articles using slug
	topic, err := GetNewsTopic(ctx, slug)

	if err != nil {
		log.Error("Error Getting News Topics from FS:  %s", err)
		span.AddEvent(fmt.Sprintf("Error Getting News Topics from FS: %s", err))
		span.SetStatus(otelCodes.Error, err.Error())
		return nil, err
	}

	result, err := json.Marshal(topic)
	if err != nil {
		log.Error("Error : %s", err)
		span.SetStatus(otelCodes.Error, err.Error())
		span.AddEvent(fmt.Sprintf("Error : %s", err))
		return nil, err
	}

	span.SetStatus(otelCodes.Ok, "Success")
	return result, nil

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

// Update trending topic for the day
func UpdateTrendingTopics(ctx0 context.Context) ([]Topic, []Topic) {
	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "UpdateTrendingTopics")
	defer span.End()
	span.AddEvent("Start Update Trending Topics")

	newsCollection := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "news")
	var (
		trendingTopics    []Topic
		notTrendingTopics []Topic
		dbSnap            *firestore.DocumentIterator
	)
	// get all topic the trending and not trending ones
	dbSnap = fs.Collection(newsCollection).Documents(ctx)

	span.AddEvent("Start Get Topics Data from FS")
	for {
		var topic Topic
		doc, err := dbSnap.Next()

		if err == iterator.Done {
			break
		}

		if err := doc.DataTo(&topic); err != nil {
			log.Error("Error Getting Topics Data from FS: %s", err)
			span.AddEvent(fmt.Sprintf("Error Getting Topics Data from FS: %s", err))
			span.SetStatus(otelCodes.Error, err.Error())
		}

		if topic.IsTrending {
			trendingTopics = append(trendingTopics, topic)
		} else {
			notTrendingTopics = append(notTrendingTopics, topic)
		}
	}
	lastTopic := trendingTopics[len(trendingTopics)-1]
	order := lastTopic.TopicOrder
	// build the new trending topics
	topicResult := BuildTrendingTopicArray(ctx0, trendingTopics, notTrendingTopics, order)
	span.SetStatus(otelCodes.Ok, "Success")
	return topicResult, trendingTopics
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

	// if the result for topic equals to 20 then return the topic with in the range
	// if it's not equals to 20 we need to get the last part from topics and append the rest of them to reach 20 topics
	if res >= 20 {
		topicResult = append(topicResult, notTrendingTopics[topicIndex-trendingTopicCount:topicIndex]...)
	} else {
		topicResult = append(topicResult, notTrendingTopics[topicIndex-trendingTopicCount:totalIndex-trendingTopicCount]...)
		if len(topicResult) < trendingTopicCount {
			t := trendingTopicCount - len(topicResult)
			topicResult = append(topicResult, notTrendingTopics[0:t]...)
		}
	}
	// second way 
	// if res >= 20 {
	// 	topicResult = append(topicResult, notTrendingTopics[topicIndex-trendingTopicCount:topicIndex]...)
	// } else {
	// 	initIndex := topicIndex - trendingTopicCount
	// 	if initIndex > notTrendingTopicsLen {
	// 		topicResult = append(topicResult, notTrendingTopics[0:totalIndex-trendingTopicCount]...)
	// 	} else {
	// 		topicResult = append(topicResult, notTrendingTopics[topicIndex-trendingTopicCount:totalIndex-trendingTopicCount]...)
	// 	}
	// 	if len(topicResult) < trendingTopicCount {
	// 		t := trendingTopicCount - len(topicResult)
	// 		topicResult = append(topicResult, trendingTopics[0:t]...)
	// 	}
	// }
	span.SetStatus(otelCodes.Ok, "Success")
	return topicResult
}

type Response struct {
	Body             string        `json:"body"`
	MatchingWords    []string      `json:"matching_words"`
	NaturalId        string        `json:"naturalid"`
	PrimaryChannelId string        `json:"primaryChannelId"`
	PV               int64         `json:"pv"`
	Source           string        `json:"source"`
	Timestamp        time.Time     `json:"timestamp"`
	Title            string        `json:"title"`
	Type             string        `json:"type"`
	BertieBadges     string        `json:"bertieBadges"`
	PrimaryAuthor    PrimaryAuthor `json:"primary_author"`
	Uri              string        `json:"uri"`
}

type PrimaryAuthor struct {
	AuthorType       string   `json:"authorType"`
	Badges           []string `json:"badges"`
	AuthorNaturalId  string   `json:"authorNaturalId"`
	Email            string   `json:"email"`
	Name             string   `json:"name"`
	PrimaryChannelId string   `json:"primaryChannelId"`
}

func GetAPIResponse(ctx context.Context, keyword string) ([]Response, error) {
	_, span := tracer.Start(ctx, "GetAPIResponse")
	defer span.End()
	span.AddEvent("Start Get API Response")
	url := fmt.Sprintf("https://recommend-dev.forbes.com/channel115?keywords=%s", keyword)
	res, err := http.Get(url)
	if err != nil {
		fmt.Print(err.Error())
		os.Exit(1)
	}

	responseData, err := ioutil.ReadAll(res.Body)
	if err != nil {
		fmt.Print(err.Error())
	}

	var response []Response
	json.Unmarshal(responseData, &response)

	return response, nil
}

func GetAllAuthorNaturalId(ctx context.Context, name string, tag string) []EducationArticle {
	ctx0, span := tracer.Start(ctx, "GetAPIResponse")
	defer span.End()
	span.AddEvent("Start Get API Response")
	res, err := GetAPIResponse(ctx0, name)

	if err != nil {
		fmt.Print(err.Error())
		return nil
	}
	var articles []EducationArticle
	for _, data := range res {
		var article EducationArticle
		article.Title = data.Title
		article.ArticleURL = data.Uri
		article.Type = data.Type
		article.AuthorType = data.PrimaryAuthor.AuthorType
		article.Description = data.Body
		article.PublishDate = data.Timestamp
		article.Author = data.PrimaryAuthor.Name
		article.BertieTag = tag
		articles = append(articles, article)
	}

	return articles
}

// func GetArticlesByAuthorNaturalId(ctx context.Context, ids []string, contentDataSet string) ([]byte, error) {
// 	// ctx := context.Background()
// 	_, span := tracer.Start(ctx, "GetEducationContentFromBertie")
// 	defer span.End()

// 	result, err := json.Marshal(educationArticle)
// 	if err != nil {
// 		log.Error("Error : %s", err)
// 		span.SetStatus(codes.Error, err.Error())
// 		span.AddEvent(fmt.Sprintf("Error : %s", err))
// 		return nil, err
// 	}
// 	return result, nil
// }

// get Topic with it's connected articles from FS using slug
func GetNewsTopicFromAPI(ctx0 context.Context, slug string) ([]byte, error) {
	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "GetNewsTopics")
	defer span.End()

	sectionCollection := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "news")

	// get topic data using slug
	dbSnap := fs.Collection(sectionCollection).Where("slug", "==", slug).Documents(ctx)

	span.AddEvent("Start Get News Topics Data from FS")

	var topic Topic
	for {
		// var articles []EducationArticle
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
		// db := fs.Collection(sectionCollection).Doc(topic.TopicName).Collection("articles").OrderBy("order", firestore.Asc).Documents(ctx)

		// for {
		// 	var article EducationArticle
		// 	doc, err := db.Next()

		// 	if err == iterator.Done {
		// 		break
		// 	}

		// 	if err := doc.DataTo(&article); err != nil {
		// 		log.Error("Error Getting Article Data from FS: %s", err)
		// 		span.AddEvent(fmt.Sprintf("Error Getting Article Data from FS: %s", err))
		// 		span.SetStatus(otelCodes.Error, err.Error())
		// 		return nil, err
		// 	}

		// 	articles = append(articles, article)
		// }
		name := strings.ToLower(strings.ReplaceAll(topic.TopicName, " ", "-"))
		// articles = append(articles, GetAllAuthorNaturalId(ctx, name, topic.BertieTag)...)
		topic.Articles = GetAllAuthorNaturalId(ctx, name, topic.BertieTag)

	}

	span.AddEvent("Modify Articles to be only 8 Articles for each Topic")
	// map 8 articles for the topics
	// topic = GetTop8ArticlesFromTopics(ctx, topic)
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









v1.HandleFunc("/ids/{slug}", GetIds).Methods(http.MethodGet, http.MethodOptions)


// Get All section With Articles for Learn Tab
func GetIds(w http.ResponseWriter, r *http.Request) {
	// update each 5 min
	setResponseHeaders(w, 300)
	labels := make(map[string]string)
	vars := mux.Vars(r)
	slug := vars["slug"]
	ctx, span := tracer.Start(r.Context(), "GetNewsTopics")

	defer span.End()

	labels["UUID"] = uuid.New().String()
	labels["function"] = "GetNewsTopics"
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))

	startTime := log.StartTime("Get News Topics Data")

	result, err := services.GetNewsTopicFromAPI(ctx, slug)

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
v1.HandleFunc("/ids", GetIds).Methods(http.MethodGet, http.MethodOptions)

// Get All section With Articles for Learn Tab
func GetIds(w http.ResponseWriter, r *http.Request) {
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

	result, err := services.BuildTopicsAPI(ctx)

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

func BuildTopicsAPI(ctx context.Context) ([]byte, error) {
	fs := GetFirestoreClient()
	ctxO, span := tracer.Start(ctx, "BuildTopics")
	defer span.End()
	topicCollection := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "news")

	// Get All Topics from FS
	dbSnap := fs.Collection(topicCollection).Documents(ctxO)
	span.AddEvent("Start Get Topics Data from FS")

	var topics []Topic

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
			span.SetStatus(otelCodes.Error, err.Error())
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
				span.SetStatus(otelCodes.Error, err.Error())
				return nil, err
			}
			article.DocId = doc.Ref.ID
			if article.UpdatedAt != nil {
				article.LastUpdated = article.UpdatedAt["timestamp"].(time.Time)
			}

			articles = append(articles, article)
		}
		topic.Articles = articles

		artic, err := GetEducationContentFromBertieAPI(topic.BertieTag, ctxO, "mv_content_latest")
		if err != nil {
			log.Error("Error Getting Articles from Bertie BQ: %s", err)
			span.SetStatus(otelCodes.Error, err.Error())
			span.AddEvent(fmt.Sprintf("Error Getting Articles from Bertie BQ: %s", err))
			return nil, err
		}

		art, err := MapArticlesToTopicAPI(ctx, topic.Articles, artic)

		if err != nil {
			log.Error("Error Getting Articles from Bertie BQ: %s", err)
			span.SetStatus(otelCodes.Error, err.Error())
			span.AddEvent(fmt.Sprintf("Error Getting Articles from Bertie BQ: %s", err))
			return nil, err
		}
		topic.Articles = art
		topics = append(topics, topic)

	}

	file, _ := json.MarshalIndent(topics, " ", "")
	_ = os.WriteFile("Topic.json", file, 0644)
	res, err := json.Marshal(topics)
	if err != nil {
		log.Error("Error Getting Articles from Bertie BQ: %s", err)
		span.SetStatus(otelCodes.Error, err.Error())
		span.AddEvent(fmt.Sprintf("Error Getting Articles from Bertie BQ: %s", err))
		return nil, err
	}
	return res, nil

}

// get all Articles from BQ
func GetEducationContentFromBertieAPI(slug string, ctx context.Context, contentDataSet string) ([]EducationArticle, error) {

	// ctx := context.Background()
	client := GetBQClient()

	_, span := tracer.Start(ctx, "GetEducationContentFromBertie")
	defer span.End()

	// span.AddEvent("Start Get Articles Data from BQ")

	queryResult := client.Query(articlesQueryAPi)
	queryResult.Parameters = []bigquery.QueryParameter{
		{
			Name:  "slug",
			Value: slug,
		},
	}

	it, err := queryResult.Read(ctx)
	if err != nil {
		log.Error("Error Getting Articles Data from BQ: %s", err)
		span.AddEvent(fmt.Sprintf("Error Getting Articles Data from BQ: %s", err))
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	var imageDomain string
	if contentDataSet == "mv_content_latest" {
		imageDomain = ""
	} else {
		imageDomain = os.Getenv("ARTICLES_IMAGE_DOMAIN")
	}

	var educationArticle []EducationArticle
	span.AddEvent("Start Map Articles Data")
	for {
		var article EducationArticle
		var articleFromBQ EducationArticleFromBQ
		err := it.Next(&articleFromBQ)
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Error("Error Map Articles Data to Struct: %s", err)
			span.AddEvent(fmt.Sprintf("Error Map Articles Data to Struct: %s", err))
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}

		if articleFromBQ.Id.Valid {
			article.Id = articleFromBQ.Id.StringVal
		}
		if articleFromBQ.Title.Valid {
			article.Title = articleFromBQ.Title.StringVal
		}
		if articleFromBQ.Image.Valid {
			article.Image = imageDomain + articleFromBQ.Image.StringVal
		}
		if articleFromBQ.Author.Valid {
			article.Author = articleFromBQ.Author.StringVal
		}
		if articleFromBQ.AuthorLink.Valid {
			article.AuthorLink = articleFromBQ.AuthorLink.StringVal
		}
		if articleFromBQ.AuthorType.Valid {
			article.AuthorType = articleFromBQ.AuthorType.StringVal
		}
		if articleFromBQ.Description.Valid {
			article.Description = articleFromBQ.Description.StringVal
		}
		if articleFromBQ.ArticleURL.Valid {
			article.ArticleURL = articleFromBQ.ArticleURL.StringVal
		}
		if articleFromBQ.Type.Valid {
			article.Type = articleFromBQ.Type.StringVal
		}
		if articleFromBQ.Disabled.Valid {
			article.Disabled = articleFromBQ.Disabled.Bool
		}
		if articleFromBQ.SeniorContributor.Valid {
			article.SeniorContributor = articleFromBQ.SeniorContributor.Bool
		}
		if articleFromBQ.BertieTag.Valid {
			article.BertieTag = articleFromBQ.BertieTag.StringVal
		}
		if articleFromBQ.BylineFormat.Valid {
			article.BylineFormat = &articleFromBQ.BylineFormat.Int64
		} else {
			article.BylineFormat = nil
		}
		article.PublishDate = articleFromBQ.PublishDate

		educationArticle = append(educationArticle, article)
	}

	return educationArticle, nil
}

// map Articles to each Topic by Bertie Tag
func MapArticlesToTopicAPI(ctxO context.Context, topicArticles []EducationArticle, articles []EducationArticle) ([]EducationArticle, error) {
	_, span := tracer.Start(ctxO, "MapArticlesToTopic")
	defer span.End()

	span.AddEvent("Start Map Articles to each topic")

	var newTopicArticles []EducationArticle
	for _, article := range articles {
		for _, sectionArticle := range topicArticles {
			// if article exist in topic map the new value article to it
			if sectionArticle.Title == article.Title {
				article.DocId = sectionArticle.DocId
				article.Order = sectionArticle.Order
				article.LastUpdated = sectionArticle.LastUpdated
				article.IsFeaturedArticle = sectionArticle.IsFeaturedArticle
				goto ADDArticles
			}
		}
	ADDArticles:
		newTopicArticles = append(newTopicArticles, article)
	}
	SortArticles(newTopicArticles, true)

	span.SetStatus(otelCodes.Ok, "Success")
	return newTopicArticles, nil
}


// articles query
const articlesQueryAPi = `
WITH base AS (
	SELECT
		c.id,
		c.title,
		c.date date,
		c.description,
		c.image,
		c.author,
		c.authorType author_type,
		aut.type type,
		aut.inactive disabled,
		aut.seniorContributor senior_contributor,
		aut.bylineFormat byline_format,
		REPLACE(c.uri, "http://", "https://") AS link,
		REPLACE(aut.url, "http://", "https://") AS author_link,
		c.timestamp,
		ROW_NUMBER() OVER (PARTITION BY c.naturalId ORDER BY c.timestamp DESC) AS rn
	FROM
	  api-project-901373404215.Content.mv_content_latest c,
	   UNNEST(c.newsKeywords) AS newsKeyword
	LEFT JOIN
	  api-project-901373404215.Content.v_author_latest aut
	ON
	  c.authorNaturalId = aut.naturalId
	WHERE
	  visible = TRUE
	  AND c.type = 'blog'
	  AND IFNULL(title, '') != ''
	  AND IFNULL(body, '') != ''
	  AND (EXISTS (SELECT 1 FROM UNNEST(channelSection) cs WHERE cs = 'channel_115') OR c.primaryChannelId = 'channel_115')
	  AND bertieBadges IS NOT NULL
	  AND c.timestamp > TIMESTAMP('2022-01-01')
	  and 
		newsKeyword in UNNEST([@slug])
	  or 
		newsKeyword in UNNEST([lower(@slug)])
	  AND c.date is not null
	)
	SELECT
		base.id,
		base.title,
		base.date date,
		base.description,
		base.image,
		base.author,
		base.author_type,
		base.type,
		base.disabled,
		base.senior_contributor,
		base.byline_format,
		base.link,
		base.author_link
	FROM base
	WHERE rn = 1
	ORDER BY timestamp DESC;
`
