package repository

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
	"google.golang.org/api/iterator"
)

type TopicsQuery interface {
	GetTrendingTopics(ctx context.Context) ([]datastruct.TrendingTopics, error)                          // Getting Trending Topics from FS
	GetNewsTopic(ctx context.Context, slug string) (*datastruct.Topic, error)                            // Getting News Topic from FS
	GetNewsTopicCategories(ctx context.Context) ([]datastruct.TopicCategories, error)                    // Getting News Topics Categories from FS
	GetTopicBubbles(ctx context.Context) ([]datastruct.TopicsBubbles, error)                             // Getting News Topics Bubbles from FS
	GetNewsTopics(ctx context.Context) ([]datastruct.Topic, error)                                       // Getting All News Topics from FS
	SaveNewsTopics(ctx context.Context, topics []datastruct.Topic)                                       // Save All topics data to FS
	UpdateIsTrendingTopics(ctx context.Context, topics []datastruct.Topic, oldTopics []datastruct.Topic) // Update Trending Topics in FS
	BuildNewsTopicsCategories(ctx context.Context) ([]datastruct.TopicCategories, error)
	SaveNewsTopicsCategories(ctx context.Context, topics []datastruct.TopicCategories)
}

type topicsQuery struct{}

// GetTrendingTopics Gets all Trending Topics form FS
// Takes a context
// Returns ([]datastruct.TrendingTopics, error)
//
// Gets the Trending Topics data from firestore
// Returns the Trending Topics and no error if successful
func (t *topicsQuery) GetTrendingTopics(ctx context.Context) ([]datastruct.TrendingTopics, error) {
	fs := fsUtils.GetFirestoreClient()
	span, labels := common.GenerateSpan("V2 TopicsQuery.GetTrendingTopics", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 TopicsQuery.GetTrendingTopics"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 TopicsQuery.GetTrendingTopics"))

	var topicsTags []datastruct.TrendingTopics

	iter := fs.Collection(datastruct.TopicsCollectionName).Where("isTrending", "==", true).Documents(ctx)

	for {
		var topicsTag datastruct.TrendingTopics
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}

		if err := doc.DataTo(&topicsTag); err != nil {
			log.Error("Error V2 TopicsQuery.GetTrendingTopics Mapping Trending Topics from FS: %s", err)
			return nil, err
		}
		topicsTags = append(topicsTags, topicsTag)
	}

	// shuffle the TrendingTopics Array
	rand.Shuffle(len(topicsTags), func(i, j int) { topicsTags[i], topicsTags[j] = topicsTags[j], topicsTags[i] })

	log.EndTimeL(labels, "V2 TopicsQuery.GetTrendingTopics", startTime, nil)
	span.SetStatus(codes.Ok, "V2 TopicsQuery.GetTrendingTopics")
	return topicsTags, nil
}

// GetNewsTopic Gets news topics from FS
// Takes a (ctx context.Context, slug string)
// Returns (*datastruct.Topic, error)
//
// Gets the News Topic data from firestore using Slug
// Returns the News Topic and no error if successful
func (t *topicsQuery) GetNewsTopic(ctx context.Context, slug string) (*datastruct.Topic, error) {
	fs := fsUtils.GetFirestoreClient()
	span, labels := common.GenerateSpan("V2 TopicsQuery.GetNewsTopic", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 TopicsQuery.GetNewsTopic"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 TopicsQuery.GetNewsTopic"))

	dbSnap := fs.Collection(datastruct.TopicsCollectionName).Where("slug", "==", slug).Documents(ctx)

	var topic datastruct.Topic
	for {
		var articles []datastruct.Article
		doc, err := dbSnap.Next()

		if err == iterator.Done {
			break
		}

		if err := doc.DataTo(&topic); err != nil {
			log.Error("Error V2 TopicsQuery.GetNewsTopic Mapping News Topic from FS: %s", err)
			return nil, err
		}

		// get topic articles
		db := fs.Collection(datastruct.TopicsCollectionName).Doc(topic.TopicName).Collection("articles").Documents(ctx)

		for {
			var article datastruct.Article
			doc, err := db.Next()

			if err == iterator.Done {
				break
			}

			if err := doc.DataTo(&article); err != nil {
				log.Error("Error V2 TopicsQuery.GetNewsTopic Mapping News Topic Articles from FS: %s", err)
				return nil, err
			}

			articles = append(articles, article)
		}
		topic.Articles = articles

	}

	log.EndTimeL(labels, "V2 TopicsQuery.GetNewsTopic", startTime, nil)
	span.SetStatus(codes.Ok, "V2 TopicsQuery.GetNewsTopic")
	return &topic, nil
}

// GetNewsTopicCategories Gets news topics categories from FS
// Takes a context
// Returns ([]datastruct.TopicCategories, error)
//
// Gets the News Topics Categories data from firestore
// Returns Array of News Topics Categories data and no error if successful
func (t *topicsQuery) GetNewsTopicCategories(ctx context.Context) ([]datastruct.TopicCategories, error) {
	fs := fsUtils.GetFirestoreClient()
	span, labels := common.GenerateSpan("V2 TopicsQuery.GetNewsTopicCategories", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 TopicsQuery.GetNewsTopicCategories"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 TopicsQuery.GetNewsTopicCategories"))

	dbSnap := fs.Collection(datastruct.TopicsCategoriesCollectionName).Documents(ctx)

	var topicsCategories []datastruct.TopicCategories
	for {
		var topicsCategory datastruct.TopicCategories
		var topics []datastruct.TrendingTopics
		doc, err := dbSnap.Next()

		if err == iterator.Done {
			break
		}

		if err := doc.DataTo(&topicsCategory); err != nil {
			log.Error("Error V2 TopicsQuery.GetNewsTopicCategories Mapping News Topics Categories from FS: %s", err)
			return nil, err
		}

		db := fs.Collection(datastruct.TopicsCategoriesCollectionName).Doc(topicsCategory.CategoryName).Collection("topics").Documents(ctx)

		for {
			var topic datastruct.TrendingTopics
			doc, err := db.Next()

			if err == iterator.Done {
				break
			}

			if err := doc.DataTo(&topic); err != nil {
				log.Error("Error V2 TopicsQuery.GetTrendingTopicsNews Mapping News Topics Categories articles from FS: %s", err)
				return nil, err
			}

			topics = append(topics, topic)
		}

		topicsCategory.CategoryTopics = topics

		topicsCategories = append(topicsCategories, topicsCategory)
	}
	log.EndTimeL(labels, "V2 TopicsQuery.GetNewsTopicCategories", startTime, nil)
	span.SetStatus(codes.Ok, "V2 TopicsQuery.GetNewsTopicCategories")
	return topicsCategories, nil
}

// GetTopicBubbles Gets news topics bubbles from FS
// Takes a context
// Returns ([]datastruct.TopicsBubbles, error)
//
// Gets the News Topics Bubbles data from firestore
// Returns Array of News Topics Bubbles data and no error if successful
func (t *topicsQuery) GetTopicBubbles(ctx context.Context) ([]datastruct.TopicsBubbles, error) {
	fs := fsUtils.GetFirestoreClient()
	span, labels := common.GenerateSpan("V2 TopicsQuery.GetTopicBubbles", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 TopicsQuery.GetTopicBubbles"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 TopicsQuery.GetTopicBubbles"))

	// get Topic Bubbles data
	dbSnap := fs.Collection(datastruct.TopicsBubblesCollection).OrderBy("topicName", firestore.Asc).Documents(ctx)

	var topicBubbles []datastruct.TopicsBubbles
	for {
		var topicBubble datastruct.TopicsBubbles
		doc, err := dbSnap.Next()

		if err == iterator.Done {
			break
		}

		if err := doc.DataTo(&topicBubble); err != nil {
			log.Error("Error V2 TopicsQuery.GetTopicBubbles Mapping Topics Bubbles from FS: %s", err)
			return nil, err
		}

		// get slug from News Rowy table for Topic Bubbles
		topic, _ := t.GetTopicsTagsByName(ctx, topicBubble.TopicName)
		topicResult := datastruct.TopicsBubbles{TopicName: topic.TopicName, Slug: topic.Slug}
		topicBubbles = append(topicBubbles, topicResult)
	}
	log.EndTimeL(labels, "V2 TopicsQuery.GetTopicBubbles", startTime, nil)
	span.SetStatus(codes.Ok, "V2 TopicsQuery.GetTopicBubbles")
	return topicBubbles, nil
}

// GetTopicsTagsByName Gets topics tags from FS by name
// Takes a (ctx context.Context, name string)
// Returns (*datastruct.TrendingTopics, error)
//
// Gets the Topics Tag data from firestore
// Returns  Topics Tag data and no error if successful
func (t *topicsQuery) GetTopicsTagsByName(ctx context.Context, name string) (*datastruct.TrendingTopics, error) {
	fs := fsUtils.GetFirestoreClient()
	span, labels := common.GenerateSpan("V2 TopicsQuery.GetTopicsTagsByName", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 TopicsQuery.GetTopicsTagsByName"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 TopicsQuery.GetTopicsTagsByName"))

	iter := fs.Collection(datastruct.TopicsCollectionName).Where("topicName", "==", name).Documents(ctx)
	var topicsTag datastruct.TrendingTopics
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}

		if err := doc.DataTo(&topicsTag); err != nil {
			log.Error("Error V2 TopicsQuery.GetTopicsTagsByName Mapping Topics Tag from FS: %s", err)
			return nil, err
		}
	}

	log.EndTimeL(labels, "V2 TopicsQuery.GetTopicsTagsByName", startTime, nil)
	span.SetStatus(codes.Ok, "V2 TopicsQuery.GetTopicsTagsByName")
	return &topicsTag, nil
}

// GetNewsTopics Get topics Data from FS
// Takes a context
// Returns ([]datastruct.Topic, error)
//
// Gets all Topics data from firestore
// Returns  Topics data and no error if successful
func (t *topicsQuery) GetNewsTopics(ctx context.Context) ([]datastruct.Topic, error) {
	fs := fsUtils.GetFirestoreClient()
	span, labels := common.GenerateSpan("V2 TopicsQuery.GetNewsTopics", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 TopicsQuery.GetNewsTopics"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 TopicsQuery.GetNewsTopics"))

	dbSnap := fs.Collection(datastruct.TopicsCollectionName).Documents(ctx)

	span.AddEvent("Start Build All News Topic From API")

	var topics []datastruct.Topic
	for {
		var topic datastruct.Topic
		var articles []datastruct.Article
		doc, err := dbSnap.Next()

		if err == iterator.Done {
			break
		}
		if err := doc.DataTo(&topic); err != nil {
			log.Error("Error V2 TopicsQuery.GetNewsTopics Mapping Topic from FS: %s", err)
			return nil, err
		}
		// Add this check for topics because sometimes we modify Rowy table like adding new row with out data
		// this will cause panic and to avoid this we add this if statement to ensure the topic has name
		if topic.TopicName != "" {
			db := fs.Collection(datastruct.TopicsCollectionName).Doc(topic.TopicName).Collection("articles").OrderBy("order", firestore.Asc).Documents(ctx)

			for {
				var article datastruct.Article
				doc, err := db.Next()

				if err == iterator.Done {
					break
				}

				if err := doc.DataTo(&article); err != nil {
					log.Error("Error V2 TopicsQuery.GetNewsTopics Mapping Topic Articles from FS: %s", err)
					return nil, err
				}
				article.DocId = doc.Ref.ID
				if article.UpdatedAt != nil {
					article.LastUpdated = article.UpdatedAt["timestamp"].(time.Time)
				}

				articles = append(articles, article)
			}
			topic.Articles = articles
			topics = append(topics, topic)
		}
	}
	log.EndTimeL(labels, "V2 TopicsQuery.GetNewsTopics", startTime, nil)
	span.SetStatus(codes.Ok, "V2 TopicsQuery.GetNewsTopics")
	return topics, nil
}

// SaveNewsTopics Save topics Data into FS
// Takes (ctx context.Context, topics []datastruct.Topic)
//
// Save all Topics data into firestore and remove the articles that doesn't contains NaturalID
// Nothing will Returns
func (t *topicsQuery) SaveNewsTopics(ctx context.Context, topics []datastruct.Topic) {

	fs := fsUtils.GetFirestoreClient()
	span, labels := common.GenerateSpan("V2 TopicsQuery.SaveNewsTopics", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 TopicsQuery.SaveNewsTopics"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 TopicsQuery.SaveNewsTopics"))

	for index, topic := range topics {
		slug := topic.Slug
		isAsset := false
		topicUrl := fmt.Sprintf("/news/%s", slug)
		fund, err := t.CheckTopicAssets(ctx, topic.AliasesName)
		if err != nil {
			isAsset = false
		}
		if fund.Symbol != "" {
			isAsset = true
			slug = fund.Slug
			topicUrl = fmt.Sprintf("/assets/%s", slug)
		}
		fs.Collection(datastruct.TopicsCollectionName).Doc(topic.TopicName).Set(ctx, map[string]interface{}{
			"topicName":            topic.TopicName,
			"bertieTag":            topic.BertieTag,
			"topicUrl":             topicUrl,
			"topicOrder":           index + 1,
			"description":          topic.Description,
			"isTrending":           topic.IsTrending,
			"isAsset":              isAsset,
			"isFeaturedHome":       topic.IsFeaturedHome,
			"titleTemplate":        topic.TitleTemplate,
			"slug":                 slug,
			"topicPageDescription": topic.TopicPageDescription,
			"newsHeader":           topic.NewsHeader,
			"aliasesName":          topic.AliasesName,
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
			doc["naturalid"] = article.NaturalID
			//if there is no natural id dont store the article
			if article.NaturalID != "" {
				fs.Collection(datastruct.TopicsCollectionName).Doc(topic.TopicName).Collection("articles").Doc(strings.ReplaceAll(article.NaturalID, "/", "_")).Set(ctx, doc, firestore.MergeAll)
			}
		}
		err = t.removeArticlesWithOutNaturalID(ctx, datastruct.TopicsCollectionName, topic.TopicName)
		if err != nil {
			log.Error("Error V2 TopicsQuery.SaveNewsTopics Removing Articles without NaturalID from FS: %s", err)
		}
	}
	log.EndTimeL(labels, "V2 TopicsQuery.SaveNewsTopics", startTime, nil)
	span.SetStatus(codes.Ok, "V2 TopicsQuery.SaveNewsTopics")

}

// removeArticlesWithOutNaturalID remove articles from FS
// Takes (ctx context.Context, collectionName string, topicName string)
// Returns an error
// This function is to remove all articles without a natural id.
// This is because we can not match them correctly to incoming articles.
// The natural id is the primary key
// Returns no error if successful
func (t *topicsQuery) removeArticlesWithOutNaturalID(ctx context.Context, collectionName string, topicName string) error {

	fs := fsUtils.GetFirestoreClient()
	span, labels := common.GenerateSpan("V2 TopicsQuery.removeArticlesWithOutNaturalID", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 TopicsQuery.removeArticlesWithOutNaturalID"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 TopicsQuery.removeArticlesWithOutNaturalID"))

	db := fs.Collection(collectionName).Doc(topicName).Collection("articles").Documents(ctx)

	for {
		var article datastruct.Article
		doc, err := db.Next()

		if err == iterator.Done {
			break
		}

		if err := doc.DataTo(&article); err != nil {
			log.Error("Error V2 TopicsQuery.removeArticlesWithOutNaturalID Mapping Topic Articles from FS: %s", err)
			return err
		}
		//if the article does not have a natural id delete it
		if article.NaturalID == "" {
			fs.Collection(collectionName).Doc(topicName).Collection("articles").Doc(doc.Ref.ID).Delete(ctx)
		}
	}

	log.EndTimeL(labels, "V2 TopicsQuery.removeArticlesWithOutNaturalID", startTime, nil)
	span.SetStatus(codes.Ok, "V2 TopicsQuery.removeArticlesWithOutNaturalID")
	return nil

}

// CheckTopicAssets Get Fundamentals Data from PG
// Takes (ctx context.Context, name string)
// Returns (*datastruct.FundamentalsData, error)
// This function is to get asset data from PG based on topic name if this topic is an asset
// Returns datastruct.FundamentalsData for topic and no error if successful
func (t *topicsQuery) CheckTopicAssets(ctx context.Context, name string) (*datastruct.FundamentalsData, error) {
	span, labels := common.GenerateSpan("V2 TopicsQuery.CheckTopicAssets", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 TopicsQuery.CheckTopicAssets"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 TopicsQuery.CheckTopicAssets"))

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
	var fundamentals datastruct.FundamentalsData

	queryResult, err := pg.QueryContext(ctx, query)
	if err != nil {
		log.Error("Error V2 TopicsQuery.CheckTopicAssets Fundamentals Query Error From PG: %s", err)
		return nil, err

	}

	for queryResult.Next() {
		err := queryResult.Scan(&fundamentals.Symbol, &fundamentals.Name, &fundamentals.Slug)
		if err != nil {
			log.Error("Error V2 TopicsQuery.CheckTopicAssets Mapping Fundamentals From PG: %s", err)
			return nil, err
		}
	}
	log.EndTimeL(labels, "V2 TopicsQuery.CheckTopicAssets", startTime, nil)
	span.SetStatus(codes.Ok, "V2 TopicsQuery.CheckTopicAssets")

	return &fundamentals, nil
}

// Update trending tags for topics from 24 hours.
// UpdateIsTrendingTopics Update trending topics Data into FS
// Takes (ctx context.Context, topics []datastruct.Topic, oldTopics []datastruct.Topic)
//
// Update Trending Topics data into firestore
// Nothing will Returns
func (t *topicsQuery) UpdateIsTrendingTopics(ctx context.Context, topics []datastruct.Topic, oldTopics []datastruct.Topic) {

	fs := fsUtils.GetFirestoreClient()
	span, labels := common.GenerateSpan("V2 TopicsQuery.UpdateIsTrendingTopics", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 TopicsQuery.UpdateIsTrendingTopics"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 TopicsQuery.UpdateIsTrendingTopics"))

	span.AddEvent("Start Update Trending as new Trending")
	for _, topic := range oldTopics {
		fs.Collection(datastruct.TopicsCollectionName).Doc(topic.TopicName).Set(ctx, map[string]interface{}{
			"isTrending": false,
		}, firestore.MergeAll)
	}

	span.AddEvent("Start Update not Trending as new Trending")
	for _, topic := range topics {
		fs.Collection(datastruct.TopicsCollectionName).Doc(topic.TopicName).Set(ctx, map[string]interface{}{
			"isTrending": true,
		}, firestore.MergeAll)
	}

	log.EndTimeL(labels, "V2 TopicsQuery.UpdateIsTrendingTopics", startTime, nil)
	span.SetStatus(codes.Ok, "V2 TopicsQuery.UpdateIsTrendingTopics")

}

// Build topics category from FS
// BuildNewsTopicsCategories Build News Topics Data into FS
// Takes a context
// Returns ([]datastruct.TopicCategories, error)
//
// Build News Categories From firestore
// Returns []datastruct.TopicCategories and no error if successful
func (t *topicsQuery) BuildNewsTopicsCategories(ctx context.Context) ([]datastruct.TopicCategories, error) {

	fs := fsUtils.GetFirestoreClient()
	span, labels := common.GenerateSpan("V2 TopicsQuery.BuildNewsTopicsCategories", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 TopicsQuery.BuildNewsTopicsCategories"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 TopicsQuery.BuildNewsTopicsCategories"))

	dbSnap := fs.Collection(datastruct.TopicsCategoriesCollectionName).Documents(ctx)

	var topicCategories []datastruct.TopicCategories
	for {
		var topicCategory datastruct.TopicCategories
		var topics []datastruct.TrendingTopics
		doc, err := dbSnap.Next()

		if err == iterator.Done {
			break
		}
		if err := doc.DataTo(&topicCategory); err != nil {
			log.Error("Error V2 TopicsQuery.BuildNewsTopicsCategories Mapping Categories from FS: %s", err)
			return nil, err
		}
		db := fs.Collection(datastruct.TopicsCategoriesCollectionName).Doc(topicCategory.CategoryName).Collection("topics").Documents(ctx)

		for {
			var topic datastruct.TrendingTopics
			doc, err := db.Next()

			if err == iterator.Done {
				break
			}

			if err := doc.DataTo(&topic); err != nil {
				log.Error("Error V2 TopicsQuery.BuildNewsTopicsCategories Mapping Topics Categories from FS: %s", err)
				return nil, err
			}
			topicResult, _ := t.GetTopicsTagsByName(ctx, topic.TopicName)
			topicResult.DocId = doc.Ref.ID
			topics = append(topics, *topicResult)
		}
		topicCategory.CategoryTopics = topics
		topicCategories = append(topicCategories, topicCategory)
	}
	log.EndTimeL(labels, "V2 TopicsQuery.BuildNewsTopicsCategories", startTime, nil)
	span.SetStatus(codes.Ok, "V2 TopicsQuery.BuildNewsTopicsCategories")
	return topicCategories, nil

}

// Save topics category into FS
// SaveNewsTopicsCategories Build News Topics Data into FS
// Takes a (ctx context.Context, topics []datastruct.TopicCategories)
//
// Save News Categories into firestore
// Nothing will Returns
func (t *topicsQuery) SaveNewsTopicsCategories(ctx context.Context, topics []datastruct.TopicCategories) {

	fs := fsUtils.GetFirestoreClient()
	span, labels := common.GenerateSpan("V2 TopicsQuery.SaveNewsTopicsCategories", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 TopicsQuery.SaveNewsTopicsCategories"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 TopicsQuery.SaveNewsTopicsCategories"))
	for _, topic := range topics {
		fs.Collection(datastruct.TopicsCategoriesCollectionName).Doc(topic.CategoryName).Set(ctx, map[string]interface{}{
			"categoryName": topic.CategoryName,
		}, firestore.MergeAll)
		for _, content := range topic.CategoryTopics {
			doc := make(map[string]interface{})
			doc["topicName"] = content.TopicName
			doc["topicUrl"] = content.TopicURL
			doc["isAsset"] = content.IsAsset
			doc["slug"] = content.Slug
			fs.Collection(datastruct.TopicsCategoriesCollectionName).Doc(topic.CategoryName).Collection("topics").Doc(content.DocId).Set(ctx, doc, firestore.MergeAll)
		}
	}
	log.EndTimeL(labels, "V2 TopicsQuery.SaveNewsTopicsCategories", startTime, nil)
	span.SetStatus(codes.Ok, "V2 TopicsQuery.SaveNewsTopicsCategories")

}
