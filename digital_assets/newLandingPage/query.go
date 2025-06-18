package repository

import (
	"context"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
	"google.golang.org/api/iterator"
)

type LandingPageQuery interface {
	GetLandingPageFeaturedCategoriesArticles(ctx context.Context, categories []string) ([]datastruct.Topic, error) // Gets landing Page Featured categories Articles
	GetLandingPageCategories(ctx context.Context) (*[]datastruct.LandingPageFeaturedCategories, error)             // Get All landing page Featured Categories
	GetLandingPageArticles(ctx context.Context, category []string) ([]datastruct.Topic, error)
}

type landingPageQuery struct{}

// GetLandingPageFeaturedCategoriesArticles Gets all content for Landing Page Featured Categories
// Takes a context and Array of Categories
// Returns (*datastruct.Article, Error)
//
// Gets the Landing Page Featured Categories data from firestore
// Returns the Landing Page Featured Categories Articles and no error if successful
func (c *landingPageQuery) GetLandingPageFeaturedCategoriesArticles(ctx context.Context, categories []string) ([]datastruct.Topic, error) {
	fs := fsUtils.GetFirestoreClient()

	span, labels := common.GenerateSpan("V2 LandingPageQuery.GetLandingPageFeaturedCategoriesArticles", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 LandingPageQuery.GetLandingPageFeaturedCategoriesArticles"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 LandingPageQuery.GetLandingPageFeaturedCategoriesArticles"))

	var (
		db                 *firestore.DocumentIterator
		featuredCategories []datastruct.Topic
	)
	if categories != nil {
		db = fs.Collection(datastruct.NewsCollectionName).Where("isFeaturedHome", "==", true).Where("topicOrder", "!=", 0).Where("slug", "in", categories).OrderBy("topicOrder", firestore.Asc).Documents(ctx)
	} else {
		db = fs.Collection(datastruct.NewsCollectionName).Where("isFeaturedHome", "==", true).Where("topicOrder", "!=", 0).OrderBy("topicOrder", firestore.Asc).Documents(ctx)
	}
	for {
		var topic datastruct.Topic
		var articles []datastruct.Article

		doc, err := db.Next()
		if err == iterator.Done {
			break
		}

		if err := doc.DataTo(&topic); err != nil {
			log.ErrorL(labels, "Error V2 LandingPageQuery.GetLandingPageFeaturedCategoriesArticles Mapping Topics Data from FS: %s", err)
			return nil, err
		}

		dbSnap := fs.Collection(datastruct.NewsCollectionName).Doc(topic.TopicName).Collection("articles").Documents(ctx)

		for {
			var categoryArticles datastruct.Article
			doc, err := dbSnap.Next()
			if err == iterator.Done {
				break
			}

			if err := doc.DataTo(&categoryArticles); err != nil {
				log.ErrorL(labels, "Error V2 LandingPageQuery.GetLandingPageFeaturedCategoriesArticles Mapping Articles Data from FS: %s", err)
				return nil, err
			}
			articles = append(articles, categoryArticles)
		}
		topic.Articles = articles
		featuredCategories = append(featuredCategories, topic)

	}

	log.EndTimeL(labels, "V2 LandingPageQuery.GetLandingPageFeaturedCategoriesArticles", startTime, nil)
	span.SetStatus(codes.Ok, "V2 LandingPageQuery.GetLandingPageFeaturedCategoriesArticles")

	return featuredCategories, nil
}

// GetLandingPageArticles Gets all content from EditorsPick or NewsFeed
// Takes a (ctx context.Context, category string)
// Returns ([]datastruct.Article, error)
//
// Gets the EditorsPick or NewsFeed from FS
// Returns all articles for EditorsPick or NewsFeed  to be displayed in Latest news Section HomePage
func (c *landingPageQuery) GetLandingPageArticles(ctx context.Context, categories []string) ([]datastruct.Topic, error) {
	span, labels := common.GenerateSpan("V2 LandingPageQuery.GetLandingPageArticles", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 LandingPageQuery.GetLandingPageArticles"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 LandingPageQuery.GetLandingPageArticles"))
	var topics []datastruct.Topic

	for _, category := range categories {
		var topic datastruct.Topic
		// if the category filter is editors this means we need all articles from EditorsPick
		if category == "editorsPick" {
			editorsPickArticles, err := c.GetLandingPageEditorsPickArticles(ctx)
			if err != nil {
				log.ErrorL(labels, "Error V2 LandingPageQuery.GetLandingPageArticles Getting editorsPick Data from FS: %s", err)
				return nil, err
			}
			topic.TopicName = "editorsPick"
			topic.Articles = editorsPickArticles
			topics = append(topics, topic)
		}
		// if the category filter is newsfeed this means we need all articles from NewsFeed
		if category == "newsfeed" {
			newsfeedArticles, err := c.GetLandingPageNewsFeedArticles(ctx)
			if err != nil {
				log.ErrorL(labels, "Error V2 LandingPageQuery.GetLandingPageArticles Getting newsfeed Data from FS: %s", err)
				return nil, err
			}
			topic.TopicName = "newsfeed"
			topic.Articles = newsfeedArticles
			topics = append(topics, topic)
		}
	}
	log.EndTimeL(labels, "V2 LandingPageQuery.GetLandingPageArticles", startTime, nil)
	span.SetStatus(codes.Ok, "V2 LandingPageQuery.GetLandingPageArticles")

	return topics, nil
}

// GetLandingPageNewsFeedArticles Gets all content for Newsfedd articles from FS
// Takes a context
// Returns ([]datastruct.Article, error)
//
// Gets the Newsfedd articles from firestore
// Returns all newsfeed articles after we build the same object that we need for all articles
func (c *landingPageQuery) GetLandingPageNewsFeedArticles(ctx context.Context) ([]datastruct.Article, error) {
	fs := fsUtils.GetFirestoreClient()

	span, labels := common.GenerateSpan("V2 LandingPageQuery.GetLandingPageNewsFeedArticles", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 LandingPageQuery.GetLandingPageNewsFeedArticles"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 LandingPageQuery.GetLandingPageNewsFeedArticles"))
	var newsFeedPayload datastruct.NewsFeedPayload

	collectionName := fmt.Sprintf("pagedata%s", os.Getenv("DATA_NAMESPACE"))
	ds, err := fs.Collection(collectionName).Doc("newsfeed").Get(ctx)
	var articles []datastruct.Article

	if err != nil {
		log.ErrorL(labels, "Error V2 LandingPageQuery.GetLandingPageNewsFeedArticles Getting Newsfeed Data from FS: %s", err)
		return articles, err
	}

	err = ds.DataTo(&newsFeedPayload)
	if err != nil {
		log.ErrorL(labels, "Error V2 LandingPageQuery.GetLandingPageNewsFeedArticles Mapping Newsfeed Data from FS: %s", err)
		return articles, err
	}

	// we build this here because we need to match all our articles in one object so the FE don't change anything in there side
	// we will map all values that match between all the response from the three sources
	for _, nfArticle := range newsFeedPayload.NewsFeedItems {
		var article datastruct.Article
		article.Id = nfArticle.Id
		article.Title = nfArticle.Title
		article.Image = nfArticle.Image
		article.ArticleURL = nfArticle.URI
		article.Author = nfArticle.Author.Name
		article.Type = nfArticle.Author.Type
		article.AuthorType = nfArticle.Author.AuthorType
		article.AuthorLink = nfArticle.Author.AuthorUrl
		article.Description = nfArticle.Description
		article.PublishDate = nfArticle.Date
		article.Disabled = nfArticle.Author.Disabled
		article.SeniorContributor = nfArticle.Author.SeniorContributor
		article.BertieTag = "Latest News"
		article.NaturalID = nfArticle.NaturalID
		article.Order = 0
		article.IsFeaturedArticle = false

		articles = append(articles, article)

	}
	log.EndTimeL(labels, "V2 LandingPageQuery.GetLandingPageNewsFeedArticles", startTime, nil)
	span.SetStatus(codes.Ok, "V2 LandingPageQuery.GetLandingPageNewsFeedArticles")

	return articles, nil
}

// GetLandingPageEditorsPickArticles Gets all content for EditorsPick articles from FS
// Takes a context
// Returns ([]datastruct.Article, error)
//
// Gets the EditorsPick articles from firestore
// Returns all EditorsPick articles after we build the same object that we need for all articles
func (c *landingPageQuery) GetLandingPageEditorsPickArticles(ctx context.Context) ([]datastruct.Article, error) {
	fs := fsUtils.GetFirestoreClient()

	span, labels := common.GenerateSpan("V2 LandingPageQuery.GetLandingPageEditorsPickArticles", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 LandingPageQuery.GetLandingPageEditorsPickArticles"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 LandingPageQuery.GetLandingPageEditorsPickArticles"))
	var editorsPicksData datastruct.EditorsPick
	var articles []datastruct.Article

	editorsPickCollection := fmt.Sprintf("editorsPicks%s", os.Getenv("DATA_NAMESPACE"))

	dbSnap, dataSnapErr := fs.Collection(editorsPickCollection).Doc("editorsPick_data").Get(ctx)

	if dataSnapErr != nil {
		log.ErrorL(labels, "Error V2 LandingPageQuery.GetLandingPageEditorsPickArticles Getting editorsPick Data from FS: %s", dataSnapErr)
		return nil, dataSnapErr
	}

	if err := dbSnap.DataTo(&editorsPicksData); err != nil {
		log.ErrorL(labels, "Error V2 LandingPageQuery.GetLandingPageEditorsPickArticles Mapping editorsPick Data from FS: %s", err)
		return nil, err
	}
	// we build this here because we need to match all our articles in one object so the FE don't change anything in there side
	// we will map all values that match between all the response from the three sources
	for _, edArticle := range editorsPicksData.PromotedContent.ContentPositions {
		var article datastruct.Article
		article.Id = edArticle.ID
		article.Title = edArticle.Title
		article.Image = edArticle.Image
		article.ArticleURL = edArticle.URI
		article.Author = edArticle.AuthorGroup.PrimaryAuthor.Name
		article.Type = edArticle.Type
		article.AuthorType = edArticle.AuthorGroup.PrimaryAuthor.AuthorType
		article.AuthorLink = edArticle.AuthorGroup.PrimaryAuthor.URL
		article.Description = edArticle.Description
		article.PublishDate = time.Unix(0, edArticle.Date*int64(time.Millisecond))
		article.Disabled = edArticle.AuthorGroup.PrimaryAuthor.Disabled
		article.SeniorContributor = edArticle.AuthorGroup.PrimaryAuthor.SeniorContributor
		article.BertieTag = "Editors' Picks"
		article.Order = 0
		article.IsFeaturedArticle = false
		article.NaturalID = edArticle.NaturalID

		articles = append(articles, article)

	}
	log.EndTimeL(labels, "V2 LandingPageQuery.GetLandingPageEditorsPickArticles", startTime, nil)
	span.SetStatus(codes.Ok, "V2 LandingPageQuery.GetLandingPageEditorsPickArticles")

	return articles, nil
}

// GetLandingPageCategories Gets all Landing Page Featured Categories
// Takes a context
// Returns (*datastruct.Article, Error)
//
// Gets the Landing Page Featured Categories data from firestore
// Returns the Landing Page Featured Categories and no error if successful
func (c *landingPageQuery) GetLandingPageCategories(ctx context.Context) (*[]datastruct.LandingPageFeaturedCategories, error) {
	fs := fsUtils.GetFirestoreClient()

	span, labels := common.GenerateSpan("V2 LandingPageQuery.GetLandingPageCategories", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 LandingPageQuery.GetLandingPageCategories"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 LandingPageQuery.GetLandingPageCategories"))

	var featuresCategories []datastruct.LandingPageFeaturedCategories

	// Get Featured Categories and order it by category order
	iter := fs.Collection(datastruct.NewsCollectionName).Where("isFeaturedHome", "==", true).Where("topicOrder", "!=", 0).OrderBy("topicOrder", firestore.Asc).Documents(ctx)
	span.AddEvent("Start Getting Landing Page Categories Data from FS")

	for {
		var featuresCategory datastruct.LandingPageFeaturedCategories

		doc, err := iter.Next()

		if err == iterator.Done {
			break
		}
		if err = doc.DataTo(&featuresCategory); err != nil {
			log.ErrorL(labels, "Error V2 LandingPageQuery.GetLandingPageCategories Mapping Features Category Data from FS: %s", err)
			return nil, err
		}
		featuresCategories = append(featuresCategories, featuresCategory)

	}

	log.EndTimeL(labels, "V2 LandingPageQuery.GetLandingPageCategories", startTime, nil)
	span.SetStatus(codes.Ok, "V2 LandingPageQuery.GetLandingPageCategories")

	return &featuresCategories, nil
}
