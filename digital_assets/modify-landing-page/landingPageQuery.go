package repository

import (
	"context"
	"fmt"
	"os"

	"cloud.google.com/go/firestore"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
	"google.golang.org/api/iterator"
)

type LandingPageQuery interface {
	GetLandingPageFeaturedCategoriesArticles(ctx context.Context, categories []string) ([]datastruct.Topic, []string, error) // Gets landing Page Featured categories Articles
	GetLandingPageCategories(ctx context.Context) (*[]datastruct.LandingPageFeaturedCategories, error)                       // Get All landing page Featured Categories
	GetLandingPageFeaturedArticles(ctx context.Context, category string) (*datastruct.EditorsPick, *datastruct.NewsFeedPayload, error)
}

type landingPageQuery struct{}

// GetLandingPageFeaturedCategoriesArticles Gets all content for Landing Page Featured Categories
// Takes a context and Array of Categories
// Returns (*datastruct.Article, Error)
//
// Gets the Landing Page Featured Categories data from firestore
// Returns the Landing Page Featured Categories Articles and no error if successful
func (c *landingPageQuery) GetLandingPageFeaturedCategoriesArticles(ctx context.Context, categories []string) ([]datastruct.Topic, []string, error) {
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
			log.Error("Error Getting Categories FS: %s", err)
			span.SetStatus(codes.Error, err.Error())
			span.AddEvent(fmt.Sprintf("Error Getting Categories FS: %s", err))
			return nil, nil, err
		}

		dbSnap := fs.Collection(datastruct.NewsCollectionName).Doc(topic.TopicName).Collection("articles").Documents(ctx)

		for {
			var categoryArticles datastruct.Article
			doc, err := dbSnap.Next()
			if err == iterator.Done {
				break
			}

			if err := doc.DataTo(&categoryArticles); err != nil {
				log.Error("Error Getting Categories Articles FS: %s", err)
				span.SetStatus(codes.Error, err.Error())
				span.AddEvent(fmt.Sprintf("Error Getting Categories Articles FS: %s", err))
				return nil, nil, err
			}
			articles = append(articles, categoryArticles)
		}
		topic.Articles = articles
		featuredCategories = append(featuredCategories, topic)

	}

	log.EndTimeL(labels, "V2 LandingPageQuery.GetLandingPageFeaturedCategoriesArticles", startTime, nil)
	span.SetStatus(codes.Ok, "V2 LandingPageQuery.GetLandingPageFeaturedCategoriesArticles")

	return featuredCategories, categories, nil
}

// GetLandingPageFeaturedCategoriesArticles Gets all content for Landing Page Featured Categories
// Takes a context and Array of Categories
// Returns (*datastruct.Article, Error)
//
// Gets the Landing Page Featured Categories data from firestore
// Returns the Landing Page Featured Categories Articles and no error if successful
func (c *landingPageQuery) GetLandingPageFeaturedArticles(ctx context.Context, category string) (*datastruct.EditorsPick, *datastruct.NewsFeedPayload, error) {
	span, labels := common.GenerateSpan("V2 LandingPageQuery.GetLandingPageFeaturedCategoriesArticles", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 LandingPageQuery.GetLandingPageFeaturedCategoriesArticles"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 LandingPageQuery.GetLandingPageFeaturedCategoriesArticles"))
	// todo build switch case to get articles from category type (newsfedd, editoPicks)
	var editorsPicksData *datastruct.EditorsPick
	var newsFeedPayload *datastruct.NewsFeedPayload
	var err error
	switch category {
	case "editors":
		editorsPicksData, err = c.GetLandingPageFeaturedEditorsPickArticles(ctx)
		if err != nil {
			return nil, nil, err
		}
	case "feeds":
		newsFeedPayload, err = c.GetLandingPageFeaturedNewsFeedArticles(ctx)
		if err != nil {
			return nil, nil, err
		}
	}

	log.EndTimeL(labels, "V2 LandingPageQuery.GetLandingPageFeaturedCategoriesArticles", startTime, nil)
	span.SetStatus(codes.Ok, "V2 LandingPageQuery.GetLandingPageFeaturedCategoriesArticles")

	// todo return the result from the switch
	return editorsPicksData, newsFeedPayload, nil
}

// Todo build function that will returns newsfeed articles
func (c *landingPageQuery) GetLandingPageFeaturedNewsFeedArticles(ctx context.Context) (*datastruct.NewsFeedPayload, error) {
	fs := fsUtils.GetFirestoreClient()

	span, labels := common.GenerateSpan("V2 LandingPageQuery.GetLandingPageFeaturedCategoriesArticles", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 LandingPageQuery.GetLandingPageFeaturedCategoriesArticles"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 LandingPageQuery.GetLandingPageFeaturedCategoriesArticles"))
	var newsFeedPayload datastruct.NewsFeedPayload

	collectionName := fmt.Sprintf("pagedata%s", os.Getenv("DATA_NAMESPACE"))
	ds, err := fs.Collection(collectionName).Doc("newsfeed").Get(ctx)

	if err != nil {
		return &newsFeedPayload, err
	}

	err = ds.DataTo(&newsFeedPayload)
	if err != nil {
		return &newsFeedPayload, err
	}
	log.EndTimeL(labels, "V2 LandingPageQuery.GetLandingPageFeaturedCategoriesArticles", startTime, nil)
	span.SetStatus(codes.Ok, "V2 LandingPageQuery.GetLandingPageFeaturedCategoriesArticles")

	return &newsFeedPayload, nil
}

// Todo build function that will returns editorPicks articles
func (c *landingPageQuery) GetLandingPageFeaturedEditorsPickArticles(ctx context.Context) (*datastruct.EditorsPick, error) {
	fs := fsUtils.GetFirestoreClient()

	span, labels := common.GenerateSpan("V2 LandingPageQuery.GetLandingPageFeaturedCategoriesArticles", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 LandingPageQuery.GetLandingPageFeaturedCategoriesArticles"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 LandingPageQuery.GetLandingPageFeaturedCategoriesArticles"))
	var editorsPicksData datastruct.EditorsPick

	editorsPickCollection := fmt.Sprintf("editorsPicks%s", os.Getenv("DATA_NAMESPACE"))

	dbSnap, dataSnapErr := fs.Collection(editorsPickCollection).Doc("editorsPick_data").Get(ctx)

	if dataSnapErr != nil {
		return nil, dataSnapErr
	}

	if err := dbSnap.DataTo(&editorsPicksData); err != nil {
		return nil, err
	}
	log.EndTimeL(labels, "V2 LandingPageQuery.GetLandingPageFeaturedCategoriesArticles", startTime, nil)
	span.SetStatus(codes.Ok, "V2 LandingPageQuery.GetLandingPageFeaturedCategoriesArticles")

	return &editorsPicksData, nil
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
			log.Error("Error Getting Landing Page Categories Data from FS: %s", err)
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}
		featuresCategories = append(featuresCategories, featuresCategory)

	}

	log.EndTimeL(labels, "V2 LandingPageQuery.GetLandingPageCategories", startTime, nil)
	span.SetStatus(codes.Ok, "V2 LandingPageQuery.GetLandingPageCategories")

	return &featuresCategories, nil
}
