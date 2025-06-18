package services

import (
	"context"
	"math/rand"
	"sort"
	"strings"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/repository"
	"github.com/Forbes-Media/go-tools/log"
)

type TopicsService interface {
	GetTrendingTopics(ctx context.Context) ([]datastruct.TrendingTopics, error)       // Getting Trending Topics from FS
	GetNewsTopic(ctx context.Context, slug string) (*datastruct.Topic, error)         // Getting News Topics from FS
	GetNewsTopicCategories(ctx context.Context) ([]datastruct.TopicCategories, error) // Getting News Topics Categories from FS
	GetTopicBubbles(ctx context.Context) ([]datastruct.TopicsBubbles, error)          // Getting News Topics Bubbles from FS
	BuildNewsTopics(ctx context.Context) error                                        // Build News Topics from DS
	UpdateTrendingTopics(ctx context.Context) error                                   // UpdateTrending  News Topics to FS
	BuildNewsTopicsCategories(ctx context.Context) error                              // Build News Topics Categories
}

type topicsService struct {
	dao repository.DAO
}

// NewTopicsService Attempts to Get Access to all Topics functions
// Takes a repository.DAO so we can use our Query functions
// Returns (TopicsService)
//
// Takes the dao and return topicsService with dao to access all our topics functions  to get data from our FS
// Returns a TopicsService interface
func NewTopicsService(dao repository.DAO) TopicsService {
	return &topicsService{dao: dao}
}

// GetTrendingTopics Gets all Trending Topics form FS
// Takes a context
// Returns ([]datastruct.TrendingTopics, error)
//
// Gets the Trending Topics data from firestore
// Returns the Trending Topics and no error if successful
func (t *topicsService) GetTrendingTopics(ctx context.Context) ([]datastruct.TrendingTopics, error) {
	trendingTopics, err := t.dao.NewTopicsQuery().GetTrendingTopics(ctx)

	if err != nil {
		log.Error("%s", err)
		return nil, err
	}
	rand.Shuffle(len(trendingTopics), func(i, j int) { trendingTopics[i], trendingTopics[j] = trendingTopics[j], trendingTopics[i] })

	return trendingTopics, nil
}

// GetNewsTopic Gets news topics from FS
// Takes a (ctx context.Context, slug string)
// Returns (*datastruct.Topic, error)
//
// Gets the News Topic data from firestore using Slug
// Returns the News Topic and no error if successful
func (t *topicsService) GetNewsTopic(ctx context.Context, slug string) (*datastruct.Topic, error) {
	newsTopic, err := t.dao.NewTopicsQuery().GetNewsTopic(ctx, slug)

	if err != nil {
		log.Error("%s", err)
		return nil, err
	}
	t.SortArticles(newsTopic.Articles)
	return newsTopic, nil
}

// SortArticles Sort news topics Articles
// Takes a []datastruct.Article
//
// Sort the News Topic Articles data
func (t *topicsService) SortArticles(articles []datastruct.Article) {
	sort.Slice(articles, func(i, j int) bool {
		return articles[i].PublishDate.After(articles[j].PublishDate)
	})
}

// GetNewsTopicCategories Gets news topics categories from FS
// Takes a context
// Returns ([]datastruct.TopicCategories, error)
//
// Gets the News Topics Categories data from firestore
// Returns Array of News Topics Categories data and no error if successful
func (t *topicsService) GetNewsTopicCategories(ctx context.Context) ([]datastruct.TopicCategories, error) {
	topicCategories, err := t.dao.NewTopicsQuery().GetNewsTopicCategories(ctx)

	if err != nil {
		log.Error("%s", err)
		return nil, err
	}
	return topicCategories, nil
}

// GetTopicBubbles Gets news topics bubbles from FS
// Takes a context
// Returns ([]datastruct.TopicsBubbles, error)
//
// Gets the News Topics Bubbles data from firestore
// Returns Array of News Topics Bubbles data and no error if successful
func (t *topicsService) GetTopicBubbles(ctx context.Context) ([]datastruct.TopicsBubbles, error) {
	topicBubbles, err := t.dao.NewTopicsQuery().GetTopicBubbles(ctx)

	if err != nil {
		log.Error("%s", err)
		return nil, err
	}
	return topicBubbles, nil
}

// GetTopicBubbles Gets news topics bubbles from FS
// Takes a context
// Returns ([]datastruct.TopicsBubbles, error)
//
// Gets the News Topics Bubbles data from firestore
// Returns Array of News Topics Bubbles data and no error if successful
func (t *topicsService) BuildNewsTopics(ctx context.Context) error {
	newsTopics, err := t.dao.NewTopicsQuery().GetNewsTopics(ctx)
	if err != nil {
		log.Error("%s", err)
		return err
	}

	topics, err := t.MapAPIArticlesToFSTopic(ctx, newsTopics)
	if err != nil {
		log.Error("%s", err)
		return err
	}
	t.dao.NewTopicsQuery().SaveNewsTopics(ctx, topics)
	return nil
}

// MapAPIArticlesToFSTopic Map all news topics Articles
// Takes a (ctx context.Context, topics []datastruct.Topic)
// Returns ([]datastruct.Topic, error)
//
// Map all News Topics articles data and get the new articles from DysonSphere
// Returns Array of News Topics data and no error if successful
func (t *topicsService) MapAPIArticlesToFSTopic(ctx context.Context, topics []datastruct.Topic) ([]datastruct.Topic, error) {
	var newsTopics []datastruct.Topic

	for _, topic := range topics {
		name := strings.ToLower(strings.ReplaceAll(topic.BertieTag, " ", "%20"))
		// get Articles for topic from DysonSphere API
		articles := t.MapDSArticles(ctx, name, topic.BertieTag)
		var topicArticles []datastruct.Article
		for _, article := range articles {
			if topic.BertieTag == article.BertieTag {
				for _, sectionArticle := range topic.Articles {
					// if article exist in topic map the new value article to it
					if sectionArticle.NaturalID == article.NaturalID {
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
		t.SortArticles(topicArticles)
		topic.Articles = topicArticles
		newsTopics = append(newsTopics, topic)
	}
	return newsTopics, nil
}

// MapDSArticles Map all new Articles fro each topic from DS
// Takes a (ctx context.Context, name string, tag string)
// Returns []datastruct.Article
//
// Map articles data from DysonSphere
// Returns Array of articles from DysonSphere
func (t *topicsService) MapDSArticles(ctx context.Context, name string, tag string) []datastruct.Article {
	res, err := common.GetArticlesFromDysonSphere(ctx, name)

	if err != nil {
		log.Error("%s", err)
		return nil
	}
	var articles []datastruct.Article
	for _, data := range res {
		var article datastruct.Article
		article.Title = data.Title
		article.ArticleURL = data.Uri
		article.Image = data.Image
		article.Description = data.Description
		article.Type = data.PrimaryAuthor.Type
		article.AuthorType = data.PrimaryAuthor.AuthorType
		article.PublishDate = data.Timestamp
		article.Author = data.PrimaryAuthor.Name
		article.AuthorLink = data.PrimaryAuthor.AuthorLink
		article.SeniorContributor = data.PrimaryAuthor.SeniorContributor
		article.BylineFormat = &data.PrimaryAuthor.BylineFormat
		article.Disabled = data.PrimaryAuthor.Disabled
		article.BertieTag = tag
		article.NaturalID = data.NaturalId
		articles = append(articles, article)
	}

	return articles
}

// UpdateTrendingTopics Update News Trending Topics
// Takes a context
// Returns ([]datastruct.Topic, []datastruct.Topic)
//
// Update all News Trending Topics what this mean we will update 20 topics to be trending and remove the old 20 one
// Returns two Arrays of old trending and new trending topics
func (t *topicsService) UpdateTrendingTopics(ctx context.Context) error {

	var (
		trendingTopics    []datastruct.Topic
		notTrendingTopics []datastruct.Topic
	)
	topics, err := t.dao.NewTopicsQuery().GetNewsTopics(ctx)
	if err != nil {
		log.Error("%s", err)
		return err
	}
	for _, topic := range topics {

		if topic.IsTrending {
			trendingTopics = append(trendingTopics, topic)
		} else {
			notTrendingTopics = append(notTrendingTopics, topic)
		}
	}
	var lastTopic datastruct.Topic
	if len(trendingTopics) > 0 {
		lastTopic = trendingTopics[len(trendingTopics)-1]
	} else {
		lastTopic = notTrendingTopics[0]
	}
	order := lastTopic.TopicOrder
	// build the new trending topics
	topicResult := t.BuildTrendingTopicArray(ctx, trendingTopics, notTrendingTopics, order)

	t.dao.NewTopicsQuery().UpdateIsTrendingTopics(ctx, topicResult, trendingTopics)

	return nil
}

// BuildTrendingTopicArray Build Trending Topics
// Takes a (ctx context.Context, trendingTopics []datastruct.Topic, notTrendingTopics []datastruct.Topic, topicIndex int)
// Returns []datastruct.Topic
//
// Build new trending topics from the topics that flagged as not trending.
// Returns Array of trending topics
func (t *topicsService) BuildTrendingTopicArray(ctx context.Context, trendingTopics []datastruct.Topic, notTrendingTopics []datastruct.Topic, topicIndex int) []datastruct.Topic {
	var topicResult []datastruct.Topic
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
		firstIndex, lastIndex = t.BuildTrendingTopicBoundaries(firstIndex, lastIndex, len(notTrendingTopics))
		topicResult = append(topicResult, notTrendingTopics[firstIndex:lastIndex]...)
	} else {
		firstIndex = topicIndex - trendingTopicCount
		lastIndex = res + firstIndex
		firstIndex, lastIndex = t.BuildTrendingTopicBoundaries(firstIndex, lastIndex, len(notTrendingTopics))
		topicResult = append(topicResult, notTrendingTopics[firstIndex:lastIndex]...)

	}
	if len(topicResult) < trendingTopicCount {
		t := trendingTopicCount - len(topicResult)
		topicResult = append(topicResult, notTrendingTopics[0:t]...)
	} else if len(topicResult) > trendingTopicCount { // add this to ensure the updated trending topics will be equal to 20 each time it update it
		topicResult = topicResult[0:20]
	}

	return topicResult
}

// BuildTrendingTopicBoundaries Build Trending Topics Boundaries
// Takes a (firstIndex int, lastIndex int, topicsLen int)
// Returns (int, int)
//
// Build Trending Topics Boundaries
// This function will take first and last index for trending topic and len of not trending topic.
// It will check if the first and last index in valid boundaries and return it.
// Returns (int, int) first and last index that we will use to build the new trending topics
func (t *topicsService) BuildTrendingTopicBoundaries(firstIndex int, lastIndex int, topicsLen int) (int, int) {
	// check it the first index out of notTrendingTopics range
	if firstIndex > topicsLen {
		firstIndex = firstIndex - topicsLen
	} else if firstIndex < 0 {
		firstIndex = 0
	}
	// check it the last index out of notTrendingTopics range
	if lastIndex > topicsLen {
		lastIndex = topicsLen
	}
	return firstIndex, lastIndex
}

// BuildNewsTopicsCategories Build news topics categories into FS
// Takes a context
// Returns  error
//
// Build the News Topics categories data into firestore
// Returns no error if successful
func (t *topicsService) BuildNewsTopicsCategories(ctx context.Context) error {
	newsTopicsCategories, err := t.dao.NewTopicsQuery().BuildNewsTopicsCategories(ctx)
	if err != nil {
		log.Error("%s", err)
		return err
	}
	t.dao.NewTopicsQuery().SaveNewsTopicsCategories(ctx, newsTopicsCategories)
	return nil
}
