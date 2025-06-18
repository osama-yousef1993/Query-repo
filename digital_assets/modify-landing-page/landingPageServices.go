package services

import (
	"context"
	"sort"
	"time"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/repository"
	"github.com/Forbes-Media/go-tools/log"
)

type LandingPageService interface {
	GetLandingPageFeaturedCategoriesArticles(ctx context.Context, categories []string) ([]datastruct.Article, error) // Returns all Landing Page Featured Categories Articles
	GetLandingPageCategories(ctx context.Context) (*[]datastruct.LandingPageFeaturedCategories, error)               // Returns all LandingPage Featured Categories
	GetLandingPageFeaturedArticles(ctx context.Context, categories []string) ([]datastruct.Article, error)
}

// Create object for the service that contains a repository.landingPage interface
type landingPageService struct {
	dao repository.DAO
}

// NewLandingPageService Attempts to Get Access to all Landing Page functions
// Takes a repository.DAO so we can use our Query functions
// Returns (LandingPageService)
//
// Takes the dao and return landingPageService with dao to access all our functions in Landing page to get data from our FS
// Returns a LandingPageService interface for Landing Page
func NewLandingPageService(dao repository.DAO) LandingPageService {
	return &landingPageService{dao: dao}
}

// GetLandingPageFeaturedCategoriesArticles Attempts to Get Landing Page Featured Categories information
// Takes a context and Array of categories
// Returns (*datastruct.LandingPageResponse, error)
//
// Takes the context and categories and get the LandingPage Featured Categories data
// Returns a *datastruct.LandingPageResponse with all of the Top latest Articles info for Landing Page
func (c *landingPageService) GetLandingPageFeaturedCategoriesArticles(ctx context.Context, categories []string) ([]datastruct.Article, error) {

	var latestArticles []datastruct.Article
	landingPageContent, categories, err := c.dao.NewLandingPageQuery().GetLandingPageFeaturedCategoriesArticles(ctx, categories)
	if err != nil {
		log.Error("%s", err)
		return nil, err
	}
	if len(landingPageContent) > 0 {
		sortedLandingPageContent := c.SortTopicArticles(landingPageContent)
		latestArticles = c.GetLatest12Articles(sortedLandingPageContent, categories)
	}

	c.SortEducationArticles(latestArticles)
	return latestArticles, nil
}

// GetLandingPageFeaturedCategoriesArticles Attempts to Get Landing Page Featured Categories information
// Takes a context and Array of categories
// Returns (*datastruct.LandingPageResponse, error)
//
// Takes the context and categories and get the LandingPage Featured Categories data
// Returns a *datastruct.LandingPageResponse with all of the Top latest Articles info for Landing Page
func (c *landingPageService) GetLandingPageFeaturedArticles(ctx context.Context, categories []string) ([]datastruct.Article, error) {

	var latestArticles []datastruct.Article
	editor := categories[0]
	latestCategories := categories[1:]
	if len(latestCategories) > 0 {
		landingPageContent, newCategories, err := c.dao.NewLandingPageQuery().GetLandingPageFeaturedCategoriesArticles(ctx, latestCategories)
		if err != nil {
			log.Error("%s", err)
			return nil, err
		}
		if len(landingPageContent) > 0 {
			sortedLandingPageContent := c.SortTopicArticles(landingPageContent)
			res := c.GetLatest12Articles(sortedLandingPageContent, newCategories)
			if len(latestCategories) > 1 {
				articlesNumber := len(latestCategories)
				latestNewsNumber := int(12 / len(categories))
				articlesLimit := latestNewsNumber * articlesNumber
				latestArticles = append(latestArticles, res[0:articlesLimit]...)
			} else {
				latestArticles = append(latestArticles, res[0:6]...)
			}
		}
	}
	editorContent, newsFeedContent, err := c.dao.NewLandingPageQuery().GetLandingPageFeaturedArticles(ctx, editor)
	if err != nil {
		log.Error("%s", err)
		return nil, err
	}
	if editorContent != nil {
		// todo create function to get the articles from editorContent
		articles := c.MapEditorArticles(editorContent)
		c.SortEducationArticles(articles)
		articlesLimit := 12 - len(latestArticles)
		// todo then append it to latestArticles
		latestArticles = append(latestArticles, articles[0:articlesLimit]...)
		// sortedLandingPageContent := c.SortTopicArticles(landingPageContent)
		// res := c.GetLatest12Articles(sortedLandingPageContent, categories)
		// latestArticles = append(latestArticles, res...)

	}
	// todo I need to add function to get data from newsfeed and editerpicks
	if newsFeedContent != nil {
		// todo create function to get the articles from editorContent
		articles := c.MapNewsFeedArticles(newsFeedContent)
		c.SortEducationArticles(articles)
		articlesLimit := 12 - len(latestArticles)
		// todo then append it to latestArticles
		latestArticles = append(latestArticles, articles[0:articlesLimit]...)
	}

	c.SortEducationArticles(latestArticles)
	return latestArticles, nil
}

func (c *landingPageService) MapEditorArticles(editorsPick *datastruct.EditorsPick) []datastruct.Article {
	var articles []datastruct.Article

	for _, edArticle := range editorsPick.PromotedContent.ContentPositions {
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
		article.BertieTag = "editorsPick"
		article.Order = 0
		article.IsFeaturedArticle = false
		article.NaturalID = edArticle.NaturalID

		articles = append(articles, article)

	}
	return articles
}
func (c *landingPageService) MapNewsFeedArticles(newsFeedArticles *datastruct.NewsFeedPayload) []datastruct.Article {
	var articles []datastruct.Article

	for _, nfArticle := range newsFeedArticles.NewsFeedItems {
		var article datastruct.Article
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
		article.BertieTag = "newsfeed"
		article.Order = 0
		article.IsFeaturedArticle = false

		articles = append(articles, article)

	}
	return articles
}

// GetLandingPageCategories Attempts to Get Landing Page Featured Categories information
// Takes a context and Array of categories
// Returns (*datastruct.LandingPageFeaturedCategories, error)
//
// Takes the context and categories and get the LandingPage Featured Categories data
// Returns a *datastruct.LandingPageFeaturedCategories with all of the Featured Categories info for Landing Page
func (c *landingPageService) GetLandingPageCategories(ctx context.Context) (*[]datastruct.LandingPageFeaturedCategories, error) {

	landingPageCategories, err := c.dao.NewLandingPageQuery().GetLandingPageCategories(ctx)
	if err != nil {
		log.Error("%s", err)
		return nil, err
	}
	return landingPageCategories, nil
}

// SortEducationArticles Sort all Articles that exist in Featured Categories
// Takes a array of articles
//
// Returns sorted articles by latest published date
func (c *landingPageService) SortEducationArticles(articles []datastruct.Article) {
	sort.Slice(articles, func(i, j int) bool {
		return articles[i].PublishDate.After(articles[j].PublishDate)
	})
}

// GetLatest12Articles Build Array of latest Articles
// Takes a array of featuredCategories with it's articles and List of categories
// Returns ([]datastruct.Article)
//
// first it will check the Categories list we have two cases:
//   - if the list contains more than one item it will loop over it and add latest articles from both items
//   - if the list contains only one item it will return 12 latest articles from it
//
// it will Loop over List of categories and generate new List of Latest Articles from All Categories in the List of categories
// Returns Top 12 latest articles that exist in featured Categories
func (c *landingPageService) GetLatest12Articles(featuredCategories []datastruct.Topic, categories []string) []datastruct.Article {
	categoriesLen := len(featuredCategories)
	var articles []datastruct.Article
	var length int
	minLen, _, _ := c.GetMinMaxValue(featuredCategories)
	// create a map to store the article IDs in this case we will ensure in this way we will have the unique articles only will added to response.
	articleIDs := make(map[string]bool)
	// this process has two options
	// First option we have a query filter added with two or more topics, Or we don't add any query filter
	if categoriesLen > 1 {
		// First Option
		// If we have a query filter.
		// this will start looping from the Topic that has lowest number of articles because we don;t need to have an error (index out of range if we choose it randomly)
		// then we need to loop over all topics and store the top article from each one of them.
		// ex: if we choose three Topics (bitcoin, xrp, shiba) the result will be 4 articles from each one of these Topics.
		// Second Option
		// If we don't have any query filter.
		// this mean we will deal with all featured Topics.
		// we will looping over all topics to add the top 2 articles in each one of them.
		// We need to ensure that articles not duplicated in response so we will add only unique articles to our result
		for i := 0; i < minLen; i++ {
			for j := 0; j < categoriesLen; j++ {
				c.SortEducationArticles(featuredCategories[j].Articles)
				// check if the article ID is already in the map
				nID := featuredCategories[j].Articles[i].NaturalID
				if _, ok := articleIDs[nID]; !ok {
					// if not, add it to the map and the articles slice
					articleIDs[nID] = true
					articles = append(articles, featuredCategories[j].Articles[i])
					if len(articles) >= 12 {
						return articles
					}
				}
			}
		}
	} else {
		// If we send only one topic in Query filter we will use this process
		c.SortEducationArticles(featuredCategories[0].Articles)
		articlesLength := len(featuredCategories[0].Articles)
		if articlesLength > 12 {
			length = 12
		} else {
			length = articlesLength
		}
		articles = append(articles, featuredCategories[0].Articles[0:length]...)
	}
	return articles
}

// GetMinMaxValue Get the max, min values and index values for Featured categories
// Takes a array of featuredCategories with it's articles
// Returns (int, int, int)
//
// GetMinMaxValue loop over featuredCategories to see which featured Category has the maximum number of articles and which has the minimum number of articles and the index for the maximum
// because we need to add more articles to the generate list if the list not contains 12 latest articles in this case we will use the index to add the missing number of articles
// Returns max, min values and index for featuredCategories
func (c *landingPageService) GetMinMaxValue(categories []datastruct.Topic) (int, int, int) {
	minLength := len(categories[0].Articles)
	maxLength := len(categories[0].Articles)
	maxLengthIndex := 0
	categoriesLen := len(categories)

	for i := 1; i < categoriesLen; i++ {
		artLength := len(categories[i].Articles)
		if artLength > 0 {
			if artLength < minLength {
				minLength = artLength
			} else if artLength > maxLength {
				maxLength = artLength
				maxLengthIndex = i
			}
		}
	}
	return minLength, maxLength, maxLengthIndex
}

// SortTopicArticles Build Array of sorted articles from topics
// Takes a array of Topic with it's articles
// Returns []datastruct.Topic
//
// SortTopicArticles will help us to insure the articles sorted correctly before we build our response
// Returns Array of Topics after sorted articles for each topic
func (c *landingPageService) SortTopicArticles(topics []datastruct.Topic) []datastruct.Topic {
	var sortedTopics []datastruct.Topic
	for _, topic := range topics {
		c.SortEducationArticles(topic.Articles)
		sortedTopics = append(sortedTopics, topic)
	}
	return sortedTopics
}

// GetTopicCount return the topic count exist in our response
// Takes a (string, []datastruct.Article)
// Returns int
//
// GetTopicCount will use this function to get the count of topic that exist in latest articles response.
// we will use this count to add the missing articles to latest articles response if the response not equals to 12 articles
// this count will put us on correct index to add the latest article from the selected Topic
// Returns count of existing topic in latest articles
func (c *landingPageService) GetTopicCount(topicName string, articles []datastruct.Article) int {
	var count int
	for _, art := range articles {
		if art.BertieTag == topicName {
			count++
		}
	}
	return count
}
