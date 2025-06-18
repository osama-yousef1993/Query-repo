} else {
	artLen := 2
	var uniqueArticles []string
	for i := 0; i < artLen; i++ {
		for j := 0; j < categoriesLen; j++ {
			c.SortEducationArticles(featuredCategories[j].Articles)
			if len(featuredCategories[j].Articles) > 0 {
				uniqueArticles = append(uniqueArticles, featuredCategories[j].Articles[i].NaturalID)
				if len(featuredCategories[j].Articles) < artLen {
					if !slices.Contains(uniqueArticles, featuredCategories[j].Articles[0].NaturalID) {
						articles = append(articles, featuredCategories[j].Articles[0])
					}
				} else {
					if !slices.Contains(uniqueArticles, featuredCategories[j].Articles[0].NaturalID) {
						articles = append(articles, featuredCategories[j].Articles[i])
					}
				}
			}
			if len(articles) >= 12 {
				goto END
			}
		}
	}
}





package services

import (
	"context"
	"sort"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/repository"
	"github.com/Forbes-Media/go-tools/log"
)

type LandingPageService interface {
	GetLandingPageFeaturedCategoriesArticles(ctx context.Context, categories []string) ([]datastruct.Article, error) // Returns all Landing Page Featured Categories Articles
	GetLandingPageCategories(ctx context.Context) (*[]datastruct.LandingPageFeaturedCategories, error)               // Returns all LandingPage Featured Categories
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
	categories, articles, err := c.dao.NewLandingPageQuery().GetLandingPageFeaturedCategoriesArticles(ctx, categories)
	if err != nil {
		log.Error("%s", err)
		return nil, err
	}

	c.SortEducationArticles(articles)
	latestArticles = c.GetLatest12Articles(categories, articles)

	return latestArticles, nil
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
func (c *landingPageService) GetLatest12Articles(categories []string, featuredArticles []datastruct.Article) []datastruct.Article {
	articlesLen := len(featuredArticles)
	var articles []datastruct.Article

	if len(categories) > 0 {
		// Start append only one Article from each section for each loop
		uniqueArticles := make(map[string]string)
		for j := 0; j < articlesLen; j++ {
			nID := featuredArticles[j].NaturalID
			if uniqueArticles[nID] == "" {
				uniqueArticles[nID] = nID
				articles = append(articles, featuredArticles[j])
			}
			if len(articles) >= 12 {
				goto END
			}
		}
	} else {
		artLen := 2
		uniqueArticles := make(map[string]string)
		for i := 0; i < artLen; i++ {
			for j := 0; j < articlesLen; j++ {
				nID := featuredArticles[j].NaturalID
				if uniqueArticles[nID] == "" {
					uniqueArticles[nID] = nID
					articles = append(articles, featuredArticles[j])
				}
				if len(articles) >= 12 {
					goto END
				}
			}
		}
	}
	// topic count that exist in latest articles
END:
	return articles
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






// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

func (c *landingPageService) GetLatest12ArticlesNew(featuredCategories []datastruct.Topic, categories []string) []datastruct.Article {
	categoriesLen := len(featuredCategories)
	var articles []datastruct.Article
	var length int
	minLen, maxLen, index := c.GetMinMaxValue(featuredCategories)
	// create a map to store the article IDs in this case we will ensure in this way we will have the unique articles only will added to response.
	articleIDs := make(map[string]bool)
	if categoriesLen > 1 {
		if categories != nil {
			// Start append only one Article from each section for each loop
			for i := 0; i < minLen; i++ {
				for j := 0; j < categoriesLen; j++ {
					// check if the article ID is already in the map
					nID := featuredCategories[j].Articles[i].NaturalID
					if _, ok := articleIDs[nID]; !ok {
						// if not, add it to the map and the articles slice
						articleIDs[nID] = true
						articles = append(articles, featuredCategories[j].Articles[i])
						if len(articles) >= 12 {
							goto END
						}
					}
				}
			}
		} else {
			artLen := 2
			for i := 0; i < artLen; i++ {
				for j := 0; j < categoriesLen; j++ {
					c.SortEducationArticles(featuredCategories[j].Articles)
					if len(featuredCategories[j].Articles) > 0 {
						nID := featuredCategories[j].Articles[i].NaturalID
						if len(featuredCategories[j].Articles) < artLen {
							// check if the article ID is already in the map
							if _, ok := articleIDs[nID]; !ok {
								// if not, add it to the map and the articles slice
								articleIDs[nID] = true
								articles = append(articles, featuredCategories[j].Articles[0])
							}
						} else {
							// check if the article ID is already in the map
							if _, ok := articleIDs[nID]; !ok {
								// if not, add it to the map and the articles slice
								articleIDs[nID] = true
								articles = append(articles, featuredCategories[j].Articles[i])
							}
						}
					}
					if len(articles) >= 12 {
						goto END
					}
				}
			}
		}
		// topic count that exist in latest articles
		minLenTopic := c.GetTopicCount(featuredCategories[index].TopicName, articles)
		for i := minLenTopic; i < maxLen; i++ {
			// check if the article ID is already in the map
			nID := featuredCategories[index].Articles[i].NaturalID
			if _, ok := articleIDs[nID]; !ok {
				// if not, add it to the map and the articles slice
				articleIDs[nID] = true
				articles = append(articles, featuredCategories[index].Articles[i])
				if len(articles) >= 12 {
					goto END
				}
			}
		}
	} else {
		c.SortEducationArticles(featuredCategories[0].Articles)
		articlesLength := len(featuredCategories[0].Articles)
		if articlesLength > 12 {
			length = 12
		} else {
			length = articlesLength
		}
		for i := 0; i < length; i++ {
			nID := featuredCategories[0].Articles[i].NaturalID
			// check if the article ID is already in the map
			if _, ok := articleIDs[nID]; !ok {
				// if not, add it to the map and the articles slice
				articleIDs[nID] = true
				articles = append(articles, featuredCategories[0].Articles[i])
			}
		}
	}
END:
	return articles
}
