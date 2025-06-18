package datastruct

import (
	"fmt"
	"os"
	"time"
)

var TopicsCollectionName = fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "news")
var TopicsCategoriesCollectionName = fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "category_news")
var TopicsBubblesCollection = fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "news_topics_bubbles")

type Topic struct {
	TopicName            string    `json:"topicName" firestore:"topicName"`                   // Topic Name
	BertieTag            string    `json:"bertieTag" firestore:"bertieTag"`                   // Bertie Tag we will use it to fetch all articles related to the topic
	Description          string    `json:"forbesMetaDataDescription" firestore:"description"` // topic Description
	IsTrending           bool      `json:"isTrending" firestore:"isTrending"`                 // Trending Tag for topic
	IsAsset              bool      `json:"isAsset" firestore:"isAsset"`                       // asset Tag for topic that will determine if this Topic is an asset
	IsFeaturedHome       bool      `json:"isFeaturedHome" firestore:"isFeaturedHome"`         // isFeaturedHome Tag for Categories that will determine if this Topic is an Featured Category or not for Home Landing page
	Slug                 string    `json:"slug" firestore:"slug"`                             // topic Slug
	TopicURl             string    `json:"topicUrl" firestore:"topicUrl"`                     // topic url
	TopicOrder           int       `json:"topicOrder" firestore:"topicOrder"`                 // topic order we will use it for updating the trending topic for 24 hour
	TitleTemplate        string    `json:"titleTemplate" firestore:"titleTemplate"`           // topic title
	TopicPageDescription string    `json:"description" firestore:"topicPageDescription"`      // topic summary description
	NewsHeader           string    `json:"newsHeader" firestore:"newsHeader"`                 // topic header
	AliasesName          string    `json:"aliasesName" firestore:"aliasesName"`               // topic header
	Articles             []Article `json:"articles" firestore:"articles"`                     // topic articles
}

// Trending Topic Tags
type TrendingTopics struct {
	DocId     string `json:"doc_id,omitempty" firestore:"doc_id"`
	TopicName string `json:"topicName" firestore:"topicName"` // Topic NAme will display in FE
	Slug      string `json:"slug" firestore:"slug"`           // Topic Slug Will use for the news topic page
	TopicURL  string `json:"topicUrl" firestore:"topicUrl"`   // Topic Slug Will use for the news topic page
	IsAsset   bool   `json:"isAsset" firestore:"isAsset"`     // IsAsset flag will use it to determine if the Topic is an assets Topic or normal news Topic
}
type TopicsBubbles struct {
	TopicName string `json:"topicName,omitempty" firestore:"topicName"` // Topic NAme will display in FE
	Slug      string `json:"slug,omitempty" firestore:"slug"`           // Topic Slug Will use for the news topic page
}

type TopicCategories struct {
	CategoryName   string           `json:"name" firestore:"categoryName"` // The category name Will use for Explore more section
	CategoryTopics []TrendingTopics `json:"topics" firestore:"topics"`     // Topics that related to Category
}

// this struct we use it to map the data that returned from DysonSphere API
type ArticleResponse struct {
	Body              string          `json:"body"`
	MatchingWords     []string        `json:"matching_words"`
	NaturalId         string          `json:"naturalid"`
	PrimaryChannelId  string          `json:"primaryChannelId"`
	PV                int64           `json:"pv"`
	Source            string          `json:"source"`
	Timestamp         time.Time       `json:"timestamp"`
	Title             string          `json:"title"`
	Type              string          `json:"type"`
	BertieBadges      string          `json:"bertieBadges"`
	PrimaryAuthor     DSPrimaryAuthor `json:"primary_author"`
	Uri               string          `json:"uri"`
	Image             string          `json:"image"`
	Description       string          `json:"description"`
	Disabled          bool            `json:"disabled"`
	SeniorContributor string          `json:"seniorContributor"`
	BylineFormat      int             `json:"bylineFormat"`
}

type DSPrimaryAuthor struct {
	AuthorType        string   `json:"authorType"`
	Badges            []string `json:"badges"`
	AuthorNaturalId   string   `json:"authorNaturalId"`
	Email             string   `json:"email"`
	Name              string   `json:"name"`
	PrimaryChannelId  string   `json:"primaryChannelId"`
	SeniorContributor bool     `json:"seniorContributor"`
	Disabled          bool     `json:"disabled"`
	BylineFormat      int64    `json:"bylineFormat"`
	AuthorLink        string   `json:"url"`
	Type              string   `json:"type"`
}

type FundamentalsData struct {
	Symbol string `json:"symbol" firestore:"symbol" postgres:"symbol"`
	Name   string `json:"name" firestore:"name" postgres:"name"`
	Slug   string `json:"slug" firestore:"slug" postgres:"slug"`
}
