package datastruct

import (
	"fmt"
	"os"
	"time"
)

// Collection name of where an news information is stored
var NewsCollectionName = fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "news")

type Article struct {
	DocId             string                 `json:"doc_id,omitempty" firestore:"doc_id"`             // Document ID from FS
	Id                string                 `json:"id" firestore:"id"`                               // Article ID
	Title             string                 `json:"title" firestore:"title"`                         // Article Title
	Image             string                 `json:"image" firestore:"image"`                         // Article Name
	ArticleURL        string                 `json:"articleURL" firestore:"articleURL"`               // Article URL
	Author            string                 `json:"author" firestore:"author"`                       // Author Name
	Type              string                 `json:"type" firestore:"type"`                           // Article Type
	AuthorType        string                 `json:"authorType" firestore:"authorType"`               // Author Type
	AuthorLink        string                 `json:"authorLink" firestore:"authorLink"`               // Author Link
	Description       string                 `json:"description" firestore:"description"`             // Article description
	PublishDate       time.Time              `json:"publishDate" firestore:"publishDate"`             // Article publishDate
	Disabled          bool                   `json:"disabled" firestore:"disabled"`                   // Author disabled
	SeniorContributor bool                   `json:"seniorContributor" firestore:"seniorContributor"` // Author seniorContributor
	BylineFormat      *int64                 `json:"bylineFormat" firestore:"bylineFormat"`           // bylineFormat
	BertieTag         string                 `json:"bertieTag" firestore:"bertieTag"`                 // Article bertieTag
	Order             int64                  `json:"order" firestore:"order"`                         // Article order
	IsFeaturedArticle bool                   `json:"isFeaturedArticle" firestore:"isFeaturedArticle"` // Article IsFeatured
	UpdatedAt         map[string]interface{} `json:"-" firestore:"_updatedBy,omitempty"`              // Article _updatedBy
	LastUpdated       time.Time              `json:"lastUpdated" firestore:"lastUpdated"`             // Article lastUpdated time
	NaturalID         string                 `json:"naturalid" firestore:"naturalid"`                 // article naturalid
}

type Topic struct {
	TopicName            string    `json:"topicName" firestore:"topicName"`                   // Topic Name
	BertieTag            string    `json:"bertieTag" firestore:"bertieTag"`                   // Bertie Tag we will use it to fetch all articles related to the topic
	Description          string    `json:"forbesMetaDataDescription" firestore:"description"` // topic Description
	IsTrending           bool      `json:"isTrending" firestore:"isTrending"`                 // Trending Tag for topic
	IsAsset              bool      `json:"isAsset" firestore:"isAsset"`                       // asset Tag for topic that will determine if this Topic is an asset
	IsFeaturedHome       bool      `json:"isFeaturedHome" firestore:"isFeaturedHome"`         // isFeaturedHome Tag for Categories that will determine if this Topic is an Featured category or not for Home Landing page
	Slug                 string    `json:"slug" firestore:"slug"`                             // topic Slug
	TopicURl             string    `json:"topicUrl" firestore:"topicUrl"`                     // topic url
	TopicOrder           int       `json:"topicOrder" firestore:"topicOrder"`                 // topic order we will use it for updating the trending topic for 24 hour
	TitleTemplate        string    `json:"titleTemplate" firestore:"titleTemplate"`           // topic title
	TopicPageDescription string    `json:"description" firestore:"topicPageDescription"`      // topic summary description
	NewsHeader           string    `json:"newsHeader" firestore:"newsHeader"`                 // topic header
	AliasesName          string    `json:"aliasesName" firestore:"aliasesName"`               // topic header
	Articles             []Article `json:"articles" firestore:"articles"`                     // topic articles
}

type LandingPageFeaturedCategories struct {
	Slug      string `json:"slug" firestore:"slug"`           // Category Slug we need to to get the articles that related to the category
	TopicName string `json:"topicName" firestore:"topicName"` // category Name  we use it to be displayed on Landing Page
}

type EditorsPick struct {
	PromotedContent PromotedContent `json:"promotedContent"`
}
type Magazine struct {
	IssueDate int64  `json:"issueDate"`
	PubName   string `json:"pubName"`
	IssueName string `json:"issueName"`
	PubDate   int64  `json:"pub_date"`
	PubName2  string `json:"pub_name"`
}

type ForbesTwitterProfile struct {
	ScreenName      string `json:"screenName"`
	Name            string `json:"name"`
	ProfileImageURL string `json:"profileImageUrl"`
	Description     string `json:"description"`
	CreatedDate     int64  `json:"createdDate"`
	Location        string `json:"location"`
	URL             string `json:"url"`
	ExpandedURL     string `json:"expandedUrl"`
	DisplayURL      string `json:"displayUrl"`
	Verified        bool   `json:"verified"`
}

type PrimaryAuthor struct {
	ID                   string               `json:"id"`
	NaturalID            string               `json:"naturalId"`
	Name                 string               `json:"name"`
	Avatars              []Avatars            `json:"avatars"`
	URL                  string               `json:"url"`
	Type                 string               `json:"type"`
	ProfileURL           string               `json:"profileUrl"`
	TwitterName          string               `json:"twitterName"`
	AuthorType           string               `json:"authorType"`
	LinkedIn             string               `json:"linkedIn"`
	Email                string               `json:"email"`
	TagName              string               `json:"tagName"`
	Blog                 bool                 `json:"blog"`
	Timestamp            int64                `json:"timestamp"`
	ShortBio             string               `json:"shortBio"`
	BlogName             string               `json:"blogName"`
	Topics               []string             `json:"topics"`
	Description          string               `json:"description"`
	RecentActivityCount  int                  `json:"recentActivityCount"`
	LatestActivityDate   int64                `json:"latestActivityDate"`
	DailyActivityCount   int                  `json:"dailyActivityCount"`
	PrimaryBlogNaturalID string               `json:"primaryBlogNaturalId"`
	ShortURI             string               `json:"shortUri"`
	DisplayChannel       string               `json:"displayChannel"`
	ContributorSince     int64                `json:"contributorSince"`
	Slug                 string               `json:"slug"`
	ShowNoVestPocket     bool                 `json:"showNoVestPocket"`
	Embargo              bool                 `json:"embargo"`
	PrimaryChannelID     string               `json:"primaryChannelId"`
	PrimarySectionID     string               `json:"primarySectionId"`
	GooglePlus           string               `json:"googlePlus"`
	EnableContribContact bool                 `json:"enableContribContact"`
	Sigfile              string               `json:"sigfile"`
	EnableTwitterFeed    bool                 `json:"enableTwitterFeed"`
	InstagramHandle      string               `json:"instagramHandle"`
	DisableCanonical     bool                 `json:"disableCanonical"`
	DisableDigest        bool                 `json:"disableDigest"`
	ForbesTwitterProfile ForbesTwitterProfile `json:"forbesTwitterProfile"`
	Inactive             bool                 `json:"inactive"`
	Division             string               `json:"division"`
	AllowEmail           bool                 `json:"allowEmail"`
	Disabled             bool                 `json:"disabled"`
	FreeNewsletter       bool                 `json:"freeNewsletter"`
	Amazon               string               `json:"amazon"`
	DisplaySection       string               `json:"displaySection"`
	SeniorContributor    bool                 `json:"seniorContributor"`
	AcceptedVersion      string               `json:"acceptedVersion"`
	AcceptedAt           int64                `json:"acceptedAt"`
	AcceptedVersionV2    string               `json:"acceptedVersionV2"`
	AcceptedAtV2         int64                `json:"acceptedAtV2"`
}

type PrimaryContributorData struct {
	ID                   string               `json:"id"`
	NaturalID            string               `json:"naturalId"`
	Name                 string               `json:"name"`
	Avatars              []Avatars            `json:"avatars"`
	URL                  string               `json:"url"`
	Type                 string               `json:"type"`
	ProfileURL           string               `json:"profileUrl"`
	TwitterName          string               `json:"twitterName"`
	AuthorType           string               `json:"authorType"`
	LinkedIn             string               `json:"linkedIn"`
	Email                string               `json:"email"`
	TagName              string               `json:"tagName"`
	Blog                 bool                 `json:"blog"`
	Timestamp            int64                `json:"timestamp"`
	ShortBio             string               `json:"shortBio"`
	BlogName             string               `json:"blogName"`
	Topics               []string             `json:"topics"`
	Description          string               `json:"description"`
	RecentActivityCount  int                  `json:"recentActivityCount"`
	LatestActivityDate   int64                `json:"latestActivityDate"`
	DailyActivityCount   int                  `json:"dailyActivityCount"`
	PrimaryBlogNaturalID string               `json:"primaryBlogNaturalId"`
	ShortURI             string               `json:"shortUri"`
	DisplayChannel       string               `json:"displayChannel"`
	ContributorSince     int64                `json:"contributorSince"`
	Slug                 string               `json:"slug"`
	ShowNoVestPocket     bool                 `json:"showNoVestPocket"`
	Embargo              bool                 `json:"embargo"`
	PrimaryChannelID     string               `json:"primaryChannelId"`
	PrimarySectionID     string               `json:"primarySectionId"`
	GooglePlus           string               `json:"googlePlus"`
	EnableContribContact bool                 `json:"enableContribContact"`
	Sigfile              string               `json:"sigfile"`
	EnableTwitterFeed    bool                 `json:"enableTwitterFeed"`
	InstagramHandle      string               `json:"instagramHandle"`
	DisableCanonical     bool                 `json:"disableCanonical"`
	DisableDigest        bool                 `json:"disableDigest"`
	ForbesTwitterProfile ForbesTwitterProfile `json:"forbesTwitterProfile"`
	Inactive             bool                 `json:"inactive"`
	Division             string               `json:"division"`
	AllowEmail           bool                 `json:"allowEmail"`
	Disabled             bool                 `json:"disabled"`
	FreeNewsletter       bool                 `json:"freeNewsletter"`
}

type Publication struct {
	ID                     string                 `json:"id"`
	NaturalID              string                 `json:"naturalId"`
	Name                   string                 `json:"name"`
	Avatars                []Avatars              `json:"avatars"`
	URL                    string                 `json:"url"`
	Type                   string                 `json:"type"`
	ProfileURL             string                 `json:"profileUrl"`
	TwitterName            string                 `json:"twitterName"`
	AuthorType             string                 `json:"authorType"`
	TagName                string                 `json:"tagName"`
	Blog                   bool                   `json:"blog"`
	Timestamp              int64                  `json:"timestamp"`
	Topics                 []string               `json:"topics"`
	Authors                []string               `json:"authors"`
	RecentActivityCount    int                    `json:"recentActivityCount"`
	LatestActivityDate     int64                  `json:"latestActivityDate"`
	DailyActivityCount     int                    `json:"dailyActivityCount"`
	PrimaryBlogNaturalID   string                 `json:"primaryBlogNaturalId"`
	PrimaryContributor     string                 `json:"primaryContributor"`
	PrimaryContributorData PrimaryContributorData `json:"primaryContributorData"`
	ShortURI               string                 `json:"shortUri"`
	DisplayChannel         string                 `json:"displayChannel"`
	ContributorSince       int64                  `json:"contributorSince"`
	Slug                   string                 `json:"slug"`
	ShowNoVestPocket       bool                   `json:"showNoVestPocket"`
	Embargo                bool                   `json:"embargo"`
	PrimaryChannelID       string                 `json:"primaryChannelId"`
	PrimarySectionID       string                 `json:"primarySectionId"`
	EnableContribContact   bool                   `json:"enableContribContact"`
	EnableTwitterFeed      bool                   `json:"enableTwitterFeed"`
	DisableCanonical       bool                   `json:"disableCanonical"`
	DisableDigest          bool                   `json:"disableDigest"`
	ForbesTwitterProfile   ForbesTwitterProfile   `json:"forbesTwitterProfile"`
	Inactive               bool                   `json:"inactive"`
	AllowEmail             bool                   `json:"allowEmail"`
	ContentPaywall         string                 `json:"contentPaywall"`
}

type AuthorGroup struct {
	PrimaryAuthor PrimaryAuthor `json:"primaryAuthor"`
	Publication   Publication   `json:"publication"`
	CoAuthors     []interface{} `json:"coAuthors"`
}

type ContentPositions struct {
	Position            int           `json:"position"`
	Type                string        `json:"type"`
	Title               string        `json:"title"`
	Image               string        `json:"image"`
	Description         string        `json:"description"`
	URI                 string        `json:"uri"`
	ID                  string        `json:"id"`
	Authors             []Authors     `json:"authors"`
	Date                int64         `json:"date"`
	BlogType            string        `json:"blogType"`
	NaturalID           string        `json:"naturalId"`
	BertieBadges        []interface{} `json:"bertieBadges"`
	Magazine            Magazine      `json:"magazine,omitempty"`
	HideDescription     bool          `json:"hideDescription"`
	FullImage           bool          `json:"fullImage"`
	Sponsored           bool          `json:"sponsored"`
	RemoveTopPadding    bool          `json:"removeTopPadding"`
	RemoveBottomPadding bool          `json:"removeBottomPadding"`
	AuthorGroup         AuthorGroup   `json:"authorGroup,omitempty"`
	BlogName            string        `json:"blogName"`
}

type PromotedContent struct {
	ContentPositions    []ContentPositions `json:"contentPositions"`
	Limit               int                `json:"limit"`
	SourceType          string             `json:"sourceType"`
	Source              string             `json:"source"`
	SourceValue         string             `json:"sourceValue"`
	Start               int                `json:"start"`
	More                bool               `json:"more"`
	EnableAds           bool               `json:"enableAds"`
	RemoveBVPrepend     bool               `json:"removeBVPrepend"`
	BrandvoiceHeader    bool               `json:"brandvoiceHeader"`
	RemovePadding       bool               `json:"removePadding"`
	FullImage           bool               `json:"fullImage"`
	RemoveTopPadding    bool               `json:"removeTopPadding"`
	RemoveBottomPadding bool               `json:"removeBottomPadding"`
	FullListLink        bool               `json:"fullListLink"`
	Pagination          bool               `json:"pagination"`
	Filters             bool               `json:"filters"`
	Year                int                `json:"year"`
	DirectLink          bool               `json:"directLink"`
}



type NewsFeedPayload struct {
	NewsFeedItems []NewsFeedResponse `json:"newsFeedItems" firestore:"newsFeedItems"`
}

type NewsFeedResponse struct {
	Title         string    `json:"title" firestore:"title"`
	URI           string    `json:"uri" firestore:"uri"`
	Date          time.Time `json:"date" firestore:"date"`
	Description   string    `json:"description" firestore:"description"`
	Image         string    `json:"image" firestore:"image"`
	PrimaryAuthor string    `json:"primaryAuthor" firestore:"primaryAuthor"`
	Publication   string    `json:"publication" firestore:"publication"`
	Author        struct {
		NaturalId         string   `json:"naturalId" firestore:"naturalId"`
		Name              string   `json:"name" firestore:"name"`
		Avatars           []Avatar `json:"avatars" firestore:"avatars"`
		AuthorUrl         string   `json:"url" firestore:"url"`
		Type              string   `json:"type" firestore:"type"`
		ProfileUrl        string   `json:"profileUrl" firestore:"profileUrl"`
		AuthorType        string   `json:"authorType" firestore:"authorType"`
		Blog              bool     `json:"blog" firestore:"blog"`
		BlogName          string   `json:"blogName" firestore:"blogName"`
		Slug              string   `json:"slug" firestore:"slug"`
		SeniorContributor bool     `json:"seniorContributor" firestore:"seniorContributor"`
		Disabled          bool     `json:"disabled" firestore:"disabled"`
	} `json:"author" firestore:"author"`
}

type Avatar struct {
	Size  int64  `json:"size" bigquery:"size"`
	Image string `json:"image" bigquery:"image"`
}