package datastruct

import (
	"fmt"
	"os"

	"cloud.google.com/go/bigquery"
)

var EditorsPickCollection = fmt.Sprintf("editorsPicks%s", os.Getenv("DATA_NAMESPACE"))

type EditorsPick struct {
	PromotedContent PromotedContent `json:"promotedContent"` // this is an object that contains all EditorsPick articles
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

type NewsFeedItem struct {
	Title             bigquery.NullString    `json:"title" bigquery:"title"`
	URI               bigquery.NullString    `json:"uri" bigquery:"uri"`
	Date              bigquery.NullTimestamp `json:"date" bigquery:"date"`
	Description       bigquery.NullString    `json:"description" bigquery:"description"`
	Author            bigquery.NullString    `json:"author" bigquery:"author"`
	Image             bigquery.NullString    `json:"image" bigquery:"image"`
	AuthorType        bigquery.NullString    `json:"authorType" bigquery:"authorType"`
	AuthorSlug        bigquery.NullString    `json:"authorSlug" bigquery:"authorSlug"`
	AuthType          bigquery.NullString    `json:"authType" bigquery:"authType"`
	SeniorContributor bigquery.NullBool      `json:"seniorContributor" bigquery:"seniorContributor"`
	Disabled          bigquery.NullBool      `json:"disabled" bigquery:"disabled"`
	PrimaryAuthor     bigquery.NullString    `json:"primaryAuthor" bigquery:"primaryAuthor"`
	Publication       bigquery.NullString    `json:"publication" bigquery:"publication"`
	NaturalId         bigquery.NullString    `json:"naturalId" bigquery:"naturalId"`
	AuthorUrl         bigquery.NullString    `json:"authorUrl" bigquery:"authorUrl"`
	Blog              bigquery.NullBool      `json:"blog" bigquery:"blog"`
	BlogName          bigquery.NullString    `json:"blogName" bigquery:"blogName"`
	ProfileUrl        bigquery.NullString    `json:"profileUrl" bigquery:"profileUrl"`
	Avatars           bigquery.NullString    `json:"avatars" bigquery:"avatars"`
}

const PrimaryAuthorQuery = `
SELECT 
	id, 
	name, 
	naturalId, 
	avatars, 
	url, 
	type, 
	profileUrl, 
	twitterName, 
	authorType, 
	linkedIn,
	email, 
	blog,
	CAST(UNIX_SECONDS(timestamp) AS INT64 ) as timestamp,
	shortBio,
	blogName,
	description,
	primaryBlogNaturalId,
	slug,
	showNoVestPocket,
	embargo,
	primaryChannelId,
	primarySectionId,
	enableContribContact,
	enableTwitterFeed,
	disableCanonical,
	disableDigest,
	inactive,
	division,
	allowEmail,
	seniorContributor,
	disabled
FROM 
	api-project-901373404215.Content.v_author_latest 
where 
	id = @id 
LIMIT 
	1
`

const PublicationQuery = `
SELECT 
	id,
	naturalId,
	name,
	avatars,
	url,
	type,
	authorType,
	blog,
	CAST(UNIX_SECONDS(timestamp) AS INT64 ) as timestamp,
	primaryContributor,
	primaryContributorData,
	contributorSince,
	slug,
	showNoVestPocket,
	primaryChannelId,
	primarySectionId,
	enableContribContact,
	enableTwitterFeed,
	disableCanonical,
	disableDigest,
	allowEmail
FROM 
	api-project-901373404215.Content.author_stage
where 
	id = @id 
	AND primaryChannelId = 'channel_115' 
LIMIT 
	1
`

const EditorsPickQuery = `
SELECT
	c.title,
	c.type,
	c.date date,
	c.templateType,
	c.templateSubType,
	c.image,
	c.author,
	c.authorType,
	aut.type authType,
	aut.inactive disabled,
	aut.seniorContributor,
	c.siteSlug authorSlug,
	c.authorGroup.primaryAuthor,
	c.authorGroup.publication,
	REPLACE(c.uri, "http://", "https://") AS uri,
FROM
	api-project-901373404215.Content.mv_content_latest c,
	UNNEST(c.bertieBadges) as bertieBadge
LEFT JOIN
	api-project-901373404215.Content.v_author_latest aut
ON
	c.authorNaturalId = aut.naturalId
LEFT JOIN
	api-project-901373404215.Content.channelsection cs
ON
	c.primaryChannelId = cs.channelId
	AND c.primarySectionId = cs.sectionId
WHERE
	c.visible = TRUE
	AND c.preview = FALSE
	AND c.date <= CURRENT_TIMESTAMP()
	AND c.timestamp <= CURRENT_TIMESTAMP()
	AND c.timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 day)
	AND "all" NOT IN UNNEST(spikeFrom)
	AND bertieBadge in ("Editors' Pick")
	AND c.primaryChannelId= "channel_115" #Forbes Digital Assets
GROUP BY
	1,
	2,
	3,
	4,
	5,
	6,
	7,
	8,
	9,
	10,
	11,
	12,
	13,
	14,
	15,
	cs.sectionName
ORDER BY
	date DESC,
	c.title DESC
LIMIT
	15
`
