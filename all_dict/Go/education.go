package datastruct

import (
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/bigquery"
)

var EducationCollectionName = fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "text_education")

// articles query from Bigquery to get the articles data
const ArticlesQuery = `
WITH
  content_data AS (
  SELECT
    c.id,
    c.naturalid,
    c.title,
    c.date,
    c.timestamp,
    c.description,
    c.image,
    c.author,
    c.authorType AS author_type,
    aut.type,
    aut.inactive AS disabled,
    aut.seniorContributor AS senior_contributor,
    aut.bylineFormat AS byline_format,
    REPLACE(c.uri, "http://", "https://") AS link,
    REPLACE(aut.url, "http://", "https://") AS author_link,
    bertieTag
  FROM
    api-project-901373404215.Content.mv_content_latest c
  LEFT JOIN
    UNNEST(c.channelSection) AS channelSection,
    UNNEST(c.bertieBadges) AS bertieTag
  LEFT JOIN
    api-project-901373404215.Content.v_author_latest aut
  ON
    c.authorNaturalId = aut.naturalId
  WHERE
    c.visible = TRUE
    AND c.preview = FALSE
    AND c.date <= CURRENT_TIMESTAMP()
    AND c.timestamp <= CURRENT_TIMESTAMP()
    AND c.timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 180 day)
    AND "all" NOT IN UNNEST(spikeFrom)
    AND (c.primaryChannelId = "channel_115"
      OR channelSection = "channel_115")
    AND ( bertieTag IN UNNEST(@bertieTag)
      OR bertieTag IN (@learnTag)
      )
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
    16
  ORDER BY
    c.date DESC )
SELECT
  f.id,
  f.naturalid,
  f.title,
  f.date,
  f.timestamp,
  f.description,
  f.image,
  f.author,
  f.author_type,
  f.type,
  f.disabled,
  f.senior_contributor,
  f.byline_format,
  f.link,
  f.author_link,
  e.bertieTag
FROM
  content_data f
LEFT JOIN
  content_data e
ON
  e.id = f.id
  AND e.title = f.title
  AND f.bertieTag = @learnTag
WHERE
  f.bertieTag IS NOT NULL
  AND e.bertieTag IS NOT NULL
  AND f.bertieTag != e.bertieTag
`

type Section struct {
	DocId        string             `json:"doc_id,omitempty" firestore:"doc_id"`   // it's Id from FS
	Name         string             `json:"name" firestore:"name"`                 // section name
	BertieTag    string             `json:"bertieTag" firestore:"bertieTag"`       // section bertie tag
	Description  string             `json:"description" firestore:"description"`   // section description
	SectionOrder int64              `json:"sectionOrder" firestore:"sectionOrder"` // section order
	SectionImage string             `json:"sectionImage" firestore:"sectionImage"` // section image
	Articles     []EducationArticle `json:"articles" firestore:"articles"`         // section articles
}
type EducationArticle struct {
	DocId             string                 `json:"doc_id,omitempty" firestore:"doc_id"`             // document Id for article created by FS
	Id                string                 `json:"id" firestore:"id"`                               // Article Id
	Title             string                 `json:"title" firestore:"title"`                         // Article title
	Image             string                 `json:"image" firestore:"image"`                         // Article image
	ArticleURL        string                 `json:"articleURL" firestore:"articleURL"`               // Article Url
	Author            string                 `json:"author" firestore:"author"`                       // Article author
	Type              string                 `json:"type" firestore:"type"`                           // Article type
	AuthorType        string                 `json:"authorType" firestore:"authorType"`               // Article author type
	AuthorLink        string                 `json:"authorLink" firestore:"authorLink"`               // Article Author Link
	Description       string                 `json:"description" firestore:"description"`             // Article description
	PublishDate       time.Time              `json:"publishDate" firestore:"publishDate"`             // Article publishDate when the article is published
	Disabled          bool                   `json:"disabled" firestore:"disabled"`                   // author active
	SeniorContributor bool                   `json:"seniorContributor" firestore:"seniorContributor"` // author  seniorContributor
	BylineFormat      *int64                 `json:"bylineFormat" firestore:"bylineFormat"`           // author bylineFormat
	BertieTag         string                 `json:"bertieTag" firestore:"bertieTag"`                 // article bertieTag
	Order             int64                  `json:"order" firestore:"order"`                         // article order
	IsFeaturedArticle bool                   `json:"isFeaturedArticle" firestore:"isFeaturedArticle"` // article is Featured Article
	UpdatedAt         map[string]interface{} `json:"-" firestore:"_updatedBy,omitempty"`              // article update by
	LastUpdated       time.Time              `json:"lastUpdated" firestore:"lastUpdated"`             // article last updated
	NaturalID         string                 `json:"naturalid" firestore:"naturalid"`                 // article naturalid
}

type EducationArticleFromBQ struct {
	Id                bigquery.NullString `bigquery:"id" json:"id" firestore:"id"`
	NaturalID         bigquery.NullString `bigquery:"naturalId" json:"naturalId" firestore:"naturalId"`
	Title             bigquery.NullString `bigquery:"title" json:"title" firestore:"title"`
	Image             bigquery.NullString `bigquery:"image" json:"image" firestore:"image"`
	ArticleURL        bigquery.NullString `bigquery:"link" json:"articleURL" firestore:"articleURL"`
	Author            bigquery.NullString `bigquery:"author" json:"author" firestore:"author"`
	AuthorType        bigquery.NullString `bigquery:"author_type" json:"authorType" firestore:"authorType"`
	Type              bigquery.NullString `bigquery:"type" json:"type" firestore:"type"`
	AuthorLink        bigquery.NullString `bigquery:"author_link" json:"authorLink" firestore:"authorLink"`
	Description       bigquery.NullString `bigquery:"description" json:"description" firestore:"description"`
	PublishDate       time.Time           `bigquery:"date" json:"publishDate" firestore:"publishDate"`
	Disabled          bigquery.NullBool   `bigquery:"disabled" json:"disabled" firestore:"disabled"`
	SeniorContributor bigquery.NullBool   `bigquery:"senior_contributor" json:"seniorContributor" firestore:"seniorContributor"`
	BylineFormat      bigquery.NullInt64  `bigquery:"byline_format" json:"bylineFormat" firestore:"bylineFormat"`
	BertieTag         bigquery.NullString `bigquery:"bertieTag" json:"bertieTag" firestore:"bertieTag"`
}

type Education struct {
	Section []Section `json:"sections" firestore:"sections"` // all education sections with it's data and articles
}

type LandingPageEducation struct {
	Education      Education          `json:"education"` // the object for Learn tab it will be the same
	LatestArticles []EducationArticle `json:"articles"`  // it will contains the latest Articles for specific Categories selected by user
}
