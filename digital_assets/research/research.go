package datastruct

import (
	"fmt"
	"os"
)

var ResearchAnalystsCollectionName = fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "research_analysts")
var ResearchFeaturedArticleCollectionName = fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "research")
var ResearchCollectionName = fmt.Sprintf("research%s", os.Getenv("DATA_NAMESPACE"))

type Analyst struct {
	Rank       int    `json:"rank" firestore:"rank"`
	Name       string `json:"name" firestore:"name"`
	Image      string `json:"image" firestore:"image"`
	Title      string `json:"title" firestore:"title"`
	Expertise  string `json:"expertise" firestore:"areaOfExpertise"`
	BioLink    string `json:"bioLink" firestore:"linkToBio"`
	FollowLink string `json:"followLink" firestore:"followUrl"`
	Slug       string `json:"slug" firestore:"slug"`
	NaturalId  string `json:"naturalId" firestore:"naturalId"`
}

type Research struct {
	Featured          Article   `json:"featured" firestore:"featuredArticle"`
	DataFeed          []Article `json:"dataFeed" firestore:"dataFeed"`
	Analysts          []Analyst `json:"analysts" firestore:"analysts"`
	DataFeedPaginated bool      `json:"dataFeedPaginated" firestore:"dataFeedPaginated"`
}

var ResearchArticlesQuery = `
	SELECT
		c.id,
		c.title,
		c.date date,
		c.description,
		c.image,
		c.author,
		c.authorType author_type,
		aut.type type,
		aut.inactive disabled,
		aut.seniorContributor senior_contributor,
		aut.bylineFormat byline_format,
		REPLACE(c.uri, "http://", "https://") AS link,
		REPLACE(aut.url, "http://", "https://") AS author_link,
	FROM
		api-project-901373404215.Content.` + os.Getenv("CONTENT_DATA_SET") + ` c
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
		AND "Digital Assets Research" IN UNNEST(c.bertieBadges)
	GROUP BY
		c.id,
		c.title,
		c.date,
		c.description,
		c.image,
		c.author,
		c.uri,
		aut.type,
		aut.url,
        c.authorType,
		aut.bylineFormat,
		aut.seniorContributor,
		aut.inactive
	ORDER BY
		date DESC,
		c.title DESC
	`

var ResearchArticleQuery = `
SELECT
	c.id,
	c.title,
	c.date date,
	c.description,
	c.image,
	c.author,
	c.authorType author_type,
	aut.type type,
	aut.inactive disabled,
	aut.seniorContributor senior_contributor,
	aut.bylineFormat byline_format,
	REPLACE(c.uri, "http://", "https://") AS link,
	REPLACE(aut.url, "http://", "https://") AS author_link,
FROM
	api-project-901373404215.Content.` + os.Getenv("CONTENT_DATA_SET") + ` c
LEFT JOIN
	api-project-901373404215.Content.v_author_latest aut
ON
	c.authorNaturalId = aut.naturalId
WHERE
	c.visible = TRUE
	AND c.preview = FALSE
	AND c.date <= CURRENT_TIMESTAMP()
	AND c.timestamp <= CURRENT_TIMESTAMP()
	AND "all" NOT IN UNNEST(spikeFrom)
	AND "Digital Assets Research" IN UNNEST(c.bertieBadges)
	AND c.id = @articleId
GROUP BY
	c.id,
	c.title,
	c.date,
	c.description,
	c.image,
	c.author,
	c.uri,
	aut.type,
	aut.url,
	c.authorType,
	aut.bylineFormat,
	aut.seniorContributor,
	aut.inactive
ORDER BY
	date DESC,
	c.title DESC
`
