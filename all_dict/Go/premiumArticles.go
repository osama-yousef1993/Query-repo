package services

import (
	"context"
	"fmt"
	"os"

	"github.com/Forbes-Media/go-tools/log"
	otelCodes "go.opentelemetry.io/otel/codes"
	"google.golang.org/api/iterator"
)

const premiumArticlesQuery = `
	SELECT distinct
		c.id,
		c.title,
		c.date date,
		c.description,
		c.image,
		c.author,
		c.authorType author_type,
		c.naturalId,
		aut.type type,
		aut.inactive disabled,
		aut.seniorContributor senior_contributor,
		aut.bylineFormat byline_format,
		REPLACE(c.uri, "http://", "https://") AS link,
		REPLACE(aut.url, "http://", "https://") AS author_link
	FROM
		api-project-901373404215.Content.mv_content_latest c,
		UNNEST(c.channelSection) AS channelSection,
		UNNEST(c.bertieBadges) AS bertieTag
	LEFT JOIN
		api-project-901373404215.Content.v_author_latest aut
	ON
		c.authorNaturalId = aut.naturalId
	WHERE
		c.visible = TRUE
		AND c.preview = FALSE
		AND "all" NOT IN UNNEST(spikeFrom)
		AND ( 
			c.primaryChannelId = "channel_115"
			OR 
			channelSection = "channel_115"
			)
		And contentPaywall = 'premium'
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
		14
	order by 
		c.date desc
`

type ContentList struct {
	PremiumArticles []PremiumArticles `json:"contentList" firestore:"id"`
}
type PremiumArticles struct {
	Id                         string                     `json:"id" firestore:"id"`
	Title                      string                     `json:"title" firestore:"title"`
	NaturalID                  string                     `json:"naturalId" firestore:"naturalid"`
	Image                      string                     `json:"image" firestore:"image"`
	ArticleURL                 string                     `json:"uri" firestore:"articleURL"`
	Description                string                     `json:"description" firestore:"description"`
	PublishDate                int64                      `json:"date" firestore:"publishDate"`
	ContentPayWall             string                     `json:"contentPaywall" firestore:"contentPaywall"`
	PremiumArticlesAuthorGroup PremiumArticlesAuthorGroup `json:"authorGroup" firestore:"authorGroup"`
}

type PremiumArticlesAuthorGroup struct {
	PrimaryAuthor PrimaryAuthor `json:"primaryAuthor" firestore:"primaryAuthor"`
}

// Get all premium articles from BQ and Build Articles Array to be inserted to FS
func BuildRecommendedPremiumArticlesBQ(ctx0 context.Context) ([]EducationArticle, error) {

	client := GetBQClient()

	ctx, span := tracer.Start(ctx0, "BuildRecommendedPremiumArticles")

	defer span.End()

	queryResult := client.Query(premiumArticlesQuery)

	it, err := queryResult.Read(ctx)

	if err != nil {
		log.Error("Error Getting Premium Articles Data from BQ: %s", err)
		span.AddEvent(fmt.Sprintf("Error Getting Premium Articles Data from BQ: %s", err))
		span.SetStatus(otelCodes.Error, err.Error())
		return nil, err
	}

	var premiumArticles []EducationArticle
	span.AddEvent("Start Map Premium Articles Data")

	imageDomain := os.Getenv("ARTICLES_IMAGE_DOMAIN")

	for {
		var article EducationArticle
		var articleFromBQ EducationArticleFromBQ

		err := it.Next(&articleFromBQ)

		if err == iterator.Done {
			break
		}

		if err != nil {
			log.Error("Error Map Premium Articles Data to Struct: %s", err)
			span.AddEvent(fmt.Sprintf("Error Map Premium Articles Data to Struct: %s", err))
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}

		if articleFromBQ.Id.Valid {
			article.Id = articleFromBQ.Id.StringVal
		}
		if articleFromBQ.NaturalID.Valid {
			article.NaturalID = articleFromBQ.NaturalID.StringVal
		}
		if articleFromBQ.Title.Valid {
			article.Title = articleFromBQ.Title.StringVal
		}
		if articleFromBQ.Image.Valid {
			article.Image = imageDomain + articleFromBQ.Image.StringVal
		}
		if articleFromBQ.Author.Valid {
			article.Author = articleFromBQ.Author.StringVal
		}
		if articleFromBQ.AuthorLink.Valid {
			article.AuthorLink = articleFromBQ.AuthorLink.StringVal
		}
		if articleFromBQ.AuthorType.Valid {
			article.AuthorType = articleFromBQ.AuthorType.StringVal
		}
		if articleFromBQ.Description.Valid {
			article.Description = articleFromBQ.Description.StringVal
		}
		if articleFromBQ.ArticleURL.Valid {
			article.ArticleURL = articleFromBQ.ArticleURL.StringVal
		}
		if articleFromBQ.Type.Valid {
			article.Type = articleFromBQ.Type.StringVal
		}
		if articleFromBQ.Disabled.Valid {
			article.Disabled = articleFromBQ.Disabled.Bool
		}
		if articleFromBQ.SeniorContributor.Valid {
			article.SeniorContributor = articleFromBQ.SeniorContributor.Bool
		}
		if articleFromBQ.BertieTag.Valid {
			article.BertieTag = articleFromBQ.BertieTag.StringVal
		}
		if articleFromBQ.BylineFormat.Valid {
			article.BylineFormat = &articleFromBQ.BylineFormat.Int64
		} else {
			article.BylineFormat = nil
		}
		article.PublishDate = articleFromBQ.PublishDate

		premiumArticles = append(premiumArticles, article)

	}

	return premiumArticles, nil
}


%5B%7B%22contentPaywall%22%3A%5B%22premium%22%5D%7D%2C%20%7B%22primaryChannelId%22%3A%5B%22channel_115%22%5D%7D%5D

[{"contentPaywall":["premium"]}, {"primaryChannelId":["channel_115"]}, {"channelSection":["channel_115"]} ]
[{"contentPaywall":["premium"]}, {"channelSection":[{"channelId": "channel_115"}]}]
%5B%7B%22contentPaywall%22%3A%5B%22premium%22%5D%7D%2C%20%7B%22channelSection%22%3A%5B%7B%22channelId%22%3A%22channel_115%22%7D%5D%7D%5D
%5B%7B%22contentPaywall%22%3A%5B%22premium%22%5D%7D%2C%20%7B%22channelSection%22%3A%5B%7B%22channelId%22%3A%22channel_115%22%7D%5D%7D%5D



// get all premium articles from Forbes API using FDA channel and contentPaywall
func GetPremiumArticlesDataFromForbesAPI(ctx context.Context) (*ForbesPremiumArticles, error) {

	_, span := tracer.Start(ctx, "GetPremiumArticlesDataFromForbesAPI")

	defer span.End()

	span.AddEvent("Start Getting Premium Articles Data")

	/*
		QueryFilter that ForbesAPI Accept To got the premium Articles.
		- limit ---> the response limit.
		- queryfilters ---> it will be in this format [{"contentPaywall":["premium"]}, {"channelSection.channelId":["channel_115"]}].
			- contentPaywall ---> we will use it because we need all the Premium Articles and it take value "premium".
			- channelSection.channelId ---> we will use it to determine the channel Id data will return from it in our case it will be FDA channel "channel_115".
			* Some of our premium articles doesn't return from ForbesAPI,
			* So in queryfilters we change PrimaryChannelId to channelSection.channelId because there are few articles that should be return with the response for our channel.
			* So we will use now channelSection to get our premium articles from ForbesAPI.
		- retrievedfields ---> it will present the fields will be included in response.
		- I f we need any new field to be returned we should add it to retrievedfields and we can see it in the response.
			- title --> article Title.
			- date --> article PublishDate.
			- description --> article Description.
			- image --> article Image.
			- author --> article author.
			- authorGroup --> authorGroup it will contains all author data that we need.
				- AuthorType --> author Type.
				- Badges --> Bertie Badges.
				- Name --> author Name.
				- SeniorContributor --> author SeniorContributor.
				- Disabled
				- BylineFormat
				- AuthorLink --> author AuthorLink.
				- Type --> author Type.
			- naturalId --> article NaturalId we need it to use it as unique Key.
			- primaryChannelId --> article PrimaryChannelId to check if the data back from FDA channel.
			- type --> article Type.
			- uri --> article URL.
			- contentPaywall --> article contentPaywall to check if this article is premium articles.
	*/
	url := fmt.Sprintf("%s%s", forbesURL, "content/all.json?limit=100&queryfilters=%5B%7B%22contentPaywall%22%3A%5B%22premium%22%5D%7D%2C%20%7B%22channelSection.channelId%22%3A%22channel_115%22%7D%5D&retrievedfields=title%2Cdate%2Cdescription%2Cimage%2Cauthor%2CauthorGroup%2CnaturalId%2CprimaryChannelId%2Ctype%2Curi%2CcontentPaywall")

	ContentList, err := MakeForbesAPIRequest[ForbesPremiumArticles](ctx, url, "GET")
	if err != nil {
		span.AddEvent(fmt.Sprintf("Error : %s", err))
		return nil, err
	}

	return ContentList, nil
}