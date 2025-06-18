package services

import (
	"context"
	"fmt"
	"os"
	"sort"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/api/iterator"
)

type Section struct {
	Name         string             `json:"name" firestore:"name"`
	BertieTag    string             `json:"bertieTag" firestore:"bertieTag"`
	Description  string             `json:"description" firestore:"description"`
	Order        string             `json:"articleOrder" firestore:"order"`
	SectionOrder int64              `json:"sectionOrder" firestore:"sectionOrder"`
	Articles     []EducationArticle `json:"articles" firestore:"articles"`
}
type EducationArticle struct {
	Id                string    `json:"id" firestore:"id"`
	Title             string    `json:"title" firestore:"title"`
	Image             string    `json:"image" firestore:"image"`
	ArticleURL        string    `json:"articleURL" firestore:"articleURL"`
	Author            string    `json:"author" firestore:"author"`
	Type              string    `json:"type" firestore:"type"`
	AuthorType        string    `json:"authorType" firestore:"authorType"`
	AuthorLink        string    `json:"authorLink" firestore:"authorLink"`
	Description       string    `json:"description" firestore:"description"`
	PublishDate       time.Time `json:"publishDate" firestore:"publishDate"`
	Disabled          bool      `json:"disabled" firestore:"disabled"`
	SeniorContributor bool      `json:"seniorContributor" firestore:"seniorContributor"`
	BylineFormat      *int64    `json:"bylineFormat" firestore:"bylineFormat"`
	PrimaryChannelId  string    `json:"primaryChannelId" firestore:"primaryChannelId"`
	ChannelSection    string    `json:"channelSection" firestore:"channelSection"`
}

type Education struct {
	Section []Section `json:"sections" firestore:"sections"`
}

// education data query

const articlesQuery = `
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
	REPLACE(aut.url, "http://", "https://") AS author_link
FROM
api-project-901373404215.Content.mv_content_latest c,
	UNNEST(c.channelSection) as channelSection
	LEFT JOIN
	api-project-901373404215.Content.v_author_latest aut
	ON
	c.authorNaturalId = aut.naturalId
WHERE
	c.visible = TRUE
	AND c.preview = FALSE
	AND c.date <= CURRENT_TIMESTAMP()
	AND c.timestamp <= CURRENT_TIMESTAMP()
	AND c.timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 day)
	AND "all" NOT IN UNNEST(spikeFrom)
	AND ( 
		channelSection in (@sectionName) 
		or
		c.primaryChannelId= @sectionName
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
	13
ORDER BY
	@orderColumn  DESC
`

func GetEducationContentFromBertie(sectionName string, span trace.Span, contentDataSet string, order string) ([]EducationArticle, error) {
	ctx := context.Background()
	client, err := bigquery.NewClient(context.Background(), "api-project-901373404215")
	if err != nil {
		span.AddEvent(fmt.Sprintf("Error Connecting to BQ: %s", err))
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	span.AddEvent("Start Get Articles Data from BQ")

	var orderColumn string

	if order == "" {
		orderColumn = "date"
	} else {
		orderColumn = order
	}

	queryResult := client.Query(articlesQuery)
	queryResult.Parameters = []bigquery.QueryParameter{
		{
			Name:  "sectionName",
			Value: sectionName,
		},
		{
			Name:  "orderColumn",
			Value: orderColumn,
		},
	}

	it, err := queryResult.Read(ctx)
	if err != nil {
		log.Error("Error Getting Articles Data from BQ: %s", err)
		span.AddEvent(fmt.Sprintf("Error Getting Articles Data from BQ: %s", err))
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	var imageDomain string
	if contentDataSet == "mv_content_latest" {
		imageDomain = ""
	} else {
		imageDomain = "https://staging.damapi.forbes.com"
	}

	var educationArticle []EducationArticle
	for {
		var articale EducationArticle
		var articleFromBQ ArticleFromBQ
		err := it.Next(&articleFromBQ)
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Error("Error Map Articles Data to Struct: %s", err)
			span.AddEvent(fmt.Sprintf("Error Map Articles Data to Struct: %s", err))
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}

		if articleFromBQ.Id.Valid {
			articale.Id = articleFromBQ.Id.StringVal
		}
		if articleFromBQ.Title.Valid {
			articale.Title = articleFromBQ.Title.StringVal
		}
		if articleFromBQ.Image.Valid {
			articale.Image = imageDomain + articleFromBQ.Image.StringVal
		}
		if articleFromBQ.Author.Valid {
			articale.Author = articleFromBQ.Author.StringVal
		}
		if articleFromBQ.AuthorLink.Valid {
			articale.AuthorLink = articleFromBQ.AuthorLink.StringVal
		}
		if articleFromBQ.AuthorType.Valid {
			articale.AuthorType = articleFromBQ.AuthorType.StringVal
		}
		if articleFromBQ.Description.Valid {
			articale.Description = articleFromBQ.Description.StringVal
		}
		if articleFromBQ.ArticleURL.Valid {
			articale.ArticleURL = articleFromBQ.ArticleURL.StringVal
		}
		if articleFromBQ.Type.Valid {
			articale.Type = articleFromBQ.Type.StringVal
		}
		if articleFromBQ.Disabled.Valid {
			articale.Disabled = articleFromBQ.Disabled.Bool
		}
		if articleFromBQ.SeniorContributor.Valid {
			articale.SeniorContributor = articleFromBQ.SeniorContributor.Bool
		}
		if articleFromBQ.BylineFormat.Valid {
			articale.BylineFormat = &articleFromBQ.BylineFormat.Int64
		} else {
			articale.BylineFormat = nil
		}
		articale.PublishDate = articleFromBQ.PublishDate

		educationArticle = append(educationArticle, articale)
	}

	return educationArticle, nil
}

func GetEducationSectionData(span trace.Span) ([]Section, error) {
	fs := GetFirestoreClient()
	ctx := context.Background()

	sectionCollection := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "education")

	dbSnap := fs.Collection(sectionCollection).Documents(ctx)
	span.AddEvent("Start Get Section Data from FS")

	var sectionEducation []Section

	var sectionsName []string
	for {
		var section Section
		doc, err := dbSnap.Next()

		if err == iterator.Done {
			break
		}

		if err := doc.DataTo(&section); err != nil {
			log.Error("Error Getting Section Data from FS: %s", err)
			span.AddEvent(fmt.Sprintf("Error Getting Section Data from FS: %s", err))
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}

		// articles, err := GetEducationContentFromBertie(section.Name, span, "mv_content_latest", section.Order)
		// if err != nil {
		// 	log.Error("Error Getting Articles from Bertie BQ: %s", err)
		// 	span.SetStatus(codes.Error, err.Error())
		// 	span.AddEvent(fmt.Sprintf("Error Getting Articles from Bertie BQ: %s", err))
		// 	return nil, err
		// }
		// section.Articles = articles
		sectionsName = append(sectionsName, section.Name)

		sectionEducation = append(sectionEducation, section)

	}

	articles, err := NewGetEducationContentFromBertie(sectionsName, span, "mv_content_latest")

	if err != nil {
		log.Error("Error Getting Articles from Bertie BQ: %s", err)
		span.SetStatus(codes.Error, err.Error())
		span.AddEvent(fmt.Sprintf("Error Getting Articles from Bertie BQ: %s", err))
		return nil, err
	}

	sections, err := SortBuildSections(sectionEducation, articles)
	if err != nil {
		log.Error("Error Map Articles to Sections: %s", err)
		span.SetStatus(codes.Error, err.Error())
		span.AddEvent(fmt.Sprintf("Error Map Articles to Sections: %s", err))
		return nil, err
	}
	return sections, nil

}

func GetEducationData(span trace.Span) (*Education, error) {
	var educationData Education

	span.AddEvent("Start Build Education Data")
	sections, err := GetEducationSectionData(span)

	if err != nil {
		log.Error("Error Getting Sections from FS:  %s", err)
		span.AddEvent(fmt.Sprintf("Error Getting Sections from FS: %s", err))
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	SortSections(sections)
	educationData.Section = sections

	return &educationData, nil
}

func SortSections(sections []Section) {
	sort.Slice(sections, func(i, j int) bool {
		return sections[j].SectionOrder > sections[i].SectionOrder
	})
}

const newArticlesQuery = `
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
	c.primaryChannelId,
	aut.bylineFormat byline_format,
	REPLACE(c.uri, "http://", "https://") AS link,
	REPLACE(aut.url, "http://", "https://") AS author_link,
	channelSection
FROM
api-project-901373404215.Content.mv_content_latest c,
	UNNEST(c.channelSection) as channelSection
	LEFT JOIN
	api-project-901373404215.Content.v_author_latest aut
	ON
	c.authorNaturalId = aut.naturalId
WHERE
	c.visible = TRUE
	AND c.preview = FALSE
	AND c.date <= CURRENT_TIMESTAMP()
	AND c.timestamp <= CURRENT_TIMESTAMP()
	AND c.timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 day)
	AND "all" NOT IN UNNEST(spikeFrom)
	AND ( 
		channelSection in UNNEST(@sectionsName)
		or
		c.primaryChannelId in  UNNEST(@sectionsName)
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
    15
`

func NewGetEducationContentFromBertie(sectionsName []string, span trace.Span, contentDataSet string) ([]EducationArticle, error) {
	ctx := context.Background()
	client, err := bigquery.NewClient(context.Background(), "api-project-901373404215")
	if err != nil {
		span.AddEvent(fmt.Sprintf("Error Connecting to BQ: %s", err))
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	span.AddEvent("Start Get Articles Data from BQ")

	queryResult := client.Query(newArticlesQuery)
	queryResult.Parameters = []bigquery.QueryParameter{
		{
			Name:  "sectionsName",
			Value: sectionsName,
		},
	}

	it, err := queryResult.Read(ctx)
	if err != nil {
		log.Error("Error Getting Articles Data from BQ: %s", err)
		span.AddEvent(fmt.Sprintf("Error Getting Articles Data from BQ: %s", err))
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	var imageDomain string
	if contentDataSet == "mv_content_latest" {
		imageDomain = ""
	} else {
		imageDomain = "https://staging.damapi.forbes.com"
	}

	var educationArticle []EducationArticle
	for {
		var articale EducationArticle
		var articleFromBQ ArticleFromBQ
		err := it.Next(&articleFromBQ)
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Error("Error Map Articles Data to Struct: %s", err)
			span.AddEvent(fmt.Sprintf("Error Map Articles Data to Struct: %s", err))
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}

		if articleFromBQ.Id.Valid {
			articale.Id = articleFromBQ.Id.StringVal
		}
		if articleFromBQ.Title.Valid {
			articale.Title = articleFromBQ.Title.StringVal
		}
		if articleFromBQ.Image.Valid {
			articale.Image = imageDomain + articleFromBQ.Image.StringVal
		}
		if articleFromBQ.Author.Valid {
			articale.Author = articleFromBQ.Author.StringVal
		}
		if articleFromBQ.AuthorLink.Valid {
			articale.AuthorLink = articleFromBQ.AuthorLink.StringVal
		}
		if articleFromBQ.AuthorType.Valid {
			articale.AuthorType = articleFromBQ.AuthorType.StringVal
		}
		if articleFromBQ.Description.Valid {
			articale.Description = articleFromBQ.Description.StringVal
		}
		if articleFromBQ.ArticleURL.Valid {
			articale.ArticleURL = articleFromBQ.ArticleURL.StringVal
		}
		if articleFromBQ.Type.Valid {
			articale.Type = articleFromBQ.Type.StringVal
		}
		if articleFromBQ.Disabled.Valid {
			articale.Disabled = articleFromBQ.Disabled.Bool
		}
		if articleFromBQ.SeniorContributor.Valid {
			articale.SeniorContributor = articleFromBQ.SeniorContributor.Bool
		}
		if articleFromBQ.PrimaryChannelId.Valid {
			articale.PrimaryChannelId = articleFromBQ.PrimaryChannelId.StringVal
		}
		if articleFromBQ.ChannelSection.Valid {
			articale.ChannelSection = articleFromBQ.ChannelSection.StringVal
		}
		if articleFromBQ.BylineFormat.Valid {
			articale.BylineFormat = &articleFromBQ.BylineFormat.Int64
		} else {
			articale.BylineFormat = nil
		}
		articale.PublishDate = articleFromBQ.PublishDate

		educationArticle = append(educationArticle, articale)
	}

	return educationArticle, nil
}

func SortBuildSections(sections []Section, articles []EducationArticle) ([]Section, error) {
	var educationSection []Section
	for _, section := range sections {
		var educationArticles []EducationArticle
		for _, article := range articles {
			if section.Name == article.PrimaryChannelId || section.Name == article.ChannelSection {
				educationArticles = append(educationArticles, article)
			}
		}
		SortArticles(educationArticles, section.Order)
		section.Articles = educationArticles
		educationSection = append(educationSection, section)
	}

	return educationSection, nil
}

func SortArticles(articles []EducationArticle, order string) {
	switch order {
	case "date":
		sort.Slice(articles, func(i, j int) bool {
			return articles[i].PublishDate.After(articles[j].PublishDate)
		})
	case "title":
		sort.Slice(articles, func(i, j int) bool {
			return articles[i].Title < articles[j].Title
		})
	case "id":
		sort.Slice(articles, func(i, j int) bool {
			return articles[i].Id < articles[j].Id
		})
	default:
		sort.Slice(articles, func(i, j int) bool {
			return articles[i].PublishDate.After(articles[j].PublishDate)
		})
	}
}






// list all the tickers of an exchange
func (c *client) GetExchangesTickers(id string, opts *ExchangesTickersOptions) (*ExchangesTickers, map[string][]string, error) {
	var exchangesTickers ExchangesTickers

	data, headers, err := c.getWithHeaders(fmt.Sprintf("/exchanges/%s/tickers", id), opts)
	if err != nil {
		return nil, nil, err
	}
	err = c.unmarshal(data, &exchangesTickers)

	if err != nil {
		return nil, nil, err
	}

	return &exchangesTickers, headers, nil
}


// get makes a GET request to CoinGeko API. It returns the response body and error.
func (c *client) getWithHeaders(path string, queryMap interface{}) (string, map[string][]string, error) {
	url := fmt.Sprintf("%s%s", c.baseURL, path)
	var headers map[string][]string
	queryParams, err := query.Values(queryMap)
	if err != nil {
		return "", nil, err
	}

	url = fmt.Sprintf("%s?%s", url, queryParams.Encode())
	fmt.Println(url)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Println(err)
		return "", nil, err
	}

	if c.apiKey != "" {
		req.Header.Set("x-cg-pro-api-key", c.apiKey)
	}

	if c.headers != nil {
		for k, v := range c.headers {
			req.Header.Set(k, v)
		}
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		log.Println(err)
		return "", nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", nil, fmt.Errorf("unexpected resonse status code of %s", resp.Status)
	}

	headers = resp.Header

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
		return "", nil, err
	}

	bodyString := string(bodyBytes)

	if resp.StatusCode == http.StatusOK {
		return bodyString, headers, nil
	}

	log.Println(resp.Status)
	log.Println(bodyString)

	return "", nil, fmt.Errorf("%s", resp.Status)
}


















Slug                        string    `json:"slug"`

func GetExchangeProfilePG(ctxO context.Context, slug string) ([]byte, error) {

	_, span := tracer.Start(ctxO, "GetExchangeProfilePG")
	defer span.End()
	startTime := StartTime("Get Exchange Profile Data Query")
	pg := PGConnect()

	query := `
	select 
		id, name, slug, year, description, location, logo_url,
		website_url, twitter_url, facebook_url, youtube_url, 
		linkedin_url, reddit_url, chat_url, slack_url, telegram_url, 
		blog_url, centralized, decentralized, has_trading_incentive, 
		trust_score, trust_score_rank, trade_volume_24h_btc, 
		trade_volume_24h_btc_normalized, last_updated
	from (
		SELECT 
			id, name, lower(concat(Replace(name,' ', '-'), '-', id)) as slug, 
			year, description, location, logo_url,
			website_url, twitter_url, facebook_url, youtube_url, 
			linkedin_url, reddit_url, chat_url, slack_url, telegram_url, 
			blog_url, centralized, decentralized, has_trading_incentive, 
			trust_score, trust_score_rank, trade_volume_24h_btc, 
			trade_volume_24h_btc_normalized, last_updated
		FROM 
			public.coingecko_exchange_metadata

		) as m
	where 
		slug = '` + slug + `';
	`

	var exchangeProfile model.CoingeckoExchangeMetadata

	queryResult, err := pg.Query(query)

	if err != nil {
		span.SetStatus(otelCodes.Error, err.Error())
		ConsumeTime("Exchange Profile Data Scan", startTime, err)
		return nil, err
	}
	defer queryResult.Close()
	for queryResult.Next() {

		err := queryResult.Scan(&exchangeProfile.ID, &exchangeProfile.Name, &exchangeProfile.Slug, &exchangeProfile.Year, &exchangeProfile.Description, &exchangeProfile.Location, &exchangeProfile.LogoURL, &exchangeProfile.WebsiteURL, &exchangeProfile.TwitterURL, &exchangeProfile.FacebookURL, &exchangeProfile.YoutubeURL, &exchangeProfile.LinkedinURL, &exchangeProfile.RedditURL, &exchangeProfile.ChatURL, &exchangeProfile.SlackURL, &exchangeProfile.TelegramURL, &exchangeProfile.BlogURL, &exchangeProfile.Centralized, &exchangeProfile.Decentralized, &exchangeProfile.HasTradingIncentive, &exchangeProfile.TrustScore, &exchangeProfile.TrustScoreRank, &exchangeProfile.TradeVolume24HBTC, &exchangeProfile.TradeVolume24HBTCNormalized, &exchangeProfile.LastUpdated)
		if err != nil {
			span.SetStatus(otelCodes.Error, err.Error())
			ConsumeTime("Exchange Profile Data Scan", startTime, err)
			return nil, err
		}

	}
	ConsumeTime("Exchange Profile Data Query", startTime, nil)
	span.SetStatus(otelCodes.Ok, "Success")

	return json.Marshal(exchangeProfile)
}


data, err := store.GetExchangeProfilePG(r.Context(), slug)