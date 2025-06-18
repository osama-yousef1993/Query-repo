package store

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"cloud.google.com/go/bigquery"
	"github.com/Forbes-Media/forbes-digital-assets/model"
	rfCommon "github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/go-tools/log"
	"github.com/google/uuid"
	"github.com/lib/pq"
	otelCodes "go.opentelemetry.io/otel/codes"
)

type Fundamentals struct {
	ForbesID                  string                      `json:"forbes_id" firestore:"forbes_id" postgres:"forbes_id"`
	Symbol                    string                      `json:"symbol" firestore:"symbol" postgres:"symbol"`
	Name                      string                      `json:"name" firestore:"name" postgres:"name"`
	Slug                      string                      `json:"slug" firestore:"slug" postgres:"slug"`
	Logo                      string                      `json:"logo" firestore:"logo" postgres:"logo"`
	FloatType                 string                      `json:"floatType" firestore:"floatType" postgres:"float_type"`
	DisplaySymbol             string                      `json:"displaySymbol" firestore:"displaySymbol" postgres:"display_symbol"`
	Source                    string                      `json:"source" firestore:"source" postgres:"source"`
	TemporaryDataDelay        bool                        `json:"temporaryDataDelay" firestore:"temporaryDataDelay" postgres:"temporary_data_delay"`
	Volume                    *float64                    `json:"volume" firestore:"volume" postgres:"volume"`
	High1H                    *float64                    `json:"high1h" firestore:"high1h" postgres:"high_1h"`
	Low1H                     *float64                    `json:"low1h" firestore:"low1h" postgres:"low_1h"`
	High24h                   *float64                    `json:"high24h" firestore:"high24h" postgres:"high_24h"`
	Low24h                    *float64                    `json:"low24h" firestore:"low24h" postgres:"low_24h"`
	High7D                    *float64                    `bigquery:"high_7d" json:"high7d" postgres:"high_7d"`
	Low7D                     *float64                    `bigquery:"low_7d" json:"low7d" postgres:"low_7d"`
	High30D                   *float64                    `bigquery:"high_30d" json:"high30d" postgres:"high_30d"`
	Low30D                    *float64                    `bigquery:"low_30d" json:"low30d" postgres:"low_30d"`
	High1Y                    *float64                    `bigquery:"high_1y" json:"high1y" postgres:"high_1y"`
	Low1Y                     *float64                    `bigquery:"low_1y" json:"low1y" postgres:"low_1y"`
	Price24h                  *float64                    `json:"price24h" firestore:"price24h" postgres:"price_24h"`
	Percentage24h             *float64                    `json:"percentage24h" firestore:"percentage24h" postgres:"percentage_24h"`
	AllTimeHigh               *float64                    `json:"allTimeHigh" firestore:"allTimeHigh" postgres:"all_time_high"`
	AllTimeLow                *float64                    `json:"allTimeLow" firestore:"allTimeLow" postgres:"all_time_low"`
	Date                      time.Time                   `json:"date" firestore:"date" postgres:"date"`
	ChangeValue24h            *float64                    `json:"changeValue24h" firestore:"changeValue24h" postgres:"change_value_24h"`
	ListedExchanges           []string                    `json:"listedExchanges" firestore:"listedExchanges" postgres:"listed_exchange"`
	MarketCap                 *float64                    `json:"marketCap" firestore:"marketCap" postgres:"market_cap"`
	Supply                    *float64                    `json:"supply" firestore:"supply" postgres:"supply"`
	Exchanges                 []ExchangeBasedFundamentals `json:"exchanges" firestore:"exchanges" postgres:"exchanges"`
	OriginalSymbol            string                      `json:"originalSymbol" firestore:"originalSymbol" postgres:"original_symbol"`
	NumberOfActiveMarketPairs *int64                      `json:"numberOfActiveMarketPairs" firestore:"numberOfActiveMarketPairs" postgres:"number_of_active_market_pairs"`
	Nomics                    Volume                      `json:"nomics" firestore:"nomics" postgres:"nomics"`
	Forbes                    Volume                      `json:"forbes" firestore:"forbes"`
	HighYTD                   *float64                    `json:"highYtd" firestore:"highYtd" postgres:"high_ytd"`
	LowYTD                    *float64                    `json:"lowYtd" firestore:"lowYtd" postgres:"low_ytd"`
	Price1H                   *float64                    `json:"price_1h" firestore:"price1h" postgres:"price_1h"`
	Price7D                   *float64                    `json:"price_7d" firestore:"price7d" postgres:"price_7d"`
	Price30D                  *float64                    `json:"price_30d" firestore:"price30d" postgres:"price_30d"`
	Price1Y                   *float64                    `json:"price_1Y" firestore:"price1Y" postgres:"price_1Y"`
	PriceYTD                  *float64                    `json:"price_ytd" firestore:"priceYtd" postgres:"price_ytd"`
	Percentage1H              *float64                    `json:"percentage_1h" firestore:"percentage_1h" postgres:"percentage_1h"`
	Percentage7D              *float64                    `json:"percentage_7d" firestore:"percentage_7d" postgres:"percentage_7d"`
	Percentage30D             *float64                    `json:"percentage_30d" firestore:"percentage_30d" postgres:"percentage_30d"`
	Percentage1Y              *float64                    `json:"percentage_1y" firestore:"percentage_1y" postgres:"percentage_1y"`
	PercentageYTD             *float64                    `json:"percentage_ytd" firestore:"percentage_ytd" postgres:"percentage_ytd"`
	MarketCapPercentChange1H  *float64                    `json:"marketCapPercentChange1h" firestore:"marketCapPercentChange1h" postgres:"market_cap_percent_change_1h"`
	MarketCapPercentChange1D  *float64                    `json:"marketCapPercentChange1d" firestore:"marketCapPercentChange1d" postgres:"market_cap_percent_change_1d"`
	MarketCapPercentChange7D  *float64                    `json:"marketCapPercentChange7d" firestore:"marketCapPercentChange7d" postgres:"market_cap_percent_change_7d"`
	MarketCapPercentChange30D *float64                    `json:"marketCapPercentChange30d" firestore:"marketCapPercentChange30d" postgres:"market_cap_percent_change_30d"`
	MarketCapPercentChange1Y  *float64                    `json:"marketCapPercentChange1y" firestore:"marketCapPercentChange1y" postgres:"market_cap_percent_change_1y"`
	MarketCapPercentChangeYTD *float64                    `json:"marketCapPercentChangeYtd" firestore:"marketCapPercentChangeYtd" postgres:"market_cap_percent_change_ytd"`
	CirculatingSupply         *float64                    `json:"circulatingSupply" firestore:"circulatingSupply" postgres:"circulating_supply"`
	MarketPairs               []MarketPairs               `postgres:"market_pairs" json:"market_pairs,omitempty" firestore:"marketPairs"`
	LastUpdated               time.Time                   `postgres:"last_updated" json:"last_updated"`
	ForbesTransparencyVolume  float64                     `postgres:"forbes_transparency_volume" json:"ForbesTransparencyVolume"`
	IsDefaultCase             bool                        `postgres:"isDefaultCase" firestore:"isDefaultCase"`
	Status                    string                      `postgres:"status" firestore:"status"`
	DateAdded                 time.Time                   `postgres:"date_added" firestore:"date_added"`
}

type CategoryFundamental struct {
	ID                          string                 `json:"id" bigquery:"id" postgres:"id"`
	Name                        string                 `json:"name" bigquery:"name" postgres:"name"`
	TotalTokens                 bigquery.NullInt64     `json:"total_tokens" bigquery:"total_tokens" postgres:"total_tokens"`
	AveragePercentage24H        bigquery.NullFloat64   `json:"average_percentage_24h" bigquery:"average_percentage_24h" postgres:"average_percentage_24h"`
	Volume24H                   bigquery.NullFloat64   `json:"volume_24h" bigquery:"volume_24h" postgres:"volume_24h"`
	Price24H                    bigquery.NullFloat64   `bigquery:"price_24h" json:"total_price_24h"` // it present the total price for all assets in a category
	AveragePrice                bigquery.NullFloat64   `json:"average_price" bigquery:"average_price" postgres:"average_price"`
	MarketCap                   bigquery.NullFloat64   `json:"market_cap" bigquery:"market_cap_24h" postgres:"market_cap"`                                             // Market cap of the category
	MarketCapPercentage24H      bigquery.NullFloat64   `json:"market_cap_percentage_24h" bigquery:"market_cap_percentage_change" postgres:"market_cap_percentage_24h"` // Market cap of the category
	WeightIndexPrice            bigquery.NullFloat64   `bigquery:"price_weight_index" json:"price_weight_index"`                                                       // it present the index weight price for an asset and it calculated by multiple index weight market cap for asset by it's price
	WeightIndexMarketCap        bigquery.NullFloat64   `bigquery:"market_cap_weight_index" json:"market_cap_weight_index"`                                             // it present the index weight market cap for an asset and it calculated by divided market cap for asset to total market cap in category
	MarketCapIndexValue24H      bigquery.NullFloat64   `bigquery:"index_market_cap_24h" json:"market_cap_24h_index"`                                                   // it present the index market cap value for a category and it is the change value in market cap
	MarketCapIndexPercentage24H bigquery.NullFloat64   `bigquery:"index_market_cap_percentage_24h" json:"market_cap_index_percentage_24h,omitempty"`                   // it present the percentage change for market cap index value in 24h
	Divisor                     bigquery.NullFloat64   `bigquery:"divisor" json:"divisor"`                                                                             // it present the constant value we will use to calculate the Market Cap Index Value and it calculated by divided total market cap on 1000 (this is a base value)
	TopGainers                  []CategoryTopGainer    `json:"top_gainers,omitempty" bigquery:"top_gainers" postgres:"top_gainers"`                                    //Top Gainers are the top assets by positive marketcap percentage
	TopMovers                   []CategoryTopGainer    `json:"top_movers,omitempty" bigquery:"top_movers" postgres:"top_movers"`                                       //Top Movers are the top assets by the absolute value of the  marketcap percentage
	Date                        bigquery.NullTimestamp `bigquery:"Date" json:"day_start"`                                                                              // it present the date for a category
	LastUpdated                 bigquery.NullTimestamp `json:"last_updated" bigquery:"row_last_updated" postgres:"last_updated"`
	ForbesID                    string                 `json:"forbesID" bigquery:"forbesID" postgres:"forbesID"`       //a unique Id assigned by seo in rowy (used as slug)
	ForbesName                  string                 `json:"forbesName" bigquery:"forbesName" postgres:"forbesName"` //Data that populates the categories description H1 tag//Data that populates the categories description H1 tag
	Slug                        string                 `json:"slug" bigquery:"slug" postgres:"slug"`                   //a slug generated by the build categories fundamentals process. It thate is a forbes is  the slug will be /forbesid otherwise it will be /id
	Inactive                    bool                   `json:"inactive" bigquery:"inactive" postgres:"inactive"`       // A category is marked as inactive when Coingecko stops sending data about it. Instead of hard-deleting it, we're soft deleting the category via this flag.
	Markets                     []CoinsMarketResult    `json:"markets" postgres:"markets"`                             // List of all the assets in the category
	IsHighlighted               bool                   `json:"is_highlighted" postgres:"is_highlighted"`               // Flag indicating that this category belongs on the highlights page
	Content                     string                 `json:"content" postgres:"content"`
}

type CategoryTopGainer struct {
	Slug                string               `json:"slug" bigquery:"slug" postgres:"slug"`
	Logo                string               `json:"logo" bigquery:"logo" postgres:"logo"`
	Symbol              string               `json:"symbol" bigquery:"symbol" postgres:"symbol"`
	Name                string               `json:"name" bigquery:"name" postgres:"name"`
	MarketCap           bigquery.NullFloat64 `json:"market_cap" bigquery:"market_cap" postgres:"market_cap"`                                  // Market cap of the category
	MarketCapPercentage bigquery.NullFloat64 `json:"market_cap_percentage" bigquery:"market_cap_percentage" postgres:"market_cap_percentage"` // Market cap of the category
	Volume              bigquery.NullFloat64 `json:"volume_24h" bigquery:"volume_24h" postgres:"volume_24h"`                                  // volume of the category
	VolumePercentage    bigquery.NullFloat64 `json:"volume_percentage" bigquery:"volume_percentage" postgres:"volume_percentage"`             // Volume Percentage of the category
}

type ExchangeBasedFundamentals struct {
	Exchange                     string              `json:"market" bigquery:"Exchange"`
	Slug                         string              `json:"slug" firestore:"slug"`
	Logo                         string              `json:"logo" firestore:"logo"`
	Symbol                       string              `json:"Symbol" bigquery:"Symbol"`
	Close                        float64             `json:"Close" bigquery:"Close"`
	NumberOfActivePairsForAssets int64               `json:"number_of_active_pairs_for_assets" bigquery:"number_of_active_pairs_for_assets"`
	VolumeByExchange1D           float64             `json:"volume_by_exchange_1d" bigquery:"volume_by_exchange_1d"`
	PriceByExchange1D            float64             `json:"price_by_exchange_1d" bigquery:"price_by_exchange_1d"`
	Nomics                       ExchangeBasedVolume `json:"nomics" bigquery:"nomics"`
	Forbes                       ExchangeBasedVolume `json:"forbes" bigquery:"forbes"`
}

type ExchangeBasedVolume struct {
	VolumeByExchange1D  float64 `firestore:"volumeByExchange1D" json:"volume_by_exchange_1d"`
	VolumeByExchange7D  float64 `firestore:"volumeByExchange7D" json:"volume_by_exchange_7d"`
	VolumeByExchange30D float64 `firestore:"volumeByExchange30D" json:"volume_by_exchange_30d"`
	VolumeByExchange1Y  float64 `firestore:"volumeByExchange1Y" json:"volume_by_exchange_1y"`
	VolumeByExchangeYTD float64 `firestore:"volumeByExchangeYTD" json:"volume_by_exchange_ytd"`
}

// Maps most recent candle data to fundamental
func MapChartDataToFundamental(ctxO context.Context, tsResults []TimeSeriesResultPG, fundamental Fundamentals, openClosePrices []OpenCloseAsset) (Fundamentals, TimeSeriesResultPG) {
	_, span := tracer.Start(ctxO, "MapChartDataToFundamental")
	defer span.End()

	span.AddEvent("Start Map Chart Data To Fundamental")
	var chartData TimeSeriesResultPG
	for _, cd := range tsResults {
		if cd.Symbol == fundamental.Symbol {
			fundamental.Price24h = &cd.Slice[len(cd.Slice)-1].AvgClose //cd.Slice[len(cd.Slice)-1].AvgClose
			fundamental.Date = cd.Slice[len(cd.Slice)-1].Time
			fundamental.LastUpdated = cd.Slice[len(cd.Slice)-1].Time
			chartData = cd
		}
	}

	for _, openClose := range openClosePrices {
		if openClose.Symbol == fundamental.Symbol {

			// open or close prices are nil, that means the asset has been inactive since 24h.
			if (&openClose).Price24H.Close != nil && (&openClose).Price24H.Open != nil {
				change24h := *(&openClose).Price24H.Close - *(&openClose).Price24H.Open
				fundamental.ChangeValue24h = &change24h
				fundamental.Percentage24h = CalculatePercentageChange((&openClose).Price24H.Open, (&openClose).Price24H.Close)
				fundamental.Price7D = openClose.Price7D.Open
				fundamental.Percentage7D = CalculatePercentageChange((&openClose).Price7D.Open, (&openClose).Price7D.Close)
				fundamental.Price30D = openClose.Price30D.Open
				fundamental.Percentage30D = CalculatePercentageChange((&openClose).Price30D.Open, (&openClose).Price30D.Close)
				fundamental.Price1Y = openClose.Price1Y.Open
				fundamental.Percentage1Y = CalculatePercentageChange((&openClose).Price1Y.Open, (&openClose).Price1Y.Close)
			} else {
				// We set a token's status while fetching the ticker data. The following condition double checks and ensures that the status is set to comatoken when there is no data available for the last 24 hours.
				fundamental.Status = "comatoken"
			}
		}
	}

	span.SetStatus(otelCodes.Ok, "Success")
	return fundamental, chartData

}

// Maps most recent candle data to fundamental
func MapCategoryFundamental(ctxO context.Context, category CategoriesData, assets *[]Fundamentals, categoryFundamental24hList CategoryFundamental, chartCategoriesData24hrResults []TimeSeriesResultPG) (CategoryFundamental, TimeSeriesResultPG) {
	ctx, span := tracer.Start(ctxO, "mapCategoryFundamental")
	defer span.End()

	span.AddEvent("Start Map Category Data To Category Fundamentals")
	totalTokens := 0
	var totalPercentageChange float64 = 0.0
	var totalPrice float64 = 0.0
	var marketCap float64 = 0.0
	var volume24h float64 = 0.0
	var topGainers []CategoryTopGainer
	var markets []CoinsMarketResult

	for _, market := range category.Markets {
		for _, asset := range *assets {
			if asset.Symbol != "" && asset.Status == "active" && asset.Symbol == market.ID {
				markets = append(markets, market)
				totalTokens++
				if asset.MarketCap != nil {
					marketCap += *asset.MarketCap
				}
				if asset.Volume != nil {
					volume24h += *asset.Volume
				}
				if asset.Percentage24h != nil {
					totalPercentageChange += *asset.Percentage24h
				}
				if asset.Price24h != nil {
					totalPrice += *asset.Price24h
				}

				if &asset.Nomics.Volume1D != nil && *&asset.Nomics.Volume1D > 0 {
					var volpct = 0.0
					var mktpct = 0.0
					if asset.Nomics.PercentageVolume1D != nil {
						volpct = *asset.Nomics.PercentageVolume1D
					}
					if asset.MarketCapPercentChange1D != nil {
						mktpct = *asset.MarketCapPercentChange1D
					}
					topGainers = append(topGainers, CategoryTopGainer{
						Slug:                asset.Slug,
						Logo:                asset.Logo,
						Symbol:              asset.Symbol,
						Name:                asset.Name,
						MarketCap:           bigquery.NullFloat64{Float64: *&asset.Nomics.Volume1D, Valid: true},
						MarketCapPercentage: bigquery.NullFloat64{Float64: mktpct, Valid: true},
						Volume:              bigquery.NullFloat64{Float64: *&asset.Nomics.Volume1D, Valid: true},
						VolumePercentage:    bigquery.NullFloat64{Float64: volpct, Valid: true},
					})
				}
				break
			}
		}
	}
	// We need to append the latest marketCap to the Slice array so this will ensure the match between the data in the chart and data in the table
	var (
		categoryChart TimeSeriesResultPG
		Slice         SlicePG
	)
	for _, chartData := range chartCategoriesData24hrResults {
		if chartData.Symbol == category.ID {
			Slice.Time = time.Now()
			Slice.MarketCapUSD = marketCap
			chartData.Slice = append(chartData.Slice, Slice)
			categoryChart = chartData
			break
		}
	}
	if categoryChart.Symbol == "" {
		categoryChart.Symbol = category.ID
		Slice.Time = time.Now()
		Slice.MarketCapUSD = marketCap
		categoryChart.Slice = append(categoryChart.Slice, Slice)
		categoryChart.AssetType = "CATEGORY"

	}

	categoryFundamental := CalculateCategoriesFundamentalsIndexPrice(ctx, marketCap, totalTokens, category, assets, categoryFundamental24hList)
	// var categoryFundamental CategoryFundamental
	categoryFundamental.ID = category.ID
	categoryFundamental.Name = category.Name
	categoryFundamental.Inactive = category.Inactive
	categoryFundamental.Content = category.Content
	var marketCap24h float64 = categoryFundamental24hList.MarketCap.Float64
	var marketCapPercentage24H float64 = 0
	if marketCap24h != 0 && marketCap != 0 {
		marketCapPercentage24H = (marketCap - marketCap24h) / marketCap24h * 100
	}
	categoryFundamental.MarketCapPercentage24H = bigquery.NullFloat64{Float64: marketCapPercentage24H, Valid: true}
	categoryFundamental.TotalTokens = bigquery.NullInt64{Int64: int64(totalTokens), Valid: true}
	categoryFundamental.MarketCap = bigquery.NullFloat64{Float64: marketCap, Valid: true}
	categoryFundamental.Volume24H = bigquery.NullFloat64{Float64: volume24h, Valid: true}
	var averagePrice float64 = 0
	var averagePercentageChange float64 = 0
	if totalTokens > 0 {
		averagePercentageChange = totalPercentageChange / float64(totalTokens)
		averagePrice = totalPrice / float64(totalTokens)
	}
	categoryFundamental.AveragePercentage24H = bigquery.NullFloat64{Float64: averagePercentageChange, Valid: true}
	categoryFundamental.Price24H = bigquery.NullFloat64{Float64: totalPrice, Valid: true}
	categoryFundamental.AveragePrice = bigquery.NullFloat64{Float64: averagePrice, Valid: true}

	// Sort top gainers by volume percentage and then select only the top 3 assets.
	sort.Slice(topGainers, func(i, j int) bool {
		return topGainers[i].VolumePercentage.Float64 > topGainers[j].VolumePercentage.Float64
	})
	// create a new array for top movers
	topMovers := make([]CategoryTopGainer, len(topGainers))
	copy(topMovers, topGainers) // performs shallow copy of the top gainers to top movers
	//sort the top movers array the the absolute value of an assets marketcap precentage
	sort.Slice(topMovers, func(i, j int) bool {
		return math.Abs(topMovers[i].MarketCapPercentage.Float64) > math.Abs(topMovers[j].MarketCapPercentage.Float64)
	})

	//get the length of top gainers (this will be the same length as top movers since they are the same size)
	topGainersLen := len(topGainers)
	if topGainersLen > 3 { // if the length is greater than three
		topGainersLen = 3 // set it to 3. We dont want more than 3 assets featured in top movers and top gainers
	}
	categoryFundamental.TopGainers = topGainers[0:topGainersLen] // assign the categoryFundamentals top gainers to our sorted topGainersList
	categoryFundamental.TopMovers = topMovers[0:topGainersLen]   // assign the categoryFundamentals top movers to our sorted list

	//UpdateFeaturedCategoriesNew(ctx, rowyCategories)

	categoryFundamental.Date = bigquery.NullTimestamp{Timestamp: time.Now(), Valid: true}
	categoryFundamental.LastUpdated = bigquery.NullTimestamp{Timestamp: time.Now(), Valid: true}
	categoryFundamental.Markets = markets

	categoryFundamental.Slug = categoryFundamental.ID // assign a slug

	span.SetStatus(otelCodes.Ok, "Success")
	return categoryFundamental, categoryChart
}

// CalculateCategoriesFundamentalsIndexPrice takes all data necessary to ca
func CalculateCategoriesFundamentalsIndexPrice(ctxO context.Context, totalMarketCap float64, totalTokens int, category CategoriesData, assets *[]Fundamentals, categoryHistoricalData CategoryFundamental) CategoryFundamental {
	var totalPriceWeightIndex float64 = 0.0
	var categoryFundamental CategoryFundamental
	// we need to calculate MarketCapWeight
	for _, market := range category.Markets {
		for _, asset := range *assets {
			if asset.Symbol != "" && asset.Status == "active" && asset.Symbol == market.ID {
				if totalMarketCap != 0 {
					MarketCapWeight := *asset.MarketCap / totalMarketCap
					totalPriceWeightIndex += MarketCapWeight * *asset.Price24h
				}
			}
		}
	}
	// we need to calculate totalMarketCapWeightIndex
	totalMarketCapWeightIndex := totalMarketCap * totalPriceWeightIndex
	//we need to calculate totalMarketCap
	var marketCapIndexValue24H float64 = 0.0
	if totalMarketCap == 0 {
		marketCapIndexValue24H = 0
	} else {
		marketCapIndexValue24H = totalMarketCap / categoryHistoricalData.Divisor.Float64
	}
	categoryFundamental.WeightIndexMarketCap = bigquery.NullFloat64{Float64: totalMarketCapWeightIndex, Valid: true}
	categoryFundamental.MarketCapIndexValue24H = bigquery.NullFloat64{Float64: marketCapIndexValue24H, Valid: true}
	categoryFundamental.WeightIndexPrice = bigquery.NullFloat64{Float64: totalPriceWeightIndex, Valid: true}
	categoryFundamental.Divisor = bigquery.NullFloat64{Float64: categoryHistoricalData.Divisor.Float64, Valid: true}
	var marketCapIndexPercentage24H float64 = 0.0
	if marketCapIndexValue24H == 0 || categoryHistoricalData.MarketCapIndexValue24H.Float64 == 0 {
		marketCapIndexPercentage24H = 0
	} else {
		marketCapIndexPercentage24H = ((marketCapIndexValue24H - categoryHistoricalData.MarketCapIndexValue24H.Float64) / categoryHistoricalData.MarketCapIndexValue24H.Float64) * 100
	}
	categoryFundamental.MarketCapIndexPercentage24H = bigquery.NullFloat64{Float64: marketCapIndexPercentage24H, Valid: true}
	return categoryFundamental
}

// GetExchangesList returns a list of exchanges that the asset is listed on based on the exchange data. Returns []string
func GetExchangesList(exchangeData []ExchangeBasedFundamentals) []string {

	exchanges := []string{}

	for _, exchange := range exchangeData {
		exchanges = append(exchanges, exchange.Exchange)
	}

	return exchanges

}

type Chart struct {
	Symbol     string    `postgres:"symbol" json:"symbol"`
	Forbes     string    `postgres:"forbes" json:"forbes"`
	Time       time.Time `postgres:"time" json:"time"`
	Price      float64   `postgres:"price" json:"price"`
	DataSource string    `postgres:"data_source" json:"dataSource"`
}

func ChartDataFromFundamentals(fundamental Fundamentals) Chart {
	var chartData Chart

	chartData.Symbol = fundamental.Symbol
	chartData.Forbes = fundamental.DisplaySymbol
	chartData.Time = fundamental.Date
	chartData.DataSource = "nomics"

	return chartData
}

// insert Fundamentals data into PG Table
// Will Lowercase the symbol
func InsertFundamental(ctx0 context.Context, fundamental Fundamentals, labels map[string]string) error {

	/*ctxn, span := tracer.Start(ctx0, "InsertFundamental")
	defer span.End()

	ctx, cancel := context.WithTimeout(ctxn, 30*time.Minute)
	defer cancel()*/
	startTime := log.StartTimeL(labels, "Fundamental Insert")

	pg := PGConnect()

	// TODO: [FDA-1077] Change to Stored Procedure that preformes an upsert
	insertStatementsFundamentals := "INSERT INTO fundamentals(symbol, name, slug, logo, float_type, display_symbol, original_symbol, source, temporary_data_delay, number_of_active_market_pairs, high_24h, low_24h, high_7d, low_7d, high_30d, low_30d, high_1y, low_1y, high_ytd, low_ytd, price_24h, price_7d, price_30d, price_1y, price_ytd, percentage_24h, percentage_7d, percentage_30d, percentage_1y, percentage_ytd,  market_cap, market_cap_percent_change_1d, market_cap_percent_change_7d, market_cap_percent_change_30d, market_cap_percent_change_1y, market_cap_percent_change_ytd, circulating_supply, supply, all_time_low, all_time_high, date, change_value_24h, listed_exchange, market_pairs, exchanges, nomics, forbes,last_updated) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47,$48)"
	// convert Exchanges[] and Nomics into json type to make it easy to store in PG table
	exchanges, _ := json.Marshal(fundamental.Exchanges)
	nomics, _ := json.Marshal(fundamental.Nomics)
	market_pairs, _ := json.Marshal(fundamental.MarketPairs)
	forbes, _ := json.Marshal(fundamental.Forbes)

	latencyTimeStart := time.Now()
	_, insertError := pg.ExecContext(ctx0, insertStatementsFundamentals, fundamental.Symbol, fundamental.Name, fundamental.Slug, fundamental.Logo, fundamental.FloatType, fundamental.DisplaySymbol, fundamental.OriginalSymbol, fundamental.Source, fundamental.TemporaryDataDelay, fundamental.NumberOfActiveMarketPairs, fundamental.High24h, fundamental.Low24h, fundamental.High7D, fundamental.Low7D, fundamental.High30D, fundamental.Low30D, fundamental.High1Y, fundamental.Low1Y, fundamental.HighYTD, fundamental.LowYTD, fundamental.Price24h, fundamental.Price7D, fundamental.Price30D, fundamental.Price1Y, fundamental.PriceYTD, fundamental.Percentage24h, fundamental.Percentage7D, fundamental.Percentage30D, fundamental.Percentage1Y, fundamental.PercentageYTD, fundamental.MarketCap, fundamental.MarketCapPercentChange1D, fundamental.MarketCapPercentChange7D, fundamental.MarketCapPercentChange30D, fundamental.MarketCapPercentChange1Y, fundamental.MarketCapPercentChangeYTD, fundamental.CirculatingSupply, fundamental.Supply, fundamental.AllTimeLow, fundamental.AllTimeHigh, fundamental.Date, fundamental.ChangeValue24h, pq.Array(fundamental.ListedExchanges), market_pairs, exchanges, nomics, forbes, fundamental.LastUpdated)
	latency := time.Since(latencyTimeStart)

	log.InfoL(labels, fmt.Sprintf("Fundamentals: time to insert %dms", latency.Milliseconds()))
	if latency.Seconds() > 1.5 {
		log.WarningL(labels, fmt.Sprintf("Fundamentals: time to insert over 1.5 second %fs", latency.Seconds()))
	}

	if insertError != nil {
		log.EndTimeL(labels, "Fundamental Insert", startTime, insertError)
		if ctx0.Err() == context.DeadlineExceeded {
			log.ErrorL(labels, "Fundamentals: Context Timeout Occured %s", ctx0.Err())
			log.WarningL(labels, fmt.Sprintf("Fundamentals: DEADLINE second %fs", latency.Seconds()))
			log.EndTimeL(labels, "Fundamentals: Context Timeout Occured End Time", startTime, insertError)
		}
		return insertError
	}

	log.EndTimeL(labels, "Fundamental Insert", startTime, nil)

	return nil
}

func InsertFundamentalLatest(ctx0 context.Context, fundamental Fundamentals, labels map[string]string) error {

	//ctxn, span := tracer.Start(ctx0, "InsertFundamentalLatest")
	//defer span.End()

	//ctx, cancel := context.WithTimeout(ctxn, 30*time.Minute)
	//defer cancel()

	startTime := log.StartTimeL(labels, "FundamentalLatest Insert")

	pg := PGConnect()

	// TODO: [FDA-1077] Change to Stored Procedure that preformes an upsert
	insertStatementsFundamentals := "CALL upsertFundamentalsLatest ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51)"

	query := insertStatementsFundamentals
	// convert Exchanges[] and Nomics into json type to make it easy to store in PG table
	exchanges, _ := json.Marshal(fundamental.Exchanges)
	nomics, _ := json.Marshal(fundamental.Nomics)
	market_pairs, _ := json.Marshal(fundamental.MarketPairs)
	forbes, _ := json.Marshal(fundamental.Forbes)

	latencyTimeStart := time.Now()
	_, insertError := pg.ExecContext(ctx0, query, fundamental.Symbol, fundamental.Name, fundamental.Slug, fundamental.Logo, fundamental.FloatType, fundamental.DisplaySymbol, fundamental.OriginalSymbol, fundamental.Source, fundamental.TemporaryDataDelay, fundamental.NumberOfActiveMarketPairs, fundamental.High24h, fundamental.Low24h, fundamental.High7D, fundamental.Low7D, fundamental.High30D, fundamental.Low30D, fundamental.High1Y, fundamental.Low1Y, fundamental.HighYTD, fundamental.LowYTD, fundamental.Price24h, fundamental.Price7D, fundamental.Price30D, fundamental.Price1Y, fundamental.PriceYTD, fundamental.Percentage24h, fundamental.Percentage7D, fundamental.Percentage30D, fundamental.Percentage1Y, fundamental.PercentageYTD, fundamental.MarketCap, fundamental.MarketCapPercentChange1D, fundamental.MarketCapPercentChange7D, fundamental.MarketCapPercentChange30D, fundamental.MarketCapPercentChange1Y, fundamental.MarketCapPercentChangeYTD, fundamental.CirculatingSupply, fundamental.Supply, fundamental.AllTimeLow, fundamental.AllTimeHigh, fundamental.Date, fundamental.ChangeValue24h, pq.Array(fundamental.ListedExchanges), market_pairs, exchanges, nomics, forbes, fundamental.LastUpdated, fundamental.ForbesTransparencyVolume, fundamental.Status, fundamental.Percentage1H)
	latency := time.Since(latencyTimeStart)
	log.InfoL(labels, fmt.Sprintf("FundamentalsLatest: time to insert %dms", latency.Milliseconds()))
	if latency.Seconds() > 1.5 {
		log.WarningL(labels, fmt.Sprintf("FundamentalsLatest: time to insert over 1.5 seconds %fs", latency.Seconds()))
	}

	if insertError != nil {
		if ctx0.Err() == context.DeadlineExceeded {
			log.ErrorL(labels, "FundamentalsLatest: Context Timeout Occured %s", ctx0.Err())
			log.WarningL(labels, fmt.Sprintf("FundamentalsLatest: DEADLINE  %fs", latency.Seconds()))
			log.EndTimeL(labels, "FundamentalsLatest: Context Timeout Occured End Time", startTime, insertError)
		}
		log.EndTimeL(labels, "FundamentalsLatest Insert", startTime, insertError)
		return insertError
	}

	log.EndTimeL(labels, "FundamentalsLatest Insert", startTime, nil)

	return nil
}

func CalculateForbesBasedVolume(ctxO context.Context, exchangeData []ExchangeBasedFundamentals) Volume {
	_, span := tracer.Start(ctxO, "CalculateForbesBasedVolume")
	defer span.End()
	var forbes Volume

	var forbesVolume float64
	span.AddEvent("Start Calculate Forbes Based Volume")
	for i := range exchangeData {

		forbesVolume += exchangeData[i].Forbes.VolumeByExchange1D
	}
	forbes.Volume1D = forbesVolume
	span.SetStatus(otelCodes.Ok, "Success")
	return forbes
}

// CombineExchangeMetaData merges the exchange Metadata data with the exchange ticker data. returns []ExchangeBasedFundamentals
func CombineExchangeDataCG(ctxO context.Context, exchangeData []ExchangeBasedFundamentals, profiles map[string]model.CoingeckoExchangeMetadata, exchangeProfiles map[string]model.ExchangeProfile) ([]ExchangeBasedFundamentals, float64) {
	_, span := tracer.Start(ctxO, "CombineExchangeDataCG")
	defer span.End()

	span.AddEvent("Start Combine Exchanges Fundamentals")
	var exchangesResult []ExchangeBasedFundamentals
	var forbesTransparencyVolume float64
	for _, exchange := range exchangeData {
		if profile, ok := profiles[exchange.Exchange]; ok {
			span.AddEvent("Start Combine Exchanges Fundamentals with Exchanges MetaData")
			exchange.Slug = cleanString(strings.ReplaceAll(profile.Name, " ", "-"))
			exchange.Logo = profile.LogoURL
			exchange.Nomics.VolumeByExchange1D = exchange.VolumeByExchange1D
			if exchangeProfile, ok := exchangeProfiles[exchange.Exchange]; ok {
				span.AddEvent("Start Combine Exchanges Fundamentals with Exchanges Profiles")
				volumeDiscount := exchangeProfile.VolumeDiscountPercent
				volumeDiscountPercent := 1 - (volumeDiscount / 100)
				exchange.Forbes.VolumeByExchange1D = exchange.VolumeByExchange1D * volumeDiscountPercent
				if volumeDiscountPercent != 0 {
					forbesTransparencyVolume += exchange.Forbes.VolumeByExchange1D
				}
			}
			exchangesResult = append(exchangesResult, exchange)
		}
	}
	if len(exchangesResult) > 0 {
		span.SetStatus(otelCodes.Ok, "Success")
		return exchangesResult, forbesTransparencyVolume
	}
	span.SetStatus(otelCodes.Ok, "Success")
	return exchangeData, forbesTransparencyVolume

}

// A Support function which extracts the HighLowValues from the PGFundamentalsResult and adds them to the Fundamentals struct while building the asset fundamentals.
func extractHighLowValues(ctxO context.Context, fundamental *Fundamentals, highLows PGFundamentalsResult) Fundamentals {
	_, span := tracer.Start(ctxO, "extractHighLowValues")
	defer span.End()
	span.AddEvent("Start extracting HighLowValues")

	// All Time Highs/Lows
	fundamental.AllTimeHigh = &highLows.AllTimeHigh.Float64
	fundamental.AllTimeLow = &highLows.AllTimeLow.Float64

	// 24H Data
	fundamental.High24h = &highLows.High.Float64
	fundamental.Low24h = &highLows.Low.Float64
	if highLows.High.Valid == true && highLows.Low.Valid == true {
		var changeVal = highLows.High.Float64 - highLows.Low.Float64
		fundamental.ChangeValue24h = &changeVal
		fundamental.Percentage24h = CalculatePercentageChange(&highLows.Low.Float64, &highLows.High.Float64)
	}

	// 1D Data
	fundamental.Nomics.PercentageVolume1D = CalculatePercentageChange(&highLows.VolumeOpen24H.Float64, &highLows.VolumeClose24H.Float64)
	fundamental.MarketCapPercentChange1D = CalculatePercentageChange(&highLows.MarketCapOpen24H.Float64, &highLows.MarketCapClose24H.Float64)

	// 7D Data
	fundamental.High7D = &highLows.High7D.Float64
	fundamental.Low7D = &highLows.Low7D.Float64
	fundamental.MarketCapPercentChange7D = CalculatePercentageChange(&highLows.MarketCapOpen7D.Float64, &highLows.MarketCapClose7D.Float64)
	fundamental.Nomics.PercentageVolume7D = CalculatePercentageChange(&highLows.VolumeOpen7D.Float64, &highLows.VolumeClose7D.Float64)

	// 30D High/Low
	fundamental.High30D = &highLows.High30D.Float64
	fundamental.Low30D = &highLows.Low30D.Float64
	fundamental.MarketCapPercentChange30D = CalculatePercentageChange(&highLows.MarketCapOpen30D.Float64, &highLows.MarketCapClose30D.Float64)
	fundamental.Nomics.PercentageVolume30D = CalculatePercentageChange(&highLows.VolumeOpen30D.Float64, &highLows.VolumeClose30D.Float64)

	// 1Y High/Low
	fundamental.High1Y = &highLows.High1Y.Float64
	fundamental.Low1Y = &highLows.Low1Y.Float64
	fundamental.MarketCapPercentChange1Y = CalculatePercentageChange(&highLows.MarketCapOpen1Y.Float64, &highLows.MarketCapClose1Y.Float64)
	fundamental.Nomics.PercentageVolume1Y = CalculatePercentageChange(&highLows.VolumeOpen1Y.Float64, &highLows.VolumeClose1Y.Float64)

	// YTD High/Low
	fundamental.HighYTD = &highLows.HighYtd.Float64
	fundamental.LowYTD = &highLows.LowYtd.Float64
	fundamental.PriceYTD = &highLows.PriceOpenYTD.Float64
	fundamental.MarketCapPercentChangeYTD = CalculatePercentageChange(&highLows.MarketCapOpenYTD.Float64, &highLows.MarketCapCloseYTD.Float64)
	fundamental.Nomics.PercentageVolumeYTD = CalculatePercentageChange(&highLows.VolumeOpenYTD.Float64, &highLows.VolumeCloseYTD.Float64)
	fundamental.PercentageYTD = CalculatePercentageChange(&highLows.PriceOpenYTD.Float64, &highLows.PriceCloseYTD.Float64)

	// 1h High/Low
	fundamental.High1H = &highLows.High1H.Float64
	fundamental.Low1H = &highLows.Low1H.Float64
	fundamental.Price1H = &highLows.PriceOpen1H.Float64
	fundamental.MarketCapPercentChange1H = CalculatePercentageChange(&highLows.MarketCapOpen1H.Float64, &highLows.MarketCapClose1H.Float64)
	fundamental.Nomics.PercentageVolume1H = CalculatePercentageChange(&highLows.VolumeOpen1H.Float64, &highLows.VolumeClose1H.Float64)
	fundamental.Percentage1H = CalculatePercentageChange(&highLows.PriceOpen1H.Float64, &highLows.PriceClose1H.Float64)

	span.SetStatus(otelCodes.Ok, "Success")
	return *fundamental
}

// BuildSlugMaps generates 2 important maps, which are used for generating unique slugs:
// 1. allSlugToSymbolMap : This is map[string]string, which maps a slug => symbol.
// 2. oldFundamentalsMap : This maps a symbol => oldFundamental.
func BuildSlugMaps(labels map[string]string, ctxO context.Context, oldFundamentals *[]Fundamentals) (rfCommon.ConcurrentMap[string, string], rfCommon.ConcurrentMap[string, Fundamentals]) {
	_, span := tracer.Start(ctxO, "BuildSlugMaps")
	defer span.End()

	labels["function"] = "BuildSlugMaps"
	labels["UUID"] = uuid.New().String()
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	startTime := log.StartTimeL(labels, "build Slug Maps")

	allSlugToSymbolMap := rfCommon.NewConcurrentMap[string, string]()
	oldFundamentalsMap := rfCommon.NewConcurrentMap[string, Fundamentals]() //make(map[string]Fundamentals)

	for _, fundamental := range *oldFundamentals {
		oldFundamentalsMap.AddValue(fundamental.Symbol, fundamental)

		if value := allSlugToSymbolMap.GetValue(fundamental.Slug); value == nil || *value == "" {
			allSlugToSymbolMap.AddValue(fundamental.Slug, fundamental.Symbol)
		}
	}

	log.EndTimeL(labels, "build Slug Maps", startTime, nil)
	span.SetStatus(otelCodes.Ok, "Success")
	return allSlugToSymbolMap, oldFundamentalsMap
}

// buildFundamentalSlug builds a unique slug for a symbol by looking at the map of previously assigned slugs to each symbol.
// Normally it builds a slug by doing "assetName-assetDisplaySymbol", note that the entire slug is lowercase.
// If there is a conflict, then it appends a numeral suffix eg "assetName-assetDisplaySymbol-2".
// If there is still a conflict, then it increases the numeral till it finds a unique slug
func buildFundamentalSlug(ctxO context.Context, allSlugToSymbolMap rfCommon.ConcurrentMap[string, string], oldFundamental Fundamentals, ticker CoinsMarketResult) string {
	_, span := tracer.Start(ctxO, "BuildSlugMaps")
	defer span.End()

	oldSlug := oldFundamental.Slug
	newSlug := strings.ToLower(strings.Replace(fmt.Sprintf("%s-%s", ticker.Name, ticker.Symbol), " ", "-", -1))

	var oldSlugSymbol *string
	// If the asset already has a valid previous slug, and its = to the new slug.retain the value
	//unique only to this asset's symbol.
	if oldSlug != "" && newSlug == oldSlug {
		oldSlugSymbol = allSlugToSymbolMap.GetValue(oldSlug)
		if oldSlugSymbol != nil && *oldSlugSymbol == ticker.ID {
			span.SetStatus(otelCodes.Ok, "Success")
			oldSlug = cleanString(oldSlug)
			return oldSlug
		}
	}
	// Only for newly added asset.
	if newSlug != oldSlug {
		allSlugToSymbolMap.AddValue(newSlug, ticker.ID)
		span.SetStatus(otelCodes.Ok, "Success")
		newSlug = cleanString(newSlug)
		return newSlug
	}

	// Only duplicate slug assets are remaining now.
	var iterator = 2
	var nextSlug = newSlug + "-" + strconv.Itoa(iterator)
	for {
		if allSlugToSymbolMap.GetValue(nextSlug) == nil || *allSlugToSymbolMap.GetValue(nextSlug) == "" {
			allSlugToSymbolMap.AddValue(nextSlug, ticker.ID)
			break
		}
		iterator++
		nextSlug = newSlug + "-" + strconv.Itoa(iterator)
	}
	span.SetStatus(otelCodes.Ok, "Success")
	nextSlug = cleanString(nextSlug)
	return nextSlug
}

// cleanString
// Takes string
// Returns string
// It will take a string and clean it from any character that will cause an issue with FE.
func cleanString(s string) string {
	// Step 1: Convert to lowercase
	s = strings.ToLower(s)

	// Step 2: Remove leading and trailing slashes
	s = strings.Trim(s, "/")

	// Step 3: Replace spaces and underscores with hyphens
	s = regexp.MustCompile(`[\s_]+`).ReplaceAllString(s, "-")

	// Step 4: Remove parentheses but keep their content
	s = regexp.MustCompile(`\(([^)]+)\)`).ReplaceAllString(s, "$1")

	// Step 5: Replace apostrophes, forward slashes, periods, and other non-alphanumeric characters (except hyphens) with hyphens
	s = regexp.MustCompile(`['./']+`).ReplaceAllString(s, "-")
	s = regexp.MustCompile(`[^a-z0-9-]+`).ReplaceAllString(s, "-")

	// Step 6: Replace multiple consecutive hyphens with a single hyphen
	s = regexp.MustCompile(`-+`).ReplaceAllString(s, "-")

	// Step 7: Trim leading and trailing hyphens
	s = strings.Trim(s, "-")

	return s
}

// CombinedFundamentals combines the fundamentals of a crypto asset with the ticker data from coinGecko and the marketCapChangePct with Volume Percent Change.
// Returns Fundamentals Struct
func CombineFundamentalsCG(ctxO context.Context, ticker CoinsMarketResult, highLows PGFundamentalsResult, metadata AssetMetaData, exchangeData []ExchangeBasedFundamentals, allSlugToSymbolMap rfCommon.ConcurrentMap[string, string], oldFundamental Fundamentals) (Fundamentals, error) {
	ctx, span := tracer.Start(ctxO, "CombineFundamentalsCG")
	defer span.End()

	span.AddEvent("Start Combine Fundamentals")
	var fundamental Fundamentals
	// Basic Data from PG Assets Metadata
	fundamental.Slug = buildFundamentalSlug(ctx, allSlugToSymbolMap, oldFundamental, ticker)
	fundamental.Logo = metadata.LogoURL
	fundamental.OriginalSymbol = metadata.OriginalSymbol

	// Basic Data from tickers
	fundamental.Symbol = ticker.ID
	fundamental.DisplaySymbol = ticker.Symbol
	fundamental.Name = ticker.Name
	// fundamental.Date = ticker.PriceTimestamp
	fundamental.MarketCap = ConvertBQFloatToFloat(ticker.MarketCap)
	fundamental.NumberOfActiveMarketPairs = &highLows.NumberOfActiveMarketPairs
	fundamental.CirculatingSupply = ConvertBQFloatToFloat(ticker.CirculatingSupply)
	fundamental.Supply = ConvertBQFloatToFloat(ticker.MaxSupply)
	fundamental.Date = ticker.OccurrenceTime
	fundamental.Price24h = ConvertBQFloatToFloat(ticker.Price)

	// // Exchange Data
	if len(exchangeData) > 0 {
		fundamental.ListedExchanges = GetExchangesList(exchangeData)
		fundamental.Exchanges = exchangeData
	}

	fundamental.Nomics.Volume1D = ConvertBQFloatToNormalFloat(ticker.Volume)
	fundamental.Volume = ConvertBQFloatToFloat(ticker.Volume)

	_ = extractHighLowValues(ctxO, &fundamental, highLows)

	// Set Data Source
	fundamental.Source = ticker.SOURCE

	// assets status
	fundamental.Status = ticker.Status

	// Set Last Updated
	fundamental.LastUpdated = time.Now()

	span.SetStatus(otelCodes.Ok, "Success")
	return fundamental, nil

}

// Check the low value if it is not Zero and
// if the low value is zero will cause Issue with data inserted as (NAN, Infinity) for percentage (24h, 7d, 30d, 1y, ytd)
func CalculatePercentageChange(priceThen, priceNow *float64) *float64 {
	var newPercentChange float64 = 0

	if priceThen == nil {
		return &newPercentChange
	}

	if *priceThen > 0 {
		percentChange := (*priceNow - *priceThen) / *priceThen
		return &percentChange
	}

	return &newPercentChange
}

// will map all exchanges with exchangesMetadata and exchangesProfiles
func CombineExchanges(ctx0 context.Context, exchangeMetadata model.CoingeckoExchangeMetadata, exchangeData ExchangeResults, exchangeProfiles map[string]model.ExchangeProfile) (ExchangeFundamentals, error) {
	_, span := tracer.Start(ctx0, "CombineExchanges")
	defer span.End()

	span.AddEvent("Start Combine Exchanges Fundamentals")
	var exchange ExchangeFundamentals
	exchange.Name = exchangeData.Name
	exchange.Slug = cleanString(strings.ReplaceAll(exchangeData.Name, " ", "-"))
	exchange.Logo = exchangeMetadata.LogoURL
	exchange.Id = exchangeData.Id
	exchange.ExchangeActiveMarketPairs = exchangeData.ExchangeActiveMarketPairs
	exchange.Nomics.VolumeByExchange1D = exchangeData.VolumeByExchange1D
	if exchangeProfile, ok := exchangeProfiles[exchange.Name]; ok {
		span.AddEvent("Start Combine Exchanges Fundamentals with Exchanges Profiles")
		volumeDiscount := exchangeProfile.VolumeDiscountPercent
		volumeDiscountPercent := 1 - (volumeDiscount / 100)
		exchange.Forbes.VolumeByExchange1D = exchange.Nomics.VolumeByExchange1D * volumeDiscountPercent
	}
	exchange.LastUpdated = time.Now()
	span.SetStatus(otelCodes.Ok, "Success")
	return exchange, nil
}

// MapNFTFundamentalsData will map all NFTVolumeData, NFTVolumePctData, NFTFloorPriceData, NFTMetaData, nftQuestionsTemplate and coinsData to NFTSalesData
// It Takes (ctx context.Context, NFTSalesData []FundamentalsNFTSalesData, NFTVolumeData, NFTFloorPriceData, NFTVolumePctData, NFTMetaData map[string]FundamentalsNFTSalesData, nftQuestionsTemplate []FSNFTQuestion, coinsData map[string]string)
// Returns []FundamentalsNFTSalesData
//
// It will map all NFTs Volume data and Floor Price Data that we calculate it from Bigquery and map it to each NFT
// It will map the Questions from each NFT and build the links inside the NFT description.
// Returns []FundamentalsNFTSalesData with all fields to be add it to PG
func MapNFTFundamentalsData(ctx context.Context, NFTSalesData []FundamentalsNFTSalesData, NFTVolumeData, NFTFloorPriceData, NFTPctData, NFTMetaData map[string]FundamentalsNFTSalesData, nftQuestionsTemplate []FSNFTQuestion, coinsData map[string]string, nftSocialMediaLinks []string) []FundamentalsNFTSalesData {
	_, span := tracer.Start(ctx, "MapNFTFundamentalsData")
	defer span.End()
	span.AddEvent("Start MapNFTFundamentalsData")
	log.Info("Start MapNFTFundamentalsData")
	var NFTSalesDataResult []FundamentalsNFTSalesData
	for _, nftSale := range NFTSalesData {
		nftPctsData := NFTPctData[nftSale.ID]
		nftVolume := NFTVolumeData[nftSale.ID]
		nftFloorPrice := NFTFloorPriceData[nftSale.ID]
		nftMetaData := NFTMetaData[nftSale.ID]

		nftSale.VolumeUSD1d = nftVolume.VolumeUSD1d
		nftSale.VolumeUSD7d = nftVolume.VolumeUSD7d
		nftSale.VolumeUSD30d = nftVolume.VolumeUSD30d
		nftSale.VolumeUSD90d = nftVolume.VolumeUSD90d
		nftSale.VolumeUSDYtd = nftVolume.VolumeUSDYtd

		nftSale.VolumeNative1d = nftVolume.VolumeNative1d
		nftSale.VolumeNative7d = nftVolume.VolumeNative7d
		nftSale.VolumeNative30d = nftVolume.VolumeNative30d
		nftSale.VolumeNative90d = nftVolume.VolumeNative90d
		nftSale.VolumeNativeYtd = nftVolume.VolumeNativeYtd

		nftSale.PctChangeVolumeNative1d = nftPctsData.PctChangeVolumeNative1d
		nftSale.PctChangeVolumeNative7d = nftPctsData.PctChangeVolumeNative7d
		nftSale.PctChangeVolumeNative30d = nftPctsData.PctChangeVolumeNative30d
		nftSale.PctChangeVolumeNative90d = nftPctsData.PctChangeVolumeNative90d
		nftSale.PctChangeVolumeNativeYtd = nftPctsData.PctChangeVolumeNativeYtd

		nftSale.PctChangeVolumeUSD1d = nftPctsData.PctChangeVolumeUSD1d
		nftSale.PctChangeVolumeUSD7d = nftPctsData.PctChangeVolumeUSD7d
		nftSale.PctChangeVolumeUSD30d = nftPctsData.PctChangeVolumeUSD30d
		nftSale.PctChangeVolumeUSD90d = nftPctsData.PctChangeVolumeUSD90d
		nftSale.PctChangeVolumeUSDYtd = nftPctsData.PctChangeVolumeUSDYtd

		nftSale.LowestFloorPrice24hUsd = nftFloorPrice.LowestFloorPrice24hUsd
		nftSale.LowestFloorPrice24hNative = nftFloorPrice.LowestFloorPrice24hNative
		nftSale.HighestFloorPrice24hUsd = nftFloorPrice.HighestFloorPrice24hUsd
		nftSale.HighestFloorPrice24hNative = nftFloorPrice.HighestFloorPrice24hNative
		nftSale.FloorPrice24hPercentageChangeUsd = nftFloorPrice.FloorPrice24hPercentageChangeUsd
		nftSale.FloorPrice24hPercentageChangeNative = nftFloorPrice.FloorPrice24hPercentageChangeNative
		nftSale.LowestFloorPrice24hPercentageChangeUSD = nftFloorPrice.LowestFloorPrice24hPercentageChangeUSD
		nftSale.LowestFloorPrice24hPercentageChangeNative = nftFloorPrice.LowestFloorPrice24hPercentageChangeNative
		nftSale.HighestFloorPrice24hPercentageChangeUSD = nftFloorPrice.HighestFloorPrice24hPercentageChangeUSD
		nftSale.HighestFloorPrice24hPercentageChangeNative = nftFloorPrice.HighestFloorPrice24hPercentageChangeNative

		nftSale.LowestFloorPrice7dUsd = nftFloorPrice.LowestFloorPrice7dUsd
		nftSale.HighestFloorPrice7dUsd = nftFloorPrice.HighestFloorPrice7dUsd
		nftSale.LowestFloorPrice7dNative = nftFloorPrice.LowestFloorPrice7dNative
		nftSale.HighestFloorPrice7dNative = nftFloorPrice.HighestFloorPrice7dNative
		nftSale.FloorPrice7dPercentageChangeUsd = nftFloorPrice.FloorPrice7dPercentageChangeUsd
		nftSale.FloorPrice7dPercentageChangeNative = nftFloorPrice.FloorPrice7dPercentageChangeNative
		nftSale.LowestFloorPrice7dPercentageChangeUSD = nftFloorPrice.LowestFloorPrice7dPercentageChangeUSD
		nftSale.LowestFloorPrice7dPercentageChangeNative = nftFloorPrice.LowestFloorPrice7dPercentageChangeNative
		nftSale.HighestFloorPrice7dPercentageChangeUSD = nftFloorPrice.HighestFloorPrice7dPercentageChangeUSD
		nftSale.HighestFloorPrice7dPercentageChangeNative = nftFloorPrice.HighestFloorPrice7dPercentageChangeNative

		nftSale.LowestFloorPrice30dUsd = nftFloorPrice.LowestFloorPrice30dUsd
		nftSale.HighestFloorPrice30dUsd = nftFloorPrice.HighestFloorPrice30dUsd
		nftSale.LowestFloorPrice30dNative = nftFloorPrice.LowestFloorPrice30dNative
		nftSale.HighestFloorPrice30dNative = nftFloorPrice.HighestFloorPrice30dNative
		nftSale.FloorPrice30dPercentageChangeUsd = nftFloorPrice.FloorPrice30dPercentageChangeUsd
		nftSale.FloorPrice30dPercentageChangeNative = nftFloorPrice.FloorPrice30dPercentageChangeNative
		nftSale.LowestFloorPrice30dPercentageChangeUSD = nftFloorPrice.LowestFloorPrice30dPercentageChangeUSD
		nftSale.LowestFloorPrice30dPercentageChangeNative = nftFloorPrice.LowestFloorPrice30dPercentageChangeNative
		nftSale.HighestFloorPrice30dPercentageChangeUSD = nftFloorPrice.HighestFloorPrice30dPercentageChangeUSD
		nftSale.HighestFloorPrice30dPercentageChangeNative = nftFloorPrice.HighestFloorPrice30dPercentageChangeNative

		nftSale.LowestFloorPrice90dUsd = nftFloorPrice.LowestFloorPrice90dUsd
		nftSale.HighestFloorPrice90dUsd = nftFloorPrice.HighestFloorPrice90dUsd
		nftSale.LowestFloorPrice90dNative = nftFloorPrice.LowestFloorPrice90dNative
		nftSale.HighestFloorPrice90dNative = nftFloorPrice.HighestFloorPrice90dNative
		nftSale.FloorPrice90dPercentageChangeUsd = nftFloorPrice.FloorPrice90dPercentageChangeUsd
		nftSale.FloorPrice90dPercentageChangeNative = nftFloorPrice.FloorPrice90dPercentageChangeNative
		nftSale.LowestFloorPrice90dPercentageChangeUSD = nftFloorPrice.LowestFloorPrice90dPercentageChangeUSD
		nftSale.LowestFloorPrice90dPercentageChangeNative = nftFloorPrice.LowestFloorPrice90dPercentageChangeNative
		nftSale.HighestFloorPrice90dPercentageChangeUSD = nftFloorPrice.HighestFloorPrice90dPercentageChangeUSD
		nftSale.HighestFloorPrice90dPercentageChangeNative = nftFloorPrice.HighestFloorPrice90dPercentageChangeNative

		nftSale.LowestFloorPriceYtdUsd = nftFloorPrice.LowestFloorPriceYtdUsd
		nftSale.HighestFloorPriceYtdUsd = nftFloorPrice.HighestFloorPriceYtdUsd
		nftSale.LowestFloorPriceYtdNative = nftFloorPrice.LowestFloorPriceYtdNative
		nftSale.HighestFloorPriceYtdNative = nftFloorPrice.HighestFloorPriceYtdNative
		nftSale.FloorPriceYtdPercentageChangeUsd = nftFloorPrice.FloorPriceYtdPercentageChangeUsd
		nftSale.FloorPriceYtdPercentageChangeNative = nftFloorPrice.FloorPriceYtdPercentageChangeNative
		nftSale.LowestFloorPriceYtdPercentageChangeUSD = nftFloorPrice.LowestFloorPriceYtdPercentageChangeUSD
		nftSale.LowestFloorPriceYtdPercentageChangeNative = nftFloorPrice.LowestFloorPriceYtdPercentageChangeNative
		nftSale.HighestFloorPriceYtdPercentageChangeUSD = nftFloorPrice.HighestFloorPriceYtdPercentageChangeUSD
		nftSale.HighestFloorPriceYtdPercentageChangeNative = nftFloorPrice.HighestFloorPriceYtdPercentageChangeNative

		nftSale.AvgTotalSalesPctChange1d = nftPctsData.AvgTotalSalesPctChange1d
		nftSale.AvgTotalSalesPctChange7d = nftPctsData.AvgTotalSalesPctChange7d
		nftSale.AvgTotalSalesPctChange30d = nftPctsData.AvgTotalSalesPctChange30d
		nftSale.AvgTotalSalesPctChange90d = nftPctsData.AvgTotalSalesPctChange90d
		nftSale.AvgTotalSalesPctChangeYtd = nftPctsData.AvgTotalSalesPctChangeYtd

		nftSale.AVGSalesPriceChange1d = nftPctsData.AVGSalesPriceChange1d
		nftSale.AVGSalesPriceChange7d = nftPctsData.AVGSalesPriceChange7d
		nftSale.AVGSalesPriceChange30d = nftPctsData.AVGSalesPriceChange30d
		nftSale.AVGSalesPriceChange90d = nftPctsData.AVGSalesPriceChange90d
		nftSale.AVGSalesPriceChangeYtd = nftPctsData.AVGSalesPriceChangeYtd

		nftSale.NFTQuestion = buildNFTsQuestions(ctx, nftMetaData, nftQuestionsTemplate, coinsData, NFTMetaData, nftSocialMediaLinks)

		NFTSalesDataResult = append(NFTSalesDataResult, nftSale)
	}

	span.SetStatus(otelCodes.Ok, "Success")
	log.Info("Finished MapNFTFundamentalsData")
	return NFTSalesDataResult
}

// isNonEnglish
// Takes string
// Returns bool
//
// It will check if the description contains Non English words
// It will returns bool value
func isNonEnglish(s string) bool {
	nonEnglishCount := 0
	totalCount := 0
	for _, r := range s {
		if !unicode.IsSpace(r) {
			totalCount++
			if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || unicode.IsPunct(r) || unicode.IsNumber(r) || r == '"') {
				nonEnglishCount++
			}
		}
	}
	// Consider text non-English if more than 30% of characters are non-English
	return totalCount > 0 && float64(nonEnglishCount)/float64(totalCount) > 0.3
}

// removeNonEnglishSentences
// Takes string
// Returns string
//
// Some text contains Non english and english words so this will clean it up and remove all Non english words
// Then it will build the english sentences and return it as result
func removeNonEnglishSentences(text string) string {
	// Split the text into sentences
	sentences := strings.Split(text, ".")

	// Regex to match any non-English character
	re := regexp.MustCompile(`[^\x00-\x7F]`)

	var englishSentences []string

	for _, sentence := range sentences {
		// If the sentence contains no non-English characters, keep it
		if !re.MatchString(sentence) {
			englishSentences = append(englishSentences, strings.TrimSpace(sentence))
		}
	}

	// Join the English sentences back together
	result := strings.Join(englishSentences, ".")

	// Remove any leading or trailing spaces
	result = strings.TrimSpace(result)

	return result
}

// isMorseCode
// Takes string
// Returns bool
//
// It will check if the description is Morse code or not
// It will returns bool value
func isMorseCode(s string) bool {
	// Morse code pattern: only dots, dashes, and spaces
	morsePattern := regexp.MustCompile(`^[.\-/ ]+$`)
	return morsePattern.MatchString(strings.TrimSpace(s))
}

// buildNFTsQuestions
// Takes (ctx context.Context, NFTMetaData FundamentalsNFTSalesData, nftQuestionsTemplate []FSNFTQuestion, coinsData map[string]string, nftsMetaData map[string]FundamentalsNFTSalesData)
// Returns  []NFTQuestion
//
// It will first replace the links exist in description and remove all Coingecko links
// Then it will check if the description has question in it from Coingecko side and it will change it to Forbes links
// Then will start to extract the Questions and Answer from description if they exist from coingecko if not we will use our template to build the Q&A for NFT
// Returns []NFTQuestion with Questions struct for NFT
func buildNFTsQuestions(ctx context.Context, NFTMetaData FundamentalsNFTSalesData, nftQuestionsTemplate []FSNFTQuestion, coinsData map[string]string, nftsMetaData map[string]FundamentalsNFTSalesData, nftSocialMediaLinks []string) []NFTQuestion {
	_, span := tracer.Start(ctx, "buildNFTsQuestions")
	defer span.End()
	span.AddEvent("Start buildNFTsQuestions")
	NFTMetaData.Description = replaceLinks(NFTMetaData.Description, nftsMetaData, coinsData, NFTMetaData.ID, nftSocialMediaLinks)
	NFTMetaData.Description = removeEmptyLinksAddSpaces(NFTMetaData.Description)

	/*
			(?:\r?\n---\r?\n)|            # Matches horizontal lines
		    (\*\*|\*)|                    # Matches ** and * for bold and italic
		    (</?[ap][^>]*>)|              # Matches HTML anchor and paragraph tags
		    (\\n)|                        # Matches literal \n
		    (\[|\])|                      # Matches square brackets
		    (👉\s*)|                      # Matches the pointing finger emoji and following space
		    [\r\n]                        # Matches newlines (from your original regex)
	*/
	cleanupRegex := regexp.MustCompile(`(?m)^---|---$|(?:\r?\n---\r?\n)|(\*\*|\*)|(</?[p][^>]*>)|(\\n)|(\[|\])|(👉\s*)|[\r\n]`)
	// Check any string that contains multiple spaces to be replaced with one space.
	cleanupSpaces := regexp.MustCompile(`\s+`)
	var nftQuestions []NFTQuestion
	if strings.Contains(NFTMetaData.Description, "<h3 dir=\"ltr\">") || strings.Contains(NFTMetaData.Description, "<h3>") {
		splitChar := "<h3 dir=\"ltr\">"
		if !strings.Contains(NFTMetaData.Description, "<h3 dir=\"ltr\">") {
			splitChar = "<h3>"
		}
		sub := strings.Split(NFTMetaData.Description, splitChar)

		for _, ques := range sub[1:] {
			questionParts := strings.Split(ques, "</h3>")
			if len(questionParts) > 1 {
				question := questionParts[0]
				answer := cleanupRegex.ReplaceAllString(questionParts[1], " ")
				answer = cleanupSpaces.ReplaceAllString(answer, " ")
				// Decoding HTML-escaped characters present in our description, if any.
				answer = html.UnescapeString(answer)
				answer = strings.TrimSpace(answer)
				nftQuestions = append(nftQuestions, NFTQuestion{
					Question: question,
					Answer:   strings.TrimSpace(answer),
				})
			}
		}
	} else {
		description := removeNonEnglishSentences(NFTMetaData.Description)
		cleanDescription := cleanupRegex.ReplaceAllString(description, " ")
		// Decoding HTML-escaped characters present in our description, if any.
		cleanDescription = html.UnescapeString(cleanDescription)
		cleanDescription = strings.ReplaceAll(cleanDescription, "\\n", " ")
		cleanDescription = cleanupSpaces.ReplaceAllString(cleanDescription, " ")
		NFTMetaData.Description = strings.TrimSpace(cleanDescription)

		if len(strings.Fields(NFTMetaData.Description)) < 50 {
			nftQuestions = BuildQuestionTemplate(ctx, NFTMetaData, true, nftQuestionsTemplate)
		} else {
			nftQuestions = BuildQuestionTemplate(ctx, NFTMetaData, false, nftQuestionsTemplate)
		}
	}

	span.SetStatus(otelCodes.Ok, "Success")
	return nftQuestions

}

// cleanSlug
// Takes string
// Returns string
//
// Some slug that exist in Coingecko links contains # so we need to remove it to map slug name from our side with our links.
// Returns string this will be the slug without any special character.
func cleanSlug(slug string) string {
	// Define a regular expression to match special characters
	re := regexp.MustCompile(`[#]`)

	// Remove the special characters
	cleaned := re.ReplaceAllString(slug, "")

	// Trim any leading or trailing spaces
	cleaned = strings.TrimSpace(cleaned)

	return cleaned
}

// replaceLinks modifies the provided HTML `description` by replacing certain external links with links to a specific domain,
// removing unwanted links, and retaining selected social media links.
// Parameters:
// - description: A string containing HTML content with links.
// - nftsMetaData: A map with NFT metadata, where the key is a sanitized NFT identifier, and the value is a FundamentalsNFTSalesData struct
//   containing additional information, such as the Forbes slug.
// - coinsData: A map linking cryptocurrency slugs to Forbes asset slugs.
// - nftId: A unique identifier for the NFT being processed, which will also be used to construct social media URLs.
//
// Process:
// 1. Replaces links to NFTs on CoinGecko with links to Forbes' NFT pages if the NFT identifier matches data in `nftsMetaData`.
// 2. Replaces links to coins on CoinGecko with links to Forbes' asset pages if the coin identifier matches data in `coinsData`.
// 3. Defines a list of allowed social media domains and custom identifiers (based on `nftId`) and removes any other external links.
//    Links to allowed domains and specific social media platforms are retained; other external links are removed.
//
// Returns:
// - The updated `description` string with the replaced or removed links.

func replaceLinks(description string, nftsMetaData map[string]FundamentalsNFTSalesData, coinsData map[string]string, nftId string, nftSocialMediaLinks []string) string {
	// Compile regex to match <a> tags linking to NFTs on CoinGecko
	nftRegex := regexp.MustCompile(`<a\s+href="(https://www\.coingecko\.com/en/nft/[^"]+)"[^>]*>(.*?)</a>`)
	description = nftRegex.ReplaceAllStringFunc(description, func(match string) string {
		// Find submatches for the link and text in the <a> tag
		submatches := nftRegex.FindStringSubmatch(match)
		if len(submatches) == 3 {
			// The URL to the NFT on CoinGecko
			link := submatches[1]
			// Lowercase and replace spaces with hyphens
			text := strings.ReplaceAll(strings.ToLower(submatches[2]), " ", "-")

			// Split the link to get the identifier part after "nft/"
			parts := strings.SplitN(link, "nft/", 2)
			if len(parts) == 2 {
				// Check if the sanitized anchor text matches the cleaned identifier in the URL
				if strings.Contains(cleanSlug(text), cleanSlug(parts[1])) {
					// Retrieve the Forbes slug from `nftsMetaData` using the cleaned identifier
					nftSlug := nftsMetaData[cleanSlug(parts[1])]
					// Return a new <a> tag pointing to Forbes with the original text
					return fmt.Sprintf("<a href=\"https://www.forbes.com/digital-assets/nfts/%s\">%s</a>", nftSlug.Slug, submatches[2])
				} else {
					// If identifiers do not match, return an empty string to remove the link
					return ""
				}
			}
			// If parsing fails, return the original match
			return match
		}
		// If there are not exactly 3 submatches, return the original match
		return match
	})

	// Compile regex to match <a> tags linking to coins on CoinGecko
	coinRegex := regexp.MustCompile(`<a\s+href="(https://www\.coingecko\.com/en/coins/[^"]+)"[^>]*>(.*?)</a>`)
	description = coinRegex.ReplaceAllStringFunc(description, func(match string) string {
		// Find submatches for the link and text in the <a> tag
		submatches := coinRegex.FindStringSubmatch(match)
		if len(submatches) == 3 {
			// The URL to the coin on CoinGecko
			link := submatches[1]
			// Split the link to get the identifier part after "coins/"
			parts := strings.SplitN(link, "coins/", 2)
			if len(parts) == 2 {
				// Get the Forbes slug for the coin from `coinsData`
				slug := coinsData[parts[1]]
				// Return a new <a> tag pointing to Forbes with the original text
				return fmt.Sprintf("<a href=\"https://www.forbes.com/digital-assets/assets/%s\">%s</a>", slug, submatches[2])
			}
		}
		// If parsing fails, return the original match
		return match
	})

	// Define allowed social media and other domains
	socialMediaDomains := []string{
		fmt.Sprintf("%s.com", nftId),
		fmt.Sprintf("www.%s", nftId),
	}
	socialMediaDomains = append(socialMediaDomains, nftSocialMediaLinks...)

	// Compile regex to match all external links (not limited to specific sites)
	foreignLinkRegex := regexp.MustCompile(`https://([^"]+)`)
	description = foreignLinkRegex.ReplaceAllStringFunc(description, func(match string) string {
		lowerMatch := strings.ToLower(match)
		// Check if the link matches any allowed domain or social media
		for _, socialMedia := range socialMediaDomains {
			if strings.Contains(lowerMatch, socialMedia) {
				// Keep the link if it's a social media link
				return match
			}
		}
		// Remove the link if it's foreign
		return ""
	})

	return description
}

// removeEmptyLinksAddSpaces
// Takes string
// Returns String
// This function will take the NFT description and remove any empty links from it
// This will help us to prevent redirect links form our NFT page
// It will returns clean description without empty links
func removeEmptyLinksAddSpaces(description string) string {
	// Regular expression to match <a> tags with empty href and target="_blank"
	re := regexp.MustCompile(`<a\s+href=""\s+(?:rel="nofollow noopener"\s*)?target="_blank">(.*?)</a>`)

	// Replace the tag with the content, adding spaces before and after
	result := re.ReplaceAllStringFunc(description, func(match string) string {
		// Extract the text content from the tag
		content := re.FindStringSubmatch(match)[1]
		return fmt.Sprintf(" %s ", content)
	})

	// Trim any leading or trailing spaces and replace multiple spaces with a single space
	result = strings.TrimSpace(result)
	spaceRe := regexp.MustCompile(`\s+`)
	result = spaceRe.ReplaceAllString(result, " ")

	return result
}

// BuildQuestionTemplate
// Takes (ctx context.Context, nftMetaData FundamentalsNFTSalesData, isShort bool, nftQuestionsTemplate []FSNFTQuestion)
// Returns []NFTQuestion
//
// This will build the questions from the templates from FS
// Returns []NFTQuestion with new questions that we build it from our templates
func BuildQuestionTemplate(ctx context.Context, nftMetaData FundamentalsNFTSalesData, isShort bool, nftQuestionsTemplate []FSNFTQuestion) []NFTQuestion {
	_, span := tracer.Start(ctx, "buildNFTsQuestions")
	defer span.End()
	span.AddEvent("Start buildNFTsQuestions")

	var nftQuestions []NFTQuestion

	isNonEnglish := isNonEnglish(nftMetaData.Description)
	isMorseCode := isMorseCode(nftMetaData.Description)

	for _, question := range nftQuestionsTemplate {
		var answer string
		switch question.QuestionOrder {
		case 1: // First question
			// this mean the description not short text and it's english
			if !isShort && !isNonEnglish && !isMorseCode {
				answer = nftMetaData.Description
			} else {
				answer = fmt.Sprintf(question.Answer, nftMetaData.Name, strings.Title(nftMetaData.AssetPlatformId), nftMetaData.Year, nftMetaData.Name, nftMetaData.TotalSupply, nftMetaData.Name)
			}
		case 2: // Second question
			var marketPlace []string
			for _, ticker := range nftMetaData.Tickers {
				// build market places links
				s := fmt.Sprintf(`<a href=`+`"%s"`+`target=`+`"_blank">%s</a>`, ticker.NFTCollectionUrl, ticker.Name)
				marketPlace = append(marketPlace, s)
			}
			var lastExplorers string
			var ticker string = ""
			if len(marketPlace) == 0 {
				continue
			} else if len(marketPlace) > 1 {
				lastExplorers = fmt.Sprintf(" and %s", marketPlace[len(marketPlace)-1])
				// Add spaces between the market places links
				ticker = fmt.Sprintf("%s %s", strings.Join(marketPlace[:len(marketPlace)-1], ", "), lastExplorers)
			} else {
				ticker = strings.Join(marketPlace, ", ")
			}
			answer = fmt.Sprintf(question.Answer, nftMetaData.Name, ticker)
		}
		nftQuestions = append(nftQuestions, NFTQuestion{
			Question: fmt.Sprintf(question.Question, nftMetaData.Name),
			Answer:   strings.TrimSpace(answer),
		})
	}

	span.SetStatus(otelCodes.Ok, "Success")
	return nftQuestions
}
