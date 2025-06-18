package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/firestore"
	"github.com/Forbes-Media/forbes-digital-assets/model"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/services"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelCodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/exp/slices"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	firestoreClient    *firestore.Client
	firstoreClientOnce sync.Once
	BQProjectID        = "api-project-901373404215"
	mu                 sync.Mutex
	tracer             = otel.Tracer("github.com/Forbes-Media/forbes-digital-assets/store")
)

type FundamentalsData struct {
	Symbol                    string              `json:"symbol" firestore:"symbol" postgres:"symbol"`
	Name                      string              `json:"name" firestore:"name" postgres:"name"`
	Slug                      string              `json:"slug" firestore:"slug" postgres:"slug"`
	Logo                      string              `json:"logo" firestore:"logo" postgres:"logo"`
	FloatType                 string              `json:"floatType" firestore:"floatType" postgres:"float_type"`
	DisplaySymbol             string              `json:"displaySymbol" firestore:"displaySymbol" postgres:"display_symbol"`
	Source                    string              `json:"source" firestore:"source" postgres:"source"`
	TemporaryDataDelay        bool                `json:"temporaryDataDelay" firestore:"temporaryDataDelay" postgres:"temporary_data_delay"`
	Volume                    *float64            `json:"volume" firestore:"volume" postgres:"volume"`
	High1h                    *float64            `json:"high1h" firestore:"high1h" postgres:"high_1h"`
	Low1h                     *float64            `json:"low1h" firestore:"low1h" postgres:"low_1h"`
	High24h                   *float64            `json:"high24h" firestore:"high24h" postgres:"high_24h"`
	Low24h                    *float64            `json:"low24h" firestore:"low24h" postgres:"low_24h"`
	High7D                    *float64            `bigquery:"high_7d" json:"high7d" postgres:"high_7d"`
	Low7D                     *float64            `bigquery:"low_7d" json:"low7d" postgres:"low_7d"`
	High30D                   *float64            `bigquery:"high_30d" json:"high30d" postgres:"high_30d"`
	Low30D                    *float64            `bigquery:"low_30d" json:"low30d" postgres:"low_30d"`
	High1Y                    *float64            `bigquery:"high_1y" json:"high1y" postgres:"high_1y"`
	Low1Y                     *float64            `bigquery:"low_1y" json:"low1y" postgres:"low_1y"`
	Price24h                  *float64            `json:"price24h" firestore:"price24h" postgres:"price_24h"`
	Percentage24h             *float64            `json:"percentage24h" firestore:"percentage24h" postgres:"percentage_24h"`
	AllTimeHigh               *float64            `json:"allTimeHigh" firestore:"allTimeHigh" postgres:"all_time_high"`
	AllTimeLow                *float64            `json:"allTimeLow" firestore:"allTimeLow" postgres:"all_time_low"`
	Date                      time.Time           `json:"date" firestore:"date" postgres:"date"`
	ChangeValue24h            *float64            `json:"changeValue24h" firestore:"changeValue24h" postgres:"change_value_24h"`
	ListedExchanges           []string            `json:"listedExchanges" firestore:"listedExchanges" postgres:"listed_exchange"`
	MarketCap                 *float64            `json:"marketCap" firestore:"marketCap" postgres:"market_cap"`
	Supply                    *float64            `json:"supply" firestore:"supply" postgres:"supply"`
	Exchanges                 []FirestoreExchange `json:"exchanges" firestore:"exchanges" postgres:"exchanges"`
	OriginalSymbol            string              `json:"originalSymbol" firestore:"originalSymbol" postgres:"original_symbol"`
	NumberOfActiveMarketPairs *int64              `json:"numberOfActiveMarketPairs" firestore:"numberOfActiveMarketPairs" postgres:"number_of_active_market_pairs"`
	Nomics                    Volume              `json:"nomics" firestore:"nomics" postgres:"nomics"`
	Forbes                    Volume              `json:"forbes" firestore:"forbes"`
	HighYTD                   *float64            `json:"highYtd" firestore:"highYtd" postgres:"high_ytd"`
	LowYTD                    *float64            `json:"lowYtd" firestore:"lowYtd" postgres:"low_ytd"`
	Price1H                   *float64            `json:"price_1h" firestore:"price1h" postgres:"price_1h"`
	Price7D                   *float64            `json:"price_7d" firestore:"price7d" postgres:"price_7d"`
	Price30D                  *float64            `json:"price_30d" firestore:"price30d" postgres:"price_30d"`
	Price1Y                   *float64            `json:"price_1Y" firestore:"price1Y" postgres:"price_1Y"`
	PriceYTD                  *float64            `json:"price_ytd" firestore:"priceYtd" postgres:"price_ytd"`
	Percentage1H              *float64            `json:"percentage_1h" firestore:"percentage_1h" postgres:"percentage_1h"`
	Percentage7D              *float64            `json:"percentage_7d" firestore:"percentage_7d" postgres:"percentage_7d"`
	Percentage30D             *float64            `json:"percentage_30d" firestore:"percentage_30d" postgres:"percentage_30d"`
	Percentage1Y              *float64            `json:"percentage_1y" firestore:"percentage_1y" postgres:"percentage_1y"`
	PercentageYTD             *float64            `json:"percentage_ytd" firestore:"percentage_ytd" postgres:"percentage_ytd"`
	MarketCapPercentChange1H  *float64            `json:"marketCapPercentChange1h" firestore:"marketCapPercentChange1h" postgres:"market_cap_percent_change_1h"`
	MarketCapPercentChange1D  *float64            `json:"marketCapPercentChange1d" firestore:"marketCapPercentChange1d" postgres:"market_cap_percent_change_1d"`
	MarketCapPercentChange7D  *float64            `json:"marketCapPercentChange7d" firestore:"marketCapPercentChange7d" postgres:"market_cap_percent_change_7d"`
	MarketCapPercentChange30D *float64            `json:"marketCapPercentChange30d" firestore:"marketCapPercentChange30d" postgres:"market_cap_percent_change_30d"`
	MarketCapPercentChange1Y  *float64            `json:"marketCapPercentChange1y" firestore:"marketCapPercentChange1y" postgres:"market_cap_percent_change_1y"`
	MarketCapPercentChangeYTD *float64            `json:"marketCapPercentChangeYtd" firestore:"marketCapPercentChangeYtd" postgres:"market_cap_percent_change_ytd"`
	CirculatingSupply         *float64            `json:"circulatingSupply" firestore:"circulatingSupply" postgres:"circulating_supply"`
	MarketPairs               []MarketPairs       `postgres:"market_pairs" json:"market_pairs,omitempty" firestore:"marketPairs"`
	LastUpdated               time.Time           `postgres:"last_updated" json:"last_updated"`
}

type MarketPairs struct {
	Base                   string            `postgres:"base" json:"base"`
	Quote                  string            `postgres:"quote" json:"quote"`
	Pair                   string            `postgres:"pair" json:"pair"`
	Exchange               string            `postgres:"exchange" json:"exchange"`
	PairStatus             string            `postgres:"pair_status" json:"pairStatus"`
	TypeOfPair             string            `postgres:"type_of_pair" json:"typeOfPair"`
	CurrentPriceForPair1D  *float64          `postgres:"current_price_for_pair_1d" json:"currentPriceForPair1D"`
	CurrentPriceForPair7D  *float64          `postgres:"current_price_for_pair_7d" json:"currentPriceForPair7D"`
	CurrentPriceForPair30D *float64          `postgres:"current_price_for_pair_30d" json:"currentPriceForPair30D"`
	CurrentPriceForPair1Y  *float64          `postgres:"current_price_for_pair_1y" json:"currentPriceForPair1Y"`
	CurrentPriceForPairYTD *float64          `postgres:"current_price_for_pair_ytd" json:"currentPriceForPairYTD"`
	Nomics                 MarketPairsVolume `postgres:"nomics" json:"nomics,omitempty"`
	Forbes                 MarketPairsVolume `postgres:"forbes" json:"forbes,omitempty"`
	UpdateTimeStamp        time.Time         `postgres:"update_timestamp" json:"update_timestamp"`
}

type MarketPairsVolume struct {
	VolumeForPair1D  float64 `postgres:"volume_for_pair_1d" json:"volumeForPair1D"`
	VolumeForPair7D  float64 `postgres:"volume_for_pair_7d" json:"volumeForPair7D"`
	VolumeForPair30D float64 `postgres:"volume_for_pair_30d" json:"volumeForPair30D"`
	VolumeForPair1Y  float64 `postgres:"volume_for_pair_1y" json:"volumeForPair1Y"`
	VolumeForPairYTD float64 `postgres:"volume_for_pair_ytd" json:"volumeForPairYTD"`
}

type Volume struct {
	Volume1H            float64  `json:"volume_1h" firestore:"volume1h"`
	Volume1D            float64  `json:"volume_1d" firestore:"volume1d"`
	Volume7D            float64  `json:"volume_7d" firestore:"volume7d"`
	Volume30D           float64  `json:"volume_30d" firestore:"volume30d"`
	Volume1Y            float64  `json:"volume_1y" firestore:"volume1y"`
	VolumeYTD           float64  `json:"volume_ytd" firestore:"volumeYtd"`
	PercentageVolume1H  *float64 `json:"percentageVolume_1h" firestore:"percentageVolume1h"`
	PercentageVolume1D  *float64 `json:"percentageVolume_1d" firestore:"percentageVolume1d"`
	PercentageVolume7D  *float64 `json:"percentageVolume_7d" firestore:"percentageVolume7d"`
	PercentageVolume30D *float64 `json:"percentageVolume_30d" firestore:"percentageVolume30d"`
	PercentageVolume1Y  *float64 `json:"percentageVolume_1y" firestore:"percentageVolume1y"`
	PercentageVolumeYTD *float64 `json:"percentageVolume_ytd" firestore:"percentageVolumeYtd"`
}

type LeadersAndLaggardsData struct {
	Symbol        string   `json:"symbol" firestore:"symbol"`
	DisplaySymbol string   `json:"displaySymbol" firestore:"displaySymbol"`
	Name          string   `json:"name" firestore:"name"`
	Slug          string   `json:"slug" firestore:"slug"`
	Logo          string   `json:"logo" firestore:"logo"`
	Price         *float64 `json:"price" firestore:"price"`
	Percentage    *float64 `json:"percentage" firestore:"percentage"`
	ChangeValue   *float64 `json:"changeValue" firestore:"changeValue"`
}
type WatchlistData struct {
	Symbol        string   `json:"symbol" firestore:"symbol,omitempty"`
	DisplaySymbol string   `json:"displaySymbol" firestore:"displaySymbol,omitempty"`
	OldSymbol     string   `json:"oldSymbol" firestore:"oldSymbol,omitempty"`
	Type          string   `json:"type" firestore:"type,omitempty"`
	Name          string   `json:"name" firestore:"name,omitempty"`
	Slug          string   `json:"slug" firestore:"slug,omitempty"`
	Logo          string   `json:"logo" firestore:"logo,omitempty"`
	Price         *float64 `json:"price" firestore:"price,omitempty"`
	Percentage    *float64 `json:"percentage" firestore:"percentage,omitempty"`
	ChangeValue   *float64 `json:"changeValue" firestore:"changeValue,omitempty"`
	WatchTime     string   `json:"watchTime" firestore:"watchTime,omitempty"`
}

type Top30MarketCap struct {
	TopAssetsByMarketCap []FundamentalsData `json:"TopAssetsByMarketCap"`
	TotalMarketCap       *float64           `json:"totalMarketCap"`
}

type TradedAssetsTable struct {
	Symbol                string   `json:"symbol" firestore:"symbol" postgres:"symbol"`
	DisplaySymbol         string   `json:"displaySymbol" firestore:"displaySymbol" postgres:"displaySymbol"`
	Name                  string   `json:"name" firestore:"name" postgres:"name"`
	Slug                  string   `json:"slug" firestore:"slug" postgres:"slug"`
	Logo                  string   `json:"logo" firestore:"logo" postgres:"logo"`
	TemporaryDataDelay    bool     `json:"temporaryDataDelay" firestore:"temporaryDataDelay" postgres:"temporary_data_delay"`
	Price                 *float64 `json:"price" firestore:"price" postgres:"price_24h"`
	Percentage            *float64 `json:"percentage" firestore:"percentage" postgres:"percentage_24h"`
	Percentage1H          *float64 `json:"percentage_1h" firestore:"percentage_1h" postgres:"percentage_1h"`
	Percentage7D          *float64 `json:"percentage_7d" firestore:"percentage_7d" postgres:"percentage_7d"`
	ChangeValue           *float64 `json:"changeValue" firestore:"changeValue" postgres:"change_value_24h"`
	MarketCap             *float64 `json:"marketCap" firestore:"marketCap" postgres:"market_cap"`
	Volume                *float64 `json:"volume" firestore:"volume" postgres:"volume_1d"`
	FullCount             *int     `postgres:"full_count"`
	Rank                  *int     `json:"rank" firestore:"rank" postgres:"rank"`
	Status                string   `postgres:"status"`
	MarketCapPercentage1d *float64 `json:"market_cap_percent_change_1d" firestore:"market_cap_percent_change_1d" postgres:"market_cap_percent_change_1d"`
}

type IndexRebalancing struct {
	IndexName     string              `json:"indexName"`
	RebalanceTime time.Time           `json:"rebalanceTime"`
	IndexStatus   bigquery.NullString `json:"indexStatus"`
	IndexPrice    float64             `firestore:"indexPrice"`
	IndexContent  []IndexContent
}

type IndexContent struct {
	Symbol    string  `json:"symbol" firestore:"symbol"`
	MarketCap float64 `json:"marketCap" firestore:"marketCap"`
	Weight    float64 `json:"weight" firestore:"weight"`
}

type IndexTableResponse struct {
	Symbol      string   `json:"symbol" firestore:"symbol"`
	Name        string   `json:"name" firestore:"name"`
	Slug        string   `json:"slug" firestore:"slug"`
	Logo        string   `json:"logo" firestore:"logo"`
	Price       *float64 `json:"price" firestore:"price"`
	Percentage  *float64 `json:"percentage" firestore:"percentage"`
	ChangeValue *float64 `json:"changeValue" firestore:"changeValue"`
	MarketCap   *float64 `json:"marketCap" firestore:"marketCap"`
	Weight      *float64 `json:"weight" firestore:"weight"`
}

type FirestoreExchange struct {
	Exchange                     string         `bigquery:"Exchange" json:"market"`
	Time                         time.Time      `bigquery:"Time" json:"time"`
	Close                        *float64       `bigquery:"Close" json:"close"`
	Slug                         string         `json:"slug" firestore:"slug"`
	NumberOfActivePairsForAssets *int64         `firestore:"numberOfActivePairs_for_assets" json:"number_of_active_pairs_for_assets"`
	PriceByExchange1D            *float64       `firestore:"price_by_exchange_1d" json:"price_by_exchange_1d"`
	PriceByExchange7D            *float64       `firestore:"priceByExchange7D" json:"price_by_exchange_7d"`
	PriceByExchange30D           *float64       `firestore:"priceByExchange30D" json:"price_by_exchange_30d"`
	PriceByExchange1Y            *float64       `firestore:"priceByExchange1Y" json:"price_by_exchange_1y"`
	PriceByExchangeYTD           *float64       `firestore:"priceByExchangeYTD" json:"price_by_exchange_ytd"`
	Nomics                       ExchangeVolume `firestore:"nomics" json:"nomics,omitempty"`
	Forbes                       ExchangeVolume `firestore:"forbes" json:"forbes,omitempty"`
	UpdatedTimeStamp             time.Time      `json:"updatedTimeStamp" firestore:"updatedTimeStamp"`
	VolumeDiscountPercent        float64        `json:"volumeDiscountPercent" firestore:"volumeDiscountPercent"`
}

type ExchangeVolume struct {
	VolumeByExchange1D  float64 `firestore:"volumeByExchange1D" json:"volume_by_exchange_1d"`
	VolumeByExchange7D  float64 `firestore:"volumeByExchange7D" json:"volume_by_exchange_7d"`
	VolumeByExchange30D float64 `firestore:"volumeByExchange30D" json:"volume_by_exchange_30d"`
	VolumeByExchange1Y  float64 `firestore:"volumeByExchange1Y" json:"volume_by_exchange_1y"`
	VolumeByExchangeYTD float64 `firestore:"volumeByExchangeYTD" json:"volume_by_exchange_ytd"`
}

type TradedAssetsResp struct {
	Assets                []TradedAssetsTable `json:"assets"`
	Total                 int                 `json:"total"`
	HasTemporaryDataDelay bool                `json:"hasTemporaryDataDelay"`
	Source                string              `json:"source"`
}

type Watch struct {
	Symbol    string `json:"symbol" firestore:"symbol"`
	WatchTime string `json:"watchTime" firestore:"watchTime"`
}
type LeaderAndLaggards struct {
	Leaders  []LeadersAndLaggardsData `json:"leaders"`
	Laggards []LeadersAndLaggardsData `json:"laggards"`
	Source   string                   `json:"source"`
}

type NomicsExchanges struct {
	Exchanges []FirestoreExchange `json:"exchanges" firestore:"exchanges"`
}
type NomicsCarouselData struct {
	Assets []LeadersAndLaggardsData `json:"assets"`
	Source string                   `json:"source"`
}

type NomicsWatchData struct {
	Assets []WatchlistData `json:"assets"`
	Source string          `json:"source"`
}

type ExchangeFundamentals struct {
	Name                      string         `json:"name" postgres:"name" bigquery:"name"`
	Slug                      string         `json:"slug" postgres:"slug" bigquery:"slug"`
	Id                        string         `json:"id" postgres:"id" bigquery:"id"`
	Logo                      string         `json:"logo" postgres:"logo" bigquery:"logo"`
	ExchangeActiveMarketPairs int            `json:"exchange_active_market_pairs" postgres:"exchange_active_market_pairs" bigquery:"exchange_active_market_pairs"`
	Nomics                    ExchangeVolume `json:"nomics" postgres:"nomics" bigquery:"nomics"`
	Forbes                    ExchangeVolume `json:"forbes" postgres:"forbes" bigquery:"forbes"`
	LastUpdated               time.Time      `json:"last_updated" postgres:"last_updated" bigquery:"last_updated"`
}

type FeaturedCategory struct {
	ID            string `json:"category_id" firestore:"categoryId"`     // The Feature category id will use it in Search Traded Assets Tags
	Name          string `json:"category_name" firestore:"categoryName"` // The Feature category Name will use it to be displayed on Category Carousel
	Inactive      bool   `json:"inactive" firestore:"inactive"`          // The Feature category Inactive status will prevent us from building its fundamentals. This status is being set from coingeck-ingestion when it encounters a category without "Top 3 coins" field.
	Link          string `json:"link" firestore:"categoryLink"`          // The Feature category Link will use to lead to the Category page that is part of the News Page feature.
	ForbesId      string `json:"forbesId" firestore:"forbesId"`          // The Feature category Link will use to lead to the Category page that is part of the News Page feature.
	ForbesName    string `json:"forbesName" firestore:"forbesName"`      // The Feature category Link will use to lead to the Category page that is part of the News Page feature.
	IsFeatured    bool   `json:"isFeatured" firestore:"isFeatured"`
	CategoryOrder int    `json:"categoryOrder" firestore:"categoryOrder"`
}

type CryptoListSection struct {
	Title string `json:"title" firestore:"description"` // The Crypto List Section Title will contains the Title for thr list ex:("Forbes Blockchain 50 2022")
	URL   string `json:"link" firestore:"link"`         // The Crypto List Section URL will contains the Url for thr list page.
	Image string `json:"image" firestore:"image"`       // The Crypto List Section Image will contains the Image that will display in lists section.
}

type ListSection struct {
	Description string              `json:"description" firestore:"description"` // This is the global description that will display above the lists section
	Lists       []CryptoListSection `json:"lists"`
}

type NFTChain struct {
	ID   string `json:"id" firestore:"id"`     // Id of chain and it will present the assets platform id from the NFT endpoint. We will use it to filter NFTs by chains.
	Name string `json:"name" firestore:"name"` // Name for Chain, it will be used to display in the NFT prices Page.
}

// This struct contains tweet information at is populated manually through rowy
type TwitterId struct {
	OrderBy        int       `json:"orderBy" firestore:"orderBy"`               // The order by column. (The higher the number the more recent the tweet)
	TwitterURL     string    `json:"twitterUrl" firestore:"twitterUrl"`         // The URL of a tweet
	TwitterHandle  string    `json:"twitterHandle" firestore:"twitterHandle"`   // The Twitter handle that published the tweet
	FDAPublishDate time.Time `json:"fdaPublishDate" firestore:"fdaPublishDate"` // The date the tweet was imorted into rowy
}

type FundamentalsTopic struct {
	Symbol string `json:"symbol" firestore:"symbol" postgres:"symbol"`
	Name   string `json:"name" firestore:"name" postgres:"name"`
	Slug   string `json:"slug" firestore:"slug" postgres:"slug"`
}

type FSNFTQuestion struct {
	Question      string `json:"question" firestore:"question"`
	Answer        string `json:"answer" firestore:"answer"`
	QuestionOrder int    `json:"questionOrder" firestore:"questionOrder"`
}

// creates a firestore client and sync it using sync.Once instead of creating it everytime we call the function
func GetFirestoreClient() *firestore.Client {
	if firestoreClient == nil {
		firstoreClientOnce.Do(func() {
			fsClient, err := firestore.NewClient(context.Background(), "digital-assets-301018")
			if err != nil {
				log.Error("%s", err)
			}
			firestoreClient = fsClient
		})
	}

	return firestoreClient
}

func FSClose() {
	if firestoreClient != nil {
		firestoreClient.Close()
	}
}

// gets fundamentals data from firestore
func GetFundamentals(ctxO context.Context, symbol string, period string) ([]byte, error) {
	// Starts new child span from the parent span in the context.
	ctx, span := tracer.Start(ctxO, "GetFundamentalsFS", trace.WithAttributes(attribute.String("symbol", symbol), attribute.String("period", period)))
	defer span.End()

	result, err := GetFundamentalsPG(ctx, symbol)
	if err != nil {
		return nil, err
	}
	SortExchangePG(result.Exchanges)
	jsonData, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}

	span.SetStatus(otelCodes.Ok, "success")

	return jsonData, nil

}

// gets leaders and laggards data from fundamentals collection
// leaders top 7 assets by percentage change
// laggards lowest 7 assets by percentage change
func GetLeadersAndLaggards(ctx0 context.Context) ([]byte, error) {
	ctx, span := tracer.Start(ctx0, "GetLeadersAndLaggardsFS")
	defer span.End()

	var resp LeaderAndLaggards

	var LeadersResponse []LeadersAndLaggardsData
	var LaggardsResponse []LeadersAndLaggardsData

	fundamentalsData, err := GetLeadersAndLaggardsPG(ctx)

	if err != nil {

		log.Info("%s", err)
		jsonData, err := json.Marshal(resp)
		if err != nil {
			return nil, err
		}
		return jsonData, nil
	}

	//if there is no data return and throw error
	if fundamentalsData == nil || len(fundamentalsData) <= 0 {
		return nil, errors.New("fundamentals rturned empty")
	}
	// sorts fundamentals data by percentage change descending
	sort.Slice(fundamentalsData, func(i, j int) bool {
		var acc float64
		var current float64

		if fundamentalsData[i].Percentage24h == nil {
			acc = 0
		} else {
			acc = *fundamentalsData[i].Percentage24h
		}

		if fundamentalsData[j].Percentage24h == nil {
			current = 0
		} else {
			current = *fundamentalsData[j].Percentage24h
		}
		return acc > current
	})

	leaders := fundamentalsData[0:7]

	for _, item := range leaders {
		var leadersData LeadersAndLaggardsData

		leadersData.ChangeValue = item.ChangeValue24h
		leadersData.Logo = item.Logo
		leadersData.Name = item.Name
		leadersData.Percentage = item.Percentage24h
		leadersData.Price = item.Price24h
		leadersData.Slug = item.Slug
		leadersData.Symbol = item.Symbol
		leadersData.DisplaySymbol = item.DisplaySymbol

		LeadersResponse = append(LeadersResponse, leadersData)
	}

	// sorts fundamentals data by percentage change ascending
	sort.Slice(fundamentalsData, func(i, j int) bool {
		var acc float64
		var current float64

		if fundamentalsData[i].Percentage24h == nil {
			acc = 0
		} else {
			acc = *fundamentalsData[i].Percentage24h
		}

		if fundamentalsData[j].Percentage24h == nil {
			current = 0
		} else {
			current = *fundamentalsData[j].Percentage24h
		}
		return acc < current
	})

	laggards := fundamentalsData[0:7]

	for _, item := range laggards {
		var laggardsData LeadersAndLaggardsData

		laggardsData.ChangeValue = item.ChangeValue24h
		laggardsData.Logo = item.Logo
		laggardsData.Name = item.Name
		laggardsData.Percentage = item.Percentage24h
		laggardsData.Price = item.Price24h
		laggardsData.Slug = item.Slug
		laggardsData.Symbol = item.Symbol
		laggardsData.DisplaySymbol = item.DisplaySymbol

		LaggardsResponse = append(LaggardsResponse, laggardsData)
	}

	resp.Leaders = LeadersResponse
	resp.Laggards = LaggardsResponse
	resp.Source = data_source

	jsonData, err := json.Marshal(resp)
	if err != nil {
		return nil, err
	}
	return jsonData, nil
}

// gets carousel data from fundamentals collection (top 10 assets by marketcap and float type not pegged)
func GetCarouselData(ctx0 context.Context, excludedAssets []string) ([]byte, error) {

	ctx, span := tracer.Start(ctx0, "GetCarouselDataFS")
	defer span.End()

	//get the top 10 by marketcap
	//get top assets by market cap. We get top 20 since some assets returned maybe in the exlusion list.
	res, err := PGGetTradedAssets(ctx, 20, 1, "market_cap", "desc")
	if err != nil {
		return nil, err
	}

	var assetstable TradedAssetsResp
	err = json.Unmarshal(res, &assetstable)
	if err != nil {
		return nil, err
	}

	var assets []TradedAssetsTable
	for _, asset := range assetstable.Assets {

		//Only get top 10 assets
		if len(assets) < 10 {
			//if the asset is not in the exclusion list include the asset
			if !slices.Contains(excludedAssets, asset.Symbol) {
				assets = append(assets, asset)
			}
		} else {
			break
		}
	}

	assetstable.Assets = assets

	jsonData, err := json.Marshal(assetstable)
	if err != nil {
		return nil, err
	}
	return jsonData, nil

}

// gets and maps index data from fundamentals collection
// TODO: needs a rework
func MapIndexData(indexData Top30MarketCap, rebalancingTime time.Time, indexStatus string) error {
	fs := GetFirestoreClient()
	ctx := context.Background()

	collectionName := fmt.Sprintf("incices_data%s", os.Getenv("DATA_NAMESPACE"))

	weightDifference := 0.0
	indexPrice := 0.0

	var indexContent []IndexContent
	var indexRebalancingData IndexRebalancing
	indexRebalancingData.RebalanceTime = rebalancingTime

	dbSnap := fs.Collection(collectionName).Doc("da-30").Collection("tableData").Documents(ctx)

	for {
		var indexResp IndexTableResponse
		IndexDoc, err := dbSnap.Next()

		if err == iterator.Done {
			break
		}

		if err := IndexDoc.DataTo(&indexResp); err != nil {
			return err
		}
		_, deleteError := fs.Collection(collectionName).Doc("da-30").Collection("tableData").Doc(indexResp.Symbol).Delete(ctx)

		if deleteError != nil {
			log.Error("Indexing error at %s%s", rebalancingTime, deleteError)
		}
	}

	for _, item := range indexData.TopAssetsByMarketCap {
		var indexResp IndexTableResponse
		var index IndexContent

		indexResp.Name = item.Name
		indexResp.Slug = item.Slug
		indexResp.Logo = item.Logo
		indexResp.ChangeValue = item.ChangeValue24h
		indexResp.Percentage = item.Percentage24h
		indexResp.Price = item.Price24h
		indexResp.Symbol = item.Symbol
		indexResp.MarketCap = item.MarketCap
		var indexMC float64
		indexMC = *item.MarketCap
		var totalMC float64
		totalMC = *indexData.TotalMarketCap
		weight := indexMC / totalMC
		if weight > 0.3 {
			weightDifference += weight - 0.3
			weight = 0.3
		} else if weight < 0.01 {
			weight = weight + (0.01 - weight)
			weightDifference -= (0.01 - weight)
		}
		indexResp.Weight = &weight

		index.MarketCap = *item.MarketCap
		index.Symbol = item.Symbol
		index.Weight = weight

		indexContent = append(indexContent, index)

		var indexWeight float64
		indexWeight = *indexResp.Weight

		indexPrice += indexWeight * (*indexResp.Price)

		_, dbErr := fs.Collection(collectionName).Doc("da-30").Collection("tableData").Doc(indexResp.Symbol).Set(ctx, indexResp)

		if dbErr != nil {
			log.Error("Indexing error at %s%s", rebalancingTime, dbErr)
			return dbErr
		}
	}

	bqClient, err := bigquery.NewClient(ctx, BQProjectID)
	if err != nil {
		log.Error("Indexing error at %s%s", rebalancingTime, err)
		return err
	}

	indexRebalancingData.IndexContent = indexContent
	indexRebalancingData.IndexName = "da30"
	indexRebalancingData.IndexPrice = indexPrice
	if indexStatus == "autoRebalancing" {
		indexRebalancingData.IndexStatus.StringVal = "Auto Rebalance"
		indexRebalancingData.IndexStatus.Valid = true
	} else {
		indexRebalancingData.IndexStatus.StringVal = "Stress Event"
		indexRebalancingData.IndexStatus.Valid = true
	}

	IndexInserter := bqClient.Dataset("digital_assets").Table("indicesRebalancingData").Inserter()
	IndexInserter.IgnoreUnknownValues = true

	if err := IndexInserter.Put(ctx, indexRebalancingData); err != nil {
		log.Error("Indexing error at %s%s", rebalancingTime, err)
		return err
	}

	indexCollection := fmt.Sprintf("indices%s", os.Getenv("DATA_NAMESPACE"))

	_, updateErr := fs.Collection(indexCollection).Doc("da-30").Set(ctx, map[string]interface{}{
		"value": indexPrice, "change": 0.0, "percentage": 0.0}, firestore.MergeAll)
	if updateErr != nil {
		log.Error("Indexing error at %s%s", rebalancingTime, updateErr)
		return updateErr
	}

	log.Info("Indexing finished at %s", rebalancingTime)

	return nil
}

// gets index content from fundamentals collection
// TODO: needs a rework
func GetIndexContent() (Top30MarketCap, time.Time, error) {
	rebalancingTime := time.Now().UTC()
	log.Info("Indexing started at %s", rebalancingTime)
	fs := GetFirestoreClient()
	ctx := context.Background()

	fundamentalsCollectionName := fmt.Sprintf("fundamentals_24h%s", os.Getenv("DATA_NAMESPACE"))
	dbSnap := fs.Collection(fundamentalsCollectionName).Where("floatType", "not-in", []string{"pegged", ""}).Documents(ctx)

	var topAssetsByMarketCap []FundamentalsData
	totalMarketCap := 0.0

	for {
		var fundamental FundamentalsData

		fundamentalsDoc, err := dbSnap.Next()

		if err == iterator.Done {
			break
		}

		if err := fundamentalsDoc.DataTo(&fundamental); err != nil {
			log.Error("Indexing error at %s%s", rebalancingTime, err)
			return Top30MarketCap{}, rebalancingTime, err
		}

		topAssetsByMarketCap = append(topAssetsByMarketCap, fundamental)
	}

	sort.Slice(topAssetsByMarketCap, func(i, j int) bool {
		var acc float64
		var current float64

		if topAssetsByMarketCap[i].MarketCap == nil {
			acc = 0
		} else {
			acc = *topAssetsByMarketCap[i].MarketCap
		}

		if topAssetsByMarketCap[j].MarketCap == nil {
			current = 0
		} else {
			current = *topAssetsByMarketCap[j].MarketCap
		}
		return acc > current
	})

	topAssetsByMarketCap = topAssetsByMarketCap[0:30]

	if len(topAssetsByMarketCap) != 30 {
		log.Error("Indexing error at %s%s%d%s", rebalancingTime, " found ", len(topAssetsByMarketCap), " assets instead of 30")
	}

	for _, item := range topAssetsByMarketCap {
		totalMarketCap += *item.MarketCap
	}

	var top30MarketCap Top30MarketCap

	top30MarketCap.TopAssetsByMarketCap = topAssetsByMarketCap
	top30MarketCap.TotalMarketCap = &totalMarketCap
	return top30MarketCap, rebalancingTime, nil
}

// gets index table data
// TODO: needs a rework
func GetIndexData() ([]byte, error) {
	fs := GetFirestoreClient()
	ctx := context.Background()

	collectionName := fmt.Sprintf("incices_data%s", os.Getenv("DATA_NAMESPACE"))

	var index []IndexTableResponse

	it := fs.Collection(collectionName).Doc("da-30").Collection("tableData").OrderBy("marketCap", firestore.Desc).Documents(ctx)
	for {
		var indexResp IndexTableResponse
		doc, done := it.Next()
		if done == iterator.Done {
			break
		}

		if err := doc.DataTo(&indexResp); err != nil {
			return nil, err
		}
		index = append(index, indexResp)
	}

	jsonData, err := json.Marshal(index)
	if err != nil {
		return nil, err
	}

	return jsonData, nil
}

// update index value, change and percentage change
// TODO: needs a rework
func UpdateIndexContentData() error {
	fs := GetFirestoreClient()
	ctx := context.Background()

	indicesCollectionName := fmt.Sprintf("incices_data%s", os.Getenv("DATA_NAMESPACE"))

	date := time.Now().UTC()

	it := fs.Collection(indicesCollectionName).Doc("da-30").Collection("tableData").OrderBy("marketCap", firestore.Desc).Documents(ctx)

	bqs, err := NewBQStore()
	if err != nil {
		if err != nil {
			log.Error("%s", err)
			return err
		}
	}

	indexData, err := bqs.QueryRebalancedIndices()

	indexPrice := 0.0
	changeValue := 0.0
	changePercentage := 0.0

	for {
		var indexResp IndexTableResponse
		doc, done := it.Next()
		if done == iterator.Done {
			break
		}

		if err := doc.DataTo(&indexResp); err != nil {
			return err
		}

		fundamentalsCollectionName := fmt.Sprintf("fundamentals_24h%s", os.Getenv("DATA_NAMESPACE"))
		dbSnap, err := fs.Collection(fundamentalsCollectionName).Doc(indexResp.Symbol).Get(ctx)

		if status.Code(err) == codes.NotFound {
			//not found
			return nil
		}

		if err != nil {
			return err
		}

		var fundamental FundamentalsData

		if err := dbSnap.DataTo(&fundamental); err != nil {
			return err
		}

		var indexWeight float64
		indexWeight = *indexResp.Weight
		indexPrice += indexWeight * *fundamental.Price24h

		_, updateErr := fs.Collection(indicesCollectionName).Doc("da-30").Collection("tableData").Doc(indexResp.Symbol).Update(ctx, []firestore.Update{
			{
				Path:  "price",
				Value: fundamental.Price24h,
			},
			{
				Path:  "percentage",
				Value: fundamental.Percentage24h,
			},
			{
				Path:  "changeValue",
				Value: fundamental.ChangeValue24h,
			},
		})

		if updateErr != nil {
			return updateErr
		}
	}
	changeValue = indexPrice - indexData.IndexPrice
	changePercentage = (changeValue / indexData.IndexPrice)

	indexCollection := fmt.Sprintf("indices%s", os.Getenv("DATA_NAMESPACE"))

	_, updateErr := fs.Collection(indexCollection).Doc("da-30").Set(ctx, map[string]interface{}{
		"value": indexPrice, "change": changeValue, "percentage": changePercentage, "date": date}, firestore.MergeAll)
	if updateErr != nil {
		return updateErr
	}
	return nil
}

// Converting String value to float64, since the data from nomics are returning as strings
func ConvertToFloat(strValue string) *float64 {
	var float *float64

	if strValue == "" {
		float = nil
	} else {
		val, _ := strconv.ParseFloat(strValue, 64)
		float = &val
	}

	return float
}

func ConvertStringToFloat(strValue string) float64 {
	var float float64

	if strValue == "" {
		float = 0
	} else {
		val, _ := strconv.ParseFloat(strValue, 64)
		float = val
	}

	return float
}

// Converting String value to int64, since the data from nomics are returning as strings
func ConvertToInt(strValue string) *int64 {
	var intVal *int64

	if strValue == "" {
		intVal = nil
	} else {
		val, err := strconv.ParseInt(strValue, 10, 64)
		if err != nil {
			log.Error("Convert type error %s", err)
		}
		intVal = &val
	}

	return intVal
}

// Converting BigQuery Float type to float64
func ConvertBQFloatToFloat(bqVal bigquery.NullFloat64) *float64 {
	if bqVal.Valid {
		return &bqVal.Float64
	}
	return nil
}

func ConvertBQFloatToNormalFloat(bqVal bigquery.NullFloat64) float64 {
	if bqVal.Valid {
		return bqVal.Float64
	}
	return 0.0
}

func SortExchangePG(exchange []FirestoreExchange) {
	sort.Slice(exchange, func(i, j int) bool {
		return exchange[i].Nomics.VolumeByExchange1D > exchange[j].Nomics.VolumeByExchange1D
	})

}

func SaveEducationSection(ctx context.Context, sections []services.Section) {

	fs := GetFirestoreClient()

	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "education")
	for _, section := range sections {
		fs.Collection(collectionName).Doc(section.DocId).Set(ctx, map[string]interface{}{
			"name":         section.Name,
			"bertieTag":    section.BertieTag,
			"description":  section.Description,
			"sectionOrder": section.SectionOrder,
			"sectionImage": section.SectionImage,
		}, firestore.MergeAll)
		for _, article := range section.Articles {
			doc := make(map[string]interface{})
			doc["id"] = article.Id
			doc["title"] = article.Title
			doc["image"] = article.Image
			doc["articleURL"] = article.ArticleURL
			doc["author"] = article.Author
			doc["type"] = article.Type
			doc["authorType"] = article.AuthorType
			doc["authorLink"] = article.AuthorLink
			doc["description"] = article.Description
			doc["publishDate"] = article.PublishDate
			doc["disabled"] = article.Disabled
			doc["seniorContributor"] = article.SeniorContributor
			doc["bylineFormat"] = article.BylineFormat
			doc["bertieTag"] = article.BertieTag
			doc["order"] = article.Order
			doc["isFeaturedArticle"] = article.IsFeaturedArticle
			doc["lastUpdated"] = article.LastUpdated
			if article.DocId != "" {
				fs.Collection(collectionName).Doc(section.DocId).Collection("articles").Doc(article.DocId).Set(ctx, doc, firestore.MergeAll)
			} else {
				fs.Collection(collectionName).Doc(section.DocId).Collection("articles").NewDoc().Set(ctx, doc, firestore.MergeAll)
			}
		}
	}

}

// Gets All Exchange Profiles from Rowy and ruturns a map of exchange profile by exchange ID
func GetExchanges(ctxO context.Context) (map[string]model.ExchangeProfile, error) {
	ctx, span := tracer.Start(ctxO, "GetExchanges")
	defer span.End()

	db := GetFirestoreClient()

	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "exchanges")

	iter := db.Collection(collectionName).Documents(ctx)

	exchanges := make(map[string]model.ExchangeProfile)

	for {
		var exchange model.ExchangeProfile
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}
		err = doc.DataTo(&exchange)
		if err != nil {
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}
		exchanges[exchange.Name] = exchange
	}

	span.SetStatus(otelCodes.Ok, "Success")

	return exchanges, nil
}

// Get All Featured Categories from FS
func GetFeaturedCategories(ctx0 context.Context) ([]byte, error) {
	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "GetFeaturedCategories")
	defer span.End()

	var featuresCategories []FeaturedCategory

	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "categories")
	// Get Featured Categories and order it by category order
	iter := fs.Collection(collectionName).Where("isFeatured", "==", true).Where("categoryOrder", "!=", 0).OrderBy("categoryOrder", firestore.Asc).Documents(ctx)
	span.AddEvent("Start Getting Featured Categories Data from FS")

	for {
		var featuresCategory FeaturedCategory

		doc, err := iter.Next()

		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Error("Error Getting Featured Categories Data from FS: %s", err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}
		err = doc.DataTo(&featuresCategory)
		if err != nil {
			log.Error("Error Getting Featured Categories Data from FS: %s", err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}
		featuresCategories = append(featuresCategories, featuresCategory)

	}
	jsonData, err := BuildJsonResponse(ctx, featuresCategories, "Featured Categories Data")

	if err != nil {
		log.Error("Error Converting Featured Categories to Json: %s", err)
		span.SetStatus(otelCodes.Error, err.Error())
		return nil, err
	}

	span.SetStatus(otelCodes.Ok, "Success")
	return jsonData, nil
}

/*
UpdateFueaturedCategories: Takes a context and a map of featured categories.
Stores Featured Categories to firestore.
*/
func UpdateFeaturedCategories(ctx0 context.Context, categories map[string]FeaturedCategory) error {
	fs := GetFirestoreClient()

	span, labels := common.GenerateSpan("UpdateFeaturedCategories", ctx0)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "UpdateFeaturedCategories"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "UpdateFeaturedCategories"))
	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "categories")
	for key, value := range categories {

		_, err := fs.Collection(collectionName).Doc(key).Set(ctx0, value)
		if err != nil {
			log.Error("%s", err)
			return err
		}
	}

	log.EndTimeL(labels, "UpdateFeaturedCategories", startTime, nil)
	//span.SetStatus(codes.Ok, "V2 watchlistQuery.AddAssetToWatchlist")
	return nil

}

/*
GetFeaturedCategoriesMap: Calls firestore to get all featured categories, and returns them in a map.
*/
func GetFeaturedCategoriesMap(ctx0 context.Context) (map[string]FeaturedCategory, error) {
	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "GetFeaturedCategoriesMap")
	defer span.End()

	var featuresCategories = make(map[string]FeaturedCategory)

	// We don't need to build these categories because this categories are global so the link for these categories shouldn't change
	// So we must excluded from the build
	excludeCategories := []string{"all-categories", "all-crypto-currencies"}

	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "categories")
	// Get Featured Categories and order it by category order
	iter := fs.Collection(collectionName).Documents(ctx)
	span.AddEvent("Start Getting Featured Categories Map Data from FS")

	for {
		var featuresCategory FeaturedCategory

		doc, err := iter.Next()

		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Error("Error Getting Featured Categories Map Data from FS: %s", err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}
		err = doc.DataTo(&featuresCategory)
		if err != nil {
			log.Error("Error Getting Featured Categories Map Data from FS: %s", err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}
		// We can considered inactive categories as basically deleted categories. They're in the DB, only because we're soft deleting it.
		if !slices.Contains(excludeCategories, featuresCategory.ID) && !featuresCategory.Inactive {
			featuresCategories[featuresCategory.ID] = featuresCategory
		}

	}

	span.SetStatus(otelCodes.Ok, "Success")
	return featuresCategories, nil
}

// Get Today's highlights according to today's date. Used in the Tip of the day section of the new Landing page.
func GetTodayHighlights(ctx0 context.Context) (*model.TipOfTheDay, error) {
	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "GetTodayHighlights")
	defer span.End()

	article := model.TipOfTheDay{}

	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "today_highlights")
	_, _, date := time.Now().Date()

	// Get today's highlight according to what day of the month it is today. It is guaranteed that there will be 31 highlights in the collection.
	iter := fs.Collection(collectionName).Where("order", "==", date).Documents(ctx)
	span.AddEvent("Start Getting Today's highlight Data from FS")

	doc, err := iter.Next()

	if err == iterator.Done {
		// No article found for today's date!
		log.Error("No Tip of the day found for today's date in FS: %s", err)
	} else if err != nil {
		log.Error("Error Getting Today's highlight Data from FS: %s", err)
		span.SetStatus(otelCodes.Error, err.Error())
		return nil, err
	} else { // No error
		err = doc.DataTo(&article)
		if err != nil {
			log.Error("Error Assigning Today's highlight Data to Article from FS: %s", err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}
	}

	span.SetStatus(otelCodes.Ok, "Success")
	return &article, nil
}

// Get All Token's metadata description from Rowy for all tokens.
func GetAllForbesTokenMetadata(ctx0 context.Context) (*[]model.ForbesMetadata, error) {
	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "GetAllForbesTokenMetadata")
	defer span.End()

	tokens := []model.ForbesMetadata{}

	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "forbes_token_metadata")

	iter := fs.Collection(collectionName).Documents(ctx)
	span.AddEvent("Start Getting all token metadata from FS")

	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			log.Error("Error Getting all token metadata from FS: %s", err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		} else { // No error
			token := model.ForbesMetadata{}
			err = doc.DataTo(&token)
			if err != nil {
				log.Error("Error Assigning all token metadata to Token from FS: %s", err)
				span.SetStatus(otelCodes.Error, err.Error())
				return nil, err
			}
			token.DocId = doc.Ref.ID
			tokens = append(tokens, token)
		}
	}
	span.SetStatus(otelCodes.Ok, "Success")
	return &tokens, nil
}

// Get Token's metadata description from Rowy for the mentioned asset id
func GetForbesTokenMetadata(ctx0 context.Context, assetId string) (*model.ForbesMetadata, error) {
	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "GetForbesTokenMetadata")
	defer span.End()

	token := model.ForbesMetadata{}

	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "forbes_token_metadata")

	iter := fs.Collection(collectionName).Where("assetId", "==", assetId).Documents(ctx)
	span.AddEvent("Start Getting token metadata from FS")

	doc, err := iter.Next()
	if err == iterator.Done {
		log.Error("No token metadata found for assetId %s from FS! %s", assetId, err)
		return nil, nil
	} else if err != nil {
		log.Error("Error Getting token metadata for assetId %s from FS: %s", assetId, err)
		span.SetStatus(otelCodes.Error, err.Error())
		return nil, err
	} else { // No error
		err = doc.DataTo(&token)
		if err != nil {
			log.Error("Error Assigning token metadata to Token %s from FS: %s", assetId, err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}
		token.DocId = doc.Ref.ID
	}
	span.SetStatus(otelCodes.Ok, "Success")
	return &token, nil
}

// Create or update the token's metadata description in Rowy.
func UpsertForbesTokenMetadata(ctx0 context.Context, fsToken model.ForbesMetadata) error {
	ctx, span := tracer.Start(ctx0, "UpsertForbesTokenMetadata")
	defer span.End()

	fs := GetFirestoreClient()
	var err error

	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "forbes_token_metadata")

	doc := make(map[string]interface{})
	doc["assetId"] = fsToken.AssetId
	doc["symbol"] = fsToken.Symbol
	doc["metadataDescription"] = fsToken.MetadataDescription
	if fsToken.DocId != "" { //only upsert the doc
		_, err = fs.Collection(collectionName).Doc(fsToken.DocId).Set(ctx, doc, firestore.MergeAll)
	} else {
		_, err = fs.Collection(collectionName).NewDoc().Set(ctx, doc, firestore.MergeAll)
	}

	if err != nil {
		log.Error("Error upserting token metadata : %s", err.Error())
		span.SetStatus(otelCodes.Error, err.Error())
	}
	return err
}

// It will return the json response for any interface you need to convert
func BuildJsonResponse(ctx0 context.Context, data interface{}, message string) ([]byte, error) {
	_, span := tracer.Start(ctx0, message)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Start Converting %s", message))
	jsonData, err := json.Marshal(data)

	if err != nil {
		log.Error("Error Converting %s to Json Response: %s", message, err)
		span.SetStatus(otelCodes.Error, err.Error())
		return nil, err
	}

	span.SetStatus(otelCodes.Ok, "Success")
	return jsonData, nil
}

// Get All List Section from FS
func GetListsSection(ctx0 context.Context) ([]byte, error) {
	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "GetListsSection")
	defer span.End()

	var listSection ListSection

	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "lists_section")
	// Get the Global Description and the Lists Section from firestore
	iter := fs.Collection(collectionName).Documents(ctx)
	span.AddEvent("Start Getting Lists Section Data from FS")

	for {
		var lists []CryptoListSection
		doc, err := iter.Next()

		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Error("Error Getting Lists Section Data from FS: %s", err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}
		err = doc.DataTo(&listSection)
		if err != nil {
			log.Error("Error Getting Lists Section Data from FS: %s", err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}
		// Get the Lists Section
		dbSnap := fs.Collection(collectionName).Doc("sections").Collection("lists").Documents(ctx)

		for {

			var list CryptoListSection
			doc, err := dbSnap.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				log.Error("Error Getting Lists Section Data from FS: %s", err)
				span.SetStatus(otelCodes.Error, err.Error())
				return nil, err
			}
			err = doc.DataTo(&list)
			if err != nil {
				log.Error("Error Getting Lists Section Data from FS: %s", err)
				span.SetStatus(otelCodes.Error, err.Error())
				return nil, err
			}

			lists = append(lists, list)

		}
		listSection.Lists = lists

	}
	jsonData, err := BuildJsonResponse(ctx, listSection, "Lists Section Data")

	if err != nil {
		log.Error("Error Converting Lists Section to Json: %s", err)
		span.SetStatus(otelCodes.Error, err.Error())
		return nil, err
	}

	span.SetStatus(otelCodes.Ok, "Success")
	return jsonData, nil
}

// Get Top the 12 most recent tweets from firestore
func GetRecentTweets(ctx0 context.Context) ([]byte, error) {
	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "GetRecentTweets")
	defer span.End()

	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "twitter_ids")
	// Get the Global Description and the Lists Section from firestore
	iter := fs.Collection(collectionName).OrderBy("orderBy", firestore.Desc).Limit(12).Documents(ctx)
	span.AddEvent("Start Getting Tweets from FS")
	var (
		tweets []TwitterId
		tweet  TwitterId
	)
	for {

		doc, err := iter.Next()

		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Error("Error Getting Tweets from FS: %s", err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}
		err = doc.DataTo(&tweet)
		if err != nil {
			log.Error("Error Getting Tweets from FS: %s", err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}

		tweets = append(tweets, tweet)

	}
	jsonData, err := json.Marshal(tweets)

	if err != nil {
		log.Error("Error Converting Tweets to Json: %s", err)
		span.SetStatus(otelCodes.Error, err.Error())
		return nil, err
	}

	span.SetStatus(otelCodes.Ok, "Success")
	return jsonData, nil

}

// Get NFT chains List from FS
func GetChainsList(ctx0 context.Context) ([]byte, error) {
	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "GetNFTChains")
	defer span.End()

	var nftChains []NFTChain

	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "nft_chains")
	// Get the NFT chains from Firestore
	iter := fs.Collection(collectionName).Documents(ctx)
	span.AddEvent("Start Getting NFT Chains Data from FS")

	for {
		var nftChain NFTChain
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			log.Error("Error Getting NFT Chains Data from FS: %s", err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}

		err = doc.DataTo(&nftChain)
		if err != nil {
			log.Error("Error Getting NFT Chains Data from FS: %s", err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}
		nftChains = append(nftChains, nftChain)
	}

	jsonData, err := BuildJsonResponse(ctx, nftChains, "NFT Chains Data")

	if err != nil {
		log.Error("Error Converting NFT Chains to Json: %s", err)
		span.SetStatus(otelCodes.Error, err.Error())
		return nil, err
	}

	span.SetStatus(otelCodes.Ok, "Success")
	return jsonData, nil
}

// Get all trending topics from Firestore
func GetTopicsTagsList(ctx0 context.Context) ([]byte, error) {
	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "GetTopicsTagsList")
	defer span.End()

	var topicsTags []services.TrendingTopics

	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "news")
	// Get all Topics from Firestore
	iter := fs.Collection(collectionName).Where("isTrending", "==", true).Documents(ctx)
	span.AddEvent("Start Getting Topics Tags List from FS")

	for {
		var topicsTag services.TrendingTopics
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			log.Error("Error Getting Topics Tags List from FS: %s", err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}

		err = doc.DataTo(&topicsTag)
		if err != nil {
			log.Error("Error Getting Topics Tags Lista from FS: %s", err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}
		topicsTags = append(topicsTags, topicsTag)
	}

	// shuffle the TrendingTopics Array
	rand.Shuffle(len(topicsTags), func(i, j int) { topicsTags[i], topicsTags[j] = topicsTags[j], topicsTags[i] })

	jsonData, err := BuildJsonResponse(ctx, topicsTags, "Topics Tags List")

	if err != nil {
		log.Error("Error Converting Topics Tags List to Json: %s", err)
		span.SetStatus(otelCodes.Error, err.Error())
		return nil, err
	}

	span.SetStatus(otelCodes.Ok, "Success")
	return jsonData, nil
}

// Add topics with all its data to FS
func SaveNewsTopics(ctx0 context.Context, topics []services.Topic) {

	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "SaveNewsTopics")
	defer span.End()
	span.AddEvent("Start Insert Topics To FS")
	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "news")
	for index, topic := range topics {
		slug := topic.Slug
		isAsset := false
		topicUrl := fmt.Sprintf("/news/%s", slug)
		fund, err := CheckTopicAssets(ctx, topic.AliasesName)
		if err != nil {
			isAsset = false
		}
		if fund.Symbol != "" {
			isAsset = true
			slug = fund.Slug
			topicUrl = fmt.Sprintf("/assets/%s", slug)
		}
		fs.Collection(collectionName).Doc(topic.TopicName).Set(ctx, map[string]interface{}{
			"topicName":            topic.TopicName,
			"bertieTag":            topic.BertieTag,
			"topicUrl":             topicUrl,
			"topicOrder":           index + 1,
			"description":          topic.Description,
			"isTrending":           topic.IsTrending,
			"isAsset":              isAsset,
			"isFeaturedHome":       topic.IsFeaturedHome,
			"titleTemplate":        topic.TitleTemplate,
			"slug":                 slug,
			"topicPageDescription": topic.TopicPageDescription,
			"newsHeader":           topic.NewsHeader,
			"aliasesName":          topic.AliasesName,
		}, firestore.MergeAll)
		for _, article := range topic.Articles {
			doc := make(map[string]interface{})
			doc["id"] = article.Id
			doc["title"] = article.Title
			doc["image"] = article.Image
			doc["articleURL"] = article.ArticleURL
			doc["author"] = article.Author
			doc["type"] = article.Type
			doc["authorType"] = article.AuthorType
			doc["authorLink"] = article.AuthorLink
			doc["description"] = article.Description
			doc["publishDate"] = article.PublishDate
			doc["disabled"] = article.Disabled
			doc["seniorContributor"] = article.SeniorContributor
			doc["bylineFormat"] = article.BylineFormat
			doc["bertieTag"] = article.BertieTag
			doc["order"] = article.Order
			doc["isFeaturedArticle"] = article.IsFeaturedArticle
			doc["lastUpdated"] = article.LastUpdated
			doc["naturalid"] = article.NaturalID
			//if there is no natural id dont store the article
			if article.NaturalID != "" {
				fs.Collection(collectionName).Doc(topic.TopicName).Collection("articles").Doc(strings.ReplaceAll(article.NaturalID, "/", "_")).Set(ctx, doc, firestore.MergeAll)
			}
		}
		err = removeArticlesWithOutNaturalID(ctx, collectionName, topic.TopicName)
		if err != nil {
			log.Error("Error Getting Article Data from FS: %s", err)
			span.AddEvent(fmt.Sprintf("Error Getting Article Data from FS: %s", err))
			span.SetStatus(otelCodes.Error, err.Error())
		}
	}
	span.SetStatus(otelCodes.Ok, "Success")

}

// This function is to remove all articles without a natural id. This is beacuse we can not match them correctly to incoming articles. The natural id is the primary key
func removeArticlesWithOutNaturalID(ctx0 context.Context, collectionName string, topicName string) error {

	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "GetNewsTopics")
	defer span.End()

	// get topic data using slug

	//get topic articles
	db := fs.Collection(collectionName).Doc(topicName).Collection("articles").Documents(ctx)

	for {
		var article services.EducationArticle
		doc, err := db.Next()

		if err == iterator.Done {
			break
		}

		if err := doc.DataTo(&article); err != nil {
			log.Error("Error Getting Article Data from FS: %s", err)
			span.AddEvent(fmt.Sprintf("Error Getting Article Data from FS: %s", err))
			span.SetStatus(otelCodes.Error, err.Error())
			return err
		}
		//if the article does not have a natural id delete it
		if article.NaturalID == "" {
			fs.Collection(collectionName).Doc(topicName).Collection("articles").Doc(doc.Ref.ID).Delete(ctx)
		}
	}

	span.AddEvent("Modify Articles to be only 8 Articles for each Topic")
	span.SetStatus(otelCodes.Ok, "Success")
	return nil

}

// Update trending tags for topics from 24 hours.
func UpdateIsTrendingTopics(ctx0 context.Context, topics []services.Topic, oldTopics []services.Topic) {

	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "SaveNewsTopics")
	defer span.End()

	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "news")
	span.AddEvent("Start Update old Trending as not Trending")
	for _, topic := range oldTopics {
		fs.Collection(collectionName).Doc(topic.TopicName).Set(ctx, map[string]interface{}{
			"isTrending": false,
		}, firestore.MergeAll)
	}

	span.AddEvent("Start Update not Trending as new Trending")
	for _, topic := range topics {
		fs.Collection(collectionName).Doc(topic.TopicName).Set(ctx, map[string]interface{}{
			"isTrending": true,
		}, firestore.MergeAll)
	}

	span.SetStatus(otelCodes.Ok, "Success")

}

// Add topics with all its data to FS
func SaveFeaturedArticle(ctx0 context.Context, articleDetails ArticleContentResult, collectionName string, docID string) {

	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "SaveNewsTopics")
	defer span.End()
	span.AddEvent("Start Insert Topics To FS")
	fs.Collection(collectionName).Doc(docID).Set(ctx, map[string]interface{}{
		"title":       articleDetails.ArticleDetails.Title,
		"image":       articleDetails.ArticleDetails.Image,
		"url":         articleDetails.ArticleDetails.Url,
		"description": articleDetails.ArticleDetails.Description,
		"authors":     articleDetails.AuthorDetails,
	}, firestore.MergeAll)

	span.SetStatus(otelCodes.Ok, "Success")

}

// Add Category with all its data to FS
func SaveNewsTopicsCategories(ctx0 context.Context, topics []services.TopicCategories) {

	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "SaveNewsTopics")
	defer span.End()
	span.AddEvent("Start Insert Topics To FS")
	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "category_news")
	for _, topic := range topics {
		fs.Collection(collectionName).Doc(topic.CategoryName).Set(ctx, map[string]interface{}{
			"categoryName": topic.CategoryName,
		}, firestore.MergeAll)
		for _, content := range topic.CategoryTopics {
			doc := make(map[string]interface{})
			doc["topicName"] = content.TopicName
			doc["topicUrl"] = content.TopicURL
			doc["isAsset"] = content.IsAsset
			doc["slug"] = content.Slug
			fs.Collection(collectionName).Doc(topic.CategoryName).Collection("topics").Doc(content.DocId).Set(ctx, doc, firestore.MergeAll)
		}
	}
	span.SetStatus(otelCodes.Ok, "Success")

}

type Announcements struct {
	Description          string                 `json:"description" firestore:"description"` // This will present the Announcement Global Description
	AnnouncementsDetails []AnnouncementsDetails `json:"lists" firestore:"lists"`             // This will present the list of Announcements Details
}

type AnnouncementsDetails struct {
	PublishedDate time.Time `json:"publishedDate,omitempty" firestore:"publishedDate"` // It will present the Published Date for the announcement
	Header        string    `json:"header,omitempty" firestore:"header"`               // It will present the Announcement Headline
	Description   string    `json:"description,omitempty" firestore:"description"`     // It will present the Announcement Description
	Link          string    `json:"link,omitempty" firestore:"link"`                   // It will present the external link
}

// Get Community Page Announcements Data from FS
func GetCommunityPageAnnouncements(ctx0 context.Context) ([]byte, error) {
	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "GetCommunityPageAnnouncements")

	defer span.End()
	span.AddEvent("Get Community Page Announcements from FS")

	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "announcements")

	db := fs.Collection(collectionName).Documents(ctx)

	var announcements Announcements

	for {
		var announcementsDetails []AnnouncementsDetails
		doc, err := db.Next()

		if err == iterator.Done {
			break
		}
		if err := doc.DataTo(&announcements); err != nil {
			log.Error("Error Community Page Announcements Data from FS: %s", err)
			span.AddEvent(fmt.Sprintf("Error Community Page Announcements Data from FS: %s", err))
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}
		// Get Announcements Details data from FS Collection
		subCollection := fs.Collection(collectionName).Doc("announcement").Collection("lists").Documents(ctx)

		for {
			var announcementsDetail AnnouncementsDetails
			do, err := subCollection.Next()

			if err == iterator.Done {
				break
			}

			if err := do.DataTo(&announcementsDetail); err != nil {
				log.Error("Error Community Page Announcements Data from FS: %s", err)
				span.AddEvent(fmt.Sprintf("Error Community Page Announcements Data from FS: %s", err))
				span.SetStatus(otelCodes.Error, err.Error())
				return nil, err
			}
			announcementsDetails = append(announcementsDetails, announcementsDetail)
		}
		announcements.AnnouncementsDetails = announcementsDetails
	}
	SortAnnouncements(announcements)
	result, err := BuildJsonResponse(ctx, announcements, "Community Page Announcements Data")
	if err != nil {
		log.Error("Error Community Page Announcements Data to Json: %s", err)
		span.SetStatus(otelCodes.Error, err.Error())
		return nil, err
	}

	return result, nil
}

/*
- Sort Announcements Order the Announcements by publishedDate for Announcements
- Takes (announcements Announcements)
- returns Announcements data sorted by latest publishedDate
*/
func SortAnnouncements(announcements Announcements) {
	sort.Slice(announcements.AnnouncementsDetails, func(i, j int) bool {
		return announcements.AnnouncementsDetails[i].PublishedDate.After(announcements.AnnouncementsDetails[j].PublishedDate)
	})
}

// Add Premium Articles with all its data to FS
func SaveRecommendedPremiumArticles(ctx0 context.Context, articles []services.EducationArticle) {

	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "SaveRecommendedPremiumArticles")
	defer span.End()
	span.AddEvent("Start Insert Topics To FS")
	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "premium_articles")
	for _, article := range articles {
		doc := make(map[string]interface{})
		doc["id"] = article.Id
		doc["title"] = article.Title
		doc["image"] = article.Image
		doc["articleURL"] = article.ArticleURL
		doc["author"] = article.Author
		doc["type"] = article.Type
		doc["authorType"] = article.AuthorType
		doc["authorLink"] = article.AuthorLink
		doc["description"] = article.Description
		doc["publishDate"] = article.PublishDate
		doc["disabled"] = article.Disabled
		doc["seniorContributor"] = article.SeniorContributor
		doc["bylineFormat"] = article.BylineFormat
		doc["bertieTag"] = article.BertieTag
		doc["bertieBadges"] = article.BertieBadges
		doc["order"] = article.Order
		doc["isFeaturedArticle"] = article.IsFeaturedArticle
		doc["lastUpdated"] = article.LastUpdated
		doc["naturalid"] = article.NaturalID
		//if there is no natural id don't store the article
		if article.NaturalID != "" {
			fs.Collection(collectionName).Doc(strings.ReplaceAll(article.NaturalID, "/", "_")).Set(ctx, doc, firestore.MergeAll)
		}
	}
	span.SetStatus(otelCodes.Ok, "Success")

}

type FAQ struct {
	Question string `json:"question" firestore:"question"` // It will present the Question for FAQ
	Answer   string `json:"answer" firestore:"answer"`     // It will present the Answer for FAQ Question
}

// Get Community Page FAQ Data from FS
func GetCommunityPageFrequentlyAskedQuestions(ctx0 context.Context) ([]byte, error) {
	fs := GetFirestoreClient()

	ctx, span := tracer.Start(ctx0, "GetCommunityPageFrequentlyAskedQuestions")

	defer span.End()

	span.AddEvent("Start Getting Frequently Asked Questions")

	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "community_benefits_faq_section")

	db := fs.Collection(collectionName).Documents(ctx)
	var faqs []FAQ
	for {
		var faq FAQ

		doc, err := db.Next()
		if err == iterator.Done {
			break
		}

		// Map the data from FS to FAQ struct
		if err := doc.DataTo(&faq); err != nil {
			log.Error("Error Community Page Frequently Asked Questions Data from FS: %s", err)
			span.AddEvent(fmt.Sprintf("Error Community Page Frequently Asked Questions Data from FS: %s", err))
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}
		faqs = append(faqs, faq)
	}
	// Build json response from FAQ to be returned
	result, err := BuildJsonResponse(ctx, faqs, "Community Page Frequently Asked Questions Data")
	if err != nil {
		log.Error("Error Community Page Frequently Asked Questions Data to Json: %s", err)
		span.SetStatus(otelCodes.Error, err.Error())
		return nil, err
	}

	return result, nil
}

func GetNFTsQuestionTemplates(ctx0 context.Context) ([]FSNFTQuestion, error) {
	fs := GetFirestoreClient()

	ctx, span := tracer.Start(ctx0, "GetNFTsQuestionTemplates")

	defer span.End()

	span.AddEvent("Start Getting Frequently Asked Questions Template")
	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "nfts_faq_questions_templates")

	db := fs.Collection(collectionName).OrderBy("questionOrder", firestore.Asc).Documents(ctx)
	var nftsQuestions []FSNFTQuestion
	for {
		var nftsQuestion FSNFTQuestion

		doc, err := db.Next()
		if err == iterator.Done {
			break
		}

		// Map the data from FS to FAQ struct
		if err := doc.DataTo(&nftsQuestion); err != nil {
			log.Error("Error Community Page Frequently Asked Questions Template Data from FS: %s", err)
			span.AddEvent(fmt.Sprintf("Error Community Page Frequently Asked Questions Template Data from FS: %s", err))
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}
		nftsQuestions = append(nftsQuestions, nftsQuestion)
	}

	span.SetStatus(otelCodes.Ok, "Success")

	return nftsQuestions, nil
}
