package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"os"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/Forbes-Media/forbes-digital-assets/model"
	"github.com/Forbes-Media/forbes-digital-assets/services"
	"github.com/Forbes-Media/go-tools/log"
	"github.com/lib/pq"
	"go.nhat.io/otelsql"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	otelCodes "go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
)

var (
	pg           *sql.DB
	DBClientOnce sync.Once
	data_source  = os.Getenv("DATASOURCE")
)

type FundamentalsChartEntry struct {
	Time  time.Time `json:"Time"`
	Price *float64  `json:"Price"`
}

// Fundamentals_NFT_Sales_Data contains all information in regard to NFT Sales
// This data is used to get sales history from bigquery
// and is upserted to the NFTDatalatest table in postgres.
type FundamentalsNFTSalesData struct {
	ID                       string               `postgres:"id" json:"id,omitempty" bigquery:"id"`
	AVGFloorPrice1d          bigquery.NullFloat64 `postgres:"avg_floor_price_1d" json:"avg_floor_price_1d,omitempty" bigquery:"avg_floor_price_1d"`
	AVGSalePrice1d           bigquery.NullFloat64 `postgres:"avg_sale_price_1d" json:"avg_sale_price_1d,omitempty" bigquery:"avg_sale_price_1d"`
	TotalSales1d             bigquery.NullFloat64 `postgres:"total_sales_1d" json:"total_sales_1d,omitempty" bigquery:"total_sales_1d,nullable"`
	AvgTotalSalesPctChange1d bigquery.NullFloat64 `postgres:"avg_total_sales_pct_change_1d" json:"avg_total_sales_pct_change_1d,omitempty" bigquery:"avg_total_sales_pct_change_1d"`
	AVGSalesPriceChange1d    bigquery.NullFloat64 `postgres:"avg_sales_price_change_1d" json:"avg_sales_price_change_1d,omitempty" bigquery:"avg_sales_price_change_1d"`

	AVGFloorPrice7d          bigquery.NullFloat64 `postgres:"avg_floor_price_7d" json:"avg_floor_price_7d,omitempty" bigquery:"avg_floor_price_7d"`
	AVGSalePrice7d           bigquery.NullFloat64 `postgres:"avg_sale_price_7d" json:"avg_sale_price_7d,omitempty" bigquery:"avg_sale_price_7d"`
	TotalSales7d             bigquery.NullFloat64 `postgres:"total_sales_7d" json:"total_sales_7d,omitempty" bigquery:"total_sales_7d,nullable"`
	AvgTotalSalesPctChange7d bigquery.NullFloat64 `postgres:"avg_total_sales_pct_change_7d" json:"avg_total_sales_pct_change_7d,omitempty" bigquery:"avg_total_sales_pct_change_7d"`
	AVGSalesPriceChange7d    bigquery.NullFloat64 `postgres:"avg_sales_price_change_7d" json:"avg_sales_price_change_7d,omitempty" bigquery:"avg_sales_price_change_7d"`

	AVGFloorPrice30d          bigquery.NullFloat64 `postgres:"avg_floor_price_30d" json:"avg_floor_price_30d,omitempty" bigquery:"avg_floor_price_30d"`
	AVGSalePrice30d           bigquery.NullFloat64 `postgres:"avg_sale_price_30d" json:"avg_sale_price_30d,omitempty" bigquery:"avg_sale_price_30d"`
	TotalSales30d             bigquery.NullFloat64 `postgres:"total_sales_30d" json:"total_sales_30d,omitempty" bigquery:"total_sales_30d,nullable"`
	AvgTotalSalesPctChange30d bigquery.NullFloat64 `postgres:"avg_total_sales_pct_change_30d" json:"avg_total_sales_pct_change_30d,omitempty" bigquery:"avg_total_sales_pct_change_30d"`
	AVGSalesPriceChange30d    bigquery.NullFloat64 `postgres:"avg_sales_price_change_30d" json:"avg_sales_price_change_30d,omitempty" bigquery:"avg_sales_price_change_30d"`

	AVGFloorPrice90d          bigquery.NullFloat64 `postgres:"avg_floor_price_90d" json:"avg_floor_price_90d,omitempty" bigquery:"avg_floor_price_90d"`
	AVGSalePrice90d           bigquery.NullFloat64 `postgres:"avg_sale_price_90d" json:"avg_sale_price_90d,omitempty" bigquery:"avg_sale_price_90d"`
	TotalSales90d             bigquery.NullFloat64 `postgres:"total_sales_90d" json:"total_sales_90d,omitempty" bigquery:"total_sales_90d,nullable"`
	AvgTotalSalesPctChange90d bigquery.NullFloat64 `postgres:"avg_total_sales_pct_change_90d" json:"avg_total_sales_pct_change_90d,omitempty" bigquery:"avg_total_sales_pct_change_90d"`
	AVGSalesPriceChange90d    bigquery.NullFloat64 `postgres:"avg_sales_price_change_90d" json:"avg_sales_price_change_90d,omitempty" bigquery:"avg_sales_price_change_90d"`

	AVGFloorPriceYtd          bigquery.NullFloat64 `postgres:"avg_floor_price_ytd" json:"avg_floor_price_ytd,omitempty" bigquery:"avg_floor_price_ytd"`
	AVGSalePriceYtd           bigquery.NullFloat64 `postgres:"avg_sale_price_ytd" json:"avg_sale_price_ytd,omitempty" bigquery:"avg_sale_price_ytd"`
	TotalSalesYtd             bigquery.NullFloat64 `postgres:"total_sales_ytd" json:"total_sales_ytd,omitempty" bigquery:"total_sales_ytd,nullable"`
	AvgTotalSalesPctChangeYtd bigquery.NullFloat64 `postgres:"avg_total_sales_pct_change_ytd" json:"avg_total_sales_pct_change_ytd,omitempty" bigquery:"avg_total_sales_pct_change_ytd"`
	AVGSalesPriceChangeYtd    bigquery.NullFloat64 `postgres:"avg_sales_price_change_ytd" json:"avg_sales_price_change_ytd,omitempty" bigquery:"avg_sales_price_change_ytd"`
}

type PGFundamentalsResult struct {
	Symbol                    string               `postgres:"symbol" json:"symbol,omitempty" bigquery:"symbol"`
	ForbesSymbol              string               `postgres:"forbes" json:"forbesSymbol,omitempty" bigquery:"forbes"`
	Volume24H                 bigquery.NullFloat64 `postgres:"volume_24h" json:"volume24h,omitempty" bigquery:"volume_24h"`
	High                      bigquery.NullFloat64 `postgres:"high_24h" json:"high24h,omitempty" bigquery:"high_24h,nullable"`
	Low                       bigquery.NullFloat64 `postgres:"low_24h" json:"low24h,omitempty" bigquery:"low_24h"`
	High1H                    bigquery.NullFloat64 `postgres:"high_1h" json:"high1h,omitempty" bigquery:"high_1h"`
	Low1H                     bigquery.NullFloat64 `postgres:"low_1h" json:"low1h,omitempty" bigquery:"low_1h"`
	High7D                    bigquery.NullFloat64 `postgres:"high_7d" json:"high7d,omitempty" bigquery:"high_7d"`
	Low7D                     bigquery.NullFloat64 `postgres:"low_7d" json:"low7d,omitempty" bigquery:"low_7d"`
	High30D                   bigquery.NullFloat64 `postgres:"high_30d" json:"high30d,omitempty" bigquery:"high_30d"`
	Low30D                    bigquery.NullFloat64 `postgres:"low_30d" json:"low30d,omitempty" bigquery:"low_30d"`
	High1Y                    bigquery.NullFloat64 `postgres:"high_1y" json:"high1y,omitempty" bigquery:"high_1y"`
	Low1Y                     bigquery.NullFloat64 `postgres:"low_1y" json:"low1y,omitempty" bigquery:"low_1y"`
	HighYtd                   bigquery.NullFloat64 `postgres:"high_ytd" json:"highYtd,omitempty" bigquery:"high_ytd"`
	LowYtd                    bigquery.NullFloat64 `postgres:"low_ytd" json:"lowYtd,omitempty" bigquery:"low_ytd"`
	AllTimeLow                bigquery.NullFloat64 `postgres:"all_time_low" json:"allTimeLow,omitempty" bigquery:"all_time_low"`
	AllTimeHigh               bigquery.NullFloat64 `postgres:"all_time_high" json:"allTimeHigh,omitempty" bigquery:"all_time_high"`
	LastClosePrice            bigquery.NullFloat64 `postgres:"last_close_price" json:"lastClosePrice,omitempty" bigquery:"last_close_price"`
	FirstOpenPrice            bigquery.NullFloat64 `postgres:"first_open_price" json:"firstOpenPrice,omitempty" bigquery:"first_open_price"`
	MarketCap                 string               `postgres:"market_cap" json:"marketCap,omitempty" bigquery:"market_cap"`
	MarketCapOpen1H           bigquery.NullFloat64 `postgres:"market_cap_open_1h" json:"market_cap_open_1h,omitempty" bigquery:"market_cap_open_1h"`
	MarketCapOpen24H          bigquery.NullFloat64 `postgres:"market_cap_open_24h" json:"market_cap_open_24h,omitempty" bigquery:"market_cap_open_24h"`
	MarketCapOpen7D           bigquery.NullFloat64 `postgres:"market_cap_open_7d" json:"market_cap_open_7d,omitempty" bigquery:"market_cap_open_7d"`
	MarketCapOpen30D          bigquery.NullFloat64 `postgres:"market_cap_open_30d" json:"market_cap_open_30d,omitempty" bigquery:"market_cap_open_30d"`
	MarketCapOpen1Y           bigquery.NullFloat64 `postgres:"market_cap_open_1y" json:"market_cap_open_1y,omitempty" bigquery:"market_cap_open_1y"`
	MarketCapOpenYTD          bigquery.NullFloat64 `postgres:"market_cap_open_ytd" json:"market_cap_open_ytd,omitempty" bigquery:"market_cap_open_ytd"`
	MarketCapClose1H          bigquery.NullFloat64 `postgres:"market_cap_close_1h" json:"market_cap_close_1h,omitempty" bigquery:"market_cap_close_1h"`
	MarketCapClose24H         bigquery.NullFloat64 `postgres:"market_cap_close_24h" json:"market_cap_close_24h,omitempty" bigquery:"market_cap_close_24h"`
	MarketCapClose7D          bigquery.NullFloat64 `postgres:"market_cap_close_7d" json:"market_cap_close_7d,omitempty" bigquery:"market_cap_close_7d"`
	MarketCapClose30D         bigquery.NullFloat64 `postgres:"market_cap_close_30d" json:"market_cap_close_30d,omitempty" bigquery:"market_cap_close_30d"`
	MarketCapClose1Y          bigquery.NullFloat64 `postgres:"market_cap_close_1y" json:"market_cap_close_1y,omitempty" bigquery:"market_cap_close_1y"`
	MarketCapCloseYTD         bigquery.NullFloat64 `postgres:"market_cap_close_ytd" json:"market_cap_close_ytd,omitempty" bigquery:"market_cap_close_ytd"`
	VolumeOpen1H              bigquery.NullFloat64 `postgres:"volume_open_1h" json:"volume_open_1h,omitempty" bigquery:"volume_open_1h"`
	VolumeOpen24H             bigquery.NullFloat64 `postgres:"volume_open_24h" json:"volume_open_24h,omitempty" bigquery:"volume_open_24h"`
	VolumeOpen7D              bigquery.NullFloat64 `postgres:"volume_open_7d" json:"volume_open_7d,omitempty" bigquery:"volume_open_7d"`
	VolumeOpen30D             bigquery.NullFloat64 `postgres:"volume_open_30d" json:"volume_open_30d,omitempty" bigquery:"volume_open_30d"`
	VolumeOpen1Y              bigquery.NullFloat64 `postgres:"volume_open_1y" json:"volume_open_1y,omitempty" bigquery:"volume_open_1y"`
	VolumeOpenYTD             bigquery.NullFloat64 `postgres:"volume_open_ytd" json:"volume_open_ytd,omitempty" bigquery:"volume_open_ytd"`
	VolumeClose1H             bigquery.NullFloat64 `postgres:"volume_close_1h" json:"volume_close_1h,omitempty" bigquery:"volume_close_1h"`
	VolumeClose24H            bigquery.NullFloat64 `postgres:"volume_close_24h" json:"volume_close_24h,omitempty" bigquery:"volume_close_24h"`
	VolumeClose7D             bigquery.NullFloat64 `postgres:"volume_close_7d" json:"volume_close_7d,omitempty" bigquery:"volume_close_7d"`
	VolumeClose30D            bigquery.NullFloat64 `postgres:"volume_close_30d" json:"volume_close_30d,omitempty" bigquery:"volume_close_30d"`
	VolumeClose1Y             bigquery.NullFloat64 `postgres:"volume_close_1y" json:"volume_close_1y,omitempty" bigquery:"volume_close_1y"`
	VolumeCloseYTD            bigquery.NullFloat64 `postgres:"volume_close_ytd" json:"volume_close_ytd,omitempty" bigquery:"volume_close_ytd"`
	PriceOpen1H               bigquery.NullFloat64 `postgres:"price_open_1h" json:"price_open_1h,omitempty" bigquery:"price_open_1h"`
	PriceClose1H              bigquery.NullFloat64 `postgres:"price_close_1h" json:"price_close_1h,omitempty" bigquery:"price_close_1h"`
	PriceOpenYTD              bigquery.NullFloat64 `postgres:"price_open_ytd" json:"price_open_ytd,omitempty" bigquery:"price_open_ytd"`
	PriceCloseYTD             bigquery.NullFloat64 `postgres:"price_close_ytd" json:"price_close_ytd,omitempty" bigquery:"price_close_ytd"`
	Supply                    string               `postgres:"supply" json:"supply,omitempty" bigquery:"supply"`
	NumberOfActiveMarketPairs int64                `postgres:"number_of_active_market_pairs" json:"number_of_active_market_pairs,omitempty" bigquery:"number_of_active_market_pairs"`
	Date                      time.Time            `postgres:"last_price_time" json:"lastPriceTime,omitempty" bigquery:"last_price_time"`
	Exchanges                 []PGExchange         `postgres:"exchanges" json:"exchanges,omitempty" bigquery:"exchanges"`
	MarketPairs               []MarketPairs        `postgres:"market_pairs" json:"market_pairs,omitempty" bigquery:"market_pairs"`
}

type NomicsOHLCVTimeTracker struct {
	LastCalled time.Time `postgres:"last_req_time" json:"last_req_time"`
	Base       string    `json:"base" postgres:"base"`
	Quote      string    `json:"quote" postgres:"quote"`
}

type PGMarketPairs struct {
	Base            string    `postgres:"base" json:"base"`
	Quote           string    `postgres:"quote" json:"quote"`
	Pair            string    `postgres:"pair" json:"pair"`
	Exchange        string    `postgres:"exchange" json:"exchange"`
	PairStatus      string    `postgres:"pair_status" json:"pairStatus"`
	UpdateTimeStamp time.Time `postgres:"update_timestamp" json:"update_timestamp"`
}
type PGMarketPairsPriceVolume struct {
	TypeOfPair             string   `postgres:"type_of_pair" json:"typeOfPair"`
	CurrentPriceForPair1D  *float64 `postgres:"current_price_for_pair_1d" json:"currentPriceForPair1D"`
	CurrentPriceForPair7D  *float64 `postgres:"current_price_for_pair_7d" json:"currentPriceForPair7D"`
	CurrentPriceForPair30D *float64 `postgres:"current_price_for_pair_30d" json:"currentPriceForPair30D"`
	CurrentPriceForPair1Y  *float64 `postgres:"current_price_for_pair_1y" json:"currentPriceForPair1Y"`
	CurrentPriceForPairYTD *float64 `postgres:"current_price_for_pair_ytd" json:"currentPriceForPairYTD"`
	VolumeForPair1D        *float64 `postgres:"volume_for_pair_1d" json:"volumeForPair1D"`
	VolumeForPair7D        *float64 `postgres:"volume_for_pair_7d" json:"volumeForPair7D"`
	VolumeForPair30D       *float64 `postgres:"volume_for_pair_30d" json:"volumeForPair30D"`
	VolumeForPair1Y        *float64 `postgres:"volume_for_pair_1y" json:"volumeForPair1Y"`
	VolumeForPairYTD       *float64 `postgres:"volume_for_pair_ytd" json:"volumeForPairYTD"`
}
type PGNomicsResult struct {
	NumberOfActiveMarketPairs *int64   `postgres:"number_of_active_market_pairs"  json:"number_of_active_market_pairs,omitempty"`
	CirculatingSupply         *float64 `postgres:"circulating_supply"  json:"circulating_supply,omitempty"`
	OriginalSymbol            string   `postgres:"original_symbol" json:"original_symbol,omitempty"`
	MaxSupply                 *float64 `postgres:"max_supply" json:"max_supply,omitempty"`
	MarketCap                 *float64 `postgres:"market_cap" json:"market_cap,omitempty"`
	Price24h                  *float64 `postgres:"price_1d" json:"price_1d"`
	Price7D                   *float64 `postgres:"price_7d" json:"price_7d"`
	Price30D                  *float64 `postgres:"price_30d" json:"price_30d"`
	Price1Y                   *float64 `postgres:"price_1y" json:"price_1y"`
	PriceYTD                  *float64 `postgres:"price_ytd" json:"price_ytd"`
	Volume                    *float64 `postgres:"volume" json:"volume"`
	ChangeValue24h            *float64 `postgres:"change_value_24h" json:"change_value_24h"`
	Percentage24h             *float64 `postgres:"percentage_1d" json:"percentage_1d,omitempty"`
	Percentage7D              *float64 `postgres:"percentage_7d" json:"percentage_7d,omitempty"`
	Percentage30D             *float64 `postgres:"percentage_30d" json:"percentage_30d,omitempty"`
	Percentage1Y              *float64 `postgres:"percentage_1y" json:"percentage_1y,omitempty"`
	PercentageYTD             *float64 `postgres:"percentage_ytd" json:"percentage_ytd,omitempty"`
	MarketCapPercentChange1D  *float64 `postgres:"market_cap_percentage_change_1d" json:"market_cap_percentage_change_1d,omitempty"`
	MarketCapPercentChange7D  *float64 `postgres:"market_cap_percentage_change_7d" json:"market_cap_percentage_change_7d,omitempty"`
	MarketCapPercentChange30D *float64 `postgres:"market_cap_percentage_change_30d" json:"market_cap_percentage_change_30d,omitempty"`
	MarketCapPercentChange1Y  *float64 `postgres:"market_cap_percentage_change_1y" json:"market_cap_percentage_change_1y,omitempty"`
	MarketCapPercentChangeYTD *float64 `postgres:"market_cap_percentage_change_ytd" json:"market_cap_percentage_change_ytd,omitempty"`
}

type NomicsVolume struct {
	Volume1D            float64  `postgres:"volume_1d" json:"volume_1d,omitempty"`
	Volume7D            float64  `postgres:"volume_7d" json:"volume_7d,omitempty"`
	Volume30D           float64  `postgres:"volume_30d" json:"volume_30d,omitempty"`
	Volume1Y            float64  `postgres:"volume_1y" json:"volume_1y,omitempty"`
	VolumeYTD           float64  `postgres:"volume_ytd" json:"volume_ytd,omitempty"`
	PercentageVolume1D  *float64 `postgres:"percentage_volume_1d" json:"percentage_volume_1d,omitempty"`
	PercentageVolume7D  *float64 `postgres:"percentage_volume_7d" json:"percentage_volume_7d,omitempty"`
	PercentageVolume30D *float64 `postgres:"percentage_volume_30d" json:"percentage_volume_30d,omitempty"`
	PercentageVolume1Y  *float64 `postgres:"percentage_volume_1y" json:"percentage_volume_1y,omitempty"`
	PercentageVolumeYTD *float64 `postgres:"percentage_volume_ytd" json:"percentage_volume_ytd,omitempty"`
}

type OpenCloseValues struct {
	Open  *float64 `postgres:"open" json:"open,omitempty" bigquery:"open"`
	Close *float64 `postgres:"close" json:"close,omitempty" bigquery:"close"`
}

type OpenCloseAsset struct {
	Symbol   string          `postgres:"symbol" json:"symbol,omitempty"`
	Price1H  OpenCloseValues `postgres:"price_1h" json:"price_1h,omitempty" bigquery:"price_1h"`
	Price24H OpenCloseValues `postgres:"price_24h" json:"price_24h,omitempty" bigquery:"price_24h"`
	Price7D  OpenCloseValues `postgres:"price_7d" json:"price_7d,omitempty" bigquery:"price_7d"`
	Price30D OpenCloseValues `postgres:"price_30d" json:"price_30d,omitempty" bigquery:"price_30d"`
	Price1Y  OpenCloseValues `postgres:"price_1y" json:"price_1y,omitempty" bigquery:"price_1y"`
	PriceMax OpenCloseValues `postgres:"price_max" json:"price_max,omitempty" bigquery:"price_max"`
}

type PGExchange struct {
	Market string    `postgres:"Market" json:"market"`
	Symbol string    `postgres:"Symbol" json:"symbol"`
	Time   time.Time `postgres:"Time" json:"time"`
	Close  float64   `postgres:"Close" json:"close"`
	Slug   string    `postgres:"slug" json:"slug"`
}

type ChartDataPG struct {
	Symbol     string    `postgres:"symbol" json:"symbol"`
	Forbes     string    `postgres:"forbes" json:"forbes"`
	Time       time.Time `postgres:"time" json:"time"`
	Price      float64   `postgres:"price" json:"price"`
	DataSource string    `postgres:"data_source" json:"dataSource"`
}

type TimeSeriesResultPG struct {
	Symbol                  string      `json:"symbol" firestore:"symbol" postgres:"symbol"  bigquery:"symbol"`
	TargetResolutionSeconds int         `json:"targetResolutionSeconds" postgres:"target_resolution_seconds" bigquery:"target_resolution_seconds"`
	Slice                   []SlicePG   `firestore:"be-prices" postgres:"be-prices" bigquery:"beprices"`
	FESlice                 []FESlicePG `json:"prices" firestore:"prices" postgres:"prices"`
	IsIndex                 bool        `json:"isIndex" postgres:"is_index" bigquery:"is_index"`
	Source                  string      `json:"source" postgres:"source" bigquery:"source"`
	Interval                string      `json:"interval" postgres:"tm_interval" bigquery:"interval"`
	Status                  string      `json:"status" postgres:"status"` // Status of the asset EX: active/inactive
	Notice                  string      `json:"notice"`                   //Used to To Notify FE with unexpected chart changes. EX there 24hr chart displaying 2 days worth of trade data
	Period                  string      `json:"period"`
	AssetType               string      `json:"assetType"`
}

type FundamentalsForbesPercentage struct {
	ForbesPercentage1D  *float64 `postgres:"forbes_percentage_1d" json:"forbes_percentage_1d,omitempty"`
	ForbesPercentage7D  *float64 `postgres:"forbes_percentage_7d" json:"forbes_percentage_7d,omitempty"`
	ForbesPercentage30D *float64 `postgres:"forbes_percentage_30d" json:"forbes_percentage_30d,omitempty"`
	ForbesPercentage1Y  *float64 `postgres:"forbes_percentage_1y" json:"forbes_percentage_1y,omitempty"`
	ForbesPercentageYTD *float64 `postgres:"forbes_percentage_ytd" json:"forbes_percentage_ytd,omitempty"`
}

type SlicePG struct {
	Time             time.Time `json:"Time" firestore:"x" postgres:"Time" bigquery:"Time"`
	AvgClose         float64   `json:"Price" firestore:"y" postgres:"Price" bigquery:"Price"`
	FloorPriceNative float64   `json:"floorprice_usd" firestore:"floorprice_usd" postgres:"floorprice_native" bigquery:"floorpricenative"`    //for NFT Table
	MarketCapNative  float64   `json:"marketCap_native" firestore:"marketCap_native" postgres:"marketCap_native" bigquery:"marketCap_native"` //for NFT Table
	MarketCapUSD     float64   `json:"marketCap_usd" firestore:"marketCap_usd" postgres:"marketCap_usd" bigquery:"marketCap_usd"`             //for NFT Table
	VolumeNative     float64   `json:"volume_native" firestore:"volume_native" postgres:"volume_native" bigquery:"volume_native"`             //for NFT Table
	VolumeUSD        float64   `json:"volume_usd" firestore:"volume_usd" postgres:"volume_usd" bigquery:"volume_usd"`                         //for NFT Table
}

type FESlicePG struct {
	Time     time.Time `json:"x" firestore:"x" postgres:"x"`
	AvgClose float64   `json:"y" firestore:"y" postgres:"y"`
}

// calculator assets data for convert assets page
type CalculatorAssets struct {
	Name   string  `postgres:"name" json:"name"`
	Symbol string  `postgres:"symbol" json:"symbol"`
	Slug   string  `postgres:"slug" json:"slug"`
	Logo   string  `postgres:"logo" json:"logo"`
	Price  float64 `postgres:"price" json:"price"`
}

type AssetMetaData struct {
	ID             string `postgres:"id" json:"id"`
	OriginalSymbol string `postgres:"original_symbol" json:"original_symbol"`
	Description    string `postgres:"description" json:"description"`
	Name           string `postgres:"name" json:"name"`
	LogoURL        string `postgres:"logo_url" json:"logo_url"`
}

type Calculator struct {
	Assets []CalculatorAssets `json:"assets"`
}

type OpenCloseResult struct {
	Close    *float64 `postgres:"close" json:"close"`
	Open     *float64 `postgres:"open" json:"open"`
	Interval string   `postgres:"interval" json:"interval"`
}

type OpenCloseResultArr []OpenCloseResult

func (a OpenCloseResultArr) Value() (driver.Value, error) {
	return json.Marshal(a)
}

func (a *OpenCloseResultArr) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}

	return json.Unmarshal(b, &a)
}

type DynamicDescription struct {
	Global Global `json:"global"`
}

type Global struct {
	MarketCap   float64    `json:"global_market_cap" postgres:"market_cap"`
	Change24H   float64    `json:"change_24h" postgres:"change_24h"`
	Volume24H   float64    `json:"volume_24h" postgres:"volume_24h"`
	Dominance   Dominance  `json:"dominance,omitempty" postgres:"dominance,omitempty"`
	AssetCount  int        `json:"assets_count" postgres:"assets_count"`
	Trending    []Trending `json:"trending" postgres:"trending"`
	LastUpdated time.Time  `json:"last_updated" postgres:"last_updated"`
}
type Dominance struct {
	DominanceOne DominanceAssetsData `json:"dominanceOne,omitempty" postgres:"dominanceOne,omitempty"` // Assets or NFTs Dominance
	DominanceTwo DominanceAssetsData `json:"dominanceTwo,omitempty" postgres:"dominanceTwo,omitempty"` // Assets or NFTs Dominance

}

type DominanceAssetsData struct {
	MarketCapDominance float64 `json:"market_cap_dominance,omitempty" postgres:"market_cap_dominance,omitempty"` // market cap dominance is a percentage for dominance NFTs or Assets
	Name               string  `json:"name,omitempty" postgres:"name,omitempty"`                                 // name for dominance NFTs or Assets
	Slug               string  `json:"slug,omitempty" postgres:"slug,omitempty"`                                 // slug for dominance NFTs or Assets
	DisplaySymbol      string  `json:"display_symbol,omitempty" postgres:"display_symbol,omitempty"`             // display_symbol for dominance NFTs or Assets
	Count              int     `json:"nfts_count,omitempty" postgres:"nfts_count,omitempty"`                     // nfts_count for  NFTs or Assets
}
type Trending struct {
	Name      string  `json:"name" postgres:"name"`             // name of Trending for NFTs and Assets
	Slug      string  `json:"slug" postgres:"slug"`             // slug of Trending for NFTs and Assets
	Change24H float64 `json:"change_24h" postgres:"change_24h"` // volume percentage change 24h of Trending for NFTs and Assets
}

/*
This documentation explains the use of this Struct  https://docs.google.com/document/d/1gjEG6fDHklE6xsVx-DpaAcmqJXNpevv7SBYmpUawlVs/edit
*/
type CategoriesData struct {
	ID                 string              `json:"id" postgres:"id"`                                       // ID of the category
	Name               string              `json:"name" postgres:"name"`                                   // Name of the category
	MarketCap          float64             `json:"market_cap" postgres:"market_cap"`                       // Market cap of the category
	MarketCapChange24H float64             `json:"market_cap_change_24h" postgres:"market_cap_change_24h"` // Market cap change in the last 24 hours
	Content            string              `json:"content" postgres:"content"`                             // Description of the category
	Top3Coins          []string            `json:"top_3_coins" postgres:"top_3_coins"`                     // Top 3 coins in the category
	Volume24H          float64             `json:"volume_24h" postgres:"volume_24h"`                       // Volume in the last 24 hours
	UpdatedAt          time.Time           `json:"updated_at" postgres:"updated_at"`                       // Last updated time
	Markets            []CoinsMarketResult `json:"markets" postgres:"markets"`                             // List of all the assets in the category
}

// NFTPrices Struct Will use to map the data that will retrieve from postgresql Table
type NFTPrices struct {
	ID                                         string  `json:"id" postgres:"id"`                                                                                             // It presents NFT Unique ID
	ContractAddress                            string  `json:"contract_address" postgres:"contract_address"`                                                                 // It presents NFT Contract Address
	AssetPlatformId                            string  `json:"asset_platform_id" postgres:"asset_platform_id"`                                                               // It presents the Chain ID that NFT is related to.
	Name                                       string  `json:"name" postgres:"name"`                                                                                         // It presents the NFT Name
	Symbol                                     string  `json:"symbol" postgres:"symbol"`                                                                                     // It presents the NFT Symbol
	DisplaySymbol                              string  `json:"displaySymbol" postgres:"display_symbol"`                                                                      // It presents the NFT Symbol
	Image                                      string  `json:"logo" postgres:"image"`                                                                                        // It presents the NFT Image
	Description                                string  `json:"description" postgres:"description"`                                                                           // It presents the NFT Description
	NativeCurrency                             string  `json:"native_currency" postgres:"native_currency"`                                                                   // It presents the NFT currency that NFT use to specify the currency like ethereum.
	FloorPriceUsd                              float64 `json:"floor_price_usd" postgres:"floor_price_usd"`                                                                   // It presents min price for the NFT in USD.
	MarketCapUsd                               float64 `json:"market_cap_usd" postgres:"market_cap_usd"`                                                                     // It presents the market cap for NFT in USD.
	Volume24hUsd                               float64 `json:"volume_24h_usd" postgres:"volume_24h_usd"`                                                                     // It presents volume for NFT in USD.
	FloorPriceNative                           float64 `json:"floor_price_native" postgres:"floor_price_native"`                                                             // It presents min price for NFT in native currency
	MarketCapNative                            float64 `json:"market_cap_native" postgres:"market_cap_native"`                                                               // It presents market cap for NFT in native currency
	Volume24hNative                            float64 `json:"volume_24h_native" postgres:"volume_24h_native"`                                                               // It presents volume for NFT in native currency
	FloorPriceInUsd24hPercentageChange         float64 `json:"floor_price_in_usd_24h_percentage_change" postgres:"floor_price_in_usd_24h_percentage_change"`                 // It presents the percentage change in floor price for 24 hours for NFT
	Volume24hPercentageChangeUsd               float64 `json:"volume_24h_percentage_change_usd" postgres:"volume_24h_percentage_change_usd"`                                 // It presents the percentage change in floor price for 24 hours for NFT
	NumberOfUniqueAddresses                    int     `json:"number_of_unique_addresses" postgres:"number_of_unique_addresses"`                                             // It presents the number of owners for the NFT
	NumberOfUniqueAddresses24hPercentageChange float64 `json:"number_of_unique_addresses_24h_percentage_change" postgres:"number_of_unique_addresses_24h_percentage_change"` // It presents the percentage change in the number of owners for 24 hours for NFTs.
	Slug                                       string  `json:"slug" postgres:"slug"`                                                                                         // It presents the slug for NFT
	TotalSupply                                float64 `json:"total_supply" postgres:"total_supply"`                                                                         // It presents total supply the NFT provide in there collection
	LastUpdated                                string  `json:"last_updated" postgres:"last_updated"`                                                                         // It presents last time NFT Data updated.
	FullCount                                  *int    `postgres:"full_count"`                                                                                               // It presents the number of NFTs that we have in Postgres.
	UUID                                       string  `json:"uuid"`                                                                                                         // It presents the number of NFTs that we have in Postgres.
}

type NFTPricesResp struct {
	NFT                   []NFTPrices `json:"nft"`   // Array of NFTs result
	Total                 int         `json:"total"` // The NFTs total exist in response that return from Postgres.
	HasTemporaryDataDelay bool        `json:"hasTemporaryDataDelay"`
	Source                string      `json:"source"` // The source that provides NFTs data.
}

func PGConnect() *sql.DB {
	if pg == nil {
		DBClientOnce.Do(func() {
			connectionString := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s", os.Getenv("DB_HOST"), os.Getenv("DB_PORT"), os.Getenv("DB_USER"), os.Getenv("DB_PASSWORD"), os.Getenv("DB_NAME"), os.Getenv("DB_SSLMODE"))

			driverName, err := otelsql.Register("postgres",
				otelsql.TraceAll(),
				otelsql.WithDatabaseName(os.Getenv("DB_NAME")),
				otelsql.WithSystem(semconv.DBSystemPostgreSQL),
			)
			if err != nil {
				log.Error("%s", err)
				return
			}

			pg, err = sql.Open(driverName, connectionString)

			if err != nil {
				log.Error("%s", err)
				return
			}

			if err := otelsql.RecordStats(pg); err != nil {
				return
			}
			maxLifetime := 5 * time.Minute

			pg.SetConnMaxLifetime(maxLifetime)
			//pg.SetConnMaxIdleTime(maxLifetime)
			connectionError := pg.Ping()

			if connectionError != nil {
				log.Error("%s", connectionError)
				return
			}
		})
	}
	return pg

}

func PGClose() {
	if pg != nil {
		pg.Close()
	}
}

type slicePGResult []SlicePG
type exchangeResult []PGExchange
type firestoreExchangeResult []FirestoreExchange
type pairsResult []MarketPairs
type coinsMarketResultResult []CoinsMarketResult
type volumePG Volume
type dominance Dominance

type trendingResult []Trending

func (c trendingResult) Value() (driver.Value, error) {
	return json.Marshal(c)
}
func (c *trendingResult) Scan(value interface{}) error {
	var b []byte
	switch t := value.(type) {
	case []byte:
		b = t
	case string:
		b = []byte(t)
	default:
		return errors.New("unknown type")
	}
	return json.Unmarshal(b, c)
}

func (c dominance) Value() (driver.Value, error) {
	return json.Marshal(c)
}
func (c *dominance) Scan(value interface{}) error {
	var b []byte
	switch t := value.(type) {
	case []byte:
		b = t
	case string:
		b = []byte(t)
	default:
		return errors.New("unknown type")
	}
	return json.Unmarshal(b, c)
}

func (c firestoreExchangeResult) Value() (driver.Value, error) {
	return json.Marshal(c)
}

func (c coinsMarketResultResult) Value() (driver.Value, error) {
	return json.Marshal(c)
}
func (c *volumePG) Scan(value interface{}) error {
	var b []byte
	switch t := value.(type) {
	case []byte:
		b = t
	case string:
		b = []byte(t)
	default:
		return errors.New("unknown type")
	}
	return json.Unmarshal(b, c)
}
func (c volumePG) Value() (driver.Value, error) {
	return json.Marshal(c)
}

func (c *firestoreExchangeResult) Scan(value interface{}) error {
	var b []byte
	switch t := value.(type) {
	case []byte:
		b = t
	case string:
		b = []byte(t)
	default:
		return errors.New("unknown type")
	}
	return json.Unmarshal(b, c)
}

func (c *coinsMarketResultResult) Scan(value interface{}) error {
	var b []byte
	switch t := value.(type) {
	case []byte:
		b = t
	case string:
		b = []byte(t)
	default:
		return errors.New("unknown type")
	}
	return json.Unmarshal(b, c)
}

func (c slicePGResult) Value() (driver.Value, error) {
	return json.Marshal(c)
}

func (c *slicePGResult) Scan(value interface{}) error {
	var b []byte
	switch t := value.(type) {
	case []byte:
		b = t
	case string:
		b = []byte(t)
	default:
		return errors.New("unknown type")
	}
	return json.Unmarshal(b, c)
}

// Make the Exchanges type implement the driver.Valuer interface. This method
// simply returns the JSON-encoded representation of the struct.
func (c exchangeResult) Value() (driver.Value, error) {
	return json.Marshal(c)
}

func (c pairsResult) Value() (driver.Value, error) {
	return json.Marshal(c)
}

// Make the Exchanges type implement the sql.Scanner interface. This method
// simply decodes a JSON-encoded value into the struct fields.
func (c *exchangeResult) Scan(value interface{}) error {
	var b []byte
	switch t := value.(type) {
	case []byte:
		b = t
	case string:
		b = []byte(t)
	default:
		return errors.New("unknown type")
	}

	return json.Unmarshal(b, c)
}

func (c *pairsResult) Scan(value interface{}) error {
	var b []byte
	switch t := value.(type) {
	case []byte:
		b = t
	case string:
		b = []byte(t)
	default:
		return errors.New("unknown type")
	}

	return json.Unmarshal(b, c)
}

func BuildPGQuery(ctx0 context.Context) string {
	_, span := tracer.Start(ctx0, "BuildPGQuery")
	defer span.End()

	candlesTable := "nomics_ohlcv_candles"
	query := `
	with 
		allTime as 
			(
				SELECT 
					CAST(MIN(Close) AS FLOAT) all_time_low, 
					base as symbol
				FROM ( 
						SELECT 
							AVG(close) as Close, 
							base 
						FROM 
							` + candlesTable + `
						GROUP BY 
							base
					) as allTime
				GROUP BY 
					base
			),
		oneDay AS 
			(
				SELECT 
					CAST(MAX(Close) AS FLOAT) high_1d, 
					CAST(MIN(Close) AS FLOAT) low_1d, 
					base as symbol
				FROM
					( 
						SELECT 
							AVG(close) as Close, 
							base
						FROM 
							` + candlesTable + `
						WHERE 
							timestamp >= cast(now() - INTERVAL '24 HOUR' as timestamp)
						GROUP BY 
							base,
							timestamp
					) as oneDay
				GROUP BY 
				base
			),
		sevenDays AS 
			(
				SELECT 
					CAST(MAX(Close) AS FLOAT) high_7d, 
					CAST(MIN(Close) AS FLOAT) low_7d, 
					base as symbol
				FROM 
					( 
						SELECT 
							AVG(close) as Close, 
							base
						FROM 
							nomics_ohlcv_candles
						WHERE 
							timestamp >=  cast(now() - INTERVAL '7 DAYS' as timestamp)
						GROUP BY 
							base,
							timestamp
					) as sevenDays
				GROUP BY 
					base
			),
		thirtyDays AS 
			(
				SELECT 
					CAST(MAX(Close) AS FLOAT) high_30d, 
					CAST(MIN(Close) AS FLOAT) low_30d, 
					base as symbol
				FROM 
					(
						SELECT 
							AVG(close) as Close, 
							base
						FROM 
							nomics_ohlcv_candles
						WHERE 
							timestamp >=  cast(now() - INTERVAL '30 DAYS' as timestamp)
						GROUP BY 
							base,
							timestamp
					) as thirtyDays
				GROUP BY 
				base
			),
		oneYear AS 
			(
				SELECT 
					CAST(MAX(Close) AS FLOAT) high_1y, 
					CAST(MIN(Close) AS FLOAT) low_1y, 
					base as symbol
				FROM 
					( 
						SELECT 
							AVG(close) as Close, 
							base
						FROM 
							nomics_ohlcv_candles
						WHERE 
							timestamp >=  cast(now() - INTERVAL '365 DAYS' as timestamp)
						GROUP BY 
							base,
							timestamp
					) as oneYear
				GROUP BY 
					base
			),

		YTD AS 
			(
				SELECT 
					CAST(MAX(Close) AS FLOAT) high_ytd, 
					CAST(MIN(Close) AS FLOAT) low_ytd, 
					base as symbol
				FROM 
					( 
						SELECT 
							AVG(close) as Close, 
							base
						FROM 
							nomics_ohlcv_candles
						WHERE 
							timestamp  >= cast(date_trunc('year', current_date) as timestamp)
						GROUP BY 
							base,
							timestamp
					) as oneYear
				GROUP BY 
					base
			)
	select 
		CAST(MAX(oneDay.high_1d) AS FLOAT) AS high_24h,
		CAST(MIN(oneDay.low_1d) AS FLOAT) AS low_24h,
		CAST(MAX(sevenDays.high_7d) AS FLOAT) AS high_7d,
		CAST(MIN(sevenDays.low_7d) AS FLOAT) AS low_7d,
		CAST(MAX(thirtyDays.high_30d) AS FLOAT) AS high_30d,
		CAST(MIN(thirtyDays.low_30d) AS FLOAT) AS low_30d,
		CAST(MAX(oneYear.high_1y) AS FLOAT) AS high_1y,
 	   	CAST(MIN(oneYear.low_1y) AS FLOAT) AS low_1y,
		CAST(MAX(YTD.high_ytd) AS FLOAT) AS high_ytd,
		CAST(MIN(YTD.low_ytd) AS FLOAT) AS low_ytd,
		CAST(MIN(allTime.all_time_low) AS FLOAT) AS all_time_low,
		oneDay.symbol
	from 
		oneDay 
		INNER JOIN 
			sevenDays 
		ON 
			sevenDays.symbol = oneDay.symbol
		INNER JOIN 
			thirtyDays 
		ON 
			thirtyDays.symbol = oneDay.symbol
		INNER JOIN 
			oneYear 
		ON 
			oneYear.symbol = oneDay.symbol
		INNER JOIN 
			allTime 
		ON 
			allTime.symbol = oneDay.symbol
		INNER JOIN 
			YTD 
		ON 
			YTD.symbol = oneDay.symbol
	group by 
		oneDay.symbol

	`

	return query
}

func BuildExchangeFundamentalsQuery() string {
	nomicsCandles := "nomics_ohlcv_candles"
	nomicsExchange := "nomics_exchange_market_ticker"
	query := `
	with 
		allTime as 
			(
				SELECT 
					base as symbol
				FROM 
					( 
						SELECT base 
						FROM 
							` + nomicsCandles + `
						GROUP BY 
							base
					) as allTime
				GROUP BY 
				base
			),
		ExchangesPrices AS 
			( 
				SELECT 
					base as Symbol, 
					exchange as Market
				FROM 
					` + nomicsExchange + `
				WHERE 
					exchange NOT IN ('bitmex','hbtc') 
					AND type = 'spot'
					AND timestamp >=  cast(now() - INTERVAL '30 MINUTE' as timestamp)
					AND status = 'active'
					AND quote IN ('USD', 'USDT', 'USDC')
				group by 
					base,
					exchange
			)
		select 
			array_to_json(ARRAY_AGG(json_build_object('Market', ExchangesPrices.Market, 'Symbol', ExchangesPrices.Symbol))) as Exchanges,
			allTime.symbol
		from 
			allTime 
			INNER JOIN 
				ExchangesPrices 
			ON 
				ExchangesPrices.Symbol = allTime.symbol
		group by 
			allTime.symbol
	`

	return query
}

func PGGetTradedAssets(ctx0 context.Context, lim int, pageNum int, sortBy string, direction string) ([]byte, error) {

	ctx, span := tracer.Start(ctx0, "PGGetTradedAssets")
	defer span.End()

	switch sortBy {
	case "price":
		sortBy = "price_24h"
	case "change":
		sortBy = "change_value_24h"
	case "volume":
		sortBy = "volume_1d"
	case "percentage": // percentage and change returns the same sortBy value.
		sortBy = "change_value_24h"
	case "percentage_1h":
		sortBy = "percentage_1h"
	case "percentage_7d":
		sortBy = "percentage_7d"
	case "name":
		sortBy = "name"
	default:
		sortBy = "market_cap"
	}

	startTime := log.StartTime("Pagination Query")
	var assets []TradedAssetsTable

	pg := PGConnect()
	query := `select 
				symbol, 
				display_symbol, 
				name,
				slug,
				logo,
				temporary_data_delay,
				price_24h,
				percentage_1h,
				percentage_24h,
				percentage_7d,
				change_value_24h,
				market_cap,
				volume_1d,
				full_count,
				market_cap_percent_change_1d
				from public.tradedAssetsPagination_BySource_1($1, $2, $3, $4, $5)` //The frontend starts at 1, while the query will consider the pagenum always has to subtract 1

	queryResult, err := pg.QueryContext(ctx, query, lim, pageNum-1, sortBy, direction, data_source)

	var nomics TradedAssetsTable

	if err != nil {
		log.EndTime("Pagination Query", startTime, err)
		return nil, err
	}
	defer queryResult.Close()

	for queryResult.Next() {
		err := queryResult.Scan(&nomics.Symbol, &nomics.DisplaySymbol, &nomics.Name, &nomics.Slug, &nomics.Logo, &nomics.TemporaryDataDelay, &nomics.Price, &nomics.Percentage1H, &nomics.Percentage, &nomics.Percentage7D, &nomics.ChangeValue, &nomics.MarketCap, &nomics.Volume, &nomics.FullCount, &nomics.MarketCapPercentage1d)
		if err != nil {
			log.EndTime("Pagination Query", startTime, err)
			return nil, err
		}
		assets = append(assets, nomics)
	}

	var resp = TradedAssetsResp{Source: data_source, Total: *assets[0].FullCount, Assets: assets}

	jsonData, err := json.Marshal(resp)
	if err != nil {
		return nil, err
	}
	log.EndTime("Pagination Query", startTime, nil)
	return jsonData, nil
}

func GetLeadersAndLaggardsPG(ctx0 context.Context) ([]FundamentalsData, error) {

	ctx, span := tracer.Start(ctx0, "GetLeadersAndLaggardsPG")
	defer span.End()

	startTime := log.StartTime("Fundamentals Data Query")

	pg := PGConnect()

	var fundamentals []FundamentalsData

	queryResult, err := pg.QueryContext(ctx, fmt.Sprintf(`
	select 
		symbol,
		name, 
		slug,
		logo,
		display_symbol,
		price_24h,
		percentage_24h,
		change_value_24h 
	from 
		public.leaders_laggardsbysource('%s');
				`, data_source))
	if err != nil {
		log.EndTime("Fundamentals Data Query", startTime, err)
		return nil, err
	}
	defer queryResult.Close()

	for queryResult.Next() {
		var fundamental FundamentalsData
		err := queryResult.Scan(&fundamental.Symbol, &fundamental.Name, &fundamental.Slug, &fundamental.Logo, &fundamental.DisplaySymbol, &fundamental.Price24h, &fundamental.Percentage24h, &fundamental.ChangeValue24h)

		if err != nil {
			log.EndTime("Fundamentals Data Query Scan", startTime, err)
			return nil, err
		}
		fundamentals = append(fundamentals, fundamental)

	}
	log.EndTime("Fundamentals Data Query", startTime, nil)
	return fundamentals, nil
}

func InsertNomicsChartData(ctx0 context.Context, period string, chartData []TimeSeriesResultPG) error {

	ctx, span := tracer.Start(ctx0, "InsertNomicsChartData")
	defer span.End()

	startTime := log.StartTime("Nomics Charts Data Insert")

	pg := PGConnect()

	valueString := make([]string, 0, len(chartData))
	valueCharts := make([]interface{}, 0, len(chartData)*7)
	var i = 0

	tableName := "nomics_chart_data"

	for y := 0; y < len(chartData); y++ {
		var chart = chartData[y]

		var valString = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d)", i*7+1, i*7+2, i*7+3, i*7+4, i*7+5, i*7+6, i*7+7)
		valueString = append(valueString, valString)
		valueCharts = append(valueCharts, chart.IsIndex)
		valueCharts = append(valueCharts, chart.Source)
		valueCharts = append(valueCharts, chart.TargetResolutionSeconds)
		slice, _ := json.Marshal(chart.Slice)
		valueCharts = append(valueCharts, slice)
		valueCharts = append(valueCharts, chart.Symbol)
		interval := fmt.Sprintf("%s_%s", chart.Symbol, period)
		valueCharts = append(valueCharts, interval)
		valueCharts = append(valueCharts, chart.AssetType)

		i++

		if len(valueCharts) >= 65000 || y == len(chartData)-1 {
			upsertStatement := "ON CONFLICT ON CONSTRAINT uniqueAsset DO UPDATE SET is_index = EXCLUDED.is_index, source = EXCLUDED.source, target_resolution_seconds = EXCLUDED.target_resolution_seconds, prices = EXCLUDED.prices, symbol = EXCLUDED.symbol"
			insertCharts := fmt.Sprintf("INSERT INTO %s VALUES %s %s", tableName, strings.Join(valueString, ","), upsertStatement)
			_, inserterError := pg.ExecContext(ctx, insertCharts, valueCharts...)

			if inserterError != nil {
				log.EndTime("Charts Insert", startTime, inserterError)
				return inserterError
			}
			valueString = make([]string, 0, len(chartData))
			valueCharts = make([]interface{}, 0, len(chartData)*7)
			i = 0

		}

	}
	log.EndTime("Nomics Charts Data Insert", startTime, nil)

	return nil
}

// takes an array of category fundamentals data and upserts it to the Category fundamentals table in Postgres.
func UpsertCategoryFundamentalsPG(ctx0 context.Context, allFundamentals *[]CategoryFundamental, labels map[string]string) error {

	ctx, span := tracer.Start(ctx0, "Upsert Category Fundamentals")
	defer span.End()

	startTime := log.StartTime("Upsert Category Fundamentals")

	pg := PGConnect()
	fundamentals := *allFundamentals

	valueString := make([]string, 0, len(fundamentals))
	valueArgs := make([]interface{}, 0, len(fundamentals)*19)
	var i = 0

	tableName := "categories_fundamentals"

	for y := 0; y < len(fundamentals); y++ {
		var f = fundamentals[y]
		var valString = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)", i*20+1, i*20+2, i*20+3, i*20+4, i*20+5, i*20+6, i*20+7, i*20+8, i*20+9, i*20+10, i*20+11, i*20+12, i*20+13, i*20+14, i*20+15, i*20+16, i*20+17, i*20+18, i*20+19, i*20+20)
		valueString = append(valueString, valString)
		valueArgs = append(valueArgs, f.ID)
		valueArgs = append(valueArgs, f.Name)
		valueArgs = append(valueArgs, f.TotalTokens.Int64)
		valueArgs = append(valueArgs, f.AveragePercentage24H.Float64)
		valueArgs = append(valueArgs, f.Volume24H.Float64)
		valueArgs = append(valueArgs, f.Price24H.Float64)
		valueArgs = append(valueArgs, f.AveragePrice.Float64)
		valueArgs = append(valueArgs, f.MarketCap.Float64)
		valueArgs = append(valueArgs, f.MarketCapPercentage24H.Float64)
		valueArgs = append(valueArgs, f.WeightIndexPrice.Float64)
		valueArgs = append(valueArgs, f.WeightIndexMarketCap.Float64)
		valueArgs = append(valueArgs, f.MarketCapIndexValue24H.Float64)
		valueArgs = append(valueArgs, f.MarketCapIndexPercentage24H.Float64)
		valueArgs = append(valueArgs, f.Divisor.Float64)
		topGainers, _ := json.Marshal(f.TopGainers)
		valueArgs = append(valueArgs, topGainers)
		topMovers, _ := json.Marshal(f.TopMovers)
		valueArgs = append(valueArgs, topMovers)
		valueArgs = append(valueArgs, f.LastUpdated.Timestamp)
		valueArgs = append(valueArgs, f.ForbesID)
		valueArgs = append(valueArgs, f.ForbesName)
		valueArgs = append(valueArgs, f.Slug)

		i++

		if len(valueArgs) >= 65000 || y == len(fundamentals)-1 {
			upsertStatement := ` ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id, name = EXCLUDED.name, total_tokens = EXCLUDED.total_tokens, average_percentage_24h = EXCLUDED.average_percentage_24h, volume_24h = EXCLUDED.volume_24h, price_24h = EXCLUDED.price_24h, average_price = EXCLUDED.average_price, market_cap = EXCLUDED.market_cap, market_cap_percentage_24h = EXCLUDED.market_cap_percentage_24h, price_weight_index = EXCLUDED.price_weight_index, market_cap_weight_index = EXCLUDED.market_cap_weight_index, index_market_cap_24h = EXCLUDED.index_market_cap_24h, index_market_cap_percentage_24h = EXCLUDED.index_market_cap_percentage_24h, divisor = EXCLUDED.divisor, top_gainers = EXCLUDED.top_gainers, top_movers = EXCLUDED.top_movers, last_updated = EXCLUDED.last_updated, "forbesID" = EXCLUDED."forbesID","forbesName" = EXCLUDED."forbesName",slug = EXCLUDED.slug;`
			insertStatement := fmt.Sprintf("INSERT INTO %s VALUES %s %s", tableName, strings.Join(valueString, ","), upsertStatement)
			latencyTimeStart := time.Now()
			_, inserterError := pg.ExecContext(ctx, insertStatement, valueArgs...)
			latency := time.Since(latencyTimeStart)

			log.InfoL(labels, fmt.Sprintf("Upsert Category Fundamentals : time to insert %dms", latency.Milliseconds()))

			if inserterError != nil {
				log.ErrorL(labels, fmt.Sprintf("UpsertCategoryFundamentals TimeElapsed: %fs", latency.Seconds()), inserterError)
				log.EndTime("Upsert Category Fundamentals", startTime, inserterError)
				return inserterError
			}
			valueString = make([]string, 0, len(fundamentals))
			valueArgs = make([]interface{}, 0, len(fundamentals)*19)
			i = 0
		}
	}
	log.EndTime("Upsert Category Fundamentals", startTime, nil)

	return nil
}

// takes an array of fundamentals data and upserts it to the fundamentals latest tabel
func UpsertFundamentalsLatest(ctx0 context.Context, fundamentals []Fundamentals, labels map[string]string) error {

	ctx, span := tracer.Start(ctx0, "Upsert Fundamentals Latest")
	defer span.End()

	startTime := log.StartTime("Upsert Fundamentals Latest")

	pg := PGConnect()

	valueString := make([]string, 0, len(fundamentals))
	valueCharts := make([]interface{}, 0, len(fundamentals)*51)
	var i = 0

	tableName := "fundamentalslatest"

	for y := 0; y < len(fundamentals); y++ {
		var f = fundamentals[y]

		var valString = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)", i*51+1, i*51+2, i*51+3, i*51+4, i*51+5, i*51+6, i*51+7, i*51+8, i*51+9, i*51+10, i*51+11, i*51+12, i*51+13, i*51+14, i*51+15, i*51+16, i*51+17, i*51+18, i*51+19, i*51+20, i*51+21, i*51+22, i*51+23, i*51+24, i*51+25, i*51+26, i*51+27, i*51+28, i*51+29, i*51+30, i*51+31, i*51+32, i*51+33, i*51+34, i*51+35, i*51+36, i*51+37, i*51+38, i*51+39, i*51+40, i*51+41, i*51+42, i*51+43, i*51+44, i*51+45, i*51+46, i*51+47, i*51+48, i*51+49, i*51+50, i*51+51)
		valueString = append(valueString, valString)
		valueCharts = append(valueCharts, f.Symbol)
		valueCharts = append(valueCharts, f.Name)
		valueCharts = append(valueCharts, f.Slug)
		valueCharts = append(valueCharts, f.Logo)
		valueCharts = append(valueCharts, f.FloatType)
		valueCharts = append(valueCharts, f.DisplaySymbol)
		valueCharts = append(valueCharts, f.OriginalSymbol)
		valueCharts = append(valueCharts, f.Source)
		valueCharts = append(valueCharts, f.TemporaryDataDelay)
		valueCharts = append(valueCharts, f.NumberOfActiveMarketPairs)
		valueCharts = append(valueCharts, f.High24h)
		valueCharts = append(valueCharts, f.Low24h)
		valueCharts = append(valueCharts, f.High7D)
		valueCharts = append(valueCharts, f.Low7D)
		valueCharts = append(valueCharts, f.High30D)
		valueCharts = append(valueCharts, f.Low7D)
		valueCharts = append(valueCharts, f.High1Y)
		valueCharts = append(valueCharts, f.Low1Y)
		valueCharts = append(valueCharts, f.HighYTD)
		valueCharts = append(valueCharts, f.LowYTD)
		valueCharts = append(valueCharts, f.Price24h)
		valueCharts = append(valueCharts, f.Price7D)
		valueCharts = append(valueCharts, f.Price30D)
		valueCharts = append(valueCharts, f.Price1Y)
		valueCharts = append(valueCharts, f.PriceYTD)
		valueCharts = append(valueCharts, f.Percentage24h)
		valueCharts = append(valueCharts, f.Percentage7D)
		valueCharts = append(valueCharts, f.Percentage30D)
		valueCharts = append(valueCharts, f.Percentage1Y)
		valueCharts = append(valueCharts, f.PercentageYTD)
		valueCharts = append(valueCharts, f.MarketCap)
		valueCharts = append(valueCharts, f.MarketCapPercentChange1D)
		valueCharts = append(valueCharts, f.MarketCapPercentChange7D)
		valueCharts = append(valueCharts, f.MarketCapPercentChange30D)
		valueCharts = append(valueCharts, f.MarketCapPercentChange1Y)
		valueCharts = append(valueCharts, f.MarketCapPercentChangeYTD)
		valueCharts = append(valueCharts, f.CirculatingSupply)
		valueCharts = append(valueCharts, f.Supply)
		valueCharts = append(valueCharts, f.AllTimeLow)
		valueCharts = append(valueCharts, f.AllTimeHigh)
		valueCharts = append(valueCharts, f.Date)
		valueCharts = append(valueCharts, f.ChangeValue24h)
		valueCharts = append(valueCharts, pq.Array(f.ListedExchanges))
		exchanges, _ := json.Marshal(f.Exchanges)
		nomics, _ := json.Marshal(f.Nomics)
		market_pairs, _ := json.Marshal(f.MarketPairs)
		forbes, _ := json.Marshal(f.Forbes)

		valueCharts = append(valueCharts, market_pairs)
		valueCharts = append(valueCharts, exchanges)
		valueCharts = append(valueCharts, nomics)
		valueCharts = append(valueCharts, forbes)
		valueCharts = append(valueCharts, f.LastUpdated)
		valueCharts = append(valueCharts, f.ForbesTransparencyVolume)
		valueCharts = append(valueCharts, f.Status)
		valueCharts = append(valueCharts, f.Percentage1H)

		i++

		if len(valueCharts) >= 65000 || y == len(fundamentals)-1 {
			upsertStatement := " ON CONFLICT (symbol) DO UPDATE SET symbol = EXCLUDED.symbol, name = EXCLUDED.name, slug = EXCLUDED.slug, logo = EXCLUDED.logo, float_type = EXCLUDED.float_type, display_symbol = EXCLUDED.display_symbol, original_symbol = EXCLUDED.original_symbol, source = EXCLUDED.source, temporary_data_delay = EXCLUDED.temporary_data_delay, number_of_active_market_pairs = EXCLUDED.number_of_active_market_pairs, high_24h = EXCLUDED.high_24h, low_24h = EXCLUDED.low_24h, high_7d = EXCLUDED.high_7d, low_7d = EXCLUDED.low_7d, high_30d = EXCLUDED.high_30d, low_30d = EXCLUDED.low_30d, high_1y = EXCLUDED.high_1y, low_1y = EXCLUDED.low_1y, high_ytd = EXCLUDED.high_ytd, low_ytd = EXCLUDED.low_ytd, price_24h = EXCLUDED.price_24h, price_7d = EXCLUDED.price_7d, price_30d = EXCLUDED.price_30d, price_1y = EXCLUDED.price_1y, price_ytd = EXCLUDED.price_ytd,market_cap_percent_change_1d = EXCLUDED.market_cap_percent_change_1d, market_cap_percent_change_7d = EXCLUDED.market_cap_percent_change_7d,market_cap_percent_change_30d  = EXCLUDED.market_cap_percent_change_30d,market_cap_percent_change_1y = EXCLUDED.market_cap_percent_change_1y,market_cap_percent_change_ytd  = EXCLUDED.market_cap_percent_change_ytd, circulating_supply = EXCLUDED.circulating_supply, supply = EXCLUDED.supply, all_time_low = EXCLUDED.all_time_low, all_time_high = EXCLUDED.all_time_high, date = EXCLUDED.date, change_value_24h = EXCLUDED.change_value_24h, listed_exchange = EXCLUDED.listed_exchange, market_pairs = EXCLUDED.market_pairs, exchanges = EXCLUDED.exchanges, nomics = EXCLUDED.nomics, last_updated = EXCLUDED.last_updated, status = EXCLUDED.status, percentage_24h = EXCLUDED.percentage_24h, percentage_7d = EXCLUDED.percentage_7d, percentage_30d = EXCLUDED.percentage_30d, percentage_1y = EXCLUDED.percentage_1y, percentage_ytd = EXCLUDED.percentage_ytd, percentage_1h = EXCLUDED.percentage_1h, market_cap = EXCLUDED.market_cap;"
			insertCharts := fmt.Sprintf("INSERT INTO %s VALUES %s %s", tableName, strings.Join(valueString, ","), upsertStatement)
			latencyTimeStart := time.Now()
			_, inserterError := pg.ExecContext(ctx, insertCharts, valueCharts...)
			latency := time.Since(latencyTimeStart)

			log.InfoL(labels, fmt.Sprintf("Upsert Fundamentals : time to insert %dms", latency.Milliseconds()))

			if inserterError != nil {
				log.ErrorL(labels, fmt.Sprintf("UpsertFundamentals TimeElapsed: %fs", latency.Seconds()), inserterError)
				log.EndTime("Upsert Fundamentals Latest", startTime, inserterError)
				return inserterError
			}
			valueString = make([]string, 0, len(fundamentals))
			valueCharts = make([]interface{}, 0, len(fundamentals)*51)
			i = 0

		}

	}
	log.EndTime("Upsert Fundamentals Latest", startTime, nil)

	return nil
}

// Queries all chart data for a given asset or nft, and the assets status and returns the results.
func GetChartData(ctxO context.Context, interval string, symbol string, period string, assetsType string) ([]byte, error) {

	ctx, span := tracer.Start(ctxO, "PGUpdateChartData")
	defer span.End()
	startTime := log.StartTime("Charts Data Query")

	pg := PGConnect()

	var timeSeriesResults []TimeSeriesResultPG
	var result TimeSeriesResultPG
	var query string
	var queryResult *sql.Rows
	var err error
	// we will remove it when we start use the new endpoint
	if assetsType == "" {
		query = `
		select
			is_index, 
			source, 
			target_resolution_seconds, 
			prices,
			tm_interval,
			symbol,
			status  
			from public.getChartData($1, $2)`

		queryResult, err = pg.QueryContext(ctx, query, interval, symbol)

		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			log.EndTime("Charts Data Query", startTime, err)
			return nil, err
		}
	} else {
		query = `
		select
			is_index, 
			source, 
			target_resolution_seconds, 
			prices,
			tm_interval,
			symbol,
			status  
			from public.getFTNFTChartData($1, $2, $3)`

		queryResult, err = pg.QueryContext(ctx, query, interval, symbol, assetsType)

		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			log.EndTime("Charts Data Query", startTime, err)
			return nil, err
		}

	}

	span.AddEvent("Query Executed")
	defer queryResult.Close()

	for queryResult.Next() {
		var timeSeriesResult TimeSeriesResultPG
		err := queryResult.Scan(&timeSeriesResult.IsIndex, &timeSeriesResult.Source, &timeSeriesResult.TargetResolutionSeconds, (*slicePGResult)(&timeSeriesResult.Slice), &timeSeriesResult.Interval, &timeSeriesResult.Symbol, &timeSeriesResult.Status)

		if err != nil {
			log.EndTime("Charts Data Query Scan", startTime, err)
			return nil, err
		}
		var newSlice []FESlicePG
		for _, sliceObject := range timeSeriesResult.Slice {
			var slice FESlicePG

			slice.Time = sliceObject.Time
			slice.AvgClose = sliceObject.AvgClose
			newSlice = append(newSlice, slice)
		}
		timeSeriesResult.Slice = nil
		timeSeriesResult.FESlice = newSlice
		timeSeriesResults = append(timeSeriesResults, timeSeriesResult)
	}

	log.EndTime("Charts Data Query", startTime, nil)

	//if data is returned from the query run it through the filter
	if len(timeSeriesResults) > 0 {
		result = FilterChartData(ctx, timeSeriesResults, period, interval)
	}
	span.SetStatus(codes.Ok, "Charts Data Query")

	result.Source = data_source

	b, err := json.Marshal(result)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		log.EndTime("Charts Data Query", startTime, err)
		return nil, err
	}
	return b, nil
}

func GetFundamentalsPG(ctxO context.Context, symbol string) (*FundamentalsData, error) {

	// Starts new child span from the parent span in the context.
	ctx, span := tracer.Start(ctxO, "GetFundamentalsPG", trace.WithAttributes(attribute.String("symbol", symbol)))
	defer span.End()

	startTime := log.StartTime("Get Fundamentals Query")

	pg := PGConnect()
	query := `
		 SELECT 
			 symbol,
			 name,
			 slug,
			 logo,
			 float_type,
			 display_symbol,
			 original_symbol,
			 source,
			 temporary_data_delay,
			 number_of_active_market_pairs,
			 high_24h,
			 low_24h,
			 high_7d,
			 low_7d,
			 high_30d,
			 low_30d,
			 high_1y,
			 low_1y,
			 high_ytd,
			 low_ytd,
			 price_24h,
			 price_7d,
			 price_30d,
			 price_1y,
			 price_ytd,
			 percentage_1h,
			 percentage_24h,
			 percentage_7d,
			 percentage_30d,
			 percentage_1y,
			 percentage_ytd,
			 market_cap,
			 market_cap_percent_change_1d,
			 market_cap_percent_change_7d,
			 market_cap_percent_change_30d,
			 market_cap_percent_change_1y,
			 market_cap_percent_change_ytd,
			 circulating_supply,
			 supply,
			 all_time_low,
			 all_time_high,
			 date,
			 change_value_24h,
			 listed_exchange,
			 market_pairs,
			 exchanges,
			 nomics,
			 forbes,
			 last_updated
		 FROM 
			 fundamentalslatest
		 where symbol = '` + symbol + `'
		 ORDER BY 
			 last_updated desc
		 limit 1
		 `
	var fundamentals FundamentalsData

	queryResult, err := pg.QueryContext(ctx, query)
	span.AddEvent("Query Executed")

	if err != nil {

		log.EndTime("Get Fundamentals Query", startTime, err)
		span.SetStatus(codes.Error, "unable to get data for symbol from PG")
		return nil, err

	}

	for queryResult.Next() {
		err := queryResult.Scan(&fundamentals.Symbol, &fundamentals.Name, &fundamentals.Slug, &fundamentals.Logo, &fundamentals.FloatType, &fundamentals.DisplaySymbol, &fundamentals.OriginalSymbol, &fundamentals.Source, &fundamentals.TemporaryDataDelay, &fundamentals.NumberOfActiveMarketPairs, &fundamentals.High24h, &fundamentals.Low24h, &fundamentals.High7D, &fundamentals.Low7D, &fundamentals.High30D, &fundamentals.Low30D, &fundamentals.High1Y, &fundamentals.Low1Y, &fundamentals.HighYTD, &fundamentals.LowYTD, &fundamentals.Price24h, &fundamentals.Price7D, &fundamentals.Price30D, &fundamentals.Price1Y, &fundamentals.PriceYTD, &fundamentals.Percentage1H, &fundamentals.Percentage24h, &fundamentals.Percentage7D, &fundamentals.Percentage30D, &fundamentals.Percentage1Y, &fundamentals.PercentageYTD, &fundamentals.MarketCap, &fundamentals.MarketCapPercentChange1D, &fundamentals.MarketCapPercentChange7D, &fundamentals.MarketCapPercentChange30D, &fundamentals.MarketCapPercentChange1Y, &fundamentals.MarketCapPercentChangeYTD, &fundamentals.CirculatingSupply, &fundamentals.Supply, &fundamentals.AllTimeLow, &fundamentals.AllTimeHigh, &fundamentals.Date, &fundamentals.ChangeValue24h, pq.Array(&fundamentals.ListedExchanges), (*pairsResult)(&fundamentals.MarketPairs), (*firestoreExchangeResult)(&fundamentals.Exchanges), (*volumePG)(&fundamentals.Nomics), (*volumePG)(&fundamentals.Forbes), &fundamentals.LastUpdated)
		if err != nil {
			log.EndTime("Get Fundamentals Query", startTime, err)
			return nil, err
		}
	}
	log.EndTime("Get Fundamentals Query", startTime, nil)

	span.SetStatus(codes.Ok, "success")

	return &fundamentals, nil
}

// To update the portfolio prices, we need to get the latest price from the fundamentalslatest table.
func GetPortfolioPricesPG(ctxO context.Context, symbol string) (*FundamentalsData, error) {

	// Starts new child span from the parent span in the context.
	ctx, span := tracer.Start(ctxO, "GetFundamentalslatestPG", trace.WithAttributes(attribute.String("symbol", symbol)))
	defer span.End()

	startTime := log.StartTime("Get Fundamentalslatest Query")

	pg := PGConnect()

	// We use the combination of display_symbol and source to get the latest price. This is because the symbol is a primary key and it is different for each source. But display_symbol is not a unique key. So we are using the sort by market_cap to get the top asset in the case of a conflict.
	query := `
		 SELECT 
		 	 symbol,
			 logo,
			 display_symbol,
			 source,
			 price_24h
		 FROM 
			 fundamentalslatest
		 WHERE 
		 	LOWER(display_symbol) = LOWER('` + symbol + `')
			AND source = '` + data_source + `' 
			AND market_cap IS NOT null 
		 ORDER BY market_cap DESC
		 LIMIT 1
		 `
	var fundamentals FundamentalsData

	queryResult, err := pg.QueryContext(ctx, query)
	span.AddEvent("Query Executed")

	if err != nil {

		log.EndTime("Get Fundamentalslatest Query", startTime, err)
		span.SetStatus(codes.Error, "unable to get Fundamentalslatest data for symbol from PG")
		return nil, err

	}
	resultCount := 0
	for queryResult.Next() {
		resultCount++
		err := queryResult.Scan(&fundamentals.Symbol, &fundamentals.Logo, &fundamentals.DisplaySymbol, &fundamentals.Source, &fundamentals.Price24h)
		if err != nil {
			log.EndTime("Get Fundamentalslatest Query", startTime, err)
			return nil, err
		}
	}

	log.EndTime("Get Fundamentalslatest Query", startTime, nil)

	if resultCount == 0 {
		return nil, errors.New("no data found for symbol")
	}

	span.SetStatus(codes.Ok, "success")

	return &fundamentals, nil
}

// GetBertieProfiles returns top 200 asset profiles.
func GetBertieProfilesPG(ctxO context.Context) (*[]model.BertieProfile, error) {

	// Starts new child span from the parent span in the context.
	ctx, span := tracer.Start(ctxO, "GetBertieProfilesPG")
	defer span.End()

	startTime := log.StartTime("GetBertieProfilesPG Query")

	pg := PGConnect()

	//We're ignore status=active for nomics because it is not available for nomics assets.
	active_status := ""
	if data_source != "nomics" {
		active_status = " status = 'active' AND "
	}

	//status = 'active' condition is skipped for nomics data_source
	//We're selecting the top 200 assets based on market_cap. And then only caching them.
	query := `
		 SELECT 
			 name,
			 display_symbol as symbol,
			 slug,
			 logo
		 FROM 
			 fundamentalslatest
		 WHERE 
		 	` + active_status + `
			source = '` + data_source + `' 
			AND market_cap IS NOT null 
		 ORDER BY market_cap DESC
		 LIMIT 200
		 `

	var results []model.BertieProfile

	queryResult, err := pg.QueryContext(ctx, query)
	span.AddEvent("Query Executed")

	if err != nil {

		log.EndTime("GetBertieProfilesPG Query", startTime, err)
		span.SetStatus(codes.Error, "unable to GetBertieProfilesPG data for symbol from PG")
		return nil, err

	}
	for queryResult.Next() {
		var assetProfile model.BertieProfile
		err := queryResult.Scan(&assetProfile.Name, &assetProfile.Symbol, &assetProfile.Slug, &assetProfile.Logo)
		if err != nil {
			log.EndTime("GetBertieProfilesPG Query", startTime, err)
			return nil, err
		}
		results = append(results, assetProfile)
	}

	log.EndTime("GetBertieProfilesPG Query", startTime, nil)

	if len(results) == 0 {
		return nil, errors.New("no data found")
	}

	span.SetStatus(codes.Ok, "success")

	return &results, nil
}

// return calculator assets data from fundamentalslatest from PG
func GetAssetsData(ctx0 context.Context) (*Calculator, error) {

	// Starts new child span from the parent span in the context.
	ctx, span := tracer.Start(ctx0, "GetAssetsData")
	defer span.End()
	startTime := log.StartTime("Calculator Assets Data")

	pg := PGConnect()

	var calculatorAssets []CalculatorAssets

	queryResult, err := pg.QueryContext(ctx, `
	select 
		name, 
		symbol,
		slug,
		logo,
		price_24h
	from 
		fundamentalslatest
	where 
		price_24h is not null;
	`)

	span.AddEvent("Query Executed")

	if err != nil {
		log.EndTime("Calculator Assets Data Query", startTime, err)
		span.SetStatus(codes.Error, "Calculator Assets Data from PG")
		return nil, err
	}
	defer queryResult.Close()

	for queryResult.Next() {
		var calculatorAsset CalculatorAssets

		err := queryResult.Scan(&calculatorAsset.Name, &calculatorAsset.Symbol, &calculatorAsset.Slug, &calculatorAsset.Logo, &calculatorAsset.Price)

		if err != nil {
			log.EndTime("Calculator Assets Data Query", startTime, err)
			span.SetStatus(codes.Error, "Calculator Assets Data Scan error")
			return nil, err
		}

		calculatorAssets = append(calculatorAssets, calculatorAsset)
	}

	var resp = Calculator{Assets: calculatorAssets}

	log.EndTime("Calculator Assets Data Query", startTime, nil)

	span.SetStatus(codes.Ok, "success")

	return &resp, nil
}

// iterate through all results. If the status is not active return the max chart
// if there has been < 3 trades in the last 24 hours return trades for the last 2 days
func FilterChartData(ctx0 context.Context, timeSeriesResults []TimeSeriesResultPG, period string, interval string) TimeSeriesResultPG {
	_, span := tracer.Start(ctx0, "FilterChartData")
	defer span.End()
	defer services.LogPanics()

	var (
		notice         = ""
		result         TimeSeriesResultPG
		responsePeriod = period
	)

	var (
		TS_24h TimeSeriesResultPG
		TS_7d  TimeSeriesResultPG
		TS_max TimeSeriesResultPG
	)

	//if we dont have more that 24 hours worth of data amke 24 hr and max assignment
	//else make the 7 day assignment as well. This is to fic nil pointer assignment issue
	if len(timeSeriesResults) == 1 {
		TS_24h = timeSeriesResults[0]
		TS_max = timeSeriesResults[len(timeSeriesResults)-1]
	} else {
		TS_24h = timeSeriesResults[0]
		TS_7d = timeSeriesResults[1]
		TS_max = timeSeriesResults[len(timeSeriesResults)-1]
	}

	// Default the chart based on interval
	for _, cd := range timeSeriesResults {
		if cd.Interval == interval {
			result = cd
			break
		}
	}

	// if the status is not active return the max chart.
	if TS_24h.Status != "active" {
		result = TS_max
		responsePeriod = "max"
		notice = "The maximum trade history available is shown since the token is no longer actively traded."
	} else if len(TS_24h.FESlice) <= 3 && period == "24h" { // if there are less than or = to 3 candles, append more candles to 24 hour chart
		notice = "Due to low trade activity additional information is being provided."
		var includedDates []FESlicePG

		for _, cd := range TS_7d.FESlice {
			//if the time is before the first time in 24hr chart data
			//and not older than 2 days include it in the 24 hour chart
			if cd.Time.Before(TS_24h.FESlice[0].Time) && cd.Time.After(TS_24h.FESlice[0].Time.Add(-time.Hour*24)) {
				includedDates = append(includedDates, cd)
			}
		}
		includedDates = append(includedDates, TS_24h.FESlice...)
		TS_24h.FESlice = includedDates
		result = TS_24h
	}

	//Catch all: If the chart being returned has <= 3 candles dont display data
	if len(result.FESlice) <= 3 {
		result.FESlice = nil
		notice = "Trade data is not currently available for this asset."
	}

	result.Notice = notice
	result.Period = responsePeriod
	return result

}

// get assets data from assets metadata from coingecko table
func GetCoinGeckoMetaData(ctx context.Context) (map[string]AssetMetaData, error) {

	ctx, span := tracer.Start(ctx, "GetAssetsData")
	defer span.End()
	startTime := log.StartTime("Assets MetaData")

	pg := PGConnect()

	var metaDataMap = make(map[string]AssetMetaData)

	queryResult, err := pg.QueryContext(ctx, `
	SELECT 
		id, 
		name,
		original_symbol, 
		description,
		logo_url
	FROM 
		public.coingecko_asset_metadata
	`)

	if err != nil {
		span.SetStatus(codes.Error, "GetCoinGeckoMetaData()")
		log.EndTime("GetCoinGeckoMetaData", startTime, err)
		return nil, err
	}
	defer queryResult.Close()

	for queryResult.Next() {
		var assetMetadata AssetMetaData
		err := queryResult.Scan(&assetMetadata.ID, &assetMetadata.Name, &assetMetadata.OriginalSymbol, &assetMetadata.Description, &assetMetadata.LogoURL)
		if err != nil {
			span.SetStatus(codes.Error, "GetCoinGeckoMetaData()")
			log.EndTime("GetCoinGeckoMetaData ", startTime, err)
			return nil, err
		}

		metaDataMap[assetMetadata.ID] = assetMetadata

	}

	log.EndTime("GetCoinGeckoMetaData", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return metaDataMap, nil
}

/* Returns Profile Data*/
func GetCyptoContent(ctxO context.Context, slug string, config model.FDAConfig) (*model.AssetProfile, error) {
	ctx, span := tracer.Start(ctxO, "GetCryptoContent")
	defer span.End()
	startTime := log.StartTime("Fundamentals Data Query")

	pg := PGConnect()

	var profile model.AssetProfile

	queryResult, err := pg.QueryContext(ctx, fmt.Sprintf(`
	select 
		symbol,
		slug,
		status,
		market_cap,
		price_24h,
		number_of_active_market_pairs,
		description,
		name,
		website_url,
    	blog_url,
    	discord_url,
    	facebook_url,
   	 	github_url,
    	medium_url,
    	reddit_url,
    	telegram_url,
    	twitter_url,
    	whitepaper_url,
    	youtube_url,
    	bitcointalk_url,
    	blockexplorer_url,
    	logo_url,
	forbesMetaDataDescription
	from 
		public.getcryptocontent('%s');
				`, slug))
	if err != nil {
		log.EndTime("Fundamentals Data Query", startTime, err)
		span.SetStatus(otelCodes.Error, err.Error())
		return nil, err
	}
	defer queryResult.Close()

	for queryResult.Next() {

		err := queryResult.Scan(&profile.Symbol, &profile.Slug, &profile.Status, &profile.Marketcap, &profile.CurrentPrice, &profile.MarketsCount, &profile.NomicsDescription, &profile.Name, &profile.Website, &profile.Blog, &profile.Discord, &profile.Facebook, &profile.Github, &profile.Medium, &profile.Reddit, &profile.Telegram, &profile.Twitter, &profile.Whitepaper, &profile.Youtube, &profile.BitcoinTalk, &profile.BlockExplorer, &profile.Logo, &profile.ForbesMetaDataDescription)

		if err != nil {
			span.SetStatus(otelCodes.Error, err.Error())
			log.EndTime("Fundamentals Data Query Scan", startTime, err)
			return nil, err
		}

	}
	log.EndTime("Fundamentals Data Query", startTime, nil)
	span.SetStatus(otelCodes.Ok, "Success")
	profile.StatusExplainerURL = config.StatusExplainerURL
	profile.ForbesStatus = profile.Status
	profile.NomicsStatus = profile.Status
	return &profile, nil
}

func GetExchangeMetaData(ctx0 context.Context) (map[string]model.CoingeckoExchangeMetadata, error) {
	ctx, span := tracer.Start(ctx0, "GetExchangeMetaData")
	defer span.End()

	exchangesMetadata := make(map[string]model.CoingeckoExchangeMetadata)

	pg := PGConnect()

	query := `
	SELECT 
		id,
		name,
		year,
		description,
		location,
		logo_url,
		website_url,
		twitter_url,
		facebook_url,
		youtube_url,
		linkedin_url,
		reddit_url,
		chat_url,
		slack_url,
		telegram_url,
		blog_url,
		centralized,
		decentralized,
		has_trading_incentive,
		trust_score,
		trust_score_rank,
		trade_volume_24h_btc,
		trade_volume_24h_btc_normalized,
		last_updated
	FROM 
		public.getTopExchanges()
	`

	queryResult, err := pg.QueryContext(ctx, query)

	if err != nil {
		return nil, err
	}

	for queryResult.Next() {
		var exchangeMetadata model.CoingeckoExchangeMetadata

		err := queryResult.Scan(&exchangeMetadata.ID, &exchangeMetadata.Name, &exchangeMetadata.Year, &exchangeMetadata.Description, &exchangeMetadata.Location, &exchangeMetadata.LogoURL, &exchangeMetadata.WebsiteURL, &exchangeMetadata.TwitterURL, &exchangeMetadata.FacebookURL, &exchangeMetadata.YoutubeURL, &exchangeMetadata.LinkedinURL, &exchangeMetadata.RedditURL, &exchangeMetadata.ChatURL, &exchangeMetadata.SlackURL, &exchangeMetadata.TelegramURL, &exchangeMetadata.BlogURL, &exchangeMetadata.Centralized, &exchangeMetadata.Decentralized, &exchangeMetadata.HasTradingIncentive, &exchangeMetadata.TrustScore, &exchangeMetadata.TrustScoreRank, &exchangeMetadata.TradeVolume24HBTC, &exchangeMetadata.TradeVolume24HBTCNormalized, &exchangeMetadata.LastUpdated)
		if err != nil {
			return nil, err
		}
		exchangesMetadata[exchangeMetadata.Name] = exchangeMetadata

	}

	return exchangesMetadata, nil
}
func GetExchangeMetaDataWithoutLimit(ctxO context.Context, labels map[string]string) ([]model.CoingeckoExchangeMetadata, error) {

	ctx, span := tracer.Start(ctxO, "GetExchangeMetaDataWithoutLimit")
	defer span.End()
	startTime := log.StartTimeL(labels, "Exchange Fundamental Insert")

	var exchangesMetadata []model.CoingeckoExchangeMetadata
	pg := PGConnect()

	query := `
	SELECT 
		id,
    name, 
    year, 
    description, 
    location, 
    logo_url, 
		website_url, 
    twitter_url, 
    facebook_url, 
    youtube_url, 
		linkedin_url, 
    reddit_url, 
    chat_url, 
    slack_url, 
		telegram_url, 
    blog_url, 
    centralized, 
    decentralized, 
		has_trading_incentive, 
    trust_score, 
    trust_score_rank, 
		trade_volume_24h_btc, 
    trade_volume_24h_btc_normalized, 
    last_updated
	FROM 
		public.coingecko_exchange_metadata
	where 
		trust_score is not null 
 	order by trust_score desc
	`

	queryResult, err := pg.QueryContext(ctx, query)

	if err != nil {
		log.EndTime("Exchange Metadata Data Query", startTime, err)
		span.SetStatus(otelCodes.Error, err.Error())
		return nil, err
	}

	for queryResult.Next() {
		var exchangeMetadata model.CoingeckoExchangeMetadata

		err := queryResult.Scan(&exchangeMetadata.ID, &exchangeMetadata.Name, &exchangeMetadata.Year, &exchangeMetadata.Description, &exchangeMetadata.Location, &exchangeMetadata.LogoURL, &exchangeMetadata.WebsiteURL, &exchangeMetadata.TwitterURL, &exchangeMetadata.FacebookURL, &exchangeMetadata.YoutubeURL, &exchangeMetadata.LinkedinURL, &exchangeMetadata.RedditURL, &exchangeMetadata.ChatURL, &exchangeMetadata.SlackURL, &exchangeMetadata.TelegramURL, &exchangeMetadata.BlogURL, &exchangeMetadata.Centralized, &exchangeMetadata.Decentralized, &exchangeMetadata.HasTradingIncentive, &exchangeMetadata.TrustScore, &exchangeMetadata.TrustScoreRank, &exchangeMetadata.TradeVolume24HBTC, &exchangeMetadata.TradeVolume24HBTCNormalized, &exchangeMetadata.LastUpdated)
		if err != nil {
			span.SetStatus(otelCodes.Error, err.Error())
			log.EndTime("Exchange Metadata Data Query Scan", startTime, err)
			return nil, err
		}
		exchangesMetadata = append(exchangesMetadata, exchangeMetadata)

	}
	log.EndTime("Exchange Metadata Data Query", startTime, nil)
	span.SetStatus(otelCodes.Ok, "Success")
	return exchangesMetadata, nil
}

func InsertExchangeFundamentals(ctxO context.Context, exchange ExchangeFundamentals, labels map[string]string) error {
	ctx, span := tracer.Start(ctxO, "InsertExchangeFundamentals")
	defer span.End()

	startTime := log.StartTimeL(labels, "Exchange Fundamental Insert")

	pg := PGConnect()

	insertStatementsExchange := "INSERT INTO exchange_fundamentals(name, slug, id, logo, exchange_active_market_pairs, nomics, forbes, last_updated) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"
	// convert Exchanges[] and Nomics into json type to make it easy to store in PG table
	nomics, _ := json.Marshal(exchange.Nomics)
	forbes, _ := json.Marshal(exchange.Forbes)

	_, insertError := pg.ExecContext(ctx, insertStatementsExchange, exchange.Name, exchange.Slug, exchange.Id, exchange.Logo, exchange.ExchangeActiveMarketPairs, nomics, forbes, exchange.LastUpdated)
	if insertError != nil {
		span.SetStatus(otelCodes.Error, insertError.Error())
		log.EndTimeL(labels, "Exchange Fundamental Insert", startTime, insertError)
		return insertError
	}

	log.EndTimeL(labels, "Exchange Fundamental Insert", startTime, nil)
	span.SetStatus(otelCodes.Ok, "Success")
	return nil
}

func InsertExchangeFundamentalsLatest(ctxO context.Context, exchange ExchangeFundamentals, labels map[string]string) error {
	ctx, span := tracer.Start(ctxO, "InsertExchangeFundamentalsLatest")
	defer span.End()

	startTime := log.StartTimeL(labels, "Exchange Fundamental Latest Insert")

	pg := PGConnect()

	insertStatementsFundamentals := "CALL upsert_exchange_fundamentalslatest ($1, $2, $3, $4, $5, $6, $7, $8)"

	query := insertStatementsFundamentals
	// convert  Nomics and forbes into json type to make it easy to store in PG table
	nomics, _ := json.Marshal(exchange.Nomics)
	forbes, _ := json.Marshal(exchange.Forbes)

	_, insertError := pg.ExecContext(ctx, query, exchange.Name, exchange.Slug, exchange.Id, exchange.Logo, exchange.ExchangeActiveMarketPairs, nomics, forbes, exchange.LastUpdated)

	if insertError != nil {
		span.SetStatus(otelCodes.Error, insertError.Error())
		log.EndTimeL(labels, "Exchange Fundamental Insert", startTime, insertError)
		return insertError
	}

	log.EndTimeL(labels, "Exchange Fundamental Insert", startTime, nil)
	span.SetStatus(otelCodes.Ok, "Success")

	return nil
}

func GetExchangeProfilePG(ctxO context.Context, slug string) (*model.CoingeckoExchangeMetadata, error) {

	ctx, span := tracer.Start(ctxO, "GetExchangeProfilePG")
	defer span.End()
	startTime := log.StartTime("Get Exchange Profile Data Query")
	pg := PGConnect()

	query := `
    SELECT
		cem.id, cem.name, ef.slug, cem.year, cem.description, cem.location, cem.logo_url, cem.logo_url as logo,
		cem.website_url, cem.twitter_url, cem.facebook_url, cem.youtube_url, 
		cem.linkedin_url, cem.reddit_url, cem.chat_url, cem.slack_url, cem.telegram_url, 
		cem.blog_url, cem.centralized, cem.decentralized, cem.has_trading_incentive, 
		cem.trust_score, cem.trust_score_rank, cem.trade_volume_24h_btc, 
		cem.trade_volume_24h_btc_normalized, cem.last_updated
	FROM 
		coingecko_exchange_metadata as cem 
	LEFT JOIN 
		exchange_fundamentalslatest as ef
	ON 
		cem.id = ef.id
	WHERE 
		ef.slug = '` + slug + `';
	`
	var exchangeProfile model.CoingeckoExchangeMetadata
	queryResult, err := pg.QueryContext(ctx, query)
	if err != nil {
		span.SetStatus(otelCodes.Error, err.Error())
		log.EndTime("Exchange Profile Data Scan", startTime, err)
		return nil, err
	}
	defer queryResult.Close()
	counter := 0
	for queryResult.Next() {
		counter++
		err := queryResult.Scan(&exchangeProfile.ID, &exchangeProfile.Name, &exchangeProfile.Slug, &exchangeProfile.Year, &exchangeProfile.Description, &exchangeProfile.Location, &exchangeProfile.LogoURL, &exchangeProfile.Logo, &exchangeProfile.WebsiteURL, &exchangeProfile.TwitterURL, &exchangeProfile.FacebookURL, &exchangeProfile.YoutubeURL, &exchangeProfile.LinkedinURL, &exchangeProfile.RedditURL, &exchangeProfile.ChatURL, &exchangeProfile.SlackURL, &exchangeProfile.TelegramURL, &exchangeProfile.BlogURL, &exchangeProfile.Centralized, &exchangeProfile.Decentralized, &exchangeProfile.HasTradingIncentive, &exchangeProfile.TrustScore, &exchangeProfile.TrustScoreRank, &exchangeProfile.TradeVolume24HBTC, &exchangeProfile.TradeVolume24HBTCNormalized, &exchangeProfile.LastUpdated)
		if err != nil {
			span.SetStatus(otelCodes.Error, err.Error())
			log.EndTime("Exchange Profile Data Scan", startTime, err)
			return nil, err
		}
	}
	log.EndTime("Exchange Profile Data Query", startTime, nil)
	if counter == 0 {
		span.SetStatus(otelCodes.Error, "Not Found")
		return nil, nil
	}
	span.SetStatus(otelCodes.Ok, "Success")

	return &exchangeProfile, nil
}

type RelatedAsset struct {
	Symbol   string  `json:"symbol" postgres:"symbol"`
	Name     string  `json:"name" postgres:"name"`
	Slug     string  `json:"slug" postgres:"slug"`
	Price24h float64 `json:"price24h" postgres:"price_24h"`
}

func GetRelatedAssetsForExchangePG(labels map[string]string, ctxO context.Context, exchangeName string) ([]byte, error) {
	ctx, span := tracer.Start(ctxO, "GetRelatedAssetsForExchangePG")
	defer span.End()
	startTime := log.StartTimeL(labels, "Get Related Assets For Exchange Data Query")
	pg := PGConnect()

	query := `
	select 
		name, UPPER(display_symbol) as symbol, slug, price_24h
	from 
		fundamentalslatest
	where 
		$1 = ANY(listed_exchange)
	`

	rows, err := pg.QueryContext(ctx, query, exchangeName)
	if err != nil {
		span.SetStatus(otelCodes.Error, err.Error())
		log.EndTimeL(labels, "Get Related Assets For Exchange Data Query", startTime, err)
		return nil, err
	}
	defer rows.Close()

	relatedAssets := make([]RelatedAsset, 0)
	counter := 0
	for rows.Next() {
		counter++
		var asset RelatedAsset
		err = rows.Scan(
			&asset.Name,
			&asset.Symbol,
			&asset.Slug,
			&asset.Price24h,
		)
		if err != nil {
			span.SetStatus(otelCodes.Error, err.Error())
			log.EndTimeL(labels, "Exchange Profile Data Scan", startTime, err)
			return nil, err
		}
		relatedAssets = append(relatedAssets, asset)
	}
	results, err := json.Marshal(relatedAssets)
	log.EndTimeL(labels, "Get Related Assets For Exchange Data Query", startTime, err)
	if err != nil {
		span.SetStatus(otelCodes.Error, err.Error())
		return nil, err
	}
	span.SetStatus(otelCodes.Ok, "Success")
	return results, nil
}

func OpenCloseResultToAsset(ctxO context.Context, assetIntervals OpenCloseResultArr, asset *OpenCloseAsset) {
	_, span := tracer.Start(ctxO, "OpenCloseResultToAsset")
	defer span.End()
	span.AddEvent("Start OpenClose Results To The Asset")

	for _, item := range assetIntervals {
		intervalArr := strings.Split(item.Interval, "_") //eg item.Interval = 'assetName_1d'
		intervalValue := intervalArr[len(intervalArr)-1] // get last element
		intervalValue = strings.ToLower(intervalValue)   // to lower case
		switch intervalValue {
		case "24h":
			asset.Price24H.Open = item.Open
			asset.Price24H.Close = item.Close
		case "7d":
			asset.Price7D.Open = item.Open
			asset.Price7D.Close = item.Close
		case "30d":
			asset.Price30D.Open = item.Open
			asset.Price30D.Close = item.Close
		case "1y":
			asset.Price1Y.Open = item.Open
			asset.Price1Y.Close = item.Close
		case "max":
			asset.PriceMax.Open = item.Open
			asset.PriceMax.Close = item.Close
		default:
		}
	}

	// We keep the close price of 24h for all other time intervals
	if asset.Price24H.Close != nil {
		asset.Price7D.Close = asset.Price24H.Close
		asset.Price30D.Close = asset.Price24H.Close
		asset.Price1Y.Close = asset.Price24H.Close
		asset.PriceMax.Close = asset.Price24H.Close
	}
	span.SetStatus(otelCodes.Ok, "Success")
}

func GetOpenClosePrice(labels map[string]string, ctxO context.Context) ([]OpenCloseAsset, error) {
	ctx, span := tracer.Start(ctxO, "GetOpenClosePrice")
	defer span.End()
	startTime := log.StartTimeL(labels, "Get OpenClose Price Query")
	pg := PGConnect()
	tableName := "nomics_chart_data"
	query := `
		select 
			symbol,
			json_agg(json_build_object(
				'open', cast(prices::json-> 0 ->>'Price' as float), 
				'close', cast(prices::json-> -1 ->>'Price' as float), 
				'interval', interval
			)) as list
		from ` + tableName + ` 
		where "assetType" = 'FT'
		group by symbol`

	rows, err := pg.QueryContext(ctx, query)
	if err != nil {
		span.SetStatus(otelCodes.Error, err.Error())
		log.EndTimeL(labels, "Get OpenClose Price Query", startTime, err)
		return nil, err
	}
	defer rows.Close()

	openClosePrices := make([]OpenCloseAsset, 0)
	counter := 0
	for rows.Next() {
		counter++
		var asset OpenCloseAsset
		var assetIntervals OpenCloseResultArr
		err = rows.Scan(
			&asset.Symbol,
			&assetIntervals,
		)
		if err != nil {
			span.SetStatus(otelCodes.Error, err.Error())
			log.EndTimeL(labels, "Get OpenClose Price Scan", startTime, err)
			return nil, err
		}
		OpenCloseResultToAsset(ctx, assetIntervals, &asset)
		openClosePrices = append(openClosePrices, asset)
	}
	log.EndTimeL(labels, "Get OpenClose Price Query", startTime, err)
	if err != nil {
		span.SetStatus(otelCodes.Error, err.Error())
		return nil, err
	}
	span.SetStatus(otelCodes.Ok, "Success")
	return openClosePrices, nil
}

// Get Dynamic Description for Traded Assets Page
func GetDynamicDescription(ctx0 context.Context, labels map[string]string) ([]byte, error) {
	// Starts new child span from the parent span in the context.
	ctx, span := tracer.Start(ctx0, "DynamicDescription")
	defer span.End()
	startTime := log.StartTime("Dynamic Description Data")

	pg := PGConnect()

	var globalDescription Global

	// I add this WHERE type = 'FT' because we don't need to mess with the old endpoint for Assets page.
	queryResult, err := pg.QueryContext(ctx, `
		SELECT 
			market_cap, 
			change_24h, 
			volume_24h, 
			dominance, 
			assets_count, 
			trending,
			last_updated
		FROM 
			public.global_description
		Where type = 'FT'
		order by 
			last_updated desc 
		limit 1;
		`)

	span.AddEvent("Query Executed")

	if err != nil {
		log.EndTime("Dynamic Description Data Query", startTime, err)
		span.SetStatus(codes.Error, "Dynamic Description Data from PG")
		return nil, err
	}
	defer queryResult.Close()

	for queryResult.Next() {

		err := queryResult.Scan(&globalDescription.MarketCap, &globalDescription.Change24H, &globalDescription.Volume24H, (*dominance)(&globalDescription.Dominance), &globalDescription.AssetCount, (*trendingResult)(&globalDescription.Trending), &globalDescription.LastUpdated)

		if err != nil {
			log.EndTime("Dynamic Description Data Query", startTime, err)
			span.SetStatus(codes.Error, "Dynamic Description Data Scan error")
			return nil, err
		}

	}
	var dynamicDescription DynamicDescription

	dynamicDescription.Global = globalDescription
	jsonData, err := json.Marshal(dynamicDescription)
	if err != nil {
		return nil, err
	}
	log.EndTime("Dynamic Description Data Query", startTime, nil)

	span.SetStatus(codes.Ok, "success")
	return jsonData, nil
}

// Get list of all the fundamentals values to build the search-dictionaries. This is used for the search functionality
func PGGetSearchAssets(ctx0 context.Context) ([]TradedAssetsTable, error) {

	ctx, span := tracer.Start(ctx0, "PGGetSearchAssets")
	defer span.End()

	startTime := log.StartTime("Search assets Query")
	var assets []TradedAssetsTable

	pg := PGConnect()
	query := fmt.Sprintf(`
		SELECT 
			symbol,
			display_symbol,						  
			name,
			slug,
			logo,
			temporary_data_delay,
			price_24h,
			percentage_1h,
			percentage_24h,
			percentage_7d,
			change_value_24h,						  
			market_cap,
			volume_1d,
			status,
			market_cap_percent_change_1d,
			(case when market_cap = 0 then null when market_cap != 0 then rank_number end) as rank_number
		from public.searchTradedAssetsBySource('%s')
	`, data_source)
	queryResult, err := pg.QueryContext(ctx, query)

	if err != nil {
		span.SetStatus(codes.Error, "PGGetSearchAssets")
		log.EndTime("Search assets Query failed", startTime, err)
		return nil, err
	}
	defer queryResult.Close()

	for queryResult.Next() {
		var tradedAsset TradedAssetsTable
		err := queryResult.Scan(&tradedAsset.Symbol, &tradedAsset.DisplaySymbol, &tradedAsset.Name, &tradedAsset.Slug, &tradedAsset.Logo, &tradedAsset.TemporaryDataDelay, &tradedAsset.Price, &tradedAsset.Percentage1H, &tradedAsset.Percentage, &tradedAsset.Percentage7D, &tradedAsset.ChangeValue, &tradedAsset.MarketCap, &tradedAsset.Volume, &tradedAsset.Status, &tradedAsset.MarketCapPercentage1d, &tradedAsset.Rank)
		if err != nil {
			span.SetStatus(codes.Error, "PGGetSearchAssets scan error")
			log.EndTime("Search assets Query scan", startTime, err)
			return nil, err
		}
		assets = append(assets, tradedAsset)
	}
	log.EndTime("Search assets Query", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return assets, nil
}

// Get all the categories from postgres. This is used for the search by categories functionality
func PGGetCategories(ctx0 context.Context) ([]CategoriesData, error) {

	ctx, span := tracer.Start(ctx0, "PGGetCategories")
	defer span.End()

	startTime := log.StartTime("Get Categories Query")
	var categories []CategoriesData

	pg := PGConnect()

	queryResult, err := pg.QueryContext(ctx, `
		SELECT 
			id,
			name,
			market_cap,
			market_cap_change_24h,
			content,
			top_3_coins,
			volume_24h,
			last_updated,
			markets
		FROM 
			public.getCategories()
		`)

	if err != nil {
		span.SetStatus(codes.Error, "PGGetCategories")
		log.EndTime("Get Categories Query", startTime, err)
		return nil, err
	}
	defer queryResult.Close()

	for queryResult.Next() {
		var category CategoriesData
		err := queryResult.Scan(&category.ID, &category.Name, &category.MarketCap, &category.MarketCapChange24H, &category.Content, pq.Array(&category.Top3Coins), &category.Volume24H, &category.UpdatedAt, (*coinsMarketResultResult)(&category.Markets))
		if err != nil {
			span.SetStatus(codes.Error, "PGGetCategories scan error")
			log.EndTime("Get Categories Query", startTime, err)
			return nil, err
		}
		categories = append(categories, category)
	}
	log.EndTime("Get Categories Query", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return categories, nil
}

// return assets data relevant for building the SEO "Forbes Token metadata" descriptions from fundamentalslatest (Postgres)
func GetAssetsSEOData(ctx0 context.Context, onlyActive bool) (*[]Fundamentals, error) {

	ctx, span := tracer.Start(ctx0, "GetAssetsSEOData")
	defer span.End()

	startTime := log.StartTime("Get Assets SEO data")

	pg := PGConnect()

	var assets []Fundamentals
	queryString := `
	select 
		name, 
		symbol,
		display_symbol,
		slug
	from 
		fundamentalslatest
	`
	if onlyActive {
		queryString += " where status = 'active' "
	}

	queryResult, err := pg.QueryContext(ctx, queryString)

	span.AddEvent("Query Executed")

	if err != nil {
		log.EndTime("Get Assets SEO data Query", startTime, err)
		span.SetStatus(codes.Error, "Get Assets SEO data from PG")
		return nil, err
	}
	defer queryResult.Close()

	for queryResult.Next() {
		var asset Fundamentals

		err := queryResult.Scan(&asset.Name, &asset.Symbol, &asset.DisplaySymbol, &asset.Slug)

		if err != nil {
			log.EndTime("Get Assets SEO data Iterator", startTime, err)
			span.SetStatus(codes.Error, "Get Assets SEO data Scan error")
			return nil, err
		}

		assets = append(assets, asset)
	}

	log.EndTime("Get Assets SEO data Query", startTime, nil)
	span.SetStatus(codes.Ok, "success")

	return &assets, nil
}

// Upsert the latest sales data to nftdata latest
func UpsertNFTSalesData(ctx0 context.Context, nftdata *[]FundamentalsNFTSalesData) error {

	ctx, span := tracer.Start(ctx0, "UpsertNFTSalesData")
	defer span.End()

	pg := PGConnect()

	exchangeListTMP := *nftdata
	valueString := make([]string, 0, len(exchangeListTMP))
	totalFields := 26 //total number of columns in the postgres collection
	valueArgs := make([]interface{}, 0, len(exchangeListTMP)*totalFields)
	var idsInserted []string
	tableName := "nftdatalatest_test1"
	var i = 0 //used for argument positions
	for y := 0; y < len(exchangeListTMP); y++ {
		mult := i * totalFields
		var nftData = exchangeListTMP[y]
		if nftData.ID == "" {
			continue
		}
		idsInserted = append(idsInserted, nftData.ID)

		/**
		* We're generating the insert value string for the postgres query.
		*
		* e.g. Let's say a collection in postgres has 5 columns. Then this looks something like this
		* ($1,$2,$3,$4,$5),($6,$7,$8,$9,$10),(..)...
		*
		* and so on. We use these variables in the postgres query builder. In our case, we currently have 46 columns in the collection.
		 */
		var valString = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)", mult+1, mult+2, mult+3, mult+4, mult+5, mult+6, mult+7, mult+8, mult+9, mult+10, mult+11, mult+12, mult+13, mult+14, mult+15, mult+16, mult+17, mult+18, mult+19, mult+20, mult+21, mult+22, mult+23, mult+24, mult+25, mult+26)
		valueString = append(valueString, valString)

		// Please note that the order of the following appending values matter. We map the following values to the 46 variables defined in the couple of lines defined above.
		valueArgs = append(valueArgs, nftData.ID)
		valueArgs = append(valueArgs, nftData.AVGFloorPrice1d.Float64)
		valueArgs = append(valueArgs, nftData.AVGFloorPrice7d.Float64)
		valueArgs = append(valueArgs, nftData.AVGFloorPrice30d.Float64)
		valueArgs = append(valueArgs, nftData.AVGFloorPrice90d.Float64)
		valueArgs = append(valueArgs, nftData.AVGFloorPriceYtd.Float64)
		valueArgs = append(valueArgs, nftData.AVGSalePrice1d.Float64)
		valueArgs = append(valueArgs, nftData.AVGSalePrice7d.Float64)
		valueArgs = append(valueArgs, nftData.AVGSalePrice30d.Float64)
		valueArgs = append(valueArgs, nftData.AVGSalePrice90d.Float64)
		valueArgs = append(valueArgs, nftData.AVGSalePriceYtd.Float64)
		valueArgs = append(valueArgs, nftData.AVGSalesPriceChange1d.Float64)
		valueArgs = append(valueArgs, nftData.AVGSalesPriceChange7d.Float64)
		valueArgs = append(valueArgs, nftData.AVGSalesPriceChange30d.Float64)
		valueArgs = append(valueArgs, nftData.AVGSalesPriceChange90d.Float64)
		valueArgs = append(valueArgs, nftData.AVGSalesPriceChangeYtd.Float64)
		valueArgs = append(valueArgs, nftData.TotalSales1d.Float64)
		valueArgs = append(valueArgs, nftData.TotalSales7d.Float64)
		valueArgs = append(valueArgs, nftData.TotalSales30d.Float64)
		valueArgs = append(valueArgs, nftData.TotalSales90d.Float64)
		valueArgs = append(valueArgs, nftData.TotalSalesYtd.Float64)
		valueArgs = append(valueArgs, nftData.AvgTotalSalesPctChange1d.Float64)
		valueArgs = append(valueArgs, nftData.AvgTotalSalesPctChange7d.Float64)
		valueArgs = append(valueArgs, nftData.AvgTotalSalesPctChange30d.Float64)
		valueArgs = append(valueArgs, nftData.AvgTotalSalesPctChange90d.Float64)
		valueArgs = append(valueArgs, nftData.AvgTotalSalesPctChangeYtd.Float64)
		i++

		if len(valueArgs) >= 65000 || y == len(exchangeListTMP)-1 {
			insertStatementCoins := fmt.Sprintf(`INSERT INTO %s (
				id,
				avg_floor_price_1d,
				avg_floor_price_7d,
				avg_floor_price_30d,
				avg_floor_price_90d,
				avg_floor_price_ytd,
				avg_sale_price_1d,
				avg_sale_price_7d,
				avg_sale_price_30d,
				avg_sale_price_90d,
				avg_sale_price_ytd,
				avg_total_sales_pct_change_1d,
				avg_total_sales_pct_change_7d,
				avg_total_sales_pct_change_30d,
				avg_total_sales_pct_change_90d,
				avg_total_sales_pct_change_ytd,
				total_sales_1d,
				total_sales_7d,
				total_sales_30d,
				total_sales_90d,
				total_sales_ytd,
				avg_sales_price_change_1d,
				avg_sales_price_change_7d,
				avg_sales_price_change_30d,
				avg_sales_price_change_90d,
				avg_sales_price_change_ytd
			) VALUES %s`, tableName, strings.Join(valueString, ","))

			//only update urls(metadata)
			updateStatement := `ON CONFLICT (id) DO UPDATE SET  avg_floor_price_1d = EXCLUDED.avg_floor_price_1d,
			avg_floor_price_7d = EXCLUDED.avg_floor_price_7d,
			avg_floor_price_30d = EXCLUDED.avg_floor_price_30d,
			avg_floor_price_90d = EXCLUDED.avg_floor_price_90d,
			avg_floor_price_ytd = EXCLUDED.avg_floor_price_ytd,
			avg_sale_price_1d = EXCLUDED.avg_sale_price_1d,
			avg_sale_price_7d = EXCLUDED.avg_sale_price_7d,
			avg_sale_price_30d = EXCLUDED.avg_sale_price_30d,
			avg_sale_price_90d = EXCLUDED.avg_sale_price_90d,
			avg_sale_price_ytd = EXCLUDED.avg_sale_price_ytd,
			avg_total_sales_pct_change_1d = EXCLUDED.avg_total_sales_pct_change_1d,
			avg_total_sales_pct_change_7d = EXCLUDED.avg_total_sales_pct_change_7d,
			avg_total_sales_pct_change_30d = EXCLUDED.avg_total_sales_pct_change_30d,
			avg_total_sales_pct_change_90d = EXCLUDED.avg_total_sales_pct_change_90d,
			avg_total_sales_pct_change_ytd = EXCLUDED.avg_total_sales_pct_change_ytd,
			total_sales_1d = EXCLUDED.total_sales_1d,
			total_sales_7d = EXCLUDED.total_sales_7d,
			total_sales_30d = EXCLUDED.total_sales_30d,
			total_sales_90d = EXCLUDED.total_sales_90d,
			total_sales_ytd = EXCLUDED.total_sales_ytd,
			avg_sales_price_change_1d = EXCLUDED.avg_sales_price_change_1d,
			avg_sales_price_change_7d = EXCLUDED.avg_sales_price_change_7d,
			avg_sales_price_change_30d = EXCLUDED.avg_sales_price_change_30d,
			avg_sales_price_change_90d = EXCLUDED.avg_sales_price_change_90d,
			avg_sales_price_change_ytd = EXCLUDED.avg_sales_price_change_ytd;`

			query := insertStatementCoins + updateStatement
			_, inserterError := pg.ExecContext(ctx, query, valueArgs...)

			if inserterError != nil {
				log.Error(" UpsertNFTSalesData Insertion error %v", inserterError)
			}

			valueString = make([]string, 0, len(exchangeListTMP))
			valueArgs = make([]interface{}, 0, len(exchangeListTMP)*totalFields)

			i = 0
		}
	}
	return nil
}

// It will retrieve all NFTs data from postgres
func PGGetNFTPrices(ctx0 context.Context) ([]NFTPrices, error) {

	ctx, span := tracer.Start(ctx0, "PGGetNFTPrices")
	defer span.End()

	startTime := log.StartTime("NFT Prices Query")
	var nfts []NFTPrices
	pg := PGConnect()
	query := `SELECT 
			id,
			contract_address,
			asset_platform_id,
			name,
			symbol,
			symbol as display_symbol,
			image,
			description,
			native_currency,
			floor_price_usd,
			market_cap_usd,
			volume_24h_usd,
			floor_price_native,
			market_cap_native,
			volume_24h_native,
			floor_price_in_usd_24h_percentage_change,
			volume_24h_percentage_change_usd,
			number_of_unique_addresses,
			number_of_unique_addresses_24h_percentage_change,
			slug,
			total_supply,
			last_updated,
			count(id) OVER() AS full_count
		FROM 
			public.nftdatalatest
		where volume_24h_percentage_change_usd is not null`
	queryResult, err := pg.QueryContext(ctx, query)

	if err != nil {
		log.EndTime("NFT Prices Query", startTime, err)
		return nil, err
	}
	defer queryResult.Close()

	for queryResult.Next() {
		var nft NFTPrices
		err := queryResult.Scan(&nft.ID, &nft.ContractAddress, &nft.AssetPlatformId, &nft.Name, &nft.Symbol, &nft.DisplaySymbol, &nft.Image, &nft.Description, &nft.NativeCurrency, &nft.FloorPriceUsd, &nft.MarketCapUsd, &nft.Volume24hUsd, &nft.FloorPriceNative, &nft.MarketCapNative, &nft.Volume24hNative, &nft.FloorPriceInUsd24hPercentageChange, &nft.Volume24hPercentageChangeUsd, &nft.NumberOfUniqueAddresses, &nft.NumberOfUniqueAddresses24hPercentageChange, &nft.Slug, &nft.TotalSupply, &nft.LastUpdated, &nft.FullCount)
		if err != nil {
			log.EndTime("NFT Prices Query", startTime, err)
			return nil, err
		}
		nfts = append(nfts, nft)
	}
	return nfts, nil
}

// Get Global Dynamic Description for NFTs or Assets Page depends on descriptionType you pass to the function
func GetDynamicDescriptionFromType(ctx0 context.Context, labels map[string]string, descriptionType string) ([]byte, error) {
	// Starts new child span from the parent span in the context.
	ctx, span := tracer.Start(ctx0, "GetDynamicDescriptionFromType")
	defer span.End()
	startTime := log.StartTime("Dynamic Description Data")

	pg := PGConnect()

	var globalDescription Global

	query := fmt.Sprintf(`
	SELECT 
		market_cap, 
		change_24h, 
		volume_24h, 
		dominance, 
		assets_count, 
		trending,
		last_updated
	FROM 
		public.global_description
	where type = $1
	order by 
		last_updated desc 
	limit 1;
	`)
	queryResult, err := pg.QueryContext(ctx, query, descriptionType)

	span.AddEvent("Query Executed")

	if err != nil {
		log.EndTime("Dynamic Description Data Query", startTime, err)
		span.SetStatus(codes.Error, "Dynamic Description Data from PG")
		return nil, err
	}
	defer queryResult.Close()

	for queryResult.Next() {

		err := queryResult.Scan(&globalDescription.MarketCap, &globalDescription.Change24H, &globalDescription.Volume24H, (*dominance)(&globalDescription.Dominance), &globalDescription.AssetCount, (*trendingResult)(&globalDescription.Trending), &globalDescription.LastUpdated)

		if err != nil {
			log.EndTime("Dynamic Description Data Query", startTime, err)
			span.SetStatus(codes.Error, "Dynamic Description Data Scan error")
			return nil, err
		}

	}
	var dynamicDescription DynamicDescription

	dynamicDescription.Global = globalDescription
	jsonData, err := json.Marshal(dynamicDescription)
	if err != nil {
		return nil, err
	}
	log.EndTime("Dynamic Description Data Query", startTime, nil)

	span.SetStatus(codes.Ok, "success")
	return jsonData, nil
}

func CheckTopicAssets(ctxO context.Context, name string) (*FundamentalsData, error) {
	// Starts new child span from the parent span in the context.
	ctx, span := tracer.Start(ctxO, "CheckTopicAssets", trace.WithAttributes(attribute.String("name", name)))
	defer span.End()

	startTime := log.StartTime("Check Topic Assets Query")

	pg := PGConnect()
	query := `
	SELECT 
		symbol,
		name,
		slug
	FROM 
		public.fundamentalslatest
	where 
		name = '` + name + `'
		 `
	var fundamentals FundamentalsData

	queryResult, err := pg.QueryContext(ctx, query)
	span.AddEvent("Query Executed")

	if err != nil {

		log.EndTime("Check Topic Assets Query", startTime, err)
		span.SetStatus(codes.Error, "unable to get data for name from PG")
		return nil, err

	}

	for queryResult.Next() {
		err := queryResult.Scan(&fundamentals.Symbol, &fundamentals.Name, &fundamentals.Slug)
		if err != nil {
			log.EndTime("Check Topic Assets Query", startTime, err)
			return nil, err
		}
	}
	log.EndTime("Check Topic Assets Query", startTime, nil)

	span.SetStatus(codes.Ok, "success")

	return &fundamentals, nil
}

func sanitizeString(data string) string {
	sanitized := template.HTMLEscapeString(data)
	return sanitized
}

type Categories struct {
	ID          string   `json:"id" postgres:"id"`                     // ID of the category
	Name        string   `json:"name" postgres:"name"`                 // Name of the category
	TotalTokens int      `json:"total_tokens" postgres:"total_tokens"` // Total Tokens  of the category
	Coins       []string `json:"coins" postgres:"coins"`               // Array of coins for the category                           // List of all the assets in the category
}

func GetCategories(ctx0 context.Context) ([]Categories, error) {
	pg := PGConnect()

	ctx, span := tracer.Start(ctx0, "GetCategories")

	defer span.End()
	startTime := log.StartTime("Get Categories Query")

	var categories []Categories
	span.AddEvent("Start Getting Categories")
	queryResult, err := pg.QueryContext(ctx, `
	select 
		id, 
		name, 
		count(markets ->> 'id') as total_tokens, 
		array_agg(markets ->> 'id') as coins
	from (
			select json_array_elements(markets) as markets, id, name
			from coingecko_categories
		) as foo
	group by id, name
	`)

	if err != nil {
		span.SetStatus(codes.Error, "PGGetCategories")
		log.EndTime("Get Categories Query", startTime, err)
		return nil, err
	}
	defer queryResult.Close()

	for queryResult.Next() {
		var category Categories
		err := queryResult.Scan(&category.ID, &category.Name, &category.TotalTokens, pq.Array(&category.Coins))
		if err != nil {
			span.SetStatus(codes.Error, "PGGetCategories scan error")
			log.EndTime("Get Categories Query", startTime, err)
			return nil, err
		}
		categories = append(categories, category)
	}
	log.EndTime("Get Categories Query", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return categories, nil
}
