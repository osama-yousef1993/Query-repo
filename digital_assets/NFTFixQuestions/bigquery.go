package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/Forbes-Media/forbes-digital-assets/model"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	otelCodes "go.opentelemetry.io/otel/codes"
	"google.golang.org/api/iterator"
)

const (
	BQDataset = "digital_assets"
	BQTable   = "cq_ohlcv"
)

var (
	once    sync.Once
	bqStore *BQStore
)

type TimeSeriesResult struct {
	Symbol                  string  `bigquery:"sym" json:"symbol" firestore:"symbol"`
	TargetResolutionSeconds int     `json:"targetResolutionSeconds"`
	Slice                   []Slice `bigquery:"slice" json:"prices" firestore:"prices"`
	IsIndex                 bool    `json:"isIndex"`
	Source                  string  `json:"source"`
}

type Slice struct {
	Time     time.Time `bigquery:"time" json:"x" firestore:"x"`
	AvgClose float64   `bigquery:"price" json:"y" firestore:"y"`
}

type FundamentalsResult struct {
	Symbol         string               `bigquery:"symbol" json:"symbol"`
	ForbesSymbol   string               `bigquery:"forbes" json:"forbesSymbol"`
	Volume24H      bigquery.NullFloat64 `bigquery:"volume_24h" json:"volume24h"`
	High           bigquery.NullFloat64 `bigquery:"high_24h" json:"high24h"`
	Low            bigquery.NullFloat64 `bigquery:"low_24h" json:"low24h"`
	High7D         bigquery.NullFloat64 `bigquery:"high_7d" json:"high7d"`
	Low7D          bigquery.NullFloat64 `bigquery:"low_7d" json:"low7d"`
	High30D        bigquery.NullFloat64 `bigquery:"high_30d" json:"high30d"`
	Low30D         bigquery.NullFloat64 `bigquery:"low_30d" json:"low30d"`
	High1Y         bigquery.NullFloat64 `bigquery:"high_1y" json:"high1y"`
	Low1Y          bigquery.NullFloat64 `bigquery:"low_1y" json:"low1y"`
	HighYtd        bigquery.NullFloat64 `bigquery:"high_ytd" json:"highYtd"`
	LowYtd         bigquery.NullFloat64 `bigquery:"low_ytd" json:"lowYtd"`
	AllTimeHigh    bigquery.NullFloat64 `bigquery:"all_time_high" json:"allTimeHigh"`
	AllTimeLow     bigquery.NullFloat64 `bigquery:"all_time_low" json:"allTimeLow"`
	LastClosePrice bigquery.NullFloat64 `bigquery:"last_close_price" json:"lastClosePrice"`
	FirstOpenPrice bigquery.NullFloat64 `bigquery:"first_open_price" json:"firstOpenPrice"`
	MarketCap      bigquery.NullString  `bigquery:"market_cap" json:"marketCap"`
	Supply         bigquery.NullString  `bigquery:"supply" json:"supply"`
	Date           time.Time            `bigquery:"last_price_time" json:"lastPriceTime"`
	Exchanges      []Exchange           `bigquery:"exchanges" json:"exchanges"`
}

type ChartData struct {
	Symbol     string               `bigquery:"symbol" json:"symbol"`
	Forbes     string               `bigquery:"forbes" json:"forbes"`
	Time       time.Time            `bigquery:"time" json:"time"`
	Price      bigquery.NullFloat64 `bigquery:"price" json:"price"`
	DataSource string               `bigquery:"dataSource" json:"dataSource"`
}

type Exchange struct {
	Market string    `bigquery:"Market" json:"market"`
	Symbol string    `bigquery:"Symbol" json:"symbol"`
	Time   time.Time `bigquery:"Time" json:"time"`
	Close  float64   `bigquery:"Close" json:"close"`
	Slug   string    `json:"slug" firestore:"slug"`
}

type BQStore struct {
	*bigquery.Client
}

// CoinsMarketResult represents the data structure for a cryptocurrency market result.
// It holds various properties related to a specific cryptocurrency including its ID, name, market data, and additional attributes.
type CoinsMarketResult struct {
	ForbesID          string               `bigquery:"forbes_id" json:"forbes_id"`                  // Unique identifier for the cryptocurrency from Forbes.
	ID                string               `bigquery:"ID" json:"id"`                                // Unique identifier for the cryptocurrency in the database.
	Name              string               `bigquery:"Name" json:"name"`                            // The name of the cryptocurrency (e.g., Bitcoin, Ethereum).
	Symbol            string               `bigquery:"Symbol" json:"symbol"`                        // The symbol of the cryptocurrency (e.g., BTC, ETH).
	Price             bigquery.NullFloat64 `bigquery:"Price" json:"price"`                          // The current price of the cryptocurrency, may be null if not available.
	CirculatingSupply bigquery.NullFloat64 `bigquery:"CirculatingSupply" json:"circulating_supply"` // The circulating supply of the cryptocurrency, may be null if not available.
	MaxSupply         bigquery.NullFloat64 `bigquery:"MaxSupply" json:"max_supply"`                 // The maximum supply of the cryptocurrency, may be null if not available.
	MarketCap         bigquery.NullFloat64 `bigquery:"MarketCap" json:"market_cap"`                 // The market capitalization of the cryptocurrency, may be null if not available.
	Volume            bigquery.NullFloat64 `bigquery:"volume" json:"volume"`                        // The 24-hour trading volume of the cryptocurrency, may be null if not available.
	QuoteCurrency     string               `bigquery:"QuoteCurrency" json:"quote_currency"`         // The currency used for quoting the cryptocurrency (e.g., USD, EUR).
	SOURCE            string               `bigquery:"SOURCE" json:"source"`                        // The source from which the market data is fetched (e.g., API provider).
	OccurrenceTime    time.Time            `bigquery:"OccurrenceTime" json:"occurrence_time"`       // The timestamp of when the market data was recorded.
	Status            string               `bigquery:"status" json:"status"`                        // The current status of the cryptocurrency (e.g., active, inactive).
}

type ChatbotData struct {
	ID                        string `bigquery:"id" json:"id,omitempty"`
	ForbesMetadataDescription string `bigquery:"forbes_metadata_description" json:"forbes_metadata_description,omitempty"`
	DisplaySymbol             string `bigquery:"display_symbol" json:"display_symbol,omitempty"`
	Name                      string `bigquery:"name" json:"name,omitempty"`
	Slug                      string `bigquery:"slug" json:"slug,omitempty"`
}

type MarketCapResult struct {
	ID                       string  `bigquery:"ID" json:"id"`
	MarketCapPercentChange1D float64 `bigquery:"marketCapPercentChange1D" json:"market_cap_percent_change_1d"`
	VolumePercentChange1D    float64 `bigquery:"volumePercentChange1D" json:"volume_percent_change_1d"`
}

type ExchangeResults struct {
	Name                      string  `json:"name" postgres:"name" bigquery:"name"`
	Slug                      string  `json:"slug" postgres:"slug" bigquery:"slug"`
	Id                        string  `json:"id" postgres:"id" bigquery:"id"`
	Logo                      string  `json:"logo" postgres:"logo" bigquery:"logo"`
	ExchangeActiveMarketPairs int     `json:"exchange_active_market_pairs" postgres:"exchange_active_market_pairs" bigquery:"exchange_active_market_pairs"`
	VolumeByExchange1D        float64 `json:"volume_by_exchange_1d" postgres:"volume_by_exchange_1d" bigquery:"volume_by_exchange_1d"`
}

type NFTPriceType string

const (
	FloorPriceUSD    NFTPriceType = "floorprice_usd"
	FloorPriceNative NFTPriceType = "floorprice_native"
)

type BQTimeInterval string

const (
	BQ_OneDay    BQTimeInterval = "24 HOUR"
	BQ_SevenDay  BQTimeInterval = "7 DAY"
	BQ_ThirtyDay BQTimeInterval = "30 DAY"
	BQ_OneYear   BQTimeInterval = "365 DAY"
	BQ_Max       BQTimeInterval = "2555 DAY"
)

type ChartQueryResSeconds string

const (
	ResSeconds_900     ChartQueryResSeconds = "900"
	ResSeconds_14400   ChartQueryResSeconds = "14400"
	ResSeconds_43200   ChartQueryResSeconds = "43200"
	ResSeconds_432000  ChartQueryResSeconds = "432000"
	ResSeconds_1296000 ChartQueryResSeconds = "1296000"
)

// SearchData represents the structure of a search record, including the search term, the results, and the timestamp of when the search was created.
type SearchData struct {
	// SearchTerm holds the term that was searched for.
	SearchTerm string `bigquery:"search_term" json:"search_term"`

	// Results contains the data or outcome of the search query.
	Results string `bigquery:"results" json:"results"`

	// CreatedAt is the timestamp when the search was initiated.
	CreatedAt time.Time `bigquery:"created_at" json:"created_at"`
}

// creating BQ client and sync it using sync.Once instead of creating it everytime we call the function
func NewBQStore() (*BQStore, error) {
	if bqStore == nil {
		once.Do(func() {
			client, err := bigquery.NewClient(context.Background(), "api-project-901373404215")
			if err != nil {
				log.Error("%s", err)
			}
			var bqs BQStore
			bqs.Client = client
			bqStore = &bqs
		})
	}
	return bqStore, nil
}

func BQClose() {
	if bqStore != nil {
		bqStore.Close()
	}
}

// query fundamentals data from BQ  by calculating the needed data inside the query for the assets the we support
func (bq *BQStore) QueryFundamentals24h() ([]FundamentalsResult, error) {
	ctx := context.Background()

	candlesTable := GetTableName("nomics_ohlcv_candles")
	marketTicketTable := GetTableName("nomics_exchange_market_ticker")

	query := bq.Query(`

	WITH oneyear as (
		SELECT 
		  MAX(Close) as high_1y, 
		  MIN(Close) as low_1y, 
		  symbol 
		FROM 
		  (
			SELECT 
			  b.Close, 
			  a.symbol 
			FROM 
			  (
				SELECT 
				  approx_quantiles(close, 4) as quantiles, 
				  approx_quantiles(close, 4) [offset(3) ] + (
					1.5 * (
					  approx_quantiles(close, 4) [offset(3) ] - approx_quantiles(close, 4) [offset(1) ]
					)
				  ) as upperfence, 
				  approx_quantiles(close, 4) [offset(1) ] - (
					1.5 * (
					  approx_quantiles(close, 4) [offset(3) ] - approx_quantiles(close, 4) [offset(1) ]
					)
				  ) as lowerfence, 
				  Base as symbol 
				FROM 
				  (
					SELECT 
					  AVG(close) close, 
					  Base, 
					  timestamp 
					FROM 
						api-project-901373404215.digital_assets.` + candlesTable + ` c 
					WHERE 
					  timestamp >= TIMESTAMP_SUB(
						CURRENT_TIMESTAMP(),
						Interval 365 DAY
					  ) 
					GROUP BY 
					  base, 
					  timestamp
				  ) 
				GROUP BY 
				  base
			  ) a  --IN "a" we work on calculating the upper fence an lower fence of all outliers 
			  Join (
				SELECT 
				  AVG(
					DISTINCT(Close)
				  ) Close, 
				  Base as symbol, 
				  Timestamp, 
				FROM 
					api-project-901373404215.digital_assets.` + candlesTable + ` c 
				WHERE 
				  Timestamp >= TIMESTAMP_SUB(
					CURRENT_TIMESTAMP(), 
					INTERVAL 365 DAY
				  ) 
				GROUP BY 
				  Base, 
				  Timestamp
			  ) b on ( --in B we Pull all of the the assets within a time frame
				b.symbol = a.symbol 
				AND (
				  b.Close BETWEEN a.lowerfence 
				  AND a.upperfence
				) --We then Join a on b, but only include closes that reside between the upper and lower fence
			  )
		  ) 
		GROUP BY 
		  symbol
	  ), 
	  thirtydays as (
		SELECT 
		  MAX(Close) as high_30d, 
		  MIN(Close) as low_30d, 
		  symbol 
		FROM 
		  (
			SELECT 
			  b.Close, 
			  a.symbol 
			FROM 
			  (
				SELECT 
				  approx_quantiles(close, 4) as quantiles, 
				  approx_quantiles(close, 4) [offset(3) ] + (
					1.5 * (
					  approx_quantiles(close, 4) [offset(3) ] - approx_quantiles(close, 4) [offset(1) ]
					)
				  ) as upperfence, 
				  approx_quantiles(close, 4) [offset(1) ] - (
					1.5 * (
					  approx_quantiles(close, 4) [offset(3) ] - approx_quantiles(close, 4) [offset(1) ]
					)
				  ) as lowerfence, 
				  Base as symbol 
				FROM 
				  (
					SELECT 
					  AVG(close) close, 
					  Base, 
					  timestamp 
					FROM 
						api-project-901373404215.digital_assets.` + candlesTable + ` c 
					WHERE 
					  timestamp >= TIMESTAMP_SUB(
						CURRENT_TIMESTAMP(), 
						Interval 30 DAY
					  ) 
					GROUP BY 
					  base, 
					  timestamp
				  ) 
				GROUP BY 
				  base
			  ) a 
			  JOIN (
				SELECT 
				  AVG(
					DISTINCT(Close)
				  ) Close, 
				  Base as symbol, 
				  Timestamp, 
				FROM 
					api-project-901373404215.digital_assets.` + candlesTable + ` c 
				WHERE 
				  Timestamp >= TIMESTAMP_SUB(
					CURRENT_TIMESTAMP(), 
					INTERVAL 30 DAY
				  ) 
				GROUP BY 
				  Base, 
				  Timestamp
			  ) b on (
				b.symbol = a.symbol 
				AND (
				  b.Close BETWEEN a.lowerfence 
				  AND a.upperfence
				)
			  )
		  ) 
		GROUP BY 
		  symbol
	  ), 
	  sevendays as (
		SELECT 
		  MAX(Close) as high_7d, 
		  MIN(Close) as low_7d, 
		  symbol 
		FROM 
		  (
			SELECT 
			  b.Close, 
			  a.symbol 
			FROM 
			  (
				SELECT 
				  approx_quantiles(close, 4) as quantiles, 
				  approx_quantiles(close, 4) [offset(3) ] + (
					1.5 * (
					  approx_quantiles(close, 4) [offset(3) ] - approx_quantiles(close, 4) [offset(1) ]
					)
				  ) as upperfence, 
				  approx_quantiles(close, 4) [offset(1) ] - (
					1.5 * (
					  approx_quantiles(close, 4) [offset(3) ] - approx_quantiles(close, 4) [offset(1) ]
					)
				  ) as lowerfence, 
				  Base as symbol 
				FROM 
				  (
					SELECT 
					  AVG(close) close, 
					  Base, 
					  timestamp 
					FROM 
						api-project-901373404215.digital_assets.` + candlesTable + ` c 
					WHERE 
					  timestamp >= TIMESTAMP_SUB(
						CURRENT_TIMESTAMP(), 
						Interval 7 DAY
					  ) 
					GROUP BY 
					  base, 
					  timestamp
				  ) 
				GROUP BY 
				  base
			  ) a 
			  JOIN (
				SELECT 
				  AVG(
					DISTINCT(Close)
				  ) Close, 
				  Base as symbol, 
				  Timestamp, 
				FROM 
					api-project-901373404215.digital_assets.` + candlesTable + ` c 
				WHERE 
				  Timestamp >= TIMESTAMP_SUB(
					CURRENT_TIMESTAMP(), 
					INTERVAL 7 DAY
				  ) 
				GROUP BY 
				  Base, 
				  Timestamp
			  ) b on (
				b.symbol = a.symbol 
				AND (
				  b.Close BETWEEN a.lowerfence 
				  AND a.upperfence
				)
			  )
		  ) 
		GROUP BY 
		  symbol
	  ), 
	  oneday as (
		SELECT 
		  MAX(Close) as high_1d, 
		  MIN(Close) as low_1d, 
		  symbol 
		FROM 
		  (
			SELECT 
			  b.Close, 
			  a.symbol 
			FROM 
			  (
				SELECT 
				  approx_quantiles(close, 4) as quantiles, 
				  approx_quantiles(close, 4) [offset(3) ] + (
					1.5 * (
					  approx_quantiles(close, 4) [offset(3) ] - approx_quantiles(close, 4) [offset(1) ]
					)
				  ) as upperfence, 
				  approx_quantiles(close, 4) [offset(1) ] - (
					1.5 * (
					  approx_quantiles(close, 4) [offset(3) ] - approx_quantiles(close, 4) [offset(1) ]

					)
				  ) as lowerfence, 
				  Base as symbol 
				FROM 
				  (
					SELECT 

					  AVG(close) close, 
					  Base, 
					  timestamp 
					FROM 
						api-project-901373404215.digital_assets.` + candlesTable + ` c 
					WHERE 
					  timestamp >= TIMESTAMP_SUB(
						CURRENT_TIMESTAMP(), 
						Interval 24 Hour
					  ) 
					GROUP BY 
					  base, 
					  timestamp
				  ) 
				GROUP BY 
				  base
			  ) a 
			  JOIN (
				SELECT 
				  AVG(
					DISTINCT(Close)
				  ) Close, 
				  Base as symbol, 
				  Timestamp, 
				FROM 
					api-project-901373404215.digital_assets.` + candlesTable + ` c 
				WHERE 
				  Timestamp >= TIMESTAMP_SUB(
					CURRENT_TIMESTAMP(), 
					INTERVAL 24 HOUR
				  ) 
				GROUP BY 
				  Base, 
				  Timestamp
			  ) b on (
				b.symbol = a.symbol 
				AND (
				  b.Close BETWEEN a.lowerfence 
				  AND a.upperfence
				)
			  )
		  ) 
		GROUP BY 
		  symbol
	  ), 
	  ExchangesPrices AS (
		SELECT 
		  Base as Base, 
		  Exchange, 
		  avg(Price) as Price, 
		FROM 
			api-project-901373404215.digital_assets.` + marketTicketTable + ` c 
		WHERE 
		  Exchange NOT IN ("bitmex", "hbtc") 
		  AND Timestamp > DATETIME_SUB(
			CURRENT_TIMESTAMP(), 
			INTERVAL 30 MINUTE
		  ) 
		  AND Type = "spot" 
		  AND status = "active" 
		  AND Quote IN ("USD", "USDT", "USDC") 
		GROUP BY 
		  Base, 
		  Exchange
	  ), 
	  allTime AS (
		SELECT 
		  CAST(
			MIN(Close) AS FLOAT64
		  ) all_time_low, 
		  Base as symbol 
		FROM 
		  (
			SELECT 
			  AVG(Close) Close, 
			  Base, 
			FROM 
				api-project-901373404215.digital_assets.` + candlesTable + ` c 
			GROUP BY 
			  Base, 
			  Timestamp
		  ) 
		GROUP BY 
		  Base
	  ) 
	  SELECT 
		ARRAY_AGG(
		  STRUCT(
			Exchange AS Market, 
			Base AS Symbol, 
			CAST(Price AS FLOAT64) AS Close
		  )
		) as Exchanges, 
		CAST(
		  MAX(oneDay.high_1d) AS FLOAT64
		) AS high_24h, 
		CAST(
		  MIN(oneDay.low_1d) AS FLOAT64
		) AS low_24h, 
		CAST(
		  MAX(sevenDays.high_7d) AS FLOAT64
		) AS high_7d, 
		CAST(
		  MIN(sevenDays.low_7d) AS FLOAT64
		) AS low_7d, 
		CAST(
		  MAX(thirtyDays.high_30d) AS FLOAT64
		) AS high_30d, 
		CAST(
		  MIN(thirtyDays.low_30d) AS FLOAT64
		) AS low_30d, 
		CAST(
		  MAX(oneYear.high_1y) AS FLOAT64
		) AS high_1y, 
		CAST(
		  MIN(oneYear.low_1y) AS FLOAT64
		) AS low_1y, 
		CAST(
		  MIN(allTime.all_time_low) AS FLOAT64
		) AS all_time_low, 
		oneday.symbol 
	  FROM 
		oneday 
		INNER JOIN sevendays on (oneday.symbol = sevendays.symbol) 
		INNER JOIN thirtydays on (
		  oneday.symbol = thirtydays.symbol
		) 
		INNER JOIN oneyear on (oneday.symbol = oneyear.symbol) 
		INNER JOIN ExchangesPrices on (
		  oneyear.symbol = ExchangesPrices.Base
		) 
		INNER JOIN alltime on (alltime.symbol = oneday.symbol) 
	  GROUP BY 
		oneday.symbol
	  
	`)

	job, err := query.Run(ctx)
	if err != nil {
		return nil, err
	}

	log.Debug("Fundamentals Query Job ID: %s", job.ID())

	status, err := job.Wait(ctx)
	if err != nil {
		return nil, err
	}
	if err := status.Err(); err != nil {
		return nil, err
	}

	it, err := job.Read(ctx)
	if err != nil {
		return nil, err
	}

	log.Debug("Started")
	var fundamentalsResults []FundamentalsResult
	for {
		var fundamentalsResult FundamentalsResult
		err := it.Next(&fundamentalsResult)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		fundamentalsResults = append(fundamentalsResults, fundamentalsResult)

	}

	return fundamentalsResults, nil
}

// query index histore from BQ
// TODO: needs a rework
func (bq *BQStore) QueryRebalancedIndices() (IndexRebalancing, error) {
	ctx := context.Background()
	query := `
  SELECT
    *
  FROM
    api-project-901373404215.digital_assets.indicesRebalancingData
  WHERE
    IndexName = "da30"
  ORDER BY
    RebalanceTime DESC
  LIMIT
    1
`

	q := bq.Client.Query(query)

	it, err := q.Read(ctx)
	if err != nil {
		return IndexRebalancing{}, err
	}

	var IndexData IndexRebalancing
	for {
		nextErr := it.Next(&IndexData)

		if nextErr == iterator.Done {
			break
		}
		if nextErr != nil {
			return IndexRebalancing{}, nextErr
		}
	}

	return IndexData, nil
}

// query index chart data for 24 hours timespan
// TODO: needs a rework
func (bq *BQStore) QueryIndexDaily() (TimeSeriesResult, error) {
	indexContent, err := bqStore.QueryRebalancedIndices()
	targetResolutionSeconds := 60 * 15

	if err != nil {
		log.Error("Index query error %s", err)
		return TimeSeriesResult{}, err
	}

	var timeSeriesResults []TimeSeriesResult

	for _, item := range indexContent.IndexContent {
		res, chartError := bqStore.QueryChartBySymbolAndTime(item.Symbol, "24 HOUR", targetResolutionSeconds, strconv.Itoa(targetResolutionSeconds))

		timeSeriesResults = append(timeSeriesResults, res)

		if chartError != nil {
			log.Error("Index chart query error %s", err)
		}
	}

	indexChartData, mappingErr := MapIndexChartData(timeSeriesResults, indexContent, targetResolutionSeconds)

	if mappingErr != nil {
		log.Error("Index query error %s", mappingErr)
		return TimeSeriesResult{}, err
	}

	return indexChartData, nil
}

// query index chart data for 7 days timespan
// TODO: needs a rework
func (bq *BQStore) QueryIndex7Days() (TimeSeriesResult, error) {
	indexContent, err := bqStore.QueryRebalancedIndices()
	targetResolutionSeconds := 60 * 60 * 4

	if err != nil {
		log.Error("Index query error %s", err)
		return TimeSeriesResult{}, err
	}

	var timeSeriesResults []TimeSeriesResult

	for _, item := range indexContent.IndexContent {
		res, chartError := bqStore.QueryChartBySymbolAndTime(item.Symbol, "7 DAY", targetResolutionSeconds, strconv.Itoa(targetResolutionSeconds))

		timeSeriesResults = append(timeSeriesResults, res)

		if chartError != nil {
			log.Error("Index chart query error %s", err)
		}
	}

	indexChartData, mappingErr := MapIndexChartData(timeSeriesResults, indexContent, targetResolutionSeconds)

	if mappingErr != nil {
		log.Error("Index query error %s", mappingErr)
		return TimeSeriesResult{}, err
	}

	return indexChartData, nil
}

// query index chart data for 30 days timespan
// TODO: needs a rework
func (bq *BQStore) QueryIndex30Days() (TimeSeriesResult, error) {
	indexContent, err := bqStore.QueryRebalancedIndices()
	targetResolutionSeconds := 60 * 60 * 12

	if err != nil {
		log.Error("Index query error %s", err)
		return TimeSeriesResult{}, err
	}

	var timeSeriesResults []TimeSeriesResult

	for _, item := range indexContent.IndexContent {
		res, chartError := bqStore.QueryChartBySymbolAndTime(item.Symbol, "30 DAY", targetResolutionSeconds, strconv.Itoa(targetResolutionSeconds))

		timeSeriesResults = append(timeSeriesResults, res)

		if chartError != nil {
			log.Error("Index chart query error %s", err)
		}
	}

	indexChartData, mappingErr := MapIndexChartData(timeSeriesResults, indexContent, targetResolutionSeconds)

	if mappingErr != nil {
		log.Error("Index query error %s", mappingErr)
		return TimeSeriesResult{}, err
	}

	return indexChartData, nil
}

// query index chart data for 1 year timespan
// TODO: needs a rework
func (bq *BQStore) QueryIndex1Year() (TimeSeriesResult, error) {
	indexContent, err := bqStore.QueryRebalancedIndices()
	targetResolutionSeconds := 60 * 60 * 24 * 5

	if err != nil {
		log.Error("Index query error %s", err)
		return TimeSeriesResult{}, err
	}

	var timeSeriesResults []TimeSeriesResult

	for _, item := range indexContent.IndexContent {
		res, chartError := bqStore.QueryChartBySymbolAndTime(item.Symbol, "365 DAY", targetResolutionSeconds, strconv.Itoa(targetResolutionSeconds))

		timeSeriesResults = append(timeSeriesResults, res)

		if chartError != nil {
			log.Error("Index chart query error %s", err)
		}
	}

	indexChartData, mappingErr := MapIndexChartData(timeSeriesResults, indexContent, targetResolutionSeconds)

	if mappingErr != nil {
		log.Error("Index query error %s", mappingErr)
		return TimeSeriesResult{}, err
	}

	return indexChartData, nil
}

// query index chart data for 7 years timespan
// TODO: needs a rework
func (bq *BQStore) QueryIndexMax() (TimeSeriesResult, error) {
	indexContent, err := bqStore.QueryRebalancedIndices()
	targetResolutionSeconds := 60 * 60 * 24 * 15

	if err != nil {
		log.Error("Index query error %s", err)
		return TimeSeriesResult{}, err
	}

	var timeSeriesResults []TimeSeriesResult

	for _, item := range indexContent.IndexContent {
		res, chartError := bqStore.QueryChartBySymbolAndTime(item.Symbol, "2555 DAY", targetResolutionSeconds, strconv.Itoa(targetResolutionSeconds))

		timeSeriesResults = append(timeSeriesResults, res)

		if chartError != nil {
			log.Error("Index chart query error %s", err)
		}
	}

	indexChartData, mappingErr := MapIndexChartData(timeSeriesResults, indexContent, targetResolutionSeconds)

	if mappingErr != nil {
		log.Error("Index query error %s", mappingErr)
		return TimeSeriesResult{}, err
	}

	return indexChartData, nil
}

// maps the result from the query
// TODO: needs a rework
func MapIndexChartData(timeSeriesResults []TimeSeriesResult, indexContent IndexRebalancing, targetResolutionSeconds int) (TimeSeriesResult, error) {
	var timeSlice []time.Time

	for _, item := range timeSeriesResults[0].Slice {
		timeSlice = append(timeSlice, item.Time)
	}

	var indexChartSlice []Slice
	var indexChartData TimeSeriesResult
	for _, item := range timeSlice {
		indexPrice := 0.0
		var slice Slice
		for _, asset := range indexContent.IndexContent {
			priceByTime := GetPriceByTime(item, timeSeriesResults, asset.Symbol)
			indexPrice += priceByTime * asset.Weight
		}

		slice.Time = item
		slice.AvgClose = indexPrice

		indexChartSlice = append(indexChartSlice, slice)
	}

	indexChartData.Symbol = "da-30"
	indexChartData.Slice = indexChartSlice
	indexChartData.TargetResolutionSeconds = targetResolutionSeconds
	indexChartData.IsIndex = true

	return indexChartData, nil
}

// gets the price by time to calculate the index value
// TODO: needs a rework
func GetPriceByTime(time time.Time, timeSeriesResult []TimeSeriesResult, symbol string) float64 {
	var slice []Slice
	for _, item := range timeSeriesResult {
		if item.Symbol == symbol {
			slice = item.Slice
			break
		}
	}

	for _, item := range slice {
		if item.Time == time {
			return item.AvgClose
		}
	}

	return 0.0
}

func (bq *BQStore) buildChartQuery(interval BQTimeInterval, stringResolutionSeconds ChartQueryResSeconds) *bigquery.Query {
	var source = os.Getenv("DATASOURCE")
	var query *bigquery.Query
	var candlesTable string
	switch source {
	case "coingecko":
		candlesTable = fmt.Sprintf("Digital_Asset_MarketData%s", os.Getenv("DATA_NAMESPACE"))
		query = bq.Query(`
		SELECT
			forbes_id,
			Symbol as symbol,
			ARRAY_AGG(STRUCT('Time',time, 'Price', price)) as beprices
		FROM
		(
			SELECT
				forbes_id,
				ID symbol,
				Occurance_Time as time,
				CAST(AVG(Price) AS FLOAT64) price,
				ROW_NUMBER() OVER (PARTITION BY ID, CAST(FLOOR(UNIX_SECONDS(Occurance_Time)/ (` + string(stringResolutionSeconds) + ` ))AS INT64 )  ORDER BY Occurance_Time) as row_num
			FROM
				api-project-901373404215.digital_assets.` + candlesTable + ` c 
			WHERE
				Occurance_Time >= TIMESTAMP_SUB(
				CURRENT_TIMESTAMP(), 
				INTERVAL ` + string(interval) + `
			  ) 
			GROUP BY
				Occurance_Time,
				ID,
				forbes_id
			order by Occurance_Time
		) as test
		where
			row_num = 1
			and forbes_id is not null
		GROUP BY
			forbes_id,
			symbol
		`)

	case "nomics":
		candlesTable = fmt.Sprintf("nomics_ohlcv_candles%s", os.Getenv("DATA_NAMESPACE"))
		query = bq.Query(`
		SELECT
			Symbol as symbol,
			ARRAY_AGG(STRUCT('Time',time, 'Price', price)) as beprices
		FROM
		(
			SELECT
				base symbol,
				timestamp as time,
				CAST(AVG(close) AS FLOAT64) price,
				ROW_NUMBER() OVER (PARTITION BY base, CAST(FLOOR(UNIX_SECONDS(timestamp)/ (` + string(stringResolutionSeconds) + ` ))AS INT64 )  ORDER BY timestamp) as row_num
			FROM
				api-project-901373404215.digital_assets.` + candlesTable + ` c 
			WHERE
			 Timestamp >= TIMESTAMP_SUB(
				CURRENT_TIMESTAMP(), 
				INTERVAL ` + string(interval) + `
			  ) 
			GROUP BY
				timestamp,
				base
			order by timestamp
		) as test
		where
		row_num = 1
		GROUP BY
		symbol
		`)
	}
	return query

}

// buildCategoriesChartQuery to build Categories charts for each interval
// Take (interval, stringResolutionSeconds)
// return the query that we need to execute so we can get the Data to build the Categories Chart Data.
func (bq *BQStore) buildCategoriesChartQuery(interval BQTimeInterval, stringResolutionSeconds ChartQueryResSeconds) *bigquery.Query {
	candlesTable := GetTableName("Digital_Assets_Categories_Historical_data")
	query := bq.Query(`
		SELECT
			Symbol as symbol,
			ARRAY_AGG(STRUCT('Time',time, 'marketCap_usd',marketCap_usd )) as beprices
		FROM
		(
			SELECT
				ID symbol,
				Date as time,
				CAST(AVG(market_cap_24h) AS FLOAT64) marketCap_usd,
				ROW_NUMBER() OVER (PARTITION BY ID, CAST(FLOOR(UNIX_SECONDS(Date)/  (` + string(stringResolutionSeconds) + ` ))AS INT64 )  ORDER BY Date) as row_num
			FROM
				api-project-901373404215.digital_assets.` + candlesTable + ` c 
			WHERE
				Date >= TIMESTAMP_SUB(
				CURRENT_TIMESTAMP(), 
				INTERVAL ` + string(interval) + `
			  )
				And market_cap_24h != 0
			GROUP BY
				Date,
				ID
			order by Date asc
		) as test
		where
			row_num = 1
		GROUP BY
			symbol
		order by symbol asc
		`)
	return query

}

/*
builds chart data for NFTs
currencyType can = floorprice_usd or floorprice_native
*/
func (bq *BQStore) buildNFTChartQuery(interval BQTimeInterval, stringResolutionSeconds ChartQueryResSeconds, currencyType NFTPriceType) *bigquery.Query {
	var source = os.Getenv("DATASOURCE")
	var query *bigquery.Query
	var candlesTable string
	switch source {
	case "coingecko":
		candlesTable = fmt.Sprintf("Digital_Assets_NFT_MarketData%s", os.Getenv("DATA_NAMESPACE"))
		query = bq.Query(`
		SELECT
			Symbol as symbol,
			ARRAY_AGG(STRUCT('Time',time, 'Price', Price, 'floorpricenative',floorpricenative, 'marketCap_native',marketCap_native, 'marketCap_usd',marketCap_usd, 'volume_native',volume_native,'volume_usd',volume_usd   )) as beprices
		FROM
		(
			SELECT
				ID symbol,
				Occurance_Time as time,
				CAST(` + string(currencyType) + ` AS FLOAT64) Price,
				CAST(floorprice_native AS FLOAT64) floorpricenative,
				CAST(marketCap_native AS FLOAT64) marketCap_native,
				CAST(marketCap_usd AS FLOAT64) marketCap_usd,
				CAST(volumeNative AS FLOAT64) volume_native,
				CAST(volumeUSD AS FLOAT64) volume_usd,
				ROW_NUMBER() OVER (PARTITION BY ID, CAST(FLOOR(UNIX_SECONDS(Occurance_Time)/ (` + string(stringResolutionSeconds) + ` ))AS INT64 )  ORDER BY Occurance_Time) as row_num
			FROM
				api-project-901373404215.digital_assets.` + candlesTable + ` c 
			WHERE
				Occurance_Time >= TIMESTAMP_SUB(
				CURRENT_TIMESTAMP(), 
				INTERVAL ` + string(interval) + `
			  ) 
			order by Occurance_Time
		) as test
		where row_num = 1
		GROUP BY
		symbol
		`)
	}
	return query

}

// QueryChartByInterval queries the chart data by interval
// Inputs:
// - Interva: String (1 DAY, 1 HOUR, 1 MINUTE)
// - TargetResolutionSeconds: Int as String (60, 300, 900, 3600, 14400, 86400)
// - UUID: String `uuid.New().String()`
// - Context: Context
// Returns the chart data
func (bq *BQStore) QueryChartByInterval(interval BQTimeInterval, stringResolutionSeconds ChartQueryResSeconds, uuid string, dataType DictionaryCategory, ctxO context.Context) ([]TimeSeriesResultPG, error) {

	ctx, span := tracer.Start(ctxO, "QueryChartByInterval")
	defer span.End()

	labels := make(map[string]string)

	labels["uuid"] = uuid
	span.SetAttributes(attribute.String("uuid", uuid))
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	labels["bigquery"] = "true"
	span.SetAttributes(attribute.Bool("bigquery", true))
	labels["subFunction"] = "QueryChartByInterval"

	var tsResult []TimeSeriesResultPG
	//var candlesTable = fmt.Sprintf("nomics_ohlcv_candles%s", os.Getenv("DATA_NAMESPACE"))
	// stringResolutionSeconds1 = "900"
	var query *bigquery.Query
	if dataType == Ft {
		query = bq.buildChartQuery(interval, stringResolutionSeconds)
	} else if dataType == Nft {
		query = bq.buildNFTChartQuery(interval, stringResolutionSeconds, FloorPriceUSD)
	} else if dataType == Category {
		query = bq.buildCategoriesChartQuery(interval, stringResolutionSeconds)
	}
	job, err := query.Run(ctx)
	if err != nil {
		return tsResult, err
	}

	log.DebugL(labels, "Fundamentals Query Job ID: %s", job.ID())
	span.SetAttributes(attribute.String("chart_by_interval_job_id", job.ID()))

	status, err := job.Wait(ctx)
	if err != nil {
		return tsResult, err
	}

	if err := status.Err(); err != nil {
		return tsResult, err
	}

	it, err := job.Read(ctx)
	if err != nil {
		return tsResult, err
	}

	span.AddEvent("Query Chart By Interval BQ Query Compelte")

	for {
		var tsObj = TimeSeriesResultPG{}
		tsObj.TargetResolutionSeconds, _ = strconv.Atoi(string(stringResolutionSeconds))
		tsObj.IsIndex = false
		err := it.Next(&tsObj)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return tsResult, err
		}
		//SortChartDataPG(tsObj.Slice)
		if dataType == Ft {
			tsObj.AssetType = "FT"
		} else if dataType == Nft {
			tsObj.AssetType = "NFT"
		} else if dataType == Category {
			tsObj.AssetType = "CATEGORY"
		}
		tsResult = append(tsResult, tsObj)
	}

	span.SetStatus(codes.Ok, "Query Chart By Interval BQ Query Compelte")
	return tsResult, nil
}

// query chart data by symbol and time
// TODO: needs a rework
func (bq *BQStore) QueryChartBySymbolAndTime(symbol string, interval string, targetResolutionSeconds int, stringResolutionSeconds string) (TimeSeriesResult, error) {
	ctx := context.Background()
	query := `
#24h query using 15m intervals
WITH
  base AS (
  SELECT
    ANY_VALUE(Symbol) sym,
    CAST(AVG(close) AS FLOAT64) avg_close,
    Time,
    STRING_AGG(Market, ", ") mkt_sample
  FROM
  	api-project-901373404215.digital_assets.v_cq_ohlcv_outliers_filtered c
  WHERE
    Symbol = "` + symbol + `"
    AND Time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL ` + interval + `)
  GROUP BY
    Time,
    Symbol QUALIFY ROW_NUMBER() OVER (PARTITION BY Symbol, CAST(FLOOR(UNIX_SECONDS(Time)/ (` + stringResolutionSeconds + `))AS INT64 )
    ORDER BY
      c.Time) = 1 )
SELECT
  sym,
  ARRAY_AGG(STRUCT(time,
      avg_close)
  ORDER BY
    time ASC) slice
FROM
  base
GROUP BY
  sym
ORDER BY
  sym ASC
`

	q := bq.Client.Query(query)

	it, err := q.Read(ctx)
	if err != nil {
		return TimeSeriesResult{}, err
	}

	var tsResult TimeSeriesResult

	for {
		tsResult.TargetResolutionSeconds = targetResolutionSeconds
		tsResult.IsIndex = false
		err := it.Next(&tsResult)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return TimeSeriesResult{}, err
		}
	}
	return tsResult, nil
}

func SortChartData(chartData []Slice) {
	sort.Slice(chartData, func(i, j int) bool {
		return chartData[i].Time.Before(chartData[j].Time)
	})
}

func GetTableName(tableName string) string {

	if os.Getenv("DATA_NAMESPACE") == "_dev" {
		return fmt.Sprintf("%s%s", tableName, os.Getenv("DATA_NAMESPACE"))
	}

	return tableName
}
func (bq *BQStore) BuildFundamentals(ctx context.Context) ([]PGFundamentalsResult, error) {
	log.Debug("Building Fundamentals")

	query := bq.Query(`
	WITH
	allTime AS (
	SELECT
		CAST(MIN(Close) AS FLOAT64) all_time_low,
		CAST(MAX(Close) AS FLOAT64) all_time_high,
		Id AS symbol
	FROM (
		SELECT
		Price AS Close,
		Id
		FROM
			api-project-901373404215.digital_assets.nomics_currencies ) AS allTime
	GROUP BY
		Id ),
	oneDay AS (
	SELECT
		CAST(MAX(Close) AS FLOAT64) high_1d,
		CAST(MIN(Close) AS FLOAT64) low_1d,
		Id AS symbol
	FROM (
		SELECT
		Price AS Close,
		Id
		FROM
			api-project-901373404215.digital_assets.nomics_currencies
		WHERE
		PriceTimestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 24 Hour) ) AS oneDay
	GROUP BY
		Id ),
	sevenDays AS (
	SELECT
		CAST(MAX(Close) AS FLOAT64) high_7d,
		CAST(MIN(Close) AS FLOAT64) low_7d,
		Id AS symbol
	FROM (
		SELECT
		Price AS Close,
		Id
		FROM
			api-project-901373404215.digital_assets.nomics_currencies
		WHERE
		PriceTimestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 7 DAY) ) AS sevenDays
	GROUP BY
		Id ),
	thirtyDays AS (
	SELECT
		CAST(MAX(Close) AS FLOAT64) high_30d,
		CAST(MIN(Close) AS FLOAT64) low_30d,
		Id AS symbol
	FROM (
		SELECT
		Price AS Close,
		Id
		FROM
			api-project-901373404215.digital_assets.nomics_currencies
		WHERE
		PriceTimestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 30 DAY) ) AS thirtyDays
	GROUP BY
		Id ),
	oneYear AS (
	SELECT
		CAST(MAX(Close) AS FLOAT64) high_1y,
		CAST(MIN(Close) AS FLOAT64) low_1y,
		Id AS symbol
	FROM (
		SELECT
		Price AS Close,
		Id
		FROM
			api-project-901373404215.digital_assets.nomics_currencies
		WHERE
		PriceTimestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 365 DAY) ) AS oneYear
	GROUP BY
		Id )
	SELECT
		CAST(MAX(oneDay.high_1d) AS FLOAT64) AS high_24h,
		CAST(MIN(oneDay.low_1d) AS FLOAT64) AS low_24h,
		CAST(MAX(sevenDays.high_7d) AS FLOAT64) AS high_7d,
		CAST(MIN(sevenDays.low_7d) AS FLOAT64) AS low_7d,
		CAST(MAX(thirtyDays.high_30d) AS FLOAT64) AS high_30d,
		CAST(MIN(thirtyDays.low_30d) AS FLOAT64) AS low_30d,
		CAST(MAX(oneYear.high_1y) AS FLOAT64) AS high_1y,
		CAST(MIN(oneYear.low_1y) AS FLOAT64) AS low_1y,
		CAST(MIN(allTime.all_time_low) AS FLOAT64) AS all_time_low,
		CAST(MAX(allTime.all_time_high) AS FLOAT64) AS all_time_high,
		oneDay.symbol
	FROM
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
	GROUP BY
		oneDay.symbol
	`)

	job, err := query.Run(ctx)
	if err != nil {
		return nil, err
	}

	log.Debug("Fundamentals Query Job ID: %s", job.ID())

	status, err := job.Wait(ctx)
	if err != nil {
		return nil, err
	}

	if err := status.Err(); err != nil {
		return nil, err
	}

	it, err := job.Read(ctx)
	if err != nil {
		return nil, err
	}

	var fundamentalsResults []PGFundamentalsResult
	for {
		var fundamentalsResult PGFundamentalsResult
		err := it.Next(&fundamentalsResult)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		fundamentalsResults = append(fundamentalsResults, fundamentalsResult)

	}

	log.Info("Fundamentals for %d symbols built", len(fundamentalsResults))

	return fundamentalsResults, nil
}

// get all market Data from BQ for Fundamentals
func (bq *BQStore) GetCoinsMarketData(ctx0 context.Context) (map[string]CoinsMarketResult, error) {
	ctx, span := tracer.Start(ctx0, "GetCoinsMarketData")
	defer span.End()

	log.Debug("GetCoinsMarketData")

	assetsTable := GetTableName("Digital_Asset_MarketData")

	query := bq.Query(`
	WITH market_data AS (
		SELECT
			forbes_id,
			ID,
			Name,
			Symbol,
			Price,
			CirculatingSupply,
			MaxSupply,
			MarketCap,
			Volume,
			QuoteCurrency,
			SOURCE,
			Occurance_Time,
			TIMESTAMP_DIFF(CAST(Occurance_Time AS TIMESTAMP), CAST(CURRENT_TIMESTAMP() AS TIMESTAMP), Day) as StatusResult,
			ROW_NUMBER() over (PARTITION BY ID order by Occurance_Time desc) as row_num
		FROM
			api-project-901373404215.digital_assets.` + assetsTable + `
		)
		SELECT
			forbes_id,
			ID,
			Name,
			Symbol,
			Price,
			CirculatingSupply,
			MaxSupply,
			MarketCap,
			Volume,
			QuoteCurrency,
			SOURCE,
			Occurance_Time,
			CASE StatusResult
				when 0 Then 'active'
				Else 'comatoken'
			end
			as status
		from market_data
		where row_num = 1
		and forbes_id is not null
	`)
	job, err := query.Run(ctx)
	if err != nil {
		return nil, err
	}

	log.Debug("CoinMarketData Query Job ID: %s", job.ID())

	status, err := job.Wait(ctx)
	if err != nil {
		return nil, err
	}

	if err := status.Err(); err != nil {
		return nil, err
	}

	it, err := job.Read(ctx)

	if err != nil {
		return nil, err
	}

	markets := make(map[string]CoinsMarketResult)

	for {
		var market CoinsMarketResult

		err := it.Next(&market)
		if err == iterator.Done {
			break
		}

		if err != nil {
			return nil, err
		}

		markets[market.ForbesID] = market
	}
	log.Info("CoinMarketData : %d", len(markets))

	return markets, nil
}

// Build High/Low from CoinGecko BQ for Fundamentals
func (bq *BQStore) BuildHighLowFundamentalsCG(ctx0 context.Context, uuid string) ([]PGFundamentalsResult, error) {
	ctx, span := tracer.Start(ctx0, "BuildHighLowFundamentalsCG")
	defer span.End()
	labels := make(map[string]string)

	labels["uuid"] = uuid
	span.SetAttributes(attribute.String("uuid", uuid))
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	labels["bigquery"] = "true"
	span.SetAttributes(attribute.Bool("bigquery", true))
	labels["subFunction"] = "BuildHighLowFundamentalsCG"

	log.DebugL(labels, "Building FundamentalsCG")
	marketTableName := GetTableName("Digital_Asset_MarketData")
	exchangesTableName := GetTableName("Digital_Asset_Exchanges_Tickers_Data")

	query := bq.Query(`
		WITH allTime AS (
		SELECT
			CAST(MIN(Price) AS FLOAT64) all_time_low,
			CAST(MAX(Price) AS FLOAT64) all_time_high,
			ID AS symbol,
      forbes_id
		FROM
			api-project-901373404215.digital_assets.` + marketTableName + `
		WHERE 
			forbes_id is not null
		GROUP BY
			ID, forbes_id
	),
	oneHour AS (
		SELECT
			CAST(MAX(Price) AS FLOAT64) high_1h,
			CAST(MIN(Price) AS FLOAT64) low_1h,
			ANY_VALUE(
				STRUCT(Occurance_Time, MarketCap, Volume, Price)
				having MIN Occurance_Time
			) AS open_value,
			ANY_VALUE(
				STRUCT(Occurance_Time, MarketCap, Volume, Price)
				having MAX Occurance_Time
			) AS close_value,
			ID AS symbol,
      forbes_id
		FROM
			api-project-901373404215.digital_assets.` + marketTableName + `
		WHERE
			Occurance_Time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
		GROUP BY
			ID, forbes_id
	),
	oneDay AS (
		SELECT
			CAST(MAX(Price) AS FLOAT64) high_1d,
			CAST(MIN(Price) AS FLOAT64) low_1d,
			ANY_VALUE(
				STRUCT(Occurance_Time, MarketCap, Volume)
				having MIN Occurance_Time
			) AS open_value,
			ANY_VALUE(
				STRUCT(Occurance_Time, MarketCap, Volume, Price)
				having MAX Occurance_Time
			) AS close_value,
			ID AS symbol, 
      forbes_id
		FROM
			api-project-901373404215.digital_assets.` + marketTableName + `
		WHERE
			Occurance_Time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 Hour)
		GROUP BY
			ID, 
      forbes_id
	),
	sevenDays AS (
		SELECT
			CAST(MAX(Price) AS FLOAT64) high_7d,
			CAST(MIN(Price) AS FLOAT64) low_7d,
			ANY_VALUE(
				STRUCT(Occurance_Time, MarketCap, Volume)
				having MIN Occurance_Time
			) AS open_value,
			ID as symbol,
      forbes_id
		FROM
			api-project-901373404215.digital_assets.` + marketTableName + `
		WHERE
			Occurance_Time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
		GROUP BY
			ID,
      forbes_id
	),
	thirtyDays AS (
		SELECT
			CAST(MAX(Price) AS FLOAT64) high_30d,
			CAST(MIN(Price) AS FLOAT64) low_30d,
			ANY_VALUE(
				STRUCT(Occurance_Time, MarketCap, Volume)
				having MIN Occurance_Time
			) AS open_value,
			ID AS symbol,
      forbes_id
		FROM
			api-project-901373404215.digital_assets.` + marketTableName + `
		WHERE
			Occurance_Time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
		GROUP BY
			ID,
      forbes_id
	),
	oneYear AS (
		SELECT
			CAST(MAX(Price) AS FLOAT64) high_1y,
			CAST(MIN(Price) AS FLOAT64) low_1y,
			ANY_VALUE(
				STRUCT(Occurance_Time, MarketCap, Volume)
				having MIN Occurance_Time
			) AS open_value,
			ID AS symbol,
      forbes_id
		FROM
			api-project-901373404215.digital_assets.` + marketTableName + `
		WHERE
			Occurance_Time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
		GROUP BY
			ID,
      forbes_id
	),
	YTD AS (
		SELECT
			CAST(MAX(Price) AS FLOAT64) high_ytd,
			CAST(MIN(Price) AS FLOAT64) low_ytd,
			ANY_VALUE(
				STRUCT(Occurance_Time, MarketCap, Volume, Price)
				having MIN Occurance_Time
			) AS open_value,
			ID AS symbol,
      forbes_id
		FROM
			api-project-901373404215.digital_assets.` + marketTableName + `
		WHERE
			Occurance_Time >= CAST(DATE_TRUNC(CURRENT_DATE(), year) AS Timestamp)
		GROUP BY
			ID,
      forbes_id
	),
	market_pairs AS (
		SELECT
			tickers.CoinID AS symbol,
			COUNT(CONCAT(tickers.CoinID, tickers.Target)) AS num_markets
		FROM
			api-project-901373404215.digital_assets.` + exchangesTableName + ` d
			JOIN UNNEST(d.Tickers) AS tickers
		WHERE
			tickers.Timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 hour)
		GROUP BY
			tickers.CoinID
	)
	SELECT
		CAST(oneHour.high_1h AS FLOAT64) AS high_1h,
		CAST(oneHour.low_1h AS FLOAT64) AS low_1h,
		CAST(oneHour.open_value.MarketCap AS FLOAT64) AS market_cap_open_1h,
		CAST(oneDay.close_value.MarketCap AS FLOAT64) AS market_cap_close_1h,
		CAST(oneHour.open_value.Volume AS FLOAT64) AS volume_open_1h,
		CAST(oneDay.close_value.Volume AS FLOAT64) AS volume_close_1h,
		CAST(oneHour.open_value.Price AS FLOAT64) AS price_open_1h,
		CAST(oneDay.close_value.Price AS FLOAT64) AS price_close_1h,

		CAST(oneDay.high_1d AS FLOAT64) AS high_24h,
		CAST(oneDay.low_1d AS FLOAT64) AS low_24h,
		CAST(oneDay.open_value.MarketCap AS FLOAT64) AS market_cap_open_24h,
		CAST(oneDay.close_value.MarketCap AS FLOAT64) AS market_cap_close_24h,
		CAST(oneDay.open_value.Volume AS FLOAT64) AS volume_open_24h,
		CAST(oneDay.close_value.Volume AS FLOAT64) AS volume_close_24h,
		CAST(sevenDays.high_7d AS FLOAT64) AS high_7d,
		CAST(sevenDays.low_7d AS FLOAT64) AS low_7d,
		CAST(sevenDays.open_value.MarketCap AS FLOAT64) AS market_cap_open_7d,
		CAST(oneDay.close_value.MarketCap AS FLOAT64) AS market_cap_close_7d,
		CAST(sevenDays.open_value.Volume AS FLOAT64) AS volume_open_7d,
		CAST(oneDay.close_value.Volume AS FLOAT64) AS volume_close_7d,
		CAST(thirtyDays.high_30d AS FLOAT64) AS high_30d,
		CAST(thirtyDays.low_30d AS FLOAT64) AS low_30d,
		CAST(thirtyDays.open_value.MarketCap AS FLOAT64) AS market_cap_open_30d,
		CAST(oneDay.close_value.MarketCap AS FLOAT64) AS market_cap_close_30d,
		CAST(thirtyDays.open_value.Volume AS FLOAT64) AS volume_open_30d,
		CAST(oneDay.close_value.Volume AS FLOAT64) AS volume_close_30d,
		CAST(oneYear.high_1y AS FLOAT64) AS high_1y,
		CAST(oneYear.low_1y AS FLOAT64) AS low_1y,
		CAST(oneYear.open_value.MarketCap AS FLOAT64) AS market_cap_open_1y,
		CAST(oneDay.close_value.MarketCap AS FLOAT64) AS market_cap_close_1y,
		CAST(oneYear.open_value.Volume AS FLOAT64) AS volume_open_1y,
		CAST(oneDay.close_value.Volume AS FLOAT64) AS volume_close_1y,
		CAST(YTD.high_ytd AS FLOAT64) AS high_ytd,
		CAST(YTD.low_ytd AS FLOAT64) AS low_ytd,
		CAST(YTD.open_value.MarketCap AS FLOAT64) AS market_cap_open_ytd,
		CAST(oneDay.close_value.MarketCap AS FLOAT64) AS market_cap_close_ytd,
		CAST(YTD.open_value.Volume AS FLOAT64) AS volume_open_ytd,
		CAST(oneDay.close_value.Volume AS FLOAT64) AS volume_close_ytd,
		CAST(YTD.open_value.Price AS FLOAT64) AS price_open_ytd,
		CAST(oneDay.close_value.Price AS FLOAT64) AS price_close_ytd,
		CAST(allTime.all_time_low AS FLOAT64) AS all_time_low,
		CAST(allTime.all_time_high AS FLOAT64) AS all_time_high,
		CASE
			When market_pairs.num_markets is null Then 0
			ELSE market_pairs.num_markets
		END as number_of_active_market_pairs,
		allTime.symbol,
    allTime.forbes_id
	FROM
		allTime
		left JOIN oneHour ON oneHour.forbes_id = allTime.forbes_id
		left JOIN oneDay ON oneDay.forbes_id = allTime.forbes_id
		left JOIN sevenDays ON sevenDays.forbes_id = allTime.forbes_id
		left JOIN thirtyDays ON thirtyDays.forbes_id = allTime.forbes_id
		left JOIN oneYear ON oneYear.forbes_id = allTime.forbes_id
		left JOIN YTD ON YTD.forbes_id = allTime.forbes_id
		LEFT JOIN market_pairs ON market_pairs.symbol = allTime.symbol
	`)

	job, err := query.Run(ctx)

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	labels["high_low_job_id"] = job.ID()
	span.SetAttributes(attribute.String("high_low_job_id", job.ID()))

	log.DebugL(labels, "FundamentalsCG Query Job ID: %s", job.ID())

	status, err := job.Wait(ctx)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	span.AddEvent("Fundamentals Query Job Complete")

	if err := status.Err(); err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	it, err := job.Read(ctx)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	var fundamentalsResults []PGFundamentalsResult
	for {
		var fundamentalsResult PGFundamentalsResult
		err := it.Next(&fundamentalsResult)
		if err == iterator.Done {
			break
		}
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}

		fundamentalsResults = append(fundamentalsResults, fundamentalsResult)

	}

	log.InfoL(labels, "Fundamentals for %d symbols built", len(fundamentalsResults))

	span.SetStatus(codes.Ok, "High Lows built")

	return fundamentalsResults, nil
}

// Get the 24h old category fundamental from BQ. This helps in calculating market_cap 24h change etc
func (bq *BQStore) GetCategoryFundamental24h(ctxO context.Context, uuid string) ([]CategoryFundamental, error) {
	ctx, span := tracer.Start(ctxO, "GetCategoryFundamental24h")
	defer span.End()

	labels := make(map[string]string)

	labels["uuid"] = uuid
	span.SetAttributes(attribute.String("uuid", uuid))
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	labels["bigquery"] = "true"
	span.SetAttributes(attribute.Bool("bigquery", true))
	labels["subFunction"] = "GetCategoryFundamental24h"

	categoriesTable := GetTableName("Digital_Asset_Categories_History")

	log.DebugL(labels, "Getting Cateogory Fundamentals 24h Old")
	query := bq.Query(`
	WITH OldData AS (
		SELECT
		  id,
		  market_cap,
		  ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at ASC) AS row_num
		FROM
			api-project-901373404215.digital_assets.` + categoriesTable + `
		WHERE
			TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), created_at, HOUR) <= 24
	  )
	  SELECT
		id,
		market_cap
	  FROM
		OldData
	  WHERE
		row_num = 1
	`)

	job, err := query.Run(ctx)
	if err != nil {
		return nil, err
	}

	labels["categories_fundamentals_24h_job_id"] = job.ID()
	span.SetAttributes(attribute.String("categories_fundamentals_24h_job_id", job.ID()))

	log.DebugL(labels, "Category Fundamentals 24h Query Job ID: %s", job.ID())

	status, err := job.Wait(ctx)
	if err != nil {
		return nil, err
	}

	span.AddEvent("Category Fundamentals 24h Query Job Completed")

	if err := status.Err(); err != nil {
		return nil, err
	}

	it, err := job.Read(ctx)
	if err != nil {
		return nil, err
	}

	var fundamentalsResults []CategoryFundamental
	for {

		var fundamentalsResult CategoryFundamental
		err := it.Next(&fundamentalsResult)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		fundamentalsResults = append(fundamentalsResults, fundamentalsResult)

	}

	log.InfoL(labels, "Category Fundamentals 24h for %d symbols built", len(fundamentalsResults))

	span.SetStatus(codes.Ok, "Category Fundamentals 24h Query Job Completed")

	return fundamentalsResults, nil

}

func (bq *BQStore) ExchangeBasedFundamentalsCG(ctxO context.Context, uuid string) (map[string][]ExchangeBasedFundamentals, error) {
	ctx, span := tracer.Start(ctxO, "ExchangeBasedFundamentalsCG")
	defer span.End()

	labels := make(map[string]string)

	labels["uuid"] = uuid
	span.SetAttributes(attribute.String("uuid", uuid))
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	labels["bigquery"] = "true"
	span.SetAttributes(attribute.Bool("bigquery", true))
	labels["subFunction"] = "ExchangeBasedFundamentalsCG"

	exchangeTable := GetTableName("Digital_Asset_Exchanges_Tickers_Data")
	assetsTable := GetTableName("Digital_Asset_MarketData")

	log.DebugL(labels, "Building Exchange Based Fundamentals")
	query := bq.Query(`
	WITH
	exchanges_data AS (
		SELECT
			ticker.market.name AS Exchange,
			ticker.coinid AS Symbol,
			AVG(ticker.last) AS price_by_exchange_1d,
			AVG(ticker.volume) AS volume_by_exchange_1d
		FROM (
			SELECT
				ARRAY_AGG(ticker
			ORDER BY
				timestamp DESC
			LIMIT
				1)[
			OFFSET
				(0)] AS ticker
			FROM
				api-project-901373404215.digital_assets.` + exchangeTable + `,
				UNNEST(tickers) AS ticker
			WHERE
				TARGET IN ('USD',
					'USDC',
					'USDT')
				AND ticker.volume IS NOT NULL
				AND ticker.timestamp >= TIMESTAMP_SUB( CURRENT_TIMESTAMP(), INTERVAL 24 hour )
			GROUP BY
				market.name,
				base,
				coinid,
				TARGET
			ORDER BY
				base ) latest_tickers
		GROUP BY
			ticker.market.name,
			ticker.base,
			ticker.coinid
		ORDER BY
			ticker.base ),
	assets_data AS (
		SELECT
			ID AS symbol,
			forbes_id
		FROM
			api-project-901373404215.digital_assets.` + assetsTable + `
		GROUP BY
			ID,
			forbes_id )
		SELECT
			exchanges_data.Exchange,
			exchanges_data.Symbol,
			exchanges_data.price_by_exchange_1d,
			exchanges_data.volume_by_exchange_1d,
			assets_data.forbes_id
		FROM
			exchanges_data
		LEFT JOIN
			assets_data
		ON
			assets_data.symbol = exchanges_data.Symbol
		Where assets_data.forbes_id is not null
	`)

	job, err := query.Run(ctx)
	if err != nil {
		return nil, err
	}

	labels["exchange_based_fundamentals_job_id"] = job.ID()
	span.SetAttributes(attribute.String("exchange_based_fundamentals_job_id", job.ID()))

	log.DebugL(labels, "Exchange Based Fundamentals Query Job ID: %s", job.ID())

	status, err := job.Wait(ctx)
	if err != nil {
		return nil, err
	}

	span.AddEvent("Exchange Based Fundamentals Query Job Completed")

	if err := status.Err(); err != nil {
		return nil, err
	}

	it, err := job.Read(ctx)
	if err != nil {
		return nil, err
	}

	fundamentalsResults := make(map[string][]ExchangeBasedFundamentals)
	for {

		var fundamentalsResult ExchangeBasedFundamentals
		err := it.Next(&fundamentalsResult)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		fundamentalsResults[fundamentalsResult.ForbesID] = append(fundamentalsResults[fundamentalsResult.ForbesID], fundamentalsResult)

	}

	log.InfoL(labels, "Exchange Based Fundamentals for %d symbols built", len(fundamentalsResults))

	span.SetStatus(codes.Ok, "Exchange Based Fundamentals Query Job Completed")

	return fundamentalsResults, nil

}

func (bq *BQStore) ExchangeFundamentalsCG(ctxO context.Context, uuid string) (map[string]ExchangeResults, error) {
	ctx, span := tracer.Start(ctxO, "ExchangeFundamentalsCG")
	defer span.End()

	labels := make(map[string]string)

	labels["uuid"] = uuid
	span.SetAttributes(attribute.String("uuid", uuid))
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	labels["bigquery"] = "true"
	span.SetAttributes(attribute.Bool("bigquery", true))
	labels["subFunction"] = "ExchangeFundamentalsCG"

	startTime := log.StartTimeL(labels, "Exchange Fundamental Insert")

	exchangeTable := GetTableName("Digital_Asset_Exchanges_Tickers_Data")

	log.DebugL(labels, "Building Exchange Fundamentals")
	query := bq.Query(`
	SELECT
		market.name AS name,
		market.Identifier AS id,
		AVG(CAST(ticker.volume AS float64)) AS volume_by_exchange_1d,
		COALESCE(COUNT(CONCAT(ticker.CoinID, '-', ticker.Target)), 0) AS exchange_active_market_pairs
	FROM
		api-project-901373404215.digital_assets.` + exchangeTable + `,
		UNNEST(tickers) AS ticker
		JOIN (
			SELECT
				market.name AS name,
				market.Identifier AS id,
				ARRAY_AGG(ticker
					ORDER BY timestamp DESC
					LIMIT 1)[OFFSET(0)] AS ticker
			FROM
				api-project-901373404215.digital_assets.` + exchangeTable + `,
				UNNEST(tickers) AS ticker
			WHERE
				ticker.volume IS NOT NULL 
				AND
				ticker.Target IN ('USD', 'USDC', 'USDT')
			GROUP BY
				market.name,
				market.Identifier
		) AS latest_tickers ON
			latest_tickers.name = market.name AND
			latest_tickers.id = market.Identifier AND
			latest_tickers.ticker.CoinID = ticker.CoinID AND
			latest_tickers.ticker.Target = ticker.Target
	GROUP BY
		market.name,
		market.Identifier

	`)

	job, err := query.Run(ctx)
	if err != nil {
		span.SetStatus(otelCodes.Error, err.Error())
		log.EndTime("Get Exchanges Fundamentals CG", startTime, err)
		return nil, err
	}

	labels["exchange_fundamentals_job_id"] = job.ID()
	span.SetAttributes(attribute.String("exchange_fundamentals_job_id", job.ID()))

	log.DebugL(labels, "Exchange Fundamentals Query Job ID: %s", job.ID())

	status, err := job.Wait(ctx)
	if err != nil {
		span.SetStatus(otelCodes.Error, err.Error())
		log.EndTime("Exchange Fundamentals CG Query", startTime, err)
		return nil, err
	}

	span.AddEvent("Exchange Fundamentals Query Job Completed")

	if err := status.Err(); err != nil {
		span.SetStatus(otelCodes.Error, err.Error())
		log.EndTime("Exchange Fundamentals CG Job", startTime, err)
		return nil, err
	}

	it, err := job.Read(ctx)
	if err != nil {
		span.SetStatus(otelCodes.Error, err.Error())
		log.EndTime("Exchange Fundamentals CG Read", startTime, err)
		return nil, err
	}

	exchangesResults := make(map[string]ExchangeResults)
	for {

		var exchangesResult ExchangeResults
		err := it.Next(&exchangesResult)
		if err == iterator.Done {
			break
		}
		if err != nil {
			span.SetStatus(otelCodes.Error, err.Error())
			log.EndTime("Exchange Fundamentals CG Scan ", startTime, err)
			return nil, err
		}

		exchangesResults[exchangesResult.Id] = exchangesResult

	}

	log.InfoL(labels, "Exchange Fundamentals for %d symbols built", len(exchangesResults))

	span.SetStatus(codes.Ok, "Exchange Fundamentals Query Job Completed")

	return exchangesResults, nil

}

func (bq *BQStore) GetBrightCoveVideos(ctxO context.Context, uuid string) (map[string]model.BqVideosResults, error) {
	ctx, span := tracer.Start(ctxO, "GetBrightCoveVideos")
	defer span.End()

	labels := make(map[string]string)

	labels["uuid"] = uuid
	span.SetAttributes(attribute.String("uuid", uuid))
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	labels["bigquery"] = "true"
	span.SetAttributes(attribute.Bool("bigquery", true))
	labels["subFunction"] = "GetBrightCoveVideos"

	startTime := log.StartTimeL(labels, "GetBrightCoveVidoes")

	log.DebugL(labels, "GetBrightCoveVidoes")
	query := bq.Query(`
	with ordered_table as (
		SELECT 
		video,	
		dt_updated,
		ROW_NUMBER() over (partition by video order by dt_updated desc) as row_num
		FROM 
				api-project-901373404215.brightcove.analytics_unique_users
		  where 
				videocustom_fieldschannelsection in ('Forbes Digital Assets :channel_115','Crypto and Blockchain :channel_72section_1095') 
				or videocustom_fieldschannelsection2 in ('Forbes Digital Assets :channel_115','Crypto and Blockchain :channel_72section_1095') 
				or videocustom_fieldschannelsection3 in ('Forbes Digital Assets :channel_115','Crypto and Blockchain :channel_72section_1095') 
				or videocustom_fieldschannelsection4 in ('Forbes Digital Assets :channel_115','Crypto and Blockchain :channel_72section_1095') 
				or videocustom_fieldschannelsection5 in ('Forbes Digital Assets :channel_115','Crypto and Blockchain :channel_72section_1095') 
			order by dt_updated desc
	  )
	   SELECT 
		video,	
		dt_updated
	   from ordered_table 
	   where row_num = 1
	   limit 100
	`)

	job, err := query.Run(ctx)
	if err != nil {
		span.SetStatus(otelCodes.Error, err.Error())
		log.EndTime("GetBrightCoveVideos", startTime, err)
		return nil, err
	}

	labels["GetBrightCoveVideos_job_id"] = job.ID()
	span.SetAttributes(attribute.String("GetBrightCoveVideos_job_id", job.ID()))

	log.DebugL(labels, "GetBrightCoveVideos Job ID: %s", job.ID())

	status, err := job.Wait(ctx)
	if err != nil {
		span.SetStatus(otelCodes.Error, err.Error())
		log.EndTime("GetBrightCoveVideos Query", startTime, err)
		return nil, err
	}

	span.AddEvent("GetBrightCoveVideos Job Completed")

	if err := status.Err(); err != nil {
		span.SetStatus(otelCodes.Error, err.Error())
		log.EndTime("GetBrightCoveVideos Job", startTime, err)
		return nil, err
	}

	it, err := job.Read(ctx)
	if err != nil {
		span.SetStatus(otelCodes.Error, err.Error())
		log.EndTime("GetBrightCoveVideos Read", startTime, err)
		return nil, err
	}

	exchangesResults := make(map[string]model.BqVideosResults)
	for {

		var exchangesResult model.BqVideosResults
		err := it.Next(&exchangesResult)
		if err == iterator.Done {
			break
		}
		if err != nil {
			span.SetStatus(otelCodes.Error, err.Error())
			log.EndTime("Exchange Fundamentals CG Scan ", startTime, err)
			return nil, err
		}

		exchangesResults[exchangesResult.VideoID.String()] = exchangesResult

	}

	log.InfoL(labels, "GetBrightCoveVidoes %d videos retrieved", len(exchangesResults))

	span.SetStatus(codes.Ok, "GetBrightCoveVidoes Completed")

	return exchangesResults, nil

}

// returns a list of chatbotData rows that needs to be upserted into the bigquery chatbot table. These rows are created from the firebase metadata description & the fundamentals data
func CalculateChatbotAssets(ctx0 context.Context, assets *[]Fundamentals, fsTokens *[]model.ForbesMetadata) *[]ChatbotData {
	_, span := tracer.Start(ctx0, "calculateChatbotAssets")
	defer span.End()

	var chatbotAssets []ChatbotData

	for _, fsToken := range *fsTokens {
		for _, asset := range *assets {
			if asset.Symbol == fsToken.AssetId {
				chatbotAsset := ChatbotData{
					ID:                        asset.Symbol,
					Name:                      asset.Name,
					DisplaySymbol:             asset.DisplaySymbol,
					Slug:                      "https://www.forbes.com/digital-assets/assets/" + asset.Slug,
					ForbesMetadataDescription: fsToken.MetadataDescription,
				}
				chatbotAssets = append(chatbotAssets, chatbotAsset)
				break
			}
		}
	}
	span.SetStatus(codes.Ok, "CalculateChatbotAssets Completed")
	return &chatbotAssets
}

/*
Upserts The category fundamentals into BigQuery historical table. This data can be used to construct charts later.
*/
func (bq *BQStore) InsertCategoryFundamentalsBQ(ctx0 context.Context, uuid string, allFundamentals *[]CategoryFundamental) error {
	ctx, span := tracer.Start(ctx0, "InsertCategoryFundamentalsBQ")
	defer span.End()

	labels := make(map[string]string)

	labels["uuid"] = uuid
	span.SetAttributes(attribute.String("uuid", uuid))
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()
	labels["bigquery"] = "true"
	span.SetAttributes(attribute.Bool("bigquery", true))
	labels["subFunction"] = "InsertCategoryFundamentalsBQ"

	startTime := log.StartTimeL(labels, "InsertCategoryFundamentalsBQ")

	categoryHistoricalTable := GetTableName("Digital_Assets_Categories_Historical_data")

	bqInserter := bq.Dataset("digital_assets").Table(categoryHistoricalTable).Inserter()
	bqInserter.IgnoreUnknownValues = true

	inserterErr := bqInserter.Put(ctx, *allFundamentals)
	var retryError error
	if inserterErr != nil {
		if strings.Contains(inserterErr.Error(), "413") {
			log.Info("413 Error. Breaking up Category Fundamentals and retrying insert")
			l := len(*allFundamentals)
			var ticks []CategoryFundamental
			ticks = append(ticks, *allFundamentals...)
			for y := (l / 3); y < l; y += (l / 3) {
				a := ticks[y-(l/3) : y]
				er := bq.InsertCategoryFundamentalsBQ(ctx, uuid, &a)
				if er != nil {
					retryError = er
				}
			}
			//If we couldnt recover return the error
			return retryError
		}
		//if not a 413 error return the error
		return inserterErr

	}

	span.SetStatus(codes.Ok, "InsertCategoryFundamentalsBQ Completed")
	log.EndTimeL(labels, "InsertCategoryFundamentalsBQ", startTime, nil)
	return nil
}

/*
Upserts Forbes Token Metadata into BigQuery chatbot table
*/
func (bq *BQStore) UpsertChatbotData(ctx0 context.Context, uuid string, assets *[]ChatbotData) error {
	ctx, span := tracer.Start(ctx0, "UpsertChatbotData")
	defer span.End()

	labels := make(map[string]string)

	labels["uuid"] = uuid
	span.SetAttributes(attribute.String("uuid", uuid))
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	labels["bigquery"] = "true"
	span.SetAttributes(attribute.Bool("bigquery", true))
	labels["subFunction"] = "UpsertChatbotData"
	startTime := log.StartTimeL(labels, "UpsertChatbotData")

	chatbotTable := "DigitalAssets_Chatbot_Data" + os.Getenv("DATA_NAMESPACE")

	// loop over assets in batch of 1000 assets
	for i := 0; i < len(*assets); i += 1000 {
		end := i + 1000
		if end > len(*assets) {
			end = len(*assets)
		}

		batch := (*assets)[i:end]

		var firstRow string
		var remainingRows string
		for i, asset := range batch {
			if i == 0 {
				firstRow = `
				SELECT 
					"` + asset.ID + `" id, 
					"` + asset.ForbesMetadataDescription + `" forbes_metadata_description, 
					"` + asset.DisplaySymbol + `" display_symbol, 
					"` + asset.Name + `" name, 
					"` + asset.Slug + `" slug  
				`
			} else {
				remainingRows += `
				UNION ALL SELECT "` + asset.ID + `", "` + asset.ForbesMetadataDescription + `", "` + asset.DisplaySymbol + `", "` + asset.Name + `", "` + asset.Slug + `" `
			}
		}
		queryString := `
		MERGE INTO api-project-901373404215.digital_assets.` + chatbotTable + ` T
		USING (
		  ` + firstRow + remainingRows + `
		) as S
		ON T.id = S.id
		WHEN MATCHED THEN
			UPDATE SET 
			forbes_metadata_description = S.forbes_metadata_description, 
			display_symbol = S.display_symbol, 
			name = S.name,
			slug = S.slug
		WHEN NOT MATCHED THEN
			INSERT (id, forbes_metadata_description, display_symbol, name, slug) values (S.id, S.forbes_metadata_description, S.display_symbol, S.name, S.slug)`

		query := bq.Query(queryString)

		_, err := query.Run(ctx)
		if err != nil {
			span.SetStatus(otelCodes.Error, err.Error())
			log.EndTimeL(labels, "UpsertChatbotData error ", startTime, err)
			return err
		}
	}

	span.SetStatus(codes.Ok, "UpsertChatbotData Completed")
	log.EndTimeL(labels, "UpsertChatbotData", startTime, nil)
	return nil
}

/*
Dumps All the Search terms into BigQuery search table along with the number of results returned.
Acceptable tableTypes are "nft" and "asset"
*/
func (bq *BQStore) InsertSearchData(ctx0 context.Context, searchTerm string, results int, tableType string) error {
	_, span := tracer.Start(ctx0, "InsertSearchData")
	defer span.End()
	startTime := log.StartTime("InsertSearchData")

	var searchTable string
	if tableType == "nft" {
		searchTable = "Digital_Assets_NFT_Search_Data" + os.Getenv("DATA_NAMESPACE")
	} else if tableType == "asset" {
		searchTable = "Digital_Assets_Coin_Search_Data" + os.Getenv("DATA_NAMESPACE")
	} else {
		return errors.New("invalid table type")
	}
	searchData := SearchData{SearchTerm: searchTerm, Results: strconv.Itoa(results), CreatedAt: time.Now()}
	bqInserter := bq.Dataset("digital_assets").Table(searchTable).Inserter()
	bqInserter.IgnoreUnknownValues = true

	// Using context.Background() because the main ctx cancled before the function ends and the inserter is still running in the background
	// So we need to use a new context to make sure the inserter is not canceled and the data is inserted.
	inserterErr := bqInserter.Put(context.Background(), searchData)
	if inserterErr != nil {
		span.SetStatus(otelCodes.Error, inserterErr.Error())
		log.EndTime("InsertSearchData error ", startTime, inserterErr)
		return inserterErr
	}

	span.SetStatus(codes.Ok, "InsertSearchData Completed")
	log.EndTime("InsertSearchData", startTime, nil)
	return nil
}

// Contains  all data associated with an article id
type ArticleContentResult struct {
	ArticleDetails BQArticleResult   `json:"articleDetails"`
	AuthorDetails  []BQAuthorsResult `json:"authorDetails"`
}

// Result of BQArticleQuery
type BQArticleResult struct {
	Title         string   `bigquery:"title" json:"title"`
	Image         string   `bigquery:"image" json:"image"`
	Url           string   `bigquery:"uri" json:"uri"`
	Description   string   `bigquery:"description" json:"description"`
	CoAuthors     []string `bigquery:"coAuthors" json:"coAuthors"`
	PrimaryAuthID string   `bigquery:"primaryAuthorID" json:"primaryAuthorID"`
}

// Result Of author Query
type BQAuthorsResult struct {
	ID                string `bigquery:"id" json:"id" firestore:"id"`
	AuthorName        string `bigquery:"name" json:"name" firestore:"name"`
	AuthorURI         string `bigquery:"uri" json:"uri" firestore:"uri"`
	AuthorType        string `bigquery:"authorType" json:"authorType" firestore:"authorType"`
	IsPrimaryAuthor   bool   `bigquery:"primaryAuthor" json:"primaryAuthor" firestore:"primaryAuthor"`
	IsDisabled        bool   `bigquery:"disabled" json:"disabled" firestore:"disabled"`
	SeniorContributor bool   `bigquery:"seniorContributor" json:"seniorContributor" firestore:"seniorContributor"`
	Type              string `bigquery:"type" json:"type" firestore:"type"`
}

// get content details, and authors of content by articleID
func (bq *BQStore) GetArticleContent(ctx0 context.Context, articleID string) (*ArticleContentResult, error) {
	ctx, span := tracer.Start(ctx0, "GetArticleContent")
	defer span.End()

	log.Debug("GetArticleContent")
	query := bq.Query(`
  	SELECT
	  authorGroup.primaryAuthor as primaryAuthorID, 
	  authorgroup.coAuthors,
	  title,
	  image,
	  uri,
	  description,
	  authorGroup.primaryAuthor as prim,

  	FROM
	  api-project-901373404215.Content.mv_content_latest 
  	WHERE
   	 id = '` + articleID + `'
	`)
	job, err := query.Run(ctx)
	if err != nil {
		return nil, err
	}

	log.Debug("GetArticleContent Query Job ID: %s", job.ID())

	status, err := job.Wait(ctx)
	if err != nil {
		return nil, err
	}

	if err := status.Err(); err != nil {
		return nil, err
	}

	it, err := job.Read(ctx)

	if err != nil {
		return nil, err
	}

	var content BQArticleResult

	for {

		err := it.Next(&content)
		if err == iterator.Done {
			break
		}

		if err != nil {
			return nil, err
		}

	}

	authorsDetails, err := bq.GetAuthorData(ctx, content)
	if err != nil {
		return nil, err
	}
	log.Info("CoinMarketData : %v", content)

	return &ArticleContentResult{ArticleDetails: content, AuthorDetails: authorsDetails}, nil
}

// get author details from all authors of an article
func (bq *BQStore) GetAuthorData(ctx0 context.Context, articleDetails BQArticleResult) ([]BQAuthorsResult, error) {
	ctx, span := tracer.Start(ctx0, "GetAuthorData")
	defer span.End()

	log.Debug("GetAuthorData")

	var (
		authorIDsFormatted []string
		inCondition        string
	)
	// wrap all author ids in single quotes
	// then build in contition string
	authorIDsFormatted = append(authorIDsFormatted, fmt.Sprintf("'%s'", articleDetails.PrimaryAuthID))
	for _, authorID := range articleDetails.CoAuthors {
		authorIDsFormatted = append(authorIDsFormatted, fmt.Sprintf("'%s'", authorID))
	}

	inCondition = strings.Join(authorIDsFormatted, ",")

	query := bq.Query(`
	SELECT
    	id,
    	name,
    	url as uri,
    	authorType,
		type,
		seniorContributor,
		disabled,

 	FROM
    	api-project-901373404215.Content.v_author_latest
   	where id in (` + inCondition + `)
	`)
	job, err := query.Run(ctx)
	if err != nil {
		return nil, err
	}

	log.Debug("GetAuthorData Query Job ID: %s", job.ID())

	status, err := job.Wait(ctx)
	if err != nil {
		return nil, err
	}

	if err := status.Err(); err != nil {
		return nil, err
	}

	it, err := job.Read(ctx)

	if err != nil {
		return nil, err
	}

	var authors []BQAuthorsResult

	for {
		var author BQAuthorsResult
		err := it.Next(&author)
		if err == iterator.Done {
			break
		}

		if err != nil {
			return nil, err
		}
		if author.ID == articleDetails.PrimaryAuthID {
			author.IsPrimaryAuthor = true
		}

		authors = append(authors, author)

	}
	log.Info("GetAuthorData : %v", authors)

	return authors, nil
}

type HistoricalCategories struct {
	Date                        time.Time                   `bigquery:"day_start" json:"day_start"`                                                       // it present the date for a category
	TotalPrice24H               float64                     `bigquery:"total_price" json:"total_price_24h"`                                               // it present the total price for all assets in a category
	TotalVolume24H              float64                     `bigquery:"total_volume" json:"total_volume_24h"`                                             // it present the total volume for all assets in a category
	TotalMarketCap24H           float64                     `bigquery:"total_market_cap" json:"total_market_cap_24h"`                                     // it present the total market cap for all assets in a category
	TotalPriceWeightIndex       float64                     `bigquery:"price_weight_index" json:"price_weight_index"`                                     // it present the index weight price for an asset and it calculated by multiple index weight market cap for asset by it's price
	TotalMarketCapWeightIndex   float64                     `bigquery:"market_cap_weight_index" json:"market_cap_weight_index"`                           // it present the index weight market cap for an asset and it calculated by divided market cap for asset to total market cap in category
	Divisor                     float64                     `bigquery:"divisor" json:"divisor"`                                                           // it present the constant value we will use to calculate the Market Cap Index Value and it calculated by divided total market cap on 1000 (this is a base value)
	MarketCapIndexValue24H      float64                     `bigquery:"market_cap_index_value_24h" json:"market_cap_index_value_24h"`                     // it present the index market cap value for a category and it is the change value in market cap
	MarketCapPercentage24H      float64                     `bigquery:"market_cap_percentage_24h" json:"market_cap_percentage_24h,omitempty"`             // it present the percentage change for market cap in 24h
	MarketCapIndexPercentage24H float64                     `bigquery:"market_cap_index_percentage_24h" json:"market_cap_index_percentage_24h,omitempty"` // it present the percentage change for market cap index value in 24h
	LastUpdated                 time.Time                   `bigquery:"last_updated" json:"last_updated"`                                                 // it present the last time this record updated
	Prices                      []HistoricalCategoriesSlice `bigquery:"beprices" json:"beprices,omitempty"`                                               // it present an array of object that contains price, symbol, volume and market cap for assets
	TotalTokens                 int                         `bigquery:"total_tokens" json:"total_tokens"`                                                 // it present the total number of tokens that exist in each category
	Name                        string                      `bigquery:"name" json:"name"`                                                                 // it present the name of category
	ID                          string                      `bigquery:"id" json:"id"`                                                                     // it present the id of category
	TopGainers                  []CategoryTopGainer         `bigquery:"top_gainers" json:"top_gainers"`                                                   // it present the top gainers in a category depends on market cap
}
type HistoricalCategoriesSlice struct {
	Symbol    string  `bigquery:"symbol" json:"symbol"`
	Price     float64 `bigquery:"price" json:"price"`
	MarketCap float64 `bigquery:"market_cap" json:"market_cap"`
}

var categoryHistoricalDataQuery = `
	SELECT
		day_start,
		SUM(price) AS total_price,
		SUM(market_cap) AS total_market_cap,
		SUM(volume) AS total_volume,
		ARRAY_AGG(STRUCT('symbol',
			symbol,
			'price',
			price,
			'marketCap',
			market_cap
			)) AS beprices
	FROM (
		SELECT
			symbol,
			TIMESTAMP_TRUNC(time, DAY) AS day_start,
			Max(price) AS price, -- Using MAX() to get one value per 24-hour interval
			MAX(MarketCap) AS market_cap,
			MAX(Volume) AS volume
		FROM (
			SELECT
				ID AS symbol,
				Occurance_Time AS time,
				CAST(AVG(Price) AS FLOAT64) AS price,
				CAST(AVG(MarketCap) AS FLOAT64) AS MarketCap,
				CAST(AVG(Volume) AS FLOAT64) AS Volume,
				ROW_NUMBER() OVER (PARTITION BY ID, TIMESTAMP_TRUNC(Occurance_Time, DAY)
				ORDER BY
					Occurance_Time) AS row_num
			FROM
				api-project-901373404215.digital_assets.Digital_Asset_MarketData c
			WHERE
				Occurance_Time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 366 day)
				AND ID in UNNEST(@coins)
				And MarketCap != 0
				AND Volume != 0
			GROUP BY 
				symbol,
				time
		) AS foo
		WHERE
			row_num = 1 -- Only select the first row within each 24-hour interval
		GROUP BY
			day_start,
			symbol
			ORDER BY
			day_start DESC ) AS fo
	GROUP BY
		day_start
	ORDER BY 
		day_start desc
`

func (bq *BQStore) BuildCategoriesHistoricalData(ctx0 context.Context, coinsCategory Categories, assetsMetaData map[string]AssetMetaData) ([]CategoryFundamental, error) {
	ctx, span := tracer.Start(ctx0, "BuildCategoriesHistoricalData")

	defer span.End()

	log.Debug("BuildCategoriesHistoricalData")

	query := bq.Query(categoryHistoricalDataQuery)

	query.Parameters = []bigquery.QueryParameter{
		{
			Name:  "coins",
			Value: coinsCategory.Coins,
		},
	}

	job, err := query.Run(ctx)
	if err != nil {
		return nil, err
	}
	log.Debug("BuildCategoriesHistoricalData Query Job ID: %s", job.ID())

	status, err := job.Wait(ctx)
	if err != nil {
		return nil, err
	}

	if err := status.Err(); err != nil {
		return nil, err
	}

	it, err := job.Read(ctx)

	if err != nil {
		return nil, err
	}
	var categories []HistoricalCategories
	for {
		var category HistoricalCategories

		err := it.Next(&category)
		if err == iterator.Done {
			break
		}

		if err != nil {
			return nil, err
		}

		categories = append(categories, category)
	}
	var historicalCategories []CategoryFundamental
	var divisor float64 = 0.0
	for _, category := range categories {
		var topGainers []CategoryTopGainer
		var totalPriceWeightIndex float64 = 0.0
		for _, priceObject := range category.Prices {
			var assetCat HistoricalCategoriesSlice
			var topGainer CategoryTopGainer

			assetMeta := assetsMetaData[priceObject.Symbol]
			topGainer.MarketCap = bigquery.NullFloat64{Float64: priceObject.MarketCap, Valid: true}
			topGainer.Logo = assetMeta.LogoURL
			topGainer.Symbol = assetMeta.ID
			topGainer.Slug = strings.ToLower(fmt.Sprintf("%s-%s", strings.ReplaceAll(assetMeta.Name, " ", "-"), assetMeta.ID))
			topGainers = append(topGainers, topGainer)

			assetCat.Symbol = priceObject.Symbol
			assetCat.MarketCap = priceObject.MarketCap
			assetCat.Price = priceObject.Price

			// PriceWeight is a value we need can calculate by divided price for an asset by total price for all assets in a category
			// PriceWeight := CheckAndConvertFloat((priceObject.Price / category.TotalPrice24H))
			// MarketCapWeight is a value we need can calculate by divided MarketCap for an asset by total marketcap for all assets in a category
			MarketCapWeight := (priceObject.MarketCap / category.TotalMarketCap24H)
			// totalPriceWeightIndex is Summation for MarketCapWeight multiple by Price for asset
			// totalPriceWeightIndex we need it to calculate totalMarketCapWeightIndex
			totalPriceWeightIndex += MarketCapWeight * assetCat.Price

		}

		// Divisor is a value we need to calculate so we can calculate the Index Value from it.
		// To calculate Divisor we need the TotalMarketCap24H for category that will divided by Base Value.
		// The Base Value can be 100 or 1000 to calculate.
		// For our calculation we will use 1000 as base value.
		// Divisor will calculated from the oldest value.
		if divisor == 0 {
			divisor = CheckAndConvertFloat(category.TotalMarketCap24H / 1000)
		}

		categoryData := MapCategoryHistoricalFundamental(category, coinsCategory, totalPriceWeightIndex, divisor, topGainers)
		categoryData.Date = bigquery.NullTimestamp{Timestamp: category.Date, Valid: true}
		categoryData.LastUpdated = bigquery.NullTimestamp{Timestamp: time.Now(), Valid: true}

		historicalCategories = append(historicalCategories, categoryData)
	}
	var categoriesResult []CategoryFundamental
	for i := 0; i < len(historicalCategories); i++ {
		y := historicalCategories[i]
		if i == len(historicalCategories)-1 {
			y.AveragePercentage24H = bigquery.NullFloat64{Float64: 0, Valid: true}
			y.MarketCapPercentage24H = bigquery.NullFloat64{Float64: 0, Valid: true}
			y.MarketCapIndexPercentage24H = bigquery.NullFloat64{Float64: 0, Valid: true}
		} else {
			y.AveragePercentage24H = bigquery.NullFloat64{Float64: CalculateValuePercentage(ConvertBQFloatToNormalFloat(y.Price24H), ConvertBQFloatToNormalFloat(historicalCategories[i+1].Price24H), "percentage_24h"), Valid: true}
			y.MarketCapPercentage24H = bigquery.NullFloat64{Float64: CalculateValuePercentage(ConvertBQFloatToNormalFloat(y.MarketCap), ConvertBQFloatToNormalFloat(historicalCategories[i+1].MarketCap), ""), Valid: true}
			y.MarketCapIndexPercentage24H = bigquery.NullFloat64{Float64: CalculateValuePercentage(ConvertBQFloatToNormalFloat(y.MarketCapIndexValue24H), ConvertBQFloatToNormalFloat(historicalCategories[i+1].MarketCapIndexValue24H), ""), Valid: true}
		}

		categoriesResult = append(categoriesResult, y)
	}
	log.Info("BuildCategoriesHistoricalData  Finished Successfully")
	return categoriesResult, nil
}

func MapCategoryHistoricalFundamental(category HistoricalCategories, coinsCategory Categories, totalPriceWeightIndex float64, divisor float64, topGainers []CategoryTopGainer) CategoryFundamental {
	var categoryData CategoryFundamental
	var totalMarketCapWeightIndex float64 = 0.0
	// totalMarketCapWeightIndex is a value we need to calculate the Divisor.
	// We need to multiple TotalMarketCap24H for category by totalPriceWeightIndex
	totalMarketCapWeightIndex = (category.TotalMarketCap24H * totalPriceWeightIndex) / category.TotalMarketCap24H
	// indexValue it will present the index value change for market cap in 24 hour
	// So we can use it to measure the changes in MarketCap value.
	var indexValue float64 = 0.0
	if category.TotalMarketCap24H == 0 {
		indexValue = 0
	} else {
		indexValue = category.TotalMarketCap24H / divisor // this will present the index value change for market cap // MarketCapIndexValue
	}

	categoryData.ID = coinsCategory.ID
	categoryData.Name = coinsCategory.Name
	categoryData.TotalTokens = bigquery.NullInt64{Int64: int64(len(category.Prices)), Valid: true}
	categoryData.Volume24H = bigquery.NullFloat64{Float64: CheckAndConvertFloat(category.TotalVolume24H), Valid: true}
	categoryData.Price24H = bigquery.NullFloat64{Float64: CheckAndConvertFloat(category.TotalPrice24H), Valid: true}
	categoryData.AveragePrice = bigquery.NullFloat64{Float64: CheckAndConvertFloat(category.TotalPrice24H / float64(len(category.Prices))), Valid: true}
	categoryData.MarketCap = bigquery.NullFloat64{Float64: CheckAndConvertFloat(category.TotalMarketCap24H), Valid: true}
	categoryData.WeightIndexPrice = bigquery.NullFloat64{Float64: CheckAndConvertFloat(totalPriceWeightIndex), Valid: true}
	categoryData.WeightIndexMarketCap = bigquery.NullFloat64{Float64: CheckAndConvertFloat(totalMarketCapWeightIndex), Valid: true}
	categoryData.MarketCapIndexValue24H = bigquery.NullFloat64{Float64: CheckAndConvertFloat(indexValue), Valid: true}
	categoryData.Divisor = bigquery.NullFloat64{Float64: divisor, Valid: true}
	sort.Slice(topGainers, func(i, j int) bool {
		return topGainers[i].MarketCap.Float64 > topGainers[j].MarketCap.Float64
	})
	topGainersLen := len(topGainers)
	if topGainersLen > 3 {
		topGainersLen = 3
	}
	categoryData.TopGainers = topGainers[0:topGainersLen]
	return categoryData
}

func CheckAndConvertFloat(v float64) float64 {
	if math.IsNaN(v) {
		return 0
	}
	convValue, _ := strconv.ParseFloat(fmt.Sprintf("%.6f", v), 64)
	return convValue
}

func CalculateValuePercentage(newValue float64, oldValue float64, name string) float64 {
	var indexCalculation float64
	if name == "percentage_24h" {
		indexCalculation = (newValue - oldValue) / oldValue
	} else {
		indexCalculation = (newValue - oldValue) / oldValue * 100
	}
	result := CheckAndConvertFloat(indexCalculation)
	return result
}

func (bq *BQStore) GetCategoriesHistoricalData(ctx0 context.Context) (map[string]CategoryFundamental, error) {
	ctx, span := tracer.Start(ctx0, "GetCategoriesHistoricalData")
	defer span.End()

	log.Debug("GetCategoriesHistoricalData")

	categoriesTableName := GetTableName("Digital_Assets_Categories_Historical_data")

	query := bq.Query(`
	SELECT
		id,
		name,
		total_tokens,
		average_percentage_24h,
		volume_24h,
		price_24h,
		average_price,
		market_cap_24h,
		market_cap_percentage_change,
		price_weight_index,
		market_cap_weight_index,
		index_market_cap_24h,
		index_market_cap_percentage_24h,
		divisor,
		Date,
		row_last_updated
		FROM (
		SELECT
			id,
			name,
			total_tokens,
			average_percentage_24h,
			volume_24h,
			price_24h,
			average_price,
			market_cap_24h,
			market_cap_percentage_change,
			price_weight_index,
			market_cap_weight_index,
			index_market_cap_24h,
			index_market_cap_percentage_24h,
			divisor,
			Date,
			row_last_updated,
			ROW_NUMBER() OVER (PARTITION BY id ORDER BY Date DESC) AS row_num
		FROM
			api-project-901373404215.digital_assets.` + categoriesTableName + ` )
		WHERE
		row_num = 1
		ORDER BY
		id ASC
	`)
	job, err := query.Run(ctx)
	if err != nil {
		return nil, err
	}

	log.Debug("GetCategoriesHistoricalData Query Job ID: %s", job.ID())

	status, err := job.Wait(ctx)
	if err != nil {
		return nil, err
	}

	if err := status.Err(); err != nil {
		return nil, err
	}

	it, err := job.Read(ctx)

	if err != nil {
		return nil, err
	}

	categoriesHistorical := make(map[string]CategoryFundamental)

	for {
		var category CategoryFundamental
		err := it.Next(&category)
		if err == iterator.Done {
			break
		}

		if err != nil {
			return nil, err
		}

		categoriesHistorical[category.ID] = category

	}
	log.Info("GetCategoriesHistoricalData Finished Successfully")
	return categoriesHistorical, nil

}

// GetNFTSalesQuery Pulls all historical sales data for NFTS
// organizes it by 1d,7d,30d, and 90d intervals
func (bq *BQStore) GetNFTSalesInfo(ctx context.Context) ([]FundamentalsNFTSalesData, error) {
	log.Debug("Building GetNFTSalesInfo")

	nftsTableName := GetTableName("Digital_Assets_NFT_MarketData")

	query := bq.Query(`
	WITH
  oneday AS (
  SELECT
    id,
	
	SUM(marketCap_usd) marketCap_usd,
	SUM(marketCap_native) marketCap_native,
    AVG(fl_natitve) floorprice_native,
	AVG(floorprice_usd) floorprice_usd,
	AVG(fl_natitve) avg_floor_price_1d,
    AVG(one_day_average_sale_price) avg_sale_price_1d,
    SUM(one_day_sales) total_sales_1d,
  FROM (
    SELECT
      id,
      occurance_time,
      floorPrice_native,
      one_day_average_sale_price,
      one_day_sales,
	  marketCap_usd,
	  marketCap_native,
	  floorprice_usd,
	  floorprice_native as fl_natitve,
      ROW_NUMBER() OVER (PARTITION BY id ORDER BY occurance_time DESC) AS rn
    FROM
      api-project-901373404215.digital_assets.` + nftsTableName + `
    WHERE
      DATE(occurance_time) = CURRENT_DATE() 
	  and one_day_sales != 0
	)
  WHERE
    rn = 1
  GROUP BY
    id ),
  sevenDay AS (
  SELECT
    id,
    AVG(floorPrice_native) avg_floor_price_7d,
    AVG(one_day_average_sale_price) avg_sale_price_7d,
    SUM(one_day_sales) total_sales_7d,
  FROM (
    SELECT
      id,
      occurance_time,
      floorPrice_native,
      one_day_average_sale_price,
      one_day_sales,
      one_day_sales_24h_percentage_change,
      one_day_average_sale_price_24h_percentage_change,
      ROW_NUMBER() OVER (PARTITION BY id, DATE(occurance_time)
      ORDER BY
        occurance_time DESC) AS rn
    FROM
      api-project-901373404215.digital_assets.` + nftsTableName + ` )
  WHERE
    rn = 1
	and one_day_sales != 0
    AND occurance_time >= CURRENT_TIMESTAMP - INTERVAL 7 day
  GROUP BY
    id ),
  thirtyDay AS (
  SELECT
    id,
    AVG(floorPrice_native) avg_floor_price_30d,
    AVG(one_day_average_sale_price) avg_sale_price_30d,
    SUM(one_day_sales) total_sales_30d,
  FROM (
    SELECT
      id,
      occurance_time,
      floorPrice_native,
      one_day_average_sale_price,
      one_day_sales,
      one_day_sales_24h_percentage_change,
      one_day_average_sale_price_24h_percentage_change,
      ROW_NUMBER() OVER (PARTITION BY id, DATE(occurance_time)
      ORDER BY
        occurance_time DESC) AS rn
    FROM
      api-project-901373404215.digital_assets.` + nftsTableName + ` )
  WHERE
    rn = 1
	and one_day_sales !=0
    AND occurance_time >= CURRENT_TIMESTAMP - INTERVAL 30 day
  GROUP BY
    id ),
  ninetyDay AS (
  SELECT
    id,
    AVG(floorPrice_native) avg_floor_price_90d,
    AVG(one_day_average_sale_price) avg_sale_price_90d,
    SUM(one_day_sales) total_sales_90d,
  FROM (
    SELECT
      id,
      occurance_time,
      floorPrice_native,
      one_day_average_sale_price,
      one_day_sales,
      ROW_NUMBER() OVER (PARTITION BY id, DATE(occurance_time)
      ORDER BY
        occurance_time DESC) AS rn
    FROM
      api-project-901373404215.digital_assets.` + nftsTableName + ` )
  WHERE
    rn = 1
	and one_day_sales !=0
    AND occurance_time >= CURRENT_TIMESTAMP - INTERVAL 90 day
  GROUP BY
    id ),
  YTD AS (
      SELECT
    id,
    AVG(floorPrice_native) avg_floor_price_ytd,
    AVG(one_day_average_sale_price) avg_sale_price_ytd,
    SUM(one_day_sales) total_sales_ytd,
  FROM (
    SELECT
      id,
      occurance_time,
      floorPrice_native,
      one_day_average_sale_price,
      one_day_sales,
      ROW_NUMBER() OVER (PARTITION BY id, DATE(occurance_time)
      ORDER BY
        occurance_time DESC) AS rn
    FROM
      api-project-901373404215.digital_assets.` + nftsTableName + ` )
  WHERE
    rn = 1
	and one_day_sales !=0
    And
    occurance_time >= CAST(DATE_TRUNC(CURRENT_DATE(), year) AS Timestamp)
  GROUP BY
    id )
SELECT
  YTD.id AS id,
  floorprice_usd,
  floorprice_native,
  marketCap_usd,
  marketCap_native,
  avg_floor_price_1d,
  avg_sale_price_1d,
  total_sales_1d,
  avg_floor_price_7d,
  avg_sale_price_7d,
  total_sales_7d,
  avg_floor_price_30d,
  avg_sale_price_30d,
  total_sales_30d,
  avg_floor_price_90d,
  avg_sale_price_90d,
  total_sales_90d,
  avg_floor_price_ytd,
  avg_sale_price_ytd,
  total_sales_ytd
FROM
  YTD
LEFT JOIN
  ninetyDay
ON
  YTD.id = ninetyDay.id
LEFT JOIN
  thirtyDay
ON
  YTD.id = thirtyday.id
LEFT JOIN
  sevenDay
ON
  YTD.id = sevenday.id
LEFT JOIN
  oneDay
ON
  YTD.id = oneDay.id
where YTD.id = 'official-larva'
`)

	job, err := query.Run(ctx)
	if err != nil {
		return nil, err
	}

	log.Debug("GetNFTSalesInfo Query Job ID: %s", job.ID())

	status, err := job.Wait(ctx)
	if err != nil {
		return nil, err
	}

	if err := status.Err(); err != nil {
		return nil, err
	}

	it, err := job.Read(ctx)
	if err != nil {
		return nil, err
	}

	var fundamentalsResults []FundamentalsNFTSalesData
	for {
		var fundamentalsResult FundamentalsNFTSalesData
		err := it.Next(&fundamentalsResult)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		fundamentalsResults = append(fundamentalsResults, fundamentalsResult)

	}

	log.Info("GetNFTSalesInfo for %d symbols built", len(fundamentalsResults))

	return fundamentalsResults, nil
}

// GetNFTVolumePctInfo Pulls all historical volume data for NFTS
// organizes it by 1d,7d,30d, and 90d intervals
func (bq *BQStore) GetNFTVolumePctInfo(ctx context.Context) (map[string]FundamentalsNFTSalesData, error) {
	log.Debug("Building GetNFTVolumePctInfo")

	nftsTableName := GetTableName("Digital_Assets_NFT_MarketData")

	query := bq.Query(`
  WITH
  partitioneddata AS (
  SELECT
    id,
    occurance_time,
    volumeNative,
    volumeUSD,
    one_day_sales,
    one_day_average_sale_price,
    one_day_average_sale_price_24h_percentage_change,
    ROW_NUMBER() OVER (PARTITION BY id, DATE(occurance_time)
    ORDER BY
      occurance_time DESC) AS rn_most_recent
  FROM
    api-project-901373404215.digital_assets.` + nftsTableName + `
  WHERE
    DATE(occurance_time) >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR), YEAR)
    AND one_day_sales IS NOT NULL
    AND one_day_sales != 0
  ORDER BY
    occurance_time DESC )
SELECT
  ytd.id,
  IFNULL(((volume_native_ytd - prev_volume_native_ytd) / NULLIF(prev_volume_native_ytd, 0)), 0 ) AS pct_change_volume_native_ytd,
  IFNULL(((volume_usd_ytd - prev_volume_usd_ytd) / NULLIF(prev_volume_usd_ytd, 0)), 0 ) AS pct_change_volume_usd_ytd,
  IFNULL(((volume_native_90d- prev_volume_native_90d) / NULLIF(prev_volume_native_90d, 0)), 0 ) AS pct_change_volume_native_90d,
  IFNULL(((volume_usd_90d- prev_volume_usd_90d) / NULLIF(prev_volume_usd_90d, 0)), 0 ) AS pct_change_volume_usd_90d,
  IFNULL(((volume_native_30d- prev_volume_native_30d) / NULLIF(prev_volume_native_30d, 0)), 0 ) AS pct_change_volume_native_30d,
  IFNULL(((volume_usd_30d- prev_volume_usd_30d) / NULLIF(prev_volume_usd_30d, 0)), 0 ) AS pct_change_volume_usd_30d,
  IFNULL(((volume_native_7d - prev_volume_native_7d) / NULLIF(prev_volume_native_7d, 0)), 0 ) AS pct_change_volume_native_7d,
  IFNULL(((volume_usd_7d- prev_volume_usd_7d) / NULLIF(prev_volume_usd_7d, 0)), 0 ) AS pct_change_volume_usd_7d,
  IFNULL(volume_native_1d,0),
  IFNULL(((volume_native_1d- prev_volume_native_1d) / NULLIF(prev_volume_native_1d, 0)), 0 ) AS pct_change_volume_native_1d,
  IFNULL(((volume_usd_1d- prev_volume_usd_1d) / NULLIF(prev_volume_usd_1d, 0)), 0 ) AS pct_change_volume_usd_1d,
  IFNULL(((price_ytd - prev_price_ytd) / NULLIF(prev_price_ytd, 0)), 0 ) AS avg_sales_price_change_ytd,
  IFNULL(((price_90d- prev_price_90d) / NULLIF(prev_price_90d, 0)), 0 ) AS avg_sales_price_change_90d,
  IFNULL(((price_30d- prev_price_30d) / NULLIF(prev_price_30d, 0)), 0 ) AS avg_sales_price_change_30d,
  IFNULL(((price_7d - prev_price_7d) / NULLIF(prev_price_7d, 0)), 0 ) AS avg_sales_price_change_7d,
  IFNULL(one_day_average_sale_price_24h_percentage_change / 100,0) AS avg_sales_price_change_1d,
  IFNULL(((one_day_sales_ytd - prev_one_day_sales_ytd) / NULLIF(prev_one_day_sales_ytd, 0)), 0 ) AS avg_total_sales_pct_change_ytd,
  IFNULL(((one_day_sales_90d - prev_one_day_sales_90d) / NULLIF(prev_one_day_sales_90d, 0)), 0 ) AS avg_total_sales_pct_change_90d,
  IFNULL(((one_day_sales_30d - prev_one_day_sales_30d) / NULLIF(prev_one_day_sales_30d, 0)), 0 ) AS avg_total_sales_pct_change_30d,
  IFNULL(((one_day_sales_7d - prev_one_day_sales_7d) / NULLIF(prev_one_day_sales_7d, 0)), 0 ) AS avg_total_sales_pct_change_7d,
  IFNULL(((one_day_sales_1d - prev_one_day_sales_1d) / NULLIF(prev_one_day_sales_1d, 0)), 0 ) AS avg_total_sales_pct_change_1d,
FROM (
  SELECT
    id,
    AVG(CAST(volumeNative AS FLOAT64)) AS prev_volume_native_ytd,
    AVG(CAST(volumeUSD AS FLOAT64)) AS prev_volume_usd_ytd,
    AVG(one_day_average_sale_price) prev_price_ytd,
    AVG(one_day_sales) prev_one_day_sales_ytd
  FROM
    partitioneddata
  WHERE
    DATE(occurance_time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR)
    AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
    AND rn_most_recent = 1
    AND one_day_sales != 0
  GROUP BY
    id ) previousYTD
RIGHT JOIN (
  SELECT
    id,
    AVG(CAST(volumeNative AS FLOAT64)) AS volume_native_ytd,
    AVG(CAST(volumeUSD AS FLOAT64)) AS volume_usd_ytd,
    AVG(one_day_average_sale_price) price_ytd,
    AVG(one_day_sales) one_day_sales_ytd
  FROM
    partitioneddata
  WHERE
    occurance_time >= CAST(DATE_TRUNC(CURRENT_DATE(), year) AS Timestamp)
    AND rn_most_recent = 1
    AND one_day_sales != 0
  GROUP BY
    id ) YTD
ON
  previousYTD.id = YTD.id
LEFT JOIN (
  SELECT
    id,
    AVG(CAST(volumeNative AS FLOAT64)) AS prev_volume_native_90d,
    AVG(CAST(volumeUSD AS FLOAT64)) AS prev_volume_usd_90d,
    AVG(one_day_average_sale_price) prev_price_90d,
    AVG(one_day_sales) prev_one_day_sales_90d,
  FROM
    partitioneddata
  WHERE
    DATE(occurance_time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY)
    AND DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    AND rn_most_recent = 1
    AND one_day_sales != 0
  GROUP BY
    id ) previousNinetyDay
ON
  previousNinetyDay.id = YTD.id
LEFT JOIN (
  SELECT
    id,
    AVG(CAST(volumeNative AS FLOAT64)) AS volume_native_90d,
    AVG(CAST(volumeUSD AS FLOAT64)) AS volume_usd_90d,
    AVG(one_day_average_sale_price) price_90d,
    AVG(one_day_sales) one_day_sales_90d
  FROM
    partitioneddata
  WHERE
    occurance_time >= CURRENT_TIMESTAMP - INTERVAL 90 day
    AND rn_most_recent = 1
    AND one_day_sales != 0
  GROUP BY
    id ) ninetyDay
ON
  ninetyDay.id = YTD.id
LEFT JOIN (
  SELECT
    id,
    AVG(CAST(volumeNative AS FLOAT64)) AS prev_volume_native_30d,
    AVG(CAST(volumeUSD AS FLOAT64)) AS prev_volume_usd_30d,
    AVG(one_day_average_sale_price) prev_price_30d,
    AVG(one_day_sales) prev_one_day_sales_30d,
  FROM
    partitioneddata
  WHERE
    DATE(occurance_time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY) -- 90 days + 90 days
    AND DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) -- LAST 90 days
    AND rn_most_recent = 1
    AND one_day_sales != 0
  GROUP BY
    id ) previous30Day
ON
  previous30Day.id = YTD.id
LEFT JOIN (
  SELECT
    id,
    AVG(CAST(volumeNative AS FLOAT64)) AS volume_native_30d,
    AVG(CAST(volumeUSD AS FLOAT64)) AS volume_usd_30d,
    AVG(one_day_average_sale_price) price_30d,
    AVG(one_day_sales) one_day_sales_30d,
  FROM
    partitioneddata
  WHERE
    occurance_time >= CURRENT_TIMESTAMP - INTERVAL 30 day
    AND rn_most_recent = 1
    AND one_day_sales != 0
  GROUP BY
    id ) thirtyDay
ON
  thirtyDay.id = YTD.id
LEFT JOIN (
  SELECT
    id,
    AVG(volumeNative) AS prev_volume_native_7d,
    AVG(CAST(volumeUSD AS FLOAT64)) AS prev_volume_usd_7d,
    AVG(one_day_average_sale_price) prev_price_7d,
    AVG(one_day_sales) prev_one_day_sales_7d,
  FROM
    partitioneddata
  WHERE
    occurance_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
    AND occurance_time <= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    AND rn_most_recent = 1
    AND one_day_sales IS NOT NULL
  GROUP BY
    id ) previous7Day
ON
  previous7Day.id = YTD.id
LEFT JOIN (
  SELECT
    id,
    AVG(CAST(volumeNative AS FLOAT64)) AS volume_native_7d,
    AVG(CAST(volumeUSD AS FLOAT64)) AS volume_usd_7d,
    AVG(one_day_average_sale_price) price_7d,
    AVG(one_day_sales) one_day_sales_7d,
  FROM
    partitioneddata
  WHERE
    occurance_time >= CURRENT_TIMESTAMP - INTERVAL 7 day
    AND rn_most_recent = 1
    AND one_day_sales != 0
  GROUP BY
    id ) sevenDay
ON
  sevenDay.id = YTD.id
LEFT JOIN (
  SELECT
    id,
    AVG(CAST(volumeNative AS FLOAT64)) AS prev_volume_native_1d,
    AVG(CAST(volumeUSD AS FLOAT64)) AS prev_volume_usd_1d,
    AVG(one_day_average_sale_price) prev_price_1d,
    AVG(one_day_sales) prev_one_day_sales_1d,
  FROM
    partitioneddata
  WHERE
    DATE(occurance_time) >= DATE_SUB(CURRENT_DATE(),INTERVAL 1 day)
    AND rn_most_recent = 1
  GROUP BY
    id ) previousDay
ON
  previousDay.id = YTD.id
LEFT JOIN (
  SELECT
    id,
    AVG(CAST(volumeNative AS FLOAT64)) AS volume_native_1d,
    AVG(CAST(volumeUSD AS FLOAT64)) AS volume_usd_1d,
    AVG(one_day_average_sale_price) price_1d,
    AVG(one_day_average_sale_price_24h_percentage_change) one_day_average_sale_price_24h_percentage_change,
    AVG(one_day_sales) one_day_sales_1d
  FROM
    partitioneddata
  WHERE
    DATE(occurance_time) = CURRENT_DATE()
    AND rn_most_recent = 1
  GROUP BY
    id ) oneDay
ON
  oneDay.id = YTD.id
where YTD.id = 'official-larva'
ORDER BY
  YTD.id
  `)

	job, err := query.Run(ctx)
	if err != nil {
		return nil, err
	}

	log.Debug("GetNFTVolumePctInfo Query Job ID: %s", job.ID())

	status, err := job.Wait(ctx)
	if err != nil {
		return nil, err
	}

	if err := status.Err(); err != nil {
		return nil, err
	}

	it, err := job.Read(ctx)
	if err != nil {
		return nil, err
	}

	fundamentalsResults := make(map[string]FundamentalsNFTSalesData)
	for {
		var fundamentalsResult FundamentalsNFTSalesData
		err := it.Next(&fundamentalsResult)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		fundamentalsResults[fundamentalsResult.ID] = fundamentalsResult

	}

	log.Info("GetNFTVolumePctInfo for %d symbols built", len(fundamentalsResults))

	return fundamentalsResults, nil
}

// GetNFTVolumeInfo Pulls all historical volume data for NFTS
// organizes it by 1d,7d,30d, and 90d intervals
func (bq *BQStore) GetNFTVolumeInfo(ctx context.Context) (map[string]FundamentalsNFTSalesData, error) {
	log.Debug("Building GetNFTVolumeInfo")

	nftsTableName := GetTableName("Digital_Assets_NFT_MarketData")

	query := bq.Query(`
		WITH 
			oneDay_volume AS (
			SELECT
				id,
				Sum(volumeUSD) as volume_usd_1d,
				Sum(volumeNative) as volume_native_1d
			FROM(
				SELECT 
					id,
					volumeUSD,
					volumeNative,
					occurance_time,
					ROW_NUMBER() OVER (PARTITION BY id ORDER BY occurance_time DESC) AS rn
				FROM 
					api-project-901373404215.digital_assets.` + nftsTableName + `
				WHERE 
					DATE(occurance_time) = CURRENT_DATE()
			)
			WHERE 
				rn = 1
			Group BY 
				id
			),
			sevenDay_volume AS (
				SELECT
					id,
					Sum(volumeUSD) as volume_usd_7d,
					Sum(volumeNative) as volume_native_7d
				FROM(
					SELECT 
						id,
						volumeUSD,
						volumeNative,
						occurance_time,
						ROW_NUMBER() OVER (PARTITION BY id, DATE(occurance_time) 
										ORDER BY ABS(TIMESTAMP_DIFF(occurance_time, 
														TIMESTAMP(DATE(occurance_time)), SECOND))) AS rn
					FROM 
						api-project-901373404215.digital_assets.` + nftsTableName + `
					WHERE 
						occurance_time >= CURRENT_TIMESTAMP - INTERVAL 7 day
				)
				WHERE 
					rn = 1
				Group BY 
					id 
			),
			thirtyDay_volume AS (
				SELECT
					id,
					Sum(volumeUSD) as volume_usd_30d,
					Sum(volumeNative) as volume_native_30d
				FROM(
					SELECT 
						id,
						volumeUSD,
						volumeNative,
						occurance_time,
						ROW_NUMBER() OVER (PARTITION BY id, DATE(occurance_time) 
										ORDER BY ABS(TIMESTAMP_DIFF(occurance_time, 
														TIMESTAMP(DATE(occurance_time)), SECOND))) AS rn
					FROM 
						api-project-901373404215.digital_assets.` + nftsTableName + `
					WHERE 
					occurance_time >= CURRENT_TIMESTAMP - INTERVAL 30 day
				)
				WHERE 
					rn = 1
				GROUP BY 
					id
			),
			ninetyDay_volume AS (
				SELECT
					id,
					Sum(volumeUSD) as volume_usd_90d,
					Sum(volumeNative) as volume_native_90d
				FROM(
					SELECT 
						id,
						volumeUSD,
						volumeNative,
						occurance_time,
						ROW_NUMBER() OVER (PARTITION BY id, DATE(occurance_time) 
										ORDER BY ABS(TIMESTAMP_DIFF(occurance_time, 
														TIMESTAMP(DATE(occurance_time)), SECOND))) AS rn
					FROM 
						api-project-901373404215.digital_assets.` + nftsTableName + `
					WHERE 
						occurance_time >= CURRENT_TIMESTAMP - INTERVAL 90 day
				)
				WHERE 
					rn = 1
				GROUP BY 
					id
			),
			ytdDay_volume AS (
				SELECT
					id,
					Sum(volumeUSD) as volume_usd_ytd,
					Sum(volumeNative) as volume_native_ytd
				FROM(
					SELECT 
						id,
						volumeUSD,
						volumeNative,
						occurance_time,
						ROW_NUMBER() OVER (PARTITION BY id, DATE(occurance_time) 
										ORDER BY ABS(TIMESTAMP_DIFF(occurance_time, 
														TIMESTAMP(DATE(occurance_time)), SECOND))) AS rn
					FROM 
						api-project-901373404215.digital_assets.` + nftsTableName + `
					WHERE 
						occurance_time >= CAST(DATE_TRUNC(CURRENT_DATE(), year) AS Timestamp)
				)
				WHERE 
					rn = 1
				GROUP BY 
					id
			)
			SELECT
				ytdDay_volume.id,
				volume_usd_1d,
				volume_native_1d,
				volume_usd_7d,
				volume_native_7d,
				volume_usd_30d,
				volume_native_30d,
				volume_usd_90d,
				volume_native_90d,
				volume_usd_ytd,
				volume_native_ytd
			FROM
				ytdDay_volume
				LEFT JOIN
				ninetyDay_volume
				ON
				ytdDay_volume.id = ninetyDay_volume.id
				LEFT JOIN
				thirtyDay_volume
				ON
				ytdDay_volume.id = thirtyDay_volume.id
				LEFT JOIN
				sevenDay_volume
				ON
				ytdDay_volume.id = sevenDay_volume.id
				LEFT JOIN
				oneDay_volume
				ON
				ytdDay_volume.id = oneDay_volume.id
				where oneDay_volume.id = 'official-larva'

`)

	job, err := query.Run(ctx)
	if err != nil {
		return nil, err
	}

	log.Debug("GetNFTVolumeInfo Query Job ID: %s", job.ID())

	status, err := job.Wait(ctx)
	if err != nil {
		return nil, err
	}

	if err := status.Err(); err != nil {
		return nil, err
	}

	it, err := job.Read(ctx)
	if err != nil {
		return nil, err
	}

	fundamentalsResults := make(map[string]FundamentalsNFTSalesData)
	for {
		var fundamentalsResult FundamentalsNFTSalesData
		err := it.Next(&fundamentalsResult)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		fundamentalsResults[fundamentalsResult.ID] = fundamentalsResult

	}

	log.Info("GetNFTVolumeInfo for %d symbols built", len(fundamentalsResults))

	return fundamentalsResults, nil
}

// GetNFTFloorPriceInfo Pulls all historical floor price data for NFTS
// organizes it by 1d,7d,30d, and 90d and ytd intervals
func (bq *BQStore) GetNFTFloorPriceInfo(ctx context.Context) (map[string]FundamentalsNFTSalesData, error) {
	log.Debug("Building GetNFTFloorPriceInfo")
	nftsTableName := GetTableName("Digital_Assets_NFT_MarketData")
	query := bq.Query(`
with high_low_price_oneDay AS (
  select
    id,
    (SELECT MIN(floorpriceusd) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS lowest_floor_price_24h_usd,
    (SELECT MAX(floorpriceusd) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS highest_floor_price_24h_usd,
    (SELECT MIN(floorpricenative) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS lowest_floor_price_24h_native,
    (SELECT MAX(floorpricenative) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS highest_floor_price_24h_native,
    ((beprices_1d[OFFSET(ARRAY_LENGTH(beprices_1d) - 1)].floorpriceusd - beprices_1d[OFFSET(0)].floorpriceusd) / NULLIF(beprices_1d[OFFSET(0)].floorpriceusd, 0)) AS floor_price_24h_percentage_change_usd,
    ((beprices_1d[OFFSET(ARRAY_LENGTH(beprices_1d) - 1)].floorpricenative - beprices_1d[OFFSET(0)].floorpricenative) / NULLIF(beprices_1d[OFFSET(0)].floorpricenative, 0)) AS floor_price_24h_percentage_change_native,
    (SELECT MAX((cur.floorpriceusd - beprices_1d[OFFSET(off-1)].floorpriceusd) / NULLIF(beprices_1d[OFFSET(off-1)].floorpriceusd, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS highest_floor_price_24h_percentage_change_usd,
    (SELECT MAX((cur.floorpricenative - beprices_1d[OFFSET(off-1)].floorpricenative) / NULLIF(beprices_1d[OFFSET(off-1)].floorpricenative, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS highest_floor_price_24h_percentage_change_native,
    (SELECT MIN((cur.floorpriceusd - beprices_1d[OFFSET(off-1)].floorpriceusd) / NULLIF(beprices_1d[OFFSET(off-1)].floorpriceusd, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS lowest_floor_price_24h_percentage_change_usd,
    (SELECT MIN((cur.floorpricenative - beprices_1d[OFFSET(off-1)].floorpricenative) / NULLIF(beprices_1d[OFFSET(off-1)].floorpricenative, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS lowest_floor_price_24h_percentage_change_native
  from(
    SELECT
      id,
      ARRAY_AGG(STRUCT( 'Time',
          time,
          'floorpriceusd',
          floorpriceusd,
          'floorpricenative',
          floorpricenative )
      ORDER BY
        time) AS beprices_1d,
    FROM (
      SELECT
        id,
        Occurance_Time AS time,
        CAST(floorprice_usd AS FLOAT64) floorpriceusd,
        CAST(floorprice_native AS FLOAT64) floorpricenative,
        ROW_NUMBER() OVER (PARTITION BY ID, CAST(FLOOR(UNIX_SECONDS(Occurance_Time)/ 900 )AS INT64 )
        ORDER BY
          Occurance_Time ) AS row_num
      FROM
        api-project-901373404215.digital_assets.` + nftsTableName + ` c
      WHERE
        Occurance_Time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 day)
      ORDER BY
        Occurance_Time ) AS test
    WHERE
      row_num = 1
    GROUP BY
      id )
  ),
  high_low_price_sevenDay AS (
  select
    id,
    (SELECT MIN(floorpriceusd) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS lowest_floor_price_7d_usd,
    (SELECT MAX(floorpriceusd) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS highest_floor_price_7d_usd,
    (SELECT MIN(floorpricenative) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS lowest_floor_price_7d_native,
    (SELECT MAX(floorpricenative) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS highest_floor_price_7d_native,
    ((beprices_1d[OFFSET(ARRAY_LENGTH(beprices_1d) - 1)].floorpriceusd - beprices_1d[OFFSET(0)].floorpriceusd) / NULLIF(beprices_1d[OFFSET(0)].floorpriceusd, 0)) AS floor_price_7d_percentage_change_usd,
    ((beprices_1d[OFFSET(ARRAY_LENGTH(beprices_1d) - 1)].floorpricenative - beprices_1d[OFFSET(0)].floorpricenative) / NULLIF(beprices_1d[OFFSET(0)].floorpricenative, 0)) AS floor_price_7d_percentage_change_native,
    (SELECT MAX((cur.floorpriceusd - beprices_1d[OFFSET(off-1)].floorpriceusd) / NULLIF(beprices_1d[OFFSET(off-1)].floorpriceusd, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS highest_floor_price_7d_percentage_change_usd,
    (SELECT MAX((cur.floorpricenative - beprices_1d[OFFSET(off-1)].floorpricenative) / NULLIF(beprices_1d[OFFSET(off-1)].floorpricenative, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS highest_floor_price_7d_percentage_change_native,
    (SELECT MIN((cur.floorpriceusd - beprices_1d[OFFSET(off-1)].floorpriceusd) / NULLIF(beprices_1d[OFFSET(off-1)].floorpriceusd, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS lowest_floor_price_7d_percentage_change_usd,
    (SELECT MIN((cur.floorpricenative - beprices_1d[OFFSET(off-1)].floorpricenative) / NULLIF(beprices_1d[OFFSET(off-1)].floorpricenative, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS lowest_floor_price_7d_percentage_change_native
  from(
    SELECT
      id,
      ARRAY_AGG(STRUCT( 'Time',
          time,
          'floorpriceusd',
          floorpriceusd,
          'floorpricenative',
          floorpricenative )
      ORDER BY
        time) AS beprices_1d,
    FROM (
      SELECT
        id,
        Occurance_Time AS time,
        CAST(floorprice_usd AS FLOAT64) floorpriceusd,
        CAST(floorprice_native AS FLOAT64) floorpricenative,
        ROW_NUMBER() OVER (PARTITION BY ID, CAST(FLOOR(UNIX_SECONDS(Occurance_Time)/ 14400 )AS INT64 )
        ORDER BY
          Occurance_Time ) AS row_num
      FROM
        api-project-901373404215.digital_assets.` + nftsTableName + ` c
      WHERE
        Occurance_Time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 day)
      ORDER BY
        Occurance_Time ) AS test
    WHERE
      row_num = 1
    GROUP BY
      id )
  ),
  high_low_price_thirtyDay AS (
  select
    id,
    (SELECT MIN(floorpriceusd) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS lowest_floor_price_30d_usd,
    (SELECT MAX(floorpriceusd) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS highest_floor_price_30d_usd,
    (SELECT MIN(floorpricenative) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS lowest_floor_price_30d_native,
    (SELECT MAX(floorpricenative) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS highest_floor_price_30d_native,
    ((beprices_1d[OFFSET(ARRAY_LENGTH(beprices_1d) - 1)].floorpriceusd - beprices_1d[OFFSET(0)].floorpriceusd) / NULLIF(beprices_1d[OFFSET(0)].floorpriceusd, 0)) AS floor_price_30d_percentage_change_usd,
    ((beprices_1d[OFFSET(ARRAY_LENGTH(beprices_1d) - 1)].floorpricenative - beprices_1d[OFFSET(0)].floorpricenative) / NULLIF(beprices_1d[OFFSET(0)].floorpricenative, 0)) AS floor_price_30d_percentage_change_native,
    (SELECT MAX((cur.floorpriceusd - beprices_1d[OFFSET(off-1)].floorpriceusd) / NULLIF(beprices_1d[OFFSET(off-1)].floorpriceusd, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS highest_floor_price_30d_percentage_change_usd,
    (SELECT MAX((cur.floorpricenative - beprices_1d[OFFSET(off-1)].floorpricenative) / NULLIF(beprices_1d[OFFSET(off-1)].floorpricenative, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS highest_floor_price_30d_percentage_change_native,
    (SELECT MIN((cur.floorpriceusd - beprices_1d[OFFSET(off-1)].floorpriceusd) / NULLIF(beprices_1d[OFFSET(off-1)].floorpriceusd, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS lowest_floor_price_30d_percentage_change_usd,
    (SELECT MIN((cur.floorpricenative - beprices_1d[OFFSET(off-1)].floorpricenative) / NULLIF(beprices_1d[OFFSET(off-1)].floorpricenative, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS lowest_floor_price_30d_percentage_change_native
  from(
    SELECT
      id,
      ARRAY_AGG(STRUCT( 'Time',
          time,
          'floorpriceusd',
          floorpriceusd,
          'floorpricenative',
          floorpricenative )
      ORDER BY
        time) AS beprices_1d,
    FROM (
      SELECT
        id,
        Occurance_Time AS time,
        CAST(floorprice_usd AS FLOAT64) floorpriceusd,
        CAST(floorprice_native AS FLOAT64) floorpricenative,
        ROW_NUMBER() OVER (PARTITION BY ID, CAST(FLOOR(UNIX_SECONDS(Occurance_Time)/ 43200 )AS INT64 )
        ORDER BY
          Occurance_Time ) AS row_num
      FROM
        api-project-901373404215.digital_assets.` + nftsTableName + ` c
      WHERE
        Occurance_Time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 day)
      ORDER BY
        Occurance_Time ) AS test
    WHERE
      row_num = 1
    GROUP BY
      id )
  ),
  high_low_price_ninetyDay AS (
  select
    id,
    (SELECT MIN(floorpriceusd) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS lowest_floor_price_90d_usd,
    (SELECT MAX(floorpriceusd) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS highest_floor_price_90d_usd,
    (SELECT MIN(floorpricenative) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS lowest_floor_price_90d_native,
    (SELECT MAX(floorpricenative) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS highest_floor_price_90d_native,
    ((beprices_1d[OFFSET(ARRAY_LENGTH(beprices_1d) - 1)].floorpriceusd - beprices_1d[OFFSET(0)].floorpriceusd) / NULLIF(beprices_1d[OFFSET(0)].floorpriceusd, 0)) AS floor_price_90d_percentage_change_usd,
    ((beprices_1d[OFFSET(ARRAY_LENGTH(beprices_1d) - 1)].floorpricenative - beprices_1d[OFFSET(0)].floorpricenative) / NULLIF(beprices_1d[OFFSET(0)].floorpricenative, 0)) AS floor_price_90d_percentage_change_native,
    (SELECT MAX((cur.floorpriceusd - beprices_1d[OFFSET(off-1)].floorpriceusd) / NULLIF(beprices_1d[OFFSET(off-1)].floorpriceusd, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS highest_floor_price_90d_percentage_change_usd,
    (SELECT MAX((cur.floorpricenative - beprices_1d[OFFSET(off-1)].floorpricenative) / NULLIF(beprices_1d[OFFSET(off-1)].floorpricenative, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS highest_floor_price_90d_percentage_change_native,
    (SELECT MIN((cur.floorpriceusd - beprices_1d[OFFSET(off-1)].floorpriceusd) / NULLIF(beprices_1d[OFFSET(off-1)].floorpriceusd, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS lowest_floor_price_90d_percentage_change_usd,
    (SELECT MIN((cur.floorpricenative - beprices_1d[OFFSET(off-1)].floorpricenative) / NULLIF(beprices_1d[OFFSET(off-1)].floorpricenative, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS lowest_floor_price_90d_percentage_change_native
  from(
    SELECT
      id,
      ARRAY_AGG(STRUCT( 'Time',
          time,
          'floorpriceusd',
          floorpriceusd,
          'floorpricenative',
          floorpricenative )
      ORDER BY
        time) AS beprices_1d,
    FROM (
      SELECT
        id,
        Occurance_Time AS time,
        CAST(floorprice_usd AS FLOAT64) floorpriceusd,
        CAST(floorprice_native AS FLOAT64) floorpricenative,
        ROW_NUMBER() OVER (PARTITION BY ID, CAST(FLOOR(UNIX_SECONDS(Occurance_Time)/ 432000 )AS INT64 )
        ORDER BY
          Occurance_Time ) AS row_num
      FROM
        api-project-901373404215.digital_assets.` + nftsTableName + ` c
      WHERE
        Occurance_Time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 day)
      ORDER BY
        Occurance_Time ) AS test
    WHERE
      row_num = 1
    GROUP BY
      id )
  ),
  high_low_price_ytd AS (
  select
    id,
    (SELECT MIN(floorpriceusd) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS lowest_floor_price_ytd_usd,
    (SELECT MAX(floorpriceusd) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS highest_floor_price_ytd_usd,
    (SELECT MIN(floorpricenative) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS lowest_floor_price_ytd_native,
    (SELECT MAX(floorpricenative) AS max_price FROM UNNEST(beprices_1d) AS price_struct) AS highest_floor_price_ytd_native,
    ((beprices_1d[OFFSET(ARRAY_LENGTH(beprices_1d) - 1)].floorpriceusd - beprices_1d[OFFSET(0)].floorpriceusd) / NULLIF(beprices_1d[OFFSET(0)].floorpriceusd, 0)) AS floor_price_ytd_percentage_change_usd,
    ((beprices_1d[OFFSET(ARRAY_LENGTH(beprices_1d) - 1)].floorpricenative - beprices_1d[OFFSET(0)].floorpricenative) / NULLIF(beprices_1d[OFFSET(0)].floorpricenative, 0)) AS floor_price_ytd_percentage_change_native,
    (SELECT MAX((cur.floorpriceusd - beprices_1d[OFFSET(off-1)].floorpriceusd) / NULLIF(beprices_1d[OFFSET(off-1)].floorpriceusd, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS highest_floor_price_ytd_percentage_change_usd,
    (SELECT MAX((cur.floorpricenative - beprices_1d[OFFSET(off-1)].floorpricenative) / NULLIF(beprices_1d[OFFSET(off-1)].floorpricenative, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS highest_floor_price_ytd_percentage_change_native,
    (SELECT MIN((cur.floorpriceusd - beprices_1d[OFFSET(off-1)].floorpriceusd) / NULLIF(beprices_1d[OFFSET(off-1)].floorpriceusd, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS lowest_floor_price_ytd_percentage_change_usd,
    (SELECT MIN((cur.floorpricenative - beprices_1d[OFFSET(off-1)].floorpricenative) / NULLIF(beprices_1d[OFFSET(off-1)].floorpricenative, 0))
     FROM UNNEST(beprices_1d) cur WITH OFFSET off
     WHERE off > 0
    ) AS lowest_floor_price_ytd_percentage_change_native
  from(
    SELECT
      id,
      ARRAY_AGG(STRUCT( 'Time',
          time,
          'floorpriceusd',
          floorpriceusd,
          'floorpricenative',
          floorpricenative )
      ORDER BY
        time) AS beprices_1d,
    FROM (
      SELECT
        id,
        Occurance_Time AS time,
        CAST(floorprice_usd AS FLOAT64) floorpriceusd,
        CAST(floorprice_native AS FLOAT64) floorpricenative,
        ROW_NUMBER() OVER (PARTITION BY ID, CAST(FLOOR(UNIX_SECONDS(Occurance_Time)/ 1296000 )AS INT64 )
        ORDER BY
          Occurance_Time ) AS row_num
      FROM
        api-project-901373404215.digital_assets.` + nftsTableName + ` c
      WHERE
        Occurance_time >= CAST(DATE_TRUNC(CURRENT_DATE(), year) AS Timestamp)
      ORDER BY
        Occurance_Time ) AS test
    WHERE
      row_num = 1
    GROUP BY
      id )
  )

select
    high_low_price_ytd.id,

    lowest_floor_price_24h_usd,
    lowest_floor_price_24h_native,
    highest_floor_price_24h_usd,
    highest_floor_price_24h_native,
    floor_price_24h_percentage_change_usd,
    floor_price_24h_percentage_change_native,
    lowest_floor_price_24h_percentage_change_usd,
    lowest_floor_price_24h_percentage_change_native,
    highest_floor_price_24h_percentage_change_usd,
    highest_floor_price_24h_percentage_change_native,

    lowest_floor_price_7d_usd,
    lowest_floor_price_7d_native,
    highest_floor_price_7d_usd,
    highest_floor_price_7d_native,
    floor_price_7d_percentage_change_usd,
    floor_price_7d_percentage_change_native,
    lowest_floor_price_7d_percentage_change_usd,
    lowest_floor_price_7d_percentage_change_native,
    highest_floor_price_7d_percentage_change_usd,
    highest_floor_price_7d_percentage_change_native,

    lowest_floor_price_30d_usd,
    lowest_floor_price_30d_native,
    highest_floor_price_30d_usd,
    highest_floor_price_30d_native,
    floor_price_30d_percentage_change_usd,
    floor_price_30d_percentage_change_native,
    lowest_floor_price_30d_percentage_change_usd,
    lowest_floor_price_30d_percentage_change_native,
    highest_floor_price_30d_percentage_change_usd,
    highest_floor_price_30d_percentage_change_native,

    lowest_floor_price_90d_usd,
    lowest_floor_price_90d_native,
    highest_floor_price_90d_usd,
    highest_floor_price_90d_native,
    floor_price_90d_percentage_change_usd,
    floor_price_90d_percentage_change_native,
    lowest_floor_price_90d_percentage_change_usd,
    lowest_floor_price_90d_percentage_change_native,
    highest_floor_price_90d_percentage_change_usd,
    highest_floor_price_90d_percentage_change_native,

    lowest_floor_price_ytd_usd,
    lowest_floor_price_ytd_native,
    highest_floor_price_ytd_usd,
    highest_floor_price_ytd_native,
    floor_price_ytd_percentage_change_usd,
    floor_price_ytd_percentage_change_native,
    lowest_floor_price_ytd_percentage_change_usd,
    lowest_floor_price_ytd_percentage_change_native,
    highest_floor_price_ytd_percentage_change_usd,
    highest_floor_price_ytd_percentage_change_native,
from 
  high_low_price_ytd
  LEFT JOIN
    high_low_price_ninetyDay
  ON
    high_low_price_ytd.id = high_low_price_ninetyDay.id
  LEFT JOIN
    high_low_price_thirtyDay
  ON
    high_low_price_ytd.id = high_low_price_thirtyDay.id
  LEFT JOIN
    high_low_price_sevenDay
  ON
    high_low_price_ytd.id = high_low_price_sevenDay.id
  LEFT JOIN
    high_low_price_oneDay
  ON
    high_low_price_ytd.id = high_low_price_oneDay.id
where  high_low_price_oneDay.id = 'official-larva'
`)

	job, err := query.Run(ctx)
	if err != nil {
		return nil, err
	}

	log.Debug("GetNFTFloorPriceInfo Query Job ID: %s", job.ID())

	status, err := job.Wait(ctx)
	if err != nil {
		return nil, err
	}

	if err := status.Err(); err != nil {
		return nil, err
	}

	it, err := job.Read(ctx)
	if err != nil {
		return nil, err
	}

	fundamentalsResults := make(map[string]FundamentalsNFTSalesData)
	for {
		var fundamentalsResult FundamentalsNFTSalesData
		err := it.Next(&fundamentalsResult)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		fundamentalsResults[fundamentalsResult.ID] = fundamentalsResult

	}

	log.Info("GetNFTFloorPriceInfo for %d symbols built", len(fundamentalsResults))

	return fundamentalsResults, nil
}

// GetFundamentalsFirstOccuranceTime retrieves the first occurrence time of fundamentals for a given UUID.
// It executes a BigQuery to fetch the minimum occurrence time for each symbol in the Digital_Asset_MarketData table.
//
// Parameters:
//   - ctx0: The context for managing request-scoped values, cancelation, and deadlines.
//   - uuid: The unique identifier for the request.
//
// Returns:
//   - A map where the key is the symbol and the value is the PGFundamentalsResult containing the first occurrence time.
//   - An error if the query execution or result processing fails.
//
// The function also logs and traces various stages of the query execution and result processing.

func (bq *BQStore) GetFundamentalsFirstOccuranceTime(ctx0 context.Context, uuid string) (map[string]PGFundamentalsResult, error) {
	ctx, span := tracer.Start(ctx0, "GetFundamentalsFirstOccuranceTime")
	defer span.End()
	labels := make(map[string]string)

	labels["uuid"] = uuid
	span.SetAttributes(attribute.String("uuid", uuid))
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	labels["bigquery"] = "true"
	span.SetAttributes(attribute.Bool("bigquery", true))
	labels["subFunction"] = "GetFundamentalsFirstOccuranceTime"

	marketTableName := GetTableName("Digital_Asset_MarketData")

	query := bq.Query(`
	SELECT
  		forbes_id,
 		 MIN(occurance_time) AS date_added
			FROM
  		api-project-901373404215.digital_assets.` + marketTableName + ` c
		GROUP BY
  		forbes_id
	`)

	job, err := query.Run(ctx)

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	labels["GetFundamentalsFirstOccuranceTime"] = job.ID()
	span.SetAttributes(attribute.String("GetFundamentalsFirstOccuranceTime", job.ID()))

	log.DebugL(labels, "FundamentalsCG Query Job ID: %s", job.ID())

	status, err := job.Wait(ctx)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	span.AddEvent("GetFundamentalsFirstOccuranceTime Query Job Complete")

	if err := status.Err(); err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	it, err := job.Read(ctx)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	fundamentalsResults := make(map[string]PGFundamentalsResult)
	for {
		var fundamentalsResult PGFundamentalsResult
		err := it.Next(&fundamentalsResult)
		if err == iterator.Done {
			break
		}
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}

		fundamentalsResults[fundamentalsResult.ForbesID] = fundamentalsResult

	}

	log.InfoL(labels, "GetFundamentalsFirstOccuranceTime for %d symbols built", len(fundamentalsResults))

	span.SetStatus(codes.Ok, "GetFundamentalsFirstOccuranceTime Built")

	return fundamentalsResults, nil
}
