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
				  approx_quantiles(close, 4) [offset(3) ] + (1.5 * (approx_quantiles(close, 4) [offset(3) ] - approx_quantiles(close, 4) [offset(1) ])) as upperfence, 
				  approx_quantiles(close, 4) [offset(1) ] - (1.5 * (approx_quantiles(close, 4) [offset(3) ] - approx_quantiles(close, 4) [offset(1) ])) as lowerfence, 
				  lower(Base) as symbol 
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
				  lower(Base) as symbol, 
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
	  )


array_to_json(ARRAY_AGG(json_build_object(
                                        'base', market.Symbol, 
                                        'exchange', market.exchange, 
                                        'quote', market.quote, 
                                        'pair', market.pair, 												 
										'pairStatus', assets.status, 
										'update_timestamp', assets.last_updated,
                                        'type', ticker.type,
                                        'current_price_for_pair_1d', CAST(oneDay.current_price_for_pair_1d AS FLOAT),
                                        'current_price_for_pair_7d', CAST(sevenDays.current_price_for_pair_7d AS FLOAT),
                                        'current_price_for_pair_30d', CAST(thirtyDays.current_price_for_pair_30d AS FLOAT),
                                        'current_price_for_pair_1y', CAST(oneYear.current_price_for_pair_1y AS FLOAT),
                                        'current_price_for_pair_ytd', CAST(YTD.current_price_for_pair_ytd AS FLOAT),
                                        'volume_for_pair_1d', CAST(oneDay.volume_for_pair_1d AS FLOAT),
                                        'volume_for_pair_7d', CAST(sevenDays.volume_for_pair_7d AS FLOAT),
                                        'volume_for_pair_30d', CAST(thirtyDays.volume_for_pair_30d AS FLOAT),
                                        'volume_for_pair_1y', CAST(oneYear.volume_for_pair_1y AS FLOAT),
                                        'volume_for_pair_ytd', CAST(YTD.volume_for_pair_ytd AS FLOAT)
                                        ))) as MarketPairs
    






select
			authorId,
			authorName,
			contentId,
			contentTitle,
			sum(views) as views,
			sum(temporaryViews) as temporaryViews,
			sum(views + temporaryViews) as totalViews
from (
			select
				authorId,
				authorName,
				contentId,
				contentTitle,
				GA_date,
				IFNULL(views, 0) as views,
				IF(views > 0, 0, IFNULL(temporaryViews, 0)) as temporaryViews
			from (
				select authorId, authorName, contentId, contentTitle, GA_date, sum(pageViews) as views
				from `forbes-tamagotchi.Tamagotchi.v_Main6`
				where GA_date >= DATE(2022,8,1) and GA_date < DATE(2022,9,1) and retracted is not true and visible is true
				group by authorId, authorName, contentId, contentTitle, GA_date
			) as historic
			join (
					select authorId, authorName, contentId, contentTitle, GA_date, sum(pageViews) as temporaryViews
					from `forbes-tamagotchi.Tamagotchi.v_Main6_RealTime`
					where GA_date >= (
						select IFNULL(max(GA_date), DATE(2022,8,1))
						from `forbes-tamagotchi.Tamagotchi.v_Main6` 
						where GA_date >= DATE(2022,8,1) and GA_date < DATE(2022,9,1)
					) and GA_date < DATE(2022,9,1) and retracted is not true and visible is true
					group by authorId, authorName, contentId, contentTitle, GA_date
			) as realTime
			using (authorId, authorName, contentId, contentTitle, GA_date)
		)
		group by  authorId, authorName, contentId, contentTitle
	


with 
	historic (

		select
			authorId,
			authorName,
			contentId,
			contentTitle,
			GA_date,
			IFNULL(views, 0) as views,
			IF(views > 0, 0, IFNULL(temporaryViews, 0)) as temporaryViews
		from (
			select authorId, authorName, contentId, contentTitle, GA_date, sum(pageViews) as views
			from `forbes-tamagotchi.Tamagotchi.v_Main6`
			where GA_date >= DATE(2022,8,1) and GA_date < DATE(2022,9,1) and retracted is not true and visible is true
			group by authorId, authorName, contentId, contentTitle, GA_date
		)

	), realTime
	(
		select authorId, authorName, contentId, contentTitle, GA_date, sum(pageViews) as temporaryViews
		from `forbes-tamagotchi.Tamagotchi.v_Main6_RealTime`
		where GA_date >= (
			select IFNULL(max(GA_date), DATE(2022,8,1))
			from `forbes-tamagotchi.Tamagotchi.v_Main6` 
			where GA_date >= DATE(2022,8,1) and GA_date < DATE(2022,9,1)
		) and GA_date < DATE(2022,9,1) and retracted is not true and visible is true
		group by authorId, authorName, contentId, contentTitle, GA_date
	)

select 
	authorId,
			authorName,
			contentId,
			contentTitle,
			GA_date,
			IFNULL(views, 0) as views,
			IF(views > 0, 0, IFNULL(temporaryViews, 0)) as temporaryViews

from 
	historic
FULL JOIN 
	realTime
ON 
historic.authorId = realTime.authorId and 
historic.authorName = realTime.authorName and 
historic.contentId = realTime.contentId and 
historic.contentTitle = realTime.contentTitle and 
historic.GA_date = realTime.GA_date and











select 
  ga_fullVisitorID, GA_referralGroup, ave, total
from (
  select 
    subscribed_traffic.ga_fullVisitorID, traffic.GA_referralGroup, avg(distinct cast(traffic.session_id as FLOAT64)) AS ave, count(distinct(traffic.session_id)) as total
    from (
      SELECT
          ga_fullVisitorID, GA_visitStartTime,GA_referralGroup, concat(GA_fullVisitorId,GA_visitId) as session_id
      FROM  `api-project-901373404215.DataMart.v_DataMart`
    ) traffic 
    Join 
    (
      SELECT 
          ga_fullVisitorID, GA_visitStartTime, concat(GA_fullVisitorId,GA_visitId) as session_id
      FROM `api-project-901373404215.DataMart.v_events_datamart`
      WHERE GA_eventAction = "subscribesuccess" AND (GA_eventLabel LIKE "r8w03as%" OR GA_eventLabel LIKE "rkpevdb%")
    ) subscribed_traffic
    On (
      subscribed_traffic.ga_fullVisitorID = traffic.ga_fullVisitorID
      AND traffic.GA_visitStartTime< TIMESTAMP_SECONDS(subscribed_traffic.GA_visitStartTime)
    )
    GROUP BY
      subscribed_traffic.ga_fullVisitorID, traffic.GA_referralGroup
)



++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


package store

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Forbes-Media/forbes-digital-assets/log"
	"github.com/lib/pq"
)

var (
	pg           *sql.DB
	DBClientOnce sync.Once
)

type PGFundamentalsResult struct {
	Symbol         string        `postgres:"symbol" json:"symbol,omitempty"`
	ForbesSymbol   string        `postgres:"forbes" json:"forbesSymbol,omitempty"`
	Volume24H      *float64      `postgres:"volume_24h" json:"volume24h,omitempty"`
	High           *float64      `postgres:"high_24h" json:"high24h,omitempty"`
	Low            *float64      `postgres:"low_24h" json:"low24h,omitempty"`
	High7D         *float64      `postgres:"high_7d" json:"high7d,omitempty"`
	Low7D          *float64      `postgres:"low_7d" json:"low7d,omitempty"`
	High30D        *float64      `postgres:"high_30d" json:"high30d,omitempty"`
	Low30D         *float64      `postgres:"low_30d" json:"low30d,omitempty"`
	High1Y         *float64      `postgres:"high_1y" json:"high1y,omitempty"`
	Low1Y          *float64      `postgres:"low_1y" json:"low1y,omitempty"`
	HighYtd        *float64      `postgres:"high_ytd" json:"highYtd,omitempty"`
	LowYtd         *float64      `postgres:"low_ytd" json:"lowYtd,omitempty"`
	AllTimeLow     *float64      `postgres:"all_time_low" json:"allTimeLow,omitempty"`
	LastClosePrice *float64      `postgres:"last_close_price" json:"lastClosePrice,omitempty"`
	FirstOpenPrice *float64      `postgres:"first_open_price" json:"firstOpenPrice,omitempty"`
	MarketCap      string        `postgres:"market_cap" json:"marketCap,omitempty"`
	Supply         string        `postgres:"supply" json:"supply,omitempty"`
	Date           time.Time     `postgres:"last_price_time" json:"lastPriceTime,omitempty"`
	Exchanges      []PGExchange  `postgres:"exchanges" json:"exchanges,omitempty"`
	MarketPairs    []MarketPairs `postgres:"market_pairs" json:"market_pairs,omitempty"`
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
	Symbol                  string    `json:"symbol" firestore:"symbol" postgres:"symbol"`
	TargetResolutionSeconds int       `json:"targetResolutionSeconds" postgres:"target_resolution_seconds"`
	Slice                   []SlicePG `json:"prices" firestore:"prices" postgres:"prices"`
	IsIndex                 bool      `json:"isIndex" postgres:"is_index"`
	Source                  string    `json:"source" postgres:"source"`
	Interval                string    `json:"interval" postgres:"interval"`
}

type FundamentalsForbesPercentage struct {
	ForbesPercentage1D  *float64 `postgres:"forbes_percentage_1d" json:"forbes_percentage_1d,omitempty"`
	ForbesPercentage7D  *float64 `postgres:"forbes_percentage_7d" json:"forbes_percentage_7d,omitempty"`
	ForbesPercentage30D *float64 `postgres:"forbes_percentage_30d" json:"forbes_percentage_30d,omitempty"`
	ForbesPercentage1Y  *float64 `postgres:"forbes_percentage_1y" json:"forbes_percentage_1y,omitempty"`
	ForbesPercentageYTD *float64 `postgres:"forbes_percentage_ytd" json:"forbes_percentage_ytd,omitempty"`
}

type SlicePG struct {
	Time     time.Time `json:"Time" firestore:"x" postgres:"Time"`
	AvgClose float64   `json:"Price" firestore:"y" postgres:"Price"`
}

func PGConnect() *sql.DB {
	if pg == nil {
		var err error
		DBClientOnce.Do(func() {
			connectionString := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s", os.Getenv("DB_HOST"), os.Getenv("DB_PORT"), os.Getenv("DB_USER"), os.Getenv("DB_PASSWORD"), os.Getenv("DB_NAME"), os.Getenv("DB_SSLMODE"))

			pg, err = sql.Open("postgres", connectionString)

			if err != nil {
				log.Error("%s", err)
			}

			connectionError := pg.Ping()

			if connectionError != nil {
				log.Error("%s", connectionError)
			}
		})
	}
	return pg

}

type slicePGResult []SlicePG
type exchangeResult []PGExchange
type pairsResult []MarketPairs

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

func StartTime(message string) time.Time {
	startTime := time.Now().UTC()
	log.Debug("%s Process Started at :: %s", message, startTime)

	return startTime
}

func ConsumeTime(message string, startTime time.Time, err error) {
	endTime := time.Now()
	elapsed := time.Since(startTime)
	if err != nil {
		log.Debug("%s Error :: %s, Finished  at :: %s, Total execution time :: %s", message, err, endTime, elapsed)
	} else {
		log.Debug("%s Process, Finished at :: %s, Total execution time :: %s", message, endTime, elapsed)
	}
}

func BuildPGQuery() string {
	candlesTable := "nomics_ohlcv_candles"
	query := `
	with 
		allTime as 
			(
				SELECT 
					CAST(MIN(Close) AS FLOAT) all_time_low, 
					lower(base) as symbol
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
					lower(base) as symbol
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
							base
					) as oneDay
				GROUP BY 
				base
			),
		sevenDays AS 
			(
				SELECT 
					CAST(MAX(Close) AS FLOAT) high_7d, 
					CAST(MIN(Close) AS FLOAT) low_7d, 
					lower(base) as symbol
				FROM 
					( 
						SELECT 
							AVG(close) as Close, 
							base
						FROM 
							` + candlesTable + `
						WHERE 
							timestamp >=  cast(now() - INTERVAL '7 DAYS' as timestamp)
						GROUP BY 
							base
					) as sevenDays
				GROUP BY 
					base
			),
		thirtyDays AS 
			(
				SELECT 
					CAST(MAX(Close) AS FLOAT) high_30d, 
					CAST(MIN(Close) AS FLOAT) low_30d, 
					lower(base) as symbol
				FROM 
					(
						SELECT 
							AVG(close) as Close, 
							base
						FROM 
							` + candlesTable + `
						WHERE 
							timestamp >=  cast(now() - INTERVAL '30 DAYS' as timestamp)
						GROUP BY 
							base
					) as thirtyDays
				GROUP BY 
				base
			),
		oneYear AS 
			(
				SELECT 
					CAST(MAX(Close) AS FLOAT) high_1y, 
					CAST(MIN(Close) AS FLOAT) low_1y, 
					lower(base) as symbol
				FROM 
					( 
						SELECT 
							AVG(close) as Close, 
							base
						FROM 
							` + candlesTable + `
						WHERE 
							timestamp >=  cast(now() - INTERVAL '365 DAYS' as timestamp)
						GROUP BY 
							base
					) as oneYear
				GROUP BY 
					base
			),

		YTD AS 
			(
				SELECT 
					CAST(MAX(Close) AS FLOAT) high_ytd, 
					CAST(MIN(Close) AS FLOAT) low_ytd, 
					lower(base) as symbol
				FROM 
					( 
						SELECT 
							AVG(close) as Close, 
							base
						FROM 
							` + candlesTable + `
						WHERE 
							timestamp  >= cast(date_trunc('year', current_date) as timestamp) 
						GROUP BY 
							base
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
					lower(base) as symbol
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
					lower(base) as Symbol, 
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

// Fundamental Query to get data from PG table
func PGQueryFundamentals24h(queryCondition string) ([]PGFundamentalsResult, error) {

	pg := PGConnect()
	var fundamentalsResults []PGFundamentalsResult

	if queryCondition == "fundamentals" {
		startTime := StartTime("Fundamental Query")

		queryResult, err := pg.Query(BuildPGQuery())

		if err != nil {
			ConsumeTime("Fundamental Query", startTime, err)
			return nil, err
		}

		for queryResult.Next() {
			var pgFundResult PGFundamentalsResult

			err := queryResult.Scan(&pgFundResult.High, &pgFundResult.Low, &pgFundResult.High7D, &pgFundResult.Low7D, &pgFundResult.High30D, &pgFundResult.Low30D, &pgFundResult.High1Y, &pgFundResult.Low1Y, &pgFundResult.HighYtd, &pgFundResult.LowYtd, &pgFundResult.AllTimeLow, &pgFundResult.Symbol)
			if err != nil {
				ConsumeTime("Fundamental Query Scan", startTime, err)
				return nil, err
			}
			fundamentalsResults = append(fundamentalsResults, pgFundResult)
		}

		ConsumeTime("Fundamental Query", startTime, nil)

	}
	if queryCondition == "exchange-fundamentals" {
		startTime := StartTime("Exchange Fundamental Query")

		queryResult, err := pg.Query(BuildExchangeFundamentalsQuery())

		if err != nil {
			ConsumeTime("Exchange Fundamental Query", startTime, err)
			return nil, err
		}
		for queryResult.Next() {
			var pgFundResult PGFundamentalsResult

			err := queryResult.Scan((*exchangeResult)(&pgFundResult.Exchanges), &pgFundResult.Symbol)
			if err != nil {
				ConsumeTime("Exchange Fundamental Query Scan", startTime, err)
				return nil, err
			}
			fundamentalsResults = append(fundamentalsResults, pgFundResult)
		}

		ConsumeTime("Exchange Fundamental Query", startTime, nil)
	}

	return fundamentalsResults, nil
}

// insert Fundamentals data into PG Table
func InsertFundamentalData(fundamental FundamentalsData) error {

	startTime := StartTime("Fundamental Insert")

	pg := PGConnect()

	insertStatementsFundamentals := "INSERT INTO fundamentals(symbol, name, slug, logo, float_type, display_symbol, original_symbol, source, temporary_data_delay, number_of_active_market_pairs, high_24h, low_24h, high_7d, low_7d, high_30d, low_30d, high_1y, low_1y, high_ytd, low_ytd, price_24h, price_7d, price_30d, price_1y, price_ytd, percentage_24h, percentage_7d, percentage_30d, percentage_1y, percentage_ytd,  market_cap, market_cap_percent_change_1d, market_cap_percent_change_7d, market_cap_percent_change_30d, market_cap_percent_change_1y, market_cap_percent_change_ytd, circulating_supply, supply, all_time_low, all_time_high, date, change_value_24h, listed_exchange, market_pairs, exchanges, nomics, forbes) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47)"
	// convert Exchanges[] and Nomics into json type to make it easy to store in PG table
	exchanges, _ := json.Marshal(fundamental.Exchanges)
	nomics, _ := json.Marshal(fundamental.Nomics)
	market_pairs, _ := json.Marshal(fundamental.MarketPairs)
	forbes, _ := json.Marshal(fundamental.Forbes)

	_, insertError := pg.Exec(insertStatementsFundamentals, fundamental.Symbol, fundamental.Name, fundamental.Slug, fundamental.Logo, fundamental.FloatType, fundamental.DisplaySymbol, fundamental.OriginalSymbol, fundamental.Source, fundamental.TemporaryDataDelay, fundamental.NumberOfActiveMarketPairs, fundamental.High24h, fundamental.Low24h, fundamental.High7D, fundamental.Low7D, fundamental.High30D, fundamental.Low30D, fundamental.High1Y, fundamental.Low1Y, fundamental.HighYTD, fundamental.LowYTD, fundamental.Price24h, fundamental.Price7D, fundamental.Price30D, fundamental.Price1Y, fundamental.PriceYTD, fundamental.Percentage24h, fundamental.Percentage7D, fundamental.Percentage30D, fundamental.Percentage1Y, fundamental.PercentageYTD, fundamental.MarketCap, fundamental.MarketCapPercentChange1D, fundamental.MarketCapPercentChange7D, fundamental.MarketCapPercentChange30D, fundamental.MarketCapPercentChange1Y, fundamental.MarketCapPercentChangeYTD, fundamental.CirculatingSupply, fundamental.Supply, fundamental.AllTimeLow, fundamental.AllTimeHigh, fundamental.Date, fundamental.ChangeValue24h, pq.Array(fundamental.ListedExchanges), market_pairs, exchanges, nomics, forbes)

	if insertError != nil {
		ConsumeTime("Fundamental Insert", startTime, insertError)
		return insertError
	}

	ConsumeTime("Fundamental Insert", startTime, nil)
	return nil
}

func InsertExchangeFundamentalData(exchangeFundamentals []ExchangeFundamentals) error {

	startTime := StartTime("Exchange Fundamental Insert")

	pg := PGConnect()

	stringValue := make([]string, 0, len(exchangeFundamentals))
	exchangeValue := make([]interface{}, 0, len(exchangeFundamentals)*7)
	var i = 0
	tableName := "exchange_fundamentals"

	for y := 0; y < len(exchangeFundamentals); y++ {
		var exchange = exchangeFundamentals[y]

		var valString = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d)", i*7+1, i*7+2, i*7+3, i*7+4, i*7+5, i*7+6, i*7+7)
		stringValue = append(stringValue, valString)
		exchangeValue = append(exchangeValue, exchange.Name)
		exchangeValue = append(exchangeValue, exchange.Slug)
		exchangeValue = append(exchangeValue, exchange.Id)
		exchangeValue = append(exchangeValue, exchange.Logo)
		exchangeValue = append(exchangeValue, exchange.ExchangeActiveMarketPairs)
		Nomics, _ := json.Marshal(exchange.Nomics)
		Forbes, _ := json.Marshal(exchange.Forbes)
		exchangeValue = append(exchangeValue, Nomics)
		exchangeValue = append(exchangeValue, Forbes)
		i++

		if len(exchangeValue) >= 65000 || y == len(exchangeFundamentals)-1 {
			insertStatementExchangeFundamentals := fmt.Sprintf("INSERT INTO %s VALUES %s", tableName, strings.Join(stringValue, ","))
			_, inserterError := pg.Exec(insertStatementExchangeFundamentals, exchangeValue...)

			if inserterError != nil {
				ConsumeTime("Exchange Fundamental Insert", startTime, inserterError)
				return inserterError
			}
			stringValue = make([]string, 0, len(exchangeFundamentals))
			exchangeValue = make([]interface{}, 0, len(exchangeFundamentals)*7)
			i = 0

		}
	}
	ConsumeTime("Exchange Fundamental Insert", startTime, nil)

	return nil
}

func BuildNomicsQuery(base string) string {

	currenciesTickers := "nomics_currencies_tickers"
	currenciesTickersOneDay := "nomics_currencies_tickers_one_day"
	currenciesTickersSevenDays := "nomics_currencies_tickers_seven_days"
	currenciesTickersThirtyDays := "nomics_currencies_tickers_thirty_days"
	currenciesTickersOneYear := "nomics_currencies_tickers_one_year"
	currenciesTickersYTD := "nomics_currencies_tickers_ytd"
	TickerMetadata := "nomics_ticker_metadata"
	marketCapHistory := "nomics_market_cap_history"

	query := `
	with 
	allTime as (
			SELECT 
				CAST(MIN(circulating_supply) AS FLOAT) circulating_supply, 
				CAST(MIN(price) AS FLOAT) price24h,
				max_supply,
				num_pairs,
				CAST(MIN(marketcap) AS FLOAT) marketcap, 
				id
			FROM ` + currenciesTickers + `
			where 
				id = '` + base + `'
			group by 
				id,
				num_pairs,
				max_supply
			order by 
				marketcap desc
			limit 1
			),
	oneDay AS (
			SELECT 
				CAST(MIN(oneDay.volume) AS FLOAT) volume_24h, 
				CAST(MIN(oneDay.price_change) AS FLOAT) change_value_24h, 
				CAST(MIN(oneDay.price_change_pct) AS FLOAT) percentage_24h,
				oneDay.id,
				((SUM(oneDay.marketcap_change - history.market_cap) / sum(history.market_cap)))  as Market_cap_percent_change1D
			FROM 
				` + currenciesTickersOneDay + ` oneDay, 
				` + marketCapHistory + ` history
			where 
				history.timestamp >= cast(now() - INTERVAL '24 HOUR' as timestamp)
				and oneDay.last_updated >= cast(now() - INTERVAL '24 HOUR' as timestamp)
				and oneDay.id = '` + base + `'
			group by 
				oneDay.id
			),

	sevenDays AS (
			SELECT 
				CAST(MIN(sevenDays.price_change) AS FLOAT) price_7d, 
				CAST(MIN(sevenDays.price_change_pct) AS FLOAT) percentage_7d,
				sevenDays.id,
				((SUM(sevenDays.marketcap_change - history.market_cap) / sum(history.market_cap)))  as Market_cap_percent_change7D
			FROM 
				` + currenciesTickersSevenDays + ` sevenDays, 
				` + marketCapHistory + ` history
			where 
				sevenDays.id = '` + base + `'
				and history.timestamp >= cast(now() - INTERVAL '7 DAYS' as timestamp)
				and sevenDays.last_updated >= cast(now() - INTERVAL '7 DAYS' as timestamp)
			group by 
				sevenDays.id
			),
	thirtyDays AS (SELECT 
				CAST(MIN(thirtyDays.price_change) AS FLOAT) price_30d, 
				CAST(MIN(thirtyDays.price_change_pct) AS FLOAT) percentage_30d,
				thirtyDays.id,
				((SUM(thirtyDays.marketcap_change - history.market_cap) / sum(history.market_cap)))  as Market_cap_percent_change30D
			FROM 
				` + currenciesTickersThirtyDays + ` thirtyDays, 
				` + marketCapHistory + ` history
			where 
				thirtyDays.id = '` + base + `'
				and history.timestamp >= cast(now() - INTERVAL '30 DAYS' as timestamp)
				and thirtyDays.last_updated >= cast(now() - INTERVAL '30 DAYS' as timestamp)
			group by 
				thirtyDays.id
			),
	oneYear AS (SELECT 
				CAST(MIN(oneYear.price_change) AS FLOAT) price_1y, 
				CAST(MIN(oneYear.price_change_pct) AS FLOAT) percentage_1y,
				oneYear.id,
				((SUM(oneYear.marketcap_change - history.market_cap) / sum(history.market_cap)))  as Market_cap_percent_change1Y
			FROM 
				` + currenciesTickersOneYear + ` oneYear, 
				` + marketCapHistory + ` history
			where 
				oneYear.id = '` + base + `'
				and history.timestamp >= cast(now() - INTERVAL '365 DAYS' as timestamp)
				and oneYear.last_updated >= cast(now() - INTERVAL '365 DAYS' as timestamp)
			group by 
				oneYear.id
			),

	YTD AS (SELECT 
				CAST(MIN(ytd.price_change) AS FLOAT) price_ytd, 
				CAST(MIN(ytd.price_change_pct) AS FLOAT) percentage_ytd,
				ytd.id,
				((SUM(ytd.marketcap_change - history.market_cap) / sum(history.market_cap))) as Market_cap_percent_changeYTD
			FROM 
				` + currenciesTickersYTD + ` ytd, 
				` + marketCapHistory + ` history
			where 
				ytd.id = '` + base + `'
				and history.timestamp  >= cast(date_trunc('year', current_date) as timestamp)
				and ytd.last_updated  >= cast(date_trunc('year', current_date) as timestamp)
			group by 
				ytd.id
		),
	metaData AS (SELECT 
				original_symbol,
				id
			FROM 
				` + TickerMetadata + `
			where 
				id = '` + base + `'
		)
	select num_pairs,
			max_supply,
			metaData.original_symbol,
			CAST(MIN(allTime.circulating_supply) AS FLOAT) circulating_supply, 
			CAST(MIN(allTime.marketcap) AS FLOAT) marketcap, 
			CAST(MIN(allTime.price24h) AS FLOAT) price24h,
			CAST(MIN(sevenDays.price_7d) AS FLOAT) price_7d,
			CAST(MIN(thirtyDays.price_30d) AS FLOAT) price_30d, 
			CAST(MIN(oneYear.price_1y) AS FLOAT) price_1y, 
			CAST(MIN(YTD.price_ytd) AS FLOAT) price_ytd, 
			CAST(MIN(oneDay.volume_24h) AS FLOAT) volume_24h, 
			CAST(MIN(oneDay.change_value_24h) AS FLOAT) change_value_24h, 
			CAST(MIN(oneDay.percentage_24h) AS FLOAT) percentage_24h,
			CAST(MIN(sevenDays.percentage_7d) AS FLOAT) percentage_7d, 
			CAST(MIN(thirtyDays.percentage_30d) AS FLOAT) percentage_30d, 
			CAST(MIN(oneYear.percentage_1y) AS FLOAT) percentage_1y,
			CAST(MIN(YTD.percentage_ytd) AS FLOAT) percentage_ytd,
			CAST(oneDay.Market_cap_percent_change1D AS FLOAT) AS Market_cap_percent_change1D,
			CAST(sevenDays.Market_cap_percent_change7D AS FLOAT) AS Market_cap_percent_change7D,
			CAST(thirtyDays.Market_cap_percent_change30D AS FLOAT) AS Market_cap_percent_change30D,
			CAST(oneYear.Market_cap_percent_change1Y AS FLOAT) AS Market_cap_percent_change1Y,
			CAST(YTD.Market_cap_percent_changeYTD AS FLOAT) AS Market_cap_percent_changeYTD
		from allTime
				INNER JOIN 
					sevenDays 
				ON 
					sevenDays.id = allTime.id
				INNER JOIN 
					thirtyDays 
				ON 
					thirtyDays.id = allTime.id
				INNER JOIN 
					oneYear 
				ON 
					oneYear.id = allTime.id
				INNER JOIN 
					oneDay 
				ON 
					oneDay.id = allTime.id
				INNER JOIN 
					YTD 
				ON 
					YTD.id = allTime.id
				INNER JOIN 
					metaData 
				ON 
					metaData.id = allTime.id
		group by 
			allTime.id, 
			metaData.original_symbol, 
			num_pairs, max_supply,
			oneDay.Market_cap_percent_change1D,
			sevenDays.Market_cap_percent_change7D,
			thirtyDays.Market_cap_percent_change30D,
			oneYear.Market_cap_percent_change1Y,
			YTD.Market_cap_percent_changeYTD
	`

	return query

}

func PGQueryNomicsTickers(base string) (*PGNomicsResult, error) {

	startTime := StartTime("Nomics Tickers Query")

	pg := PGConnect()

	queryResult, err := pg.Query(BuildNomicsQuery(base))

	var nomics PGNomicsResult

	if err != nil {
		ConsumeTime("Nomics Tickers Query", startTime, err)
		return nil, err
	}
	defer queryResult.Close()

	for queryResult.Next() {
		err := queryResult.Scan(&nomics.NumberOfActiveMarketPairs, &nomics.MaxSupply, &nomics.OriginalSymbol, &nomics.CirculatingSupply, &nomics.MarketCap, &nomics.Price24h, &nomics.Price7D, &nomics.Price30D, &nomics.Price1Y, &nomics.PriceYTD, &nomics.Volume, &nomics.ChangeValue24h, &nomics.Percentage24h, &nomics.Percentage7D, &nomics.Percentage30D, &nomics.Percentage1Y, &nomics.PercentageYTD, &nomics.MarketCapPercentChange1D, &nomics.MarketCapPercentChange7D, &nomics.MarketCapPercentChange30D, &nomics.MarketCapPercentChange1Y, &nomics.MarketCapPercentChangeYTD)
		if err != nil {
			ConsumeTime("Nomics Tickers Query Scan", startTime, err)
			return nil, err
		}
	}

	ConsumeTime("Nomics Tickers Query", startTime, nil)

	return &nomics, nil
}

func BuildNomicsVolumeQuery(base string) string {

	currenciesTickersOneDay := "nomics_currencies_tickers_one_day"
	currenciesTickersSevenDays := "nomics_currencies_tickers_seven_days"
	currenciesTickersThirtyDays := "nomics_currencies_tickers_thirty_days"
	currenciesTickersOneYear := "nomics_currencies_tickers_one_year"
	currenciesTickersYTD := "nomics_currencies_tickers_ytd"
	query := `
	with 
	oneDay AS 
		(
			select 
				((max_volume - min_volume) / min_volume) as percentage_1d, 
				volume
			from 
				(
					select 
					volume,
						(select 
							max(volume)  
							from ` + currenciesTickersOneDay + ` 
							where last_updated >= cast(now() - INTERVAL '24 HOUR' as timestamp)
						 and id = '` + base + `'
						) as max_volume, 
						(select 
							min(volume)  
							from ` + currenciesTickersOneDay + ` 
							where last_updated <= cast(now() - INTERVAL '24 HOUR' as timestamp)
						 and id = '` + base + `'
						) as min_volume
					from 
						` + currenciesTickersOneDay + `
					where 
						last_updated >= cast(now() - INTERVAL '24 HOUR' as timestamp)
					and id = '` + base + `'
					group by volume, last_updated
					order by last_updated desc
					limit 1
				) as oneDay
		),

	sevenDays AS 
		(
			select 
				((max_volume - min_volume) / min_volume) as percentage_7d, 
				volume
			from 
				(
					select 
					volume,
						(select 
							max(volume)  
							from ` + currenciesTickersSevenDays + ` 
							where last_updated >= cast(now() - INTERVAL '7 DAYS' as timestamp)
						 and id = '` + base + `'
						) as max_volume, 
						(select 
							min(volume)  
							from ` + currenciesTickersSevenDays + ` 
							where last_updated >= cast(now() - INTERVAL '7 DAYS' as timestamp)
						 and id = '` + base + `'
						) as min_volume

					from 
						` + currenciesTickersSevenDays + `
					where 
						last_updated >= cast(now() - INTERVAL '7 DAYS' as timestamp)
					and id = '` + base + `'
					group by volume, last_updated
					order by last_updated desc
					limit 1
				) as sevenDays
		),
	thirtyDays AS 
		(
			select 
				((max_volume - min_volume) / min_volume) as percentage_30d, 
				volume
			from 
				(
					select 
					volume,
						(select 
							max(volume)  
							from ` + currenciesTickersThirtyDays + ` 
							where last_updated >= cast(now() - INTERVAL '30 DAYS' as timestamp)
						 and id = '` + base + `'
						) as max_volume, 
						(select 
							min(volume)  
							from ` + currenciesTickersThirtyDays + ` 
							where last_updated >= cast(now() - INTERVAL '30 DAYS' as timestamp)
						 and id = '` + base + `'
						) as min_volume
					from 
						` + currenciesTickersThirtyDays + `
					where 
						last_updated >= cast(now() - INTERVAL '30 DAYS' as timestamp)
					and id = '` + base + `'
					group by volume, last_updated
					order by last_updated desc
					limit 1
				) as thirtyDays
		),
	oneYear AS 
		(
			select 
				((max_volume - min_volume) / min_volume) as percentage_1y, 
				volume
			from 
				(
					select 
					volume,
						(select 
							max(volume)  
							from ` + currenciesTickersOneYear + ` 
							where last_updated >= cast(now() - INTERVAL '365 DAYS' as timestamp)
						 and id = '` + base + `'
						) as max_volume, 
						(select 
							min(volume)  
							from ` + currenciesTickersOneYear + ` 
							where last_updated >= cast(now() - INTERVAL '365 DAYS' as timestamp)
						 and id = '` + base + `'
						) as min_volume
					from 
						` + currenciesTickersOneYear + `
					where 
						last_updated >= cast(now() - INTERVAL '365 DAYS' as timestamp)
					and id = '` + base + `'
					group by volume, last_updated
					order by last_updated desc
					limit 1
				) as oneYear
		),

	YTD AS 
		(
			select 
				((max_volume - min_volume) / min_volume) as percentage_ytd, 
				volume
			from 
				(
					select 
					volume,
						(select 
							max(volume)  
							from ` + currenciesTickersYTD + ` 
							where last_updated >= cast(date_trunc('year', current_date) as timestamp)
						 and id = '` + base + `'
						) as max_volume, 
						(select 
							min(volume)  
							from ` + currenciesTickersYTD + ` 
							where last_updated >= cast(date_trunc('year', current_date) as timestamp)
						 and id = '` + base + `'
						) as min_volume
					from 
						` + currenciesTickersYTD + `
					where 
						last_updated >= cast(date_trunc('year', current_date) as timestamp)
					and id = '` + base + `'
					group by volume, last_updated
					order by last_updated desc
					limit 1
				) as ytd	
	   )
	select 
			CAST((oneDay.percentage_1d) AS FLOAT) percentage_1d,
			CAST((oneDay.volume) AS FLOAT) volume_1d,
			CAST((sevenDays.percentage_7d) AS FLOAT) percentage_7d,
			CAST((sevenDays.volume) AS FLOAT) volume_7d,
			CAST((thirtyDays.percentage_30d) AS FLOAT) percentage_30d,
			CAST((thirtyDays.volume) AS FLOAT) volume_30d,
			CAST((oneYear.percentage_1y) AS FLOAT) percentage_1y,
			CAST((oneYear.volume) AS FLOAT) volume_1y,
			CAST((YTD.percentage_ytd) AS FLOAT) percentage_ytd,
			CAST((YTD.volume) AS FLOAT) volume_ytd
		from 
			oneDay,
			sevenDays,
			thirtyDays,
			oneYear,
			YTD
	`

	return query
}

func PGQueryNomicsVolume(base string) (*NomicsVolume, error) {

	startTime := StartTime("Nomics Volume Query")

	pg := PGConnect()

	queryResult, err := pg.Query(BuildNomicsVolumeQuery(base))

	if err != nil {
		ConsumeTime("Nomics Volume Query", startTime, err)
		return nil, err
	}
	defer queryResult.Close()

	var nomics NomicsVolume
	for queryResult.Next() {
		err := queryResult.Scan(&nomics.PercentageVolume1D, &nomics.Volume1D, &nomics.PercentageVolume7D, &nomics.Volume7D, &nomics.PercentageVolume30D, &nomics.Volume30D, &nomics.PercentageVolume1Y, &nomics.Volume1Y, &nomics.PercentageVolumeYTD, &nomics.VolumeYTD)
		if err != nil {
			ConsumeTime("Nomics Volume Query Scan", startTime, err)
			return nil, err
		}
	}

	ConsumeTime("Nomics Volume Query", startTime, nil)

	return &nomics, nil
}


func BuildExchangePriceQuery(exchange string, base string) string {

	exchangeMarket := "nomics_exchange_market_ticker"

	query := `
	with 
		oneDay AS 
			(
				SELECT 
					CAST(Close AS FLOAT) price_by_exchange_1d, 
					lower(base) as symbol
				FROM 
					(
						SELECT 
							AVG(price) as Close, 
							base 
						FROM 
							` + exchangeMarket + `
						WHERE 
							timestamp >= cast(now() - INTERVAL '24 HOUR' as timestamp)
							AND base =  '` + base + `'
							AND exchange = '` + exchange + `'
							AND quote IN ('USD', 'USDT', 'USDC')
						GROUP BY 
							base
					) as oneDay
			),
		sevenDays AS 
			(
				SELECT 
					CAST(Close AS FLOAT) price_by_exchange_7d, 
					lower(base) as symbol
				FROM 
					( 
						SELECT 
							AVG(price) as Close, 
							base 
						FROM 
							` + exchangeMarket + `
						WHERE 
							timestamp >=  cast(now() - INTERVAL '7 DAYS' as timestamp)
							AND base =  '` + base + `'
							AND exchange = '` + exchange + `'
							AND quote IN ('USD', 'USDT', 'USDC')
						GROUP BY 
							base
					) as sevenDays
			),
		thirtyDays AS 
			(
				SELECT 
					CAST(Close AS FLOAT) price_by_exchange_30d, 
					lower(base) as symbol
				FROM 
					( 
						SELECT 
							AVG(price) as Close, 
							base 
						FROM 
							` + exchangeMarket + `
						WHERE 
							timestamp >=  cast(now() - INTERVAL '30 DAYS' as timestamp)
							AND base =  '` + base + `'
							AND exchange = '` + exchange + `'
							AND quote IN ('USD', 'USDT', 'USDC')
						GROUP BY 
							base
					) as thirtyDays
			),
		oneYear AS 
			(
				SELECT 
					CAST(Close AS FLOAT) price_by_exchange_1y, 
					lower(base) as symbol
				FROM 
					( 
						SELECT 
							AVG(price) as Close, 
							base 
						FROM 
							` + exchangeMarket + `
						WHERE 
							timestamp >=  cast(now() - INTERVAL '365 DAYS' as timestamp)
							AND base =  '` + base + `'
							AND exchange = '` + exchange + `'
							AND quote IN ('USD', 'USDT', 'USDC')
						GROUP BY 
							base
					) as oneYear
			),
		YTD AS 
			(
				SELECT 
					CAST(Close AS FLOAT) price_by_exchange_ytd, 
					lower(base) as symbol
				FROM 
					( 
						SELECT 
							AVG(price) as Close, 
							base 
						FROM 
							` + exchangeMarket + `
						WHERE 
							timestamp  >= cast(date_trunc('year', current_date) as timestamp) 
							AND base =  '` + base + `'
							AND exchange = '` + exchange + `'
							AND quote IN ('USD', 'USDT', 'USDC')
						GROUP BY 
							base
					) as YTD
			)
		select 
			CAST((oneDay.price_by_exchange_1d) AS FLOAT) AS price_by_exchange_1d,
			CAST((sevenDays.price_by_exchange_7d) AS FLOAT) AS price_by_exchange_7d,
			CAST((thirtyDays.price_by_exchange_30d) AS FLOAT) AS price_by_exchange_30d,
			CAST((oneYear.price_by_exchange_1y) AS FLOAT) AS price_by_exchange_1y,
			CAST((YTD.price_by_exchange_ytd) AS FLOAT) AS price_by_exchange_ytd
		from oneDay 
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
				 YTD 
			ON
				 YTD.symbol = oneDay.symbol
		`

	return query
}

func PGQueryExchangePriceData(exchange string, base string) (*FirestoreExchange, error) {

	startTime := StartTime("Exchange Price Query")

	pg := PGConnect()

	queryResult, err := pg.Query(BuildExchangePriceQuery(exchange, base))

	if err != nil {
		ConsumeTime("Exchange Price Query", startTime, err)
		return nil, err
	}
	defer queryResult.Close()

	var exchangePrice FirestoreExchange
	for queryResult.Next() {
		err := queryResult.Scan(&exchangePrice.PriceByExchange1D, &exchangePrice.PriceByExchange7D, &exchangePrice.PriceByExchange30D, &exchangePrice.PriceByExchange1Y, &exchangePrice.PriceByExchangeYTD)

		if err != nil {
			ConsumeTime("Exchange Price Query Scan", startTime, err)
			return nil, err
		}
	}

	ConsumeTime("Exchange Price Query", startTime, nil)
	return &exchangePrice, nil

}

func BuildMarketPairsQuery(base string, exchange string) string {
	nomicsExchangeOneDay := "nomics_exchange_market_ticker_one_day"
	nomicsExchangeSevenDays := "nomics_exchange_market_ticker_seven_days"
	nomicsExchangeThirtyDays := "nomics_exchange_market_ticker_thirty_days"
	nomicsExchangeOneYear := "nomics_exchange_market_ticker_one_year"
	nomicsExchangeYTD := "nomics_exchange_market_ticker_ytd"

	query := `
	with 
		oneDay As 
			(
				SELECT 
					Symbol, 
					CAST(Min(volume_for_pair_1d) As FLOAT) volume_for_pair_1d
				from 
					(
						SELECT 
							CASE WHEN AVG(volume) is null THEN CAST(0 AS FLOAT)
							ELSE AVG(volume)
							END
							as volume_for_pair_1d , 
							lower(base) as Symbol, 
							exchange
						from 
							` + nomicsExchangeOneDay + `
						where 
							base = '` + base + `'
							and exchange = '` + exchange + `'
							and last_updated >= cast(now() - INTERVAL '24 HOUR' as timestamp)
						group by 
							base, 
							exchange
					) as oneDay
				group by 
					Symbol
			),
		sevenDays As 
			(
				SELECT 
					Symbol, 
					CAST(Min(volume_for_pair_7d) As FLOAT) volume_for_pair_7d
				from 
					(
						SELECT 
							CASE WHEN AVG(volume) is null THEN CAST(0 AS FLOAT)
							ELSE AVG(volume)
							END
							as volume_for_pair_7d , 
							lower(base) as Symbol,
							exchange
						from 
							` + nomicsExchangeSevenDays + `
						where 
							base = '` + base + `'
							and exchange = '` + exchange + `'
							and last_updated >= cast(now() - INTERVAL '7 DAYS' as timestamp)
						group by 
							base, 
							exchange
					) as sevenDay
				group by 
					Symbol
			),
		thirtyDays As 
			(
				SELECT 
					Symbol,
					CAST(Min(volume_for_pair_30d) As FLOAT) volume_for_pair_30d
				from 
					(
						SELECT 
							CASE WHEN AVG(volume) is null THEN CAST(0 AS FLOAT)
							ELSE AVG(volume)
							END 
							as volume_for_pair_30d , 
							lower(base) as Symbol, 
							exchange
						from 
							` + nomicsExchangeThirtyDays + `
						where 
							base = '` + base + `'
							and exchange = '` + exchange + `'
							and last_updated >= cast(now() - INTERVAL '30 DAYS' as timestamp)
						group by 
							base, 
							exchange
					) as thirtyDay
				group by 
					Symbol
			),
		oneYear As 
			(
				SELECT 
					Symbol,
					CAST(Min(volume_for_pair_1y) As FLOAT) volume_for_pair_1y
				from 
					(
						SELECT 
							CASE WHEN AVG(volume) is null THEN CAST(0 AS FLOAT)
							ELSE AVG(volume)
							END
							as volume_for_pair_1y , 
							lower(base) as Symbol, 
							exchange
						from 
							` + nomicsExchangeOneYear + `
						where 
							base = '` + base + `'
							and exchange = '` + exchange + `'
							and last_updated >= cast(now() - INTERVAL '365 DAYS' as timestamp)
						group by 
							base, 
							exchange
					) as oneYear
				group by 
					Symbol
			),
		YTD As 
			(
				SELECT 
					Symbol, 
					CAST(Min(volume_for_pair_ytd) As FLOAT) volume_for_pair_ytd
				from 
					(
						SELECT 
							CASE WHEN AVG(volume) is null THEN CAST(0 AS FLOAT)
							ELSE AVG(volume)
							END
							as volume_for_pair_ytd , 
							lower(base) as Symbol, 
							exchange
						from 
							` + nomicsExchangeYTD + `
						where 
							base = '` + base + `'
							and exchange = '` + exchange + `'
							and last_updated  >= cast(date_trunc('year', current_date) as timestamp)
						group by 
							base, 
							exchange
					) as ytd
				group by 
					Symbol
			)
		SELECT
			CAST(MIN(oneDay.volume_for_pair_1d) AS FLOAT) AS volume_for_pair_1d,
			CAST(MIN(sevenDays.volume_for_pair_7d) AS FLOAT) AS volume_for_pair_7d,
			CAST(MIN(thirtyDays.volume_for_pair_30d) AS FLOAT) AS volume_for_pair_30d,
			CAST(MIN(oneYear.volume_for_pair_1y) AS FLOAT) AS volume_for_pair_1y,
			CAST(MIN(YTD.volume_for_pair_ytd) AS FLOAT) AS volume_for_pair_ytd

		from oneDay 
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
				YTD 
			ON
			 	YTD.symbol = oneDay.symbol
		group by 
			oneDay.symbol
	`
	return query

}

func PGQueryMarketPairData(base string, exchange string) (*MarketPairs, error) {

	startTime := StartTime("Market Pairs Query Process")

	pg := PGConnect()

	queryResult, err := pg.Query(BuildMarketPairsQuery(base, exchange))

	if err != nil {
		ConsumeTime("Market Pairs Query", startTime, err)
		return nil, err
	}
	defer queryResult.Close()

	var marketPair MarketPairs
	for queryResult.Next() {
		err := queryResult.Scan(&marketPair.Nomics.VolumeForPair1D, &marketPair.Nomics.VolumeForPair7D, &marketPair.Nomics.VolumeForPair30D, &marketPair.Nomics.VolumeForPair1Y, &marketPair.Nomics.VolumeForPairYTD)

		if err != nil {
			ConsumeTime("Market Pairs Query Scan", startTime, err)
			return nil, err
		}
	}

	ConsumeTime("Market Pairs Query", startTime, nil)

	return &marketPair, nil
}

func BuildExchangeDataQuery(exchangeID string) string {

	exchangeMetadata := "nomics_exchange_metadata"
	exchangeHighlight := "nomics_exchange_highlight"
	exchangeOneDay := "nomics_exchange_market_ticker_one_day"
	exchangeSevenDays := "nomics_exchange_market_ticker_seven_days"
	exchangeThirtyDays := "nomics_exchange_market_ticker_thirty_days"
	exchangeOneYear := "nomics_exchange_market_ticker_one_year"
	exchangeYtd := "nomics_exchange_market_ticker_ytd"
	query := `
	with 
		exchangeMetadata as (
			select 
				id, 
				name,
				logo_url
			from 
				` + exchangeMetadata + `
			where 
				id = '` + exchangeID + `'
		),
		exchangeHighLight as (
			select 
				num_markets,
				exchange
			from 
				` + exchangeHighlight + `
			where 
				exchange = '` + exchangeID + `'
			order by 
				num_markets desc
			limit 1
		),
	oneDay as (
		SELECT 
			exchange,
			min(volume) as volume
		FROM 
			(
				select 
					exchange, 
					CASE WHEN avg(volume) is null THEN CAST(0 AS FLOAT)
					ELSE avg(volume)
					END
					as volume
				from 
					` + exchangeOneDay + `
				where 
					last_updated >= cast(now() - INTERVAL '24 HOUR' as timestamp)
					and exchange = '` + exchangeID + `'
				group by 
					exchange
			) as oneDay
		group by 
			exchange
	),
	sevenDays as (
		SELECT 
			exchange, 
			min(volume) as volume
		FROM 
			(
				select 
					exchange, 
					CASE WHEN avg(volume) is null THEN CAST(0 AS FLOAT)
					ELSE avg(volume)
					END
					as volume
				from 
					` + exchangeSevenDays + `
				where 
					last_updated >= cast(now() - INTERVAL '7 DAYS' as timestamp)
					and exchange = '` + exchangeID + `'
				group by 
					exchange
			) as sevenDays
		group by 
			exchange
	),
	thirtyDays as (
		SELECT 
			exchange, 
			min(volume) as volume
		FROM 
			(
				select 
					exchange, 
					CASE WHEN avg(volume) is null THEN CAST(0 AS FLOAT)
					ELSE avg(volume)
					END
					as volume
				from 
					` + exchangeThirtyDays + `
				where 
					last_updated >= cast(now() - INTERVAL '30 DAYS' as timestamp)
					and exchange = '` + exchangeID + `'
				group by 
					exchange
			) as thirtyDays
		group by 
			exchange
	),
	oneYear as (
		SELECT 
			exchange, 
			min(volume) as volume
		FROM 
			(
				select 
					exchange, 
					CASE WHEN avg(volume) is null THEN CAST(0 AS FLOAT)
					ELSE avg(volume)
					END
					as volume
				from 
					` + exchangeOneYear + `
				where 
					last_updated >= cast(now() - INTERVAL '365 DAYS' as timestamp)
					and exchange = '` + exchangeID + `'
				group by exchange
			) as oneYear
		group by 
			exchange
	),
	YTD as (
		SELECT 
			exchange, 
			min(volume) as volume
		FROM 
			(
				select 
					exchange, 
					CASE WHEN avg(volume) is null THEN CAST(0 AS FLOAT)
					ELSE avg(volume)
					END
					as volume
				from 
					` + exchangeYtd + `
				where 
					last_updated >= cast(date_trunc('year', current_date) as timestamp)
					and exchange = '` + exchangeID + `'
				group by exchange
			) as YTD
		group by 
			exchange
	)
	select 
		exchangeMetadata.id, 
		exchangeMetadata.name, 
		exchangeMetadata.logo_url, 
		cast(exchangeHighLight.num_markets as int),
		cast(oneDay.volume as float) as volume_exchange_1d,
		cast(sevenDays.volume as float) as volume_exchange_7d,
		cast(thirtyDays.volume as float) as volume_exchange_30d,
		cast(oneYear.volume as float) as volume_exchange_1y,
		cast(YTD.volume as float) as volume_exchange_ytd
	from 
		exchangeMetadata
		INNER Join 
			exchangeHighLight
		ON 
			exchangeHighLight.exchange = exchangeMetadata.id
		INNER Join 
			oneDay
		ON 
			oneDay.exchange = exchangeMetadata.id
		INNER Join 
			sevenDays
		ON 
			sevenDays.exchange = exchangeMetadata.id
		INNER Join 
			thirtyDays
		ON 
			thirtyDays.exchange = exchangeMetadata.id
		INNER Join 
			oneYear
		ON 
			oneYear.exchange = exchangeMetadata.id
		INNER Join 
			YTD
		ON 
			YTD.exchange = exchangeMetadata.id
		
	group by
		exchangeMetadata.id, 
		exchangeMetadata.name, 
		exchangeMetadata.logo_url, 
		exchangeHighLight.num_markets,
		oneDay.volume ,
		sevenDays.volume,
		thirtyDays.volume,
		oneYear.volume,
		YTD.volume
	`

	return query
}

func PGQueryExchangeData(exchangeID string) (*ExchangeFundamentals, error) {

	startTime := StartTime("Exchange Data Query")

	pg := PGConnect()

	queryResult, err := pg.Query(BuildExchangeDataQuery(exchangeID))

	if err != nil {
		ConsumeTime("Exchange Data Query", startTime, err)
		return nil, err
	}
	defer queryResult.Close()

	var exchange ExchangeFundamentals

	for queryResult.Next() {
		err := queryResult.Scan(&exchange.Id, &exchange.Name, &exchange.Logo, &exchange.ExchangeActiveMarketPairs, &exchange.Nomics.VolumeByExchange1D, &exchange.Nomics.VolumeByExchange7D, &exchange.Nomics.VolumeByExchange30D, &exchange.Nomics.VolumeByExchange1Y, &exchange.Nomics.VolumeByExchangeYTD)

		if err != nil {
			ConsumeTime("Exchange Data Query Scan", startTime, err)
			return nil, err
		}
	}

	ConsumeTime("Exchange Data Query", startTime, nil)
	return &exchange, nil
}

func GetFundamentalsDataPG() ([]FundamentalsData, error) {

	startTime := StartTime("Fundamentals Data Query")

	pg := PGConnect()

	var fundamentals []FundamentalsData

	queryResult, err := pg.Query(`
			WITH fundamentals AS (
				SELECT  
						symbol,
						row_number() OVER(PARTITION BY symbol ORDER BY last_updated desc) AS row_num,
						name, 
						slug,
						logo,
						display_symbol,
						price_24h,
						percentage_24h,
						change_value_24h
				FROM    fundamentals
			)
			SELECT  
				symbol,
				name, 
				slug,
				logo,
				display_symbol,
				price_24h,
				percentage_24h,
				change_value_24h
			FROM    fundamentals
			WHERE   row_num = 1
				`)
	if err != nil {
		ConsumeTime("Fundamentals Data Query", startTime, err)
		return nil, err
	}
	defer queryResult.Close()

	for queryResult.Next() {
		var fundamental FundamentalsData
		err := queryResult.Scan(&fundamental.Symbol, &fundamental.Name, &fundamental.Slug, &fundamental.Logo, &fundamental.DisplaySymbol, &fundamental.Price24h, &fundamental.Percentage24h, &fundamental.ChangeValue24h)

		if err != nil {
			ConsumeTime("Fundamentals Data Query Scan", startTime, err)
			return nil, err
		}
		fundamentals = append(fundamentals, fundamental)

	}
	ConsumeTime("Fundamentals Data Query", startTime, nil)
	return fundamentals, nil
}

func InsertChartData(chartData []ChartDataPG) error {

	startTime := StartTime("Fundamental Charts Data Insert")

	pg := PGConnect()

	tableName := "chart_data_fundamentals"

	valueString := make([]string, 0, len(chartData))
	valueCharts := make([]interface{}, 0, len(chartData)*5)
	var i = 0

	for y := 0; y < len(chartData); y++ {
		var chart = chartData[y]
		var valString = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d)", i*5+1, i*5+2, i*5+3, i*5+4, i*5+5)
		valueString = append(valueString, valString)
		valueCharts = append(valueCharts, chart.Symbol)
		valueCharts = append(valueCharts, chart.Forbes)
		valueCharts = append(valueCharts, chart.Time)
		valueCharts = append(valueCharts, chart.Price)
		valueCharts = append(valueCharts, chart.DataSource)

		i++

	}

	insertQuery := fmt.Sprintf("INSERT INTO %s VALUES %s", tableName, strings.Join(valueString, ","))

	_, inserterError := pg.Exec(insertQuery, valueCharts...)

	if inserterError != nil {
		ConsumeTime("Fundamentals Charts Data Insert", startTime, inserterError)
		return inserterError
	}

	ConsumeTime("Fundamentals Charts Data Insert", startTime, nil)
	return nil
}

func QueryCharts(interval string, targetResolutionSeconds int) ([]TimeSeriesResultPG, error) {

	startTime := time.Now().UTC()

	log.Debug("Charts Query %s Process Started at :: %s", interval, startTime)

	pg := PGConnect()

	query := `
	SELECT
		symbol,
		array_to_json(ARRAY_AGG(json_build_object('Time',time, 'Price', price))) prices
	FROM
		(
			SELECT
				LOWER(symbol) symbol,
				time,
				CAST(AVG(price) AS FLOAT) price,
				ROW_NUMBER() OVER (PARTITION BY forbes, CAST(FLOOR(extract(epoch from Time)/ ($1))AS INT )  ORDER BY Time) as row_num
			FROM
				chart_data_fundamentals
				
			WHERE
				Time >= cast(now () - INTERVAL '` + interval + `' as Timestamp)
			GROUP BY
				Time,
				symbol,
				forbes
			
		) as test
	where 
		row_num = 1
	GROUP BY
		symbol
	`

	var timeSeriesResults []TimeSeriesResultPG

	queryResult, err := pg.Query(query, targetResolutionSeconds)

	if err != nil {
		ConsumeTime("Charts Query", startTime, err)
		return nil, err
	}
	defer queryResult.Close()

	for queryResult.Next() {
		var timeSeriesResult TimeSeriesResultPG
		timeSeriesResult.TargetResolutionSeconds = targetResolutionSeconds
		timeSeriesResult.IsIndex = false
		err := queryResult.Scan(&timeSeriesResult.Symbol, (*slicePGResult)(&timeSeriesResult.Slice))

		if err != nil {
			ConsumeTime("Charts Query Scan", startTime, err)
			return nil, err
		}

		SortChartDataPG(timeSeriesResult.Slice)
		timeSeriesResults = append(timeSeriesResults, timeSeriesResult)
	}

	ConsumeTime("Charts Query", startTime, nil)

	return timeSeriesResults, nil
}

func SortChartDataPG(chartData []SlicePG) {
	sort.Slice(chartData, func(i, j int) bool {
		return chartData[i].Time.Before(chartData[j].Time)
	})
}

func InsertNomicsChartData(interval string, chartData TimeSeriesResultPG) error {

	startTime := StartTime("Nomics Charts Data Insert")

	pg := PGConnect()

	insertChartDataStatement := "INSERT INTO nomics_chart_data(is_index, source, target_resolution_seconds, prices, symbol, interval) VALUES($1, $2, $3, $4, $5, $6)"

	fullChartDataStatement := insertChartDataStatement + "ON CONFLICT (interval) DO UPDATE SET is_index = $1, source = $2, target_resolution_seconds = $3, prices = $4, symbol = $5"

	slice, _ := json.Marshal(chartData.Slice)
	_, inserterError := pg.Exec(fullChartDataStatement, chartData.IsIndex, chartData.Source, chartData.TargetResolutionSeconds, slice, chartData.Symbol, interval)
	if inserterError != nil {
		ConsumeTime("Nomics Charts Data Insert", startTime, inserterError)
		return inserterError
	}

	ConsumeTime("Nomics Charts Data Insert", startTime, nil)

	return nil
}

func GetChartData(interval string) (*TimeSeriesResultPG, error) {

	startTime := StartTime("Charts Data Query")

	pg := PGConnect()

	var timeSeriesResult TimeSeriesResultPG

	query := `
		SELECT 
			is_index, 
			source, 
			target_resolution_seconds, 
			prices, 
			symbol
		FROM 
			nomics_chart_data
		WHERE 
			interval = '` + interval + `'
	`

	queryResult, err := pg.Query(query)

	if err != nil {
		ConsumeTime("Charts Data Query", startTime, err)
		return nil, err
	}
	defer queryResult.Close()

	for queryResult.Next() {
		err := queryResult.Scan(&timeSeriesResult.IsIndex, &timeSeriesResult.Source, &timeSeriesResult.TargetResolutionSeconds, (*slicePGResult)(&timeSeriesResult.Slice), &timeSeriesResult.Symbol)

		if err != nil {
			ConsumeTime("Charts Data Query Scan", startTime, err)
			return nil, err
		}
	}

	ConsumeTime("Charts Data Query", startTime, nil)

	return &timeSeriesResult, nil
}

func BuildMarketPairsVolumeQuery(base string, exchange string) string {

	currenciesTickersOneDay := "nomics_currencies_tickers_one_day"
	currenciesTickersSevenDays := "nomics_currencies_tickers_seven_days"
	currenciesTickersThirtyDays := "nomics_currencies_tickers_thirty_days"
	currenciesTickersOneYear := "nomics_currencies_tickers_one_year"
	currenciesTickersYTD := "nomics_currencies_tickers_ytd"

	exchangeOneDay := "nomics_exchange_market_ticker_one_day"
	exchangeSevenDays := "nomics_exchange_market_ticker_seven_days"
	exchangeThirtyDays := "nomics_exchange_market_ticker_thirty_days"
	exchangeOneYear := "nomics_exchange_market_ticker_one_year"
	exchangeYtd := "nomics_exchange_market_ticker_ytd"

	query :=
		`
	with 
	oneDay as (
		SELECT sum(ticker.volume) as volume, ticker.id as base
		from 
			` + currenciesTickersOneDay + ` ticker,
			` + exchangeOneDay + ` market
		where 
		ticker.id = market.base
		and ticker.last_updated >=  cast(now() - INTERVAL '24 HOUR' as timestamp)
		and market.last_updated >=  cast(now() - INTERVAL '24 HOUR' as timestamp)
		and ticker.id = '` + base + `'
		and market.exchange = '` + exchange + `'
		group by ticker.id
	),
	sevenDays as (
		SELECT sum(ticker.volume) as volume, ticker.id as base
		from 
			` + currenciesTickersSevenDays + ` ticker,
			` + exchangeSevenDays + ` market
		where 
		ticker.id = market.base
		and ticker.last_updated >=  cast(now() - INTERVAL '7 DAYS' as timestamp)
		and market.last_updated >=  cast(now() - INTERVAL '7 DAYS' as timestamp)
		and ticker.id = '` + base + `'
		and market.exchange = '` + exchange + `'
		group by ticker.id
	),
	thirtyDays as (		
		SELECT sum(ticker.volume) as volume, ticker.id as base
		from 
			` + currenciesTickersThirtyDays + ` ticker,
			` + exchangeThirtyDays + ` market
		where 
		ticker.id = market.base
		and ticker.last_updated >=  cast(now() - INTERVAL '30 DAYS' as timestamp)
		and market.last_updated >=  cast(now() - INTERVAL '30 DAYS' as timestamp)
		and ticker.id = '` + base + `'
		and market.exchange = '` + exchange + `'
		group by ticker.id
	),
	oneYear as (		
		SELECT sum(ticker.volume) as volume, ticker.id as base
		from 
			` + currenciesTickersOneYear + ` ticker,
			` + exchangeOneYear + ` market
		where 
		ticker.id = market.base
		and ticker.last_updated >=  cast(now() - INTERVAL '365 DAYS' as timestamp)
		and market.last_updated >=  cast(now() - INTERVAL '365 DAYS' as timestamp)
		and ticker.id = '` + base + `'
		and market.exchange = '` + exchange + `'
		group by ticker.id
	),
	ytd as (		
		SELECT sum(ticker.volume) as volume, ticker.id as base
		from 
			` + currenciesTickersYTD + ` ticker,
			` + exchangeYtd + ` market
		where 
		ticker.id = market.base
		and ticker.last_updated >=  cast(date_trunc('year', current_date) as timestamp)
		and market.last_updated >=  cast(date_trunc('year', current_date) as timestamp)
		and ticker.id = '` + base + `'
		and market.exchange = '` + exchange + `'
		group by ticker.id
	)
	select 
		cast(MIN(oneDay.volume) as FLOAT) as volume_for_Pair_1d,
		cast(MIN(sevenDays.volume) as FLOAT) as volume_for_Pair_7d,
		cast(MIN(thirtyDays.volume) as FLOAT) as volume_for_Pair_30d,
		cast(MIN(oneYear.volume) as FLOAT) as volume_for_Pair_1y,
		cast(MIN(ytd.volume) as FLOAT) as volume_for_Pair_ytd
	from 
		oneDay
		INNER JOIN 
			sevenDays
		ON 
			sevenDays.base = oneDay.base
		INNER JOIN 
			thirtyDays
		ON 
			thirtyDays.base = oneDay.base
		INNER JOIN 
			oneYear
		ON 
			oneYear.base = oneDay.base
		INNER JOIN 
			ytd
		ON 
			ytd.base = oneDay.base
		
	group by oneDay.base
	`

	return query
}

func PGQueryMarketPairsVolume(base string, exchange string) (*MarketPairsVolume, error) {

	startTime := StartTime("Market Pairs Volume Query")

	pg := PGConnect()

	var marketVolume MarketPairsVolume

	queryResult, err := pg.Query(BuildMarketPairsVolumeQuery(base, exchange))

	if err != nil {
		ConsumeTime("Market Pairs Volume Query", startTime, err)
		return nil, err
	}
	defer queryResult.Close()

	for queryResult.Next() {
		err := queryResult.Scan(&marketVolume.VolumeForPair1D, &marketVolume.VolumeForPair7D, &marketVolume.VolumeForPair30D, &marketVolume.VolumeForPair1Y, &marketVolume.VolumeForPairYTD)

		if err != nil {
			ConsumeTime("Market Pairs Volume Query", startTime, err)
			return nil, err
		}
	}

	ConsumeTime("Market Pairs Volume Query", startTime, nil)
	return &marketVolume, nil
}

func BuildFundamentalsForbesPercentageQuery(base string) string {
	candlesTable := "nomics_ohlcv_candles"
	query :=
		`
		with 
			oneDay AS 
					(
						SELECT 
							lower(base) as symbol, 
							CAST((( MAX(volume) - MIN(volume) /  MIN(volume))) AS FLOAT) as percentage_1d
						FROM
							( 
								SELECT 
									base, 
									volume 
								FROM 
									` + candlesTable + `
								WHERE 
									timestamp >= cast(now() - INTERVAL '24 HOUR' as timestamp)
								and base = '` + base + `'
								GROUP BY 
									base, 
									volume
							) as oneDay
						GROUP BY 
						base
					),
				sevenDays AS 
					(
						SELECT 
							lower(base) as symbol, 
							CAST((( MAX(volume) - MIN(volume) /  MIN(volume))) AS FLOAT) as percentage_7d
						FROM 
							( 
								SELECT 
									base, 
									volume 
								FROM 
									` + candlesTable + `
								WHERE 
									timestamp >=  cast(now() - INTERVAL '7 DAYS' as timestamp)
								and base = '` + base + `'
								GROUP BY 
									base, 
									volume
							) as sevenDays
						GROUP BY 
							base
					),
				thirtyDays AS 
					(
						SELECT 
							lower(base) as symbol, 
							CAST((( MAX(volume) - MIN(volume) /  MIN(volume))) AS FLOAT) as percentage_30d
						FROM 
							(
								SELECT 
									base, 
									volume
								FROM 
									` + candlesTable + `
								WHERE 
									timestamp >=  cast(now() - INTERVAL '30 DAYS' as timestamp)
								and base = '` + base + `'
								GROUP BY 
									base, 
									volume
							) as thirtyDays
						GROUP BY 
						base
					),
				oneYear AS 
					(
						SELECT 
							lower(base) as symbol, 
							CAST((( MAX(volume) - MIN(volume) /  MIN(volume))) AS FLOAT) as percentage_1y
						FROM 
							( 
								SELECT 
									base, 
									volume
								FROM 
									` + candlesTable + `
								WHERE 
									timestamp >=  cast(now() - INTERVAL '365 DAYS' as timestamp)
								and base = '` + base + `'
								GROUP BY 
									base, 
									volume
							) as oneYear
						GROUP BY 
							base
					),

				YTD AS 
					(
						SELECT  
							lower(base) as symbol, 
							CAST((( MAX(volume) - MIN(volume) /  MIN(volume))) AS FLOAT) as percentage_ytd
						FROM 
							( 
								SELECT 
									base, 
									volume
								FROM 
									` + candlesTable + `
								WHERE 
									timestamp  >= cast(date_trunc('year', current_date) as timestamp)
								and base = '` + base + `'
								GROUP BY 
									base, 
									volume
							) as oneYear
						GROUP BY 
							base
					)

			select 
				oneDay.percentage_1d, 
				sevenDays.percentage_7d, 
				thirtyDays.percentage_30d, 
				oneYear.percentage_1y, 
				YTD.percentage_ytd
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
					YTD 
				ON 
					YTD.symbol = oneDay.symbol
	
	`
	return query
}

func PGQueryForbesPercentage(base string) (*FundamentalsForbesPercentage, error) {

	startTime := StartTime("Forbes Percentage Query")

	pg := PGConnect()

	queryResult, err := pg.Query(BuildFundamentalsForbesPercentageQuery(base))

	if err != nil {
		ConsumeTime("Forbes Percentage Query", startTime, err)
		return nil, err
	}

	defer queryResult.Close()
	var fundamentalsForbesPercentage FundamentalsForbesPercentage

	for queryResult.Next() {
		err := queryResult.Scan(&fundamentalsForbesPercentage.ForbesPercentage1D, &fundamentalsForbesPercentage.ForbesPercentage7D, &fundamentalsForbesPercentage.ForbesPercentage30D, &fundamentalsForbesPercentage.ForbesPercentage1Y, &fundamentalsForbesPercentage.ForbesPercentageYTD)

		if err != nil {
			ConsumeTime("Forbes Percentage Query", startTime, err)
			return nil, err
		}
	}

	ConsumeTime("Forbes Percentage Query", startTime, nil)
	return &fundamentalsForbesPercentage, err
}

func BuildAllMarketPairsQuery() string {

	candlesTable := "nomics_ohlcv_candles"
	exchangeTable := "nomics_exchange_market_ticker"
	marketTable := "nomics_markets"
	assetsTable := "nomics_assets"

	query := `
	with 
		fundExchanges as 
		(
			with
			oneDay as 
				(
					SELECT 
						lower(base) as symbol
					FROM 
						( 
							SELECT base 
							FROM 
								` + candlesTable + `
							where timestamp >= cast(now() - INTERVAL '24 HOUR' as timestamp)
							GROUP BY 
								base, 
								timestamp
						) as oneDay
					GROUP BY 
					base
				),
			ExchangesPrices AS 
				( 
					SELECT 
						lower(base) as Symbol, 
						exchange as Market
					FROM 
						` + exchangeTable + `
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
				oneDay.symbol
			from 
				oneDay 
				INNER JOIN 
					ExchangesPrices 
				ON 
					ExchangesPrices.Symbol = oneDay.symbol
			group by 
				oneDay.symbol
		),
		fundMarketPairs as 
		(
			with
			market as 
				(
					select
						lower(base) as Symbol, 
						exchange, 
						quote , 
						CONCAT(base, quote) as pair
					from 
						` + marketTable + `
					group by
						base,
						exchange, 
						quote
					
				),
			assets as 
				(
					select
						lower(id) as base,
						status, 
						last_updated
					from 
						` + assetsTable + `
					group by 
						id
			
				),
			ticker as 
				(
					select
						lower(base) as base,
						type
					from 
						` + exchangeTable + `
					where 
						type != ''
					group by
						base,
						type
				),
			oneDay As 
				(
					SELECT 
						Symbol,   
						CAST(MIN(Price) AS FLOAT) current_price_for_pair_1d
					from
						(
							SELECT 
								lower(base) as Symbol,
								exchange,
								AVG(price) price
							from 
								` + exchangeTable + `
							where 
								timestamp >= cast(now() - INTERVAL '24 HOUR' as timestamp)
							group by 
								base, 
								exchange
						) as oneDay
					group by Symbol
				),
			sevenDays As 
				(
					SELECT 
						Symbol,   
						CAST(MIN(Price) AS FLOAT) current_price_for_pair_7d
					from
						(
							SELECT 
								lower(base) as Symbol,
								exchange,
								AVG(price) price
							from 
								` + exchangeTable + `
							where 
								timestamp >= cast(now() - INTERVAL '7 DAYS' as timestamp)
							group by 
								base, 
								exchange
						) as sevenDays
					group by Symbol
				),
			thirtyDays As 
				(
					SELECT 
						Symbol,   
						CAST(MIN(Price) AS FLOAT) current_price_for_pair_30d
					from
						(
							SELECT 
								lower(base) as Symbol,
								exchange,
								AVG(price) price
							from 
								` + exchangeTable + `
							where 
								timestamp >= cast(now() - INTERVAL '30 DAYS' as timestamp)
							group by 
								base, 
								exchange
						) as thirtyDays
					group by Symbol
				),
			oneYear As 
				(
					SELECT 
						Symbol,   
						CAST(MIN(Price) AS FLOAT) current_price_for_pair_1y
					from
						(
							SELECT 
								lower(base) as Symbol,
								exchange,
								AVG(price) price
							from 
								` + exchangeTable + `
							where 
								timestamp >= cast(now() - INTERVAL '365 DAYS' as timestamp)
							group by 
								base, 
								exchange
						) as oneYear
					group by Symbol
				),
			YTD As 
				(
					SELECT 
						Symbol,   
						CAST(MIN(Price) AS FLOAT) current_price_for_pair_ytd
					from
						(
							SELECT 
								lower(base) as Symbol,
								exchange,
								AVG(price) price
							from 
								` + exchangeTable + `
							where 
								timestamp >= cast(date_trunc('year', current_date) as timestamp)
							group by 
								base, 
								exchange
						) as YTD
					group by Symbol
				)

			select 
				assets.base, 
				array_to_json(ARRAY_AGG(json_build_object(
													'base', market.Symbol, 
													'exchange', market.exchange, 
													'quote', market.quote, 
													'pair', market.pair, 												 
													'pairStatus', assets.status, 
													'update_timestamp', assets.last_updated,
													'TypeOfPair', ticker.type,
													'currentPriceForPair1D', CAST(oneDay.current_price_for_pair_1d AS FLOAT),
													'currentPriceForPair7D', CAST(sevenDays.current_price_for_pair_7d AS FLOAT),
													'currentPriceForPair30D', CAST(thirtyDays.current_price_for_pair_30d AS FLOAT),
													'currentPriceForPair1Y', CAST(oneYear.current_price_for_pair_1y AS FLOAT),
													'currentPriceForPairYTD', CAST(YTD.current_price_for_pair_ytd AS FLOAT)
													))) as MarketPairs
			from 
				assets
				INNER JOIN 
					market
				ON
					market.Symbol = assets.base
				INNER JOIN 
					ticker
				ON
					ticker.base = assets.base
				INNER JOIN 
					oneDay 
				ON
					oneDay.symbol = assets.base
				INNER JOIN 
					sevenDays 
				ON
					sevenDays.symbol = assets.base
				INNER JOIN 
					thirtyDays 
				ON
					thirtyDays.symbol = assets.base
				INNER JOIN 
					oneYear 
				ON
					oneYear.symbol = assets.base
				INNER JOIN 
					YTD 
				ON
					YTD.symbol = assets.base
				group by 
					assets.base
		)
	select 
		fundExchanges.symbol,
		fundExchanges.Exchanges,
		fundMarketPairs.MarketPairs
	from 
		fundExchanges
		INNER JOIN 
			fundMarketPairs
		ON
			fundMarketPairs.base = fundExchanges.symbol

	`

	return query
}

func PGQueryMarketPairsAndExchange() ([]PGFundamentalsResult, error) {

	startTime := StartTime("Fundamental Market Pairs and Exchange Query")

	pg := PGConnect()

	var fundamentalsResults []PGFundamentalsResult

	queryResult, err := pg.Query(BuildAllMarketPairsQuery())

	if err != nil {
		ConsumeTime("Fundamental Market Pairs and Exchange Query", startTime, err)
		return nil, err

	}

	defer queryResult.Close()

	for queryResult.Next() {
		var fundamentalsResult PGFundamentalsResult

		err := queryResult.Scan(&fundamentalsResult.Symbol, (*exchangeResult)(&fundamentalsResult.Exchanges), (*pairsResult)(&fundamentalsResult.MarketPairs))

		if err != nil {
			ConsumeTime("Fundamental Market Pairs  and Exchange Query", startTime, err)
			return nil, err
		}
		fundamentalsResults = append(fundamentalsResults, fundamentalsResult)
	}

	ConsumeTime("Fundamental Market Pairs and Exchange Query", startTime, nil)
	return fundamentalsResults, nil

}

























with
	assets as 
		(
			select
				lower(id) as base,
				status, 
				last_updated
			from 
				nomics_assets
			group by 
				id

		),
	ExchangesPrices AS 
		( 
			SELECT 
				lower(base) as Symbol, 
				exchange as Market
			FROM 
				nomics_exchange_market_ticker
			WHERE 
				exchange NOT IN ('bitmex','hbtc') 
				AND type = 'spot'
				AND timestamp >=  cast(now() - INTERVAL '7 HOUR' as timestamp)
				AND status = 'active'
				AND quote IN ('USD', 'USDT', 'USDC')
			group by 
				base,
				exchange
		),
	market as 
		(
			select
				lower(base) as Symbol, 
				exchange, 
				quote , 
				CONCAT(base, quote) as pair
			from 
				nomics_markets
			group by
				base,
				exchange, 
				quote

		),
	oneYear As 
		(
			SELECT 
				Symbol,   
				CAST(MIN(Price) AS FLOAT) current_price_for_pair_1y
			from
				(
					SELECT 
						lower(base) as Symbol,
						AVG(price) price
					from 
						nomics_exchange_market_ticker
					where 
						timestamp >= cast(now() - INTERVAL '365 DAYS' as timestamp)
					group by 
						base
				) as oneYear
			group by Symbol
		),
	YTD As 
		(
			SELECT 
				Symbol,   
				CAST(MIN(Price) AS FLOAT) current_price_for_pair_ytd
			from
				(
					SELECT 
						lower(base) as Symbol,
						AVG(price) price
					from 
						nomics_exchange_market_ticker
					where 
						timestamp >= cast(date_trunc('year', current_date) as timestamp)
					group by 
						base
				) as YTD
			group by Symbol
		),
	oneDay As 
		(
			SELECT 
				Symbol,   
				CAST(MIN(Price) AS FLOAT) current_price_for_pair_1d
			from
				(
					SELECT 
						lower(base) as Symbol,
						AVG(price) price
					from 
						nomics_exchange_market_ticker
					where 
						timestamp >= cast(now() - INTERVAL '24 HOUR' as timestamp)
					group by 
						base
				) as oneDay
			group by Symbol
		),
	thirtyDays As 
		(
			SELECT 
				Symbol,   
				CAST(MIN(Price) AS FLOAT) current_price_for_pair_30d
			from
				(
					SELECT 
						lower(base) as Symbol,
						AVG(price) price
					from 
						nomics_exchange_market_ticker
					where 
						timestamp >= cast(now() - INTERVAL '30 DAYS' as timestamp)
					group by 
						base
				) as thirtyDays
			group by Symbol
		),
	sevenDays As 
		(
			SELECT 
				Symbol,   
				CAST(MIN(Price) AS FLOAT) current_price_for_pair_7d
			from
				(
					SELECT 
						lower(base) as Symbol,
						AVG(price) price
					from 
						nomics_exchange_market_ticker
					where 
						timestamp >= cast(now() - INTERVAL '7 DAYS' as timestamp)
					group by 
						base
				) as sevenDays
			group by Symbol
		),
	oneDayCandles as 
		(
			SELECT 
				lower(base) as symbol
			FROM 
				( 
					SELECT base 
					FROM 
						nomics_ohlcv_candles
					where timestamp >= cast(now() - INTERVAL '24 HOUR' as timestamp)
					GROUP BY 
						base
				) as oneDay
			GROUP BY 
			base
		),
	ticker as 
		(
			select
				lower(base) as base,
				type
			from 
				nomics_exchange_market_ticker
			where 
				type != ''
			group by
				base,
				type
		)
	
	select 
		array_to_json(ARRAY_AGG(json_build_object('Market', ExchangesPrices.Market, 'Symbol', ExchangesPrices.Symbol))) as Exchanges,
		oneDayCandles.symbol,
		array_to_json(ARRAY_AGG(json_build_object(
											'base', market.Symbol, 
											'exchange', market.exchange, 
											'quote', market.quote, 
											'pair', market.pair, 												 
											'pairStatus', assets.status, 
											'update_timestamp', assets.last_updated,
											'TypeOfPair', ticker.type,
											'currentPriceForPair1D', CAST(oneDay.current_price_for_pair_1d AS FLOAT),
											'currentPriceForPair7D', CAST(sevenDays.current_price_for_pair_7d AS FLOAT),
											'currentPriceForPair30D', CAST(thirtyDays.current_price_for_pair_30d AS FLOAT),
											'currentPriceForPair1Y', CAST(oneYear.current_price_for_pair_1y AS FLOAT),
											'currentPriceForPairYTD', CAST(YTD.current_price_for_pair_ytd AS FLOAT)
											))) as MarketPairs
	from 
		oneDayCandles
		INNER JOIN 
			assets
		ON
			assets.base = oneDayCandles.symbol
		INNER JOIN 
			ExchangesPrices 
		ON 
			ExchangesPrices.Symbol = oneDayCandles.symbol
		INNER JOIN 
			market
		ON
			market.Symbol = oneDayCandles.symbol
		INNER JOIN 
			oneYear 
		ON
			oneYear.symbol = oneDayCandles.symbol
		INNER JOIN 
			YTD 
		ON
			YTD.symbol = oneDayCandles.symbol
		INNER JOIN 
			oneDay 
		ON
			oneDay.symbol = oneDayCandles.symbol
		INNER JOIN 
			thirtyDays 
		ON
			thirtyDays.symbol = oneDayCandles.symbol
		INNER JOIN 
			sevenDays 
		ON
			sevenDays.symbol = oneDayCandles.symbol
		INNER JOIN 
			ticker
		ON
			ticker.base = oneDayCandles.symbol
	group by 
		oneDayCandles.symbol
	











************************************************************************************************
SELECT 
	ARRAY_AGG(distinct exchange) as Exchanges
FROM 
	nomics_exchange_market_ticker
WHERE 
	exchange NOT IN ('bitmex','hbtc')
	AND base = 'BTC'
	AND type = 'spot'
	AND timestamp >=  cast(now() - INTERVAL '30 MINUTE' as timestamp)
	AND status = 'active'
	AND quote IN ('USD', 'USDT', 'USDC')
group by 
	base


************************************************************************************************
with 
		allTime as 
			(
				SELECT 
					CAST(MIN(Close) AS FLOAT) all_time_low, 
					lower(base) as symbol
				FROM ( 
						SELECT 
							AVG(close) as Close, 
							base 
						FROM 
							nomics_ohlcv_candles
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
					lower(base) as symbol
				FROM
					( 
						SELECT 
							AVG(close) as Close, 
							base
						FROM 
							nomics_ohlcv_candles
						WHERE 
							timestamp >= cast(now() - INTERVAL '24 HOUR' as timestamp)
						GROUP BY 
							base
					) as oneDay
				GROUP BY 
				base
			),
		sevenDays AS 
			(
				SELECT 
					CAST(MAX(Close) AS FLOAT) high_7d, 
					CAST(MIN(Close) AS FLOAT) low_7d, 
					lower(base) as symbol
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
							base
					) as sevenDays
				GROUP BY 
					base
			),
		thirtyDays AS 
			(
				SELECT 
					CAST(MAX(Close) AS FLOAT) high_30d, 
					CAST(MIN(Close) AS FLOAT) low_30d, 
					lower(base) as symbol
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
							base
					) as thirtyDays
				GROUP BY 
				base
			),
		oneYear AS 
			(
				SELECT 
					CAST(MAX(Close) AS FLOAT) high_1y, 
					CAST(MIN(Close) AS FLOAT) low_1y, 
					lower(base) as symbol
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
							base
					) as oneYear
				GROUP BY 
					base
			),

		YTD AS 
			(
				SELECT 
					CAST(MAX(Close) AS FLOAT) high_ytd, 
					CAST(MIN(Close) AS FLOAT) low_ytd, 
					lower(base) as symbol
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
							base
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
	************************************************************************************************

with 
		allTime as 
			(
			SELECT lower(base) as symbol 
			FROM 
				nomics_ohlcv_candles
			where base = '0XCX'
			GROUP BY 
				base
			),
		ExchangesPrices AS 
			( 
				SELECT 
					lower(base) as Symbol, 
					exchange as Market,
					avg(Price) as Price
				FROM 
					nomics_exchange_market_ticker
				WHERE 
					exchange NOT IN ('bitmex','hbtc')
					AND base = '0XCX'
					AND type = 'spot'
					AND timestamp >=  cast(now() - INTERVAL '30 MINUTE' as timestamp)
					AND status = 'active'
					AND quote IN ('USD', 'USDT', 'USDC')
				group by 
					base,
					exchange
			),
		exchangeMetadata as (
			select 
				id, 
				name,
				logo_url
			from 
				nomics_exchange_metadata
			where 
				id = 'aax'
		    ),
		exchangeHighLight as (
			select 
				num_markets,
				exchange
			from 
				nomics_exchange_highlight
			where 
				exchange = 'aax'
			order by 
				num_markets desc
			limit 1
		    ),
	    oneDay as (
            SELECT 
                exchange,
                min(volume) as volume
            FROM 
                (
                    select 
                        exchange, 
                        CASE WHEN avg(volume) is null THEN CAST(0 AS FLOAT)
                        ELSE avg(volume)
                        END
                        as volume
                    from 
                        nomics_exchange_market_ticker_one_day
                    where 
                        last_updated >= cast(now() - INTERVAL '24 HOUR' as timestamp)
                        and exchange = 'aax'
                    group by 
                        exchange
                ) as oneDay
            group by 
                exchange
        ),
        sevenDays as (
            SELECT 
                exchange, 
                min(volume) as volume
            FROM 
                (
                    select 
                        exchange, 
                        CASE WHEN avg(volume) is null THEN CAST(0 AS FLOAT)
                        ELSE avg(volume)
                        END
                        as volume
                    from 
                        nomics_exchange_market_ticker_seven_days
                    where 
                        last_updated >= cast(now() - INTERVAL '7 DAYS' as timestamp)
                        and exchange = 'aax'
                    group by 
                        exchange
                ) as sevenDays
            group by 
                exchange
        ),
        thirtyDays as (
            SELECT 
                exchange, 
                min(volume) as volume
            FROM 
                (
                    select 
                        exchange, 
                        CASE WHEN avg(volume) is null THEN CAST(0 AS FLOAT)
                        ELSE avg(volume)
                        END
                        as volume
                    from 
                        nomics_exchange_market_ticker_thirty_days
                    where 
                        last_updated >= cast(now() - INTERVAL '30 DAYS' as timestamp)
                        and exchange = 'aax'
                    group by 
                        exchange
                ) as thirtyDays
            group by 
                exchange
        ),
        oneYear as (
            SELECT 
                exchange, 
                min(volume) as volume
            FROM 
                (
                    select 
                        exchange, 
                        CASE WHEN avg(volume) is null THEN CAST(0 AS FLOAT)
                        ELSE avg(volume)
                        END
                        as volume
                    from 
                        nomics_exchange_market_ticker_one_year
                    where 
                        last_updated >= cast(now() - INTERVAL '365 DAYS' as timestamp)
                        and exchange = 'aax'
                    group by exchange
                ) as oneYear
            group by 
                exchange
        ),
        YTD as (
            SELECT 
                exchange, 
                min(volume) as volume
            FROM 
                (
                    select 
                        exchange, 
                        CASE WHEN avg(volume) is null THEN CAST(0 AS FLOAT)
                        ELSE avg(volume)
                        END
                        as volume
                    from 
                        nomics_exchange_market_ticker_ytd
                    where 
                        last_updated >= cast(date_trunc('year', current_date) as timestamp)
                        and exchange = 'aax'
                    group by exchange
                ) as YTD
            group by 
                exchange
        ),
		oneDayPrice AS 
			(
				SELECT 
					CAST(Close AS FLOAT) price_by_exchange_1d, 
					lower(base) as symbol
				FROM 
					(
						SELECT 
							AVG(price) as Close, 
							base 
						FROM 
							nomics_exchange_market_ticker
						WHERE 
							timestamp >= cast(now() - INTERVAL '24 HOUR' as timestamp)
							AND base =  'BTC'
							AND exchange = 'aax'
							AND quote IN ('USD', 'USDT', 'USDC')
						GROUP BY 
							base
					) as oneDay
			),
		sevenDaysPrice AS 
			(
				SELECT 
					CAST(Close AS FLOAT) price_by_exchange_7d, 
					lower(base) as symbol
				FROM 
					( 
						SELECT 
							AVG(price) as Close, 
							base 
						FROM 
							nomics_exchange_market_ticker
						WHERE 
							timestamp >=  cast(now() - INTERVAL '7 DAYS' as timestamp)
							AND base =  'BTC'
							AND exchange = 'aax'
							AND quote IN ('USD', 'USDT', 'USDC')
						GROUP BY 
							base
					) as sevenDays
			),
		thirtyDaysPrice AS 
			(
				SELECT 
					CAST(Close AS FLOAT) price_by_exchange_30d, 
					lower(base) as symbol
				FROM 
					( 
						SELECT 
							AVG(price) as Close, 
							base 
						FROM 
							nomics_exchange_market_ticker
						WHERE 
							timestamp >=  cast(now() - INTERVAL '30 DAYS' as timestamp)
							AND base =  'BTC'
							AND exchange = 'aax'
							AND quote IN ('USD', 'USDT', 'USDC')
						GROUP BY 
							base
					) as thirtyDays
			),
		oneYearPrice AS 
			(
				SELECT 
					CAST(Close AS FLOAT) price_by_exchange_1y, 
					lower(base) as symbol
				FROM 
					( 
						SELECT 
							AVG(price) as Close, 
							base 
						FROM 
							nomics_exchange_market_ticker
						WHERE 
							timestamp >=  cast(now() - INTERVAL '365 DAYS' as timestamp)
							AND base =  'BTC'
							AND exchange = 'aax'
							AND quote IN ('USD', 'USDT', 'USDC')
						GROUP BY 
							base
					) as oneYear
			),
		YTDPrice AS 
			(
				SELECT 
					CAST(Close AS FLOAT) price_by_exchange_ytd, 
					lower(base) as symbol
				FROM 
					( 
						SELECT 
							AVG(price) as Close, 
							base 
						FROM 
							nomics_exchange_market_ticker
						WHERE 
							timestamp  >= cast(date_trunc('year', current_date) as timestamp) 
							AND base =  'BTC'
							AND exchange = 'aax'
							AND quote IN ('USD', 'USDT', 'USDC')
						GROUP BY 
							base
					) as YTD
			)
		select 
			ExchangesPrices.Market as Market,
			CASE WHEN ExchangesPrices.Price = 0 THEN null ELSE ExchangesPrices.Price END as Close,
			exchangeMetadata.id as Slug, 
			cast(exchangeHighLight.num_markets as int) as number_of_active_pairs_for_assets,
			cast(oneDay.volume as float) as volume_by_exchange_1d,
			cast(sevenDays.volume as float) as volume_by_exchange_7d,
			cast(thirtyDays.volume as float) volume_by_exchange_30d,
			cast(oneYear.volume as float) as volume_by_exchange_1y,
			cast(YTD.volume as float) volume_by_exchange_ytd , 
			CAST((oneDayPrice.price_by_exchange_1d) AS FLOAT),
			CAST((sevenDaysPrice.price_by_exchange_7d) AS FLOAT),
			CAST((thirtyDaysPrice.price_by_exchange_30d) AS FLOAT),
			CAST((oneYearPrice.price_by_exchange_1y) AS FLOAT),
			CAST((YTDPrice.price_by_exchange_ytd) AS FLOAT)
		from 
			allTime 
			INNER JOIN 
				ExchangesPrices 
			ON 
				ExchangesPrices.Symbol = allTime.symbol
			INNER JOIN 
				exchangeMetadata
			ON
				exchangeMetadata.id = ExchangesPrices.Market
			INNER Join 
				exchangeHighLight
			ON 
				exchangeHighLight.exchange = exchangeMetadata.id
			INNER Join 
				oneDay
			ON 
				oneDay.exchange = exchangeMetadata.id
			INNER Join 
				sevenDays
			ON 
				sevenDays.exchange = exchangeMetadata.id
			INNER Join 
				thirtyDays
			ON 
				thirtyDays.exchange = exchangeMetadata.id
			INNER Join 
				oneYear
			ON 
				oneYear.exchange = exchangeMetadata.id
			INNER Join 
				YTD
			ON 
				YTD.exchange = exchangeMetadata.id
			INNER JOIN
				oneDayPrice 
			ON
				oneDayPrice.symbol = allTime.symbol
			INNER JOIN
			 	sevenDaysPrice
			ON 
				sevenDaysPrice.symbol = allTime.symbol
			INNER JOIN
			 	thirtyDaysPrice 
			ON
			 	thirtyDaysPrice.symbol = allTime.symbol
			INNER JOIN
				 oneYearPrice 
			ON
				 oneYearPrice.symbol = allTime.symbol
			INNER JOIN
				 YTDPrice 
			ON
				 YTDPrice.symbol = allTime.symbol
************************************************************************************************
with oneDayCandles as 
		(

			SELECT lower(base) as symbol 
			FROM 
				nomics_ohlcv_candles
			where timestamp >= cast(now() - INTERVAL '24 HOUR' as timestamp)
			GROUP BY 
				base
		),
	ExchangesPrices AS 
		( 
			SELECT 
				lower(base) as Symbol, 
				exchange as Market
			FROM 
				nomics_exchange_market_ticker
			WHERE 
				exchange NOT IN ('bitmex','hbtc') 
				AND type = 'spot'
				AND timestamp >=  cast(now() - INTERVAL '7 HOUR' as timestamp)
				AND status = 'active'
				AND quote IN ('USD', 'USDT', 'USDC')
			group by 
				base,
				exchange
		),
		market as 
				(
					select
						lower(base) as Symbol, 
						exchange, 
						quote , 
						CONCAT(base, quote) as pair
					from 
						nomics_markets
					group by
						base,
						exchange, 
						quote
					
				),
			assets as 
				(
					select
						lower(id) as base,
						status, 
						last_updated
					from 
						nomics_assets
					group by 
						id
			
				),
			ticker as 
				(
					select
						lower(base) as base,
						type
					from 
						nomics_exchange_market_ticker
					where 
						type != ''
					group by
						base,
						type
				),
			oneDay As 
				(
					SELECT 
						Symbol,   
						CAST(MIN(Price) AS FLOAT) current_price_for_pair_1d
					from
						(
							SELECT 
								lower(base) as Symbol,
								AVG(price) price
							from 
								nomics_exchange_market_ticker
							where 
								timestamp >= cast(now() - INTERVAL '24 HOUR' as timestamp)
							group by 
								base
						) as oneDay
					group by Symbol
				),
			sevenDays As 
				(
					SELECT 
						Symbol,   
						CAST(MIN(Price) AS FLOAT) current_price_for_pair_7d
					from
						(
							SELECT 
								lower(base) as Symbol,
								AVG(price) price
							from 
								nomics_exchange_market_ticker
							where 
								timestamp >= cast(now() - INTERVAL '7 DAYS' as timestamp)
							group by 
								base
						) as sevenDays
					group by Symbol
				),
			thirtyDays As 
				(
					SELECT 
						Symbol,   
						CAST(MIN(Price) AS FLOAT) current_price_for_pair_30d
					from
						(
							SELECT 
								lower(base) as Symbol,
								AVG(price) price
							from 
								nomics_exchange_market_ticker
							where 
								timestamp >= cast(now() - INTERVAL '30 DAYS' as timestamp)
							group by 
								base
						) as thirtyDays
					group by Symbol
				),
			oneYear As 
				(
					SELECT 
						Symbol,   
						CAST(MIN(Price) AS FLOAT) current_price_for_pair_1y
					from
						(
							SELECT 
								lower(base) as Symbol,
								AVG(price) price
							from 
								nomics_exchange_market_ticker
							where 
								timestamp >= cast(now() - INTERVAL '365 DAYS' as timestamp)
							group by 
								base
						) as oneYear
					group by Symbol
				),
			YTD As 
				(
					SELECT 
						Symbol,   
						CAST(MIN(Price) AS FLOAT) current_price_for_pair_ytd
					from
						(
							SELECT 
								lower(base) as Symbol,
								AVG(price) price
							from 
								nomics_exchange_market_ticker
							where 
								timestamp >= cast(date_trunc('year', current_date) as timestamp)
							group by 
								base
						) as YTD
					group by Symbol
				)
	select 
				array_to_json(ARRAY_AGG(json_build_object('Market', ExchangesPrices.Market, 'Symbol', ExchangesPrices.Symbol))) as Exchanges,
				oneDayCandles.symbol,
				array_to_json(ARRAY_AGG(json_build_object(
													'base', market.Symbol, 
													'exchange', market.exchange, 
													'quote', market.quote, 
													'pair', market.pair, 												 
													'pairStatus', assets.status, 
													'update_timestamp', assets.last_updated,
													'TypeOfPair', ticker.type,
													'currentPriceForPair1D', CAST(oneDay.current_price_for_pair_1d AS FLOAT),
													'currentPriceForPair7D', CAST(sevenDays.current_price_for_pair_7d AS FLOAT),
													'currentPriceForPair30D', CAST(thirtyDays.current_price_for_pair_30d AS FLOAT),
													'currentPriceForPair1Y', CAST(oneYear.current_price_for_pair_1y AS FLOAT),
													'currentPriceForPairYTD', CAST(YTD.current_price_for_pair_ytd AS FLOAT)
													))) as MarketPairs
			from 
				oneDayCandles 
				INNER JOIN 
					ExchangesPrices 
				ON 
					ExchangesPrices.Symbol = oneDayCandles.symbol
				INNER JOIN 
					assets
				ON
					assets.base = oneDayCandles.symbol
				INNER JOIN 
					market
				ON
					market.Symbol = oneDayCandles.symbol
				INNER JOIN 
					ticker
				ON
					ticker.base = oneDayCandles.symbol
				INNER JOIN 
					oneDay 
				ON
					oneDay.symbol = oneDayCandles.symbol
				INNER JOIN 
					sevenDays 
				ON
					sevenDays.symbol = oneDayCandles.symbol
				INNER JOIN 
					thirtyDays 
				ON
					thirtyDays.symbol = oneDayCandles.symbol
				INNER JOIN 
					oneYear 
				ON
					oneYear.symbol = oneDayCandles.symbol
				INNER JOIN 
					YTD 
				ON
					YTD.symbol = oneDayCandles.symbol
			group by 
				oneDayCandles.symbol
			
************************************************************************************************

-- SELECT base, type FROM public.nomics_exchange_market_ticker
--  group by base, type ;

with
	market as 
		(
			select
				lower(base) as Symbol, 
				exchange, 
				quote , 
				CONCAT(base, quote) as pair
			from 
				nomics_markets
			group by
				base,
				exchange, 
				quote
			
		),
	assets as 
		(
			select
				lower(id) as base,
				status, 
				last_updated
			from 
				nomics_assets
			group by 
				id
	
		),
	 ticker as 
		(
			select
				 lower(base) as base,
				 type
			 from 
				 nomics_exchange_market_ticker
			 where 
				 type != ''
			 group by
				 base,
				 type
			limit 63000
		 ),

	oneDay As 
		(
			SELECT 
				Symbol,   
				CAST(MIN(Price) AS FLOAT) current_price_for_pair_1d,
				CAST(Min(volume_for_pair_1d) As FLOAT) volume_for_pair_1d
			from
				(
					SELECT 
						lower(ticker.base) as Symbol,
						AVG(ticker.price) price,
						CASE WHEN AVG(one.volume) is null THEN CAST(0 AS FLOAT)
						ELSE AVG(one.volume)
						END
						as volume_for_pair_1d
					from 
						nomics_exchange_market_ticker ticker,
						nomics_exchange_market_ticker_one_day one
					where 
						ticker.timestamp >= cast(now() - INTERVAL '4 DAYS' as timestamp)
						AND one.last_updated >= cast(now() - INTERVAL '4 DAYS' as timestamp)
						AND ticker.base = one.base
						AND ticker.exchange = one.exchange
					group by 
						ticker.base
				) as oneDay
			group by Symbol
		),
	sevenDays As 
		(
			SELECT 
				Symbol,   
				CAST(MIN(Price) AS FLOAT) current_price_for_pair_7d,
				CAST(Min(volume_for_pair_7d) As FLOAT) volume_for_pair_7d
			from
				(
					SELECT 
						lower(ticker.base) as Symbol,
						AVG(ticker.price) price,
						CASE WHEN AVG(seven.volume) is null THEN CAST(0 AS FLOAT)
						ELSE AVG(seven.volume)
						END
						as volume_for_pair_7d 
					from 
						nomics_exchange_market_ticker ticker, 
						nomics_exchange_market_ticker_seven_days seven
					where 
						ticker.timestamp >= cast(now() - INTERVAL '7 DAYS' as timestamp)
						AND seven.last_updated >= cast(now() - INTERVAL '7 DAYS' as timestamp)
						AND ticker.base = seven.base
						AND ticker.exchange = seven.exchange
					group by 
						ticker.base
				) as sevenDays
			group by Symbol
		)
 	thirtyDays As 
 		(
 			SELECT 
 				Symbol,   
 				CAST(MIN(Price) AS FLOAT) current_price_for_pair_30d,
 				CAST(Min(volume_for_pair_30d) As FLOAT) volume_for_pair_30d
 			from
 				(
 					SELECT 
 						lower(ticker.base) as Symbol,
 						AVG(ticker.price) price,
 						CASE WHEN AVG(thirty.volume) is null THEN CAST(0 AS FLOAT)
 						ELSE AVG(thirty.volume)
 						END 
 						as volume_for_pair_30d
 					from 
 						nomics_exchange_market_ticker ticker,
 						nomics_exchange_market_ticker_thirty_days thirty
 					where 
 						ticker.timestamp >= cast(now() - INTERVAL '30 DAYS' as timestamp)
 						AND thirty.last_updated >= cast(now() - INTERVAL '30 DAYS' as timestamp)
 						AND ticker.base = thirty.base
 						AND ticker.exchange = thirty.exchange
 					group by 
 						ticker.base
 				) as thirtyDays
 			group by Symbol
 		),
 	oneYear As 
 		(
 			SELECT 
 				Symbol,   
 				CAST(MIN(Price) AS FLOAT) current_price_for_pair_1y,
 				CAST(Min(volume_for_pair_1y) As FLOAT) volume_for_pair_1y
 			from
 				(
 					SELECT 
 						lower(ticker.base) as Symbol,
 						AVG(ticker.price) price,
 						CASE WHEN AVG(one.volume) is null THEN CAST(0 AS FLOAT)
 						ELSE AVG(one.volume)
 						END
 						as volume_for_pair_1y
 					from 
 						nomics_exchange_market_ticker ticker,
 						nomics_exchange_market_ticker_one_year one
 					where 
 						ticker.timestamp >= cast(now() - INTERVAL '365 DAYS' as timestamp)
 						AND one.last_updated >= cast(now() - INTERVAL '365 DAYS' as timestamp)
 						AND ticker.base = one.base
 						AND ticker.exchange = one.exchange
 					group by 
 						ticker.base
 				) as oneYear
 			group by Symbol
 		),
 	YTD As 
 		(
 			SELECT 
 				Symbol,   
 				CAST(MIN(Price) AS FLOAT) current_price_for_pair_ytd,
 				CAST(Min(volume_for_pair_ytd) As FLOAT) volume_for_pair_ytd
 			from
 				(
 					SELECT 
 						lower(ticker.base) as Symbol,
 						AVG(ticker.price) price,
 						CASE WHEN AVG(ytd.volume) is null THEN CAST(0 AS FLOAT)
 						ELSE AVG(ytd.volume)
 						END
 						as volume_for_pair_ytd
 					from 
 						nomics_exchange_market_ticker ticker,
 						nomics_exchange_market_ticker_ytd ytd
 					where 
 						ticker.timestamp >= cast(date_trunc('year', current_date) as timestamp)
 						AND ytd.last_updated >= cast(date_trunc('year', current_date) as timestamp)
 						AND ticker.base = ytd.base
 						AND ticker.exchange = ytd.exchange
 					group by 
 						ticker.base
 				) as YTD
 			group by Symbol
 		)