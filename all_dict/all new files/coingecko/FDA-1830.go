package internal

import (
	"context"
	"fmt"
	"strings"

	"net/http"
	"os"

	"strconv"

	"sync"

	"time"

	"github.com/Forbes-Media/coingecko-client/coingecko"
	"github.com/Forbes-Media/fda-coingecko-ingestion/store"
	"github.com/Forbes-Media/go-tools/log"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"golang.org/x/exp/slices"
	"golang.org/x/time/rate"
)

var tracer = otel.Tracer("github.com/Forbes-Media/fda-nomics-ingestion/internal")
var c = coingecko.NewClient("")

var (
	cgRateLimit, _    = strconv.Atoi(os.Getenv("CG_RATE_LIMIT"))
	cgmonthlyLimit, _ = strconv.Atoi(os.Getenv("MON_LIMIT"))
)

var (
	lock     = &sync.Mutex{}
	firstRun = true
	/*
		allows for a burst of cgRateLimit, then limits based cgRatelimit per minute, we only burst 1
		this way we dont go over the limit on subsequent calls
	*/
	cgRateLimiter  = rate.NewLimiter(rate.Every(time.Minute/time.Duration(cgRateLimit-1)), 1)
	limiterContext = context.Background()
	coingeckoCalls = 0
	test, _        = store.GetCoinGeckoRate()
)

// Flags to supress usage alert after it has been raised in the session
var (
	usageAt100  = false
	usageAt90   = false
	usageAt75   = false
	usageAtHalf = false
)

// Retreives a list of assets from coingecko and runs
func ConsumeAssetList(w http.ResponseWriter, r *http.Request) {

	labels, span := generateSpan(r, "ConsumeAssetList")
	defer span.End()

	startTime := log.StartTimeL(labels, "ConsumeAssetList")
	var maxRetries = 3
RETRY:
	cgRateLimiter.Wait(limiterContext)
	updatedIds, err := c.GetCoinsList(&coingecko.CoinsListOptions{IncludePlatform: true})
	addToTotalCalls()
	saveCount()

	if err != nil {
		log.EndTimeL(labels, "Error getting Assets: %s", startTime, err)
		if maxRetries > 0 {
			maxRetries--
			goto RETRY
		}
		w.WriteHeader(http.StatusInternalServerError)

	}
	//1. get asset list prior to store
	currentIds, _ := store.GetCoinGeckoIDs()
	//2. store asset list recieved from coingeko
	store.UpsertCoinGecko_Assets(updatedIds)
	//3. check if assets are new. and store historical data if we dont have the data
	bq, err := store.NewBQStore()
	if err != nil {
		log.EndTimeL(labels, "Error getting Assets: %s", startTime, err)
	}

	for _, asset := range *updatedIds {
		if !slices.Contains(currentIds, asset.ID) {
			getHistoricalDataForNewAssets(asset, r, bq)
		}
	}

	saveCount()
	log.EndTimeL(labels, "ConsumeAssetList", startTime, nil)
	span.SetStatus(codes.Ok, "OK")
	w.WriteHeader(200)
	w.Write([]byte("OK"))

}

// Retreives a list of assets from coingecko and runs
func ConsumeCoinGeckoMarkets(w http.ResponseWriter, r *http.Request) {

	labels, span := generateSpan(r, "ConsumeCoinGeckoMarkets")
	defer span.End()

	startTime := log.StartTimeL(labels, "ConsumeCoinGeckoMarkets")

	ids, err := store.GetCoinGeckoIDs()

	if err != nil {
		log.EndTimeL(labels, "Error getting AssetsList: %s", startTime, err)
		w.WriteHeader(http.StatusInternalServerError)
	}

	var (
		throttleChan = make(chan bool, 5)
		wg           sync.WaitGroup
		mu           = &sync.Mutex{}
		marketData   []coingecko.CoinsMarketData
	)

	// This api allows for 250 items per page so we add 250 assets at a time
	for i := 0; i < len(ids); i += 250 {
		throttleChan <- true
		wg.Add(1)
		go func(ids string) {
			var maxRetries = 3
		RETRY:
			cgRateLimiter.Wait(limiterContext)
			data, err := c.GetCoinsMarketData(&coingecko.CoinsMarketOptions{VSCurrency: "usd", Ids: ids, Page: 1, Per_Page: 250})
			addToTotalCalls()
			if err != nil {
				log.EndTimeL(labels, "Error getting Assets: %s", startTime, err)
				if maxRetries > 0 {
					maxRetries--
					goto RETRY
				}

			}
			mu.Lock()
			marketData = append(marketData, *data...)
			mu.Unlock()
			<-throttleChan
			wg.Done()
		}(strings.Join(ids[i:i+250], ","))
	}
	wg.Wait()
	store.CGMarketDataToBQMarketData(&marketData)
	saveCount()
	log.EndTimeL(labels, "ConsumeCoinGeckoMarkets", startTime, nil)
	span.SetStatus(codes.Ok, "OK")
	w.WriteHeader(200)
	w.Write([]byte("OK"))

}

func ConsumeExchangesList(w http.ResponseWriter, r *http.Request) {
	labels, span := generateSpan(r, "ConsumeExchangesList")
	defer span.End()
	startTime := log.StartTimeL(labels, "ConsumeExchangesList")
	var maxRetries = 3
RETRY:
	cgRateLimiter.Wait(limiterContext)
	data, err := c.GetExchangesList()
	addToTotalCalls()
	if err != nil {
		if maxRetries > 0 {
			log.DebugL(labels, "Retrying Call for CoinGeckoExchangesList Attempt #%v ", maxRetries)
			time.Sleep(1 * time.Second) // sleep for a second before retrying. this should help prevent us from overloading CoinGecko with calls
			maxRetries--
			goto RETRY
		}
		log.DebugL(labels, "Retrying Call for CoinGeckoExchangesList Attempt #%v ", maxRetries)
		log.EndTimeL(labels, "Error getting Assets: %s", startTime, err)
		w.WriteHeader(http.StatusInternalServerError)
	}

	store.UpsertCoinGeckoExchanges(data)
	saveCount()
	log.EndTimeL(labels, "ConsumeExchangesList", startTime, nil)
	span.SetStatus(codes.Ok, "OK")
	w.WriteHeader(200)
	w.Write([]byte("OK"))

}

func ConsumeExchangesTickers(w http.ResponseWriter, r *http.Request) {
	var (
		mu               = &sync.Mutex{}
		exchangesTickers []coingecko.ExchangesTickers
		wg               = sync.WaitGroup{}
		throttleChan     = make(chan bool, 10)
	)

	labels, span := generateSpan(r, "ConsumeExchangesTickers")
	defer span.End()

	startTime := log.StartTimeL(labels, "ConsumeExchangesTickers")

	cgRateLimiter.Wait(limiterContext)

	exchangesIDs, exchangesIDsErr := store.GetExchangesList()

	if exchangesIDsErr != nil {
		log.EndTimeL(labels, "Error getting Exchanges IDs from PG : %s", startTime, exchangesIDsErr)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	ids, err := store.GetCoinGeckoIDs()

	if err != nil {
		log.EndTimeL(labels, "Error getting AssetsList: %s", startTime, err)
		w.WriteHeader(http.StatusInternalServerError)
	}

	span.AddEvent("Starting Getting Exchanges Tickers By exchangesIDs from Coingecko")

	for i := 0; i < len(exchangesIDs); i++ {
		for j := 0; j < len(ids); j += 250 {
			throttleChan <- true
			wg.Add(1)

			go func(exchangeId string, coinIds string) {
				exchangeOption := coingecko.ExchangesTickersOptions{
					CoinIds:             coinIds,
					IncludeExchangeLogo: true,
					Page:                1,
					Depth:               "",
					Order:               "",
				}
				maxRetries := 3
			RETRY:
				exchangesTickersData, exchangesTickersError := c.GetExchangesTickers(exchangeId, &exchangeOption)
				addToTotalCalls()
				if exchangesTickersError != nil {
					log.ErrorL(labels, "Error getting Exchange Ticker data from CoinGecko: %s", exchangesTickersError)
					if maxRetries > 0 {
						log.DebugL(labels, "Retrying Call for GetExchangesTickers from CoinGecko Attempt #%v ", maxRetries)
						time.Sleep(1 * time.Second) // sleep for a 3 second before retrying. this should help prevent us from overloading CoinGecko with calls
						maxRetries--
						goto RETRY
					}
					<-throttleChan
					wg.Done()
					return
				}

				mu.Lock()
				exchangesTickers = append(exchangesTickers, *exchangesTickersData)
				mu.Unlock()
				<-throttleChan
				wg.Done()

			}(exchangesIDs[i], strings.Join(ids[j:j+250], ","))
		}

	}
	wg.Wait()
	log.InfoL(labels, "END ConsumeExchangesTickers TotalTime:%.2fm", time.Since(startTime).Minutes())
	span.AddEvent("Starting Upsert ExchangesTickers to PG")

	span.AddEvent("Starting Upsert ExchangesTickers to PG")

	go func(data []coingecko.ExchangesTickers) {
		InsertBQExchangesTickersHandler(labels, startTime, data)
		saveCount()
	}(exchangesTickers)

	log.EndTimeL(labels, "ConsumeExchangesTickers", startTime, nil)
	span.SetStatus(codes.Ok, "OK")
	w.WriteHeader(200)
	w.Write([]byte("OK"))

}

func InsertBQExchangesTickersHandler(labels map[string]string, startTime time.Time, data []coingecko.ExchangesTickers) {

	bq, err := store.NewBQStore()
	if err != nil {
		log.EndTimeL(labels, "Error getting Assets: %s", startTime, err)
	}

	var start = time.Now()

	exchangesMapper := store.CGExchangesTickersToBQExchangesTickers(&data)

	log.InfoL(labels, "START Insert Exchanges Tickers Data to BQ")
	bq.InsertExchangesTickersData(exchangesMapper)
	log.InfoL(labels, "END Insert Exchanges Tickers Data to BQ, totalTime:%.2fs", time.Since(start).Seconds())

}

func generateSpan(r *http.Request, funcName string) (map[string]string, trace.Span) {

	span := trace.SpanFromContext(r.Context())

	labels := make(map[string]string)
	labels["function"] = funcName
	labels["UUID"] = uuid.New().String()
	span.SetAttributes(attribute.String("UUID", labels["UUID"]))
	labels["traceID"] = span.SpanContext().TraceID().String()
	labels["spanID"] = span.SpanContext().SpanID().String()

	return labels, span
}

/*
gets historical data for prices, volume, and market cap
*/
func getHistoricalDataForNewAssets(asset coingecko.Coins, r *http.Request, bq *store.BQStore) {
	labels, span := generateSpan(r, "getCoinHistoryData")
	defer span.End()

	startTime := log.StartTimeL(labels, "getCoinHistoryData")

	var maxRetries = 3

	var years = -5
	var days = 0
RETRY:

	var toDate = time.Now().Unix()
	var fromDate = time.Now().AddDate(years, 0, days).Unix()
	cgRateLimiter.Wait(limiterContext)
	data, err := c.GetCoinMarketChartRange(asset.ID, &coingecko.CoinMarketChartRangeOptions{VS_Currency: "usd", From: fromDate, To: toDate})
	addToTotalCalls()

	if err != nil {

		if maxRetries > 0 {
			maxRetries--
			time.Sleep(1 * time.Second)
			goto RETRY
		}
		log.EndTimeL(labels, "Error getting Assets: %s", startTime, err)

	}
	/*
		If we dont have an error but the api did not return history for 5 years
		start to shrink tha amount of history we need
	*/
	if data == nil {
		log.DebugL(labels, "No History found for %s,to:%s,from: %s retrying with new date", asset.ID, toDate, fromDate)
		if years == -5 {
			years = -1
			goto RETRY

		} else if years == -1 {
			years = 0
			days = -60
			goto RETRY
		} else if days == -60 {
			years = 0
			days = -30
			goto RETRY
		} else if days == -30 {
			years = 0
			days = -7
			goto RETRY
		} else if days == -7 {
			years = 0
			days = -1
			goto RETRY
		}
	}
	if data != nil {

		if err != nil {
			log.EndTimeL(labels, "Error storing history: %s", startTime, err)
		}
		bqData := store.CGHistoryToBQMarketData(asset, *data)

		bq.InsertTickerData(bqData)
	}
	//fmt.Println(len(data.MarketCaps), len(data.Prices), len(data.TotalVolumes))

	log.EndTimeL(labels, "getCoinHistoryData", startTime, nil)

}

func GetLimitCount() int {
	return coingeckoCalls
}

// adds to the rate limit count
func addToTotalCalls() {
	lock.Lock()
	test.Count++
	lock.Unlock()

	pct := float32(test.Count) / float32(cgmonthlyLimit)
	fmt.Println(pct)
	if float64(test.Count/cgmonthlyLimit) > float64(1) && usageAt100 == false {
		usageAt100 = true
		log.Alert("CoinGecko Usage is at or past 100%")
	} else if float64(test.Count/cgmonthlyLimit) > float64(0.90) && usageAt90 == false {
		usageAt90 = true
		log.Alert("CoinGecko Usage is at or past 90%")
	} else if float64(test.Count/cgmonthlyLimit) >= float64(0.75) && usageAt75 == false {
		usageAt75 = true
		log.Alert("CoinGecko Usage is at or past 75%")
	} else if float64(test.Count/cgmonthlyLimit) >= float64(0.5) && usageAtHalf == false {
		usageAtHalf = true
		log.Alert("CoinGecko Usage is at or past 50%")
	}

}

// updates the coingecko count to postgres
func saveCount() {
	lock.Lock()
	store.SaveCGRate(test)
	lock.Unlock()
}


// bigquery
type BQExchangesTickers struct {
	Name    string     `bigquery:"name" json:"name"`       //name of exchange
	Tickers []BQTicker `bigquery:"tickers" json:"tickers"` //list of tickers
}

type BQTicker struct {
	Base                   string                 `bigquery:"base" json:"base,omitempty"`                                           //Ticker's coin
	Target                 string                 `bigquery:"target" json:"target,omitempty"`                                       //Ticker's target coin
	Market                 BQMarketSimple         `bigquery:"market" json:"market,omitempty"`                                       //Ticker's simple market
	Last                   bigquery.NullFloat64   `bigquery:"last" json:"last,omitempty"`                                           //Ticker's last price against the target token
	Volume                 bigquery.NullFloat64   `bigquery:"volume" json:"volume,omitempty"`                                       //Ticker's volume
	ConvertedLast          BQConvertedLast        `bigquery:"converted_last" json:"converted_last,omitempty"`                       //Ticker's last traded price
	ConvertedVolume        BQConvertedVolume      `bigquery:"converted_volume" json:"converted_volume,omitempty"`                   //Ticker's converted volume
	CostToMoveUpUsd        bigquery.NullFloat64   `bigquery:"cost_to_move_up_usd" json:"cost_to_move_up_usd,omitempty"`             //Cost to move up in USD
	CostToMoveDownUsd      bigquery.NullFloat64   `bigquery:"cost_to_move_down_usd" json:"cost_to_move_down_usd,omitempty"`         //Cost to move down in USD
	TrustScore             string                 `bigquery:"trust_score" json:"trust_score,omitempty"`                             //Trust score
	BidAskSpreadPercentage bigquery.NullFloat64   `bigquery:"bid_ask_spread_percentage" json:"bid_ask_spread_percentage,omitempty"` //Bid & Ask spread's percentage
	Timestamp              bigquery.NullTimestamp `bigquery:"timestamp" json:"timestamp,omitempty"`                                 //Timestamp
	LastTradedAt           bigquery.NullTimestamp `bigquery:"last_traded_at" json:"last_traded_at,omitempty"`                       //Last traded at timestamp
	LastFetchAt            bigquery.NullTimestamp `bigquery:"last_fetch_at" json:"last_fetch_at,omitempty"`                         //Last data fetched at timestamp
	IsAnomaly              bool                   `bigquery:"is_anomaly" json:"is_anomaly,omitempty"`                               //Whether the ticker is an anomaly
	IsStale                bool                   `bigquery:"is_stale" json:"is_stale,omitempty"`                                   //Whether the ticker is stale!
	TradeURL               string                 `bigquery:"trade_url" json:"trade_url,omitempty"`                                 //Trade URL
	TokenInfoURL           string                 `bigquery:"token_info_url" json:"token_info_url,omitempty"`                       //URL of the ticker
	CoinID                 string                 `bigquery:"coin_id" json:"coin_id,omitempty"`                                     //Coin ID
	TargetCoinID           string                 `bigquery:"target_coin_id" json:"target_coin_id,omitempty"`                       // Target coin's id
}

type BQMarketSimple struct {
	Name                string `bigquery:"name" json:"name,omitempty"`                                   //market's name
	Identifier          string `bigquery:"identifier" json:"identifier,omitempty"`                       //market's id
	HasTradingIncentive bool   `bigquery:"has_trading_incentive" json:"has_trading_incentive,omitempty"` //Whether the market has trading incentives
}
type BQConvertedLast struct {
	Btc bigquery.NullFloat64 `bigquery:"btc" json:"btc,omitempty"` //in BTC
	Eth bigquery.NullFloat64 `bigquery:"eth" json:"eth,omitempty"` //in ETH
	Usd bigquery.NullFloat64 `bigquery:"usd" json:"usd,omitempty"` //in USD
}

// Converted volume
type BQConvertedVolume struct {
	Btc bigquery.NullFloat64 `bigquery:"btc" json:"btc,omitempty"` //in BTC
	Eth bigquery.NullFloat64 `bigquery:"eth" json:"eth,omitempty"` //in ETH
	Usd bigquery.NullFloat64 `bigquery:"usd" json:"usd,omitempty"` //in USD
}

// ppostreasql
type ExchangeList struct {
	ID string `json:id,omitempty`
}

// adapter 
func CGExchangesTickersToBQExchangesTickers(cgData *[]coingecko.ExchangesTickers) *[]models.BQExchangesTickers {

	var bqExchangesTickersData []models.BQExchangesTickers
	var bqTickerData []models.BQTicker

	for _, exchangesTickersData := range *cgData {
		for _, tickers := range exchangesTickersData.Tickers {
			bqTickerData = append(bqTickerData, models.BQTicker{
				Base:   tickers.Base,
				Target: tickers.Target,
				Market: models.BQMarketSimple{
					Name:                tickers.Market.Name,
					Identifier:          tickers.Market.Identifier,
					HasTradingIncentive: tickers.Market.HasTradingIncentive,
				},
				Last:   bigquery.NullFloat64{Float64: tickers.Last, Valid: true},
				Volume: bigquery.NullFloat64{Float64: tickers.Volume, Valid: true},
				ConvertedLast: models.BQConvertedLast{
					Btc: bigquery.NullFloat64{Float64: tickers.ConvertedLast.Btc, Valid: true},
					Eth: bigquery.NullFloat64{Float64: tickers.ConvertedLast.Eth, Valid: true},
					Usd: bigquery.NullFloat64{Float64: tickers.ConvertedLast.Usd, Valid: true},
				},
				ConvertedVolume: models.BQConvertedVolume{
					Btc: bigquery.NullFloat64{Float64: tickers.ConvertedVolume.Btc, Valid: true},
					Eth: bigquery.NullFloat64{Float64: tickers.ConvertedVolume.Eth, Valid: true},
					Usd: bigquery.NullFloat64{Float64: tickers.ConvertedVolume.Usd, Valid: true},
				},
				CostToMoveUpUsd:        bigquery.NullFloat64{Float64: tickers.CostToMoveUpUsd, Valid: true},
				CostToMoveDownUsd:      bigquery.NullFloat64{Float64: tickers.CostToMoveDownUsd, Valid: true},
				TrustScore:             tickers.TrustScore,
				BidAskSpreadPercentage: bigquery.NullFloat64{Float64: tickers.BidAskSpreadPercentage, Valid: true},
				Timestamp: bigquery.NullTimestamp{Timestamp: tickers.Timestamp, Valid: true},
				LastTradedAt: bigquery.NullTimestamp{Timestamp: tickers.LastTradedAt, Valid: true},
				LastFetchAt: bigquery.NullTimestamp{Timestamp: tickers.LastFetchAt, Valid: true},
				IsAnomaly: tickers.IsAnomaly,
				IsStale: tickers.IsStale,
				TradeURL: tickers.TradeURL,
				TokenInfoURL: tickers.TokenInfoURL,
				CoinID: tickers.CoinID,
				TargetCoinID: tickers.TargetCoinID,
			})
		}
		bqExchangesTickersData = append(bqExchangesTickersData, models.BQExchangesTickers{
			Name:    exchangesTickersData.Name,
			Tickers: bqTickerData,
		})
	}

	return &bqExchangesTickersData
}
// bigquery 2
func (bq *BQStore) InsertExchangesTickersData(exchangesTickers *[]models.BQExchangesTickers) error {
	ctx := context.Background()

	currenciesTable := GetTableName("Digital_Asset_Exchanges_Tickers_Data")

	bqInserter := bq.Dataset("digital_assets").Table(currenciesTable).Inserter()
	bqInserter.IgnoreUnknownValues = true

	inserterErr := bqInserter.Put(ctx, *exchangesTickers)
	var retryError error
	if inserterErr != nil {
		if strings.Contains(inserterErr.Error(), "413") {
			log.Info("413 Error. Breaking up tickers and retrying insert")
			l := len(*exchangesTickers)
			var ticks []models.BQExchangesTickers
			ticks = append(ticks, *exchangesTickers...)
			for y := (l / 3); y < l; y += (l / 3) {
				a := ticks[y-(l/3) : y]
				er := bq.InsertExchangesTickersData(&a)
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

	return nil
}
// post 
package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"reflect"

	"github.com/Forbes-Media/coingecko-client/coingecko"
	"github.com/Forbes-Media/fda-coingecko-ingestion/models"
	"github.com/Forbes-Media/go-tools/log"
	_ "github.com/lib/pq"
)

var (
	pg           *sql.DB
	DBClientOnce sync.Once
)

func PGConnect() *sql.DB {
	println(sql.ErrNoRows)
	if pg == nil {
		var err error
		DBClientOnce.Do(func() {
			connectionString := fmt.Sprintf("port=%s host=%s user=%s password=%s dbname=%s sslmode=%s", os.Getenv("DB_PORT"), os.Getenv("DB_HOST"), os.Getenv("DB_USER"), os.Getenv("DB_PASSWORD"), os.Getenv("DB_NAME"), os.Getenv("DB_SSLMODE"))

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

func UpsertCoinGecko_Assets(assetList *[]coingecko.Coins) error {

	pg := PGConnect()

	assetListTMP := *assetList
	valueString := make([]string, 0, len(*assetList))
	valueArgs := make([]interface{}, 0, len(*assetList)*4)
	tableName := "coingecko_assets"

	var i = 0 //used for argument positions

	for y := 0; y < len(assetListTMP); y++ {

		var candleData = assetListTMP[y]

		v := reflect.ValueOf(candleData.Platforms)
		platforms := make(map[string]string)

		if v.Kind() == reflect.Map {
			for _, key := range v.MapKeys() {
				strct := v.MapIndex(key)
				platforms[fmt.Sprint(key.Interface())] = fmt.Sprint(strct.Interface())
			}
		}

		plJSON, _ := json.Marshal(platforms)

		var valString = fmt.Sprintf("($%d,$%d,$%d,$%d)", i*4+1, i*4+2, i*4+3, i*4+4)
		//pairsString = append(pairsString, fmt.Sprintf("%s/%s", candleData.Base, candleData.Quote))
		valueString = append(valueString, valString)
		valueArgs = append(valueArgs, candleData.ID)
		valueArgs = append(valueArgs, candleData.Symbol)
		valueArgs = append(valueArgs, candleData.Name)
		valueArgs = append(valueArgs, string(plJSON))

		i++

		if len(valueArgs) >= 65000 || y == len(assetListTMP)-1 {
			insertStatementCandles := fmt.Sprintf("INSERT INTO %s VALUES %s", tableName, strings.Join(valueString, ","))
			updateStatement := "ON CONFLICT (id) DO UPDATE SET symbol = EXCLUDED.symbol, name = EXCLUDED.name, platforms = EXCLUDED.platforms"

			query := insertStatementCandles + updateStatement
			_, inserterError := pg.Exec(query, valueArgs...)

			if inserterError != nil {
				log.Error("%s", inserterError)
			}

			valueString = make([]string, 0, len(assetListTMP))
			valueArgs = make([]interface{}, 0, len(assetListTMP)*4)

			i = 0
		}
	}

	return nil
}

// returns a list of symbols
func GetCoinGeckoIDs() ([]string, error) {
	var coingecko_assets []string

	pg := PGConnect()

	query := `
		SELECT 
			ID 
		FROM coingecko_assets
	`

	queryResult, err := pg.Query(query)

	var id string
	if err != nil {
		return coingecko_assets, err
	}
	for queryResult.Next() {
		err := queryResult.Scan(&id)

		if err != nil {
			return coingecko_assets, err
		}
		coingecko_assets = append(coingecko_assets, id)
	}
	//log.Info("%s", id)
	return coingecko_assets, nil
}

func UpsertCoinGeckoExchanges(exchangesList *[]coingecko.ExchangeListShort) error {
	pg := PGConnect()

	exchangesListTMP := *exchangesList
	valueString := make([]string, 0, len(*exchangesList))
	for y := 0; y < len(exchangesListTMP); y++ {
		var exchange = exchangesListTMP[y]
		var valString = fmt.Sprintf("('%s','%s')::coingecko_exchange", exchange.ID, exchange.Name)
		valueString = append(valueString, valString)
	}
	exchangesData := strings.Join(valueString, ",")

	exchangeStoredProc := fmt.Sprintf("CALL upsertCoingeckoExchanges(ARRAY[%s])", exchangesData)
	_, inserterError := pg.Exec(exchangeStoredProc)
	if inserterError != nil {
		log.Error("%s", inserterError)
	}
	return nil
}

// returns a list of symbols
func GetCoinGeckoRate() (models.CoingeckoCount, error) {
	var coingeckoCount models.CoingeckoCount

	pg := PGConnect()

	query := `
		SELECT 
			*
		FROM coingecko_counthistory 
		order by last_updated desc 
		limit 1
	`

	queryResult, err := pg.Query(query)

	if err != nil {
		return coingeckoCount, err
	}
	for queryResult.Next() {
		err := queryResult.Scan(&coingeckoCount.Count, &coingeckoCount.LastUpdated)

		if err != nil {
			return coingeckoCount, err
		}
	}
	//If we dont have a valid result (This should only happen at first deploy), or the current day is after a month
	if (coingeckoCount.Count == 0 && coingeckoCount.LastUpdated.IsZero()) || time.Now().After(coingeckoCount.LastUpdated.AddDate(0, 30, 0)) {
		coingeckoCount.LastUpdated = time.Now()
		coingeckoCount.Count = 0
		SaveCGRate(coingeckoCount)
	}
	//log.Info("%s", id)
	return coingeckoCount, nil
}

// returns a list of symbols
func SaveCGRate(cgCount models.CoingeckoCount) error {

	pg := PGConnect()
	//valString := fmt.Sprintf("(%v,'%s')::coingecko_counthist", cgCount.Count, cgCount.LastUpdated)

	//query := fmt.Sprintf("call upsertCGCount(%s)", valString)

	_, err := pg.Exec("call upsertCGCount(($1,$2)::coingecko_counthist)", cgCount.Count, cgCount.LastUpdated)

	if err != nil {
		return err
	}

	//log.Info("%s", id)
	return nil
}

func GetExchangesList() ([]string, error) {
	pg := PGConnect()

	var exchangesIds []string
	query := `
		SELECT id from public.getCoinGeckoExchangesList()
	`
	queryResult, err := pg.Query(query)
	if err != nil {
		log.Error("Error Getting Coingecko Exchanges List from  PG  %s", err)
		return nil, err
	}

	for queryResult.Next() {
		var exchange models.ExchangeList
		err := queryResult.Scan(&exchange.ID)
		if err != nil {
			log.Error("Error Mapping Coingecko Exchanges List %s", err)
			return nil, err
		}
		exchangesIds = append(exchangesIds, exchange.ID)
	}

	return exchangesIds, nil

}

// if we don't need to add CGExchangesTickers to PG this function will be removed
func UpsertCoinGeckoExchangesTickers(exchangesTickers *[]coingecko.ExchangesTickers) error {
	pg := PGConnect()
	exchangesTickersTMP := *exchangesTickers
	valueString := make([]string, 0, len(*exchangesTickers))
	for y := 0; y < len(exchangesTickersTMP); y++ {
		var exchange = exchangesTickersTMP[y]
		var valString = fmt.Sprintf("('%s', json('%v'))::coingecko_exchanges_tickers", exchange.Name, exchange.Tickers)
		valueString = append(valueString, valString)
	}
	exchangesTickersData := strings.Join(valueString, ",")

	_, inserterError := pg.Exec("CALL upsertCoingeckoExchangesTickers(ARRAY[%s])", exchangesTickersData)
	if inserterError != nil {
		log.Error("Error Upsert Coingecko Exchanges Tickers to PG  %s", inserterError)
	}
	return nil
}

