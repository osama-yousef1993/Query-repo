package internal

import (
	"context"
	"fmt"
	"html"
	"math"
	"strings"

	"net/http"
	"os"

	"strconv"

	"sync"

	"time"

	"github.com/Forbes-Media/coingecko-client/coingecko"
	"github.com/Forbes-Media/fda-coingecko-ingestion/models"
	"github.com/Forbes-Media/fda-coingecko-ingestion/store"
	"github.com/Forbes-Media/fda-coingecko-ingestion/utils"
	"github.com/Forbes-Media/go-tools/log"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"golang.org/x/exp/slices"
	"golang.org/x/sync/syncmap"
	"golang.org/x/time/rate"
)

var tracer = otel.Tracer("github.com/Forbes-Media/fda-nomics-ingestion/internal")
var c = coingecko.NewClient(os.Getenv("COINGECKO_API_KEY"), os.Getenv("COINGECKO_URL"))

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
	test, _        = store.GetCoinGeckoRate(context.Background())

	/* verifiedImages is a local cache of verified large image URLs of NFTs.
	 */
	verifiedImages = syncmap.Map{}
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
	ctx, span := tracer.Start(r.Context(), "ConsumeAssetList")
	defer span.End()
	labels := generateLabelFromContext(ctx)

	startTime := log.StartTimeL(labels, "ConsumeAssetList")
	var maxRetries = 3
	bq, err := store.NewBQStore()
	if err != nil {
		log.EndTimeL(labels, "ConsumeAssetList: Error connecting to BigQuery Client: %s", startTime, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
RETRY:
	cgRateLimiter.Wait(limiterContext)
	updatedIds, err := c.GetCoinsList(ctx, &coingecko.CoinsListOptions{IncludePlatform: true})
	addToTotalCalls(ctx)
	saveCount(ctx)

	if err != nil {
		log.EndTimeL(labels, "ConsumeAssetList: Error getting data from CoinGecko API: %s", startTime, err)
		if maxRetries > 0 {
			maxRetries--
			goto RETRY
		}
		w.WriteHeader(http.StatusInternalServerError)

	}
	//1. get asset list prior to store
	currentIds, _ := store.GetCoinGeckoIDs(ctx)
	//2. store asset list recieved from coingeko
	store.UpsertCoinGecko_Assets(ctx, updatedIds)
	//3. check if assets are new. and store historical data if we dont have the data

	var (
		throttleChan = make(chan bool, 20)
		wg           sync.WaitGroup
		bgsData      []models.BQDAMarketData
	)
	for _, asset := range *updatedIds {
		throttleChan <- true
		wg.Add(1)
		if !slices.Contains(currentIds, asset.ID) {
			log.DebugL(labels, "ConsumeAssetList: Start Getting Historical Data For : %s", asset)
			bgData := getHistoricalDataForNewAssets(asset, ctx)
			if bgData != nil {
				bgsData = append(bgsData, *bgData...)
			}

		}
		<-throttleChan
		wg.Done()
	}
	// Insert assets list to BigQuery
	bq.InsertTickerData(ctx, &bgsData)

	saveCount(ctx)
	log.EndTimeL(labels, "ConsumeAssetList: Successfully finished consuming asset list From CoinGecko", startTime, nil)
	span.SetStatus(codes.Ok, "OK")
	w.WriteHeader(200)
	w.Write([]byte("OK"))

}

// Retreives a list of assets from coingecko and runs
func ConsumeCoinGeckoMarkets(w http.ResponseWriter, r *http.Request) {

	ctx, span := tracer.Start(r.Context(), "ConsumeCoinGeckoMarkets")
	defer span.End()
	labels := generateLabelFromContext(ctx)

	startTime := log.StartTimeL(labels, "ConsumeCoinGeckoMarkets")

	ids, err := store.GetCoinGeckoIDs(ctx)

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		log.EndTimeL(labels, "ConsumeCoinGeckoMarkets: Error retrieving CoinGecko IDs from PostgreSQL: %s", startTime, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	start := html.EscapeString(r.URL.Query().Get("start"))
	var startIndex int
	end := html.EscapeString(r.URL.Query().Get("end"))
	var endIndex int

	startIndex, err = strconv.Atoi(start)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		log.EndTimeL(labels, "ConsumeCoinGeckoMarkets: Error converting startIndex string to integer: %s", startTime, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	//if we are not getting prices for the max number in the list, set the end index
	if end != "max" {
		endIndex, err = strconv.Atoi(end)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			log.EndTimeL(labels, "ConsumeCoinGeckoMarkets: Error converting endIndex string to integer: %s", startTime, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	} else {
		endIndex = len(ids)
	}

	bq, err := store.NewBQStore()
	if err != nil {
		log.EndTimeL(labels, "ConsumeCoinGeckoMarkets: Error connecting to BigQuery Client: %s", startTime, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var (
		throttleChan = make(chan bool, 5)
		wg           sync.WaitGroup
		mu           = &sync.Mutex{}
		marketData   []coingecko.CoinsMarketData
		sliceEnd     int
	)

	// This api allows for 250 items per page so we add 250 assets at a time
	for i := (startIndex); i < endIndex; i += 250 {
		throttleChan <- true
		wg.Add(1)
		sliceEnd = i + 250
		if sliceEnd > len(ids) {
			sliceEnd = len(ids)
		}
		go func(ids string) {
			var maxRetries = 3
		RETRY:
			cgRateLimiter.Wait(limiterContext)
			data, err := c.GetCoinsMarketData(ctx, &coingecko.CoinsMarketOptions{VSCurrency: "usd", Ids: ids, Page: 1, Per_Page: 250})
			addToTotalCalls(ctx)
			if err != nil {
				log.EndTimeL(labels, "ConsumeCoinGeckoMarkets: Error retrieving CoinsMarketData from CoinGecko API: %s", startTime, err)
				if maxRetries > 0 {
					maxRetries--
					goto RETRY
				}

			}
			mu.Lock()
			if data != nil {
				marketData = append(marketData, *data...)
			}
			mu.Unlock()
			<-throttleChan
			wg.Done()
		}(strings.Join(ids[i:sliceEnd], ","))
	}
	wg.Wait()
	saveCount(ctx)
	log.DebugL(labels, "ConsumeCoinGeckoMarkets: Start Converting CGMarketDataToBQMarketData")
	data := store.CGMarketDataToBQMarketData(ctx, &marketData)
	log.DebugL(labels, "ConsumeCoinGeckoMarkets: Start Inserting CGMarketDataToBQMarketData To BQ")
	bq.InsertTickerData(ctx, data)
	log.EndTimeL(labels, "ConsumeCoinGeckoMarkets: Successfully finished consuming CoinGeckoMarkets", startTime, nil)
	span.SetStatus(codes.Ok, "OK")
	w.WriteHeader(200)
	w.Write([]byte("OK"))

}

// Retreives a list of assets from postgres and fetches metadata for each of those assets. Then it stores the metadata in coingecko_asset_metadata table
func ConsumeAssetMetadata(w http.ResponseWriter, r *http.Request) {

	ctx, span := tracer.Start(context.Background(), "ConsumeAssetMetadata") // this is a long process that takes over 30min which is the max timeout for gcp scheduler. So we will use context.Background() and send a 202 accepted

	defer span.End()
	labels := generateLabelFromContext(ctx)

	startTime := log.StartTimeL(labels, "ConsumeAssetMetadata")

	var wg sync.WaitGroup
	throttleChan := make(chan bool, 20) //question: how much should this be?

	var (
		allCoins []coingecko.CoinsCurrentData
		mu       = &sync.Mutex{}
	)

	allAssets, getAssetsErr := store.GetCoinGeckoIDs(ctx)
	if getAssetsErr != nil {
		log.ErrorL(labels, "ConsumeAssetMetadata: Error retrieving CoinGecko IDs from PostgreSQL: %v", getAssetsErr)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if len(allAssets) <= 0 {
		log.ErrorL(labels, "ConsumeAssetMetadata: No assets loaded!")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte("ACCEPTED"))
	go func(allAssets []string) {
		log.Info("Loaded %d Forbes Supported Assets", len(allAssets))
		span.AddEvent("Starting Markets Loop")
		for index, asset := range allAssets {

			throttleChan <- true
			wg.Add(1)
			go func(index int, asset string) {
				currentCoinOptions := coingecko.CoinsCurrentDataOptions{
					Tickers:        false,
					Market_Data:    false,
					Community_Data: true,
					Developer_Data: true,
					Sparkline:      false,
				}
				var maxRetries = 3
			RETRY:
				cgRateLimiter.Wait(limiterContext)
				coinData, getCoinErr := c.GetCurrentCoinData(ctx, asset, &currentCoinOptions)
				addToTotalCalls(ctx)
				if getCoinErr != nil {
					log.ErrorL(labels, "ConsumeAssetMetadata: Error retrieving coin data from CoinGecko API: %s", getCoinErr)
					if maxRetries > 0 && !strings.Contains(getCoinErr.Error(), "404") {
						log.DebugL(labels, "ConsumeAssetMetadata: Retrying call for Asset %s . Attempt #%v ", asset, maxRetries)
						time.Sleep(1 * time.Second) // sleep for a second before retrying. this should help prevent us from overloading nomics with calls
						maxRetries--
						goto RETRY
					}
					log.DebugL(labels, "ConsumeAssetMetadata: Retrying call for Asset %s . Attempt #%v ", asset, maxRetries)
					<-throttleChan
					wg.Done()
					return
				}

				mu.Lock()
				allCoins = append(allCoins, *coinData)
				mu.Unlock()

				<-throttleChan
				wg.Done()
			}(index, asset)
		}

		wg.Wait()
		log.DebugL(labels, "ConsumeAssetMetadata: Start upserting allCoins data to PostgreSql")
		upsertErr := store.UpsertAssetMetadata(ctx, &allCoins)

		if upsertErr != nil {
			log.ErrorL(labels, "ConsumeAssetMetadata: Error upserting Asset Metadata to PostgreSQL at time %s, error %v", startTime, upsertErr)
			return
		}
		saveCount(ctx)

		log.EndTimeL(labels, "ConsumeAssetMetadata: Successfully finished consuming AssetMetadata", startTime, nil)
		span.SetStatus(codes.Ok, "OK")
	}(allAssets)

}

// Retreives a list of assets from postgres and fetches metadata for each of those assets. Then it stores the metadata in coingecko_asset_metadata table
func ConsumeExchangeMetadata(w http.ResponseWriter, r *http.Request) {

	ctx, span := tracer.Start(r.Context(), "ConsumeExchangeMetadata")
	defer span.End()
	labels := generateLabelFromContext(ctx)

	startTime := log.StartTimeL(labels, "ConsumeExchangeMetadata")

	var wg sync.WaitGroup
	throttleChan := make(chan bool, 20) //question: how much should this be?

	var (
		allExchanges []coingecko.FullExchange
		mu           = &sync.Mutex{}
	)

	fullList, getListErr := store.GetExchangesList(ctx)
	if getListErr != nil {
		log.ErrorL(labels, "ConsumeExchangeMetadata: Error retrieving ExchangesList data for PostgreSQL: %v", getListErr)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if len(fullList) <= 0 {
		log.ErrorL(labels, "ConsumeExchangeMetadata: No Exchanges loaded!")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	log.Info("ConsumeExchangeMetadata: Loaded %d Forbes Supported exchanges", len(fullList))
	span.AddEvent("ConsumeExchangeMetadata: Starting Exchanges Loop")
	for index, exchangeId := range fullList {

		throttleChan <- true
		wg.Add(1)
		go func(index int, exchangeId string) {
			var maxRetries = 3
		RETRY:
			cgRateLimiter.Wait(limiterContext)
			exchangeData, getCoinErr := c.GetExchangesId(ctx, exchangeId)
			addToTotalCalls(ctx)
			if getCoinErr != nil {
				log.ErrorL(labels, "ConsumeExchangeMetadata: Error retrieving exchange metadata from CoinGecko API: %s", getCoinErr)
				if maxRetries > 0 {
					log.DebugL(labels, "ConsumeExchangeMetadata: Retrying call for exchangeId %s . Attempt #%v ", exchangeId, maxRetries)
					time.Sleep(1 * time.Second) // sleep for a second before retrying. this should help prevent us from overloading nomics with calls
					maxRetries--
					goto RETRY
				}
				log.DebugL(labels, "ConsumeExchangeMetadata: Retrying call for exchangeId %s . Attempt #%v ", exchangeId, maxRetries)
				<-throttleChan
				wg.Done()
				return
			}
			exchangeData.ID = exchangeId
			mu.Lock()
			allExchanges = append(allExchanges, *exchangeData)
			mu.Unlock()

			<-throttleChan
			wg.Done()
		}(index, exchangeId)
	}

	wg.Wait()
	log.DebugL(labels, "ConsumeExchangeMetadata: Start upserting allExchanges data to PostgreSql")
	upsertErr := store.UpsertExchangeMetadata(ctx, &allExchanges)

	if upsertErr != nil {
		log.ErrorL(labels, "ConsumeExchangeMetadata: Error upserting Exchange Metadata to PostgreSQL at time %s, error %v", startTime, upsertErr)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	saveCount(ctx)

	log.EndTimeL(labels, "ConsumeExchangeMetadata: Successfully finished consuming ExchangeMetadata", startTime, nil)
	span.SetStatus(codes.Ok, "OK")
	w.WriteHeader(200)
	w.Write([]byte("OK"))

}

// Retrieves a list of Exchanges from coingecko and runs
func ConsumeExchangesList(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "ConsumeExchangesList")
	defer span.End()
	labels := generateLabelFromContext(ctx)
	startTime := log.StartTimeL(labels, "ConsumeExchangesList")
	var maxRetries = 3
RETRY:
	cgRateLimiter.Wait(limiterContext)
	data, err := c.GetExchangesList(ctx)
	addToTotalCalls(ctx)
	if err != nil {
		log.EndTimeL(labels, "ConsumeExchangesList: Error retrieving Exchange List Data from CoinGecko API: %s", startTime, err)
		if maxRetries > 0 {
			log.DebugL(labels, "ConsumeExchangesList: Retrying Call for CoinGeckoExchangesList Attempt #%v ", maxRetries)
			time.Sleep(1 * time.Second) // sleep for a second before retrying. this should help prevent us from overloading CoinGecko with calls
			maxRetries--
			goto RETRY
		}
		log.DebugL(labels, "ConsumeExchangesList: Retrying Call for CoinGeckoExchangesList Attempt #%v ", maxRetries)
		w.WriteHeader(http.StatusInternalServerError)
	}
	log.DebugL(labels, "ConsumeExchangesList: Start upserting ExchangesList data to PostgreSql")
	store.UpsertCoinGeckoExchanges(ctx, data)
	saveCount(ctx)
	log.EndTimeL(labels, "ConsumeExchangesList: Successfully finished consuming ExchangeList", startTime, nil)
	span.SetStatus(codes.Ok, "OK")
	w.WriteHeader(200)
	w.Write([]byte("OK"))

}

// Retrieves a Exchanges Tickers based on ExchangesList and CoinIds
func ConsumeExchangesTickers(w http.ResponseWriter, r *http.Request) {
	var (
		mu           = &sync.Mutex{}
		wg           = sync.WaitGroup{}
		throttleChan = make(chan bool, 20)
	)

	ctx, span := tracer.Start(r.Context(), "ConsumeExchangesTickers")
	defer span.End()
	labels := generateLabelFromContext(ctx)

	startTime := log.StartTimeL(labels, "ConsumeExchangesTickers")

	// get exchange list from PG
	exchangesIDs, exchangesIDsErr := store.GetExchangesList(ctx)

	if exchangesIDsErr != nil {
		log.EndTimeL(labels, "ConsumeExchangesTickers: Error retrieving Exchanges IDs data for PostgreSQL: %s", startTime, exchangesIDsErr)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	log.Info("ConsumeExchangesTickers: Build Exchanges Tickers for Exchanges IDs Numbers: %d", len(exchangesIDs))

	for i := 0; i < len(exchangesIDs); i++ {
		throttleChan <- true
		wg.Add(1)
		var maxRetries = 3
	RETRY:
		exchangeOption := coingecko.ExchangesTickersOptions{
			CoinIds:             "",
			IncludeExchangeLogo: true,
			Page:                1,
			Depth:               "",
			Order:               "",
		}
		// we call the first time to get the response from coingecko with total number of tickers for specific exchange
		// we use options with page number 1 to get the first response from coingecko
		exchangesTickersData, exchangesTickersHeaders, exchangesTickersError := c.GetExchangesTickers(ctx, exchangesIDs[i], &exchangeOption)
		addToTotalCalls(ctx)
		if exchangesTickersError != nil {
			log.ErrorL(labels, "ConsumeExchangesTickers: Error retrieving Exchanges Tickers data from CoinGecko API: %s", exchangesTickersError)
			if strings.Contains(exchangesTickersError.Error(), "404") {
				<-throttleChan
				wg.Done()
				continue
			}
			if maxRetries > 0 && (exchangesTickersError != nil) {
				log.DebugL(labels, "ConsumeExchangesTickers: Retrying Call for GetExchangesTickers from CoinGecko Attempt #%v ", maxRetries)
				time.Sleep(1 * time.Second) // sleep for a 1 Minute before retrying. this should help prevent us from overloading CoinGecko with calls
				maxRetries--
				goto RETRY
			}
		}

		// build all tickers for exchange using pagination
		mu.Lock()
		tickers, err := GetTickersForExchanges(ctx, exchangesIDs[i], exchangesTickersData, GetTotalPagesFromTickersNumber(ctx, exchangesTickersHeaders))
		mu.Unlock()
		if err != nil {
			span.SetStatus(codes.Error, "ConsumeExchangesTickers: Error Getting Tickers For Exchanges")
			<-throttleChan
			wg.Done()
			return
		}

		if len(tickers) > 0 {
			respExchangesTickers := coingecko.ExchangesTickers{Name: exchangesIDs[i], Tickers: tickers}
			log.Info("ConsumeExchangesTickers: Starting Insert ExchangesTickers to BQ")
			span.AddEvent("ConsumeExchangesTickers: Starting Insert ExchangesTickers to BQ")

			go func(data coingecko.ExchangesTickers) {
				bq, err := store.NewBQStore()
				if err != nil {
					log.EndTimeL(labels, "ConsumeExchangesTickers: Error connecting to BigQuery Client:: %s", startTime, err)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				log.Info("ConsumeExchangesTickers: Starting Insert ExchangesTickers to BQ")
				InsertBQExchangesTickersHandler(ctx, bq, labels, data)
			}(respExchangesTickers)
			span.AddEvent("ConsumeExchangesTickers: End Insert ExchangesTickers to BQ")
			log.Info("ConsumeExchangesTickers: End Insert ExchangesTickers to BQ")
		}
		<-throttleChan
		wg.Done()

	}
	wg.Wait()
	log.DebugL(labels, "END ConsumeExchangesTickers TotalTime:%.2fm", time.Since(startTime).Minutes())
	saveCount(ctx)
	log.EndTimeL(labels, "ConsumeExchangesTickers: Successfully finished consuming ExchangesTickers", startTime, nil)
	span.SetStatus(codes.Ok, "OK")
	w.WriteHeader(200)
	w.Write([]byte("OK"))

}

// it will paginate for all tickers for specific exchange and it will append all tickers and returns it as array of tickers
func GetTickersForExchanges(ctxO context.Context, exchangeId string, exchangesTickersData *coingecko.ExchangesTickers, pagesNumber int) ([]coingecko.Ticker, error) {
	ctx, span := tracer.Start(ctxO, "GetTickersForExchanges")
	labels := generateLabelFromContext(ctx)
	defer span.End()
	var (
		maxRetries = 3
		err        error
		tickers    []coingecko.Ticker
	)
	span.AddEvent(fmt.Sprintf("GetTickersForExchanges: Append Tickers for Exchange %s", exchangeId))
	tickers = append(tickers, exchangesTickersData.Tickers...)
	for y := 2; y <= pagesNumber; y++ {
	RETRY:
		exchangeOption := coingecko.ExchangesTickersOptions{
			CoinIds:             "",
			IncludeExchangeLogo: true,
			Page:                y,
			Depth:               "",
			Order:               "",
		}

		cgRateLimiter.Wait(limiterContext)
		span.AddEvent(fmt.Sprintf("GetTickersForExchanges: Getting Exchanges Tickers for %s for page Number %d", exchangeId, y))
		exchangesTickersData, _, exchangesTickersError := c.GetExchangesTickers(ctx, exchangeId, &exchangeOption)
		addToTotalCalls(ctx)
		if exchangesTickersError != nil {
			log.ErrorL(labels, "GetTickersForExchanges: Error getting Exchanges Tickers data from CoinGecko API: %s", exchangesTickersError)
			span.SetStatus(codes.Error, "GetTickersForExchanges: Error retrieving Exchanges Tickers data from CoinGecko API")
			if maxRetries > 0 && (err != nil) {
				log.DebugL(labels, "GetTickersForExchanges: Retrying Call for GetExchangesTickers from CoinGecko Attempt #%v ", maxRetries)
				time.Sleep(1 * time.Minute) // sleep for a 1 Minute before retrying. this should help prevent us from overloading CoinGecko with calls
				maxRetries--
				goto RETRY
			}
			if maxRetries <= 0 {
				return tickers, exchangesTickersError
			}

		}
		tickers = append(tickers, exchangesTickersData.Tickers...)
	}
	span.SetStatus(codes.Ok, "GetTickersForExchanges: Exchanges Tickers Total Pages Completed")
	return tickers, err
}

func GetTotalPagesFromTickersNumber(ctxO context.Context, headers map[string][]string) int {

	_, span := tracer.Start(ctxO, "GetTotalPagesFromTickersNumber")
	defer span.End()
	span.AddEvent("GetTotalPagesFromTickersNumber: Getting Exchange Tickers Total Pages")
	for key, value := range headers {
		if key == "Total" {
			tickersNum, _ := strconv.Atoi(value[0])
			pageNum := tickersNum / 100.0
			res := math.Ceil(float64(pageNum))
			log.Debug("GetTotalPagesFromTickersNumber: getting Exchange Tickers Numbers: %d, with number of pages %v", tickersNum, res)
			span.SetStatus(codes.Ok, "GetTotalPagesFromTickersNumber: Get Total Pages From Exchange Tickers Number Completed")
			return int(res)
		}
	}
	span.SetStatus(codes.Ok, "GetTotalPagesFromTickersNumber: Get Total Pages From ExchangeTickers Number Completed")
	return 0
}

// insert exchanges tickers into BQ
func InsertBQExchangesTickersHandler(ctx0 context.Context, bq *store.BQStore, labels map[string]string, data coingecko.ExchangesTickers) {

	ctx, span := tracer.Start(ctx0, "InsertBQExchangesTickersHandler")
	defer span.End()

	var start = time.Now()
	span.AddEvent("InsertBQExchangesTickersHandler: Start convert ExchangesTickers To BQExchangesTickers")
	exchangesMapper := store.CGExchangesTickersToBQExchangesTickers(ctx, &data)

	log.DebugL(labels, "InsertBQExchangesTickersHandler: START Insert Exchanges Tickers Data to BQ")
	bq.InsertExchangesTickersData(ctx, exchangesMapper)
	log.DebugL(labels, "InsertBQExchangesTickersHandler: END Insert Exchanges Tickers Data to BQ, totalTime:%.2fs", time.Since(start).Seconds())

}

func generateSpan(ctx context.Context, funcName string) (map[string]string, trace.Span) {

	span := trace.SpanFromContext(ctx)

	labels := make(map[string]string)
	labels["function"] = funcName
	labels["UUID"] = uuid.New().String()
	span.SetAttributes(attribute.String("UUID", labels["UUID"]))
	labels["traceID"] = span.SpanContext().TraceID().String()
	labels["spanID"] = span.SpanContext().SpanID().String()

	return labels, span
}

// generateLabelFromContext creates the map[string]string for the labels from the context
func generateLabelFromContext(ctx context.Context) map[string]string {

	span := trace.SpanFromContext(ctx)

	labels := make(map[string]string)
	labels["UUID"] = uuid.New().String()
	span.SetAttributes(attribute.String("UUID", labels["UUID"]))
	labels["traceID"] = span.SpanContext().TraceID().String()
	labels["spanID"] = span.SpanContext().SpanID().String()

	return labels
}

/*
gets historical data for prices, volume, and market cap
*/
func getHistoricalDataForNewAssets(asset coingecko.Coins, ctx0 context.Context) *[]models.BQDAMarketData {

	ctx, span := tracer.Start(ctx0, "getHistoricalDataForNewAssets")
	defer span.End()
	labels := generateLabelFromContext(ctx)

	startTime := log.StartTimeL(labels, "getHistoricalDataForNewAssets")

	var maxRetries = 3

	var years = -5
	var days = 0
RETRY:

	var toDate = time.Now().Unix()
	var fromDate = time.Now().AddDate(years, 0, days).Unix()
	cgRateLimiter.Wait(limiterContext)
	data, err := c.GetCoinMarketChartRange(ctx, asset.ID, &coingecko.CoinMarketChartRangeOptions{VS_Currency: "usd", From: fromDate, To: toDate})
	addToTotalCalls(ctx)

	if err != nil {

		if maxRetries > 0 {
			maxRetries--
			time.Sleep(1 * time.Second)
			goto RETRY
		}
		log.EndTimeL(labels, "getHistoricalDataForNewAssets: Error retrieving CoinMarketChartRange from CoinGecko API: %s", startTime, err)

	}
	/*
		If we dont have an error but the api did not return history for 5 years
		start to shrink tha amount of history we need
	*/
	if data == nil || len(data.Prices) <= 0 {
		log.DebugL(labels, "getHistoricalDataForNewAssets: No History found for %s,to:%d,from: %d retrying with new date", asset.ID, toDate, fromDate)
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
	var bqData *[]models.BQDAMarketData
	if data != nil && len(data.Prices) > 0 {

		if err != nil {
			log.EndTimeL(labels, "getHistoricalDataForNewAssets: Error storing history: %s", startTime, err)
		}
		bqData = store.CGHistoryToBQMarketData(ctx, asset, *data)
		return bqData
	}
	//fmt.Println(len(data.MarketCaps), len(data.Prices), len(data.TotalVolumes))

	log.EndTimeL(labels, "getHistoricalDataForNewAssets: Successfully finished Getting HistoricalDataForNewAssets", startTime, nil)
	return nil
}

func GetLimitCount() int {
	return coingeckoCalls
}

// adds to the rate limit count
func addToTotalCalls(ctx0 context.Context) {

	_, span := tracer.Start(ctx0, "addToTotalCalls")
	defer span.End()

	lock.Lock()
	test.Count++
	lock.Unlock()

	if float64(test.Count/cgmonthlyLimit) > float64(1) && !usageAt100 {
		usageAt100 = true
		log.Alert("CoinGecko Usage is at or past 100 percent")
	} else if float64(test.Count/cgmonthlyLimit) > float64(0.90) && !usageAt90 {
		usageAt90 = true
		log.Alert("CoinGecko Usage is at or past 90 percent")
	} else if float64(test.Count/cgmonthlyLimit) >= float64(0.75) && !usageAt75 {
		usageAt75 = true
		log.Alert("CoinGecko Usage is at or past 75 percent")
	} else if float64(test.Count/cgmonthlyLimit) >= float64(0.5) && !usageAtHalf {
		usageAtHalf = true
		log.Alert("CoinGecko Usage is at or past 50 percent")
	}

}

// updates the coingecko count to postgres
func saveCount(ctx0 context.Context) {

	ctx, span := tracer.Start(ctx0, "saveCount")
	defer span.End()

	lock.Lock()
	store.SaveCGRate(ctx, test)
	lock.Unlock()
}

// Retrieves top 100 tickers for x Exchanges by trust score where
func ConsumeTopNumExchanges(w http.ResponseWriter, r *http.Request) {
	var (
		wg           = sync.WaitGroup{}
		throttleChan = make(chan bool, 20)
	)

	ctx, span := tracer.Start(r.Context(), "ConsumeTopNumExchanges")
	defer span.End()
	labels := generateLabelFromContext(ctx)

	startTime := log.StartTimeL(labels, "ConsumeTopNumExchanges")

	// get exchange list from PG
	exchangesIDs, exchangesIDsErr := store.GetxExchangeIDsByTrust(ctx)

	if exchangesIDsErr != nil {
		log.EndTimeL(labels, "ConsumeTopNumExchanges: Error retrieving ExchangeIDsByTrust from PostgreSQL: %s", startTime, exchangesIDsErr)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	bq, err := store.NewBQStore()
	if err != nil {
		log.EndTimeL(labels, "ConsumeTopNumExchanges: Error connecting to BigQuery Client: %s", startTime, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	span.AddEvent("ConsumeTopNumExchanges: Starting Getting Exchanges Tickers By exchangesIDs from Coingecko")
	//var tickers []coingecko.Ticker
	for i := 0; i < len(exchangesIDs); i++ {

		throttleChan <- true
		wg.Add(1)

		go func(exchangeId string) {
			maxretries := 3
		RETRY:
			exchangesTickersData, exchangesTickersError := c.GetExchangesId(ctx, exchangeId)
			if exchangesTickersError != nil {
				log.EndTimeL(labels, "ConsumeTopNumExchanges: Error getting ExchangesId from CoinGecko API: %s", startTime, err)
				span.SetStatus(codes.Error, "ConsumeTopNumExchanges: Error getting ExchangesId from CoinGecko API")
				if maxretries > 0 {
					maxretries--
					goto RETRY
				}
				<-throttleChan
				wg.Done()
				return
			}

			respExchangesTickers := coingecko.ExchangesTickers{Name: exchangeId, Tickers: exchangesTickersData.Tickers}
			span.AddEvent("ConsumeTopNumExchanges: Starting Insert ExchangesTickers to BQ")
			InsertBQExchangesTickersHandler(ctx, bq, labels, respExchangesTickers)

			<-throttleChan
			wg.Done()

			span.AddEvent("ConsumeTopNumExchanges: End Insert ExchangesTickers to BQ")

		}(exchangesIDs[i])

	}
	wg.Wait()
	log.DebugL(labels, "END ConsumeTopNumExchanges TotalTime:%.2fm", time.Since(startTime).Minutes())
	saveCount(ctx)
	log.EndTimeL(labels, "ConsumeTopNumExchanges: Successfully finished consuming TopNumExchanges from Coingecko API", startTime, nil)
	span.SetStatus(codes.Ok, "OK")
	w.WriteHeader(200)
	w.Write([]byte("OK"))

}

// Retrieves the Global data from Coingecko
func ConsumeGlobalData(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "ConsumeGlobalData")
	defer span.End()
	labels := generateLabelFromContext(ctx)
	startTime := log.StartTimeL(labels, "ConsumeGlobalData")
	globalDescription, err := BuildDynamicDescription(ctx, labels)

	if err != nil {
		log.ErrorL(labels, "ConsumeGlobalData: Error Building Global Description  at time %s, error %v", startTime, err)
		w.WriteHeader(http.StatusInternalServerError)
		span.SetStatus(codes.Error, err.Error())
		return
	}
	span.AddEvent("ConsumeGlobalData: Start Insert Global Data to PG")
	store.InsertGlobalDescription(ctx, labels, globalDescription)
	log.DebugL(labels, "END ConsumeGlobalData TotalTime:%.2fm", time.Since(startTime).Minutes())
	saveCount(ctx)
	log.EndTimeL(labels, "ConsumeGlobalData: Successfully finished consuming GlobalData", startTime, nil)
	span.SetStatus(codes.Ok, "OK")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// Build the Global Description from Coingecko and Postgresql
// we need the Trending data from Postgresql
func BuildDynamicDescription(ctx0 context.Context, labels map[string]string) (*models.Global, error) {
	ctx, span := tracer.Start(ctx0, "BuildDynamicDescription")
	defer span.End()
	var maxRetries = 3
RETRY:
	cgRateLimiter.Wait(limiterContext)
	data, err := c.GetGlobal(ctx)
	addToTotalCalls(ctx)
	if err != nil {
		log.DebugL(labels, "BuildDynamicDescription: Error getting Global Data from CoinGecko API: %s", err)
		span.SetStatus(codes.Error, err.Error())
		if maxRetries > 0 {
			maxRetries--
			time.Sleep(1 * time.Second)
			goto RETRY
		}
	}

	dynamicDescription, err := store.GetDynamicDescription(ctx, labels)
	if err != nil {
		log.DebugL(labels, "BuildDynamicDescription: Get Dynamic Description Data returns Empty from PG, error %v", err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	dynamicDominanceData, err := store.GetDynamicDescriptionDominanceData(ctx, labels)
	if err != nil {
		log.DebugL(labels, "BuildDynamicDescription: Get Dynamic Description Dominance Data returns Empty from PG, error %v", err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	span.AddEvent("BuildDynamicDescription: Map Global Description from Coingecko Global Data and PG")

	dynamicDescription.MarketCap = data.Data.TotalMarketCap["usd"]
	dynamicDescription.Change24H = data.Data.MarketCapChangePercentage24HUsd
	dynamicDescription.Volume24H = data.Data.TotalVolume["usd"]
	dynamicDescription.Dominance.DominanceOne.MarketCapDominance = data.Data.MarketCapPercentage["btc"]
	dynamicDescription.Dominance.DominanceOne.Name = dynamicDominanceData["btc"].Name
	dynamicDescription.Dominance.DominanceOne.Slug = dynamicDominanceData["btc"].Slug
	dynamicDescription.Dominance.DominanceTwo.MarketCapDominance = data.Data.MarketCapPercentage["eth"]
	dynamicDescription.Dominance.DominanceTwo.Name = dynamicDominanceData["eth"].Name
	dynamicDescription.Dominance.DominanceTwo.Slug = dynamicDominanceData["eth"].Slug
	dynamicDescription.AssetCount = data.Data.ActiveCryptocurrencies
	dynamicDescription.LastUpdated = time.Now()
	dynamicDescription.Type = "FT"
	log.DebugL(labels, "BuildDynamicDescription: Successfully finished Building Dynamic Description Data Finished")

	span.SetStatus(codes.Ok, "success")
	return dynamicDescription, nil

}

// Retrieves Categories data from coingecko with All related data for it
// And Retrieves all markets for each Category
func ConsumeCategories(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "ConsumeCategories")
	defer span.End()

	var (
		wg           = sync.WaitGroup{}
		throttleChan = make(chan bool, 20)
		mu           = &sync.Mutex{}
		categories   []models.CategoriesData
		assetsMap    = make(map[string][]string)
	)

	labels := generateLabelFromContext(ctx)

	startTime := log.StartTimeL(labels, "ConsumeCategories")
	var maxRetries = 3
RETRY:
	cgRateLimiter.Wait(limiterContext)
	categoriesData, err := c.GetCategoriesData(ctx)
	if err != nil {
		log.EndTimeL(labels, "ConsumeCategories: Error getting Categories Data from CoinGecko API: %s", startTime, err)
		if maxRetries > 0 {
			maxRetries--
			goto RETRY
		}
		w.WriteHeader(http.StatusInternalServerError)
	}
	// get all market for each category
	for i := 0; i < len(categoriesData); i++ {
		categoryData := categoriesData[i]
		var category models.CategoriesData
		throttleChan <- true
		wg.Add(1)
		go func(categoryId string) {
		RETRY:
			cgRateLimiter.Wait(limiterContext)
			span.AddEvent("ConsumeCategories: Get Assets Data for Category")
			data, err := c.GetCoinsMarketData(ctx, &coingecko.CoinsMarketOptions{VSCurrency: "usd", Category: categoryId, Page: 1, Per_Page: 250})
			addToTotalCalls(ctx)
			if err != nil {
				log.EndTimeL(labels, "ConsumeCategories: Error getting Assets For category from CoinGecko API: %s", startTime, err)
				if maxRetries > 0 {
					maxRetries--
					goto RETRY
				}
				log.DebugL(labels, "ConsumeCategories: Retrying call for assets with category ID %s . Attempt #%v ", categoryId, maxRetries)
				<-throttleChan
				wg.Done()
				return
			}
			mu.Lock()
			category.ID = categoryData.ID
			category.Name = categoryData.Name
			category.MarketCap = categoryData.MarketCap
			category.MarketCapChange24H = categoryData.MarketCapChange24H
			category.Content = categoryData.Content
			category.Top3Coins = categoryData.Top3Coins
			category.Volume24H = categoryData.Volume24H
			category.UpdatedAt = categoryData.UpdatedAt
			category.Markets = *data
			category.Inactive = false
			if len(categoryData.Top3Coins) < 1 { //It means this is a deprecated category from upstream (coingecko).
				category.Inactive = true
			}
			span.AddEvent("ConsumeCategories: Build Assets Tags")
			assetsMap, err = BuildAssetsTags(ctx, assetsMap, category.Markets, categoryId)
			if err != nil {
				log.ErrorL(labels, "ConsumeCategories: Can't Build Assets Tags at time %s, error %v", startTime, err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			categories = append(categories, category)
			mu.Unlock()
			<-throttleChan
			wg.Done()

		}(categoryData.ID)
	}
	wg.Wait()
	log.DebugL(labels, "ConsumeCategories: Start Upsert Categories Data")
	span.AddEvent("ConsumeCategories: Start Upsert Categories Data")
	// Upsert Categories with all it's data
	store.UpsertCoinGeckoCategoriesData(ctx, categories)

	// Update the tags in  coingecko_asset_metadata
	log.DebugL(labels, "ConsumeCategories: Start Update Tags Assets Metadata")
	span.AddEvent("ConsumeCategories: Start Update Tags Assets Metadata")
	store.UpdateAssetsMetaData(ctx, assetsMap)

	// Insert The Categories to Featured Categories in FS
	log.DebugL(labels, "ConsumeCategories: Start Inserting categories To Firestore")
	store.FSInsertCategories(ctx, categories)

	saveCount(ctx)
	log.EndTimeL(labels, "ConsumeCategories: Successfully finished consuming Categories", startTime, nil)
	span.SetStatus(codes.Ok, "OK")
	w.WriteHeader(200)
	w.Write([]byte("OK"))
}

// Build Assets Tags Array to connect each asset with related Categories
func BuildAssetsTags(ctx0 context.Context, assetsMap map[string][]string, markets []coingecko.CoinsMarketData, categoryId string) (map[string][]string, error) {
	_, span := tracer.Start(ctx0, "BuildAssetsTags")
	defer span.End()
	for _, asset := range markets {
		assetsMap[asset.ID] = append(assetsMap[asset.ID], categoryId)
	}
	return assetsMap, nil
}

/*
- Retrieves a list of Exchanges from coingecko and runs until no resposes are left.
- Upserts data to postgres
- Inserts current price info into postgres
- Gets market history if we are pulling for the first time
- Inserts current market data to bigquery for historical chart
*/
func ConsumeNFTsList(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "ConsumeNFTsList")
	defer span.End()
	labels := generateLabelFromContext(ctx)
	startTime := log.StartTimeL(labels, "ConsumeNFTsList")
	var (
		makeRequest = true
		pageNum     = 1
		cgNFTList   coingecko.NFTMarketsList
	)
	nftIDs, err := store.GetIDNFTList(ctx)
	nftSlugList, err := store.GetNFTSlugs(ctx)
	if err != nil {
		log.EndTimeL(labels, "ConsumeNFTsList: Error getting NFT ID Data PostgreSQL: %s", startTime, err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
	}
	//we will make requests until the response is empty. This is due to a lack of recieving a TOTAL header
	for makeRequest {

		cgRateLimiter.Wait(limiterContext)
		data, _, err := c.GetNFTMarketsList(ctx, &coingecko.NFTCollectionListOptions{Per_Page: 100, Page: pageNum, ApiKey: os.Getenv("COINGECKO_API_KEY")})
		addToTotalCalls(ctx)
		if err != nil {
			log.EndTimeL(labels, "ConsumeNFTsList: Error getting NFTsList from CoinGecko API: %s", startTime, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		cgNFTList = append(cgNFTList, *data...)
		if len(*data) <= 0 {
			makeRequest = false
		}
		pageNum++
	}
	// Get the market chart data if it is the first time we recieve the colletion in our list
	log.DebugL(labels, "ConsumeNFTsList: Start consume NFT Market Charts")
	consumeNFTMarketCharts(ctx, nftIDs, &cgNFTList)

	// Inserts the large images for each NFTs.
	log.DebugL(labels, "ConsumeNFTsList: Start Inserting large images for each NFTs")
	insertLargeImagesNFT(ctx, &cgNFTList)

	//Store the new NFT Trade data to the historical table
	log.DebugL(labels, "ConsumeNFTsList: Start Converting NFTMarket to BQNFTMarket")
	tickers := store.CGNFTMarketDataBQNFTMarketHistory(ctx, &cgNFTList)
	bq, err := store.NewBQStore()
	if err != nil {
		log.EndTimeL(labels, "ConsumeNFTsList: Error connecting to BigQuery Client: %s", startTime, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	log.DebugL(labels, "generateUniqueSlugMap: Started generating unique slugs for each nfts")
	slugList, updateNFTSlugs := generateUniqueSlugMap(ctx, &cgNFTList, &nftSlugList)

	log.DebugL(labels, "ConsumeNFTsList: Start Inserting NFTMarket to BigQuery")
	bq.InsertNFTData(ctx, tickers)

	//Store the data to postgres
	log.DebugL(labels, "ConsumeNFTsList: Start Inserting NFTMarket to PostgreSQL")
	store.UpsertNFTData(ctx, &cgNFTList, slugList)

	if len(updateNFTSlugs) > 0 {
		// Only when the inactive nfts have duplicate slugs, the len(updateNFTSlugs) will be greater than 0 and their updated slugs will get upserted in postgres `nftdatalatest` table
		log.DebugL(labels, "UpsertNFTSlugs: Start Upserting NFT slugs to PostgreSQL")
		store.UpsertNFTSlugs(ctx, &updateNFTSlugs)
	}

	saveCount(ctx)
	log.EndTimeL(labels, "ConsumeNFTsList: Successfully finished consuming NFTsList from Coingecko API", startTime, nil)
	span.SetStatus(codes.Ok, "OK")
	w.WriteHeader(200)
	w.Write([]byte("OK"))

}

/*
Generates unique slugs
Returns a slugMap that is map[id]=>slug, for all the elements that are present in the nftList.
*/
func generateUniqueSlugMap(ctx0 context.Context, nftList *coingecko.NFTMarketsList, nftSlugList *[]models.NftSlugData) (map[string]string, []models.NftSlugData) {
	ctx, span := tracer.Start(ctx0, "generateUniqueSlugMap")
	defer span.End()
	labels := generateLabelFromContext(ctx)
	startTime := log.StartTimeL(labels, "generateUniqueSlugMap")

	slugToNFTMap := map[string]models.NftSlugData{}
	idToSlugMap := map[string]string{}
	updateNFTSlugs := []models.NftSlugData{}
	for _, nft := range *nftSlugList {
		if nft.Slug != "" && slugToNFTMap[nft.Slug].Slug == "" {
			slugToNFTMap[nft.Slug] = nft
			idToSlugMap[nft.ID] = nft.Slug
		}
	}

	for _, nft := range *nftList {

		if nft.ID == "" || idToSlugMap[nft.ID] != "" {
			continue
		}

		newSlug := store.FormatSlug(nft.Name, nft.Symbol)
		idToSlugMap[nft.ID] = newSlug

		// If some other asset has claimed this slug, then look for a slug with a suffix like this: slug-(number)
		if slugToNFTMap[newSlug].ID != "" && slugToNFTMap[newSlug].ID != nft.ID {
			slugSuffix := 1
			for {
				foundValue := slugToNFTMap[newSlug+"-"+strconv.Itoa(slugSuffix)].ID
				if foundValue == "" || foundValue == nft.ID {
					break
				}
				slugSuffix++
			}
			newSlug = newSlug + "-" + strconv.Itoa(slugSuffix)

			nftSlugObj := models.NftSlugData{
				ID:     nft.ID,
				Slug:   newSlug,
				Name:   nft.Name,
				Symbol: nft.Symbol,
			}
			(*nftSlugList) = append((*nftSlugList), nftSlugObj)
			slugToNFTMap[newSlug] = nftSlugObj
			idToSlugMap[nft.ID] = newSlug
		}
	}

	// Some inactive assets that are still in db, but their slugs won't get updated because the NFTMarketList doesn't contain it.
	// So this loop helps keep them in line.
	for idx, nft := range *nftSlugList {
		if nft.ID == "" || idToSlugMap[nft.ID] != "" {
			continue
		}
		if nft.Slug == "" {
			nft.Slug = store.FormatSlug(nft.Name, nft.Symbol)
			if nft.Slug == "" {
				nft.Slug = nft.ID
			}
		}
		idToSlugMap[nft.ID] = nft.Slug

		// If some other asset has claimed this slug, then look for a slug with a suffix like this: slug-(number)
		if slugToNFTMap[nft.Slug].ID != "" && slugToNFTMap[nft.Slug].ID != nft.ID {
			slugSuffix := 1
			for {
				foundValue := slugToNFTMap[nft.Slug+"-"+strconv.Itoa(slugSuffix)].ID
				if foundValue == "" || foundValue == nft.ID {
					break
				}
				slugSuffix++
			}
			nft.Slug = nft.Slug + "-" + strconv.Itoa(slugSuffix)

			nftSlugObj := models.NftSlugData{
				ID:     nft.ID,
				Slug:   nft.Slug,
				Name:   nft.Name,
				Symbol: nft.Symbol,
			}
			(*nftSlugList) = append((*nftSlugList), nftSlugObj)
			slugToNFTMap[nft.Slug] = nftSlugObj
			idToSlugMap[nft.ID] = nft.Slug
		}

		if nft.Slug != (*nftSlugList)[idx].Slug {
			updateNFTSlugs = append(updateNFTSlugs, nft)
			(*nftSlugList)[idx].Slug = nft.Slug
		}
	}

	log.EndTimeL(labels, "generateUniqueSlugMap: Successfully finished generating unique slugs for NFTs", startTime, nil)
	span.SetStatus(codes.Ok, "OK")
	return idToSlugMap, updateNFTSlugs

}

/*
Inserts Large Image data for each NFT collection.

Coingecko only provides small images for each NFT. We replace /small/ in their image link with /large/, then verify by making an API call and if it exists, then we set it into the Large field. If it does not work, then we assign a default large image link.
*/
func insertLargeImagesNFT(ctx0 context.Context, nftList *coingecko.NFTMarketsList) {
	ctx, span := tracer.Start(ctx0, "insertLargeImagesNFT")
	defer span.End()
	labels := generateLabelFromContext(ctx)
	startTime := log.StartTimeL(labels, "insertLargeImagesNFT")
	defaultImage := "https://i.forbesimg.com/media/lists/people/no-pic_416x416.jpg"

	var (
		wg           sync.WaitGroup
		throttleChan = make(chan bool, 15) // Max 5 concurrent requests.
	)

	for idx := range *nftList {

		throttleChan <- true
		wg.Add(1)
		go func(ctxO context.Context, nftList *coingecko.NFTMarketsList, idx int) {
			small_image := (*nftList)[idx].Image.Small
			_, span := tracer.Start(ctxO, "Go Routine inside insertLargeImagesNFT")
			defer span.End()

			large_url := defaultImage

			if small_image == "missing_small.png" {
				// Coingecko provides image url as "missing_small.png", that means we'll use our default image.
				large_url = defaultImage
				(*nftList)[idx].Image.Small = defaultImage

			} else if small_image != "" {
				// When the small image URL exist, we replace the `small` with `large` and see if it exists by making a network call.
				// We store the result inside `verifiedImages` map as a cache to avoid future network calls.
				large_url = strings.ReplaceAll(small_image, "/small/", "/large/")
				_, status := verifiedImages.Load(large_url) //Checking local cache if the url is valid.
				if !status {                                //When a local cache does not exist, we make a network call.
					httpStatus := utils.CheckURLStatus(large_url)
					if httpStatus == http.StatusOK {
						verifiedImages.Store(large_url, true)
					} else {
						large_url = defaultImage
					}
				}
			}
			(*nftList)[idx].Image.Large = large_url

			<-throttleChan // Remove from the throttle channel - allowing another go routine to start
			wg.Done()
		}(ctx, nftList, idx)

	}

	wg.Wait()

	log.EndTimeL(labels, "insertLargeImagesNFT: Successfully finished inserting large images for NFTs", startTime, nil)
	span.SetStatus(codes.Ok, "OK")
	return
}

/*
Gets Marketchart data from	coingecko

	-nftIDS is a list of the ids that FDA is currently aware of. If there is a new id in the nftList we will get its history
*/
func consumeNFTMarketCharts(ctx0 context.Context, nftIDs []string, nftList *coingecko.NFTMarketsList) {
	ctx, span := tracer.Start(ctx0, "consumeNFTMarketCharts")
	defer span.End()
	labels := generateLabelFromContext(ctx)
	startTime := log.StartTimeL(labels, "consumeNFTMarketCharts")

	for _, nft := range *nftList {
		var (
			maxRetries = 3
		)

		//if we have the asset in our db continue, otherwise get the history
		// Or Coingecko didnt provide an id continue
		if slices.Contains(nftIDs, nft.ID) || nft.ID == "" {
			continue
		}
	RETRY:
		cgRateLimiter.Wait(limiterContext)
		data, _, err := c.GetNFTMarketChart(ctx, &coingecko.NFTMarketChartOptions{Days: "max", ApiKey: os.Getenv("COINGECKO_API_KEY")}, nft.ID)
		//addToTotalCalls(ctx)
		if err != nil {
			log.EndTimeL(labels, "consumeNFTMarketCharts: Error getting NFTMarketCharts from CoinGecko API: %s", startTime, err)
			span.SetStatus(codes.Error, "consumeNFTMarketCharts: Error getting NFTMarketCharts from CoinGecko API")
			if maxRetries > 0 {
				log.DebugL(labels, "consumeNFTMarketCharts: Retrying Call for NFTMarketCharts Attempt #%v ", maxRetries)
				time.Sleep(1 * time.Minute) // sleep for a second before retrying. this should help prevent us from overloading CoinGecko with calls
				maxRetries--
				goto RETRY
			} else {
				continue
			}

		}
		hist := store.CGNFTHistoryToBQNFTMarketHistory(ctx, *data, nft.ID)
		bq, err := store.NewBQStore()
		if err != nil {
			log.EndTimeL(labels, "consumeNFTMarketCharts: Error connecting to BigQuery Client: %s", startTime, err)
			return
		}
		bq.InsertNFTData(ctx, hist)

	}

	//store.UpsertCoinGeckoExchanges(ctx, data)

	log.EndTimeL(labels, "consumeNFTMarketCharts: Successfully finished consuming NFTMarketCharts from Coingecko API", startTime, nil)
	span.SetStatus(codes.Ok, "OK")
	return

}

/*
Calls Coingecko to retrive the metadata for NFT Collections.
It Retrieves the discord twitter and website links of the project
This will be called once per day.
*/
func ConsumeNFTMetaData(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "ConsumeNFTMetaData")
	defer span.End()
	labels := generateLabelFromContext(ctx)
	startTime := log.StartTimeL(labels, "ConsumeNFTMetaData")
	var (
		cgNFTList    []coingecko.NFTMarket
		wg           = sync.WaitGroup{}
		throttleChan = make(chan bool, 200)
		mu           = sync.Mutex{}
	)
	nftIDs, err := store.GetIDNFTList(ctx) //Get a list of the NFT collections that FDA supports
	if err != nil {
		log.EndTimeL(labels, "ConsumeNFTMetaData: Error getting NFTs ID Data from PostgreSQL: %s", startTime, err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
	}
	//makes a call for each NFT ID and retrieves the data for the collection
	for _, nft := range nftIDs {

		throttleChan <- true
		wg.Add(1)
		go func(id string) {
			var maxRetries = 3
		RETRY:
			cgRateLimiter.Wait(limiterContext)
			//calls nft market endpoint for the nft id
			data, _, err := c.GetNFTMarket(ctx, id)
			addToTotalCalls(ctx)
			if err != nil {
				log.EndTimeL(labels, "ConsumeNFTMetaData: Error getting NFTMarket from CoinGecko API: %s", startTime, err)
				span.SetStatus(codes.Error, "ConsumeNFTMetaData: Error getting NFTMarket from CoinGecko API")
				if maxRetries > 0 {
					log.DebugL(labels, "ConsumeNFTMetaData: Retrying Call for NFTMarket Attempt #%v ", maxRetries)
					time.Sleep(1 * time.Second) // sleep for a second before retrying. this should help prevent us from overloading CoinGecko with calls
					maxRetries--
					goto RETRY
				}
				<-throttleChan
				wg.Done()
			}
			//appends the data to the nft list
			if data != nil {
				mu.Lock()
				cgNFTList = append(cgNFTList, *data)
				mu.Unlock()
			}
			<-throttleChan
			wg.Done()
		}(nft)
	}
	wg.Wait()

	//passes off of the data to upserNFTMetadata for storage
	log.DebugL(labels, "ConsumeNFTMetaData: Start Upserting NFTMetaData in PostgreSQL")
	store.UpsertNFTMetaData(ctx, &cgNFTList)

	saveCount(ctx)
	log.EndTimeL(labels, "ConsumeNFTMetaData: Successfully finished consuming NFTMetaData from Coingecko API", startTime, nil)
	span.SetStatus(codes.Ok, "OK")
	w.WriteHeader(200)
	w.Write([]byte("OK"))

}

// Consume the NFT Global data from Coingecko
func ConsumeNFTGlobalData(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "ConsumeNFTGlobalData")
	defer span.End()
	labels := generateLabelFromContext(ctx)
	startTime := log.StartTimeL(labels, "ConsumeNFTGlobalData")
	// Build NFT dynamic description from PG and BQ
	globalDescription, err := BuildNFTDynamicDescription(ctx, labels)

	if err != nil {
		log.ErrorL(labels, "ConsumeNFTGlobalData: Error Building NFTDynamicDescription  at time %s, error %v", startTime, err)
		w.WriteHeader(http.StatusInternalServerError)
		span.SetStatus(codes.Error, err.Error())
		return
	}
	span.AddEvent("ConsumeNFTGlobalData: Start Insert NFT Dynamic Description Data to PG")
	store.InsertGlobalDescription(ctx, labels, globalDescription)
	log.DebugL(labels, "END ConsumeNFTGlobalData TotalTime:%.2fm", time.Since(startTime).Minutes())
	saveCount(ctx)
	log.EndTimeL(labels, "ConsumeNFTGlobalData: Successfully finished consuming NFTGlobalData", startTime, nil)
	span.SetStatus(codes.Ok, "OK")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// Build the Global Description from Coingecko and Postgresql
// we need the Trending and Dominance data from Postgresql
// we will build the Market cap, volume and change 24h from BQ.
func BuildNFTDynamicDescription(ctx0 context.Context, labels map[string]string) (*models.Global, error) {
	ctx, span := tracer.Start(ctx0, "BuildNFTDynamicDescription")
	defer span.End()
	startTime := log.StartTimeL(labels, "BuildNFTDynamicDescription")

	bq, err := store.NewBQStore()
	if err != nil {
		log.EndTimeL(labels, "BuildNFTDynamicDescription: Error connecting to BigQuery Client: %s", startTime, err)
		return nil, err
	}
	// Get Market Cap, Volume 24h and Change 24h from bigQuery
	nftDynamicDescription, err := bq.GetNFTMarketCapVolumeChange24H(ctx)
	if err != nil {
		log.DebugL(labels, "BuildNFTDynamicDescription: Error Getting NFT Market Cap Volume Change 24H from BQ, error %v", err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	// Get Trending Data from PG
	dynamicDescription, err := store.GetNFTTrending(ctx, labels)
	if err != nil {
		log.DebugL(labels, "BuildNFTDynamicDescription: Error Getting NFT Dynamic Description Trending Data from PG, error %v", err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	// Get Dominance Data from PG
	dynamicDominanceData, err := store.GetNFTDynamicDescriptionDominanceData(ctx, labels)
	if err != nil {
		log.DebugL(labels, "BuildNFTDynamicDescription: Getting NFT Dynamic Description Dominance Data from PG, error %v", err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	span.AddEvent("BuildNFTDynamicDescription: Map NFT Global Description from Coingecko Global Data and PG")

	dynamicDescription.MarketCap = nftDynamicDescription.MarketCap
	dynamicDescription.Change24H = nftDynamicDescription.Change24H
	dynamicDescription.Volume24H = nftDynamicDescription.Volume24H
	dynamicDescription.Dominance.DominanceOne.MarketCapDominance = dynamicDominanceData["bayc"].MarketCapDominance
	dynamicDescription.Dominance.DominanceOne.Name = dynamicDominanceData["bayc"].Name
	dynamicDescription.Dominance.DominanceOne.Slug = dynamicDominanceData["bayc"].Slug
	dynamicDescription.Dominance.DominanceTwo.MarketCapDominance = dynamicDominanceData["punk"].MarketCapDominance
	dynamicDescription.Dominance.DominanceTwo.Name = dynamicDominanceData["punk"].Name
	dynamicDescription.Dominance.DominanceTwo.Slug = dynamicDominanceData["punk"].Slug
	dynamicDescription.AssetCount = dynamicDominanceData["bayc"].Count
	dynamicDescription.LastUpdated = time.Now()
	dynamicDescription.Type = "NFT"
	log.EndTimeL(labels, "BuildNFTDynamicDescription: Successfully finished building NFT Dynamic Description", startTime, nil)

	span.SetStatus(codes.Ok, "success")
	return dynamicDescription, nil

}

/*
- Retrieves the Tickers from coingecko for each NFTs.
- Upserts data to postgres
- Inserts tickers info into postgres
*/
func ConsumeNFTsTickers(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "ConsumeNFTsTickers")
	defer span.End()
	labels := generateLabelFromContext(ctx)
	startTime := log.StartTimeL(labels, "ConsumeNFTsTickers")
	var (
		cgNFTList    []coingecko.NFTTickers
		throttleChan = make(chan bool, 20)
		wg           sync.WaitGroup
	)
	nftIDs, err := store.GetIDNFTList(ctx)
	if err != nil {
		log.EndTimeL(labels, "ConsumeNFTsTickers: Error getting NFT ID Data PostgreSQL: %s", startTime, err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
	}

	// We add this context because the context from Cloud Run will cancelled after 5 min 
	// And this process takes from 15 to 18 minute 
	opCtx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()

	//we will make requests until the response is empty. This is due to a lack of receiving a TOTAL header
	for _, id := range nftIDs {
		throttleChan <- true
		wg.Add(1)
		go func(id string) {
			var maxRetries = 3
		RETRY:

			cgRateLimiter.Wait(limiterContext)
			data, _, err := c.GetNFTTickers(opCtx, id)
			addToTotalCalls(opCtx)
			if err != nil {
				// If the NFT not exist we don't need to RETRY the call in this way we can save some calls to Coingecko
				if strings.Contains(err.Error(), "404 Not Found") {
					log.EndTimeL(labels, "ConsumeNFTsTickers: Error getting NFTsTIckers from CoinGecko API: %s", startTime, err)
				} else if opCtx.Err() != nil {
					log.EndTimeL(labels, "ConsumeNFTsTickers: NFTsTIckers Context Error => : %s", startTime, opCtx.Err())
				} else {
					log.EndTimeL(labels, "ConsumeNFTsTickers: Error getting NFTsTIckers from CoinGecko API: %s", startTime, err)
					if maxRetries > 0 {
						maxRetries--
						goto RETRY
					}
				}
			}
			if data != nil {
				data.ID = id
				cgNFTList = append(cgNFTList, *data)
			}
			<-throttleChan
			wg.Done()

		}(id)
	}
	wg.Wait()
	//Store the data to postgres
	log.DebugL(labels, "ConsumeNFTsTickers: Start Inserting NFTsTIckers to PostgreSQL")
	store.UpsertNFTTickersData(ctx, &cgNFTList)

	saveCount(ctx)
	log.EndTimeL(labels, "ConsumeNFTsTickers: Successfully finished consuming NFTsTIckers from Coingecko API", startTime, nil)
	span.SetStatus(codes.Ok, "OK")
	w.WriteHeader(200)
	w.Write([]byte("OK"))

}
