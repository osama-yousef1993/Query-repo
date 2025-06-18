package internal

import (
	"context"
	"errors"
	"fmt"
	"html"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/Forbes-Media/fda-arkham-client/arkham"
	"github.com/Forbes-Media/fda-arkham-ingestion/models"
	"github.com/Forbes-Media/fda-arkham-ingestion/store.go"
	"github.com/Forbes-Media/go-tools/log"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/time/rate"
)

var tracer = otel.Tracer("github.com/Forbes-Media/fda-arkham-ingestion/internal")

var c = arkham.NewClient(os.Getenv("ARKHAM_API_KEY"), os.Getenv("ARKHAM_URL"))

var rateLimiter = rate.NewLimiter(rate.Every(time.Second/time.Duration(5)), 1) //every 5 seconds with a burst of 5
// Calls arkham for transferEvents
func RunConsumeArkhamTransferEvents(w http.ResponseWriter, r *http.Request) {

	ctx, span := tracer.Start(context.Background(), "ConsumeArkhamTransferEvents")
	defer span.End()
	labels := generateLabelFromContext(ctx)

	startTime := log.StartTimeL(labels, "ConsumeArkhamTransferEvents")
	config, _ := store.GetArkhamTransferSettings(ctx)
	reqTimes, _ := store.GetRequestTimestamps(ctx)

	var (
		bqData         []models.BQArkhamTransferRecord //Bigqurey object that rpresents arkham transfer data
		transfers      []arkham.Transfers              // transfer data recived from arkham
		lock           = &sync.Mutex{}                 //lock used when altering our reqTimesMap
		limiterContext = context.Background()          // CONTExt for out rate limiter
	)

	var (
		throttleChan = make(chan bool, 10)
		wg           sync.WaitGroup
	)
	// Break entity list into thirds. If the request object is to long arkham fails.
	///for i := (len(config.ArkhamEntities) / 80); i < len(config.ArkhamEntities); i += (len(config.ArkhamEntities) / 80) {
	for _, entity := range config.ArkhamEntities {
		throttleChan <- true
		wg.Add(1)
		//entities := strings.Join(config.ArkhamEntities[i-(len(config.ArkhamEntities)/80):i], ",")
		go func(ent string) {
			_, ok := reqTimes[ent]
			if !ok {
				newTS := time.Now().Add(-time.Minute).UnixMilli() // if we dont have a request timestamp we will request transfer data starting a minute back
				lock.Lock()
				reqTimes[ent] = models.ArkhamLastCallTime{Entity: &ent, LastRecordTime: &newTS, DocID: &ent}
				lock.Unlock()
			}
			rateLimiter.Wait(limiterContext) // dont call unless rate is not hit
			resp, err := consumeArkhamTransferEvents(ctx, *reqTimes[ent].Entity, *reqTimes[ent].LastRecordTime)
			if err != nil {
				//if there is an error continue to next iteration
				fmt.Println(fmt.Sprintf("none %s", ent))
				log.EndTimeL(labels, "ConsumeArkhamTransferEvents", startTime, err)
				span.SetStatus(codes.Error, err.Error())
				goto END
			}
			//if no data was returned continue
			if resp == nil || resp.Count <= 0 {
				req := reqTimes[ent]
				tm := time.Now().UnixMilli()
				req.LastRecordTime = &tm
				lock.Lock()
				reqTimes[ent] = req
				lock.Unlock()
				fmt.Println(fmt.Sprintf("none %s", ent))
				goto END
			} else {
				fmt.Println(fmt.Sprintf("%s : %d", ent, resp.Count))
				req := reqTimes[ent]
				tm := resp.Transfers[0].BlockTimestamp.Add(time.Millisecond).UnixMilli() //add one millisecond to next call. this is to avoid getting dupilicates. When passing a timestamp to arkham it retrieves records >= the timestamp passed in the request.

				req.LastRecordTime = &tm
				lock.Lock()
				reqTimes[ent] = req
				transfers = append(transfers, resp.Transfers...)
				lock.Unlock()

			}

		END:
			<-throttleChan
			wg.Done()
		}(entity)
	}
	wg.Wait()

	insertTime := time.Now()
	bqData, err := store.ConverArkhamTransferToBq(ctx, transfers, insertTime)
	if handleErr(err, labels, startTime, span) {
		w.WriteHeader(500)
		return
	}

	//1. create bigquery client
	bq, err := store.NewBQStore()
	if handleErr(err, labels, startTime, span) {
		w.WriteHeader(500)
		return
	}
	//2. Insert TransferData
	err = bq.InsertTransferData(ctx, &bqData)
	if handleErr(err, labels, startTime, span) {
		w.WriteHeader(500)
		return
	}

	//gather and send alerts. Pass timeGTE this will query everything we pulled for this window
	SendAlerts(ctx, insertTime)

	//3. Persist the most recent transaction time. (We will pass this on the next call to arkham)
	store.SetRequestTimestamps(ctx, reqTimes)

	span.SetStatus(codes.Ok, codes.Ok.String())
	w.WriteHeader(200)
	w.Write([]byte("OK"))

}

// Retreives a list of assets from postgres and fetches metadata for each of those assets. Then it stores the metadata in coingecko_asset_metadata table
func consumeArkhamTransferEvents(ctx context.Context, e string, timeGTE int64) (*arkham.ArkhamTransferResponse, error) {
	labels := generateLabelFromContext(ctx)
	startTime := log.StartTimeL(labels, "ConsumeArkhamTransferEvents")
	var (
		limit = 1500 // max 10000 only use 1500 the larger the number the slower the response from arkham
	)

	data, err := c.GetTransferData(ctx, &arkham.ArkhamTransferOptions{Base: &e, TimeGte: &timeGTE, Limit: &limit})
	if err != nil {
		log.EndTimeL(labels, "ConsumeArkhamTransferEvents", startTime, err)
		return nil, err
	}

	if data == nil || len(data.Transfers) == 0 {
		//if we have no data dont continue
		log.EndTimeL(labels, "ConsumeArkhamTransferEvents", startTime, err)
		return nil, errors.New("No Data recieved")
	}
	log.EndTimeL(labels, "ConsumeArkhamTransferEvents", startTime, nil)
	return data, nil

}

// Retreives a list of assets from postgres and fetches metadata for each of those assets. Then it stores the metadata in coingecko_asset_metadata table
func consumeArkhamProfiles(ctx context.Context, e string, timeGTE int64) (*[]models.BQArkhamPortfolioEntry, error) {
	labels := generateLabelFromContext(ctx)
	startTime := log.StartTimeL(labels, "ConsumeArkhamTransferEvents")

	//data, err := c.GetTransferData(ctx, &arkham.ArkhamTransferOptions{Base: &e, TimeGte: &timeGTE, Limit: &limit})
	data, err := c.GetEntityPortfolio(ctx, &arkham.ArkhamEntityPortfolioRequstOptions{Time: &timeGTE, Entity: &e})
	if err != nil {
		log.EndTimeL(labels, "ConsumeArkhamTransferEvents", startTime, err)
		return nil, err
	}

	if data == nil {
		//if we have no data dont continue
		log.EndTimeL(labels, "ConsumeArkhamTransferEvents", startTime, err)
		return nil, errors.New("No Data recieved")
	}
	bqData := store.ConvertPortfolioDataToBQ(e, *data)
	log.EndTimeL(labels, "ConsumeArkhamTransferEvents", startTime, nil)
	return &bqData, nil

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

// handles errors
func handleErr(err error, labels map[string]string, startTime time.Time, span trace.Span) bool {
	if err != nil {
		log.EndTimeL(labels, "Error inserting transfer data: %s", startTime, err)
		span.SetStatus(codes.Error, err.Error())
		return true
	}
	return false

}

// Etry point for running profiles fetch, and store logic
func RunConsumeArkhamProfiles(w http.ResponseWriter, r *http.Request) {

	ctx, span := tracer.Start(context.Background(), "ConsumeArkhamTransferEvents")
	defer span.End()
	labels := generateLabelFromContext(ctx)

	startTime := log.StartTimeL(labels, "ConsumeArkhamTransferEvents")
	config, _ := store.GetArkhamTransferSettings(ctx)

	var (
		bqData         []models.BQArkhamPortfolioEntry // Arkham portfolio data in our BQ format
		lock           = &sync.Mutex{}                 //lock used when altering our reqTimesMap
		limiterContext = context.Background()          // CONTExt for out rate limiter
		now            = time.Now()                    // instructs the arkham api to fetch entity holdings at this timestamp
	)

	var (
		throttleChan     = make(chan bool, 10)
		wg               sync.WaitGroup
		requestTimestamp = now.UnixMilli()
	)
	// Break entity list into thirds. If the request object is to long arkham fails.
	///for i := (len(config.ArkhamEntities) / 80); i < len(config.ArkhamEntities); i += (len(config.ArkhamEntities) / 80) {
	for _, entity := range config.ArkhamEntities {
		throttleChan <- true
		wg.Add(1)
		//entities := strings.Join(config.ArkhamEntities[i-(len(config.ArkhamEntities)/80):i], ",")
		go func(ent string) {

			rateLimiter.Wait(limiterContext) // dont call unless rate is not hit
			resp, err := consumeArkhamProfiles(ctx, ent, requestTimestamp)
			if err != nil {
				//if there is an error continue to next iteration
				fmt.Println(fmt.Sprintf("none %s", ent))
				log.EndTimeL(labels, "ConsumeArkhamTransferEvents", startTime, err)
				span.SetStatus(codes.Error, err.Error())
				goto END
			}
			//if no data was returned continue
			if resp == nil {

				fmt.Println(fmt.Sprintf("none %s", ent))
				goto END
			} else {
				fmt.Println(fmt.Sprintf("%s : %d", ent, len(*resp)))
				lock.Lock()
				bqData = append(bqData, *resp...)
				lock.Unlock()

			}

		END:
			<-throttleChan
			wg.Done()
		}(entity)
	}
	wg.Wait()

	//1. create bigquery client
	bq, err := store.NewBQStore()
	if handleErr(err, labels, startTime, span) {
		w.WriteHeader(500)
		return
	}
	//2. Insert TransferData
	err = bq.InsertPortfolioData(ctx, &bqData)
	if handleErr(err, labels, startTime, span) {
		w.WriteHeader(500)
		return
	}

	//3. Persist the most recent transaction time. (We will pass this on the next call to arkham)
	span.SetStatus(codes.Ok, codes.Ok.String())
	w.WriteHeader(200)
	w.Write([]byte("OK"))

}

func ConsumePubsub(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(context.Background(), "ConsumePubsub")
	defer span.End()
	labels := generateLabelFromContext(ctx)

	startTime := log.StartTimeL(labels, "ConsumePubsub")
	x := html.EscapeString(r.URL.Query().Get("threshold"))
	threshold, err := strconv.ParseFloat(x, 64)
	if err != nil {
		w.WriteHeader(500)
		return
	}
	pubsubC, err := store.NewPubSubClient()
	if err != nil {
		fmt.Printf("%s \n", err)
		log.EndTimeL(labels, "ConsumePubsub", startTime, err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(500)
		return
	}
	defer pubsubC.Close()
	res := pubsubC.GetPubSubMessages(ctx, threshold)

	bqs, err := store.NewBQStore()
	if err != nil {
		fmt.Printf("%s \n", err)
		log.EndTimeL(labels, "BigQuery Connection error", startTime, err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(500)
		return
	}

	err = bqs.InsertTransactionData(ctx, res)
	if err != nil {
		fmt.Printf("%s \n", err)
		log.EndTimeL(labels, "BigQuery Insert error", startTime, err)
		span.SetStatus(codes.Error, err.Error())
		w.WriteHeader(500)
		return
	}

	//3. Persist the most recent transaction time. (We will pass this on the next call to arkham)
	span.SetStatus(codes.Ok, codes.Ok.String())
	log.EndTimeL(labels, "ConsumePubsub", startTime, err)
	w.WriteHeader(200)
	w.Write([]byte("OK"))
}
