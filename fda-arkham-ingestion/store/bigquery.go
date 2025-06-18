package store

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/Forbes-Media/fda-arkham-ingestion/models"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/attribute"
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

type BQStore struct {
	*bigquery.Client
}

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

/*
Inserts NFT Data into bigquery
*/
func (bq *BQStore) InsertTransferData(ctx0 context.Context, tickers *[]models.BQArkhamTransferRecord) error {
	ctx, span := tracer.Start(ctx0, "InsertTickerData")
	defer span.End()

	currenciesTable := "Digital_Asset_Arkham_Transfers_dev"

	bqInserter := bq.Dataset("digital_assets").Table(currenciesTable).Inserter()
	bqInserter.IgnoreUnknownValues = true

	inserterErr := bqInserter.Put(ctx, *tickers)
	var retryError error
	if inserterErr != nil {
		if strings.Contains(inserterErr.Error(), "413") {
			log.Info("413 Error. Breaking up tickers and retrying insert")
			l := len(*tickers)
			var ticks []models.BQArkhamTransferRecord
			ticks = append(ticks, *tickers...)
			for y := (l / 3); y < l; y += (l / 3) {
				a := ticks[y-(l/3) : y]
				er := bq.InsertTransferData(ctx, &a)
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

/*
Inserts Profile Data into Big Query
*/
func (bq *BQStore) InsertPortfolioData(ctx0 context.Context, tickers *[]models.BQArkhamPortfolioEntry) error {
	ctx, span := tracer.Start(ctx0, "InsertProfileData")
	defer span.End()

	tableName := GetTableName("Digital_Asset_Arkham_Portfolio_Data")

	bqInserter := bq.Dataset("digital_assets").Table(tableName).Inserter()
	bqInserter.IgnoreUnknownValues = true

	inserterErr := bqInserter.Put(ctx, *tickers)
	var retryError error
	if inserterErr != nil {
		if strings.Contains(inserterErr.Error(), "413") {
			log.Info("413 Error. Breaking up Profiles and retrying insert")
			l := len(*tickers)
			var ticks []models.BQArkhamPortfolioEntry
			ticks = append(ticks, *tickers...)
			for y := (l / 3); y < l; y += (l / 3) {
				a := ticks[y-(l/3) : y]
				er := bq.InsertPortfolioData(ctx, &a)
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

/*
Inserts Profile Data into Big Query
*/
func (bq *BQStore) InsertTransactionData(ctx0 context.Context, transactions []models.BQTransaction) error {
	ctx, span := tracer.Start(ctx0, "InsertTransactionData")
	defer span.End()

	tableName := GetTableName("Digital_Asset_Transactions_data_dev")

	bqInserter := bq.Dataset("digital_assets").Table(tableName).Inserter()
	bqInserter.IgnoreUnknownValues = true

	inserterErr := bqInserter.Put(ctx, transactions)
	var retryError error
	if inserterErr != nil {
		if strings.Contains(inserterErr.Error(), "413") {
			log.Info("413 Error. Breaking up Transaction and retrying insert")
			l := len(transactions)
			var tran []models.BQTransaction
			tran = append(tran, transactions...)
			for y := (l / 3); y < l; y += (l / 3) {
				a := tran[y-(l/3) : y]
				er := bq.InsertTransactionData(ctx, a)
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

// Builds Queries off the alert rules set in firestore
func BuildAlertQuery(rule models.ArkhamAlertRules, lastUpdatedTime time.Time) string {
	var (
		query string
	)
	tableName := GetTableName("Digital_Asset_Arkham_Transfers")
	base := `
		SELECT
  			distinct *
		FROM
			api-project-901373404215.digital_assets.` + tableName + ` c 
		WHERE `

	var conditions []string

	//if entity is set add condition to look for entities
	if rule.Entity[0].Name != "any" && rule.Entity[0].Name != "" {
		conditions = append(conditions, fmt.Sprintf("(to_entity='%s' or from_entity='%s')", rule.Entity[0].Name, rule.Entity[0].Name))
	}
	//if toWallet is not empty add condition fo filter for wallet
	if rule.ToWallet != "any" && rule.ToWallet != "" {
		conditions = append(conditions, fmt.Sprintf("to_addr='%s'", rule.ToWallet))
	}
	// if fromWallet is not empty add condition for from wallet
	if rule.FromWallet != "any" && rule.FromWallet != "" {
		conditions = append(conditions, fmt.Sprintf("from_addr='%s'", rule.FromWallet))
	}

	if len(rule.Token) > 0 {
		con := generateTokenBQInStatement(rule.Token)
		if con != "" {
			conditions = append(conditions, generateTokenBQInStatement(rule.Token))
		}
	}
	if len(rule.Chain) > 0 {
		con := generateChainBQInStatement(rule.Chain)
		if con != "" {
			conditions = append(conditions, con)
		}
	}
	min, err := strconv.ParseFloat(strings.ReplaceAll(rule.MinUSDTreshold, ",", ""), 64)
	if err != nil {
		log.Error(" Could Not parse min value setting defaulting the value to 1.0 %s", err)
		min = 1.0
	}
	max, err := strconv.ParseFloat(strings.ReplaceAll(rule.MaxUSDTreshold, ",", ""), 64)
	if err != nil {
		log.Error(" Could Not parse max value setting defaulting the value to 10000000.0 %s", err)
		max = 10000000.0
	}

	//add condition fo find transactions within price limit
	conditions = append(conditions, fmt.Sprintf("total_price BETWEEN %f AND %f", min, max))
	conditions = append(conditions, fmt.Sprintf("insert_timestamp >= cast('%s' as timestamp)", lastUpdatedTime.Format(time.RFC3339)))
	query = addConconditionsToQuery(conditions, base)

	return query
}

func generateTokenBQInStatement(tokens []models.ArkhamRulesToken) string {

	var inString = "token_symbol in "
	var list = ""

	for _, tok := range tokens {
		//if any is in the list we will search for any token. Else we will build a condition where token_symbol in ('btc','matic','etc')
		if tok.Token == "any" {
			return ""
		}
		list += fmt.Sprintf("'%s',", tok.Token)
	}

	list = strings.TrimRight(list, ",")

	return fmt.Sprintf("%s(%s)", inString, list)
}
func generateChainBQInStatement(chains []models.ArkhamRulesChain) string {
	var inString = "chain in "
	var list = ""

	for _, chain := range chains {
		if chain.Chain == "any" {
			return ""
		}
		list += fmt.Sprintf("'%s',", chain.Chain)
	}

	list = strings.TrimRight(list, ",")

	return fmt.Sprintf("%s(%s)", inString, list)

}

func addConconditionsToQuery(conditions []string, baseQuery string) string {
	baseQuery += strings.Join(conditions, " AND ")
	return baseQuery
}

// gets data from arkham data that should be sent to slack
func (bq *BQStore) GetAlertData(ctxO context.Context, uuid string, baseQuery string) ([]models.BQArkhamTransferRecord, error) {
	ctx, span := tracer.Start(ctxO, "GetAlertData")
	defer span.End()

	labels := make(map[string]string)

	labels["uuid"] = uuid
	span.SetAttributes(attribute.String("uuid", uuid))
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	labels["bigquery"] = "true"
	span.SetAttributes(attribute.Bool("bigquery", true))
	labels["subFunction"] = "GetAlertData"

	startTime := log.StartTimeL(labels, "GetAlertData")

	log.DebugL(labels, "GetAlertData")

	query := bq.Query(baseQuery)

	job, err := query.Run(ctx)
	if err != nil {
		span.SetStatus(otelCodes.Error, err.Error())
		ConsumeTimeL(labels, "GetAlertData", startTime, err)
		return nil, err
	}

	labels["GetAlertData_job_id"] = job.ID()
	span.SetAttributes(attribute.String("GetAlertData_job_id", job.ID()))

	log.DebugL(labels, "GetAlertData Job ID: %s", job.ID())

	status, err := job.Wait(ctx)
	if err != nil {
		span.SetStatus(otelCodes.Error, err.Error())
		ConsumeTimeL(labels, "GetAlertData Query", startTime, err)
		return nil, err
	}

	span.AddEvent("GetAlertData Job Completed")

	if err := status.Err(); err != nil {
		span.SetStatus(otelCodes.Error, err.Error())
		ConsumeTimeL(labels, "GetAlertData Job", startTime, err)
		return nil, err
	}

	it, err := job.Read(ctx)
	if err != nil {
		span.SetStatus(otelCodes.Error, err.Error())
		ConsumeTimeL(labels, "GetAlertData Read", startTime, err)
		return nil, err
	}

	var exchangesResults []models.BQArkhamTransferRecord
	for {

		var exchangesResult models.BQArkhamTransferRecord
		err := it.Next(&exchangesResult)
		if err == iterator.Done {
			break
		}
		if err != nil {
			span.SetStatus(otelCodes.Error, err.Error())
			ConsumeTimeL(labels, "Exchange Fundamentals CG Scan ", startTime, err)
			return nil, err
		}

		exchangesResults = append(exchangesResults, exchangesResult)

	}

	log.InfoL(labels, "GetAlertData %d videos retrieved", len(exchangesResults))

	span.SetStatus(otelCodes.Ok, "GetAlertData Completed")

	return exchangesResults, nil

}

// gets data from arkham data that should be sent to slack
func (bq *BQStore) GetChainData(ctxO context.Context, uuid string) ([]string, error) {
	ctx, span := tracer.Start(ctxO, "GetAlertData")
	defer span.End()

	labels := make(map[string]string)
	tableName := GetTableName("Digital_Asset_Arkham_Transfers")
	labels["uuid"] = uuid
	span.SetAttributes(attribute.String("uuid", uuid))
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	labels["bigquery"] = "true"
	span.SetAttributes(attribute.Bool("bigquery", true))
	labels["subFunction"] = "GetAlertData"

	startTime := log.StartTimeL(labels, "GetAlertData")

	log.DebugL(labels, "GetAlertData")
	query := bq.Query(`
	SELECT
  		DISTINCT chain
	FROM
	api-project-901373404215.digital_assets.` + tableName + ` c 
	WHERE
  	chain != ''
	`)

	job, err := query.Run(ctx)
	if err != nil {
		span.SetStatus(otelCodes.Error, err.Error())
		ConsumeTimeL(labels, "GetAlertData", startTime, err)
		return nil, err
	}

	labels["GetAlertData_job_id"] = job.ID()
	span.SetAttributes(attribute.String("GetAlertData_job_id", job.ID()))

	log.DebugL(labels, "GetAlertData Job ID: %s", job.ID())

	status, err := job.Wait(ctx)
	if err != nil {
		span.SetStatus(otelCodes.Error, err.Error())
		ConsumeTimeL(labels, "GetAlertData Query", startTime, err)
		return nil, err
	}

	span.AddEvent("GetAlertData Job Completed")

	if err := status.Err(); err != nil {
		span.SetStatus(otelCodes.Error, err.Error())
		ConsumeTimeL(labels, "GetAlertData Job", startTime, err)
		return nil, err
	}

	it, err := job.Read(ctx)
	if err != nil {
		span.SetStatus(otelCodes.Error, err.Error())
		ConsumeTimeL(labels, "GetAlertData Read", startTime, err)
		return nil, err
	}

	var chains []string
	for {

		var exchangesResult models.BQGetChainResult
		err := it.Next(&exchangesResult)
		if err == iterator.Done {
			break
		}
		if err != nil {
			span.SetStatus(otelCodes.Error, err.Error())
			ConsumeTimeL(labels, "Exchange Fundamentals CG Scan ", startTime, err)
			return nil, err
		}

		chains = append(chains, exchangesResult.Chain)

	}

	log.InfoL(labels, "GetAlertData %d videos retrieved", len(chains))

	span.SetStatus(otelCodes.Ok, "GetAlertData Completed")

	return chains, nil

}

// gets data from arkham data that should be sent to slack
func (bq *BQStore) GetTokenData(ctxO context.Context, uuid string) ([]string, error) {
	ctx, span := tracer.Start(ctxO, "GetAlertData")
	defer span.End()

	labels := make(map[string]string)
	tableName := GetTableName("Digital_Asset_Arkham_Transfers")
	labels["uuid"] = uuid
	span.SetAttributes(attribute.String("uuid", uuid))
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	labels["bigquery"] = "true"
	span.SetAttributes(attribute.Bool("bigquery", true))
	labels["subFunction"] = "GetAlertData"

	startTime := log.StartTimeL(labels, "GetAlertData")

	log.DebugL(labels, "GetAlertData")
	query := bq.Query(`
	SELECT
		token_symbol,
		count (*) AS count
  	FROM
	  api-project-901373404215.digital_assets.` + tableName + ` c 
  	WHERE
		token_symbol != ''
  	GROUP BY
		token_symbol
  	ORDER BY
		count DESC
  	LIMIT
		100
	`)

	job, err := query.Run(ctx)
	if err != nil {
		span.SetStatus(otelCodes.Error, err.Error())
		ConsumeTimeL(labels, "GetAlertData", startTime, err)
		return nil, err
	}

	labels["GetAlertData_job_id"] = job.ID()
	span.SetAttributes(attribute.String("GetAlertData_job_id", job.ID()))

	log.DebugL(labels, "GetAlertData Job ID: %s", job.ID())

	status, err := job.Wait(ctx)
	if err != nil {
		span.SetStatus(otelCodes.Error, err.Error())
		ConsumeTimeL(labels, "GetAlertData Query", startTime, err)
		return nil, err
	}

	span.AddEvent("GetAlertData Job Completed")

	if err := status.Err(); err != nil {
		span.SetStatus(otelCodes.Error, err.Error())
		ConsumeTimeL(labels, "GetAlertData Job", startTime, err)
		return nil, err
	}

	it, err := job.Read(ctx)
	if err != nil {
		span.SetStatus(otelCodes.Error, err.Error())
		ConsumeTimeL(labels, "GetAlertData Read", startTime, err)
		return nil, err
	}

	var chains []string
	for {

		var exchangesResult models.BQGetTokenResult
		err := it.Next(&exchangesResult)
		if err == iterator.Done {
			break
		}
		if err != nil {
			span.SetStatus(otelCodes.Error, err.Error())
			ConsumeTimeL(labels, "Exchange Fundamentals CG Scan ", startTime, err)
			return nil, err
		}

		chains = append(chains, exchangesResult.Token)

	}

	log.InfoL(labels, "GetAlertData %d videos retrieved", len(chains))

	span.SetStatus(otelCodes.Ok, "GetAlertData Completed")

	return chains, nil

}

// Deprecated: use github.com/Forbes-Media/go-tools/log..ConsumeTimeL instead
func ConsumeTimeL(labels map[string]string, message string, startTime time.Time, err error) {
	endTime := time.Now()
	elapsed := time.Since(startTime)
	if err != nil {
		log.DebugL(labels, "%s Error :: %s, Finished  at :: %s, Total execution time :: %s", message, err, endTime, elapsed)
	} else {
		log.DebugL(labels, "%s Process, Finished at :: %s, Total execution time :: %s", message, endTime, elapsed)
	}
}

func GetTableName(tableName string) string {

	if os.Getenv("DATA_NAMESPACE") == "_dev" {
		return fmt.Sprintf("%s%s", tableName, os.Getenv("DATA_NAMESPACE"))
	}

	return tableName
}
