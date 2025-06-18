package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/Forbes-Media/fda-arkham-ingestion/models"
	"github.com/Forbes-Media/go-tools/log"
	"github.com/lib/pq"
	"go.nhat.io/otelsql"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

var (
	pg           *PostgresqlConnect
	DBClientOnce sync.Once
)

type PostgresqlConnect struct {
	*sql.DB
}

func PGConnect() *PostgresqlConnect {
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
			client, err := sql.Open(driverName, connectionString)
			if err != nil {
				log.Error("%s", err)
				return
			}
			var pgs PostgresqlConnect
			pgs.DB = client
			pg = &pgs
			if err := otelsql.RecordStats(pg.DB); err != nil {
				return
			}
			maxLifetime := 5 * time.Minute

			pg.DB.SetConnMaxLifetime(maxLifetime)

			connectionError := pg.DB.Ping()

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

func (pg *PostgresqlConnect) InsertTransactions(ctx context.Context, transaction *models.MessagesSchemas) error {

	valueArg := make([]interface{}, 0, 46)

	tableName := "transitions"

	var valString = fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d,$%d, $%d, $%d, $%d)", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46)
	valueArg = append(valueArg, transaction.ChainId)
	valueArg = append(valueArg, transaction.Id)
	valueArg = append(valueArg, transaction.LogIndex)
	valueArg = append(valueArg, transaction.TransactionHash)
	valueArg = append(valueArg, transaction.TransactionIndex)
	valueArg = append(valueArg, transaction.Address)
	valueArg = append(valueArg, transaction.CreatorAddress)
	valueArg = append(valueArg, transaction.TxFromAddress)
	valueArg = append(valueArg, transaction.Data)
	valueArg = append(valueArg, pq.Array(transaction.Topics))
	// timestampUnix, _ := strconv.ParseInt(transaction.BlockTimestamp, 10, 64)
	timestamp := time.Unix(transaction.BlockTimestamp, 0).UTC()
	valueArg = append(valueArg, timestamp)
	valueArg = append(valueArg, transaction.BlockNumber)
	valueArg = append(valueArg, transaction.BlockHash)
	valueArg = append(valueArg, transaction.Signature)
	valueArg = append(valueArg, transaction.FromAddress)
	valueArg = append(valueArg, transaction.ToAddress)
	valueArg = append(valueArg, transaction.ToCreatorAddress)
	valueArg = append(valueArg, transaction.ToTxFromAddress)
	decoded := make(map[string][]models.DecodedList)
	json.Unmarshal([]byte(transaction.Decoded), &decoded)
	fmt.Printf("%s\n", transaction.Decoded)
	decodedList, _ := json.Marshal(decoded)
	valueArg = append(valueArg, decodedList)
	valueArg = append(valueArg, transaction.DecodedError)
	valueArg = append(valueArg, transaction.IsDecoded)
	valueArg = append(valueArg, transaction.ValueLossless.String())
	valueArg = append(valueArg, transaction.Value)
	valueArg = append(valueArg, transaction.Nonce)
	valueArg = append(valueArg, transaction.Gas)
	valueArg = append(valueArg, transaction.GasPrice)
	valueArg = append(valueArg, transaction.Input)
	valueArg = append(valueArg, transaction.ReceiptCumulativeGasUsed)
	valueArg = append(valueArg, transaction.ReceiptGasUsed)
	valueArg = append(valueArg, transaction.ReceiptContractAddress)
	valueArg = append(valueArg, transaction.ReceiptRoot)
	valueArg = append(valueArg, transaction.ReceiptStatus)
	valueArg = append(valueArg, transaction.MaxFeePerGas)
	valueArg = append(valueArg, transaction.MaxPriorityFeePerGas)
	valueArg = append(valueArg, transaction.TransactionType)
	valueArg = append(valueArg, transaction.ReceiptEffectiveGasPrice)
	valueArg = append(valueArg, transaction.Fee)
	valueArg = append(valueArg, transaction.TxnSaving)
	valueArg = append(valueArg, transaction.BurnedFee)
	valueArg = append(valueArg, transaction.MethodId)
	valueArg = append(valueArg, transaction.R)
	valueArg = append(valueArg, transaction.S)
	valueArg = append(valueArg, transaction.V)
	accessList, _ := json.Marshal(transaction.AccessList)
	valueArg = append(valueArg, accessList)
	valueArg = append(valueArg, transaction.ValueUsd)
	valueArg = append(valueArg, transaction.FeeUsd)

	// upsertStatement := `On Conflict ()`

	insertStatement := fmt.Sprintf(`INsert into %s Values %s`, tableName, valString)
	_, inserterError := pg.ExecContext(ctx, insertStatement, valueArg...)

	if inserterError != nil {
		log.Error("Error Inserting to Postgresql => %s", inserterError)
		return inserterError
	}

	return nil

}
