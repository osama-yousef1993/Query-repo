package repository

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Forbes-Media/fda-common/cloudUtils"
	"github.com/Forbes-Media/fda-common/crypto-filter-protocol/common"
	"github.com/Forbes-Media/fda-common/crypto-filter-protocol/dto"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
)

type ExchangeQuery interface {
	GetForbesExchanges(mapBy string) (exchanges *map[string]dto.ForbesExchange, err error)
	GetExchangeFundamentalsDistinct() (exchanges *[]dto.ForbesExchange, err error)
	UpsertForbesExchanges(exchanges *[]dto.ForbesExchange) (err error)
	SendForRemediation(map[string]dto.ExchangeSubmission) (err error)
}

type exchangeQuery struct {
}

func (r *exchangeQuery) GetForbesExchanges(mapBy string) (exchanges *map[string]dto.ForbesExchange, err error) {
	span, labels := common.GenerateSpan("crypto-filter-protocol.GetForbesExchanges", context.Background())
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "crypto-filter-protocol.GetForbesExchanges"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "crypto-filter-protocol.GetForbesExchanges"))
	exchanges = &map[string]dto.ForbesExchange{}
	c, err := pg.GetClient()

	if err != nil {
		log.Error("Error closing client: %v", err)
		return
	}

	query := fmt.Sprintf(`
		SELECT
			forbes_id,
			name,
			coingecko_id,
			coinpaprika_id,
			last_updated
		FROM %s`, "forbes_exchanges")

	queryResult, err := c.Query(query)
	if err != nil {
		log.Error("Error querying forbes_exchanges: %v", err)
		return
	}

	for queryResult.Next() {
		var exchange dto.ForbesExchange
		err = queryResult.Scan(
			&exchange.ForbesID, &exchange.Name, &exchange.CoingeckoID,
			&exchange.CoinpaprikaID, &exchange.LastUpdated)
		if err != nil {
			log.Error("Error scanning forbes_exchanges: %v", err)
			return
		}
		switch mapBy {
		case "forbes_id":
			(*exchanges)[strings.ToLower(*exchange.ForbesID)] = exchange
		case "name":
			(*exchanges)[strings.ToLower(*exchange.Name)] = exchange
		case "coingecko_id":
			(*exchanges)[strings.ToLower(*exchange.CoingeckoID)] = exchange
		case "coinpaprika_id":
			(*exchanges)[strings.ToLower(*exchange.CoinpaprikaID)] = exchange
		}

	}

	log.EndTime("Search assets Query", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return

}

func (r *exchangeQuery) UpsertForbesExchanges(exchanges *[]dto.ForbesExchange) (err error) {
	span, labels := common.GenerateSpan("crypto-filter-protocol.GetForbesExchanges", context.Background())
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "crypto-filter-protocol.GetForbesExchanges"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "crypto-filter-protocol.GetForbesExchanges"))

	c, err := pg.GetClient()

	if err != nil {
		log.Error("Error closing client: %v", err)
		return
	}
	exchangeList := *exchanges
	valueString := make([]string, 0, len(exchangeList))
	valueArgs := make([]interface{}, 0, len(exchangeList)*5)
	colCount := 5
	tableName := "forbes_exchanges"
	var i = 0
	for y := 0; y < len(exchangeList); y++ {
		e := exchangeList[y]

		var valString = fmt.Sprintf("($%d, $%d, $%d, $%d, $%d)", i*colCount+1, i*colCount+2, i*colCount+3, i*colCount+4, i*colCount+5)
		valueString = append(valueString, valString)

		valueArgs = append(valueArgs, e.ForbesID)
		valueArgs = append(valueArgs, e.Name)
		valueArgs = append(valueArgs, e.CoingeckoID)
		valueArgs = append(valueArgs, e.CoinpaprikaID)
		valueArgs = append(valueArgs, time.Now().UTC())
		i++

		if len(valueString) >= 65000 || y == len(exchangeList)-1 {
			insertStatement := fmt.Sprintf("INSERT INTO %s (forbes_id, name, coingecko_id, coinpaprika_id, last_updated) VALUES %s", tableName, strings.Join(valueString, ","))
			updateStatement := "ON CONFLICT (forbes_id) DO UPDATE SET name = excluded.name, coingecko_id = COALESCE(excluded.coingecko_id, excluded.coingecko_id), coinpaprika_id = COALESCE(excluded.coinpaprika_id, excluded.coinpaprika_id), last_updated = excluded.last_updated"

			query := insertStatement + " " + updateStatement
			_, inserterError := c.ExecContext(context.Background(), query, valueArgs...)
			if inserterError != nil {
				log.Error("UpsertExchangeList: Error Upserting Exchange List to PostgreSQL: %v", inserterError)
				return inserterError
			}
			valueString = make([]string, 0, len(exchangeList))
			valueArgs = make([]interface{}, 0, len(exchangeList)*colCount)
			i = 0
		}
	}
	log.EndTime("Search assets Query", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return

}

func (r *exchangeQuery) GetExchangeFundamentalsDistinct() (exchanges *[]dto.ForbesExchange, err error) {
	span, labels := common.GenerateSpan("crypto-filter-protocol.GetCoinGeckoExchanges", context.Background())
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "crypto-filter-protocol.GetCoinGeckoExchanges"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "crypto-filter-protocol.GetCoinGeckoExchanges"))
	exchanges = &[]dto.ForbesExchange{}
	c, err := pg.GetClient()

	if err != nil {
		log.Error("Error closing client: %v", err)
		return
	}

	query := fmt.Sprintf(`
		SELECT ef.slug,ef.id, ef.name
		FROM exchange_fundamentals ef
		INNER JOIN (
			SELECT id, MAX(last_updated) as max_last_updated
			FROM %s
			GROUP BY id
		) ef1
		ON ef.id = ef1.id AND ef.last_updated = ef1.max_last_updated
		ORDER BY ef.name`, "exchange_fundamentals")

	queryResult, err := c.Query(query)
	if err != nil {
		log.Error("Error querying forbes_exchanges: %v", err)
		return
	}

	for queryResult.Next() {
		var exchange dto.ForbesExchange
		err = queryResult.Scan(
			&exchange.ForbesID, &exchange.CoingeckoID, &exchange.Name)
		if err != nil {
			log.Error("Error scanning forbes_exchanges: %v", err)
			return
		}
		*exchanges = append(*exchanges, exchange)
	}

	log.EndTime("Search assets Query", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return

}

func (r *exchangeQuery) SendForRemediation(exchanges map[string]dto.ExchangeSubmission) (err error) {
	span, labels := common.GenerateSpan("crypto-filter-protocol.GetCoinGeckoExchanges", context.Background())
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "crypto-filter-protocol.GetCoinGeckoExchanges"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "crypto-filter-protocol.GetCoinGeckoExchanges"))
	//exchanges = &[]dto.ForbesExchange{}

	if err != nil {
		log.Error("Error closing client: %v", err)
		return
	}

	err = cloudUtils.WriteMapToCollection[dto.ExchangeSubmission](exchanges, "dev_cryptoIntake_exchanges", fs, context.Background())
	if err != nil {
		log.Error("Error querying forbes_exchanges: %v", err)
		return
	}

	log.EndTime("Search assets Query", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return

}
