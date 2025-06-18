package services

import (
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/Forbes-Media/fda-common/crypto-filter-protocol/dto"
	"github.com/Forbes-Media/fda-common/crypto-filter-protocol/repository"
	"github.com/Forbes-Media/go-tools/log"
)

type ExchangeIntake interface {
	BackFillExchangeTickers()
	SubmitExchangesToForbes(submissions []dto.ExchangeSubmission) (err error)
}

type exchangeIntake struct {
	dao repository.Dao
}

func NewExchangeIntake(dao repository.Dao) ExchangeIntake {
	return &exchangeIntake{dao: dao}
}

// SubmitExchangesToForbes processes a list of exchange submissions and updates the Forbes exchanges database.
// It performs the following steps:
// 1. Retrieves the current list of Forbes exchanges from the database.
// 2. Maps the provided submissions to the retrieved exchanges.
// 3. Upserts the mapped exchanges into the Forbes exchanges database.
// 4. Sends any failed mappings to the remediation collection.
//
// Parameters:
// - submissions: A slice of ExchangeSubmission containing the exchange submissions to be processed.
// Returns:
// - error: An error if any issues occur during the process, otherwise nil.

func (e *exchangeIntake) SubmitExchangesToForbes(submissions []dto.ExchangeSubmission) (err error) {

	db := e.dao.NewExchangeQuery()
	exchanges, err := db.GetForbesExchanges("name")
	if err != nil {
		log.Error("Error getting exchanges: %v", err)
		return err
	}
	updates, failed, err := mapExchangesToForbes(submissions, exchanges)
	if err != nil {
		log.Error("Error mapping exchanges: %v", err)
		return err
	}

	err = db.UpsertForbesExchanges(updates)
	if err != nil {
		log.Error("Error upserting exchanges: %v", err)
		return err
	}

	err = db.SendForRemediation(failed)
	if err != nil {
		log.Error("Error writing to exchange remediation collection: %v", err)
		return err
	}
	return nil
}

func GetForbesExchanges() (exchanges *[]dto.ForbesExchange, err error) {
	db := repository.NewDao()
	exchanges, err = db.NewExchangeQuery().GetExchangeFundamentalsDistinct()
	if err != nil {
		log.Error("Error closing client: %v", err)

		return
	}
	return

}

// validateExchangeTicker validates the tickers in the given exchangeTickers.
// It checks if the required fields (Base, Target, Name, Market.Name, Market.Identifier, Source) are non-empty.
// If any of these fields are empty, the ticker is added to the failed list; otherwise, it is added to the success list.
// It also ensures that the Timestamp, LastTradedAt, and LastFetchAt fields are set to the current time if they are zero.
//
// Parameters:
//   - exchangeTickers: dto.BQExchangesTickers containing the tickers to be validated.
//
// Returns:
//   - error: Always returns nil.
func validateExchangeTicker(exchangeTickers dto.BQExchangesTickers) (failed []dto.BQTicker, successful []dto.BQTicker, err error) {

	//if the source is not a valid enum value then log an error and return
	if exchangeTickers.Source != "coingecko" && exchangeTickers.Source != "coinpaprika" {
		log.Error("Invalid source: %s", exchangeTickers.Source)
		err = fmt.Errorf("Invalid source: %s", exchangeTickers.Source)
		return
	}

	tickers := exchangeTickers.Tickers

	for _, ticker := range tickers {
		if ticker.Base == "" || ticker.Target == "" ||
			exchangeTickers.Name == "" || ticker.Market.Name == "" ||
			ticker.Market.Identifier == "" || exchangeTickers.Source == "" {
			failed = append(failed, ticker)
		} else {
			successful = append(successful, ticker)
		}
		if ticker.Timestamp.Timestamp.IsZero() {
			ticker.Timestamp = bigquery.NullTimestamp{Timestamp: time.Now(), Valid: true}
		}
		if ticker.LastTradedAt.Timestamp.IsZero() {
			ticker.LastTradedAt = bigquery.NullTimestamp{Timestamp: time.Now(), Valid: true}
		}
		if ticker.LastFetchAt.Timestamp.IsZero() {
			ticker.LastFetchAt = bigquery.NullTimestamp{Timestamp: time.Now(), Valid: true}
		}

	}
	return
}

// mapExchangesToForbes maps a list of exchange submissions to Forbes exchanges.
// It updates the Forbes exchanges with IDs from the submissions based on their source.
// If a submission has an invalid source, it logs an error and returns immediately.
// If a submission's name is not found in the Forbes exchanges, it adds the submission to the failed map.
//
// Parameters:
// - submissions: A slice of ExchangeSubmission containing the exchange submissions to be mapped.
// - fbsExchanges: A pointer to a map of ForbesExchange where the key is the exchange name.
//
// Returns:
// - updates: A pointer to a slice of ForbesExchange containing the updated exchanges.
// - failed: A map of ExchangeSubmission containing the submissions that failed to be mapped.
// - err: An error if an invalid source is encountered, otherwise nil.
func mapExchangesToForbes(submissions []dto.ExchangeSubmission, fbsExchanges *map[string]dto.ForbesExchange) (updates *[]dto.ForbesExchange, failed map[string]dto.ExchangeSubmission, err error) {
	updates = &[]dto.ForbesExchange{}
	failed = make(map[string]dto.ExchangeSubmission)
	for _, submission := range submissions {
		if submission.Source != "coingecko" && submission.Source != "coinpaprika" {
			log.Error("Invalid source: %s", submission.Source)
			err = fmt.Errorf("Invalid source: %s", submission.Source)
			return
		}
		val, ok := (*fbsExchanges)[strings.ToLower(submission.Name)]
		if ok {
			switch submission.Source {
			case "coingecko":
				val.CoingeckoID = &submission.ID
			case "coinpaprika":
				val.CoinpaprikaID = &submission.ID
			}
			*updates = append(*updates, val)
		} else {
			failed[submission.Name] = dto.ExchangeSubmission{ID: submission.ID, Name: submission.Name, Source: submission.Source}
		}

	}

	return

}

// BackFillExchangeTickers retrieves exchange data and updates the database with new exchange information.
// It first checks if there are any existing exchanges defined. If there are, it logs an error and returns.
// If no exchanges are defined, it retrieves distinct exchange fundamentals and upserts them into the database.
// Any errors encountered during the process are logged.
func (e *exchangeIntake) BackFillExchangeTickers() {
	db := e.dao

	exchanges, err := db.NewExchangeQuery().GetForbesExchanges("forbes_id")
	if err != nil {
		log.Error("Error getting exchanges: %v", err)
	}
	if exchanges != nil && len(*exchanges) > 0 {
		log.Error("cannot backfill exchanges are already defined")
		return
	}

	exchangesMap, err := db.NewExchangeQuery().GetExchangeFundamentalsDistinct()
	if err != nil {
		log.Error("Error closing client: %v", err)
		return
	}

	exchangeArr := &[]dto.ForbesExchange{}
	for _, exchange := range *exchangesMap {
		*exchangeArr = append(*exchangeArr, exchange)
	}

	err = db.NewExchangeQuery().UpsertForbesExchanges(exchangeArr)
	if err != nil {
		log.Error("Error upserting exchanges: %v", err)
		return
	}

	return

}
