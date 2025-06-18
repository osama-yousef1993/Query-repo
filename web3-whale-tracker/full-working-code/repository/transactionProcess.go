package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Forbes-Media/go-tools/log"
	"github.com/Forbes-Media/web3-whale-tracker/common"
	"github.com/Forbes-Media/web3-whale-tracker/datastruct"
	"github.com/patrickmn/go-cache"
	"go.opentelemetry.io/otel/codes"
	"google.golang.org/api/iterator"
)

type TransactionsProcess interface {
	SendTransaction(context.Context, *datastruct.PubSubMessage) error
}

type transactionsProcess struct {
	cache *cache.Cache
}

// Get the alert rules
// GetFSAlertRules
// Takes Context
// Returns ([]datastruct.WhaleTrackerAlertRules, error)
//
// Get Alert Rules value from FS
// Returns Alert Rules and no err if successfully.
func (t *transactionsProcess) GetFSAlertRules(ctx context.Context) ([]datastruct.WhaleTrackerAlertRules, error) {
	span, labels := common.GenerateSpan("transactionsQuery.GetFSAlertRules", ctx)
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "transactionsQuery.GetFSAlertRules"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "transactionsQuery.GetFSAlertRules"))
	fs, err := fsUtils.GetFirestoreClient()
	if err != nil {
		log.ErrorL(labels, "transactionsQuery.GetFSAlertRules Error %s", err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	var rules []datastruct.WhaleTrackerAlertRules

	iter := fs.Collection(datastruct.CollectionName).Where("isActive", "==", true).Documents(ctx)

	for {
		doc, err := iter.Next()
		var rule datastruct.WhaleTrackerAlertRules

		if err == iterator.Done {
			break
		} else if err != nil {
			log.ErrorL(labels, "transactionsQuery.GetFSAlertRules Error %s", err)
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		} else {
			err = doc.DataTo(&rule)
			if err != nil {
				log.ErrorL(labels, "transactionsQuery.GetFSAlertRules Error %s", err)
				span.SetStatus(codes.Error, err.Error())
				return nil, err
			}
			rules = append(rules, rule)
		}
	}

	log.EndTimeL(labels, "transactionsQuery.GetFSAlertRules", startTime, nil)
	span.SetStatus(codes.Ok, "transactionsQuery.GetFSAlertRules")
	return rules, nil
}

// Get the Min and Max threshold value
// GetMinMaxValue
// Takes (ctx context.Context, MinUSDThreshold, MaxUSDThreshold string)
// Returns (float64, float64)
//
// Extract the Min and Max threshold value from Rule
// Returns Min and Max value After convert it from string to float64.
func (t *transactionsProcess) ConvertMinMaxValue(ctx context.Context, MinUSDThreshold, MaxUSDThreshold string) (float64, float64) {
	span, labels := common.GenerateSpan("transactionsQuery.GetMinMaxValue", ctx)
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "transactionsQuery.GetMinMaxValue"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "transactionsQuery.GetMinMaxValue"))

	var (
		min float64 = 1.0
		max float64 = 1.0
		err error
	)

	min, err = strconv.ParseFloat(strings.ReplaceAll(MinUSDThreshold, ",", ""), 64)
	if err != nil {
		log.Error("Error transactionsQuery.GetMinMaxValue Could Not parse min value setting defaulting the value to 1.0 %s", err)
		span.SetStatus(codes.Error, err.Error())
	}
	max, err = strconv.ParseFloat(strings.ReplaceAll(MaxUSDThreshold, ",", ""), 64)
	if err != nil {
		log.Error("Error transactionsQuery.GetMinMaxValue Could Not parse max value setting defaulting the value to 10000000.0 %s", err)
		span.SetStatus(codes.Error, err.Error())
	}

	log.EndTimeL(labels, "transactionsQuery.GetMinMaxValue", startTime, nil)
	span.SetStatus(codes.Ok, "transactionsQuery.GetMinMaxValue")
	return min, max
}

// SendTransaction
// Takes (ctx context.Context, threshold float64, wallets []string)
// - threshold : Determine the Amount for Transaction we need to check
// - wallets : array of string that contains all wallets stores in BQ
// Returns []datastruct.BQTransaction
//
// SendTransaction It will fetch the messages from PubSub then check the threshold if it meets the requirement the Transaction will added to Transaction array and send the Transaction to Slack channel.
// Returns []datastruct.BQTransaction
func (t *transactionsProcess) SendTransaction(ctx context.Context, message *datastruct.PubSubMessage) error {
	span, labels := common.GenerateSpan("transactionsQuery.SendTransaction", ctx)
	defer span.End()

	var (
		ethereumTransactionRow datastruct.EthereumTransactionRow
		rules                  []datastruct.WhaleTrackerAlertRules
		err                    error
	)

	span.AddEvent(fmt.Sprintf("Starting %s", "transactionsQuery.SendTransaction"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "transactionsQuery.SendTransaction"))

	cacheRules, found := t.cache.Get(datastruct.CacheKey)
	if found {
		rules = cacheRules.([]datastruct.WhaleTrackerAlertRules)
	} else {
		rules, err = t.GetFSAlertRules(ctx)
		if err != nil {
			log.ErrorL(labels, "")
			span.SetStatus(codes.Error, "Error getting rules from FS")
		}
		t.cache.Set(datastruct.CacheKey, rules, 10*time.Minute)
	}

	if err := json.Unmarshal(message.Message.Data, &ethereumTransactionRow); err != nil {
		log.ErrorL(labels, "Failed to decode message: %s", err)
		return err
	}

	var (
		min float64
		max float64
	)
	for _, rule := range rules {

		if rule.Entity != nil {
			// If Entity does not contain fromAddress or ToAddress then --> continue and check the next Rule
			if !slices.Contains(rule.Entity, ethereumTransactionRow.FromAddress) && !slices.Contains(rule.Entity, ethereumTransactionRow.ToAddress) {
				continue
			}
		}
		min, max = t.ConvertMinMaxValue(ctx, rule.MinUSDThreshold, rule.MaxUSDThreshold)
		if ethereumTransactionRow.ValueUsd >= min && ethereumTransactionRow.ValueUsd <= max {
			// Send the transaction to slack channel
			common.SendSlack(ethereumTransactionRow, rule.Color.Hex)
			return nil
		}

	}

	log.EndTimeL(labels, "transactionsQuery.SendTransaction", startTime, nil)
	span.SetStatus(codes.Ok, "transactionsQuery.SendTransaction")

	return nil
}
