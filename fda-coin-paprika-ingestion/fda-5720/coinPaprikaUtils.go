package coinPaprikaUtils

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Forbes-Media/fda-coin-paprika-ingestion/common"
	"github.com/Forbes-Media/go-tools/log"
	"github.com/coinpaprika/coinpaprika-api-go-client/coinpaprika"
	"go.opentelemetry.io/otel/codes"
	"golang.org/x/time/rate"
)

type CoinPaprikaUtils interface {
	// GetCoinList fetches the list of all coins from CoinPaprika.
	GetCoinList(ctx context.Context) ([]*coinpaprika.Coin, error)

	// GetCoinByID fetches details of specific coins by their IDs from CoinPaprika.
	GetCoinByID(ctx context.Context, coinsID []string) ([]*coinpaprika.Coin, error)
}

type coinPaprikaUtils struct {
	cpRateLimit   *rate.Limiter
	paprikaClient *coinpaprika.Client
}

// NewCoinPaprikaUtils initializes a new instance of CoinPaprikaUtils with a rate limiter.
func NewCoinPaprikaUtils(rateLimit int) CoinPaprikaUtils {
	cpRateLimit := rate.NewLimiter(rate.Every(time.Minute/time.Duration(rateLimit-1)), 1)
	paprikaClient := coinpaprika.NewClient(nil)
	return &coinPaprikaUtils{cpRateLimit: cpRateLimit, paprikaClient: paprikaClient}
}

// GetCoinList fetches the list of all coins from CoinPaprika.
// Parameters:
//   - ctx: Context for the operation.
//
// Returns:
//   - A slice of `coinpaprika.Coin` or an error if the operation fails.
func (c coinPaprikaUtils) GetCoinList(ctx context.Context) ([]*coinpaprika.Coin, error) {
	span, labels := common.GenerateSpan("GetCoinList", ctx)
	defer span.End()

	span.AddEvent("Starting GetCoinList")
	startTime := log.StartTimeL(labels, "Starting GetCoinList")

	// Wait for the rate limiter before making the API call.
	err := c.cpRateLimit.Wait(ctx)
	if err != nil {
		span.SetStatus(codes.Error, "Rate limiter error")
		log.EndTimeL(labels, "Rate limiter error", startTime, err)
		return nil, err
	}

	coins, err := c.paprikaClient.Coins.List()
	if err != nil {
		span.SetStatus(codes.Error, "Failed to fetch coin list")
		log.EndTimeL(labels, "Failed to fetch coin list", startTime, err)
		return nil, err
	}

	span.SetStatus(codes.Ok, "Success")
	log.EndTimeL(labels, "GetCoinList Finished", startTime, nil)
	return coins, nil
}

// GetCoinByID fetches details of specific coins by their IDs from CoinPaprika.
// Parameters:
//   - ctx: Context for the operation.
//   - coinsID: A slice of coin IDs to fetch details for.
//
// Returns:
//   - A slice of `coinpaprika.Coin` or an error if the operation fails.
func (c coinPaprikaUtils) GetCoinByID(ctx context.Context, coinsID []string) ([]*coinpaprika.Coin, error) {
	span, labels := common.GenerateSpan("GetCoinByID", ctx)
	defer span.End()

	span.AddEvent("Starting GetCoinByID")
	startTime := log.StartTimeL(labels, "Starting GetCoinByID")

	var (
		coins      []*coinpaprika.Coin
		wg         sync.WaitGroup
		mu         sync.Mutex // Mutex to protect shared slice.
		maxRetries = 3
	)

	// Iterate over coin IDs and fetch details concurrently.
	for _, coinID := range coinsID {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()

			for attempt := 1; attempt <= maxRetries; attempt++ {
				// Wait for the rate limiter before making the API call.
				err := c.cpRateLimit.Wait(ctx)
				if err != nil {
					log.Debug("Rate limiter error on attempt %d for coin %s: %v", attempt, id, err)
					continue
				}

				coin, err := c.paprikaClient.Coins.GetByID(id)
				if err == nil {
					// Successfully fetched coin data, append it to the list.
					mu.Lock()
					coins = append(coins, coin)
					mu.Unlock()
					return
				}

				log.Debug("Attempt %d: Failed to fetch coin %s: %v", attempt, id, err)
				time.Sleep(time.Duration(attempt) * time.Second) // Exponential backoff.
			}

			// Log and record the failure after retries.
			log.Error("Failed to fetch coin %s after %d attempts", id, maxRetries)
			span.SetStatus(codes.Error, fmt.Sprintf("Failed to fetch %s after retries", id))
		}(coinID)
	}

	// Wait for all goroutines to complete.
	wg.Wait()

	span.SetStatus(codes.Ok, "Success")
	log.EndTimeL(labels, "GetCoinByID Finished", startTime, nil)
	return coins, nil
}
