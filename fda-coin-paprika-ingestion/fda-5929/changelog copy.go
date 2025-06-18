package coinPaprikaUtils

import (
	"context"

	"github.com/Forbes-Media/fda-coin-paprika-ingestion/common"
	"github.com/Forbes-Media/go-tools/log"
	"github.com/coinpaprika/coinpaprika-api-go-client/coinpaprika"
	"go.opentelemetry.io/otel/codes"
)

// GetCoinList fetches the list of all coins from CoinPaprika.
// Parameters:
//   - ctx: Context for the operation.
//
// Returns:
//   - A slice of `coinpaprika.Coin` or an error if the operation fails.
func (c *coinPaprikaUtils) GetChangeLogList(ctx context.Context) ([]*coinpaprika.Coin, error) {
	span, labels := common.GenerateSpan("GetChangeLogList", ctx)
	defer span.End()

	span.AddEvent("Starting GetChangeLogList")
	startTime := log.StartTimeL(labels, "Starting GetChangeLogList")

	// Wait for the rate limiter before making the API call.
	err := c.cpRateLimit.Wait(ctx)
	if err != nil {
		span.SetStatus(codes.Error, "Rate limiter error")
		log.EndTimeL(labels, "Rate limiter error", startTime, err)
		return nil, err
	}

	coins, err := c.paprikaClient.Coins.List()
	if err != nil {
		span.SetStatus(codes.Error, "GetChangeLogList: Failed to fetch coin list")
		log.EndTimeL(labels, "GetChangeLogList: Failed to fetch coin list", startTime, err)
		return nil, err
	}

	span.SetStatus(codes.Ok, "Success")
	log.EndTimeL(labels, "GetChangeLogList Finished", startTime, nil)
	return coins, nil
}
