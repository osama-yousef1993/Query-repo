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
)

func (c *coinPaprikaUtils) GetHistoricalOHLV(ctx context.Context, coinsID []string) (map[string][]*coinpaprika.OHLCVEntry, error) {
	span, labels := common.GenerateSpan("GetHistoricalOHLV", ctx)
	defer span.End()

	span.AddEvent("Stating GetHistoricalOHLV")
	startTime := log.StartTimeL(labels, "Stating GetHistoricalOHLV")

	var (
		historical   = make(map[string][]*coinpaprika.OHLCVEntry)
		wg           sync.WaitGroup
		mu           = &sync.Mutex{} // Mutex to protect shared slice.
		maxRetries   = 3
		throttleChan = make(chan bool, 20)
		// Get Quote Price and Volume for BTC, ETH and USD
		options = &coinpaprika.HistoricalOHLCVOptions{Start: time.Now(), Quote: "BTC,ETH,USD"}
	)

	for _, coin := range coinsID {
		throttleChan <- true
		wg.Add(1)
		go func(id string, options coinpaprika.HistoricalOHLCVOptions) {
			for attempt := 1; attempt <= maxRetries; attempt++ {

				err := c.cpRateLimit.Wait(ctx)
				if err != nil {
					span.SetStatus(codes.Error, "GetHistoricalOHLV: Rate limiter error")
					log.EndTimeL(labels, "GetHistoricalOHLV: Rate limiter error", startTime, err)
					<-throttleChan
					wg.Done()
					return
				}
				ohlcv, err := c.paprikaClient.Coins.GetHistoricalOHLCVByCoinID(id, &options)
				if err == nil {
					mu.Lock()
					historical[id] = ohlcv
					mu.Unlock()
					<-throttleChan
					wg.Done()
					return
				}
				log.Debug("GetHistoricalOHLV Attempt %d: Failed to fetch markets %s: %v", attempt, id, err)
			}
			span.SetStatus(codes.Error, fmt.Sprintf("GetHistoricalOHLV Failed to fetch %s after retries", id))
			<-throttleChan
			wg.Done()
		}(coin, *options)
	}
	wg.Wait()
	log.EndTimeL(labels, "", startTime, nil)
	span.SetStatus(codes.Ok, "")
	return historical, nil

}
