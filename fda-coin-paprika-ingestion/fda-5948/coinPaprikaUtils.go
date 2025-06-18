package coinPaprikaUtils

import (
	"context"
	"time"

	"github.com/Forbes-Media/fda-coin-paprika-ingestion/datastruct"
	"github.com/coinpaprika/coinpaprika-api-go-client/coinpaprika"
	"golang.org/x/time/rate"
)

type CoinPaprikaUtils interface {
	// GetCoinList fetches the list of all coins from CoinPaprika.
	GetCoinList(ctx context.Context) ([]*coinpaprika.Coin, error)

	// GetCoinByID fetches details of specific coins by their IDs from CoinPaprika.
	GetCoinByID(ctx context.Context, coinsID []string) ([]*coinpaprika.Coin, map[string]string, error)

	// GetExchangesList fetches the list of all exchanges from CoinPaprika.
	GetExchangesList(ctx context.Context) ([]*coinpaprika.Exchange, error)

	// GetTagsList fetches the list of all tags from CoinPaprika.
	GetTagsList(ctx context.Context) ([]*coinpaprika.Tag, error)

	// GetExchangeMarkets fetches markets of specific exchange by their IDs from CoinPaprika.
	GetExchangeMarkets(ctx context.Context, exchanges map[string]datastruct.Exchanges) (map[string][]*coinpaprika.Market, error)

	// GetHistoricalOHLV fetches historical OHLCV data for specific coins from CoinPaprika.
	GetHistoricalOHLV(ctx context.Context, coinsID []string) (map[string][]*coinpaprika.OHLCVEntry, error)
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
