package store

import (
	"bytes"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Forbes-Media/fda-nomics-ingestion/log"
	"github.com/Forbes-Media/fda-nomics-ingestion/models"
	"github.com/Forbes-Media/fda-nomics-ingestion/service"
	"github.com/Forbes-Media/nomics-client/nomics"
	_ "github.com/lib/pq"
)

var (
	pg           *sql.DB
	DBClientOnce sync.Once
)

func PGConnect() *sql.DB {
	if pg == nil {
		var err error
		DBClientOnce.Do(func() {
			connectionString := fmt.Sprintf("port=%s host=%s user=%s password=%s dbname=%s sslmode=%s", os.Getenv("DB_PORT"), os.Getenv("DB_HOST"), os.Getenv("DB_USER"), os.Getenv("DB_PASSWORD"), os.Getenv("DB_NAME"), os.Getenv("DB_SSLMODE"))

			pg, err = sql.Open("postgres", connectionString)

			if err != nil {
				log.Error("%s", err)
			}

			connectionError := pg.Ping()

			if connectionError != nil {
				log.Error("%s", connectionError)
			}

		})
	}

	return pg
}

func PgUpsertTickersMetadataBulk(tickersMetadata *[]nomics.Metadata) error {
	log.Info("START PgUpsertTickersMetadataBulk()")
	var (
		start           = time.Now()
		valueString     = make([]string, 0, len(*tickersMetadata))
		valueArgs       = make([]interface{}, 0, len(*tickersMetadata)*25)
		upsertStatement = "ON CONFLICT (id) DO UPDATE SET original_symbol = EXCLUDED.original_symbol, name = EXCLUDED.name, description = EXCLUDED.description, website_url = EXCLUDED.website_url, logo_url = EXCLUDED.logo_url, blog_url = EXCLUDED.blog_url, discord_url = EXCLUDED.discord_url, facebook_url = EXCLUDED.facebook_url, github_url = EXCLUDED.github_url, medium_url  = EXCLUDED.medium_url, reddit_url = EXCLUDED.reddit_url, telegram_url = EXCLUDED.telegram_url, twitter_url = EXCLUDED.twitter_url, whitepaper_url = EXCLUDED.whitepaper_url, youtube_url = EXCLUDED.youtube_url, linkedin_url = EXCLUDED.linkedin_url, bitcointalk_url = EXCLUDED.bitcointalk_url, blockexplorer_url = EXCLUDED.blockexplorer_url , replaced_by = EXCLUDED.replaced_by, markets_count = EXCLUDED.markets_count, cryptocontrol_coin_id = EXCLUDED.cryptocontrol_coin_id, used_for_pricing = EXCLUDED.used_for_pricing, platform_currency_id = EXCLUDED.platform_currency_id, platform_contract_address = EXCLUDED.platform_contract_address, last_updated = Now()"
	)

	pg := PGConnect()

	var i = 0 //used for argument positions
	for y := 0; y < len(*tickersMetadata); y++ {
		var tm = (*tickersMetadata)[y]
		var valString = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)", i*25+1, i*25+2, i*25+3, i*25+4, i*25+5, i*25+6, i*25+7, i*25+8, i*25+9, i*25+10, i*25+11, i*25+12, i*25+13, i*25+14, i*25+15, i*25+16, i*25+17, i*25+18, i*25+19, i*25+20, i*25+21, i*25+22, i*25+23, i*25+24, i*25+25)

		valueString = append(valueString, valString)
		valueArgs = append(valueArgs, tm.ID)
		valueArgs = append(valueArgs, tm.OriginalSymbol)
		valueArgs = append(valueArgs, tm.Name)
		valueArgs = append(valueArgs, tm.Description)
		valueArgs = append(valueArgs, tm.WebsiteURL)
		valueArgs = append(valueArgs, tm.LogoURL)
		valueArgs = append(valueArgs, tm.BlogURL)
		valueArgs = append(valueArgs, tm.DiscordURL)
		valueArgs = append(valueArgs, tm.FacebookURL)
		valueArgs = append(valueArgs, tm.GithubURL)
		valueArgs = append(valueArgs, tm.MediumURL)
		valueArgs = append(valueArgs, tm.RedditURL)
		valueArgs = append(valueArgs, tm.TelegramURL)
		valueArgs = append(valueArgs, tm.TwitterURL)
		valueArgs = append(valueArgs, tm.WhitepaperURL)
		valueArgs = append(valueArgs, tm.YoutubeURL)
		valueArgs = append(valueArgs, tm.LinkedinURL)
		valueArgs = append(valueArgs, tm.BitcointalkURL)
		valueArgs = append(valueArgs, tm.BlockExplorerURL)
		valueArgs = append(valueArgs, tm.ReplacedBy)
		valueArgs = append(valueArgs, tm.MarketsCount)
		valueArgs = append(valueArgs, tm.CryptocontrolCoinID)
		valueArgs = append(valueArgs, tm.UsedForPricing)
		valueArgs = append(valueArgs, tm.PlatformCurrencyID)
		valueArgs = append(valueArgs, tm.PlatformContractAddress)

		i++
		if len(valueArgs) >= 65000 || y == len(*tickersMetadata)-1 {
			query := fmt.Sprintf("INSERT INTO nomics_ticker_metadata VALUES %s %s", strings.Join(valueString, ","), upsertStatement)
			_, inserterExchangeError := pg.Exec(query, valueArgs...)
			if inserterExchangeError != nil {
				log.Error("PgUpsertTickersMetadataBulk() error: %s ", inserterExchangeError)
				return inserterExchangeError
			}
			valueString = make([]string, 0, len(*tickersMetadata))
			valueArgs = make([]interface{}, 0, len(*tickersMetadata)*25)
			i = 0

		}

	}
	log.Info("END PgUpsertTickersMetadataBulk() totatTime:%.2fs", time.Since(start).Seconds())
	return nil

}

// Updates the Nomics Asset (Ticker) Metadata for the given currency.
func PgUpsertTickersMetadata(tickersMetadata *[]nomics.Metadata) error {
	pg := PGConnect()
	insertQuery := fmt.Sprintf("INSERT INTO nomics_ticker_metadata VALUES %s %s", StructIterator(nomics.Metadata{}), " ")

	for _, tickerMetaData := range *tickersMetadata {
		insertStatementTickerMetaData := insertQuery + "ON CONFLICT (id) DO UPDATE SET original_symbol = $2, name = $3, description = $4, website_url = $5, logo_url = $6, blog_url = $7, discord_url = $8, facebook_url = $9, github_url = $10, medium_url  = $11, reddit_url = $12, telegram_url = $13, twitter_url = $14, whitepaper_url = $15, youtube_url = $16, linkedin_url = $17, bitcointalk_url = $18, blockExplorer_url = $19 , replaced_by = $20, markets_count = $21, cryptocontrol_coin_id = $22, used_for_pricing = $23, platform_currency_id = $24, platform_contract_address = $25, last_updated = Now()"

		_, inserterError := pg.Exec(insertStatementTickerMetaData, tickerMetaData.ID, tickerMetaData.OriginalSymbol, tickerMetaData.Name, tickerMetaData.Description, tickerMetaData.WebsiteURL, tickerMetaData.LogoURL, tickerMetaData.BlogURL, tickerMetaData.DiscordURL, tickerMetaData.FacebookURL, tickerMetaData.GithubURL, tickerMetaData.MediumURL, tickerMetaData.RedditURL, tickerMetaData.TelegramURL, tickerMetaData.TwitterURL, tickerMetaData.WhitepaperURL, tickerMetaData.YoutubeURL, tickerMetaData.LinkedinURL, tickerMetaData.BitcointalkURL, tickerMetaData.BlockExplorerURL, tickerMetaData.ReplacedBy, tickerMetaData.MarketsCount, tickerMetaData.CryptocontrolCoinID, tickerMetaData.UsedForPricing, tickerMetaData.PlatformCurrencyID, tickerMetaData.PlatformContractAddress)

		if inserterError != nil {
			return inserterError
		}
	}
	return nil
}

func PgUpsertExchangeMetadataBulk(exchangesMetadata *[]nomics.ExchangeMetadata) error {
	var (
		labels      = make(map[string]string)
		start       = time.Now()
		valueString = make([]string, 0, len(*exchangesMetadata))
		valueArgs   = make([]interface{}, 0, len(*exchangesMetadata)*26)
		onConflict  = "ON CONFLICT (id) DO UPDATE SET capability_markets = EXCLUDED.capability_markets, capability_trades = EXCLUDED.capability_trades, capability_trades_by_timestamp = EXCLUDED.capability_trades_by_timestamp, capability_trades_snapshot = EXCLUDED.capability_trades_snapshot, capability_orders_snapshot = EXCLUDED.capability_orders_snapshot, capability_candles = EXCLUDED.capability_candles, capability_ticker = EXCLUDED.capability_ticker, integrated = EXCLUDED.integrated, name = EXCLUDED.name, description = EXCLUDED.description, location = EXCLUDED.location, logo_url = EXCLUDED.logo_url, website_url = EXCLUDED.website_url, fees_url = EXCLUDED.fees_url, twitter_url = EXCLUDED.twitter_url, facebook_url = EXCLUDED.facebook_url, reddit_url = EXCLUDED.reddit_url, chat_url = EXCLUDED.chat_url, blog_url = EXCLUDED.blog_url, year = EXCLUDED.year, transparency_grade = EXCLUDED.transparency_grade, order_books_interval = EXCLUDED.order_books_interval, centralized = EXCLUDED.centralized, decentralized = EXCLUDED.decentralized, replaced_by = EXCLUDED.replaced_by,last_updated = Now()"
	)
	labels["metadata"] = "PgUpsertExchangeMetadataBulk"
	labels["postgres"] = "PgUpsertExchangeMetadataBulk"
	log.InfoL(labels, "START PgUpsertExchangeMetadataBulk()")

	pg := PGConnect()

	//_, inserterError :=   exchangeMetaData.OrderBooksInterval, exchangeMetaData.Centralized, exchangeMetaData.Decentralized, exchangeMetaData.ReplacedBy)
	var i = 0 //used for argument positions
	for y := 0; y < len(*exchangesMetadata); y++ {
		var em = (*exchangesMetadata)[y]
		var valString = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)", i*26+1, i*26+2, i*26+3, i*26+4, i*26+5, i*26+6, i*26+7, i*26+8, i*26+9, i*26+10, i*26+11, i*26+12, i*26+13, i*26+14, i*26+15, i*26+16, i*26+17, i*26+18, i*26+19, i*26+20, i*26+21, i*26+22, i*26+23, i*26+24, i*26+25, i*26+26)

		valueString = append(valueString, valString)
		valueArgs = append(valueArgs, em.ID)
		valueArgs = append(valueArgs, em.CapabilityMarkets)
		valueArgs = append(valueArgs, em.CapabilityTrades)
		valueArgs = append(valueArgs, em.CapabilityTradesByTimestamp)
		valueArgs = append(valueArgs, em.CapabilityTradesSnapshot)
		valueArgs = append(valueArgs, em.CapabilityOrdersSnapshot)
		valueArgs = append(valueArgs, em.CapabilityCandles)
		valueArgs = append(valueArgs, em.CapabilityTicker)
		valueArgs = append(valueArgs, em.Integrated)
		valueArgs = append(valueArgs, em.Name)
		valueArgs = append(valueArgs, em.Description)
		valueArgs = append(valueArgs, em.Location)
		valueArgs = append(valueArgs, em.LogoURL)
		valueArgs = append(valueArgs, em.WebsiteURL)
		valueArgs = append(valueArgs, em.FeesURL)
		valueArgs = append(valueArgs, em.TwitterURL)
		valueArgs = append(valueArgs, em.FacebookURL)
		valueArgs = append(valueArgs, em.RedditURL)
		valueArgs = append(valueArgs, em.ChatURL)
		valueArgs = append(valueArgs, em.BlogURL)
		valueArgs = append(valueArgs, service.ConvertToInt(em.Year))
		valueArgs = append(valueArgs, em.TransparencyGrade)
		valueArgs = append(valueArgs, em.OrderBooksInterval)
		valueArgs = append(valueArgs, em.Centralized)
		valueArgs = append(valueArgs, em.Decentralized)
		valueArgs = append(valueArgs, em.ReplacedBy)

		i++
		if len(valueArgs) >= 65000 || y == len(*exchangesMetadata)-1 {
			query := fmt.Sprintf("INSERT INTO nomics_exchange_metadata VALUES %s %s", strings.Join(valueString, ","), onConflict)
			_, inserterExchangeError := pg.Exec(query, valueArgs...)
			if inserterExchangeError != nil {
				log.ErrorL(labels, "PgUpsertExchangeMetadataBulk() error: %s ", inserterExchangeError)
				return inserterExchangeError
			}
			valueString = make([]string, 0, len(*exchangesMetadata))
			valueArgs = make([]interface{}, 0, len(*exchangesMetadata)*26)
			i = 0

		}

	}

	log.InfoL(labels, "END PgUpsertExchangeMetadataBulk() totalTime:%.2fs", time.Since(start).Seconds())
	return nil

}

// Updates the Nomics Exchnage Metadata for the given currency.
func PgUpsertExchangeMetadata(exchangesMetadata *[]nomics.ExchangeMetadata) error {
	pg := PGConnect()

	insertQuery := fmt.Sprintf("INSERT INTO nomics_exchange_metadata VALUES %s %s", StructIterator(nomics.ExchangeMetadata{}), " ")

	for _, exchangeMetaData := range *exchangesMetadata {
		insertStatementExchangeMetadata := insertQuery + "ON CONFLICT (id) DO UPDATE SET capability_markets = $2, capability_trades = $3, capability_trades_by_timestamp = $4, capability_trades_snapshot = $5, capability_orders_snapshot = $6, capability_candles = $7, capability_ticker = $8, integrated = $9, name = $10, description  = $11, location = $12, logo_url = $13, website_url = $14, fees_url = $15, twitter_url = $16, facebook_url = $17, reddit_url = $18, chat_url = $19 , blog_url = $20, year = $21, transparency_grade = $22, order_books_interval = $23, centralized = $24, decentralized = $25, replaced_by = $26,last_updated = Now()"

		_, inserterError := pg.Exec(insertStatementExchangeMetadata, exchangeMetaData.ID, exchangeMetaData.CapabilityMarkets, exchangeMetaData.CapabilityTrades, exchangeMetaData.CapabilityTradesByTimestamp, exchangeMetaData.CapabilityTradesSnapshot, exchangeMetaData.CapabilityOrdersSnapshot, exchangeMetaData.CapabilityCandles, exchangeMetaData.CapabilityTicker, exchangeMetaData.Integrated, exchangeMetaData.Name, exchangeMetaData.Description, exchangeMetaData.Location, exchangeMetaData.LogoURL, exchangeMetaData.WebsiteURL, exchangeMetaData.FeesURL, exchangeMetaData.TwitterURL, exchangeMetaData.FacebookURL, exchangeMetaData.RedditURL, exchangeMetaData.ChatURL, exchangeMetaData.BlogURL, exchangeMetaData.Year, exchangeMetaData.TransparencyGrade, exchangeMetaData.OrderBooksInterval, exchangeMetaData.Centralized, exchangeMetaData.Decentralized, exchangeMetaData.ReplacedBy)

		if inserterError != nil {
			return inserterError
		}
	}
	return nil
}

func InsertExchangeHighlight(exchangeHighlightData models.ExchangeHighlights) error {
	pg := PGConnect()

	insertQuery := "INSERT INTO nomics_exchange_highlight VALUES ($1,$2,$3,$4,$5,$6,$7,$8)"

	_, inserterError := pg.Exec(insertQuery, exchangeHighlightData.Exchange, exchangeHighlightData.TotalVolume, exchangeHighlightData.NumTrades, exchangeHighlightData.NumMarkets, exchangeHighlightData.NumCryptoMarkets, exchangeHighlightData.NumFiatMarkets, exchangeHighlightData.NumCryptoCurrencies, exchangeHighlightData.NumFiatMarkets)

	if inserterError != nil {
		return inserterError
	}

	InsertExchangeHighlightDominance(exchangeHighlightData.Exchange, "nomics_exchange_highlight_tqd", exchangeHighlightData.TopQuoteDominance)
	InsertExchangeHighlightDominance(exchangeHighlightData.Exchange, "nomics_exchange_highlight_tcqd", exchangeHighlightData.TopCryptoQuoteDominance)
	InsertExchangeHighlightDominance(exchangeHighlightData.Exchange, "nomics_exchange_highlight_tfqd", exchangeHighlightData.TopFiatQuoteDominance)
	InsertExchangeHighlightTop(exchangeHighlightData.Exchange, "nomics_exchange_highlight_tm", exchangeHighlightData.TopMarkets)
	InsertExchangeHighlightTop(exchangeHighlightData.Exchange, "nomics_exchange_highlight_tvg", exchangeHighlightData.TopVolumeGainers)
	InsertExchangeHighlightTop(exchangeHighlightData.Exchange, "nomics_exchange_highlight_tvd", exchangeHighlightData.TopVolumeDeltas)

	return nil
}

func InsertExchangeHighlightDominance(exchange string, tableName string, dominanceData []models.Dominance) {
	pg := PGConnect()

	insertQuery := fmt.Sprintf("INSERT INTO %s VALUES ($1,$2,$3,$4)", tableName)

	for _, dominance := range dominanceData {
		_, inserterError := pg.Exec(insertQuery, exchange, dominance.Symbol, dominance.Name, dominance.DominancePct)

		if inserterError != nil {
			errMsg := fmt.Sprintf("Error inserting Currencies Ticker times on %s %s", tableName, inserterError)
			log.Error(errMsg)
		}
	}
}

func InsertExchangeHighlightTop(exchange string, tableName string, topData []models.Top) error {
	pg := PGConnect()

	insertQuery := fmt.Sprintf("INSERT INTO %s VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)", tableName)

	for _, top := range topData {
		_, inserterError := pg.Exec(insertQuery, exchange, top.Market, top.Base, top.BaseName, top.Quote, top.QuoteName, top.Volume, top.VolumeChange, top.VolumeChangePct)

		if inserterError != nil {
			return inserterError
		}
	}
	return nil
}

func StructIterator(structData interface{}) string {
	valueOf := reflect.ValueOf(structData)

	values := make([]interface{}, valueOf.NumField())
	var output bytes.Buffer

	output.WriteString("(")
	for index := range values {
		if index+1 != len(values) {
			output.WriteString("$" + strconv.Itoa(index+1) + ",")
		} else {
			output.WriteString("$" + strconv.Itoa(index+1))
		}
	}
	output.WriteString(")")

	return output.String()
}

func BuildInsertQuery(tableName string) string {
	query := fmt.Sprintf("INSERT INTO %s VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22)", tableName)
	return query
}

func BulkInsertExchangeMarketsTickerDataIntervals(tickers []models.PGExchangeMarketsTicker, table string) error {
	pg := PGConnect()
	valueString := make([]string, 0, len(tickers))
	valueArgs := make([]interface{}, 0, len(tickers)*8)

	tableName := fmt.Sprintf("nomics_exchange_market_ticker_%s", table)

	var i = 0 //used for argument positions

	for y := 0; y < len(tickers); y++ {
		var exchange = tickers[y]
		var valString = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)", i*8+1, i*8+2, i*8+3, i*8+4, i*8+5, i*8+6, i*8+7, i*8+8)

		valueString = append(valueString, valString)
		valueArgs = append(valueArgs, exchange.Base)
		valueArgs = append(valueArgs, exchange.OneD.Volume)
		valueArgs = append(valueArgs, exchange.OneD.VolumeBase)
		valueArgs = append(valueArgs, exchange.OneD.VolumeBaseChange)
		valueArgs = append(valueArgs, exchange.OneD.Trades)
		valueArgs = append(valueArgs, exchange.OneD.TradesChange)
		valueArgs = append(valueArgs, exchange.OneD.PriceChange)
		valueArgs = append(valueArgs, exchange.OneD.PriceQuoteChange)

		i++
		if len(valueArgs) >= 65000 || y == len(tickers)-1 {

			query := fmt.Sprintf("INSERT INTO %s VALUES %s", tableName, strings.Join(valueString, ","))
			_, inserterExchangeError := pg.Exec(query, valueArgs...)
			if inserterExchangeError != nil {
				log.Error("error: %s", inserterExchangeError)
				return inserterExchangeError
			}
			valueString = make([]string, 0, len(tickers))
			valueArgs = make([]interface{}, 0, len(tickers)*8)
			i = 0

		}

	}

	return nil
}

func BuildBulkInsertQuery(tableName string, tickers []models.PGExchangeMarketsTicker) error {

	pg := PGConnect()
	valueString := make([]string, 0, len(tickers))
	valueArgs := make([]interface{}, 0, len(tickers)*22)

	var i = 0 //used for argument positions
	for y := 0; y < len(tickers); y++ {
		var exchange = tickers[y]
		var valString = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)", i*22+1, i*22+2, i*22+3, i*22+4, i*22+5, i*22+6, i*22+7, i*22+8, i*22+9, i*22+10, i*22+11, i*22+12, i*22+13, i*22+14, i*22+15, i*22+16, i*22+17, i*22+18, i*22+19, i*22+20, i*22+21, i*22+22)

		valueString = append(valueString, valString)
		valueArgs = append(valueArgs, exchange.Base)
		valueArgs = append(valueArgs, exchange.Exchange)
		valueArgs = append(valueArgs, exchange.Market)
		valueArgs = append(valueArgs, exchange.Type)
		valueArgs = append(valueArgs, exchange.SubType)
		valueArgs = append(valueArgs, exchange.Aggregated)
		valueArgs = append(valueArgs, exchange.PriceExclude)
		valueArgs = append(valueArgs, exchange.VolumeExclude)
		valueArgs = append(valueArgs, exchange.Quote)

		valueArgs = append(valueArgs, exchange.BaseSymbol)
		valueArgs = append(valueArgs, exchange.QuoteSymbol)
		valueArgs = append(valueArgs, exchange.Price)
		valueArgs = append(valueArgs, exchange.PriceQuote)

		valueArgs = append(valueArgs, exchange.VolumeUsd)
		valueArgs = append(valueArgs, exchange.LastUpdated)
		valueArgs = append(valueArgs, exchange.Status)
		valueArgs = append(valueArgs, exchange.Weight)

		valueArgs = append(valueArgs, exchange.FirstTrade)
		valueArgs = append(valueArgs, exchange.FirstCandle)
		valueArgs = append(valueArgs, exchange.FirstOrderBook)
		valueArgs = append(valueArgs, exchange.Timestamp)
		valueArgs = append(valueArgs, exchange.TotalTrades)

		i++
		if len(valueArgs) >= 65000 || y == len(tickers)-1 {

			query := fmt.Sprintf("INSERT INTO %s VALUES %s", tableName, strings.Join(valueString, ","))
			_, inserterExchangeError := pg.Exec(query, valueArgs...)
			if inserterExchangeError != nil {
				log.Error("BuildBulkInsertQuery() error: %s ", inserterExchangeError)
				return inserterExchangeError
			}
			valueString = make([]string, 0, len(tickers))
			valueArgs = make([]interface{}, 0, len(tickers)*22)
			i = 0

		}

	}
	return nil
}

func UpsertTickers(tickerData []models.PGAssets) error {

	pg := PGConnect()

	valueString := make([]string, 0, len(tickerData))

	for y := 0; y < len(tickerData); y++ {
		var ticker = tickerData[y]
		var valString = fmt.Sprintf("('%s','%s','%s','%s','%s','%s')::assetsValues", ticker.ID, ticker.Currency, ticker.Symbol, strings.Replace(ticker.Name, "'", "`", -1), ticker.LogoURL, ticker.Status)

		valueString = append(valueString, valString)
	}
	var lastUpdated time.Time
	allData := strings.Join(valueString, ",")
	storedPro := fmt.Sprintf("CALL upsertAssets(ARRAY[%s])", allData)
	result, inserterTickerError := pg.Query(storedPro)

	for result.Next() {
		if err := result.Scan(&lastUpdated); err != nil {
			log.Info("%s", err)
		}
	}
	if inserterTickerError != nil {
		log.Error("error: %s", inserterTickerError)
		return inserterTickerError
	}

	if currenciesTickersError := InsertCurrenciesTickers(tickerData, lastUpdated); currenciesTickersError != nil {
		return currenciesTickersError
	}

	return nil
}

func InsertCurrenciesTickers(tickers []models.PGAssets, lastUpdated time.Time) error {

	pg := PGConnect()

	var (
		valueStringCurrenciesTickers = make([]string, 0, len(tickers))
		valueStringOneHour = make([]string, 0, len(tickers))
		valueStringOneDay = make([]string, 0, len(tickers))
		valueStringSevenDays = make([]string, 0, len(tickers))
		valueStringThirtyDays = make([]string, 0, len(tickers))
		valueStringOneYear = make([]string, 0, len(tickers))
		valueStringYTD = make([]string, 0, len(tickers))
	)


	for y := 0; y < len(tickers); y++ {
		var ticker = tickers[y]
		var valStringCurrenciesTickers = fmt.Sprintf("('%s','%s',%d,'%s','%s','%s',%d,%d,%d,%d,%d,%d,%d,%d,'%s','%s','%s','%s',%d,%d,%d,'%s','%s')::nomicsCurrenciesTickers", ticker.ID, lastUpdated.Format("2006-01-02 15:04:05"), ticker.Price, ticker.Status, ticker.PriceDate.Format("2006-01-02 15:04:05"), ticker.PriceTimestamp.Format("2006-01-02 15:04:05") , ticker.CirculatingSupply, ticker.MaxSupply, ticker.MarketCap, ticker.TransparentMarketCap, ticker.MarketCapDominance, ticker.NumExchanges, ticker.NumPairs, ticker.NumPairsUnmapped, ticker.FirstCandle.Format("2006-01-02 15:04:05"), ticker.FirstTrade.Format("2006-01-02 15:04:05"), ticker.FirstOrderBook.Format("2006-01-02 15:04:05"), ticker.FirstPricedAt.Format("2006-01-02 15:04:05"), ticker.Rank, ticker.RankDelta, ticker.High, ticker.HighTimestamp.Format("2006-01-02 15:04:05"), ticker.PlatformCurrency)
		valueStringCurrenciesTickers = append(valueStringCurrenciesTickers, valStringCurrenciesTickers)

		var valStringOneHour = fmt.Sprintf("('%s','%s',%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,json('%v'))::nomicsCurrenciesTickersIntervals", ticker.ID, lastUpdated.Format("2006-01-02 15:04:05"), ticker.OneH.Volume, ticker.OneH.PriceChange, ticker.OneH.PriceChangePct, ticker.OneH.VolumeChange, ticker.OneH.VolumeChangePct, ticker.OneH.MarketCapChange, ticker.OneH.MarketCapChangePct, ticker.OneH.TransparentMarketCapChange, ticker.OneH.TransparentMarketCapChangePct, ticker.OneH.VolumeTransparencyGrade, ticker.OneH.VolumeTransparency)
		valueStringOneHour = append(valueStringOneHour, valStringOneHour)

		var valStringOneDay = fmt.Sprintf("('%s','%s',%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,json('%v'))::nomicsCurrenciesTickersIntervals", ticker.ID, lastUpdated.Format("2006-01-02 15:04:05"), ticker.OneD.Volume, ticker.OneD.PriceChange, ticker.OneD.PriceChangePct, ticker.OneD.VolumeChange, ticker.OneD.VolumeChangePct, ticker.OneD.MarketCapChange, ticker.OneD.MarketCapChangePct, ticker.OneD.TransparentMarketCapChange, ticker.OneD.TransparentMarketCapChangePct, ticker.OneD.VolumeTransparencyGrade, ticker.OneD.VolumeTransparency)
		valueStringOneDay = append(valueStringOneHour, valStringOneDay)

		var valStringSevenDays = fmt.Sprintf("('%s','%s',%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,json('%v'))::nomicsCurrenciesTickersIntervals", ticker.ID, lastUpdated.Format("2006-01-02 15:04:05"), ticker.SevenD.Volume, ticker.SevenD.PriceChange, ticker.SevenD.PriceChangePct, ticker.SevenD.VolumeChange, ticker.SevenD.VolumeChangePct, ticker.SevenD.MarketCapChange, ticker.SevenD.MarketCapChangePct, ticker.SevenD.TransparentMarketCapChange, ticker.SevenD.TransparentMarketCapChangePct, ticker.SevenD.VolumeTransparencyGrade, ticker.SevenD.VolumeTransparency)
		valueStringSevenDays = append(valueStringOneHour, valStringSevenDays)

		var valStringThirtyDays = fmt.Sprintf("('%s','%s',%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,json('%v'))::nomicsCurrenciesTickersIntervals", ticker.ID, lastUpdated.Format("2006-01-02 15:04:05"), ticker.Three0D.Volume, ticker.Three0D.PriceChange, ticker.Three0D.PriceChangePct, ticker.Three0D.VolumeChange, ticker.Three0D.VolumeChangePct, ticker.Three0D.MarketCapChange, ticker.Three0D.MarketCapChangePct, ticker.Three0D.TransparentMarketCapChange, ticker.Three0D.TransparentMarketCapChangePct, ticker.Three0D.VolumeTransparencyGrade, ticker.Three0D.VolumeTransparency)
		valueStringThirtyDays = append(valueStringOneHour, valStringThirtyDays)

		var valStringOneYear = fmt.Sprintf("('%s','%s',%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,json('%v'))::nomicsCurrenciesTickersIntervals", ticker.ID, lastUpdated.Format("2006-01-02 15:04:05"), ticker.Three65D.Volume, ticker.Three65D.PriceChange, ticker.Three65D.PriceChangePct, ticker.Three65D.VolumeChange, ticker.Three65D.VolumeChangePct, ticker.Three65D.MarketCapChange, ticker.Three65D.MarketCapChangePct, ticker.Three65D.TransparentMarketCapChange, ticker.Three65D.TransparentMarketCapChangePct, ticker.Three65D.VolumeTransparencyGrade, ticker.Three65D.VolumeTransparency)
		valueStringOneYear = append(valueStringOneHour, valStringOneYear)

		var valStringYTD = fmt.Sprintf("('%s','%s',%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,json('%v'))::nomicsCurrenciesTickersIntervals", ticker.ID, lastUpdated.Format("2006-01-02 15:04:05"), ticker.YTD.Volume, ticker.YTD.PriceChange, ticker.YTD.PriceChangePct, ticker.YTD.VolumeChange, ticker.YTD.VolumeChangePct, ticker.YTD.MarketCapChange, ticker.YTD.MarketCapChangePct, ticker.YTD.TransparentMarketCapChange, ticker.YTD.TransparentMarketCapChangePct, ticker.YTD.VolumeTransparencyGrade, ticker.YTD.VolumeTransparency)
		valueStringYTD = append(valueStringOneHour, valStringYTD)

	}

	allData := strings.Join(valueStringCurrenciesTickers, ",")
	storedPro := fmt.Sprintf("CALL upsertCurrenciesTickers(ARRAY[%s])", allData)
	_, inserterCurrenciesTickersError := pg.Exec(storedPro)
	if inserterCurrenciesTickersError != nil {
		return inserterCurrenciesTickersError
	}

	InsertCurrenciesTickersTimes(valueStringOneHour, "one_hour")
	InsertCurrenciesTickersTimes(valueStringOneDay, "one_day")
	InsertCurrenciesTickersTimes(valueStringSevenDays, "seven_days")
	InsertCurrenciesTickersTimes(valueStringThirtyDays, "thirty_days")
	InsertCurrenciesTickersTimes(valueStringOneYear, "one_year")
	InsertCurrenciesTickersTimes(valueStringYTD, "ytd")

	return nil
}

func InsertCurrenciesTickersTimes(valueString []string, table string) {
	pg := PGConnect()

	switch table {
		case "one_hour":
			tableName := fmt.Sprintf("nomics_currencies_tickers_%s", table)
			allData := strings.Join(valueString, ",")
			storedPro := fmt.Sprintf("CALL upsertCurrenciesTickersOneHour(ARRAY[%s])", allData)
			_, inserterError := pg.Exec(storedPro)

			if inserterError != nil {
				errMsg := fmt.Sprintf("Error inserting Currencies Ticker times on %s %s", tableName, inserterError)
				log.Error(errMsg)
				return
			}
		case "one_day":
			tableName := fmt.Sprintf("nomics_currencies_tickers_%s", table)
			allData := strings.Join(valueString, ",")
			storedPro := fmt.Sprintf("CALL upsertCurrenciesTickersOneDay(ARRAY[%s])", allData)
			_, inserterError := pg.Exec(storedPro)

			if inserterError != nil {
				errMsg := fmt.Sprintf("Error inserting Currencies Ticker times on %s %s", tableName, inserterError)
				log.Error(errMsg)
				return
			}
		case "seven_days":
			tableName := fmt.Sprintf("nomics_currencies_tickers_%s", table)
			allData := strings.Join(valueString, ",")
			storedPro := fmt.Sprintf("CALL upsertCurrenciesTickersSevenDays(ARRAY[%s])", allData)
			_, inserterError := pg.Exec(storedPro)

			if inserterError != nil {
				errMsg := fmt.Sprintf("Error inserting Currencies Ticker times on %s %s", tableName, inserterError)
				log.Error(errMsg)
				return
			}
		case "thirty_days":
			tableName := fmt.Sprintf("nomics_currencies_tickers_%s", table)
			allData := strings.Join(valueString, ",")
			storedPro := fmt.Sprintf("CALL upsertCurrenciesTickersThirtyDays(ARRAY[%s])", allData)
			_, inserterError := pg.Exec(storedPro)

			if inserterError != nil {
				errMsg := fmt.Sprintf("Error inserting Currencies Ticker times on %s %s", tableName, inserterError)
				log.Error(errMsg)
				return
			}
		case "one_year":
			tableName := fmt.Sprintf("nomics_currencies_tickers_%s", table)
			allData := strings.Join(valueString, ",")
			storedPro := fmt.Sprintf("CALL upsertCurrenciesTickersOneYear(ARRAY[%s])", allData)
			_, inserterError := pg.Exec(storedPro)

			if inserterError != nil {
				errMsg := fmt.Sprintf("Error inserting Currencies Ticker times on %s %s", tableName, inserterError)
				log.Error(errMsg)
				return
			}
		case "ytd":
			tableName := fmt.Sprintf("nomics_currencies_tickers_%s", table)
			allData := strings.Join(valueString, ",")
			storedPro := fmt.Sprintf("CALL upsertCurrenciesTickersYTD(ARRAY[%s])", allData)
			_, inserterError := pg.Exec(storedPro)

			if inserterError != nil {
				errMsg := fmt.Sprintf("Error inserting Currencies Ticker times on %s %s", tableName, inserterError)
				log.Error(errMsg)
				return
			}

	}
}

// Get a list of the last time OHLCV tickers were called
// This is for persistence.
func GetOHLCVRequestTimestamps() (map[string]models.NomicsOHLCVTimeTracker, error) {
	var lastRequestTimes = make(map[string]models.NomicsOHLCVTimeTracker)

	pg := PGConnect()

	query := `SELECT * from nomics_ohlcv_last_req_times`

	queryResult, err := pg.Query(query)
	var tsData models.NomicsOHLCVTimeTracker
	if err != nil {
		return lastRequestTimes, err
	}
	for queryResult.Next() {
		err := queryResult.Scan(&tsData.LastCalled, &tsData.Base, &tsData.Quote)

		if err != nil {
			return lastRequestTimes, err
		}
		lastRequestTimes[fmt.Sprintf("%s/%s", tsData.Base, tsData.Quote)] = tsData
	}
	//log.Info("%s", id)
	return lastRequestTimes, nil
}

/*
	Stores The last recorded request times from the ohlcv calls
*/
func InsertOHLCVRequestTimestamps(requestTimes map[string]models.NomicsOHLCVTimeTracker) error {

	pg := PGConnect()

	var candlesData []models.NomicsOHLCVTimeTracker
	//extract all data from map
	for _, reqTime := range requestTimes {
		candlesData = append(candlesData, reqTime)
	}

	valueString := make([]string, 0, len(candlesData))
	valueArgs := make([]interface{}, 0, len(candlesData)*3)
	var pairsString []string
	tableName := "nomics_ohlcv_last_req_times"
	var i = 0 //used for argument positions
	for y := 0; y < len(candlesData); y++ {
		var candleData = candlesData[y]

		var valString = fmt.Sprintf("($%d,$%d,$%d)", i*3+1, i*3+2, i*3+3)
		pairsString = append(pairsString, fmt.Sprintf("%s/%s", candleData.Base, candleData.Quote))
		valueString = append(valueString, valString)
		valueArgs = append(valueArgs, candleData.LastCalled)
		valueArgs = append(valueArgs, candleData.Base)
		valueArgs = append(valueArgs, candleData.Quote)

		i++

		if len(valueArgs) >= 65000 || y == len(candlesData)-1 {
			insertStatementCandles := fmt.Sprintf("INSERT INTO %s VALUES %s", tableName, strings.Join(valueString, ","))
			updateStatement := "ON CONFLICT (base, quote) DO UPDATE SET base = EXCLUDED.base, last_req_time = EXCLUDED.last_req_time, quote = EXCLUDED.quote"

			query := insertStatementCandles + updateStatement
			_, inserterError := pg.Exec(query, valueArgs...)

			if inserterError != nil {
				log.Debug("%v", pairsString)
				//return inserterError
			}
			valueString = make([]string, 0, len(candlesData))
			valueArgs = make([]interface{}, 0, len(candlesData)*11)
			pairsString = make([]string, 6500)
			i = 0
		}
	}

	return nil
}

/*
	Stores The last recorded request times from the ohlcv calls
*/
func InsertExchangeMarketLastUpdatedTS(requestTimes map[string]models.ExchangeMarketTimeTracker) error {

	pg := PGConnect()

	var candlesData []models.ExchangeMarketTimeTracker
	//extract all data from map
	for _, reqTime := range requestTimes {
		candlesData = append(candlesData, reqTime)
	}

	valueString := make([]string, 0, len(candlesData))
	valueArgs := make([]interface{}, 0, len(candlesData)*2)
	var pairsString []string
	tableName := "nomics_market_last_updated_times"
	var i = 0 //used for argument positions
	for y := 0; y < len(candlesData); y++ {
		var candleData = candlesData[y]

		var valString = fmt.Sprintf("($%d,$%d)", i*2+1, i*2+2)
		pairsString = append(pairsString, fmt.Sprintf("%s", candleData.Market))
		valueString = append(valueString, valString)
		valueArgs = append(valueArgs, candleData.LastUpdated)
		valueArgs = append(valueArgs, candleData.Market)

		i++

		if len(valueArgs) >= 65000 || y == len(candlesData)-1 {
			insertStatementCandles := fmt.Sprintf("INSERT INTO %s VALUES %s", tableName, strings.Join(valueString, ","))
			updateStatement := "ON CONFLICT (market) DO UPDATE SET market = EXCLUDED.market, last_updated_time = EXCLUDED.last_updated_time"

			query := insertStatementCandles + updateStatement
			_, inserterError := pg.Exec(query, valueArgs...)

			if inserterError != nil {
				log.Debug("%v", pairsString)
				//return inserterError
			}
			valueString = make([]string, 0, len(candlesData))
			valueArgs = make([]interface{}, 0, len(candlesData)*2)
			pairsString = make([]string, 6500)
			i = 0
		}
	}

	return nil
}

// Get a list of the last time OHLCV tickers were called
// This is for persistence.
func GetExchangeMarketLastUpdatedTS() (map[string]models.ExchangeMarketTimeTracker, error) {
	var lastUpdatedTimes = make(map[string]models.ExchangeMarketTimeTracker)

	pg := PGConnect()

	query := `SELECT * from nomics_market_last_updated_times`

	queryResult, err := pg.Query(query)
	var tsData models.ExchangeMarketTimeTracker
	if err != nil {
		return lastUpdatedTimes, err
	}
	for queryResult.Next() {
		err := queryResult.Scan(&tsData.LastUpdated, &tsData.Market)

		if err != nil {
			return lastUpdatedTimes, err
		}
		lastUpdatedTimes[tsData.Market] = tsData
	}
	//log.Info("%s", id)
	return lastUpdatedTimes, nil
}

func InsertMarketsCandles(candlesData []models.PGCandle) error {

	pg := PGConnect()

	valueString := make([]string, 0, len(candlesData))
	var pairsString []string
	for y := 0; y < len(candlesData); y++ {
		var candleData = candlesData[y]
		var valString = fmt.Sprintf("('%s','%s',%d,%d,%d,%d,%d,%d,%d,%d,'%s')::candlesValues", candleData.Base, candleData.Timestamp.Format("2006-01-02 15:04:05"), candleData.Open, candleData.High, candleData.Low, candleData.Close, candleData.Volume, candleData.NumTrades, candleData.PriceOutlier, candleData.VolumeOutlier, candleData.Quote)
		pairsString = append(pairsString, fmt.Sprintf("%s/%s", candleData.Base, candleData.Quote))
		valueString = append(valueString, valString)
	}
	allData := strings.Join(valueString, ",")
	storedPro := fmt.Sprintf("CALL upsertCandles(ARRAY[%s])", allData)
	_, inserterError := pg.Exec(storedPro)

	if inserterError != nil {
		log.Debug("%v", pairsString)
	}
	return nil
}

func InsertMarkets(markets []nomics.Markets) error {
	pg := PGConnect()
	valueString := make([]string, 0, len(markets))

	for y := 0; y < len(markets); y++ {
		var market = markets[y]
		var valString = fmt.Sprintf("('%s','%s','%s','%s')::marketsValue", market.Base,market.Exchange, market.Market, market.Quote)
		valueString = append(valueString, valString)
	}

	allData := strings.Join(valueString, ",")
	storedPro := fmt.Sprintf("CALL upsertMarkets(ARRAY[%s])", allData)
	_, inserterError := pg.Exec(storedPro)

	if inserterError != nil {
		return inserterError
	}
	return nil

}
func InsertExchangeMarketsTickerData(exchangesMarketTicker []models.PGExchangeMarketsTicker) error {
	pg := PGConnect()

	valueString := make([]string, 0, len(exchangesMarketTicker))

	for y := 0; y < len(exchangesMarketTicker); y++ {
		var exchange = exchangesMarketTicker[y]

		var valString = fmt.Sprintf("('%s','%s','%s','%s','%s','%s',%v,%v,%v,'%s','%s',%d,%d,%d,'%s','%s','%s','%s','%s','%s',%d)::exchangesValues", exchange.Base, exchange.Exchange, exchange.Market, exchange.Quote, exchange.Type, exchange.SubType, exchange.Aggregated, exchange.PriceExclude, exchange.VolumeExclude, exchange.BaseSymbol, exchange.QuoteSymbol, exchange.Price, exchange.PriceQuote, exchange.VolumeUsd, exchange.Status, exchange.Weight, exchange.FirstTrade.Format("2006-01-02 15:04:05"), exchange.FirstCandle.Format("2006-01-02 15:04:05"), exchange.FirstOrderBook.Format("2006-01-02 15:04:05"), exchange.Timestamp.Format("2006-01-02 15:04:05"), exchange.TotalTrades)
		valueString = append(valueString, valString)

	}
	allData := strings.Join(valueString, ",")
	storedPro := fmt.Sprintf("CALL upsertExchangesMarketTicker(ARRAY[%s])", allData)
	_, inserterExchangeError := pg.Exec(storedPro)

	if inserterExchangeError != nil {
		log.Error("error: %s", inserterExchangeError)
		return inserterExchangeError
	}
	if exchangeMarketIntervals := ExchangeMapIntervals(exchangesMarketTicker); exchangeMarketIntervals != nil {
		return exchangeMarketIntervals
	}
	return nil
}

func ExchangeMapIntervals(exchangesMarketTicker []models.PGExchangeMarketsTicker) error {
	var (
		valueStringOneDay     = make([]string, 0, len(exchangesMarketTicker))
		valueStringSevenDays  = make([]string, 0, len(exchangesMarketTicker))
		valueStringThirtyDays = make([]string, 0, len(exchangesMarketTicker))
		valueStringOneYear    = make([]string, 0, len(exchangesMarketTicker))
		valueStringYTD        = make([]string, 0, len(exchangesMarketTicker))
	)

	for y := 0; y < len(exchangesMarketTicker); y++ {
		var exchange = exchangesMarketTicker[y]
		var valStringOneDay = fmt.Sprintf("('%s','%s','%s','%s',%d,%d,%d,%d,%d,%d,%d,%d)::exchangeIntervals", exchange.Base, exchange.Timestamp.Format("2006-01-02 15:04:05"), exchange.Market, exchange.Exchange, exchange.OneD.Volume, exchange.OneD.VolumeBase, exchange.OneD.VolumeBaseChange, exchange.OneD.VolumeChange, exchange.OneD.Trades, exchange.OneD.TradesChange, exchange.OneD.PriceChange, exchange.OneD.PriceQuoteChange)
		valueStringOneDay = append(valueStringOneDay, valStringOneDay)
		var valStringSevenDays = fmt.Sprintf("('%s','%s','%s','%s',%d,%d,%d,%d,%d,%d,%d,%d)::exchangeIntervals", exchange.Base, exchange.Timestamp.Format("2006-01-02 15:04:05"), exchange.Market, exchange.Exchange, exchange.SevenD.Volume, exchange.SevenD.VolumeBase, exchange.SevenD.VolumeBaseChange, exchange.SevenD.VolumeChange, exchange.SevenD.Trades, exchange.SevenD.TradesChange, exchange.SevenD.PriceChange, exchange.SevenD.PriceQuoteChange)
		valueStringSevenDays = append(valueStringSevenDays, valStringSevenDays)
		var valStringThirtyDays = fmt.Sprintf("('%s','%s','%s','%s',%d,%d,%d,%d,%d,%d,%d,%d)::exchangeIntervals", exchange.Base, exchange.Timestamp.Format("2006-01-02 15:04:05"), exchange.Market, exchange.Exchange, exchange.Three0D.Volume, exchange.Three0D.VolumeBase, exchange.Three0D.VolumeBaseChange, exchange.Three0D.VolumeChange, exchange.Three0D.Trades, exchange.Three0D.TradesChange, exchange.Three0D.PriceChange, exchange.Three0D.PriceQuoteChange)
		valueStringThirtyDays = append(valueStringThirtyDays, valStringThirtyDays)
		var valStringOneYear = fmt.Sprintf("('%s','%s','%s','%s',%d,%d,%d,%d,%d,%d,%d,%d)::exchangeIntervals", exchange.Base, exchange.Timestamp.Format("2006-01-02 15:04:05"), exchange.Market, exchange.Exchange, exchange.Three65D.Volume, exchange.Three65D.VolumeBase, exchange.Three65D.VolumeBaseChange, exchange.Three65D.VolumeChange, exchange.Three65D.Trades, exchange.Three65D.TradesChange, exchange.Three65D.PriceChange, exchange.Three65D.PriceQuoteChange)
		valueStringOneYear = append(valueStringOneYear, valStringOneYear)
		var valStringYTD = fmt.Sprintf("('%s','%s','%s','%s',%d,%d,%d,%d,%d,%d,%d,%d)::exchangeIntervals", exchange.Base, exchange.Timestamp.Format("2006-01-02 15:04:05"), exchange.Market, exchange.Exchange, exchange.YTD.Volume, exchange.YTD.VolumeBase, exchange.YTD.VolumeBaseChange, exchange.YTD.VolumeChange, exchange.YTD.Trades, exchange.YTD.TradesChange, exchange.YTD.PriceChange, exchange.YTD.PriceQuoteChange)
		valueStringYTD = append(valueStringYTD, valStringYTD)
	}
	InsertExchangeMarketsTickerDataIntervals(valueStringOneDay, "one_day")
	InsertExchangeMarketsTickerDataIntervals(valueStringSevenDays, "seven_days")
	InsertExchangeMarketsTickerDataIntervals(valueStringThirtyDays, "thirty_days")
	InsertExchangeMarketsTickerDataIntervals(valueStringOneYear, "one_year")
	InsertExchangeMarketsTickerDataIntervals(valueStringYTD, "ytd")

	return nil
}

func InsertExchangeMarketsTickerDataIntervals(valueString []string, table string) {
	pg := PGConnect()

	switch table {
	case "one_day":
		tableName := fmt.Sprintf("nomics_exchange_market_ticker_%s", table)
		allData := strings.Join(valueString, ",")
		storedPro := fmt.Sprintf("CALL upsertExchangesMarketTickerOneDay(ARRAY[%s])", allData)
		_, inserterError := pg.Exec(storedPro)

		if inserterError != nil {
			errMsg := fmt.Sprintf("Error inserting Exchange Market Ticker times on %s %s", tableName, inserterError)
			log.Error(errMsg)
			return
		}
	case "seven_days":
		tableName := fmt.Sprintf("nomics_exchange_market_ticker_%s", table)
		allData := strings.Join(valueString, ",")
		storedPro := fmt.Sprintf("CALL upsertExchangesMarketTickerSevenDays(ARRAY[%s])", allData)
		_, inserterError := pg.Exec(storedPro)

		if inserterError != nil {
			errMsg := fmt.Sprintf("Error inserting Exchange Market Ticker times on %s %s", tableName, inserterError)
			log.Error(errMsg)
			return
		}
	case "thirty_days":
		tableName := fmt.Sprintf("nomics_exchange_market_ticker_%s", table)
		allData := strings.Join(valueString, ",")
		storedPro := fmt.Sprintf("CALL upsertExchangesMarketTickerThirtyDays(ARRAY[%s])", allData)
		_, inserterError := pg.Exec(storedPro)

		if inserterError != nil {
			errMsg := fmt.Sprintf("Error inserting Exchange Market Ticker times on %s %s", tableName, inserterError)
			log.Error(errMsg)
			return
		}
	case "one_year":
		tableName := fmt.Sprintf("nomics_exchange_market_ticker_%s", table)
		allData := strings.Join(valueString, ",")
		storedPro := fmt.Sprintf("CALL upsertExchangesMarketTickerOneYear(ARRAY[%s])", allData)
		_, inserterError := pg.Exec(storedPro)

		if inserterError != nil {
			errMsg := fmt.Sprintf("Error inserting Exchange Market Ticker times on %s %s", tableName, inserterError)
			log.Error(errMsg)
			return
		}
	case "ytd":
		tableName := fmt.Sprintf("nomics_exchange_market_ticker_%s", table)
		allData := strings.Join(valueString, ",")
		storedPro := fmt.Sprintf("CALL upsertExchangesMarketTickerYTD(ARRAY[%s])", allData)
		_, inserterError := pg.Exec(storedPro)

		if inserterError != nil {
			errMsg := fmt.Sprintf("Error inserting Exchange Market Ticker times on %s %s", tableName, inserterError)
			log.Error(errMsg)
			return
		}
	}
}

func InsertGlobalVolumeHistory(globalData []models.PGVolumeHistory) error {
	pg := PGConnect()

	valueString := make([]string, 0, len(globalData))
	valueGlobal := make([]interface{}, 0, len(globalData)*7)
	var i = 0
	tableName := "nomics_global_volume_history"

	for y := 0; y < len(globalData); y++ {
		var global = globalData[y]

		var valString = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d, $%d)", i*7+1, i*7+2, i*7+3, i*7+4, i*7+5, i*7+6, i*7+7)

		valueString = append(valueString, valString)
		valueGlobal = append(valueGlobal, global.TimeStamp)
		valueGlobal = append(valueGlobal, global.Volume)
		valueGlobal = append(valueGlobal, global.SpotVolume)
		valueGlobal = append(valueGlobal, global.DerivativeVolume)
		valueGlobal = append(valueGlobal, global.TransparencyVolume)
		valueGlobal = append(valueGlobal, global.TransparencySpotVolume)
		valueGlobal = append(valueGlobal, global.TransparencyDerivativeVolume)

		i++

	}
	insertQuery := fmt.Sprintf("INSERT INTO %s VALUES %s", tableName, strings.Join(valueString, ", "))

	_, inserterError := pg.Exec(insertQuery, valueGlobal...)

	if inserterError != nil {
		return inserterError
	}
	return nil
}

func InsertMarketCapHistory(marketCapHistory []models.PGMarketCapHistory) error {
	pg := PGConnect()

	tableName := "nomics_market_cap_history"

	valueString := make([]string, 0, len(marketCapHistory))
	valueMarket := make([]interface{}, 0, len(marketCapHistory)*3)
	var i = 0

	for y := 0; y < len(marketCapHistory); y++ {
		var market = marketCapHistory[y]
		var valString = fmt.Sprintf("($%d,$%d,$%d)", i*3+1, i*3+2, i*3+3)
		valueString = append(valueString, valString)
		valueMarket = append(valueMarket, market.Timestamp)
		valueMarket = append(valueMarket, market.MarketCap)
		valueMarket = append(valueMarket, market.TransparentMarketCap)

		i++

	}

	insertQuery := fmt.Sprintf("INSERT INTO %s VALUES %s", tableName, strings.Join(valueString, ","))

	_, inserterError := pg.Exec(insertQuery, valueMarket...)

	if inserterError != nil {
		return inserterError
	}

	return nil
}

func CheckExchangeExist(exchange string) bool {

	pg := PGConnect()

	var id string

	query := `
	SELECT 
		id
	FROM 
		nomics_exchange_metadata
	where 
		id = '` + exchange + `'
	`

	queryResult, err := pg.Query(query)

	if err != nil {
		return false
	}
	for queryResult.Next() {
		err := queryResult.Scan(&id)

		if err != nil {
			return false
		}
	}
	log.Info("%s", id)
	return len(id) > 0
}

func GetActiveMarketPairs() ([]nomics.Markets, error) {
	var markets []nomics.Markets

	pg := PGConnect()

	query := `SELECT * from public.GetActiveMarketPairs()`

	queryResult, err := pg.Query(query)
	var market nomics.Markets
	if err != nil {
		return markets, err
	}
	for queryResult.Next() {
		err := queryResult.Scan(&market.Base, &market.Quote)

		if err != nil {
			return markets, err
		}
		markets = append(markets, market)
	}
	//log.Info("%s", id)
	return markets, nil
}

// returns a list of symbols
func GetListOfNomicsAssets() ([]string, error) {
	var nomics_assets []string

	pg := PGConnect()

	query := `
		SELECT 
			ID 
		FROM nomics_ticker_metadata
	`

	queryResult, err := pg.Query(query)

	var symbol string
	if err != nil {
		return nomics_assets, err
	}
	for queryResult.Next() {
		err := queryResult.Scan(&symbol)

		if err != nil {
			return nomics_assets, err
		}
		nomics_assets = append(nomics_assets, symbol)
	}
	//log.Info("%s", id)
	return nomics_assets, nil
}






++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
package store

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"cloud.google.com/go/firestore"
	"github.com/Forbes-Media/fda-nomics-ingestion/models"
	"github.com/Forbes-Media/fda-nomics-ingestion/service"
	"github.com/Forbes-Media/go-tools/log"
	"github.com/Forbes-Media/nomics-client/nomics"
	"google.golang.org/api/iterator"
)

var (
	firestoreClient    *firestore.Client
	firstoreClientOnce sync.Once
	BQProjectID        = "api-project-901373404215"
)

func GetFirestoreClient() *firestore.Client {
	if firestoreClient == nil {
		firstoreClientOnce.Do(func() {
			fsClient, err := firestore.NewClient(context.Background(), "digital-assets-301018")
			if err != nil {
				panic(err)
			}
			firestoreClient = fsClient
		})
	}

	return firestoreClient
}

func GetForbesSupportedAssets() ([]string, error) {
	ctx := context.Background()
	client := GetFirestoreClient()

	assetsCollectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "assets")

	dbSnap := client.Collection(assetsCollectionName).Documents(ctx)
	var allAssets []string

	for {
		var assetProfile models.AssetsProfiles
		doc, err := dbSnap.Next()

		if err == iterator.Done {
			break
		}
		if err := doc.DataTo(&assetProfile); err != nil {
			return nil, err
		}

		allAssets = append(allAssets, assetProfile.Symbol)
	}

	return allAssets, nil
}

func GetForbesSupportedExchanges() ([]string, error) {
	ctx := context.Background()
	client := GetFirestoreClient()

	exchangesCollection := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "exchanges")

	dbSnap := client.Collection(exchangesCollection).Documents(ctx)
	var exchanges []string

	for {
		var exchangeProfile models.ExchangeProfile
		doc, err := dbSnap.Next()

		if err == iterator.Done {
			break
		}
		if err := doc.DataTo(&exchangeProfile); err != nil {
			return nil, err
		}

		exchanges = append(exchanges, exchangeProfile.Name)
	}

	return exchanges, nil
}

func SaveMarkets(markets *[]nomics.Markets) {
	log.Info("Inserting %v markets", len(*markets))
	ctx := context.Background()
	client := GetFirestoreClient()

	marketsCollection := fmt.Sprintf("nomics_markets%s", os.Getenv("DATA_NAMESPACE"))

	for _, market := range *markets {
		doc := fmt.Sprintf("%s_%s", strings.ToLower(market.Exchange), strings.ToLower(market.Base))

		client.Collection(marketsCollection).Doc(doc).Set(ctx, market)
	}
}

func GetAllMarkets() ([]nomics.Markets, error) {
	ctx := context.Background()
	client := GetFirestoreClient()

	marketsCollection := fmt.Sprintf("nomics_markets%s", os.Getenv("DATA_NAMESPACE"))

	dbSnap := client.Collection(marketsCollection).Documents(ctx)

	var allMarkets []nomics.Markets
	var marketsSet sync.Map

	for {
		var market nomics.Markets
		doc, err := dbSnap.Next()

		if err == iterator.Done {
			break
		}
		if err := doc.DataTo(&market); err != nil {
			return nil, err
		}

		mkt, ok := marketsSet.Load(market.Market)
		if mkt != true || !ok {
			marketsSet.Store(market.Market, true)
			allMarkets = append(allMarkets, market)
		}
	}

	return allMarkets, nil
}

func SaveTickerData(markets *[]nomics.TickerData) {
	ctx := context.Background()
	client := GetFirestoreClient()

	throttleChan := make(chan bool, 10)
	var wg sync.WaitGroup

	tickersCollection := fmt.Sprintf("nomics_tickers%s", os.Getenv("DATA_NAMESPACE"))

	log.Info("Saving ticker data to firestore")

	for _, ticker := range *markets {
		throttleChan <- true
		wg.Add(1)
		go func(ticker nomics.TickerData) {
			client.Collection(tickersCollection).Doc(strings.ToLower(ticker.ID)).Set(ctx, ticker)
			<-throttleChan
			wg.Done()
		}(ticker)
	}
	log.Info("Done saving ticker data to firestore")
	wg.Wait()
}

func UpsertTickersMetadata(tickersMetadata *[]nomics.Metadata) {
	ctx := context.Background()
	client := GetFirestoreClient()

	metadataCollection := fmt.Sprintf("nomics_tickers_metadata%s", os.Getenv("DATA_NAMESPACE"))
	var metadataMap sync.Map

	iter := client.Collection(metadataCollection).Documents(ctx)

	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Error("Error getting metadata: %s", err)
		}
		var metadata models.TickerMetada
		doc.DataTo(&metadata)
		metadataMap.Store(metadata.ID, metadata)
	}

	throttleChan := make(chan bool, 10)
	var wg sync.WaitGroup

	log.Info("Updating Ticker Metadata on Firestore")

	for _, ticker := range *tickersMetadata {
		throttleChan <- true
		wg.Add(1)
		go func(ticker nomics.Metadata) {

			data, ok := metadataMap.Load(ticker.ID)

			if !ok {
				metadata := service.MapTickerMetadata(ticker)
				client.Collection(metadataCollection).Doc(strings.ToLower(ticker.ID)).Set(ctx, metadata)
			} else {
				tickerMetada := data.(models.TickerMetada)
				tickerMetada = service.UpdateMetada(ticker, tickerMetada)
				client.Collection(metadataCollection).Doc(strings.ToLower(ticker.ID)).Set(ctx, tickerMetada)
			}

			<-throttleChan
			wg.Done()
		}(ticker)
	}
	wg.Wait()
}

func GetNomicsIDForbesSupportedExchanges() ([]string, error) {
	ctx := context.Background()
	client := GetFirestoreClient()

	exchangesCollection := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "exchanges")

	dbSnap := client.Collection(exchangesCollection).Documents(ctx)
	var exchanges []string

	for {
		var exchangeProfile models.ExchangeProfile
		doc, err := dbSnap.Next()

		if err == iterator.Done {
			break
		}
		if err := doc.DataTo(&exchangeProfile); err != nil {
			return nil, err
		}

		if exchangeProfile.NomicsID == "" {
			exchanges = append(exchanges, exchangeProfile.Slug)
		} else {
			exchanges = append(exchanges, exchangeProfile.NomicsID)
		}
	}

	return exchanges, nil
}

type assetProfile struct {
	ID                      string  `json:"id" firestore:"id"`
	Symbol                  string  `json:"symbol" firestore:"symbol"`
	Slug                    string  `json:"slug,omitempty" firestore:"slug,omitempty"`
	CurrentPrice            string  `json:"currentPrice" firestore:"currentPrice"`
	MarketCap               float64 `json:"marketCap" firestore:"marketCap"`
	NomicsStatus            string  `json:"nomicsStatus" firestore:"nomicsStatus"`
	ForbesStatus            string  `json:"forbesStatus" firestore:"forbesStatus"`
	OriginalSymbol          string  `json:"original_symbol" firestore:"originalSymbol"`
	Name                    string  `json:"name" firestore:"name"`
	Description             string  `json:"description" firestore:"nomicsDescription"`
	Website                 string  `json:"website_url" firestore:"website"`
	Logo                    string  `json:"logo_url" firestore:"nomicsLogo"`
	Blog                    string  `json:"blog_url" firestore:"blog"`
	Discord                 string  `json:"discord_url" firestore:"discord"`
	Facebook                string  `json:"facebook_url" firestore:"facebook"`
	Github                  string  `json:"github_url"	firestore:"github"`
	Medium                  string  `json:"medium_url" firestore:"medium"`
	Reddit                  string  `json:"reddit_url" firestore:"reddit"`
	Telegram                string  `json:"telegram_url" firestore:"telegram"`
	Twitter                 string  `json:"twitter_url" firestore:"twitter"`
	Whitepaper              string  `json:"whitepaper_url" firestore:"whitepaper"`
	Youtube                 string  `json:"youtube_url" firestore:"youtube"`
	Linkedin                string  `json:"linkedin_url" firestore:"linkedin"`
	Bitcointalk             string  `json:"bitcointalk_url" firestore:"bitcointalk"`
	BlockExplorer           string  `json:"block_explorer_url" firestore:"blockExplorer"`
	ReplacedBy              string  `json:"replaced_by" firestore:"replacedBy"`
	MarketsCount            int64   `json:"markets_count" firestore:"marketsCount"`
	CryptocontrolCoinID     string  `json:"cryptocontrol_coin_id" firestore:"cryptocontrolCoinId"`
	UsedForPricing          bool    `json:"used_for_pricing" firestore:"usedForPricing"`
	PlatformCurrencyID      string  `json:"platform_currency_id,omitempty" firestore:"platformCurrencyId,omitempty"`
	PlatformContractAddress string  `json:"platform_contract_address,omitempty" 	firestore:"platformContractAddress,omitempty"`
	BlockchainForAsset      string  `json:"blockchainForAsset,omitempty" firestore:"blockchainForAsset,omitempty"`
}

func getAssetNameFromSymbol(symbol string, assets *[]nomics.Metadata) string {

	for _, data := range *assets {
		if data.ID == symbol {
			return data.Name
		}
	}
	return ""
}

func FSUpdateAssetProfilesMetaData(ctx context.Context, assets *[]nomics.Metadata) error {
	client := GetFirestoreClient()
	metadataCollection := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "assets")

	var data []*assetProfile

	for _, asset := range *assets {

		var blockchainForAsset = ""
		//We want to display the full blockchain name, not just the symbol
		if asset.PlatformCurrencyID != "" {
			blockchainForAsset = getAssetNameFromSymbol(asset.PlatformCurrencyID, assets)
		}

		//use original symbol to build slug so if the ID ever changes it wont break the frontend
		var slug = strings.ToLower(fmt.Sprintf("%s-%s", strings.ReplaceAll(asset.Name, " ", "-"), asset.ID))

		marketsCount, _ := strconv.ParseInt(asset.MarketsCount, 10, 64)

		data = append(data, &assetProfile{
			Symbol:                  asset.ID,
			OriginalSymbol:          asset.OriginalSymbol,
			Name:                    asset.Name,
			Description:             asset.Description,
			Website:                 asset.WebsiteURL,
			Logo:                    asset.LogoURL,
			Blog:                    asset.BlogURL,
			Discord:                 asset.DiscordURL,
			Facebook:                asset.FacebookURL,
			Github:                  asset.GithubURL,
			Medium:                  asset.MediumURL,
			Reddit:                  asset.RedditURL,
			Telegram:                asset.TelegramURL,
			Twitter:                 asset.TwitterURL,
			Whitepaper:              asset.WhitepaperURL,
			Youtube:                 asset.YoutubeURL,
			Linkedin:                asset.LinkedinURL,
			Bitcointalk:             asset.BitcointalkURL,
			BlockExplorer:           asset.BlockExplorerURL,
			ReplacedBy:              asset.ReplacedBy,
			MarketsCount:            marketsCount,
			CryptocontrolCoinID:     asset.CryptocontrolCoinID,
			UsedForPricing:          asset.UsedForPricing,
			PlatformCurrencyID:      asset.PlatformCurrencyID,
			PlatformContractAddress: asset.PlatformContractAddress,
			BlockchainForAsset:      blockchainForAsset,
			Slug:                    slug,
		})
	}

	log.Debug("Reformated %d assets", len(data))

	throttleChan := make(chan bool, 8)
	var wg sync.WaitGroup
	for _, asset := range data {
		throttleChan <- true
		wg.Add(1)
		go func(asset *assetProfile) {
			client.Collection(metadataCollection).Doc(strings.ToLower(asset.Symbol)).Set(ctx,
				map[string]interface{}{
					"symbol":                  asset.Symbol,
					"originalSymbol":          asset.OriginalSymbol,
					"name":                    asset.Name,
					"nomicsDescription":       asset.Description,
					"website":                 asset.Website,
					"logo":                    asset.Logo,
					"blog":                    asset.Blog,
					"discord":                 asset.Discord,
					"facebook":                asset.Facebook,
					"github":                  asset.Github,
					"medium":                  asset.Medium,
					"reddit":                  asset.Reddit,
					"telegram":                asset.Telegram,
					"twitter":                 asset.Twitter,
					"whitepaper":              asset.Whitepaper,
					"youtube":                 asset.Youtube,
					"linkedin":                asset.Linkedin,
					"bitcointalk":             asset.Bitcointalk,
					"blockExplorer":           asset.BlockExplorer,
					"replacedBy":              asset.ReplacedBy,
					"marketsCount":            asset.MarketsCount,
					"cryptocontrolCoinID":     asset.CryptocontrolCoinID,
					"usedForPricing":          asset.UsedForPricing,
					"platformCurrencyID":      asset.PlatformCurrencyID,
					"platformContractAddress": asset.PlatformContractAddress,
					"blockchainForAsset":      asset.BlockchainForAsset,
					"slug":                    asset.Slug,
				},

				firestore.MergeAll)
			<-throttleChan
			wg.Done()
		}(asset)
	}
	wg.Wait()

	/*
		for _, asset := range data {
			res, err := client.Collection(metadataCollection).Doc(strings.ToLower(asset.Symbol)).Set(ctx, &asset)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println(res)
		}
	*/
	log.Debug("Updated asset profiles")

	return nil
}

func FSUpdateAssetCurrencyData(assets *[]nomics.TickerData) error {
	ctx := context.Background()
	client := GetFirestoreClient()
	metadataCollection := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "assets")

	var data []*assetProfile

	for _, asset := range *assets {

		marketCap, _ := strconv.ParseFloat(asset.MarketCap, 64)

		data = append(data, &assetProfile{
			Symbol:       asset.ID,
			MarketCap:    marketCap,
			CurrentPrice: asset.Price,
			Name:         asset.Name,
			NomicsStatus: asset.Status,
		})
	}

	log.Debug("Reformated %d assets", len(data))

	throttleChan := make(chan bool, 10)
	var wg sync.WaitGroup

	for _, asset := range data {
		throttleChan <- true
		wg.Add(1)
		go func(asset *assetProfile) {

			asset.ForbesStatus = "comatoken"
			if strings.ToLower(asset.NomicsStatus) == "active" {
				asset.ForbesStatus = "active"
			}
			_, err := client.Collection(metadataCollection).Doc(strings.ToLower(asset.Symbol)).Set(ctx,
				map[string]interface{}{
					"symbol":       asset.Symbol,
					"marketCap":    asset.MarketCap,
					"currentPrice": asset.CurrentPrice,
					"name":         asset.Name,
					"nomicsStatus": asset.NomicsStatus,
					"forbesStatus": asset.ForbesStatus,
				},
				firestore.MergeAll)
			if err != nil {
				fmt.Println(err)
			}
			<-throttleChan
			wg.Done()
		}(asset)
	}
	wg.Wait()
	/*
		for _, asset := range data {
			if asset.Symbol == "UDC" {
				fmt.Println("UDC")
			}
			res, err := client.Collection(metadataCollection).Doc(strings.ToLower(asset.Symbol)).Set(ctx, &asset)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println(res)
		}
	*/
	log.Debug("Updated asset profiles")

	return nil
}

type exchangeProfile struct {
	ID                          string `json:"id" firestore:"id"`
	Slug                        string `json:"slug" firestore:"slug"`
	CapabilityMarkets           bool   `json:"capability_markets" firestore:"capabilityMarkets"`
	CapabilityTrades            bool   `json:"capability_trades" firestore:"capabilityTrades"`
	CapabilityTradesByTimestamp bool   `json:"capability_trades_by_timestamp" firestore:"capabilityTradesByTimestamp"`
	CapabilityTradesSnapshot    bool   `json:"capability_trades_snapshot" firestore:"capabilityTradesSnapshot"`
	CapabilityOrdersSnapshot    bool   `json:"capability_orders_snapshot" firestore:"capabilityOrdersSnapshot"`
	CapabilityCandles           bool   `json:"capability_candles" firestore:"capabilityCandles"`
	CapabilityTicker            bool   `json:"capability_ticker" firestore:"capabilityTicker"`
	Integrated                  bool   `json:"integrated" firestore:"integrated"`
	NomicsID                    string `json:"nomicsId" firestore:"nomicsId"`
	Name                        string `json:"name" firestore:"name"`
	Description                 string `json:"description" firestore:"nomicsDescription"`
	Location                    string `json:"location" firestore:"location"`
	LogoURL                     string `json:"logo_url" firestore:"nomicsLogo"`
	WebsiteURL                  string `json:"website_url" firestore:"website"`
	FeesURL                     string `json:"fees_url" firestore:"fees"`
	TwitterURL                  string `json:"twitter_url" firestore:"twitter"`
	FacebookURL                 string `json:"facebook_url" firestore:"facebook"`
	RedditURL                   string `json:"reddit_url" firestore:"reddit"`
	ChatURL                     string `json:"chat_url" firestore:"chat"`
	BlogURL                     string `json:"blog_url" firestore:"blog"`
	Year                        string `json:"year" firestore:"yearFounded"`
	TransparencyGrade           string `json:"transparency_grade" firestore:"transparencyGrade"`
	OrderBooksInterval          int    `json:"order_books_interval" firestore:"orderBooksInterval"`
	Centralized                 bool   `json:"centralized" firestore:"centralized"`
	Decentralized               bool   `json:"decentralized" firestore:"decentralized"`
	ReplacedBy                  string `json:"replaced_by" firestore:"replacedBy"`
	VolumeDiscountPercent       int64  `json:"Volume_discount_percent" firestore:"VolumeDiscountPercent"`
}

var exchangeVolumeDiscount = map[string]int64{
	"nami_exchange":       95,
	"hitbtc":              95,
	"bitoffer":            95,
	"bigone":              95,
	"cbx":                 95,
	"fanbit":              95,
	"btse":                95,
	"playroyal":           95,
	"gokumarket":          95,
	"hopex":               95,
	"b2bx":                95,
	"bitcoke":             95,
	"tokok":               95,
	"zipmex":              95,
	"zb":                  85,
	"btcc":                80,
	"fairdesk":            80,
	"bingbon":             40,
	"fameex":              80,
	"cointiger":           80,
	"btc6x":               80,
	"birake":              80,
	"bitfront":            80,
	"resfinex":            80,
	"exmarkets":           80,
	"emirex":              80,
	"zbg":                 80,
	"hoo":                 75,
	"cryptomkt":           70,
	"coinstore":           70,
	"bitonbay":            70,
	"prime_xbt":           70,
	"azbit":               70,
	"biconomy":            60,
	"bitrue":              60,
	"coinw":               60,
	"bit_com":             60,
	"btcbox":              60,
	"bankcex":             60,
	"btcex":               50,
	"bibox":               50,
	"aex":                 50,
	"paritex":             50,
	"coinfield":           50,
	"bitget":              45,
	"aax":                 45,
	"huobi_com":           45,
	"lbank":               45,
	"digifinex":           45,
	"p2pb2b":              45,
	"bkex":                45,
	"latoken":             45,
	"bithumb_global":      45,
	"probit":              45,
	"poloniex":            45,
	"coinsbit":            45,
	"yobit":               45,
	"catex":               45,
	"finexbox":            45,
	"coinjar":             45,
	"decoin":              45,
	"phemex":              35,
	"mexc":                35,
	"kucoin":              35,
	"bitmart":             35,
	"bitforex":            35,
	"bitmax":              35,
	"bitfinex":            35,
	"tidex":               35,
	"dcoin":               35,
	"coincheck":           35,
	"ztglobal":            35,
	"alterdice":           35,
	"arthbit":             35,
	"btcmarkets":          35,
	"hotbit":              35,
	"oceanex":             35,
	"synthetix":           35,
	"coin_egg":            35,
	"max_maicoin":         35,
	"nicex":               35,
	"yunex":               35,
	"exmo":                35,
	"apollox":             35,
	"bw":                  35,
	"counos":              35,
	"btc_alpha":           35,
	"chiliz":              35,
	"belfrics":            35,
	"bithash":             35,
	"bityard":             35,
	"betconix":            35,
	"tokenize":            35,
	"bitexlive":           35,
	"billance":            35,
	"novadax":             35,
	"timex":               35,
	"bilaxy":              35,
	"nominex":             35,
	"cryptology":          35,
	"bisq":                35,
	"ampmcx":              35,
	"bitopro":             35,
	"flybit":              35,
	"okex":                25,
	"gateio":              25,
	"whitebit":            25,
	"bitmex":              25,
	"xt":                  20,
	"deribit":             15,
	"bullish":             15,
	"currency":            10,
	"independent_reserve": 10,
	"coinbase":            0,
	"binance":             0,
	"cryptocom":           0,
	"cmegroup":            0,
	"bybit":               0,
	"upbit":               0,
	"kraken":              0,
	"binance_us":          0,
	"gemini":              0,
	"lmaxdigital":         0,
	"bitstamp":            0,
	"ftx_us":              0,
	"bitflyer":            0,
	"okcoinusd":           0,
	"fxt_tr":              0,
	"btcturk":             0,
	"coinone":             0,
	"coin.z.com":          0,
	"bithumb":             0,
	"bitvavo":             0,
	"paribu":              0,
	"bitkub":              0,
	"bitbank":             0,
	"blockchain_com":      0,
	"liquid":              0,
	"bitso":               0,
	"bittrex":             0,
	"luno":                0,
	"indodax":             0,
	"itbit":               0,
	"bitpanda":            0,
	"cex":                 0,
	"coinzoom":            0,
	"butterswap":          0,
	"curve":               0,
	"deversifi":           0,
	"dextrade":            0,
	"dodo":                0,
	"dydx":                0,
	"mdex":                0,
	"pancakeswapv2":       0,
	"uniswapv2":           0,
	"uniswapv3":           0,
	"apeswap":             0,
	"apollox_dex":         0,
	"astroport":           0,
	"babyswap":            0,
	"balancer":            0,
	"balancerv2":          0,
	"biswap":              0,
	"compound_finance":    0,
	"curve_arbitrum":      0,
	"curve_avalanche":     0,
	"curve_matic":         0,
	"defichain":           0,
	"dexzbitz":            0,
	"dodo_polygon":        0,
	"sushiswap":           0,
	"zaif":                0,
	"mercado_bitcoin":     0,
	"coindcx":             0,
	"bitbuy":              0,
	"wazirx":              0,
	"bitbns":              0,
	"huobi_japan":         0,
	"korbit":              0,
}

func FSUpdateExchangeProfiles(ctx context.Context, nomics *[]nomics.ExchangeMetadata) error {
	client := GetFirestoreClient()
	exchangeCollection := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "exchanges")

	var data []*exchangeProfile

	var volumeDiscount int64

	for _, exchange := range *nomics {
		discountPercent, discount := exchangeVolumeDiscount[exchange.ID]
		if discount {
			volumeDiscount = discountPercent
		} else {
			volumeDiscount = 0
		}
		data = append(data, &exchangeProfile{
			ID:                          exchange.ID,
			Slug:                        exchange.ID,
			NomicsID:                    exchange.ID,
			CapabilityMarkets:           exchange.CapabilityMarkets,
			CapabilityTrades:            exchange.CapabilityTrades,
			CapabilityTradesByTimestamp: exchange.CapabilityTradesByTimestamp,
			CapabilityTradesSnapshot:    exchange.CapabilityTradesSnapshot,
			CapabilityOrdersSnapshot:    exchange.CapabilityOrdersSnapshot,
			CapabilityCandles:           exchange.CapabilityCandles,
			CapabilityTicker:            exchange.CapabilityTicker,
			Integrated:                  exchange.Integrated,
			Name:                        exchange.Name,
			Description:                 exchange.Description,
			Location:                    exchange.Location,
			LogoURL:                     exchange.LogoURL,
			WebsiteURL:                  exchange.WebsiteURL,
			FeesURL:                     exchange.FeesURL,
			TwitterURL:                  exchange.TwitterURL,
			FacebookURL:                 exchange.FacebookURL,
			RedditURL:                   exchange.RedditURL,
			ChatURL:                     exchange.ChatURL,
			BlogURL:                     exchange.BlogURL,
			Year:                        exchange.Year,
			TransparencyGrade:           exchange.TransparencyGrade,
			OrderBooksInterval:          exchange.OrderBooksInterval,
			Centralized:                 exchange.Centralized,
			Decentralized:               exchange.Decentralized,
			ReplacedBy:                  exchange.ReplacedBy,
			VolumeDiscountPercent:       volumeDiscount,
		})
	}

	log.Debug("Reformated %d exchanges", len(data))

	for _, exchange := range data {
		client.Collection(exchangeCollection).Doc(strings.ToLower(exchange.ID)).Set(ctx, &exchange)
	}

	log.Debug("Updated exchange profiles")

	return nil
}
