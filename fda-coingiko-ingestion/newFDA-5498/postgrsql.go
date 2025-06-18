package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"reflect"

	"regexp"

	"github.com/Forbes-Media/coingecko-client/coingecko"
	"github.com/Forbes-Media/fda-coingecko-ingestion/models"
	"github.com/Forbes-Media/fda-coingecko-ingestion/utils"
	"github.com/Forbes-Media/go-tools/log"
	"github.com/lib/pq"
	"go.nhat.io/otelsql"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"golang.org/x/exp/slices"
)

var (
	pg           *sql.DB
	DBClientOnce sync.Once
	tracer       = otel.Tracer("github.com/Forbes-Media/fda-nomics-ingestion/store")
)

type trendingResult []models.Trending

func (c trendingResult) Value() (driver.Value, error) {
	return json.Marshal(c)
}
func (c *trendingResult) Scan(value interface{}) error {
	var b []byte
	switch t := value.(type) {
	case []byte:
		b = t
	case string:
		b = []byte(t)
	default:
		return errors.New("unknown type")
	}
	return json.Unmarshal(b, c)
}

func PGConnect() *sql.DB {
	println(sql.ErrNoRows)
	if pg == nil {
		DBClientOnce.Do(func() {
			connectionString := fmt.Sprintf("port=%s host=%s user=%s password=%s dbname=%s sslmode=%s", os.Getenv("DB_PORT"), os.Getenv("DB_HOST"), os.Getenv("DB_USER"), os.Getenv("DB_PASSWORD"), os.Getenv("DB_NAME"), os.Getenv("DB_SSLMODE"))

			driverName, err := otelsql.Register("postgres",
				otelsql.TraceAll(),
				otelsql.WithDatabaseName(os.Getenv("DB_NAME")),
				otelsql.WithSystem(semconv.DBSystemPostgreSQL),
			)
			if err != nil {
				log.Error("%s", err)
				return
			}

			pg, err = sql.Open(driverName, connectionString)

			if err != nil {
				log.Error("%s", err)
				return
			}

			if err := otelsql.RecordStats(pg); err != nil {
				return
			}

			connectionError := pg.Ping()

			if connectionError != nil {
				log.Error("%s", connectionError)
				return
			}

		})
	}

	return pg
}

func UpsertCoinGecko_Assets(ctx0 context.Context, assetList *[]coingecko.Coins) error {

	ctx, span := tracer.Start(ctx0, "UpsertCoinGecko_Assets")
	defer span.End()
	startTime := log.StartTime("UpsertCoinGecko_Assets")

	pg := PGConnect()

	assetListTMP := *assetList
	valueString := make([]string, 0, len(*assetList))
	valueArgs := make([]interface{}, 0, len(*assetList)*4)
	tableName := "coingecko_assets"

	var i = 0 //used for argument positions

	for y := 0; y < len(assetListTMP); y++ {

		var candleData = assetListTMP[y]

		v := reflect.ValueOf(candleData.Platforms)
		platforms := make(map[string]string)

		if v.Kind() == reflect.Map {
			for _, key := range v.MapKeys() {
				strct := v.MapIndex(key)
				platforms[fmt.Sprint(key.Interface())] = fmt.Sprint(strct.Interface())
			}
		}

		plJSON, _ := json.Marshal(platforms)

		var valString = fmt.Sprintf("($%d,$%d,$%d,$%d)", i*4+1, i*4+2, i*4+3, i*4+4)
		//pairsString = append(pairsString, fmt.Sprintf("%s/%s", candleData.Base, candleData.Quote))
		valueString = append(valueString, valString)
		valueArgs = append(valueArgs, candleData.ID)
		valueArgs = append(valueArgs, candleData.Symbol)
		valueArgs = append(valueArgs, candleData.Name)
		valueArgs = append(valueArgs, string(plJSON))

		i++

		if len(valueArgs) >= 65000 || y == len(assetListTMP)-1 {
			log.Debug("UpsertCoinGecko_Assets: Start Upserting Coingecko Assets")
			insertStatementCandles := fmt.Sprintf("INSERT INTO %s VALUES %s", tableName, strings.Join(valueString, ","))
			updateStatement := "ON CONFLICT (id) DO UPDATE SET symbol = EXCLUDED.symbol, name = EXCLUDED.name, platforms = EXCLUDED.platforms"

			query := insertStatementCandles + updateStatement
			_, inserterError := pg.ExecContext(ctx, query, valueArgs...)

			if inserterError != nil {
				log.Error("UpsertCoinGecko_Assets: Error Upserting Coingecko Assets to PostgreSQL : %s", inserterError)
			}

			valueString = make([]string, 0, len(assetListTMP))
			valueArgs = make([]interface{}, 0, len(assetListTMP)*4)

			i = 0
		}
	}
	log.EndTime("UpsertCoinGecko_Assets: Successfully finished Upserting Coingecko Assets at time : %s", startTime, nil)
	return nil
}

// returns a list of symbols
func GetCoinGeckoIDs(ctx0 context.Context) ([]string, error) {

	ctx, span := tracer.Start(ctx0, "GetCoinGeckoIDs")
	defer span.End()
	startTime := log.StartTime("GetCoinGeckoIDs")
	var coingecko_assets []string

	pg := PGConnect()

	query := `
	SELECT 
		ID from 
		(select ID FROM coingecko_assets) a
		left join
		(select symbol,market_cap from fundamentalslatest) b
		on a.ID = b.symbol
		order by market_cap desc NULLS LAST
	`

	queryResult, err := pg.QueryContext(ctx, query)

	var id string
	if err != nil {
		log.EndTime("GetCoinGeckoIDs: Error Getting CoinGecko IDs from PostgreSQL", startTime, err)
		return coingecko_assets, err
	}
	for queryResult.Next() {
		err := queryResult.Scan(&id)

		if err != nil {
			log.EndTime("GetCoinGeckoIDs: Error Mapping CoinGecko IDs from PostgreSQL", startTime, err)
			return coingecko_assets, err
		}
		coingecko_assets = append(coingecko_assets, id)
	}
	log.EndTime("GetCoinGeckoIDs: Successfully finished Getting Coingecko IDs at time : %s", startTime, nil)
	return coingecko_assets, nil
}

func UpsertAssetMetadata(ctx0 context.Context, coinList *[]coingecko.CoinsCurrentData) error {

	ctx, span := tracer.Start(ctx0, "UpsertAssetMetadata")
	defer span.End()
	startTime := log.StartTime("UpsertAssetMetadata")
	pg := PGConnect()

	coinListTMP := *coinList
	valueString := make([]string, 0, len(coinListTMP))
	totalFields := 46 //total number of columns in the postgres collection
	valueArgs := make([]interface{}, 0, len(coinListTMP)*totalFields)

	tableName := "coingecko_asset_metadata"
	var i = 0 //used for argument positions
	for y := 0; y < len(coinListTMP); y++ {
		mult := i * totalFields
		var coinData = coinListTMP[y]

		/**
		* We're generating the insert value string for the postgres query.
		*
		* e.g. Let's say a collection in postgres has 5 columns. Then this looks something like this
		* ($1,$2,$3,$4,$5),($6,$7,$8,$9,$10),(..)...
		*
		* and so on. We use these variables in the postgres query builder. In our case, we currently have 46 columns in the collection.
		 */
		var valString = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)", mult+1, mult+2, mult+3, mult+4, mult+5, mult+6, mult+7, mult+8, mult+9, mult+10, mult+11, mult+12, mult+13, mult+14, mult+15, mult+16, mult+17, mult+18, mult+19, mult+20, mult+21, mult+22, mult+23, mult+24, mult+25, mult+26, mult+27, mult+28, mult+29, mult+30, mult+31, mult+32, mult+33, mult+34, mult+35, mult+36, mult+37, mult+38, mult+39, mult+40, mult+41, mult+42, mult+43, mult+44, mult+45, mult+46)
		valueString = append(valueString, valString)

		// Please note that the order of the following appending values matter. We map the following values to the 46 variables defined in the couple of lines defined above.
		valueArgs = append(valueArgs, coinData.ID)                                                       //id
		valueArgs = append(valueArgs, coinData.Symbol)                                                   //original_symbol
		valueArgs = append(valueArgs, coinData.Description["en"])                                        //description
		valueArgs = append(valueArgs, coinData.Name)                                                     //name
		valueArgs = append(valueArgs, utils.GetMetadataFromCoin(&coinData, "website_url"))               //website_url
		valueArgs = append(valueArgs, utils.GetMetadataFromCoin(&coinData, "logo_url"))                  //logo_url
		valueArgs = append(valueArgs, utils.GetMetadataFromCoin(&coinData, "blog_url"))                  //blog_url
		valueArgs = append(valueArgs, utils.GetMetadataFromCoin(&coinData, "slack_url"))                 //slack_url
		valueArgs = append(valueArgs, utils.GetMetadataFromCoin(&coinData, "discord_url"))               //discord_url
		valueArgs = append(valueArgs, utils.GetMetadataFromCoin(&coinData, "facebook_url"))              //facebook_url
		valueArgs = append(valueArgs, utils.GetMetadataFromCoin(&coinData, "github_url"))                //github_url
		valueArgs = append(valueArgs, utils.GetMetadataFromCoin(&coinData, "bitbucket_url"))             //bitbucket_url
		valueArgs = append(valueArgs, utils.GetMetadataFromCoin(&coinData, "medium_url"))                //medium_url
		valueArgs = append(valueArgs, utils.GetMetadataFromCoin(&coinData, "reddit_url"))                //reddit_url
		valueArgs = append(valueArgs, utils.GetMetadataFromCoin(&coinData, "telegram_url"))              //telegram_url
		valueArgs = append(valueArgs, utils.GetMetadataFromCoin(&coinData, "twitter_url"))               //twitter_url
		valueArgs = append(valueArgs, utils.GetMetadataFromCoin(&coinData, "youtube_url"))               //youtube_url
		valueArgs = append(valueArgs, utils.GetMetadataFromCoin(&coinData, "whitepaper_url"))            //whitepaper_url
		valueArgs = append(valueArgs, utils.GetMetadataFromCoin(&coinData, "blockexplorer_url"))         //blockexplorer_url
		valueArgs = append(valueArgs, utils.GetMetadataFromCoin(&coinData, "bitcointalk_url"))           //bitcointalk_url
		valueArgs = append(valueArgs, utils.GetMetadataFromCoin(&coinData, "platform_currency_id"))      //platform_currency_id
		valueArgs = append(valueArgs, utils.GetMetadataFromCoin(&coinData, "platform_contract_address")) //platform_contract_address
		valueArgs = append(valueArgs, coinData.IcoData.IcoStartDate)                                     //ico_start_date
		valueArgs = append(valueArgs, coinData.IcoData.IcoEndDate)                                       //ico_end_date
		valueArgs = append(valueArgs, coinData.IcoData.TotalRaised)                                      //ico_total_raised
		valueArgs = append(valueArgs, coinData.IcoData.TotalRaisedCurrency)                              //ico_total_raised_currency
		valueArgs = append(valueArgs, coinData.PublicInterestStats.AlexaRank)                            //alexa_rank
		valueArgs = append(valueArgs, coinData.CommunityData.FacebookLikes)                              //facebook_likes
		valueArgs = append(valueArgs, coinData.CommunityData.TwitterFollowers)                           //twitter_followers
		valueArgs = append(valueArgs, coinData.CommunityData.RedditAveragePosts48H)                      //reddit_average_posts_48h
		valueArgs = append(valueArgs, coinData.CommunityData.RedditAverageComments48H)                   //reddit_average_comments_48h
		valueArgs = append(valueArgs, coinData.CommunityData.RedditSubscribers)                          //reddit_subscribers
		valueArgs = append(valueArgs, coinData.CommunityData.RedditAccountsActive48H)                    //reddit_accounts_active_48h
		valueArgs = append(valueArgs, coinData.CommunityData.TelegramChannelUserCount)                   //telegram_channel_user_count
		valueArgs = append(valueArgs, coinData.DeveloperData.Forks)                                      //repo_forks
		valueArgs = append(valueArgs, coinData.DeveloperData.Stars)                                      //repo_stars
		valueArgs = append(valueArgs, coinData.DeveloperData.Subscribers)                                //repo_subscribers
		valueArgs = append(valueArgs, coinData.DeveloperData.TotalIssues)                                //repo_total_issues
		valueArgs = append(valueArgs, coinData.DeveloperData.ClosedIssues)                               //repo_closed_issues
		valueArgs = append(valueArgs, coinData.DeveloperData.PullRequestsMerged)                         //repo_pull_requests_merged
		valueArgs = append(valueArgs, coinData.DeveloperData.PullRequestContributors)                    //repo_pull_request_contributors
		valueArgs = append(valueArgs, coinData.DeveloperData.CodeAdditionsDeletions4Weeks.Additions)     //repo_code_additions_4_weeks
		valueArgs = append(valueArgs, coinData.DeveloperData.CodeAdditionsDeletions4Weeks.Deletions)     //repo_code_deletions_4_weeks
		valueArgs = append(valueArgs, coinData.DeveloperData.CommitCount4Weeks)                          //repo_commit_count_4_weeks

		valueArgs = append(valueArgs, utils.NewNullString(coinData.GenesisDate)) //genesis_date
		valueArgs = append(valueArgs, coinData.LastUpdated)                      //last_updated
		i++

		if len(valueArgs) >= 65000 || y == len(coinListTMP)-1 {
			log.Debug("UpsertAssetMetadata: Start Upserting Asset Metadata")
			insertStatementCoins := fmt.Sprintf("INSERT INTO %s VALUES %s", tableName, strings.Join(valueString, ","))

			//To make our query upsert, we use this conflict resolution.
			updateStatement := "ON CONFLICT (id) DO UPDATE SET original_symbol = EXCLUDED.original_symbol, description = EXCLUDED.description, name = EXCLUDED.name, website_url = EXCLUDED.website_url, logo_url = EXCLUDED.logo_url, blog_url = EXCLUDED.blog_url, slack_url = EXCLUDED.slack_url, discord_url = EXCLUDED.discord_url, facebook_url = EXCLUDED.facebook_url, github_url = EXCLUDED.github_url, bitbucket_url = EXCLUDED.bitbucket_url, medium_url = EXCLUDED.medium_url, reddit_url = EXCLUDED.reddit_url, telegram_url = EXCLUDED.telegram_url, twitter_url = EXCLUDED.twitter_url, youtube_url = EXCLUDED.youtube_url, whitepaper_url = EXCLUDED.whitepaper_url, blockexplorer_url = EXCLUDED.blockexplorer_url, bitcointalk_url = EXCLUDED.bitcointalk_url, platform_currency_id = EXCLUDED.platform_currency_id, platform_contract_address = EXCLUDED.platform_contract_address, ico_start_date = EXCLUDED.ico_start_date, ico_end_date = EXCLUDED.ico_end_date, ico_total_raised = EXCLUDED.ico_total_raised, ico_total_raised_currency = EXCLUDED.ico_total_raised_currency, alexa_rank = EXCLUDED.alexa_rank, facebook_likes = EXCLUDED.facebook_likes, twitter_followers = EXCLUDED.twitter_followers, reddit_average_posts_48h = EXCLUDED.reddit_average_posts_48h, reddit_average_comments_48h = EXCLUDED.reddit_average_comments_48h, reddit_subscribers = EXCLUDED.reddit_subscribers, reddit_accounts_active_48h = EXCLUDED.reddit_accounts_active_48h, telegram_channel_user_count = EXCLUDED.telegram_channel_user_count, repo_forks = EXCLUDED.repo_forks, repo_stars = EXCLUDED.repo_stars, repo_subscribers = EXCLUDED.repo_subscribers, repo_total_issues = EXCLUDED.repo_total_issues, repo_closed_issues = EXCLUDED.repo_closed_issues, repo_pull_requests_merged = EXCLUDED.repo_pull_requests_merged, repo_pull_request_contributors = EXCLUDED.repo_pull_request_contributors, repo_code_additions_4_weeks = EXCLUDED.repo_code_additions_4_weeks, repo_code_deletions_4_weeks = EXCLUDED.repo_code_deletions_4_weeks, repo_commit_count_4_weeks = EXCLUDED.repo_commit_count_4_weeks, last_updated = EXCLUDED.last_updated"

			query := insertStatementCoins + updateStatement
			_, inserterError := pg.ExecContext(ctx, query, valueArgs...)

			if inserterError != nil {
				log.Error("UpsertAssetMetadata: Error Upserting Asset Metadata to PostgreSQL : %s", inserterError)
			}

			valueString = make([]string, 0, len(coinListTMP))
			valueArgs = make([]interface{}, 0, len(coinListTMP)*totalFields)

			i = 0
		}
	}
	log.EndTime("UpsertAssetMetadata: Successfully finished Upserting Asset Metadata at time : %s", startTime, nil)
	return nil
}

// Upsert exchange metadata from coingecko
func UpsertExchangeMetadata(ctx0 context.Context, exchangeList *[]coingecko.FullExchange) error {

	ctx, span := tracer.Start(ctx0, "UpsertExchangeMetadata")
	defer span.End()
	startTime := log.StartTime("UpsertExchangeMetadata")
	pg := PGConnect()

	exchangeListTMP := *exchangeList
	valueString := make([]string, 0, len(exchangeListTMP))
	totalFields := 24 //total number of columns in the postgres collection
	valueArgs := make([]interface{}, 0, len(exchangeListTMP)*totalFields)

	tableName := "coingecko_exchange_metadata"
	var i = 0 //used for argument positions
	for y := 0; y < len(exchangeListTMP); y++ {
		mult := i * totalFields
		var exchangeData = exchangeListTMP[y]

		/**
		* We're generating the insert value string for the postgres query.
		*
		* e.g. Let's say a collection in postgres has 5 columns. Then this looks something like this
		* ($1,$2,$3,$4,$5),($6,$7,$8,$9,$10),(..)...
		*
		* and so on. We use these variables in the postgres query builder. In our case, we currently have 46 columns in the collection.
		 */
		var valString = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)", mult+1, mult+2, mult+3, mult+4, mult+5, mult+6, mult+7, mult+8, mult+9, mult+10, mult+11, mult+12, mult+13, mult+14, mult+15, mult+16, mult+17, mult+18, mult+19, mult+20, mult+21, mult+22, mult+23, mult+24)
		valueString = append(valueString, valString)

		// Please note that the order of the following appending values matter. We map the following values to the 46 variables defined in the couple of lines defined above.
		valueArgs = append(valueArgs, exchangeData.ID)                                              //id
		valueArgs = append(valueArgs, exchangeData.Name)                                            //name
		valueArgs = append(valueArgs, exchangeData.YearEstablished)                                 //year
		valueArgs = append(valueArgs, exchangeData.Description)                                     //description
		valueArgs = append(valueArgs, exchangeData.Country)                                         //location
		valueArgs = append(valueArgs, exchangeData.Image)                                           //logo_url
		valueArgs = append(valueArgs, exchangeData.URL)                                             //website_url
		valueArgs = append(valueArgs, utils.GetMetadataFromExchange(&exchangeData, "twitter_url"))  //twitter_url
		valueArgs = append(valueArgs, exchangeData.FacebookURL)                                     //facebook_url
		valueArgs = append(valueArgs, utils.GetMetadataFromExchange(&exchangeData, "youtube_url"))  //youtube_url
		valueArgs = append(valueArgs, utils.GetMetadataFromExchange(&exchangeData, "linkedin_url")) //linkedin_url
		valueArgs = append(valueArgs, exchangeData.RedditURL)                                       //reddit_url
		valueArgs = append(valueArgs, utils.GetMetadataFromExchange(&exchangeData, "chat_url"))     //chat_url
		valueArgs = append(valueArgs, exchangeData.SlackURL)                                        //slack_url
		valueArgs = append(valueArgs, exchangeData.TelegramURL)                                     //telegram_url
		valueArgs = append(valueArgs, utils.GetMetadataFromExchange(&exchangeData, "blog_url"))     //blog_url
		valueArgs = append(valueArgs, exchangeData.Centralized)                                     //centralized
		valueArgs = append(valueArgs, !exchangeData.Centralized)                                    //decentralized
		valueArgs = append(valueArgs, exchangeData.HasTradingIncentive)                             //has_trading_incentive
		valueArgs = append(valueArgs, exchangeData.TrustScore)                                      //trust_score
		valueArgs = append(valueArgs, exchangeData.TrustScoreRank)                                  //trust_score_rank
		valueArgs = append(valueArgs, exchangeData.TradeVolume24HBtc)                               //trade_volume_24h_btc
		valueArgs = append(valueArgs, exchangeData.TradeVolume24HBtcNormalized)                     //trade_volume_24h_btc_normalized
		valueArgs = append(valueArgs, time.Now())                                                   //last_updated
		i++

		if len(valueArgs) >= 65000 || y == len(exchangeListTMP)-1 {
			log.Debug("UpsertExchangeMetadata: Start Upserting Exchange Metadata")
			insertStatementCoins := fmt.Sprintf("INSERT INTO %s VALUES %s", tableName, strings.Join(valueString, ","))

			//To make our query upsert, we use this conflict resolution.
			updateStatement := "ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, year = EXCLUDED.year, description = EXCLUDED.description, location = EXCLUDED.location, logo_url = EXCLUDED.logo_url, website_url = EXCLUDED.website_url, twitter_url = EXCLUDED.twitter_url, facebook_url = EXCLUDED.facebook_url, youtube_url = EXCLUDED.youtube_url, linkedin_url = EXCLUDED.linkedin_url, reddit_url = EXCLUDED.reddit_url, chat_url = EXCLUDED.chat_url, slack_url = EXCLUDED.slack_url, telegram_url = EXCLUDED.telegram_url, blog_url = EXCLUDED.blog_url, centralized = EXCLUDED.centralized, decentralized = EXCLUDED.decentralized, has_trading_incentive = EXCLUDED.has_trading_incentive, trust_score = EXCLUDED.trust_score, trust_score_rank = EXCLUDED.trust_score_rank, trade_volume_24h_btc = EXCLUDED.trade_volume_24h_btc, trade_volume_24h_btc_normalized = EXCLUDED.trade_volume_24h_btc_normalized, last_updated = EXCLUDED.last_updated"

			query := insertStatementCoins + updateStatement
			_, inserterError := pg.ExecContext(ctx, query, valueArgs...)

			if inserterError != nil {
				log.Error("UpsertExchangeMetadata: Error Upserting Exchange Metadata to PostgreSQL : %s", inserterError)
			}

			valueString = make([]string, 0, len(exchangeListTMP))
			valueArgs = make([]interface{}, 0, len(exchangeListTMP)*totalFields)

			i = 0
		}
	}
	log.EndTime("UpsertExchangeMetadata: Successfully finished Upserting Exchange Metadata at time : %s", startTime, nil)
	return nil
}

func UpsertCoinGeckoExchanges(ctx0 context.Context, exchangesList *[]coingecko.ExchangeListShort) error {

	ctx, span := tracer.Start(ctx0, "UpsertCoinGeckoExchanges")
	defer span.End()
	startTime := log.StartTime("UpsertCoinGeckoExchanges")
	pg := PGConnect()

	exchangesListTMP := *exchangesList
	valueString := make([]string, 0, len(*exchangesList))
	for y := 0; y < len(exchangesListTMP); y++ {
		log.Debug("UpsertCoinGeckoExchanges: Start Upserting CoinGecko Exchanges")
		var exchange = exchangesListTMP[y]
		var valString = fmt.Sprintf("('%s','%s')::coingecko_exchange", exchange.ID, exchange.Name)
		valueString = append(valueString, valString)
	}
	exchangesData := strings.Join(valueString, ",")

	exchangeStoredProc := fmt.Sprintf("CALL upsertCoingeckoExchanges(ARRAY[%s])", exchangesData)
	_, inserterError := pg.ExecContext(ctx, exchangeStoredProc)
	if inserterError != nil {
		log.Error("UpsertCoinGeckoExchanges: Error Upserting Coingecko Exchanges to PostgreSQL : %s", inserterError)
	}
	log.EndTime("UpsertCoinGeckoExchanges: Successfully finished Upserting Coingecko Exchanges at time : %s", startTime, nil)
	return nil
}

// returns a list of symbols
func GetCoinGeckoRate(ctx0 context.Context) (models.CoingeckoCount, error) {

	ctx, span := tracer.Start(ctx0, "GetCoinGeckoRate")
	defer span.End()
	startTime := log.StartTime("GetCoinGeckoRate")

	var coingeckoCount models.CoingeckoCount

	pg := PGConnect()

	query := `
		SELECT 
			*
		FROM coingecko_counthistory 
		order by last_updated desc 
		limit 1
	`

	queryResult, err := pg.QueryContext(ctx, query)

	if err != nil {
		log.EndTime("GetCoinGeckoRate: Error Getting CoinGecko Rate from PostgreSQL", startTime, err)
		return coingeckoCount, err
	}
	for queryResult.Next() {
		err := queryResult.Scan(&coingeckoCount.Count, &coingeckoCount.LastUpdated)

		if err != nil {
			log.EndTime("GetCoinGeckoRate: Error Mapping CoinGecko Rate from PostgreSQL", startTime, err)
			return coingeckoCount, err
		}
	}
	//If we dont have a valid result (This should only happen at first deploy), or the current day is after a month
	if coingeckoCount.Count == 0 && coingeckoCount.LastUpdated.IsZero() || time.Now().Month() != coingeckoCount.LastUpdated.Month() {
		coingeckoCount.LastUpdated = time.Now()
		coingeckoCount.Count = 0
		SaveCGRate(ctx, coingeckoCount)
	}
	log.EndTime("GetCoinGeckoRate: Successfully finished Getting CoinGecko Rate at time : %s", startTime, nil)
	return coingeckoCount, nil
}

// returns a list of symbols
func SaveCGRate(ctx0 context.Context, cgCount models.CoingeckoCount) error {

	ctx, span := tracer.Start(ctx0, "SaveCGRate")
	defer span.End()
	startTime := log.StartTime("SaveCGRate")
	pg := PGConnect()

	log.Debug("SaveCGRate: Start Upserting Coingecko Rate")
	_, err := pg.ExecContext(ctx, "call upsertCGCount(($1,$2)::coingecko_counthist)", cgCount.Count, cgCount.LastUpdated)

	if err != nil {
		log.EndTime("SaveCGRate: Error Upserting Coingecko Rate to PostgreSQL", startTime, err)
		return err
	}

	log.EndTime("SaveCGRate: Successfully finished Upserting Coingecko Rate at time : %s", startTime, nil)
	return nil
}

func GetExchangesList(ctx0 context.Context) ([]string, error) {

	ctx, span := tracer.Start(ctx0, "GetExchangesList")
	defer span.End()
	startTime := log.StartTime("GetExchangesList")
	pg := PGConnect()

	var exchangesIds []string
	query := `
		SELECT id from public.getCoinGeckoExchangesList()
	`
	queryResult, err := pg.QueryContext(ctx, query)
	if err != nil {
		log.EndTime("GetExchangesList: Error Getting Coingecko Exchanges List from PostgreSQL", startTime, err)
		return nil, err
	}

	for queryResult.Next() {
		var exchange models.ExchangeList
		err := queryResult.Scan(&exchange.ID)
		if err != nil {
			log.EndTime("GetExchangesList: Error Mapping Coingecko Exchanges List from PostgreSQL", startTime, err)
			return nil, err
		}
		exchangesIds = append(exchangesIds, exchange.ID)
	}
	log.EndTime("GetExchangesList: Successfully finished Getting Coingecko Exchanges List at time : %s", startTime, nil)
	return exchangesIds, nil

}

// if we don't need to add CGExchangesTickers to PG this function will be removed
func UpsertCoinGeckoExchangesTickers(ctx0 context.Context, exchangesTickers *[]coingecko.ExchangesTickers) error {
	ctx, span := tracer.Start(ctx0, "UpsertCoinGeckoExchangesTickers")
	defer span.End()
	startTime := log.StartTime("UpsertCoinGeckoExchangesTickers")
	pg := PGConnect()
	exchangesTickersTMP := *exchangesTickers
	valueString := make([]string, 0, len(*exchangesTickers))
	for y := 0; y < len(exchangesTickersTMP); y++ {
		log.Debug("UpsertCoinGeckoExchangesTickers: Start Building Coingecko Exchanges Tickers")
		var exchange = exchangesTickersTMP[y]
		var valString = fmt.Sprintf("('%s', json('%v'))::coingecko_exchanges_tickers", exchange.Name, exchange.Tickers)
		valueString = append(valueString, valString)
	}

	exchangesTickersData := strings.Join(valueString, ",")
	log.Debug("UpsertCoinGeckoExchangesTickers: Start Upserting Coingecko Exchanges Tickers")
	_, inserterError := pg.ExecContext(ctx, "CALL upsertCoingeckoExchangesTickers(ARRAY[%s])", exchangesTickersData)
	if inserterError != nil {
		log.Error("UpsertCoinGeckoExchangesTickers: Error Upsert Coingecko Exchanges Tickers to PostgreSQL : %s", inserterError)
	}
	log.EndTime("UpsertCoinGeckoExchangesTickers: Successfully finished Upserting Coingecko Exchanges Tickers at time : %s", startTime, nil)
	return nil
}

// returns a list of exchange ids based on trust score desc.
// x is defined in the stored procedure so we can adjust without updating code
func GetxExchangeIDsByTrust(ctx0 context.Context) ([]string, error) {
	ctx, span := tracer.Start(ctx0, "GetExchangeIDsByTrust")
	defer span.End()
	startTime := log.StartTime("GetExchangeIDsByTrust")
	var coingecko_assets []string

	pg := PGConnect()

	query := `
		SELECT 
			id 
		FROM getxexchangeidsbytrust()
	`

	queryResult, err := pg.QueryContext(ctx, query)

	var id string
	if err != nil {
		log.EndTime("GetExchangeIDsByTrust: Error Getting CoinGecko ExchangeIDs By Trust from PostgreSQL", startTime, err)
		return coingecko_assets, err
	}
	for queryResult.Next() {
		err := queryResult.Scan(&id)

		if err != nil {
			log.EndTime("GetExchangeIDsByTrust: Error Mapping CoinGecko ExchangeIDs By Trust from PostgreSQL", startTime, err)
			return coingecko_assets, err
		}
		coingecko_assets = append(coingecko_assets, id)
	}
	log.EndTime("GetExchangeIDsByTrust: Successfully finished Getting CoinGecko ExchangeIDs By Trust at time : %s", startTime, nil)
	return coingecko_assets, nil
}

// Get Dynamic Description for Traded Assets Page
func GetDynamicDescription(ctx0 context.Context, labels map[string]string) (*models.Global, error) {
	// Starts new child span from the parent span in the context.
	ctx, span := tracer.Start(ctx0, "GetDynamicDescription")
	defer span.End()
	startTime := log.StartTimeL(labels, "GetDynamicDescription")

	pg := PGConnect()

	// Will MAp the Global Data from Coingecko
	var globalDescription models.Global
	span.AddEvent("Query Executed")
	queryResult, err := pg.QueryContext(ctx, `
	select
		array_to_json(ARRAY_AGG(json_build_object('Name', name, 'Slug', slug, 'change_24h', percentage_volume_1d))) as trending
	from(
			select 
				name,
				slug,
				percentage_volume_1d
			from 
				public.tradedAssetsPagination_BySource_1(100,0,'market_cap','desc','coingecko')
			order by 
				percentage_volume_1d desc
			limit 2
		) as fo
		`)

	if err != nil {
		log.EndTimeL(labels, "GetDynamicDescription: Error Getting Dynamic Description Data from PostgreSQL", startTime, err)
		span.SetStatus(codes.Error, "GetDynamicDescription: Error Getting Dynamic Description Data from PG")
		return nil, err
	}
	defer queryResult.Close()

	for queryResult.Next() {

		err := queryResult.Scan((*trendingResult)(&globalDescription.Trending))

		if err != nil {
			log.EndTimeL(labels, "GetDynamicDescription: Error Mapping Dynamic Description Data from PostgreSQL", startTime, err)
			span.SetStatus(codes.Error, "GetDynamicDescription: Dynamic Description Data Scan error")
			return nil, err
		}

	}
	log.EndTimeL(labels, "GetDynamicDescription: Successfully finished Getting Dynamic Description", startTime, nil)
	return &globalDescription, nil
}

func InsertGlobalDescription(ctx0 context.Context, labels map[string]string, globalData *models.Global) error {
	ctx, span := tracer.Start(ctx0, "InsertGlobalDescription")
	defer span.End()

	startTime := log.StartTimeL(labels, "InsertGlobalDescription")

	pg := PGConnect()
	insertStatementsFundamentals := "CALL InsertGlobalDescription($1, $2, $3, $4, $5, $6, $7, $8)"

	query := insertStatementsFundamentals
	// convert Trending[] and Dominance into json type to make it easy to store in PG table
	trending, _ := json.Marshal(globalData.Trending)
	dominance, _ := json.Marshal(globalData.Dominance)
	span.AddEvent("Insert Global Description To PG")
	_, insertError := pg.ExecContext(ctx, query, globalData.MarketCap, globalData.Change24H, globalData.Volume24H, dominance, globalData.AssetCount, trending, globalData.LastUpdated, globalData.Type)

	if insertError != nil {
		log.EndTimeL(labels, "InsertGlobalDescription: Error Insert Global Description to PostgreSQL", startTime, insertError)
		span.SetStatus(codes.Error, insertError.Error())
		return insertError
	}

	log.EndTimeL(labels, "InsertGlobalDescription: Successfully finished Inserting Global Description", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return nil
}

// Upsert Categories Data to PG
func UpsertCoinGeckoCategoriesData(ctx0 context.Context, categoriesData []models.CategoriesData) error {
	ctx, span := tracer.Start(ctx0, "UpsertCoinGeckoCategoriesData")
	defer span.End()
	startTime := log.StartTime("UpsertCoinGeckoCategoriesData")
	pg := PGConnect()
	categoriesDataTMP := categoriesData
	valueString := make([]string, 0, len(categoriesData))
	valueArgs := make([]interface{}, 0, len(categoriesData)*9)
	tableName := "coingecko_categories"
	var i = 0
	span.AddEvent("UpsertCoinGeckoCategoriesData: Starting Inserting Categories Data to PG")
	for y := 0; y < len(categoriesDataTMP); y++ {
		var category = categoriesDataTMP[y]
		var valString = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)", i*9+1, i*9+2, i*9+3, i*9+4, i*9+5, i*9+6, i*9+7, i*9+8, i*9+9)
		valueString = append(valueString, valString)

		valueArgs = append(valueArgs, category.ID)
		valueArgs = append(valueArgs, category.Name)
		valueArgs = append(valueArgs, category.MarketCap)
		valueArgs = append(valueArgs, category.MarketCapChange24H)
		valueArgs = append(valueArgs, category.Content)
		valueArgs = append(valueArgs, pq.Array(category.Top3Coins))
		valueArgs = append(valueArgs, category.Volume24H)
		markets, _ := json.Marshal(category.Markets)
		valueArgs = append(valueArgs, markets)
		valueArgs = append(valueArgs, category.UpdatedAt)
		i++

		if len(valueArgs) >= 65000 || y == len(categoriesDataTMP)-1 {
			log.Debug("UpsertCoinGeckoCategoriesData: Start Upserting Categories Data")
			insertStatementCoins := fmt.Sprintf("INSERT INTO %s VALUES %s", tableName, strings.Join(valueString, ","))
			updateStatement := "on conflict (id) DO UPDATE SET id=EXCLUDED.id, name=EXCLUDED.name, market_cap=EXCLUDED.market_cap, market_cap_change_24h=EXCLUDED.market_cap_change_24h, content=EXCLUDED.content, top_3_coins=EXCLUDED.top_3_coins, volume_24h=EXCLUDED.volume_24h, markets=EXCLUDED.markets, last_updated = EXCLUDED.last_updated"
			query := insertStatementCoins + updateStatement

			_, inserterError := pg.ExecContext(ctx, query, valueArgs...)

			if inserterError != nil {
				log.Error("UpsertCoinGeckoCategoriesData: Error Upserting Categories Data to PostgreSQL : %s", inserterError)
				span.SetStatus(codes.Error, "UpsertCoinGeckoCategoriesData: Error Upserting Categories")
			}

			valueString = make([]string, 0, len(categoriesDataTMP))
			valueArgs = make([]interface{}, 0, len(categoriesDataTMP)*9)

			i = 0
		}
	}
	log.EndTime("UpsertCoinGeckoCategoriesData: Successfully finished Upserting Categories Data at time : %s", startTime, nil)
	return nil
}

// update assets metadata tags
func UpdateAssetsMetaData(ctx0 context.Context, assetsData map[string][]string) error {
	ctx, span := tracer.Start(ctx0, "UpdateAssetsMetaData")
	defer span.End()
	startTime := log.StartTime("UpdateAssetsMetaData")
	pg := PGConnect()
	valueString := make([]string, 0, len(assetsData))
	for key, value := range assetsData {
		tags := strings.Join(value, ",")
		var valString = fmt.Sprintf("('%s', ARRAY['%s'])::assets_tags_data", key, tags)
		valueString = append(valueString, valString)
	}
	assetsMetaData := strings.Join(valueString, ",")
	assetsMetadataProc := fmt.Sprintf("CALL UpdateAssetsMetadata(ARRAY[%s])", assetsMetaData)
	span.AddEvent("UpdateAssetsMetaData: Starting Updating Tags for Assets Metadata PG")
	log.Debug("UpdateAssetsMetaData: Start Updating Tags for Assets Metadata")
	_, inserterError := pg.ExecContext(ctx, assetsMetadataProc)
	if inserterError != nil {
		log.Error("UpdateAssetsMetaData: Error Updating Assets Metadata Tags to PostgreSQL : %s", inserterError)
		span.SetStatus(codes.Error, "UpdateAssetsMetaData: Error Updating Assets Metadata Tags to PostgreSQL")
	}
	log.EndTime("UpdateAssetsMetaData: Successfully finished Updating Assets Metadata at time : %s", startTime, nil)
	return nil
}

// Get Dynamic Description Dominance Data for Bitcoin and Ethereum
func GetDynamicDescriptionDominanceData(ctx0 context.Context, labels map[string]string) (map[string]models.DominanceAssetsData, error) {
	// Starts new child span from the parent span in the context.
	ctx, span := tracer.Start(ctx0, "GetDynamicDescriptionDominanceData")
	defer span.End()
	startTime := log.StartTime("GetDynamicDescriptionDominanceData")

	pg := PGConnect()

	// Will Map the Global Data from Coingecko
	assetsData := make(map[string]models.DominanceAssetsData)

	queryResult, err := pg.QueryContext(ctx, `
		select 
			name, 
			slug,
			display_symbol
		from 
			fundamentalslatest
		where 
			symbol in ('bitcoin', 'ethereum')
		order by 
			name asc 
		limit 2
		`)

	span.AddEvent("Query Executed")

	if err != nil {
		log.EndTimeL(labels, "GetDynamicDescriptionDominanceData: Error Getting Dynamic Description Dominance Data from PostgreSQL", startTime, err)
		span.SetStatus(codes.Error, "GetDynamicDescriptionDominanceData: Error Getting Dynamic Description Dominance Data from PostgreSQL")
		return nil, err
	}
	defer queryResult.Close()

	for queryResult.Next() {
		var assetData models.DominanceAssetsData
		err := queryResult.Scan(&assetData.Name, &assetData.Slug, &assetData.DisplaySymbol)

		if err != nil {
			log.EndTimeL(labels, "GetDynamicDescriptionDominanceData: Error Mapping Dynamic Description Dominance Data from PostgreSQL", startTime, err)
			span.SetStatus(codes.Error, "GetDynamicDescriptionDominanceData: Error Mapping Dynamic Description Dominance Data from PostgreSQL")
			return nil, err
		}
		assetsData[assetData.DisplaySymbol] = assetData

	}
	log.EndTimeL(labels, "GetDynamicDescriptionDominanceData: Successfully finished Getting Dynamic Description Dominance Data at time : %s", startTime, nil)
	return assetsData, nil
}

/*
formatSlug: Takes a symbol, and asset name to create a slug.
Rules:
 1. the result will be name-symbol
 2. any "." will be rplaced with "-"
 3. Remove Special Characters excluding "-"
 4. shorten any repeating "-" to a singular -
*/
func formatSlug(assetName string, assetSymbol string) string {

	sanitizedSlug := strings.ToLower(strings.Replace(fmt.Sprintf("%s-%s", assetName, assetSymbol), " ", "-", -1))

	re := regexp.MustCompile(`[.]`) // replace .'s with -
	sanitizedSlug = re.ReplaceAllString(sanitizedSlug, "-")

	re = regexp.MustCompile(`[^a-zA-Z0-9äöüÄÖÜßμ-]`) // replace all characters thata are not alpha numeric. The exception is '-'
	sanitizedSlug = re.ReplaceAllString(sanitizedSlug, "")
	re = regexp.MustCompile(`-{2,}`) // replace 2 or more of '-' that occures in a row
	sanitizedSlug = re.ReplaceAllString(sanitizedSlug, "-")

	sanitizedSlug = strings.Trim(sanitizedSlug, "-") //trim any slugs with - that trail or lead

	return sanitizedSlug
}

/*
Upserts all finacial data for an NFT collection, along with its id, symbol, and slug
*/
func UpsertNFTData(ctx0 context.Context, nftList *coingecko.NFTMarketsList) error {

	ctx, span := tracer.Start(ctx0, "UpsertNFTData")
	defer span.End()
	startTime := log.StartTime("UpsertNFTData")
	pg := PGConnect()

	exchangeListTMP := *nftList
	valueString := make([]string, 0, len(exchangeListTMP))
	totalFields := 29 //total number of columns in the postgres collection
	valueArgs := make([]interface{}, 0, len(exchangeListTMP)*totalFields)
	var idsInserted []string
	tableName := "NFTDataLatest"
	var i = 0 //used for argument positions
	for y := 0; y < len(exchangeListTMP); y++ {
		mult := i * totalFields
		var nftData = exchangeListTMP[y]
		if nftData.ID == "" {
			nftData.ID = nftData.Name
		}
		if !slices.Contains(idsInserted, nftData.ID) {
			idsInserted = append(idsInserted, nftData.ID)

			/**
			* We're generating the insert value string for the postgres query.
			*
			* e.g. Let's say a collection in postgres has 5 columns. Then this looks something like this
			* ($1,$2,$3,$4,$5),($6,$7,$8,$9,$10),(..)...
			*
			* and so on. We use these variables in the postgres query builder. In our case, we currently have 46 columns in the collection.
			 */
			var valString = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)", mult+1, mult+2, mult+3, mult+4, mult+5, mult+6, mult+7, mult+8, mult+9, mult+10, mult+11, mult+12, mult+13, mult+14, mult+15, mult+16, mult+17, mult+18, mult+19, mult+20, mult+21, mult+22, mult+23, mult+24, mult+25, mult+26, mult+27, mult+28, mult+29)
			valueString = append(valueString, valString)

			// Please note that the order of the following appending values matter. We map the following values to the 46 variables defined in the couple of lines defined above.
			//Percentages are divided by 100 since to keep with the consistency of our fungible token data which is decimal form
			//This is also a bug fix linked to https://forbesmedia.atlassian.net/browse/FDA-3033
			valueArgs = append(valueArgs, nftData.ID)
			valueArgs = append(valueArgs, nftData.ContractAddress)
			valueArgs = append(valueArgs, nftData.AssetPlatformID)
			valueArgs = append(valueArgs, nftData.Name)
			valueArgs = append(valueArgs, nftData.Symbol)
			large_image := strings.ReplaceAll(nftData.Image.Small, "/small/", "/large/")
			valueArgs = append(valueArgs, nftData.Image.Small)
			valueArgs = append(valueArgs, large_image)
			valueArgs = append(valueArgs, nftData.Description)
			valueArgs = append(valueArgs, nftData.NativeCurrency)
			valueArgs = append(valueArgs, nftData.FloorPrice.Usd)
			valueArgs = append(valueArgs, nftData.MarketCap.Usd)
			valueArgs = append(valueArgs, nftData.Volume24H.Usd)
			valueArgs = append(valueArgs, nftData.FloorPrice.NativeCurrency)
			valueArgs = append(valueArgs, nftData.MarketCap.NativeCurrency)
			valueArgs = append(valueArgs, nftData.Volume24H.NativeCurrency)
			valueArgs = append(valueArgs, nftData.FloorPriceInUsd24HPercentageChange/100)
			valueArgs = append(valueArgs, nftData.NumberOfUniqueAddresses)
			valueArgs = append(valueArgs, nftData.NumberOfUniqueAddresses24HPercentageChange/100)
			valueArgs = append(valueArgs, nftData.TotalSupply)
			valueArgs = append(valueArgs, formatSlug(nftData.Name, nftData.Symbol)) // Slug
			//URLS are ignored since the coingecko call /nfts/markets does not return them. These will not be updated in the insert query
			valueArgs = append(valueArgs, "")         //website_url
			valueArgs = append(valueArgs, "")         //twitter_url
			valueArgs = append(valueArgs, "")         //discordurl_url
			valueArgs = append(valueArgs, time.Now()) //last_updated
			valueArgs = append(valueArgs, nftData.NativeCurrencySymbol)
			valueArgs = append(valueArgs, (nftData.MarketCap24HPercentageChange.Usd / 100))
			valueArgs = append(valueArgs, (nftData.MarketCap24HPercentageChange.NativeCurrency / 100))
			valueArgs = append(valueArgs, (nftData.Volume24HPercentageChange.Usd / 100))
			valueArgs = append(valueArgs, (nftData.Volume24HPercentageChange.NativeCurrency / 100))
			i++

		}
		if len(valueArgs) >= 65000 || y == len(exchangeListTMP)-1 {
			log.Debug("UpsertNFTData: Start Upserting NFTs Data")
			insertStatementCoins := fmt.Sprintf(`INSERT INTO %s (id, contract_address, asset_platform_id, name, symbol, image, large_image, description, native_currency, floor_price_usd, market_cap_usd, volume_24h_usd, floor_price_native, market_cap_native, volume_24h_native, floor_price_in_usd_24h_percentage_change, number_of_unique_addresses, number_of_unique_addresses_24h_percentage_change, total_supply, slug, website_url, twitter_url, discord_url, last_updated, native_currency_symbol, market_cap_24h_percentage_change_usd, market_cap_24h_percentage_change_native, volume_24h_percentage_change_usd, volume_24h_percentage_change_native) VALUES %s`, tableName, strings.Join(valueString, ","))

			//To make our query upsert, we use this conflict resolution.
			updateStatement := "ON CONFLICT (id) DO UPDATE SET contract_address = EXCLUDED.contract_address, asset_platform_id = EXCLUDED.asset_platform_id, name = EXCLUDED.name, description = EXCLUDED.description, native_currency = EXCLUDED.native_currency, native_currency_symbol = EXCLUDED.native_currency_symbol, floor_price_usd = EXCLUDED.floor_price_usd, market_cap_usd = EXCLUDED.market_cap_usd, market_cap_24h_percentage_change_usd = EXCLUDED.market_cap_24h_percentage_change_usd, volume_24h_usd = EXCLUDED.volume_24h_usd, volume_24h_percentage_change_usd = EXCLUDED.volume_24h_percentage_change_usd, floor_price_native = EXCLUDED.floor_price_native, market_cap_native = EXCLUDED.market_cap_native, market_cap_24h_percentage_change_native = EXCLUDED.market_cap_24h_percentage_change_native, volume_24h_native = EXCLUDED.volume_24h_native, volume_24h_percentage_change_native = EXCLUDED.volume_24h_percentage_change_native, floor_price_in_usd_24h_percentage_change = EXCLUDED.floor_price_in_usd_24h_percentage_change, number_of_unique_addresses = EXCLUDED.number_of_unique_addresses, number_of_unique_addresses_24h_percentage_change = EXCLUDED.number_of_unique_addresses_24h_percentage_change, total_supply = EXCLUDED.total_supply,slug = EXCLUDED.slug, symbol = EXCLUDED.symbol, image = EXCLUDED.image, large_image = EXCLUDED.large_image, last_updated = EXCLUDED.last_updated"
			query := insertStatementCoins + updateStatement
			_, inserterError := pg.ExecContext(ctx, query, valueArgs...)

			if inserterError != nil {
				log.Error("UpsertNFTData: Error Upserting NFTs Data to PostgreSQL : %s", inserterError)
			}

			valueString = make([]string, 0, len(exchangeListTMP))
			valueArgs = make([]interface{}, 0, len(exchangeListTMP)*totalFields)

			i = 0
		}
	}
	log.EndTime("UpsertNFTData: Successfully finished Upserting NFTs Data at time : %s", startTime, nil)
	return nil
}

/*
 Upserts Metadata to the NFDatalatest table
 website_url
 twitter_url
 discord_url
*/

func UpsertNFTMetaData(ctx0 context.Context, nftdata *[]coingecko.NFTMarket) error {

	ctx, span := tracer.Start(ctx0, "UpsertNFTMetaData")
	defer span.End()
	startTime := log.StartTime("UpsertNFTMetaData")
	pg := PGConnect()

	exchangeListTMP := *nftdata
	valueString := make([]string, 0, len(exchangeListTMP))
	totalFields := 5 //total number of columns in the postgres collection
	valueArgs := make([]interface{}, 0, len(exchangeListTMP)*totalFields)
	var idsInserted []string
	tableName := "NFTDataLatest"
	var i = 0 //used for argument positions
	for y := 0; y < len(exchangeListTMP); y++ {
		mult := i * totalFields
		var nftData = exchangeListTMP[y]
		if nftData.ID == "" {
			nftData.ID = nftData.Name
		}
		idsInserted = append(idsInserted, nftData.ID)

		/**
		* We're generating the insert value string for the postgres query.
		*
		* e.g. Let's say a collection in postgres has 5 columns. Then this looks something like this
		* ($1,$2,$3,$4,$5),($6,$7,$8,$9,$10),(..)...
		*
		* and so on. We use these variables in the postgres query builder. In our case, we currently have 46 columns in the collection.
		 */
		var valString = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d)", mult+1, mult+2, mult+3, mult+4, mult+5)
		valueString = append(valueString, valString)

		// Please note that the order of the following appending values matter. We map the following values to the 46 variables defined in the couple of lines defined above.
		valueArgs = append(valueArgs, nftData.ID)
		valueArgs = append(valueArgs, nftData.Links.Homepage) //website_url
		valueArgs = append(valueArgs, nftData.Links.Twitter)  //twitter_url
		valueArgs = append(valueArgs, nftData.Links.Discord)  //discord_url
		explorers, _ := json.Marshal(nftData.Explorers)
		valueArgs = append(valueArgs, explorers) // Explorers urls
		i++

		if len(valueArgs) >= 65000 || y == len(exchangeListTMP)-1 {
			log.Debug("UpsertNFTMetaData: Start Upserting NFT MetaData")
			insertStatementCoins := fmt.Sprintf("INSERT INTO %s (id,website_url,twitter_url,discord_url,explorers) VALUES %s", tableName, strings.Join(valueString, ","))

			//only update urls(metadata)
			updateStatement := "ON CONFLICT (id) DO UPDATE SET  website_url = EXCLUDED.website_url,twitter_url = EXCLUDED.twitter_url, discord_url = EXCLUDED.discord_url, explorers = EXCLUDED.explorers"
			query := insertStatementCoins + updateStatement
			_, inserterError := pg.ExecContext(ctx, query, valueArgs...)

			if inserterError != nil {
				log.Error("UpsertNFTMetaData: Error Upserting NFT MetaData to PostgreSQL : %s", inserterError)
			}

			valueString = make([]string, 0, len(exchangeListTMP))
			valueArgs = make([]interface{}, 0, len(exchangeListTMP)*totalFields)

			i = 0
		}
	}
	log.EndTime("UpsertNFTMetaData: Successfully finished Upserting NFT MetaData at time : %s", startTime, nil)
	return nil
}

func UpsertNFTTickersData(ctx0 context.Context, nftdata *[]coingecko.NFTTickers) error {

	ctx, span := tracer.Start(ctx0, "UpsertNFTTickersData")
	defer span.End()
	startTime := log.StartTime("UpsertNFTTickersData")
	pg := PGConnect()

	tickerListTMP := *nftdata
	valueString := make([]string, 0, len(tickerListTMP))
	totalFields := 2 //total number of columns in the postgres collection
	valueArgs := make([]interface{}, 0, len(tickerListTMP)*totalFields)
	var idsInserted []string
	tableName := "nftdatalatest"
	var i = 0 //used for argument positions
	for y := 0; y < len(tickerListTMP); y++ {
		mult := i * totalFields
		var nftData = tickerListTMP[y]
		idsInserted = append(idsInserted, nftData.ID)

		/**
		* We're generating the insert value string for the postgres query.
		*
		* e.g. Let's say a collection in postgres has 2 columns. Then this looks something like this
		* ($1,$2),($3,$4),(..)...
		*
		* and so on. We use these variables in the postgres query builder. In our case, we currently have 2 columns in the collection.
		 */
		var valString = fmt.Sprintf("($%d,$%d)", mult+1, mult+2)
		valueString = append(valueString, valString)

		// Please note that the order of the following appending values matter. We map the following values to the 2 variables defined in the couple of lines defined above.
		valueArgs = append(valueArgs, nftData.ID)
		tickers, _ := json.Marshal(nftData.Tickers)
		valueArgs = append(valueArgs, tickers)
		i++

		if len(valueArgs) >= 65000 || y == len(tickerListTMP)-1 {
			log.Debug("UpsertNFTTickersData: Start Upserting NFT Ticker data")
			insertStatementCoins := fmt.Sprintf("INSERT INTO %s (id,tickers) VALUES %s", tableName, strings.Join(valueString, ","))

			//only update urls(metadata)
			updateStatement := "ON CONFLICT (id) DO UPDATE SET  tickers = EXCLUDED.tickers"
			query := insertStatementCoins + updateStatement
			_, inserterError := pg.ExecContext(ctx, query, valueArgs...)

			if inserterError != nil {
				log.Error("UpsertNFTTickersData: Error Upserting NFT Ticker Data to PostgreSQL : %s", inserterError)
			}

			valueString = make([]string, 0, len(tickerListTMP))
			valueArgs = make([]interface{}, 0, len(tickerListTMP)*totalFields)

			i = 0
		}
	}
	log.EndTime("UpsertNFTTickersData: Successfully finished Upserting NFT Ticker Data at time : %s", startTime, nil)
	return nil
}

// Gets a list of NFT IDS from postgres
func GetIDNFTList(ctx0 context.Context) ([]string, error) {

	ctx, span := tracer.Start(ctx0, "GetIDNFTList")
	defer span.End()
	startTime := log.StartTime("GetIDNFTList")
	pg := PGConnect()

	var nftIds []string
	query := `
		SELECT id from nftdatalatest
	`
	queryResult, err := pg.QueryContext(ctx, query)
	if err != nil {
		log.EndTime("GetIDNFTList: Error Getting NFTs IDs List from PostgreSQL", startTime, err)
		return nil, err
	}

	for queryResult.Next() {
		var exchange models.ExchangeList
		err := queryResult.Scan(&exchange.ID)
		if err != nil {
			log.EndTime("GetIDNFTList: Error Mapping NFTs IDs List from PostgreSQL ", startTime, err)
			return nil, err
		}
		nftIds = append(nftIds, exchange.ID)
	}
	log.EndTime("GetIDNFTList: Successfully finished Getting NFTs IDs List at time : %s", startTime, nil)
	return nftIds, nil

}

// Get NFT Trending for NFT Dynamic Description from PG
func GetNFTTrending(ctx0 context.Context, labels map[string]string) (*models.Global, error) {
	// Starts new child span from the parent span in the context.
	ctx, span := tracer.Start(ctx0, "GetNFTTrending")
	defer span.End()
	startTime := log.StartTime("GetNFTTrending")

	pg := PGConnect()

	// Will Map the trending NFT to Global
	var globalDescription models.Global

	queryResult, err := pg.QueryContext(ctx, `
	select
		array_to_json(ARRAY_AGG(json_build_object('Name', name, 'Slug', slug, 'change_24h', volume_24h_percentage_change_usd))) as trending
	from(
			select 
				name,
				slug,
				volume_24h_percentage_change_usd
			from 
				public.NFTPagination(100,0,'market_cap_usd','desc')
			order by 
				volume_24h_percentage_change_usd desc
			limit 2
		) as fo
		`)

	span.AddEvent("Query Executed")

	if err != nil {
		log.EndTimeL(labels, "GetNFTTrending: Error Getting NFT Trending Data Query from PostgreSQL", startTime, err)
		span.SetStatus(codes.Error, "GetNFTTrending: Error Getting NFT Trending Data Query from PostgreSQL")
		return nil, err
	}
	defer queryResult.Close()

	for queryResult.Next() {

		err := queryResult.Scan((*trendingResult)(&globalDescription.Trending))

		if err != nil {
			log.EndTimeL(labels, "GetNFTTrending: Error Mapping NFT Trending Data from PostgreSQL", startTime, err)
			span.SetStatus(codes.Error, "GetNFTTrending: Error Mapping NFT Trending Data from PostgreSQL")
			return nil, err
		}

	}
	log.EndTime("GetNFTTrending: Successfully finished Getting NFT Trending Data at time : %s", startTime, nil)
	return &globalDescription, nil
}

// Get Dynamic Description Dominance Data from PG
func GetNFTDynamicDescriptionDominanceData(ctx0 context.Context, labels map[string]string) (map[string]models.DominanceAssetsData, error) {
	// Starts new child span from the parent span in the context.
	ctx, span := tracer.Start(ctx0, "GetNFTDynamicDescriptionDominanceData")
	defer span.End()
	startTime := log.StartTime("GetNFTDynamicDescriptionDominanceData")

	pg := PGConnect()

	// Will Map the Global Data from Coingecko
	nftsData := make(map[string]models.DominanceAssetsData)

	queryResult, err := pg.QueryContext(ctx, `
	
		select
			name, 
			slug,
			display_symbol,
			(market_cap_usd / global_market_cap) * 100 as market_cap_dominance,
			nfts_count
		from (
			select
				name, 
				slug,
				symbol as display_symbol,
				market_cap_usd,
				(
					SELECT  
						sum(market_cap_usd)
					from 
						nftdatalatest
				) as global_market_cap,
				(
					SELECT  
						count(id)
					from 
						nftdatalatest
				) as nfts_count
			from 
				nftdatalatest
			where 
				name in ('CryptoPunks', 'Bored Ape Yacht Club')
			order by 
				name asc 
		) as fo
		`)

	span.AddEvent("Query Executed")

	if err != nil {
		log.EndTimeL(labels, "GetNFTDynamicDescriptionDominanceData: Error Getting NFT Dynamic Description Dominance Data from PostgreSQL", startTime, err)
		span.SetStatus(codes.Error, "GetNFTDynamicDescriptionDominanceData: Error Getting NFT Dynamic Description Dominance Data from PostgreSQL")
		return nil, err
	}
	defer queryResult.Close()

	for queryResult.Next() {
		var nftData models.DominanceAssetsData
		err := queryResult.Scan(&nftData.Name, &nftData.Slug, &nftData.DisplaySymbol, &nftData.MarketCapDominance, &nftData.Count)

		if err != nil {
			log.EndTimeL(labels, "GetNFTDynamicDescriptionDominanceData: Error Mapping NFT Dynamic Description Dominance Data from PostgreSQL", startTime, err)
			span.SetStatus(codes.Error, "GetNFTDynamicDescriptionDominanceData: Error Mapping NFT Dynamic Description Dominance Data from PostgreSQL")
			return nil, err
		}
		nftsData[strings.ToLower(nftData.DisplaySymbol)] = nftData

	}
	log.EndTime("GetNFTDynamicDescriptionDominanceData: Successfully finished Getting NFT Dynamic Description Dominance at time : %s", startTime, nil)
	return nftsData, nil
}


/* https://staging-a.forbesapi.forbes.com/forbesapi/content/all.json?retrievedfields=title%2CCdate%2Cdescription%2Cimage%2Cauthor%2CauthorGroup%2CnaturalId%2CprimaryChannelId%2Ctype%2Curi%2CchannelSection%2CbertieBadges&queryfilters=%5B%7B%22primaryChannelId%22%3A%5B%22channel_115%22%5D%7D%5D&sortasc=false&
showresultcount=false&
uri=string&
filter=string&
commentPage=true&
alltypes=true&
shortcodes=true&
skipbertieshortcodes=true&
slides=true&
type=string&
mobile=true&
autovpslider=true&
tickers=true&
stream=true&
videoplayertype=string&
swimlane=string&
wordcount=0&
vplimit=0&
lazyimages=true&
compressimages=true&
entitylinktype=string&
videoids=string&
bucket=string&
articlenumber=0&
progressive=true&
redesign=true&
code=string&
adLayout=string&
nextJs=true&
adLight=true

*/