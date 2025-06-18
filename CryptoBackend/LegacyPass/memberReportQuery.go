package repository

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/Forbes-Media/crypto-backend-api/datastruct"
	"github.com/Forbes-Media/crypto-backend-api/repository/common"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
)

type MemberReportQuery interface {
	GetCommunityMembersInfo(ctx context.Context) ([]datastruct.CommunityMemberInfo, error)                 // returns members info data from PG
	BQInsertCommunityMembersInfo(ctx context.Context, memberInfo []datastruct.BQCommunityMemberInfo) error // Insert members info data into BQ
	GetLegacyPassInfo(ctx context.Context) ([]datastruct.LegacyPassInfo, error)                            // returns Legacy Pass info data from MySql
	BQInsertLegacyPassInfo(ctx context.Context, memberInfo []datastruct.BQLegacyPassInfo) error            // Insert legacy pass info data from MySql
}

// a member info query struct that implements the memberReportQuery interface
type memberReportQuery struct{}

// GetCommunityMembersInfo get all members info from PG
// Takes context.Context
// Returns ([]datastruct.CommunityMemberInfo, error)
//
// Returns the a CommunityMemberInfo with all required data. and no error if successful
func (m *memberReportQuery) GetCommunityMembersInfo(ctx context.Context) ([]datastruct.CommunityMemberInfo, error) {
	span, labels := common.GenerateSpan("memberReportQuery.GetCommunityMembersInfo", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "memberReportQuery.GetCommunityMembersInfo"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "memberReportQuery.GetCommunityMembersInfo"))

	pg := pgUtils.GetPostgresqlClient()

	query := `
		select 
			email_addr,
			member_id,
			registration_date,
			display_name,
			grant_expiration 
		from getCommunityMemberInfo()`

	queryResult, err := pg.QueryContext(ctx, query)

	var usersInfo []datastruct.CommunityMemberInfo

	//var asset model.CoinGeckoAsset
	if err != nil {
		return nil, err
	}

	defer queryResult.Close()

	var userInfo datastruct.CommunityMemberInfo
	for queryResult.Next() {

		err := queryResult.Scan(&userInfo.EmailAddress, &userInfo.MemberId, &userInfo.RegistrationDate, &userInfo.DisplayName, &userInfo.GrantExpiration)
		if err != nil {
			log.EndTime("Pagination Query", startTime, err)
			return nil, err
		}
		usersInfo = append(usersInfo, userInfo)
	}

	log.EndTimeL(labels, "memberReportQuery.GetCommunityMembersInfo", startTime, nil)
	span.SetStatus(codes.Ok, "memberReportQuery.GetCommunityMembersInfo")

	return usersInfo, err
}

// BQInsertCommunityMembersInfo insert all members info that fetched from PG to BQ
// Takes (ctx context.Context, memberInfo []datastruct.BQCommunityMemberInfo)
// Returns (error)
//
// Returns  error if the insert process to BQ failed and no error if successful
func (m *memberReportQuery) BQInsertCommunityMembersInfo(ctx context.Context, memberInfo []datastruct.BQCommunityMemberInfo) error {
	span, labels := common.GenerateSpan("memberReportQuery.BQInsertCommunityMembersInfo", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "memberReportQuery.BQInsertCommunityMembersInfo"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "memberReportQuery.BQInsertCommunityMembersInfo"))

	client, err := bqUtils.GetBigQueryClient()

	if err != nil {
		return err
	}

	communityMemberTable := common.GetTableName("Community_Member_Info")
	var initialRecord string
	var subsequentRecords string
	for index, member := range memberInfo {

		// start build the Select statement for all rows that will be inserted or updated
		if index == 0 {
			initialRecord = BuildSelectStatementForCommunityMembersInfo(member)
		} else {
			subsequentRecords += fmt.Sprintf(" UNION ALL %s", BuildSelectStatementForCommunityMembersInfo(member))
		}
	}
	queryString := `MERGE INTO api-project-901373404215.digital_assets.` + communityMemberTable + ` T
	USING (
	  ` + initialRecord + subsequentRecords + `
	) AS S
	ON T.member_id = S.member_id
	WHEN MATCHED THEN
	  UPDATE SET
		email_addr = S.email_addr,
		display_name = S.display_name,
		registration_date = S.registration_date,
		free_trial_end_date = S.free_trial_end_date,
		row_last_updated = S.row_last_updated
	WHEN NOT MATCHED THEN
	  INSERT (member_id, email_addr, display_name, registration_date, free_trial_end_date, row_last_updated)
	  VALUES (
		S.member_id,
		S.email_addr,
		S.display_name,
		S.registration_date,
		S.free_trial_end_date,
		S.row_last_updated
	  );`

	query := client.Query(queryString)

	job, err := query.Run(ctx)
	var retryError error
	if err != nil {
		// We need to check the error if it contains 400
		// If it contains 400 we need to divide the Query so the BigQuery can handle it.
		if strings.Contains(err.Error(), "400") {
			l := len(memberInfo)
			var memInfo []datastruct.BQCommunityMemberInfo
			memInfo = append(memInfo, memberInfo...)
			for y := (l / 3); y < l; y += (l / 3) {
				a := memInfo[y-(l/3) : y]
				er := m.BQInsertCommunityMembersInfo(ctx, a)
				if er != nil {
					retryError = er
				}
			}
			log.EndTimeL(labels, "memberReportQuery.BQInsertCommunityMembersInfo Error Sub Upserting Member Info ", startTime, retryError)
			return retryError
		}
		log.EndTimeL(labels, "memberReportQuery.BQInsertCommunityMembersInfo Error Upserting Member Info ", startTime, err)
		return err
	}
	log.Info("memberReportQuery.BQInsertCommunityMembersInfo BigQuery Job ID : %s", job.ID())

	_, err = job.Wait(ctx)
	if err != nil {
		log.EndTimeL(labels, "memberReportQuery.BQInsertCommunityMembersInfo Error Upserting Member Info ", startTime, err)
		return err
	}

	log.EndTimeL(labels, "memberReportQuery.BQInsertCommunityMembersInfo Finished Successfully ", startTime, nil)
	span.SetStatus(codes.Ok, "memberReportQuery.BQInsertCommunityMembersInfo Finished Successfully ")

	return nil
}

// BuildSelectStatementForCommunityMembersInfo build member select Query
// Takes (memberInfo datastruct.BQCommunityMemberInfo)
// Returns (string)
//
// Returns query string that we need to use in merge statement
func BuildSelectStatementForCommunityMembersInfo(member datastruct.BQCommunityMemberInfo) string {
	// convert timestamp to String so the Query can proceed it
	Registration := member.RegistrationDate.Timestamp
	RegistrationDate := Registration.Format("2006-01-02 15:04:05")
	GrantExpiration := member.GrantExpiration.Timestamp
	GrantExpirationDate := GrantExpiration.Format("2006-01-02 15:04:05")
	RowLastUpdated := member.RowLastUpdated.Timestamp
	RowLastUpdatedDate := RowLastUpdated.Format("2006-01-02 15:04:05")

	record := `
	SELECT 
	` + fmt.Sprintf("%d", member.MemberId.Int64) + ` AS member_id,
	` + fmt.Sprintf("%d", member.EmailAddress.Int64) + ` AS email_addr,
	"` + string(member.DisplayName) + `" AS display_name,
	TIMESTAMP("` + string(RegistrationDate) + `") AS registration_date,
	TIMESTAMP("` + string(GrantExpirationDate) + `") AS free_trial_end_date,
	TIMESTAMP("` + string(RowLastUpdatedDate) + `") AS row_last_updated`

	return record
}

// GetLegacyPassInfo get all members info from MySql
// Takes context.Context
// Returns ([]datastruct.LegacyPassInfo, error)
//
// Returns the a LegacyPassInfo with all required data. and no error if successful
func (m *memberReportQuery) GetLegacyPassInfo(ctx context.Context) ([]datastruct.LegacyPassInfo, error) {
	span, labels := common.GenerateSpan("memberReportQuery.GetLegacyPassInfo", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "memberReportQuery.GetLegacyPassInfo"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "memberReportQuery.GetLegacyPassInfo"))

	mysql := mysqlUtils.GetMySqlClient()

	tableName := fmt.Sprintf("%s.user", os.Getenv("MYSQL_DB"))

	query := `select id, email from ` + tableName + ``

	queryResult, err := mysql.QueryContext(ctx, query)

	var usersInfo []datastruct.LegacyPassInfo

	if err != nil {
		return nil, err
	}

	defer queryResult.Close()

	var userInfo datastruct.LegacyPassInfo
	for queryResult.Next() {

		err := queryResult.Scan(&userInfo.Id, &userInfo.Email)
		if err != nil {
			log.EndTimeL(labels, "Error memberReportQuery.GetLegacyPassInfo Scanning Data fro MySql", startTime, err)
			return nil, err
		}
		usersInfo = append(usersInfo, userInfo)
	}

	log.EndTimeL(labels, "memberReportQuery.GetLegacyPassInfo", startTime, nil)
	span.SetStatus(codes.Ok, "memberReportQuery.GetLegacyPassInfo")

	return usersInfo, err
}

// BQInsertLegacyPassInfo insert all members info that fetched from MySql to BQ
// Takes (ctx context.Context, memberInfo []datastruct.BQLegacyPassInfo)
// Returns (error)
//
// Returns  error if the insert process to BQ failed and no error if successful
func (m *memberReportQuery) BQInsertLegacyPassInfo(ctx context.Context, memberInfo []datastruct.BQLegacyPassInfo) error {
	span, labels := common.GenerateSpan("memberReportQuery.BQInsertLegacyPassInfo", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "memberReportQuery.BQInsertLegacyPassInfo"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "memberReportQuery.BQInsertLegacyPassInfo"))

	client, err := bqUtils.GetBigQueryClient()

	if err != nil {
		return err
	}
	var (
		wg           = sync.WaitGroup{}
		throttleChan = make(chan bool, 200)
	)

	legacyPassTable := common.GetTableName("Legacy_Pass_Info")
	batchSize := 1000
	for i := 0; i < len(memberInfo); i += batchSize {
		var initialRecord string
		var subsequentRecords string
		end := i + batchSize
		if end > len(memberInfo) {
			end = len(memberInfo)
		}
		throttleChan <- true
		wg.Add(1)
		go func(i int, end int) error {
			for index, member := range memberInfo[i:end] {
				if index == 0 {
					initialRecord = BuildSelectStatementForLegacyPassInfo(member)
				} else {
					subsequentRecords += fmt.Sprintf(" UNION ALL %s", BuildSelectStatementForLegacyPassInfo(member))
				}
			}
			queryString := `MERGE INTO api-project-901373404215.digital_assets.` + legacyPassTable + ` T
			USING (
			  ` + initialRecord + subsequentRecords + `
			) AS S
			ON T.id = S.id
			WHEN MATCHED THEN
			  UPDATE SET
				id = S.id,
				email = S.email,
				row_last_updated = S.row_last_updated
			WHEN NOT MATCHED THEN
			  INSERT (id, email, row_last_updated)
			  VALUES (
				S.id,
				S.email,
				S.row_last_updated
			  );`

			query := client.Query(queryString)

			job, err := query.Run(ctx)
			var retryError error
			if err != nil {
				// We need to check the error if it contains 400
				// If it contains 400 we need to divide the Query so the BigQuery can handle it.
				if strings.Contains(err.Error(), "400") || strings.Contains(err.Error(), "413") {
					l := len(memberInfo)
					var memInfo []datastruct.BQLegacyPassInfo
					memInfo = append(memInfo, memberInfo...)
					for y := (l / 3); y < l; y += (l / 3) {
						a := memInfo[y-(l/3) : y]
						er := m.BQInsertLegacyPassInfo(ctx, a)
						if er != nil {
							retryError = er
						}
					}
					log.EndTimeL(labels, "memberReportQuery.BQInsertLegacyPassInfo Error Sub Upserting Member Info for recursive", startTime, retryError)
					return retryError
				}
				log.EndTimeL(labels, "memberReportQuery.BQInsertLegacyPassInfo Error Upserting Member Info ", startTime, err)
				return err
			}
			log.Info("memberReportQuery.BQInsertLegacyPassInfo BigQuery Job ID : %s", job.ID())

			_, err = job.Wait(ctx)
			if err != nil {
				log.EndTimeL(labels, "memberReportQuery.BQInsertLegacyPassInfo Error Upserting Member Info ", startTime, err)
				return err
			}
			<-throttleChan
			wg.Done()
			return nil
		}(i, end)
	}
	wg.Wait()
	log.EndTimeL(labels, "memberReportQuery.BQInsertLegacyPassInfo Finished Successfully ", startTime, nil)
	span.SetStatus(codes.Ok, "memberReportQuery.BQInsertLegacyPassInfo Finished Successfully ")

	return nil
}

// BuildSelectStatementForLegacyPassInfo build member select Query
// Takes (memberInfo datastruct.BQLegacyPassInfo)
// Returns (string)
//
// Returns query string that we need to use in merge statement
func BuildSelectStatementForLegacyPassInfo(member datastruct.BQLegacyPassInfo) string {
	RowLastUpdated := member.RowLastUpdated.Timestamp
	RowLastUpdatedDate := RowLastUpdated.Format("2006-01-02 15:04:05")

	record := `
	SELECT 
	` + fmt.Sprintf("%d", member.Id.Int64) + ` AS id,
	"` + member.Email.StringVal + `" AS email,
	TIMESTAMP("` + string(RowLastUpdatedDate) + `") AS row_last_updated`

	return record
}
