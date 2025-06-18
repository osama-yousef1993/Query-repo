package repository

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Forbes-Media/crypto-backend-api/datastruct"
	"github.com/Forbes-Media/crypto-backend-api/repository/common"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
)

// An interface that defines functions for intercasing with memberinfo quieries
type MemberInfoQuery interface {
	InsertMemberInfo(ctx context.Context, memberInfo *datastruct.MemberInfo) error                        // inserts member info data
	UpdateMemberInfo(ctx context.Context, memberInfo *datastruct.MemberInfo) error                        // updates member info data
	InsertMemberGrantsInfo(ctx context.Context, grantConfig *[]datastruct.UpsertGrantInfo) error          // inserts member grants info
	GetMemberInfo(ctx context.Context, memberInfo *datastruct.MemberInfo) (*datastruct.MemberInfo, error) // returns member info data
}

// a member info query struct that implements the MemberInfoQuery interface
type memberinfoQuery struct{}

// GetSymbolDetailsForWatchlist Inserts, or updates a member info record. wallet_address is the priimary key
//
// Returns (error)
//
// Takes the symbol and quries postgres for information associated with the symbol.
// Returns the a populated watchlistData object with all required data. and no error if successful
func (m *memberinfoQuery) InsertMemberInfo(ctx context.Context, memberInfo *datastruct.MemberInfo) error {
	span, labels := common.GenerateSpan("memberinfoQuery.InsertMemberInfo", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "memberinfoQuery.InsertMemberInfo"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "memberinfoQuery.InsertMemberInfo"))

	pg := pgUtils.GetPostgresqlClient()

	query := "CALL public.addnewmember ($1, $2, $3)"

	_, err := pg.ExecContext(ctx, query, memberInfo.WalletAddress, memberInfo.EmailAddress, memberInfo.DisplayName)

	//var asset model.CoinGeckoAsset
	if err != nil {
		return err
	}

	log.EndTimeL(labels, "memberinfoQuery.InsertMemberInfo", startTime, nil)
	span.SetStatus(codes.Ok, "memberinfoQuery.InsertMemberInfo")

	return nil
}

// UpdateMemberInfo or updates a member info record. wallet_address is the priimary key
//
// Returns (error)
//
// Takes the symbol and quries postgres for information associated with the symbol.
// Returns the a populated watchlistData object with all required data. and no error if successful
func (m *memberinfoQuery) UpdateMemberInfo(ctx context.Context, memberInfo *datastruct.MemberInfo) error {
	span, labels := common.GenerateSpan("memberinfoQuery.InsertMemberInfo", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "memberinfoQuery.InsertMemberInfo"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "memberinfoQuery.InsertMemberInfo"))

	pg := pgUtils.GetPostgresqlClient()

	query := "CALL public.updatememberdetails($1, $2, $3)"

	_, err := pg.ExecContext(ctx, query, memberInfo.WalletAddress, memberInfo.EmailAddress, memberInfo.DisplayName)

	//var asset model.CoinGeckoAsset
	if err != nil {
		return err
	}

	log.EndTimeL(labels, "memberinfoQuery.InsertMemberInfo", startTime, nil)
	span.SetStatus(codes.Ok, "memberinfoQuery.InsertMemberInfo")

	return nil
}

// Get list of all the fundamentals values to build the search-dictionaries. This is used for the search functionality
func (m *memberinfoQuery) GetMemberInfo(ctx context.Context, memberInfo *datastruct.MemberInfo) (*datastruct.MemberInfo, error) {

	span, labels := common.GenerateSpan("memberinfoQuery.GetMemberInfo", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "memberinfoQuery.GetMemberInfo"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "memberinfoQuery.GetMemberInfo"))

	var grantsJSON []byte
	var info *datastruct.MemberInfo

	pg := pgUtils.GetPostgresqlClient()
	query := fmt.Sprintf(`
		SELECT 
		wallet_addr, 
		email_addr,
		member_id,
		registration_date,
		display_name,
		grants
		from public.getMemberInfo('%s')
	`, memberInfo.WalletAddress)
	queryResult, err := pg.QueryContext(ctx, query)

	if err != nil {
		span.SetStatus(codes.Error, "PGGetSearchAssets")
		log.EndTime("Search assets Query failed", startTime, err)
		return nil, err
	}
	defer queryResult.Close()

	for queryResult.Next() {
		var res = datastruct.MemberInfo{}
		err := queryResult.Scan(&res.WalletAddress, &res.EmailAddress, &res.MemberId, &res.RegistrationDate, &res.DisplayName, &grantsJSON)
		if err != nil {
			span.SetStatus(codes.Error, "memberinfoQuery.GetMemberInfo")
			log.EndTime("memberinfoQuery.GetMemberInfo", startTime, err)
			return nil, err
		}

		// Deserialize the JSON data into the Grants slice
		err = json.Unmarshal(grantsJSON, &res.Grants)
		if err != nil {
			span.SetStatus(codes.Error, "memberinfoQuery.GetMemberInfo")
			log.EndTime("memberinfoQuery.GetMemberInfo", startTime, err)
			return nil, err
		}
		info = &res
	}
	log.EndTime("Search assets Query", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return info, nil
}

// GetSymbolDetailsForWatchlist Inserts, or updates a member info record. wallet_address is the priimary key
//
// Returns (error)
//
// Takes the symbol and quries postgres for information associated with the symbol.
// Returns the a populated watchlistData object with all required data. and no error if successful
func (m *memberinfoQuery) InsertMemberGrantsInfo(ctx context.Context, grantConfig *[]datastruct.UpsertGrantInfo) error {
	span, labels := common.GenerateSpan("memberinfoQuery.InsertMemberGrantsInfo", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "memberinfoQuery.InsertMemberGrantsInfo"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "memberinfoQuery.InsertMemberGrantsInfo"))

	pg := pgUtils.GetPostgresqlClient()

	for _, configGrant := range *grantConfig {
		query := "CALL public.upsertMemberGrants ($1, $2, $3)"

		_, err := pg.ExecContext(ctx, query, configGrant.WalletAddress, configGrant.GrantId, configGrant.Expiration)

		//var asset model.CoinGeckoAsset
		if err != nil {
			return err
		}

	}

	log.EndTimeL(labels, "memberinfoQuery.InsertMemberGrantsInfo", startTime, nil)
	span.SetStatus(codes.Ok, "memberinfoQuery.InsertMemberGrantsInfo")

	return nil
}
