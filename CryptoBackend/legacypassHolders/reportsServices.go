package services

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/Forbes-Media/crypto-backend-api/datastruct"
	"github.com/Forbes-Media/crypto-backend-api/repository"
	"github.com/Forbes-Media/crypto-backend-api/repository/common"
	"github.com/Forbes-Media/crypto-backend-api/repository/common/nftapigateway"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
)

type MemberReportService interface {
	GetCommunityMembersInfo(ctx context.Context) ([]datastruct.BQCommunityMemberInfo, error)               // Get a information about a all members from PG
	BQInsertCommunityMembersInfo(ctx context.Context, memberInfo []datastruct.BQCommunityMemberInfo) error // insert all members information in BQ
	GetLegacyPassInfo(ctx context.Context) ([]datastruct.BQLegacyPassInfo, error)                          // Get a information about a all Legacy Pass members from MySql
	BQInsertLegacyPassInfo(ctx context.Context, memberInfo []datastruct.BQLegacyPassInfo) error            // insert all legacy Pass members information into BQ
	GetLegacyPassHolderProfiles(ctx context.Context) (map[string]datastruct.CommunityMemberData, error)
}

type memberReportService struct {
	dao repository.DAO
}

// generates a new memberReportService object
func NewMemberReportService(dao repository.DAO) MemberReportService {
	return &memberReportService{dao: dao}
}

// GetCommunityMembersInfo
// Takes context.
// returns ([]datastruct.BQCommunityMemberInfo, error)
//
// GetCommunityMembersInfo get the Information about all members from PG then convert it to  BQCommunityMemberInfo so we can insert it to BQ
// if there is an error the  process will failed if
func (m *memberReportService) GetCommunityMembersInfo(ctx context.Context) ([]datastruct.BQCommunityMemberInfo, error) {
	span, labels := common.GenerateSpan("GetCommunityMembersInfo", ctx)
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "memberReportService.GetCommunityMembersInfo"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "memberReportService.GetCommunityMembersInfo"))
	queryMGR := m.dao.NewMemberReportQuery()

	memberInfo, err := queryMGR.GetCommunityMembersInfo(ctx)

	if err != nil {
		log.ErrorL(labels, "memberReportService.GetCommunityMembersInfo Error executing query: %s", err)
		log.EndTime("memberReportService.GetCommunityMembersInfo", startTime, err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	bgMemberInfo, err := ConvertCommunityMemberInfoToBQCommunityMemberInfoData(ctx, memberInfo)
	if err != nil {
		log.ErrorL(labels, "memberReportService.GetCommunityMembersInfo Error converting member info: %s", err)
		log.EndTime("memberReportService.GetCommunityMembersInfo", startTime, err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	log.EndTime("memberReportService.GetCommunityMembersInfo", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return bgMemberInfo, nil
}

// GetCommunityMembersInfo
// Takes (ctx context.Context, memberInfo []datastruct.BQCommunityMemberInfo).
// returns ( error)
//
// BQInsertCommunityMembersInfo insert the Information about all members that comes from PG to BQ
// if the insert process failed an error will return if the process successfully finished return nil
func (m *memberReportService) BQInsertCommunityMembersInfo(ctx context.Context, memberInfo []datastruct.BQCommunityMemberInfo) error {
	span, labels := common.GenerateSpan("BQInsertCommunityMembersInfo", ctx)
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "memberReportService.BQInsertCommunityMembersInfo"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "memberReportService.BQInsertCommunityMembersInfo"))
	queryMGR := m.dao.NewMemberReportQuery()
	err := queryMGR.BQInsertCommunityMembersInfo(ctx, memberInfo)

	if err != nil {
		log.ErrorL(labels, "memberReportService.BQInsertCommunityMembersInfo Error inserting community members info into BigQuery: %s", err)
		log.EndTime("memberReportService.BQInsertCommunityMembersInfo", startTime, err)
		span.SetStatus(codes.Error, err.Error())
		return err

	}
	log.EndTime("memberReportService.BQInsertCommunityMembersInfo", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return nil
}

// ConvertCommunityMemberInfoToBQCommunityMemberInfoData
// Takes (ctx context.Context, memberInfo []datastruct.CommunityMemberInfo)
// returns ([]datastruct.BQCommunityMemberInfo, error)
//
// ConvertCommunityMemberInfoToBQCommunityMemberInfoData convert data that comes from PG to object that accepted to be inserted to BQ
// if the insert process failed an error will return if the process successfully finished return new object
func ConvertCommunityMemberInfoToBQCommunityMemberInfoData(ctx context.Context, memberInfo []datastruct.CommunityMemberInfo) ([]datastruct.BQCommunityMemberInfo, error) {
	span, labels := common.GenerateSpan("ConvertCommunityMemberInfoToBQCommunityMemberInfoData", ctx)
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "memberReportService.ConvertCommunityMemberInfoToBQCommunityMemberInfoData"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "memberReportService.ConvertCommunityMemberInfoToBQCommunityMemberInfoData"))

	var bqCommunityMemberInfo []datastruct.BQCommunityMemberInfo
	for _, member := range memberInfo {
		bqCommunityMemberInfo = append(bqCommunityMemberInfo, datastruct.BQCommunityMemberInfo{
			EmailAddress:     bigquery.NullInt64{Int64: member.EmailAddress, Valid: true},
			MemberId:         bigquery.NullInt64{Int64: member.MemberId, Valid: true},
			RegistrationDate: bigquery.NullTimestamp{Timestamp: member.RegistrationDate, Valid: true},
			DisplayName:      member.DisplayName,
			GrantExpiration:  bigquery.NullTimestamp{Timestamp: member.GrantExpiration, Valid: true},
			RowLastUpdated:   bigquery.NullTimestamp{Timestamp: time.Now(), Valid: true},
		})
	}
	log.EndTime("memberReportService.ConvertCommunityMemberInfoToBQCommunityMemberInfoData", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return bqCommunityMemberInfo, nil
}

// GetLegacyPassInfo
// Takes context.
// returns ([]datastruct.LegacyPassInfo, error)
//
// GetLegacyPassInfo get the Information about all members from MySQL then convert it to BQLegacyPassInfo so we can insert it to BQ
// if there is an error the  process will failed if
func (m *memberReportService) GetLegacyPassInfo(ctx context.Context) ([]datastruct.BQLegacyPassInfo, error) {
	span, labels := common.GenerateSpan("GetLegacyPassInfo", ctx)
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "memberReportService.GetLegacyPassInfo"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "memberReportService.GetLegacyPassInfo"))
	queryMGR := m.dao.NewMemberReportQuery()

	memberInfo, err := queryMGR.GetLegacyPassInfo(ctx)

	if err != nil {
		log.ErrorL(labels, "memberReportService.GetLegacyPassInfo Error executing query: %s", err)
		log.EndTime("memberReportService.GetLegacyPassInfo", startTime, err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	communityMember, err := m.GetLegacyPassHolderProfiles(ctx)

	if err != nil {
		log.ErrorL(labels, "memberReportService.GetLegacyPassInfo Error executing query: %s", err)
		log.EndTime("memberReportService.GetLegacyPassInfo", startTime, err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	bgMemberInfo, err := ConvertLegacyPassInfoToBQLegacyPassInfoData(ctx, memberInfo, communityMember)
	if err != nil {
		log.ErrorL(labels, "memberReportService.GetLegacyPassInfo Error converting legacyPass info: %s", err)
		log.EndTime("memberReportService.GetLegacyPassInfo", startTime, err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	log.EndTime("memberReportService.GetLegacyPassInfo", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return bgMemberInfo, nil
}

// BQInsertLegacyPassInfo
// Takes (ctx context.Context, memberInfo []datastruct.BQLegacyPassInfo).
// returns ( error)
//
// BQInsertLegacyPassInfo insert the Information about all members that comes from MySQL to BQ
// if the insert process failed an error will return if the process successfully finished return nil
func (m *memberReportService) BQInsertLegacyPassInfo(ctx context.Context, memberInfo []datastruct.BQLegacyPassInfo) error {
	span, labels := common.GenerateSpan("BQInsertLegacyPassInfo", ctx)
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "memberReportService.BQInsertLegacyPassInfo"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "memberReportService.BQInsertLegacyPassInfo"))
	queryMGR := m.dao.NewMemberReportQuery()
	err := queryMGR.BQInsertLegacyPassInfo(ctx, memberInfo)

	if err != nil {
		log.ErrorL(labels, "memberReportService.BQInsertLegacyPassInfo Error inserting Legacy Pass info into BigQuery: %s", err)
		log.EndTime("memberReportService.BQInsertLegacyPassInfo", startTime, err)
		span.SetStatus(codes.Error, err.Error())
		return err

	}
	log.EndTime("memberReportService.BQInsertLegacyPassInfo", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return nil
}

// ConvertLegacyPassInfoToBQLegacyPassInfoData
// Takes (ctx context.Context, memberInfo []datastruct.LegacyPassInfo)
// returns ([]datastruct.BQLegacyPassInfo, error)
//
// ConvertLegacyPassInfoToBQLegacyPassInfoData convert data that comes from MySQL to object that accepted to be inserted to BQ
// if the insert process failed an error will return if the process successfully finished return new object
func ConvertLegacyPassInfoToBQLegacyPassInfoData(ctx context.Context, memberInfo []datastruct.LegacyPassInfo, communityMember map[string]datastruct.CommunityMemberData) ([]datastruct.BQLegacyPassInfo, error) {
	span, labels := common.GenerateSpan("ConvertLegacyPassInfoToBQLegacyPassInfoData", ctx)
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "memberReportService.ConvertLegacyPassInfoToBQLegacyPassInfoData"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "memberReportService.ConvertLegacyPassInfoToBQLegacyPassInfoData"))

	var bqCommunityMemberInfo []datastruct.BQLegacyPassInfo
	for _, member := range memberInfo {
		var isLegacyPass bool
		communityAddress := communityMember[member.Email]
		if communityAddress.WalletAddress != "" {
			isLegacyPass = false
		}
		bqCommunityMemberInfo = append(bqCommunityMemberInfo, datastruct.BQLegacyPassInfo{
			Id:                 bigquery.NullInt64{Int64: member.Id, Valid: true},
			Email:              bigquery.NullString{StringVal: member.Email, Valid: true},
			IsLegacyPassHolder: bigquery.NullBool{Bool: isLegacyPass, Valid: true},
			RowLastUpdated:     bigquery.NullTimestamp{Timestamp: time.Now(), Valid: true},
		})
	}
	log.EndTime("memberReportService.ConvertLegacyPassInfoToBQLegacyPassInfoData", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return bqCommunityMemberInfo, nil
}

func (m *memberReportService) GetLegacyPassHolderProfiles(ctx context.Context) (map[string]datastruct.CommunityMemberData, error) {
	span, labels := common.GenerateSpan("GetLegacyPassHolderProfiles", ctx)
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "memberReportService.GetLegacyPassHolderProfiles"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "memberReportService.GetLegacyPassHolderProfiles"))
	queryMGR := m.dao.NewMemberReportQuery()
	nftResult := queryMGR.GetLegacyPassHolderProfiles(ctx)

	profilesWallets := m.BuildLegacyPassHolder(ctx, nftResult)
	communityInfo, err := queryMGR.GetAllCommunityMembersInfo(ctx, profilesWallets)
	if err != nil {
		log.ErrorL(labels, "memberReportService.GetLegacyPassHolderProfiles Error executing query: %s", err)
		log.EndTime("memberReportService.GetLegacyPassHolderProfiles", startTime, err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	log.EndTime("memberReportService.GetLegacyPassHolderProfiles", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return communityInfo, nil
}

func (m *memberReportService) BuildLegacyPassHolder(ctx context.Context, nftResult *nftapigateway.NFTProfilesResult) []string {
	span, labels := common.GenerateSpan("BuildLegacyPassHolder", ctx)
	defer span.End()
	var (
		nftProfiles []nftapigateway.NFTProfiles
		wallets     []string
	)

	span.AddEvent(fmt.Sprintf("Starting %s", "memberReportService.BuildLegacyPassHolder"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "memberReportService.BuildLegacyPassHolder"))

	for _, profile := range nftResult.Profiles {
		if profile.OwnerWallet != "" {
			nftProfiles = append(nftProfiles, profile)
			wallets = append(wallets, "'"+profile.OwnerWallet+"'")
		}
	}

	log.EndTime("memberReportService.BuildLegacyPassHolder", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return wallets
}
