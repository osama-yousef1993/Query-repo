package services

import (
	"context"
	"fmt"
	"time"

	"github.com/Forbes-Media/crypto-backend-api/datastruct"
	"github.com/Forbes-Media/crypto-backend-api/dto"
	"github.com/Forbes-Media/crypto-backend-api/repository"
	"github.com/Forbes-Media/crypto-backend-api/repository/common"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
)

// Member Info Service is responsible for serving information in regards to forbes web3 walled users
type MemberInfoService interface {
	GetMemberInfo(context.Context, *dto.MemberInfo) (*dto.MemberInfo, error) // creates a new member info object if it does not exist returns a member info object
	UpdateMemberInfo(context.Context, *dto.MemberInfo) error                 // updates a information about a member
}

// a memberService object that implements the MemberInfoServiceInterface
type memberInfoService struct {
	dao     repository.DAO
	cordial repository.CordialGateway
}

// generates a new memberInfoService object
func NewMemberInfoService(dao repository.DAO) MemberInfoService {
	return &memberInfoService{dao: dao}
}

// GetMember Service Looks up information about a wallet user.
// If they dont exist we add them to our tables
func (m *memberInfoService) GetMemberInfo(ctx context.Context, memberInfo *dto.MemberInfo) (*dto.MemberInfo, error) {
	span, labels := common.GenerateSpan("memberinfoQuery.GetMemberInfo", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "memberinfoQuery.GetMemberInfo"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "memberinfoQuery.GetMemberInfo"))

	queryMGR := m.dao.NewMemberInfoQuery()
	// check to see if member exists
	member, err := queryMGR.GetMemberInfo(ctx, memberInfo_ConvertDTOToDatastruct(ctx, memberInfo))
	if err != nil {
		goto ERR
	}
	//if there was no error but no data returned insert a new object. No user exists
	if member == nil {
		err = queryMGR.InsertMemberInfo(ctx, memberInfo_ConvertDTOToDatastruct(ctx, memberInfo))
		if err != nil {
			goto ERR
		}

		//Get the generated data
		member, err = queryMGR.GetMemberInfo(ctx, memberInfo_ConvertDTOToDatastruct(ctx, memberInfo))
		if err != nil {
			goto ERR
		}
	}

	log.EndTime("memberinfoQuery.GetMemberInfo", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return memberInfo_ConvertDatastructToDTO(ctx, member), nil
ERR:
	log.EndTime("memberinfoQuery.GetMemberInfo", startTime, nil)
	span.SetStatus(codes.Error, err.Error())
	return nil, err

}

// UpdateMemberInfo
// UpdateMemberInfo Service Takes information about a user.
// - first we need to check if the user exist in our tables here we have two options:
// 1- If user exist inn our tables we wil return the user data
// 2- If the user not exist in our tables we will add it to our tables and then we will returned it
// Then we need to check if the user Exist in Cordial:
// 1- if the user exist in cordial we will update his profile and add it to our List
// 2- If the user doesn't exist we will create new profile for him and add it to our List
// this function will return error if exist and nil if the process finished successfully
func (m *memberInfoService) UpdateMemberInfo(ctx context.Context, memberInfo *dto.MemberInfo) error {

	span, labels := common.GenerateSpan("memberInfoQuery.UpdateMemberInfo", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "memberInfoQuery.UpdateMemberInfo"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "memberInfoQuery.UpdateMemberInfo"))

	var userInfo *datastruct.MemberInfo

	// Get member Information from Database
	memInfo, err := m.GetMemberInfo(ctx, memberInfo)
	if err != nil {
		goto ERR
	}
	// Convert dto.MemberInfo to datastruct.MemberInfo
	userInfo = memberInfo_ConvertDTOToDatastruct(ctx, memInfo)

	// Check User Profile Data from Cordial
	err = m.cordial.CheckCordialUser(ctx, userInfo)
	if err != nil {
		goto ERR
	}

	log.EndTime("memberInfoQuery.UpdateMemberInfo", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return nil

ERR:
	log.EndTime("memberInfoQuery.UpdateMemberInfo", startTime, nil)
	span.SetStatus(codes.Error, err.Error())
	return nil

}

func memberInfo_ConvertDTOToDatastruct(ctx context.Context, memberInfo *dto.MemberInfo) *datastruct.MemberInfo {
	return &datastruct.MemberInfo{
		WalletAddress:    memberInfo.WalletAddress,
		EmailAddress:     memberInfo.EmailAddress,
		MemberId:         memberInfo.MemberId,
		DisplayName:      memberInfo.DisplayName,
		RegistrationDate: memberInfo.RegistrationDate,
	}
}

// TODO convert grants to a jwt
func memberInfo_ConvertDatastructToDTO(ctx context.Context, memberInfo *datastruct.MemberInfo) *dto.MemberInfo {

	// create a new dto jwt object base on data from the member info
	var dtoJWT = dto.JWT{
		IAT: time.Now().UnixMilli(),
		ISS: "fbs_web3",
		SUB: memberInfo.WalletAddress,
	}
	// convert datastruct grants to the dto.grants object
	for _, grant := range memberInfo.Grants {
		dtoJWT.Grants = append(dtoJWT.Grants, dto.Grant{
			GrantId:    grant.GrantId,
			Expiration: grant.Expiration.UnixMilli(),
		})
	}
	// generate a jwt based on the dto object
	jwt, _ := NewTokenManagerService().GenerateJWT(ctx, dtoJWT)

	return &dto.MemberInfo{
		WalletAddress:    memberInfo.WalletAddress,
		EmailAddress:     memberInfo.EmailAddress,
		MemberId:         memberInfo.MemberId,
		DisplayName:      memberInfo.DisplayName,
		RegistrationDate: memberInfo.RegistrationDate,
		JWT:              jwt,
	}
}
