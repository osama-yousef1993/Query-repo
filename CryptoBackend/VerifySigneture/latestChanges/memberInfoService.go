package services

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/Forbes-Media/crypto-backend-api/datastruct"
	"github.com/Forbes-Media/crypto-backend-api/dto"
	"github.com/Forbes-Media/crypto-backend-api/repository"
	"github.com/Forbes-Media/crypto-backend-api/repository/common"
	"github.com/Forbes-Media/go-tools/log"
	"github.com/golang-jwt/jwt"
	"go.opentelemetry.io/otel/codes"
)

// Member Info Service is responsible for serving information in regards to forbes web3 walled users
type MemberInfoService interface {
	GetMemberInfo(context.Context, *dto.MemberInfo) (*dto.MemberInfo, error)    // creates a new member info object if it does not exist returns a member info object
	UpdateMemberInfo(context.Context, *dto.MemberInfo) (*dto.MemberInfo, error) // updates a information about a member
}

// a memberService object that implements the MemberInfoServiceInterface
type memberInfoService struct {
	dao     repository.DAO
	cordial repository.CordialGateway
}

// generates a new memberInfoService object
func NewMemberInfoService(dao repository.DAO, cordial repository.CordialGateway) MemberInfoService {
	return &memberInfoService{dao: dao, cordial: cordial}
}

// GetMember Service Looks up information about a wallet user.
// If they dont exist we add them to our tables
func (m *memberInfoService) GetMemberInfo(ctx context.Context, memberInfo *dto.MemberInfo) (*dto.MemberInfo, error) {
	span, labels := common.GenerateSpan("memberinfoQuery.GetMemberInfo", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "memberinfoQuery.GetMemberInfo"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "memberinfoQuery.GetMemberInfo"))

	var recalculatedGrants *[]datastruct.Grant
	queryMGR := m.dao.NewMemberInfoQuery()
	// check to see if member exists
	member, err := queryMGR.GetMemberInfo(ctx, memberInfo_ConvertDTOToDatastruct(ctx, memberInfo))
	if err != nil {
		log.EndTime("memberinfoQuery.GetMemberInfo", startTime, err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	//if there was no error but no data returned insert a new object. No user exists
	if member == nil {
		err = queryMGR.InsertMemberInfo(ctx, memberInfo_ConvertDTOToDatastruct(ctx, memberInfo))
		if err != nil {
			log.EndTime("memberinfoQuery.GetMemberInfo", startTime, err)
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}

		//Get the generated data
		member, err = queryMGR.GetMemberInfo(ctx, memberInfo_ConvertDTOToDatastruct(ctx, memberInfo))
		if err != nil {
			log.EndTime("memberinfoQuery.GetMemberInfo", startTime, err)
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}
	}
	// recalculate the grant expiration times
	recalculatedGrants, err = m.RecalculateMemberGrants(ctx, member)
	if err != nil {
		log.EndTime("memberinfoQuery.GetMemberInfo", startTime, err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	member.Grants = *recalculatedGrants

	log.EndTime("memberinfoQuery.GetMemberInfo", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return memberInfo_ConvertDatastructToDTO(ctx, member), nil

}

// UpdateMemberInfo
// UpdateMemberInfo Service Takes information about a user.
// - first we need to check if the user exist in our tables here we have two options:
// 1- If user exist inn our tables we will return the user data
// 2- If the user not exist in our tables we will add it to our tables and then we will returned it
// Then we need to check if the user Exist in Cordial:
// 1- if the user exist in cordial we will update his profile and add it to our List
// 2- If the user doesn't exist we will create new profile for him and add it to our List
// this function will return error if exist and nil if the process finished successfully
func (m *memberInfoService) UpdateMemberInfo(ctx context.Context, memberInfo *dto.MemberInfo) (*dto.MemberInfo, error) {

	span, labels := common.GenerateSpan("memberInfoQuery.UpdateMemberInfo", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "memberInfoQuery.UpdateMemberInfo"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "memberInfoQuery.UpdateMemberInfo"))

	var (
		userInfo = memberInfo_ConvertDTOToDatastruct(ctx, memberInfo)
		queryMGR = m.dao.NewMemberInfoQuery()
	)
	// 1. Insert/Upsert data to database
	log.Info("Updating user in db")
	err := queryMGR.UpdateMemberInfo(ctx, userInfo)
	if err != nil {
		log.EndTime("memberInfoQuery.UpdateMemberInfo", startTime, err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	// 2. Passs information to cordial gateway.
	//    There it will add email to cordial, if it doews not exist and update the email with the WEb3_FDA_Users list
	log.Info("running check cordial")
	err = m.checkCordialUser(ctx, memberInfo)
	if err != nil {
		log.EndTime("memberInfoQuery.UpdateMemberInfo", startTime, err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	log.Info("running get member info")
	// 3. gets all info from the database after updates were successful
	member, err := queryMGR.GetMemberInfo(ctx, userInfo)
	if err != nil {

		log.EndTime("memberInfoQuery.UpdateMemberInfo", startTime, err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	// recalculates the grant information
	recalculatedGrants, err := m.RecalculateMemberGrants(ctx, member)
	if err != nil {
		log.EndTime("memberInfoQuery.UpdateMemberInfo", startTime, err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	member.Grants = *recalculatedGrants

	log.EndTime("memberInfoQuery.UpdateMemberInfo", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	//returns dto object wich is then sent to the client
	return memberInfo_ConvertDatastructToDTO(ctx, member), nil
}

func memberInfo_ConvertDTOToDatastruct(ctx context.Context, memberInfo *dto.MemberInfo) *datastruct.MemberInfo {
	sanitizedDisplayName := common.SanitizeString(memberInfo.DisplayName)
	sanitzedEmail := common.SanitizeString(memberInfo.EmailAddress)
	return &datastruct.MemberInfo{
		WalletAddress:    memberInfo.WalletAddress,
		EmailAddress:     sanitzedEmail,
		MemberId:         memberInfo.MemberId,
		DisplayName:      sanitizedDisplayName,
		RegistrationDate: memberInfo.RegistrationDate,
	}
}

func Build_GatewayContactsRequest_From_MemberInfo(ctx context.Context, memberInfo *dto.MemberInfo) *datastruct.CordialContactsRequest {

	log.Info(fmt.Sprintf("Building Cordial Request Body  email= %s ", memberInfo.EmailAddress))
	email := datastruct.Email{Address: memberInfo.EmailAddress, SubscribeStatus: "Subscribed"}
	channels := datastruct.Channels{Email: email}

	return &datastruct.CordialContactsRequest{
		Channels:           channels,
		AddToWeb3UsersList: true,
		ForceSubscribe:     true,
		AddToLegacyPass:    true,
	}

}

// TODO convert grants to a jwt
func memberInfo_ConvertDatastructToDTO(ctx context.Context, memberInfo *datastruct.MemberInfo) *dto.MemberInfo {

	// create a new dto jwt object base on data from the member info
	var dtoJWT = BuildJWTClaims(ctx, "fbs_web3", memberInfo)
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

func BuildJWTClaims(ctx context.Context, tokenMember string, memberInfo *datastruct.MemberInfo) jwt.MapClaims {
	var (
		claims jwt.MapClaims
		Grants []dto.Grant
	)
	// based on request
	switch tokenMember {
	case "fbs_web3":
		// convert datastruct grants to the dto.grants object
		for _, grant := range memberInfo.Grants {
			Grants = append(Grants, dto.Grant{
				GrantId:    grant.GrantId,
				Expiration: grant.Expiration.UnixMilli(),
			})
		}
		claims = jwt.MapClaims{
			"sub":    memberInfo.WalletAddress,
			"iat":    time.Now().Unix(),
			"iss":    "fbs_web3", // Subject (a unique identifier for the token)
			"grants": Grants,
		}
	}
	return claims
}

// RecalculateMemberGrants Reads available grants from local configuration cache, and updates a members grant expirations
//
// 1. Takes a context and memberinfo object
// 2. Loads grant configurations from local cache
// 3. iterates through each configuration
// 4. Prapres data for insert into postgres database
// 5. Prepares a grant array with recalculates expiration dates base on configuration data.
// 6. Persists data to postgres
// 7. returns the new grant information for a member.
func (m *memberInfoService) RecalculateMemberGrants(ctx context.Context, memberInfo *datastruct.MemberInfo) (*[]datastruct.Grant, error) {

	span, labels := common.GenerateSpan("memberinfoService.RecalculateMemberGrants", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "memberinfoService.RecalculateMemberGrants"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "memberinfoService.RecalculateMemberGrants"))

	var grantInfo []datastruct.UpsertGrantInfo
	cachedGrantConfigs := m.dao.GetCachedGrants()
	var grants []datastruct.Grant

	for _, config := range *cachedGrantConfigs {

		var expiration_date time.Time

		if config.StartOnRegistration == true {
			expiration_date = memberInfo.RegistrationDate.AddDate(0, int(config.MonthsUntilExpiration), 0)
		} else if config.StartTime != nil {
			expiration_date = config.StartTime.AddDate(0, int(config.MonthsUntilExpiration), 0)
		} else {
			log.ErrorL(labels, "Could Not parse Grant Start Time. Moving to next Grant")
		}
		// builds the grant info for upsert
		grantInfo = append(grantInfo, datastruct.UpsertGrantInfo{
			WalletAddress: memberInfo.WalletAddress,
			GrantId:       config.GrantId,
			Expiration:    expiration_date,
		})
		// builds grant info to avoid performing another get
		grants = append(grants, datastruct.Grant{
			GrantId:    config.GrantId,
			Expiration: expiration_date,
		})
	}
	// inserts the updated grants
	err := m.dao.NewMemberInfoQuery().InsertMemberGrantsInfo(ctx, &grantInfo)
	if err != nil {
		log.EndTime("memberinfoQuery.RecalculateMemberGrants", startTime, err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err

	}

	log.EndTime("memberinfoService.RecalculateMemberGrants", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return &grants, nil

}

// CheckCordialUser takes context and *datastruct.MemberInfo
// there are three steps in this function:
// 1- GetUserProfileFromCordial here will get the user data from Cordial
// 2- Will check the response that returned from GetUserProfileFromCordial if user not exist will create one and add to our list
// 3- Will check the response that returned from GetUserProfileFromCordial if user exist will add to our list
// if the process done successfully it will return nil in not will return error
func (m *memberInfoService) checkCordialUser(ctx context.Context, memberInfo *dto.MemberInfo) error {

	span, labels := common.GenerateSpan("memberInfoService.checkCordialUser", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "memberInfoService.checkCordialUser"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "memberInfoService.checkCordialUser"))

	var (
		cordialContactsRequest = Build_GatewayContactsRequest_From_MemberInfo(ctx, memberInfo)
	)
	log.Info("Calling GetUserProfileFromCordial ")
	cordialResponse, err := m.cordial.GetUserProfileFromCordial(ctx, cordialContactsRequest.Channels.Email.Address)
	cordialUser := cordialResponse.ID
	// If the user does not exist in Cordial we need to create new account for it and add it to the FDA list in Cordial
	if cordialResponse.Message != nil && *cordialResponse.Message == "record not found" {
		log.Info("cordial user was not found ")
		_, err := m.cordial.CreateCordialUserProfile(ctx, cordialContactsRequest)
		if err != nil {
			log.ErrorL(labels, "Could Not Create Cordial User: %s", err)
			log.EndTime("memberInfoService.checkCordialUser", startTime, err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
	}

	// if we have a user and the user does not have the "Web3_FDA_Users" in their lists add it
	if cordialUser != nil && !slices.Contains(cordialResponse.Attributes.Lists, "Web3_FDA_Users") {
		log.Info("cordial does not have web3_fda_users list ")
		// We need to rebuild the CordialContactsUrl for Update response, because it different from Post URL
		res, err := m.cordial.UpdateCordialUserProfile(ctx, cordialContactsRequest)

		if res != nil && res.Success != nil && *res.Success == true {
			//do nothing. operation successful
		} else if err != nil {
			//if the error is set internally something went wrong throw error and return
			log.ErrorL(labels, "Error when processing update codial user request: %s", err)
			log.EndTime("memberInfoService.checkCordialUser", startTime, err)
			span.SetStatus(codes.Error, err.Error())
			return err
		} else {
			//Log a warning there was an issue with updating the users codial profile
			log.WarningL(labels, "Cordial Failed to Update the user profile: %s", err)
		}

	}

	if err != nil {
		log.ErrorL(labels, "Error Calling Cordial: %s", err)
		log.EndTime("memberInfoService.checkCordialUser", startTime, err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	log.EndTime("memberInfoService.checkCordialUser", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return nil
}
