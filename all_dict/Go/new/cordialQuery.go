package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/Forbes-Media/crypto-backend-api/HTTPGateway"
	"github.com/Forbes-Media/crypto-backend-api/datastruct"
	"github.com/Forbes-Media/crypto-backend-api/repository/common"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
)

var header = http.Header{
	"Content-Type":  {"application/json"},
	"authorization": {fmt.Sprintf("%s%s", "Basic ", os.Getenv("CORDIAL_SECRET"))},
}

var (
	CordialContactsUrl = os.Getenv("CORDIAL_ACCESS")
	FDAList            = "Web3_FDA_Users"
)

type CordialGateway interface {
	CreateUpdateCordialUser(ctx context.Context, memberInfo *datastruct.MemberInfo, httpMethod string, listName string) error
	GetUserProfileFromCordial(ctx context.Context, memberInfo *datastruct.MemberInfo) (*datastruct.GetCordialContactResponse, error)
	CheckCordialUser(ctx context.Context, memberInfo *datastruct.MemberInfo) error
}

type cordialGateway struct{}

// CreateUpdateCordialUser it Takes context, *datastruct.MemberInfo, httpMethod as string and the listName as string
// it will build the request body to CallCordial function
// this function will create a new Cordial user Or it will update an exist user
// it will return error if exist and nil if not exist
func (c *cordialGateway) CreateUpdateCordialUser(ctx context.Context, memberInfo *datastruct.MemberInfo, httpMethod string, listName string) error {
	span, labels := common.GenerateSpan("CreateUpdateCordialUser", ctx)
	defer span.End()

	span.AddEvent("Create New Cordial User")
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "CreateUpdateCordialUser"))

	// Build channels body for Cordial call endpoints
	email := datastruct.Email{Address: memberInfo.EmailAddress, SubscribeStatus: "unsubscribed", SubscribedAt: time.Now()}
	channels := datastruct.Channels{Email: email}
	// Create the request body with exact format from Cordial
	body := make(map[string]interface{})
	// Add list name that the user will be add to.
	body[listName] = true
	body["channels"] = channels

	// Convert the request body to json object
	reqBody, err := json.Marshal(body)
	if err != nil {
		log.EndTime("CreateUpdateCordialUser", startTime, nil)
		span.SetStatus(codes.Error, err.Error())
		return nil
	}

	// Call Cordial endpoint with our requested data.
	_, err = HTTPGateway.CallCordial[datastruct.GetCordialContactResponse](ctx, CordialContactsUrl, string(reqBody), httpMethod, header)

	if err != nil {
		log.EndTime("CreateUpdateCordialUser", startTime, nil)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	log.EndTime("tokenManagerService.VerifyHMAC", startTime, nil)
	return nil
}

// GetUserProfileFromCordial Takes context and  *datastruct.MemberInfo
// It will check if the user exist in Cordial and it will return the data for this user from Cordial
// If it not exist it will return error
func (c *cordialGateway) GetUserProfileFromCordial(ctx context.Context, memberInfo *datastruct.MemberInfo) (*datastruct.GetCordialContactResponse, error) {
	span, labels := common.GenerateSpan("memberInfoQuery.UpdateMemberInfo", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "memberInfoQuery.UpdateMemberInfo"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "memberInfoQuery.UpdateMemberInfo"))

	// Build the endpoint that will be hit to get the user data from Cordial.
	url := fmt.Sprintf("%s%s%s", CordialContactsUrl, "/", memberInfo.EmailAddress)
	// Get the response from Cordial.
	t, err := HTTPGateway.CallCordial[datastruct.GetCordialContactResponse](ctx, url, "", "GET", header)

	if err != nil {
		// Check the error response if it contains 404 this mean the user not exist in Cordial
		if strings.Contains(err.Error(), "404 Not Found") {
			log.Info("User With this Email Not Exist")
			return nil, nil
		}
		log.EndTime("memberInfoQuery.GetUserProfileFromCordial", startTime, nil)
		return nil, err
	}

	log.EndTime("tokenManagerService.VerifyHMAC", startTime, nil)

	return t, nil
}

// CheckCordialUser takes context and *datastruct.MemberInfo
// there are three steps in this function:
// 1- GetUserProfileFromCordial here will get the user data from Cordial
// 2- Will check the response that returned from GetUserProfileFromCordial if user not exist will create one and add to our list
// 3- Will check the response that returned from GetUserProfileFromCordial if user exist will add to our list
// if the process done successfully it will return nil in not will return error
func (c *cordialGateway) CheckCordialUser(ctx context.Context, memberInfo *datastruct.MemberInfo) error {

	span, labels := common.GenerateSpan("memberInfoQuery.CheckCordialUser", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "memberInfoQuery.CheckCordialUser"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "memberInfoQuery.UpdateMemberInfo"))

	cordialUser, err := c.GetUserProfileFromCordial(ctx, memberInfo)

	// If the User Not exist in Cordial We need to create new Account for it and add it to the FDA list in Cordial
	if cordialUser == nil && err == nil {
		err := c.CreateUpdateCordialUser(ctx, memberInfo, "POST", FDAList)
		if err != nil {
			log.Info("User Not created in Cordial")
			goto ERR
		}
	}

	// if the user exist in Cordial We need to add it to the FDA list in Cordial
	if cordialUser != nil {
		err := c.CreateUpdateCordialUser(ctx, memberInfo, "PUT", FDAList)
		if err != nil {
			log.Info("User Not created in Cordial")
			goto ERR
		}
	}

	if err != nil {
		log.Info("User Not created in Cordial")
		goto ERR
	}

	log.EndTime("memberInfoQuery.CheckCordialUser", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return nil

ERR:
	log.EndTime("memberInfoQuery.UpdateMemberInfo", startTime, nil)
	span.SetStatus(codes.Error, err.Error())
	return err
}
