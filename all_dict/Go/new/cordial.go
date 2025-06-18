package common

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
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
)

var header = http.Header{
	"Content-Type":  {"application/json"},
	"authorization": {fmt.Sprintf("%s%s", "Basic ", os.Getenv("CORDIAL_CONTACTS_KEY"))},
}

var (
	CordialContactsUrl = os.Getenv("CORDIAL_CONTACTS_URL")
	FDAList            = "Web3_FDA_Users"
	FINCRYList         = "Web3_FDA_Users"
)

func CreateUpdateCordialUser(ctx context.Context, memberInfo *datastruct.MemberInfo, httpMethod string, listName string) error {
	span, labels := GenerateSpan("CreateUpdateCordialUser", ctx)
	defer span.End()

	span.AddEvent("Create New Cordial User")
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "CreateUpdateCordialUser"))

	email := datastruct.Email{Address: memberInfo.EmailAddress, SubscribeStatus: "unsubscribed", SubscribedAt: time.Now()}
	channels := datastruct.Channels{Email: email}
	body := make(map[string]interface{})
	body[listName] = true
	body["channels"] = channels

	reqBody, err := json.Marshal(body)
	if err != nil {
		log.EndTime("CreateUpdateCordialUser", startTime, nil)
		span.SetStatus(codes.Error, err.Error())
		return nil
	}

	_, err = HTTPGateway.CallCordial[datastruct.GetCordialContactResponse](ctx, CordialContactsUrl, string(reqBody), httpMethod, header)

	if err != nil {
		log.EndTime("CreateUpdateCordialUser", startTime, nil)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	log.EndTime("tokenManagerService.VerifyHMAC", startTime, nil)
	return nil
}

func GetUserProfileFromCordial(ctx context.Context, memberInfo *datastruct.MemberInfo) (*datastruct.GetCordialContactResponse, error) {
	span, labels := GenerateSpan("memberInfoQuery.UpdateMemberInfo", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "memberInfoQuery.UpdateMemberInfo"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "memberInfoQuery.UpdateMemberInfo"))

	url := fmt.Sprintf("%s%s%s", CordialContactsUrl, "/", memberInfo.EmailAddress)
	t, err := HTTPGateway.CallCordial[datastruct.GetCordialContactResponse](ctx, url, "", "GET", header)

	if err != nil {
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

func CheckCordialUser(ctx context.Context, memberInfo *datastruct.MemberInfo) error {

	span, labels := GenerateSpan("memberInfoQuery.CheckCordialUser", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "memberInfoQuery.CheckCordialUser"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "memberInfoQuery.UpdateMemberInfo"))

	cordialUser, err := GetUserProfileFromCordial(ctx, memberInfo)

	// If the User Not exist in Cordial We need to create new Account for it and add it to the FDA list in Cordial
	if cordialUser == nil && err == nil {
		err := CreateUpdateCordialUser(ctx, memberInfo, "POST", FDAList)
		if err != nil {
			log.Info("User Not created in Cordial")
			goto ERR
		}
	}

	// if the user exist in Cordial We need to add it to the FDA list in Cordial
	if cordialUser != nil {
		err := CreateUpdateCordialUser(ctx, memberInfo, "PUT", FDAList)
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

func CheckGrandAuthorization(ctx context.Context, memberInfo *datastruct.MemberInfo) error {
	span, labels := GenerateSpan("memberInfoQuery.CheckGrandAuthorization", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "memberInfoQuery.CheckGrandAuthorization"))
	var err error
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "memberInfoQuery.CheckGrandAuthorization"))
	if memberInfo.Grants != nil {
		for _, grant := range memberInfo.Grants {
			if grant.GrantId == "fin_cry" {
				if grant.Expiration.After(time.Now()) {
					err = CreateUpdateCordialUser(ctx, memberInfo, "PUT", FINCRYList)
					if err != nil {
						log.Info("User Not created in Cordial")
						goto ERR
					}
				}
			}
		}
	}
	log.EndTime("memberInfoQuery.CheckGrandAuthorization", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return nil

ERR:
	log.EndTime("memberInfoQuery.UpdateMemberInfo", startTime, nil)
	span.SetStatus(codes.Error, err.Error())
	return err
}
