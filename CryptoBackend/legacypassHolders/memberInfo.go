package datastruct

import (
	"time"

	"cloud.google.com/go/bigquery"
)

type MemberInfo struct {
	WalletAddress    string    `json:"wallet_addr"`            // wallet address of user
	EmailAddress     string    `json:"email_addr"`             // email address of user
	MemberId         string    `json:"member_id"`              //member id of user
	RegistrationDate time.Time `json:"registration_date"`      //date the user registered to be a community member
	DisplayName      string    `json:"display_name"`           //display name a user chose
	Grants           []Grant   `json:"grants"`                 //An array of granted products
	FDAClgnJWT       *string   `json:"fda_clgn_jwt,omitempty"` // By passes signature request on FE x amount of days
}

type Grant struct {
	GrantId    string    `json:"grant_id"`        //id of the promotion ex:fin_cry
	Expiration time.Time `json:"expiration_date"` //expiration date of the grant
}

// Object that is upserted to postgres
type UpsertGrantInfo struct {
	WalletAddress string    `json:"wallet_addr"`     // wallet address of user
	GrantId       string    `json:"grant_id"`        //id of the promotion ex:fin_cry
	Expiration    time.Time `json:"expiration_date"` //expiration time of the grant
}

// Object that map user data from postgres
type CommunityMemberInfo struct {
	EmailAddress     int64     `postgres:"email_addr"`        // email address of user
	MemberId         int64     `postgres:"member_id"`         // member id of user
	RegistrationDate time.Time `postgres:"registration_date"` // date the user registered to be a community member
	DisplayName      string    `postgres:"display_name"`      // display name a user chose
	GrantExpiration  time.Time `postgres:"grant_expiration"`  // free trail end date for the user
}

// BQCommunityMemberInfo That will inserted to Bigquery
type BQCommunityMemberInfo struct {
	EmailAddress     bigquery.NullInt64     `bigquery:"email_addr"`          // email address of user
	MemberId         bigquery.NullInt64     `bigquery:"member_id"`           // member id of user
	RegistrationDate bigquery.NullTimestamp `bigquery:"registration_date"`   // date the user registered to be a community member
	DisplayName      string                 `bigquery:"display_name"`        // display name a user chose
	GrantExpiration  bigquery.NullTimestamp `bigquery:"free_trial_end_date"` // free trail end date for the user
	RowLastUpdated   bigquery.NullTimestamp `bigquery:"row_last_updated"`    // last time the record updated
}

// Object that map legacy pass user data from MySql
type LegacyPassInfo struct {
	Id    int64  `json:"id" mysql:"id"`       // id of user
	Email string `json:"email" mysql:"email"` // email address of user
}

// Object that map legacy pass user data from MySql to BQ
type BQLegacyPassInfo struct {
	Id                 bigquery.NullInt64     `bigquery:"id"`                 // id of user
	Email              bigquery.NullString    `bigquery:"email"`              // email address of user
	IsLegacyPassHolder bigquery.NullBool      `bigquery:"isLegacyPassHolder"` // email address of user
	RowLastUpdated     bigquery.NullTimestamp `bigquery:"row_last_updated"`   // last time the record updated
}

type CommunityMemberData struct {
	WalletAddress string `postgres:"wallet_addr"`
	EmailAddress  string `postgres:"email_addr"` // email address of user
}
