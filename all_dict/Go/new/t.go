package structs

import "time"

//SortDirection is a type of string
type SortDirection string

//Enum for valis sort direction values
const (
	Ascending  SortDirection = "asc"
	Descending SortDirection = "desc"
)

// GetCordialContactsParams contains all parameter options for the Coordial Gateway Get Contacts Call
type GetCordialContactsParams struct {
	Email       string        //Filter records for a specific contact using the email address value.
	AudiencKey  string        //Filter records for contacts in an audience using a saved audience rule key.
	Page        string        //Specific page number to be returned
	PerPage     string        //Number or records per page.
	SortBy      string        //Field by which results should be sorted.
	sort_dir    SortDirection //Direction to sort by. Works in conjunction with sort_by (e.g. asc, desc).
	ReturnCount bool          //Show the total count of records returned.
}

//Information returned from the codial get conacts call
type GetCordialContactsResponse []struct {
	ID         string     `json:"_id"`        // Primary ID of Contact
	Attributes Attributes `json:"attributes"` // Attributes of the contact
	Channels   Channels   `json:"channels"`   // Contact infomation
}
type ListJoinDate struct {
	Registration         string `json:"Registration"`
	Follow               string `json:"Follow"`
	DailyDozen           string `json:"DailyDozen"`
	ChecksimbalancesFree string `json:"checksimbalances-free"`
	CryptocodexFree      string `json:"cryptocodex-free"`
	CryptoConfidential   string `json:"CryptoConfidential"`
	TransformationalTech string `json:"TransformationalTech"`
	EHFTeam              string `json:"EHFTeam"`
	ForbesWeekly         string `json:"ForbesWeekly"`
	InvestingDigest      string `json:"InvestingDigest"`
}
type MdtLastSent struct {
	FiveE3F03705B099Ce02F2Ea284 time.Time `json:"5e3f03705b099ce02f2ea284"`
	FiveD1Ca3921802C8C5243D4E66 time.Time `json:"5d1ca3921802c8c5243d4e66"`
	FiveDf3A796A806E2781760C8D7 time.Time `json:"5df3a796a806e2781760c8d7"`
	Six1F9B46B6E1A1D1211Ac31B6  time.Time `json:"61f9b46b6e1a1d1211ac31b6"`
	Six1B24Df96E1A1D12112F08Bc  time.Time `json:"61b24df96e1a1d12112f08bc"`
	Six0Ad546Afe2C195E911Eff1E  time.Time `json:"60ad546afe2c195e911eff1e"`
	Six06F2A53Fe2C195E91F42122  time.Time `json:"606f2a53fe2c195e91f42122"`
	Six0Bf7Bc1Fe2C195E91692A1D  time.Time `json:"60bf7bc1fe2c195e91692a1d"`
	Six1Fd6D586E1A1D1211Eb3Ec5  time.Time `json:"61fd6d586e1a1d1211eb3ec5"`
	Six22232226E1A1D1211Adb803  time.Time `json:"622232226e1a1d1211adb803"`
	Six116B4E4521306D4897Cf391  time.Time `json:"6116b4e4521306d4897cf391"`
	Six5119Cbd554Da33A6Bf67568  time.Time `json:"65119cbd554da33a6bf67568"`
}
type Loc struct {
	Lat string `json:"lat"`
	Lon string `json:"lon"`
}
type Geolocation struct {
	Auto          bool   `json:"auto"`
	Country       string `json:"country"`
	CountryISO    string `json:"countryISO"`
	Loc           Loc    `json:"loc"`
	PostalCode    string `json:"postal_code"`
	State         string `json:"state"`
	Tz            string `json:"tz"`
	StreetAddress string `json:"street_address"`
	City          string `json:"city"`
}
type Attributes struct {
	CreatedAt          string        `json:"createdAt"`
	LastName           string        `json:"LastName"`
	PianoID            string        `json:"pianoId"`
	CompanyIndustry    string        `json:"company-industry"`
	FunctionalArea     string        `json:"functional_area"`
	Gender             string        `json:"gender"`
	Seniority          string        `json:"seniority"`
	BlueconicSegments  string        `json:"blueconic_segments"`
	LastUpdateSource   string        `json:"lastUpdateSource"`
	Lists              []string      `json:"lists"`
	LastModified       string        `json:"lastModified"`
	ListJoinDate       ListJoinDate  `json:"listJoinDate"`
	MdtLastSent        MdtLastSent   `json:"mdtLastSent"`
	Dob                time.Time     `json:"dob"`
	BlueconicDataDate  time.Time     `json:"blueconic_data_date"`
	FollowSource       []string      `json:"follow_source"`
	FollowedAuthorSlug []string      `json:"followed_author_slug"`
	FollowedNaturalID  []string      `json:"followed_natural_id"`
	Sourcearray        []string      `json:"sourcearray"`
	Eventinterests     []string      `json:"eventinterests"`
	Unsubscribe        []interface{} `json:"unsubscribe"`
	HasBeenWelcomed    []string      `json:"has_been_welcomed"`
	Geolocation        Geolocation   `json:"geolocation"`
	CID                string        `json:"cID"`
	ID                 string        `json:"ID"`
}
type Email struct {
	Address         string    `json:"address"`         // email address
	SubscribeStatus string    `json:"subscribeStatus"` // status if they are sub scribed
	SubscribedAt    time.Time `json:"subscribedAt"`    // time they subscribed
}
type Channels struct {
	Email Email `json:"email"`
}
