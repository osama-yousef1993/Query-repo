package datastruct

import "time"

// SortDirection is a type of string
type SortDirection string

// Enum for valis sort direction values
const (
	Ascending  SortDirection = "asc"
	Descending SortDirection = "desc"
)

// GetCordialContactsParams contains all parameter options for the Cordial Gateway Get Contacts Call
type GetCordialContactsParams struct {
	Email       string        //Filter records for a specific contact using the email address value.
	AudiencKey  string        //Filter records for contacts in an audience using a saved audience rule key.
	Page        string        //Specific page number to be returned
	PerPage     string        //Number or records per page.
	SortBy      string        //Field by which results should be sorted.
	sort_dir    SortDirection //Direction to sort by. Works in conjunction with sort_by (e.g. asc, desc).
	ReturnCount bool          //Show the total count of records returned.
}

// Information returned from the Cordial get contacts call
type GetCordialContactsResponse []struct {
	ID         string     `json:"_id"`        // Primary ID of Contact
	Attributes Attributes `json:"attributes"` // Attributes of the contact
	Channels   Channels   `json:"channels"`   // Contact information
}

type GetCordialContactResponse struct {
	ID         string     `json:"_id"`        // Primary ID of Contact
	Attributes Attributes `json:"attributes"` // Attributes of the contact
	Channels   Channels   `json:"channels"`   // Contact information
}

// ListJoinDate it contains all the lists and the date that user Register to it ex("DailyDozen": "2022-12-07T01:48:50+0000")
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

// Loc contains latitude and longitude for the User
type Loc struct {
	Lat string `json:"lat"` // latitude index
	Lon string `json:"lon"` // longitude index
}

// Geo Location Information related to Cordial User
type Geolocation struct {
	Auto          bool   `json:"auto"`
	Country       string `json:"country"`        // User Country
	CountryISO    string `json:"countryISO"`     // Country Code
	Loc           Loc    `json:"loc"`            // Loc contains latitude and longitude for the User
	PostalCode    string `json:"postal_code"`    // User Postal code
	State         string `json:"state"`          // User State
	Tz            string `json:"tz"`             // User Timezone ex(Europe/London)
	StreetAddress string `json:"street_address"` // User street address
	City          string `json:"city"`           // User City
}
type Attributes struct {
	CreatedAt          string        `json:"createdAt"`            // User Account created
	LastName           string        `json:"LastName"`             // User LastName
	PianoID            string        `json:"pianoId"`              // User PianoID
	CompanyIndustry    string        `json:"company-industry"`     // Company Industry User Work in
	FunctionalArea     string        `json:"functional_area"`      //
	Gender             string        `json:"gender"`               // User gender
	Seniority          string        `json:"seniority"`            //
	BlueconicSegments  string        `json:"blueconic_segments"`   //
	LastUpdateSource   string        `json:"lastUpdateSource"`     // Last source updated User Profile
	Lists              []string      `json:"lists"`                // Array of List User Join in
	LastModified       string        `json:"lastModified"`         // last time User account modified
	ListJoinDate       ListJoinDate  `json:"listJoinDate"`         // All the lists and the date that User Join to
	MdtLastSent        MdtLastSent   `json:"mdtLastSent"`          //
	Dob                time.Time     `json:"dob"`                  // user Day of birth
	BlueconicDataDate  time.Time     `json:"blueconic_data_date"`  // blueconic data date
	FollowSource       []string      `json:"follow_source"`        // array of follow Source ex('article')
	FollowedAuthorSlug []string      `json:"followed_author_slug"` // Author slug
	FollowedNaturalID  []string      `json:"followed_natural_id"`  // Author natural id
	Sourcearray        []string      `json:"sourcearray"`          //
	Eventinterests     []string      `json:"eventinterests"`       //
	Unsubscribe        []interface{} `json:"unsubscribe"`          //
	HasBeenWelcomed    []string      `json:"has_been_welcomed"`    //
	Geolocation        Geolocation   `json:"geolocation"`          // User Geo Data
	CID                string        `json:"cID"`                  // Primary ID of Contact
	ID                 string        `json:"ID"`                   // User Primary ID
}
type Email struct {
	Address         string    `json:"address"`         // email address
	SubscribeStatus string    `json:"subscribeStatus"` // status if they are sub scribed
	SubscribedAt    time.Time `json:"subscribedAt"`    // time they subscribed
}
type Channels struct {
	Email Email `json:"email"`
}
