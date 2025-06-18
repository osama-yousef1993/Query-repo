package datastruct

import (
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/civil"
)

var EventsCollectionName = fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "events")

// will use this struct to map Event data
type Event struct {
	Headline      string     `json:"headline" firestore:"headline"`           // event headline
	StartDateTime time.Time  `json:"startDateTime" firestore:"startDateTime"` // event startDateTime
	StartDate     civil.Date `json:"startDate" firestore:"startDate"`         // event startDate
	StartTime     civil.Time `json:"startTime" firestore:"startTime"`         // event startTime
	EndDateTime   time.Time  `json:"endDateTime" firestore:"endDateTime"`     // event endDateTime
	EndDate       civil.Date `json:"endDate" firestore:"endDate"`             // event endDate
	EndTime       civil.Time `json:"endTime" firestore:"endTime"`             // event endTime
	Description   string     `json:"description" firestore:"description"`     // event description
	EventImage    string     `json:"eventImage" firestore:"eventImage"`       // event eventImage
	TagText       string     `json:"tagText" firestore:"tag"`                 // event tag
	Location      string     `json:"location" firestore:"location"`           // event location
	EventURL      string     `json:"eventURL" firestore:"url"`                // event url
}

// will use this struct to map all Events data
type Events struct {
	FeaturedEvent *Event  `json:"featuredEvent" firestore:"featuredEvent"` // featured Event
	FutureEvents  []Event `json:"futureEvents" firestore:"futureEvents"`   // future Events
	PastEvents    []Event `json:"pastEvents" firestore:"pastEvents"`       // Past events
}
