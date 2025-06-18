package datastruct

import "time"

type Announcements struct {
	Description          string                 `json:"description" firestore:"description"` // This will present the Announcement Global Description
	AnnouncementsDetails []AnnouncementsDetails `json:"lists" firestore:"lists"`             // This will present the list of Announcements Details
}

type AnnouncementsDetails struct {
	PublishedDate time.Time `json:"publishedDate,omitempty" firestore:"publishedDate"` // It will present the Published Date for the announcement
	Header        string    `json:"header,omitempty" firestore:"header"`               // It will present the Announcement Headline
	Description   string    `json:"description,omitempty" firestore:"description"`     // It will present the Announcement Description
	Link          string    `json:"link,omitempty" firestore:"link"`                   // It will present the external link
}
