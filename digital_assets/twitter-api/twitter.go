package datastruct

// Twitter API credentials
var (
	// my cred
	ConsumerKey    string = "o9vncMMM6RNSyUZkt8CvDzjcG"
	ConsumerSecret string = "AnyvBZ9taZ1BeY0SyAkmmoZzj7pahOuMGQ30Hy6Sgj5TbpGFzJ"
	AccessToken    string = "1797280081041039360-1K3Ycy0Ye6O6cJxO2xKSAEtF53nBb1"
	AccessSecret   string = "vJiQ6yLU2j3rNin4B56arWn23P60Rlsxsf6AEdbklXjxx"
	// Ahmad cred
	// ConsumerKey    string = "g73oT387VeI3aFSC9c53gSp0x"
	// ConsumerSecret string = "OYu7sqZ82rM6nZgNthNJ7GrGBkyd6R6HJ01FmcSPgXkUzqe5t0"
	// AccessToken    string = "1794648127716179968-VgzObXseRlSacp5cJqo1U4co01gEU0"
	// AccessSecret   string = "3WY89V0dr5PhhbCm1VSYEJyW7E0SG74N5OZAVsdE5SY5S"
	// URL of the image to upload
	ImageURL  string = "https://bafybeihqiqjvrvtoscppn4aty4mdq32wsiqsqd6cjc2rfzyzgkmkmorlmm.ipfs.dweb.link/images/1.jpg"
	UploadURL string = "https://upload.twitter.com/1.1/media/upload.json"
	// tweetURL  = "https://api.twitter.com/1.1/statuses/update.json"
	TweetURL         string = "https://api.twitter.com/2/tweets"
	TokenURl         string = "https://api.twitter.com/2/oauth2/token"
	MyConsumerKey           = "o9vncMMM6RNSyUZkt8CvDzjcG"
	MyConsumerSecret        = "AnyvBZ9taZ1BeY0SyAkmmoZzj7pahOuMGQ30Hy6Sgj5TbpGFzJ"

	RequestTokenURL  = "https://api.twitter.com/oauth/request_token"
	AuthorizationURL = "https://api.twitter.com/oauth/authorize"
	CallbackURL      = "http://localhost:3000/v1/twitter/callback"
	RequestSecret    = ""
	TokenSecret      = ""
)

type RequestBody struct {
	AccessToken string `json:"access_token"`
	Text        string `json:"text"`
	ImageURL    string `json:"image_url"`
}

// This struct will use to add the media Ids from twitter after we push it to twitter
type TweetMedia struct {
	MediaIds []string `json:"media_ids"`
}

// Twitter request
// we will use this struct to send data that we need to post on twitter
type TweetRequest struct {
	Text  string     `json:"text"`  // the text we need to add as post
	Media TweetMedia `json:"media"` // the media Ids from twitter after we publish the image to twitter
}

// we will use this struct to map the twitter response
type TweetPostResponse struct {
	Text string `json:"text"` // this will contain the post link
	Id   string `json:"id"`   // this will contain the post id
}

// we will use this struct to return the response to FE side
type TwitterResponse struct {
	Data TweetPostResponse `json:"data"` // this is object will contain the post response data
}

type ShareContent struct {
	Title       string `json:"title"`
	Description string `json:"description"`
	URL         string `json:"url"`
}
