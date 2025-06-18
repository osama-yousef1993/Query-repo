
r.HandleFunc("/upload", Process).Methods(http.MethodGet, http.MethodOptions)


const (
	uploadURL = "https://upload.twitter.com/1.1/media/upload.json"
	// tweetURL  = "https://api.twitter.com/1.1/statuses/update.json"
	tweetURL = "https://api.twitter.com/2/tweets"
)

type SubTweetResponse struct {
	Text string `json:"text"`
	Id   string `json:"id"`
}

type TweetResponse struct {
	Data SubTweetResponse `json:"data"`
}

func Process(w http.ResponseWriter, r *http.Request) {
	// Twitter API credentials
	consumerKey := "g73oT387VeI3aFSC9c53gSp0x"
	consumerSecret := "OYu7sqZ82rM6nZgNthNJ7GrGBkyd6R6HJ01FmcSPgXkUzqe5t0"
	accessToken := "1794648127716179968-VgzObXseRlSacp5cJqo1U4co01gEU0"
	accessSecret := "3WY89V0dr5PhhbCm1VSYEJyW7E0SG74N5OZAVsdE5SY5S"

	// URL of the image to upload
	imageURL := "https://bafybeihqiqjvrvtoscppn4aty4mdq32wsiqsqd6cjc2rfzyzgkmkmorlmm.ipfs.dweb.link/images/1.jpg"

	// OAuth1 authentication setup
	config := oauth1.NewConfig(consumerKey, consumerSecret)
	token := oauth1.NewToken(accessToken, accessSecret)
	httpClient := config.Client(oauth1.NoContext, token)

	// Step 1: Download the image
	filePath, err := DownloadImage(imageURL)
	if err != nil {
		log.Error("Failed to download image: %v", err)
	}
	defer os.Remove(filePath)

	// Step 2: Upload the image
	mediaID, err := UploadMedia(httpClient, filePath)
	if err != nil {
		log.Error("Failed to upload media: %v", err)
	}

	// Step 3: Post a tweet with the uploaded media
	tweetText := "Here is an image"
	var result TweetResponse
	response := PostTweet(httpClient, tweetText, mediaID)
	json.Unmarshal(response, &result)

	res, _ := json.Marshal(result)
	fmt.Println("Tweet posted successfully!")
	fmt.Printf("%v", result)
	w.WriteHeader(200)
	w.Write(res)
	return
}

// DownloadImage downloads an image from a URL and saves it to a temporary file
func DownloadImage(url string) (string, error) {
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	tempFile, err := os.CreateTemp("", "image-*.jpg")
	if err != nil {
		return "", err
	}
	defer tempFile.Close()

	_, err = io.Copy(tempFile, resp.Body)
	if err != nil {
		return "", err
	}

	return tempFile.Name(), nil
}

// UploadMedia uploads an image to Twitter and returns the media ID
func UploadMedia(client *http.Client, filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	var b bytes.Buffer
	writer := multipart.NewWriter(&b)
	part, err := writer.CreateFormFile("media", filepath.Base(file.Name()))
	if err != nil {
		return "", err
	}
	_, err = io.Copy(part, file)
	if err != nil {
		return "", err
	}
	writer.Close()

	req, err := http.NewRequest("POST", uploadURL, &b)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var response struct {
		MediaIDString string `json:"media_id_string"`
	}
	err = json.NewDecoder(resp.Body).Decode(&response)
	if err != nil {
		return "", err
	}

	return response.MediaIDString, nil
}

type TMedia struct {
	MediaIds []string `json:"media_ids"`
}
type ResT struct {
	Text  string `json:"text"`
	Media TMedia `json:"media"`
}

// PostTweet posts a tweet with the specified text and media ID
func PostTweet(client *http.Client, text string, mediaID string) []byte {
	var res ResT
	res.Text = text
	t := TMedia{MediaIds: []string{mediaID}}
	res.Media = t
	resMedia, err := json.Marshal(res)

	req, err := http.NewRequest("POST", tweetURL, strings.NewReader(string(resMedia)))
	if err != nil {
		return nil
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		// fmt.Printf("%s", resp.)
		// Log response body for debugging

		fmt.Printf("unexpected status code: %d", resp.StatusCode)
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("unexpected status code: %d, response: %s", resp.StatusCode, string(body))
		return nil
		// return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	return body
}
