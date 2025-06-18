package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
)

type TwitterQuery interface {
	DownloadImage(ctx context.Context, url string) (string, error)
	UploadMedia(ctx context.Context, client *http.Client, filePath string, accessToken string) (string, error)
	PostTweet(ctx context.Context, client *http.Client, text string, mediaID string) (*datastruct.TwitterResponse, error)
}

type twitterQuery struct{}

// DownloadImage downloads an image from a URL and saves it to a temporary file
func (t *twitterQuery) DownloadImage(ctx context.Context, url string) (string, error) {
	span, labels := common.GenerateSpan("DownloadImage", ctx)
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "DownloadImage"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "DownloadImage"))

	resp, err := http.Get(url)
	if err != nil {
		log.ErrorL(labels, "DownloadImage %s", err)
		return "", err
	}
	defer resp.Body.Close()

	tempFile, err := os.CreateTemp("", "image-*.jpg")
	if err != nil {
		log.ErrorL(labels, "DownloadImage %s", err)
		return "", err
	}
	defer tempFile.Close()

	_, err = io.Copy(tempFile, resp.Body)
	if err != nil {
		log.ErrorL(labels, "DownloadImage %s", err)
		return "", err
	}
	log.EndTimeL(labels, "DownloadImage", startTime, nil)
	span.SetStatus(codes.Ok, "DownloadImage")
	return tempFile.Name(), nil
}

// UploadMedia uploads an image to Twitter and returns the media ID
func (t *twitterQuery) UploadMedia(ctx context.Context, client *http.Client, filePath string, accessToken string) (string, error) {
	span, labels := common.GenerateSpan("UploadMedia", ctx)
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "UploadMedia"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "UploadMedia"))

	file, err := os.Open(filePath)
	if err != nil {
		log.ErrorL(labels, "DownloadImage %s", err)
		return "", err
	}
	defer file.Close()

	var b bytes.Buffer
	writer := multipart.NewWriter(&b)
	part, err := writer.CreateFormFile("media", filepath.Base(file.Name()))
	if err != nil {
		log.ErrorL(labels, "UploadMedia %s", err)
		return "", err
	}
	_, err = io.Copy(part, file)
	if err != nil {
		log.ErrorL(labels, "UploadMedia %s", err)
		return "", err
	}
	writer.Close()

	req, err := http.NewRequest("POST", datastruct.UploadURL, &b)
	if err != nil {
		log.ErrorL(labels, "UploadMedia %s", err)
		return "", err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	// req.Header.Set("Authorization", "Bearer "+accessToken)
	// req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := client.Do(req)
	if err != nil {
		log.ErrorL(labels, "UploadMedia %s", err)
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.ErrorL(labels, "UploadMedia %s", err)
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("unexpected status code: %d, response: %s", resp.StatusCode, string(body))
		return "", fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var response struct {
		MediaIDString string `json:"media_id_string"`
	}
	err = json.NewDecoder(resp.Body).Decode(&response)
	if err != nil {
		log.ErrorL(labels, "UploadMedia %s", err)
		return "", err
	}
	log.EndTimeL(labels, "UploadMedia", startTime, nil)
	span.SetStatus(codes.Ok, "UploadMedia")
	return response.MediaIDString, nil
}

// PostTweet posts a tweet with the specified text and media ID
func (t *twitterQuery) PostTweet(ctx context.Context, client *http.Client, text string, mediaID string) (*datastruct.TwitterResponse, error) {
	span, labels := common.GenerateSpan("UploadMedia", ctx)
	defer span.End()

	span.AddEvent(fmt.Sprintf("Starting %s", "UploadMedia"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "UploadMedia"))
	var res datastruct.TweetRequest
	res.Text = text
	m := datastruct.TweetMedia{MediaIds: []string{mediaID}}
	res.Media = m
	resMedia, err := json.Marshal(res)
	if err != nil {
		log.ErrorL(labels, "UploadMedia %s", err)
		return nil, err
	}

	req, err := http.NewRequest("POST", datastruct.TweetURL, strings.NewReader(string(resMedia)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		fmt.Printf("unexpected status code: %d", resp.StatusCode)
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("unexpected status code: %d, response: %s", resp.StatusCode, string(body))
		return nil, err
	}
	body, _ := io.ReadAll(resp.Body)
	var response datastruct.TwitterResponse
	json.Unmarshal(body, &response)
	log.EndTimeL(labels, "UploadMedia", startTime, nil)
	span.SetStatus(codes.Ok, "UploadMedia")
	return &response, nil
}
