package common

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/go-tools/log"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

//JSONRequestBodyToObject Reads the body of a http.request and attempts to parse it into an object og the requested return type.
//Takes a any type as a generic
//Takes a context.Context and a  *http.Request
//Returns an object of the generic type, and an error object

// This function reads the body of a request object and tries to parse the body to an object of the requested type
func JSONRequestBodyToObject[T interface{}](ctx context.Context, r *http.Request) (*T, error) {

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	var requestBody T
	if err := json.Unmarshal(bodyBytes, &requestBody); err != nil {
		log.Error("%s", err)
		return nil, err
	}

	return &requestBody, nil
}

// Generates a span  an label mapping use in logging
// Takes a  string and a context.Context
// returns a a trace.span object and a map[string]strings
//
// This function should ideally be used to pass in the name of the function calling it.
// It then generates labels that are used by open telemetry to track  the workflow of a process. In this case
// the lifespan of an http request. This als return a span object that is used to create open telemetry logs
func GenerateSpan(functionName string, ctx context.Context) (trace.Span, map[string]string) {
	labels := make(map[string]string)
	span := trace.SpanFromContext(ctx)

	labels["function"] = functionName
	labels["UUID"] = uuid.New().String()
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))
	return span, labels
}

// StringArrayContains. Looks through an array and sees if it contains the desired value.
//
// Takes string array, and takes a string
// Iterates through array to see if it contains values.
// If it finds match return true else returns false
func StringArrayContains(array []string, value string) bool {

	for _, v := range array {
		if v == value {
			return true
		}
	}
	return false
}

// encodes a string to base 64
func EncodeStringToBase64(str string) string {
	data := []byte(str)
	encoded := base64.StdEncoding.EncodeToString(data)
	return encoded
}

// decodes a base64string
func DecodeBase64ToString(str string) (string, error) {
	decoded, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return "", err
	}
	return string(decoded), nil
}

func SetResponseHeaders(w http.ResponseWriter, cacheTime int) {
	w.Header().Set("Content-Type", "application/json")
	cacheValue := fmt.Sprintf("max-age=%v, public", cacheTime)
	if cacheTime > 0 {
		w.Header().Set("Cache-Control", cacheValue)
	} else {
		w.Header().Set("Cache-Control", "max-age=-1, no-cache, no-store, must-revalidate, public")
	}
}

// GetArticlesFromDysonSphere Get Articles from DysonSphere API using keyword
// Takes a (ctx context.Context, keyword string)
// returns ([]datastruct.ArticleResponse, error)
//
// This function should ideally be used to pass the keyword we need to fetch articles to from DysonSphere API.
// It then returns the articles data that related to this keyword.
func GetArticlesFromDysonSphere(ctx context.Context, keyword string) ([]datastruct.ArticleResponse, error) {
	span, labels := GenerateSpan("GetArticlesFromDysonSphere", ctx)
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 GetArticlesFromDysonSphere"))
	span.AddEvent("Start Getting Articles from DysonSphere API")
	url := fmt.Sprintf("https://recommend-dev.forbes.com/channel115?keywords=%s", keyword)
	res, err := http.Get(url)
	if err != nil {
		fmt.Print(err.Error())
		os.Exit(1)
	}
	responseData, err := io.ReadAll(res.Body)
	if err != nil {
		fmt.Print(err.Error())
	}

	var response []datastruct.ArticleResponse
	json.Unmarshal(responseData, &response)
	log.EndTimeL(labels, "V2 GetArticlesFromDysonSphere", startTime, nil)
	span.SetStatus(codes.Ok, "V2 GetArticlesFromDysonSphere")
	return response, nil
}
