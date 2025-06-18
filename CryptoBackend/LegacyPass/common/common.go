package common

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"os"

	"github.com/Forbes-Media/go-tools/log"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

//JSONRequestBodyToObject Reads the body of a http.request and atteptes to parse it into an object of the requested return type.
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

// Generates a span  an laeble mapping use in logging
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

// Sanitize String removes html symbols from unput
func SanitizeString(data string) string {
	sanitized := template.HTMLEscapeString(data)
	return sanitized
}

// SetResponseHeaders
// Takes (w http.ResponseWriter, cacheTime int)
// this function ResponseWriter and cacheTime
// so we can add the cache time and the type that we need the response to be.
func SetResponseHeaders(w http.ResponseWriter, cacheTime int) {
	w.Header().Set("Content-Type", "application/json")
	cacheValue := fmt.Sprintf("max-age=%v, public", cacheTime)
	w.Header().Set("Cache-Control", cacheValue)
}

// GetTableName
// takes table name
// returns Table name with DATA_NAMESPACE so we can access the table in different environment
func GetTableName(tableName string) string {

	if os.Getenv("DATA_NAMESPACE") == "_dev" {
		return fmt.Sprintf("%s%s", tableName, os.Getenv("DATA_NAMESPACE"))
	}

	return tableName
}
