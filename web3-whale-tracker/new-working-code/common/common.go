package common

import (
	"context"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
)

// GenerateSpan
// Takes (functionName string, ctx context.Context)
// Returns (trace.Span, map[string]string)
//
// GenerateSpan Takes function name and context build the labels object and the span tracer to use it in each function.
// returns the span and the labels.
func GenerateSpan(functionName string, ctx context.Context) (trace.Span, map[string]string) {
	labels := make(map[string]string)
	span := trace.SpanFromContext(ctx)
	labels["function"] = functionName
	labels["UUID"] = uuid.New().String()
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	return span, labels
}

// SetResponseHeaders
// Takes (w http.ResponseWriter, cacheTime int)
// This ill build the cache header max age and the type of the response.
func SetResponseHeaders(w http.ResponseWriter, cacheTime int) {
	w.Header().Set("Content-Type", "application/json")
	cacheValue := fmt.Sprintf("max-age=%v, public", cacheTime)
	if cacheTime > 0 {
		w.Header().Set("Cache-Control", cacheValue)
	} else {
		w.Header().Set("Cache-Control", "max-age=-1, no-cache, no-store, must-revalidate, public")
	}
}
