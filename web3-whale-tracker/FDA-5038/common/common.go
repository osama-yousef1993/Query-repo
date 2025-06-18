package common

import (
	"context"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
)

func GenerateSpan(functionName string, ctx context.Context) (trace.Span, map[string]string) {
	labels := make(map[string]string)
	span := trace.SpanFromContext(ctx)
	labels["function"] = functionName
	labels["UUID"] = uuid.New().String()
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	return span, labels
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
