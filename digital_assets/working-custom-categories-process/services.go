package services

import "go.opentelemetry.io/otel"

// Functionality used acrross all services packages

var (
	tracer = otel.Tracer("github.com/Forbes-Media/forbes-digital-assets/repository/services")
)
