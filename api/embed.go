// Package api embeds the OpenAPI contract so the running service serves the
// exact document it was built with at GET /openapi.yaml.
package api

import _ "embed"

// OpenAPI is the OpenAPI 3.1 specification.
//
//go:embed openapi.yaml
var OpenAPI []byte
