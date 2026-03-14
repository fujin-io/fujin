// Package all imports all available connector middlewares for side-effect registration.
// Import this package to enable all connector middlewares:
//
//	import _ "github.com/fujin-io/fujin/public/plugins/middleware/connector/all"
package all

import (
	// prom connector middleware - provides Prometheus metrics
	_ "github.com/fujin-io/fujin/public/plugins/middleware/connector/prom"
	// otel connector middleware - provides OpenTelemetry distributed tracing
	_ "github.com/fujin-io/fujin/public/plugins/middleware/connector/otel"
)
