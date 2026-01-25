// Package all imports all available decorators for side-effect registration.
// Import this package to enable all decorators:
//
//	import _ "github.com/fujin-io/fujin/public/plugins/decorator/all"
package all

import (
	// metrics decorator - provides Prometheus metrics
	_ "github.com/fujin-io/fujin/public/plugins/decorator/metrics"
	// tracing decorator - provides OpenTelemetry distributed tracing
	_ "github.com/fujin-io/fujin/public/plugins/decorator/tracing"
)
