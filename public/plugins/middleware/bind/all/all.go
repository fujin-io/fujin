// Package all imports all available bind middlewares for side-effect registration.
// Import this package to enable all bind middlewares:
//
//	import _ "github.com/fujin-io/fujin/public/plugins/middleware/bind/all"
package all

import (
	// auth_api_key bind middleware - provides API key authentication
	_ "github.com/fujin-io/fujin/public/plugins/middleware/bind/auth_api_key"
)

