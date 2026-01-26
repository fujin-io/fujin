// Package all imports all available config loader plugins for side-effect registration.
// Import this package to enable all config loaders:
//
//	import _ "github.com/fujin-io/fujin/public/plugins/configloader/all"
package all

import (
	// File config loader plugin
	_ "github.com/fujin-io/fujin/public/plugins/configloader/file"
)

