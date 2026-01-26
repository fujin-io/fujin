package file

import (
	"fmt"

	"github.com/fujin-io/fujin/public/plugins/configloader"
)

func init() {
	if err := configloader.Register("file", NewFileLoader); err != nil {
		panic(fmt.Sprintf("failed to register file config loader: %v", err))
	}
}

