package streams

import (
	"fmt"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

func init() {
	if err := connector.Register("resp_streams", NewStreamsConnector); err != nil {
		panic(fmt.Sprintf("failed to register resp_streams connector: %v", err))
	}
}
