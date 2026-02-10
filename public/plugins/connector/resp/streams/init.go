package streams

import (
	"fmt"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

func init() {
	if err := connector.Register("resp_streams", newRESPStreamsConnector); err != nil {
		panic(fmt.Sprintf("register resp_streams connector: %v", err))
	}
}
