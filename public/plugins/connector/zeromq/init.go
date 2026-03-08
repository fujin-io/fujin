package zeromq

import (
	"fmt"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

// Register the ZeroMQ connector under protocol name "zeromq".
func init() {
	if err := connector.Register("zeromq", newZeroMQConnector); err != nil {
		panic(fmt.Sprintf("register zeromq connector: %v", err))
	}
}

