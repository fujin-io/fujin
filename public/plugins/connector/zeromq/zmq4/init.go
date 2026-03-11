package zmq4

import (
	"fmt"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

func init() {
	if err := connector.Register("zeromq_zmq4", newZmqConnector); err != nil {
		panic(fmt.Sprintf("register zeromq_zmq4 connector: %v", err))
	}
}
