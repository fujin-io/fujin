package amqp10

import (
	"fmt"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

func init() {
	if err := connector.Register("amqp10", NewAMQP10Connector); err != nil {
		panic(fmt.Sprintf("failed to register amqp10 connector: %v", err))
	}
}

