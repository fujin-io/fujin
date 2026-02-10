package amqp10

import (
	"fmt"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

func init() {
	if err := connector.Register("amqp10", newAMQP10Connector); err != nil {
		panic(fmt.Sprintf("register amqp10 connector: %v", err))
	}
}
