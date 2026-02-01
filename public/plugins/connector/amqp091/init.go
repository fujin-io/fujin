package amqp091

import (
	"fmt"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

func init() {
	if err := connector.Register("amqp091", NewAMQP091Connector); err != nil {
		panic(fmt.Sprintf("register amqp091 connector: %v", err))
	}
}
