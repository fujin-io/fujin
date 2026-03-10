package amqp1

import (
	"fmt"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

func init() {
	if err := connector.Register("azure_amqp1", newAzureAMQP1Connector); err != nil {
		panic(fmt.Sprintf("register azure_amqp1 connector: %v", err))
	}
}
