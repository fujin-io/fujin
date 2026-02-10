package kafka

import (
	"fmt"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

func init() {
	if err := connector.Register("kafka", newKafkaConnector); err != nil {
		panic(fmt.Sprintf("register kafka connector: %v", err))
	}
}
