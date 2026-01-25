package kafka

import (
	"fmt"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

func init() {
	if err := connector.Register("kafka", NewKafkaConnector); err != nil {
		panic(fmt.Sprintf("failed to register kafka connector: %v", err))
	}
}
