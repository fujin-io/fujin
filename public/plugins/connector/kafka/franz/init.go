package franz

import (
	"fmt"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

func init() {
	if err := connector.Register("kafka_franz", newKafkaFranzConnector); err != nil {
		panic(fmt.Sprintf("register kafka_franz connector: %v", err))
	}
}
