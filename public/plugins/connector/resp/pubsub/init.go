package pubsub

import (
	"fmt"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

func init() {
	if err := connector.Register("resp_pubsub", NewPubSubConnector); err != nil {
		panic(fmt.Sprintf("failed to register resp_pubsub connector: %v", err))
	}
}

