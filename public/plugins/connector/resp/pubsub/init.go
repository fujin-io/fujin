package pubsub

import (
	"fmt"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

func init() {
	if err := connector.Register("resp_pubsub", newRESPPubSubConnector); err != nil {
		panic(fmt.Sprintf("register resp_pubsub connector: %v", err))
	}
}
