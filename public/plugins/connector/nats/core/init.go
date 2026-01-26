package core

import (
	"fmt"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

func init() {
	if err := connector.Register("nats_core", NewNATSConnector); err != nil {
		panic(fmt.Sprintf("failed to register nats_core connector: %v", err))
	}
}

