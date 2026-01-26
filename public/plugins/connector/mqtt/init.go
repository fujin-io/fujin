package mqtt

import (
	"fmt"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

func init() {
	if err := connector.Register("mqtt", NewMQTTConnector); err != nil {
		panic(fmt.Sprintf("failed to register mqtt connector: %v", err))
	}
}

