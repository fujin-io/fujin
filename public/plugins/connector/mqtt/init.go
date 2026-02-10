package mqtt

import (
	"fmt"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

func init() {
	if err := connector.Register("mqtt", newMQTTConnector); err != nil {
		panic(fmt.Sprintf("register mqtt connector: %v", err))
	}
}
