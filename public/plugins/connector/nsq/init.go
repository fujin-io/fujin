package nsq

import (
	"fmt"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

func init() {
	if err := connector.Register("nsq", NewNSQConnector); err != nil {
		panic(fmt.Sprintf("failed to register nsq connector: %v", err))
	}
}

