package all

import (
	_ "github.com/fujin-io/fujin/public/connectors/impl/amqp091"
	_ "github.com/fujin-io/fujin/public/connectors/impl/amqp10"

	// _ "github.com/fujin-io/fujin/public/connectors/impl/kafka"
	_ "github.com/fujin-io/fujin/public/connectors/impl/mqtt"
	_ "github.com/fujin-io/fujin/public/connectors/impl/nats/core"
	_ "github.com/fujin-io/fujin/public/connectors/impl/nsq"
	_ "github.com/fujin-io/fujin/public/connectors/impl/resp/pubsub"
	_ "github.com/fujin-io/fujin/public/connectors/impl/resp/streams"
	_ "github.com/fujin-io/fujin/public/connectors/v2/impl/kafka"
)
