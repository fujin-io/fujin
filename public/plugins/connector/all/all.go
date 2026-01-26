// Package all imports all available connector plugins for side-effect registration.
// Import this package to enable all connectors:
//
//	import _ "github.com/fujin-io/fujin/public/plugins/connector/all"
package all

import (
	// AMQP10 connector plugin
	_ "github.com/fujin-io/fujin/public/plugins/connector/amqp10"
	// AMQP091 connector plugin
	_ "github.com/fujin-io/fujin/public/plugins/connector/amqp091"
	// Kafka connector plugin
	_ "github.com/fujin-io/fujin/public/plugins/connector/kafka"
	// MQTT connector plugin
	_ "github.com/fujin-io/fujin/public/plugins/connector/mqtt"
	// NATS Core connector plugin
	_ "github.com/fujin-io/fujin/public/plugins/connector/nats/core"
	// NSQ connector plugin
	_ "github.com/fujin-io/fujin/public/plugins/connector/nsq"
	// Redis PubSub connector plugin
	_ "github.com/fujin-io/fujin/public/plugins/connector/resp/pubsub"
	// Redis Streams connector plugin
	_ "github.com/fujin-io/fujin/public/plugins/connector/resp/streams"
)

