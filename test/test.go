package test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/fujin-io/fujin/internal/api/fujin/v1/proto/request"
	"github.com/fujin-io/fujin/internal/api/fujin/v1/proto/response"
	"github.com/fujin-io/fujin/internal/api/fujin/v1/version"
	"github.com/fujin-io/fujin/public/plugins/connector/amqp091"
	"github.com/fujin-io/fujin/public/plugins/connector/amqp10"
	connector_config "github.com/fujin-io/fujin/public/plugins/connector/config"
	"github.com/fujin-io/fujin/public/plugins/connector/kafka"
	"github.com/fujin-io/fujin/public/plugins/connector/mqtt"
	"github.com/fujin-io/fujin/public/plugins/connector/nats/core"
	"github.com/fujin-io/fujin/public/plugins/connector/nsq"
	respconfig "github.com/fujin-io/fujin/public/plugins/connector/resp/config"
	"github.com/fujin-io/fujin/public/plugins/connector/resp/pubsub"
	"github.com/fujin-io/fujin/public/plugins/connector/resp/streams"
	v1 "github.com/fujin-io/fujin/public/proto/fujin/v1"
	"github.com/fujin-io/fujin/public/server"
	"github.com/fujin-io/fujin/public/server/config"
	"github.com/quic-go/quic-go"
)

const (
	defaultSendBufSize = 32768
	defaultRecvBufSize = 32768
)

var DefaultFujinServerTestConfig = config.FujinServerConfig{
	Enabled: true,
	Addr:    ":4848",
	TLS:     generateTLSConfig(),
}

var DefaultTestConfigWithKafka3Brokers = config.Config{
	Fujin: DefaultFujinServerTestConfig,
	Connectors: connector_config.ConnectorsConfig{
		"connector": {
			Protocol: "kafka",
			Settings: kafka.Config{
				Common: kafka.CommonSettings{
					Brokers: []string{"localhost:9092", "localhost:9093", "localhost:9094"},
				},
				Clients: map[string]kafka.ClientSpecificSettings{
					"sub": {
						ConsumeTopics:          []string{"my_pub_topic"},
						Group:                  "fujin",
						AllowAutoTopicCreation: true,
					},
					"pub": {
						ProduceTopic:           "my_pub_topic",
						AllowAutoTopicCreation: true,
						Linger:                 10 * time.Millisecond,
					},
				},
			},
		},
	},
}

var DefaultTestConfigWithNats = config.Config{
	Fujin: DefaultFujinServerTestConfig,
	Connectors: connector_config.ConnectorsConfig{
		"connector": {
			Protocol: "nats_core",
			Settings: core.Config{
				Common: core.CommonSettings{
					URL: "nats://localhost:4222",
				},
				Clients: map[string]core.ClientSpecificSettings{
					"pub": {
						Subject: "my_subject",
					},
					"sub": {
						Subject: "my_subject",
					},
				},
			},
		},
	},
}

var DefaultTestConfigWithAMQP091 = config.Config{
	Fujin: DefaultFujinServerTestConfig,
	Connectors: connector_config.ConnectorsConfig{
		"connector": {
			Protocol: "amqp091",
			Settings: amqp091.Config{
				Clients: map[string]amqp091.ClientSpecificSettings{
					"pub": {
						Conn: amqp091.ConnConfig{
							URL: "amqp://guest:guest@localhost",
						},
						Exchange: amqp091.ExchangeConfig{
							Name: "events",
							Kind: "fanout",
						},
						Queue: amqp091.QueueConfig{
							Name: "my_queue",
						},
						QueueBind: amqp091.QueueBindConfig{
							RoutingKey: "my_routing_key",
						},
						Publish: &amqp091.PublishConfig{
							ContentType: "text/plain",
						},
					},
					"sub": {
						Conn: amqp091.ConnConfig{
							URL: "amqp://guest:guest@localhost",
						},
						Exchange: amqp091.ExchangeConfig{
							Name: "events",
							Kind: "fanout",
						},
						Queue: amqp091.QueueConfig{
							Name: "my_queue",
						},
						QueueBind: amqp091.QueueBindConfig{
							RoutingKey: "my_routing_key",
						},
						Consume: &amqp091.ConsumeConfig{
							Consumer: "fujin",
						},
					},
				},
			},
		},
	},
}

var DefaultTestConfigWithAMQP10 = config.Config{
	Fujin: DefaultFujinServerTestConfig,
	Connectors: connector_config.ConnectorsConfig{
		"connector": {
			Protocol: "amqp10",
			Settings: amqp10.Config{
				Clients: map[string]amqp10.ClientSpecificSettings{
					"pub": {
						Conn: amqp10.ConnConfig{
							Addr: "amqp://artemis:artemis@localhost:61616",
						},
						Sender: &amqp10.SenderConfig{
							Target: "queue",
						},
					},
					"sub": {
						Conn: amqp10.ConnConfig{
							Addr: "amqp://artemis:artemis@localhost:61616",
						},
						Receiver: &amqp10.ReceiverConfig{
							Source: "queue",
						},
					},
				},
			},
		},
	},
}
var DefaultTestConfigWithRedisPubSub = config.Config{
	Fujin: DefaultFujinServerTestConfig,
	Connectors: connector_config.ConnectorsConfig{
		"connector": {
			Protocol: "resp_pubsub",
			Settings: pubsub.Config{
				Common: pubsub.CommonSettings{
					RedisConfig: respconfig.RedisConfig{
						InitAddress:  []string{"localhost:6379"},
						DisableCache: true,
					},
					WriterBatchConfig: respconfig.WriterBatchConfig{
						BatchSize: 1000,
						Linger:    5 * time.Millisecond,
					},
				},
				Clients: map[string]pubsub.ClientSpecificSettings{
					"pub": {
						Channel: "channel",
					},
					"sub": {
						Channels: []string{"channel"},
					},
				},
			},
		},
	},
}

var DefaultTestConfigWithRedisStreams = config.Config{
	Fujin: DefaultFujinServerTestConfig,
	Connectors: connector_config.ConnectorsConfig{
		"connector": connector_config.ConnectorConfig{
			Protocol: "resp_streams",
			Settings: streams.Config{
				Common: streams.CommonSettings{
					RedisConfig: respconfig.RedisConfig{
						InitAddress:  []string{"localhost:6379"},
						DisableCache: true,
					},
					WriterBatchConfig: respconfig.WriterBatchConfig{
						BatchSize: 1000,
						Linger:    5 * time.Millisecond,
					},
				},
				Clients: map[string]streams.ClientSpecificSettings{
					"pub": {
						Stream: "stream",
					},
					"sub": {
						Streams: map[string]streams.StreamConf{
							"stream": {
								StartID:       ">",
								GroupCreateID: "$",
							},
						},
						Group: streams.GroupConf{
							Name:     "fujin",
							Consumer: "fujin",
						},
					},
				},
			},
		},
	},
}

var DefaultTestConfigWithMQTT = config.Config{
	Fujin: DefaultFujinServerTestConfig,
	Connectors: connector_config.ConnectorsConfig{
		"connector": {
			Protocol: "mqtt",
			Settings: mqtt.Config{
				Common: mqtt.CommonSettings{
					BrokerURL:         "tcp://localhost:1883",
					KeepAlive:         2,
					DisconnectTimeout: 5 * time.Second,
					ConnectTimeout:    300 * time.Second,
				},
				Clients: map[string]mqtt.ClientSpecificSettings{
					"pub": {
						ClientID:      "fujin_pub",
						Topic:         "my_topic",
						QoS:           0,
						Retain:        true,
						CleanStart:    true,
						SessionExpiry: 5,
						Pool: mqtt.PoolConfig{
							Size:           1000000,
							PreAlloc:       true,
							ReleaseTimeout: 30 * time.Second,
						},
					},
					"sub": {
						ClientID:         "fujin_sub",
						Topic:            "my_topic",
						QoS:              0,
						Retain:           false,
						CleanStart:       true,
						SessionExpiry:    0,
						SendAcksInterval: 50 * time.Millisecond,
						AckTTL:           5 * time.Minute,
					},
				},
			},
		},
	},
}

var DefaultTestConfigWithNSQ = config.Config{
	Fujin: DefaultFujinServerTestConfig,
	Connectors: connector_config.ConnectorsConfig{
		"connector": {
			Protocol: "nsq",
			Settings: nsq.Config{
				Common: nsq.CommonSettings{
					Address:   "localhost:4150",
					Addresses: []string{"localhost:4150"},
				},
				Clients: map[string]nsq.ClientSpecificSettings{
					"pub": {
						Topic: "my_topic",
						Pool: nsq.PoolConfig{
							Size:           1000000,
							PreAlloc:       true,
							ReleaseTimeout: 5 * time.Second,
						},
					},
					"sub": {
						Topic:       "my_topic",
						Channel:     "my_channel",
						MaxInFlight: 1000000,
					},
				},
			},
		},
	},
}

func RunDefaultServerWithKafka3Brokers(ctx context.Context) *server.Server {
	return RunServer(ctx, DefaultTestConfigWithKafka3Brokers)
}

func RunDefaultServerWithNats(ctx context.Context) *server.Server {
	return RunServer(ctx, DefaultTestConfigWithNats)
}

func RunDefaultServerWithAMQP091(ctx context.Context) *server.Server {
	return RunServer(ctx, DefaultTestConfigWithAMQP091)
}

func RunDefaultServerWithAMQP10(ctx context.Context) *server.Server {
	return RunServer(ctx, DefaultTestConfigWithAMQP10)
}

func RunDefaultServerWithRedisPubSub(ctx context.Context) *server.Server {
	return RunServer(ctx, DefaultTestConfigWithRedisPubSub)
}

func RunDefaultServerWithRedisStreams(ctx context.Context) *server.Server {
	return RunServer(ctx, DefaultTestConfigWithRedisStreams)
}

func RunDefaultServerWithMQTT(ctx context.Context) *server.Server {
	return RunServer(ctx, DefaultTestConfigWithMQTT)
}

func RunDefaultServerWithNSQ(ctx context.Context) *server.Server {
	return RunServer(ctx, DefaultTestConfigWithNSQ)
}

func RunServer(ctx context.Context, conf config.Config) *server.Server {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelDebug,
	}))

	s, _ := server.NewServer(conf, logger)

	go func() {
		if err := s.ListenAndServe(ctx); err != nil {
			panic(fmt.Errorf("Unable to start fujin server: %w", err))
		}
	}()

	if !s.ReadyForConnections(10 * time.Second) {
		panic("Unable to start fujin server: timeout")
	}

	logger.Info("Server started")

	return s
}

func createClientConn(ctx context.Context, addr string) *quic.Conn {
	conn, err := quic.DialAddr(ctx, addr, generateTLSConfig(), nil)
	if err != nil {
		panic(fmt.Errorf("dial addr: %w", err))
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				str, err := conn.AcceptStream(ctx)
				if err != nil {
					return
				}

				handlePing(str)
			}
		}
	}()

	return conn
}

func doDefaultBind(conn *quic.Conn) *quic.Stream {
	req := bindCmd("connector", nil, nil)

	str, err := conn.OpenStream()
	if err != nil {
		panic(fmt.Errorf("open stream: %w", err))
	}

	if _, err := str.Write(req); err != nil {
		panic(fmt.Errorf("write bind request: %w", err))
	}

	return str
}

func drainStream(b *testing.B, str *quic.Stream, ch chan int) {
	buf := make([]byte, defaultRecvBufSize)
	bytes := 0

	for {
		str.SetReadDeadline(time.Now().Add(30 * time.Second))
		n, err := str.Read(buf)
		bytes += n
		if err != nil {
			if !errors.Is(err, io.EOF) {
				b.Errorf("Error on read: %v\n", err)
			}
			break
		}
	}

	ch <- bytes
}

func handlePing(str *quic.Stream) {
	defer str.Close()
	var pingBuf [1]byte

	n, err := str.Read(pingBuf[:])
	if err == io.EOF {
		if n != 0 {
			if pingBuf[0] != byte(request.OP_CODE_PING) {
				return
			}
		}
		pingBuf[0] = byte(response.RESP_CODE_PONG)
		if _, err := str.Write(pingBuf[:]); err != nil {
			return
		}
		return
	}
	if err != nil {
		return
	}
}

func generateTLSConfig() *tls.Config {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	template := x509.Certificate{SerialNumber: big.NewInt(1)}
	cert, _ := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	tlsCert := tls.Certificate{
		Certificate: [][]byte{cert},
		PrivateKey:  key,
	}
	return &tls.Config{Certificates: []tls.Certificate{tlsCert}, InsecureSkipVerify: true, NextProtos: []string{version.Fujin1}}
}

func bindCmd(connector string, meta, configOverrides map[string]string) []byte {
	var buf []byte
	buf = append(buf, byte(v1.OP_CODE_BIND))
	buf = appendFujinString(buf, connector)
	buf = appendFujinUint16StringArray(buf, meta)
	buf = appendFujinUint16StringArray(buf, configOverrides)

	return buf
}

func appendFujinString(buf []byte, v string) []byte {
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(v)))
	buf = append(buf, v...)
	return buf
}

func appendFujinUint16StringArray(buf []byte, m map[string]string) []byte {
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(m)))
	for k, v := range m {
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(k)))
		buf = append(buf, k...)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(v)))
		buf = append(buf, v...)
	}
	return buf
}
