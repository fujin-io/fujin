package test

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"
	"time"

	v1 "github.com/fujin-io/fujin/public/proto/fujin/v1"
	"github.com/fujin-io/fujin/public/server"
)

const (
	PERF_ADDR = "localhost:4848"
)

// No op benchmarks
func Benchmark_Produce_1BPayload_Nop_QUIC(b *testing.B) {
	benchProduceQUIC(b, "nop", "pub", sizedString(1))
}

func Benchmark_Produce_32BPayload_Nop_QUIC(b *testing.B) {
	benchProduceQUIC(b, "nop", "pub", sizedString(32))
}

func Benchmark_Produce_128BPayload_Nop_QUIC(b *testing.B) {
	benchProduceQUIC(b, "nop", "pub", sizedString(128))
}

func Benchmark_Produce_256BPayload_Nop_QUIC(b *testing.B) {
	benchProduceQUIC(b, "nop", "pub", sizedString(256))
}

func Benchmark_Produce_1KBPayload_Nop_QUIC(b *testing.B) {
	benchProduceQUIC(b, "nop", "pub", sizedString(1024))
}

func Benchmark_Produce_4KBPayload_Nop_QUIC(b *testing.B) {
	benchProduceQUIC(b, "nop", "pub", sizedString(4*1024))
}

func Benchmark_Produce_8KBPayload_Nop_QUIC(b *testing.B) {
	benchProduceQUIC(b, "nop", "pub", sizedString(8*1024))
}

func Benchmark_Produce_32KBPayload_Nop_QUIC(b *testing.B) {
	benchProduceQUIC(b, "nop", "pub", sizedString(32*1024))
}

// No op TCP benchmarks
func Benchmark_Produce_1BPayload_Nop_TCP(b *testing.B) {
	benchProduceTCP(b, "nop", "pub", sizedString(1))
}

func Benchmark_Produce_32BPayload_Nop_TCP(b *testing.B) {
	benchProduceTCP(b, "nop", "pub", sizedString(32))
}

func Benchmark_Produce_128BPayload_Nop_TCP(b *testing.B) {
	benchProduceTCP(b, "nop", "pub", sizedString(128))
}

func Benchmark_Produce_256BPayload_Nop_TCP(b *testing.B) {
	benchProduceTCP(b, "nop", "pub", sizedString(256))
}

func Benchmark_Produce_1KBPayload_Nop_TCP(b *testing.B) {
	benchProduceTCP(b, "nop", "pub", sizedString(1024))
}

func Benchmark_Produce_4KBPayload_Nop_TCP(b *testing.B) {
	benchProduceTCP(b, "nop", "pub", sizedString(4*1024))
}

func Benchmark_Produce_8KBPayload_Nop_TCP(b *testing.B) {
	benchProduceTCP(b, "nop", "pub", sizedString(8*1024))
}

func Benchmark_Produce_32KBPayload_Nop_TCP(b *testing.B) {
	benchProduceTCP(b, "nop", "pub", sizedString(32*1024))
}

// No op Unix benchmarks
func Benchmark_Produce_1BPayload_Nop_Unix(b *testing.B) {
	benchProduceUnix(b, "nop", "pub", sizedString(1))
}

func Benchmark_Produce_32BPayload_Nop_Unix(b *testing.B) {
	benchProduceUnix(b, "nop", "pub", sizedString(32))
}

func Benchmark_Produce_128BPayload_Nop_Unix(b *testing.B) {
	benchProduceUnix(b, "nop", "pub", sizedString(128))
}

func Benchmark_Produce_256BPayload_Nop_Unix(b *testing.B) {
	benchProduceUnix(b, "nop", "pub", sizedString(256))
}

func Benchmark_Produce_1KBPayload_Nop_Unix(b *testing.B) {
	benchProduceUnix(b, "nop", "pub", sizedString(1024))
}

func Benchmark_Produce_4KBPayload_Nop_Unix(b *testing.B) {
	benchProduceUnix(b, "nop", "pub", sizedString(4*1024))
}

func Benchmark_Produce_8KBPayload_Nop_Unix(b *testing.B) {
	benchProduceUnix(b, "nop", "pub", sizedString(8*1024))
}

func Benchmark_Produce_32KBPayload_Nop_Unix(b *testing.B) {
	benchProduceUnix(b, "nop", "pub", sizedString(32*1024))
}

// Kafka benchmarks
func Benchmark_Produce_1BPayload_Kafka_3Brokers_QUIC(b *testing.B) {
	benchProduceQUIC(b, "kafka3", "pub", sizedString(1))
}

func Benchmark_Produce_32BPayload_Kafka_3Brokers_QUIC(b *testing.B) {
	benchProduceQUIC(b, "kafka3", "pub", sizedString(32))
}

func Benchmark_Produce_128BPayload_Kafka_3Brokers_QUIC(b *testing.B) {
	benchProduceQUIC(b, "kafka3", "pub", sizedString(128))
}

func Benchmark_Produce_256BPayload_Kafka_3Brokers_QUIC(b *testing.B) {
	benchProduceQUIC(b, "kafka3", "pub", sizedString(256))
}

func Benchmark_Produce_1KBPayload_Kafka_3Brokers_QUIC(b *testing.B) {
	benchProduceQUIC(b, "kafka3", "pub", sizedString(1024))
}

func Benchmark_Produce_4KBPayload_Kafka_3Brokers_QUIC(b *testing.B) {
	benchProduceQUIC(b, "kafka3", "pub", sizedString(4*1024))
}

func Benchmark_Produce_8KBPayload_Kafka_3Brokers_QUIC(b *testing.B) {
	benchProduceQUIC(b, "kafka3", "pub", sizedString(8*1024))
}

func Benchmark_Produce_32KBPayload_Kafka_3Brokers_QUIC(b *testing.B) {
	benchProduceQUIC(b, "kafka3", "pub", sizedString(32*1024))
}

func Benchmark_Produce_1BPayload_Kafka_3Brokers_TCP(b *testing.B) {
	benchProduceTCP(b, "kafka3", "pub", sizedString(1))
}

func Benchmark_Produce_32BPayload_Kafka_3Brokers_TCP(b *testing.B) {
	benchProduceTCP(b, "kafka3", "pub", sizedString(32))
}

func Benchmark_Produce_128BPayload_Kafka_3Brokers_TCP(b *testing.B) {
	benchProduceTCP(b, "kafka3", "pub", sizedString(128))
}

func Benchmark_Produce_256BPayload_Kafka_3Brokers_TCP(b *testing.B) {
	benchProduceTCP(b, "kafka3", "pub", sizedString(256))
}

func Benchmark_Produce_1KBPayload_Kafka_3Brokers_TCP(b *testing.B) {
	benchProduceTCP(b, "kafka3", "pub", sizedString(1024))
}

func Benchmark_Produce_4KBPayload_Kafka_3Brokers_TCP(b *testing.B) {
	benchProduceTCP(b, "kafka3", "pub", sizedString(4*1024))
}

func Benchmark_Produce_8KBPayload_Kafka_3Brokers_TCP(b *testing.B) {
	benchProduceTCP(b, "kafka3", "pub", sizedString(8*1024))
}

func Benchmark_Produce_32KBPayload_Kafka_3Brokers_TCP(b *testing.B) {
	benchProduceTCP(b, "kafka3", "pub", sizedString(32*1024))
}

func Benchmark_Produce_1BPayload_Kafka_3Brokers_Unix(b *testing.B) {
	benchProduceUnix(b, "kafka3", "pub", sizedString(1))
}

func Benchmark_Produce_32BPayload_Kafka_3Brokers_Unix(b *testing.B) {
	benchProduceUnix(b, "kafka3", "pub", sizedString(32))
}

func Benchmark_Produce_128BPayload_Kafka_3Brokers_Unix(b *testing.B) {
	benchProduceUnix(b, "kafka3", "pub", sizedString(128))
}

func Benchmark_Produce_256BPayload_Kafka_3Brokers_Unix(b *testing.B) {
	benchProduceUnix(b, "kafka3", "pub", sizedString(256))
}

func Benchmark_Produce_1KBPayload_Kafka_3Brokers_Unix(b *testing.B) {
	benchProduceUnix(b, "kafka3", "pub", sizedString(1024))
}

func Benchmark_Produce_4KBPayload_Kafka_3Brokers_Unix(b *testing.B) {
	benchProduceUnix(b, "kafka3", "pub", sizedString(4*1024))
}

func Benchmark_Produce_8KBPayload_Kafka_3Brokers_Unix(b *testing.B) {
	benchProduceUnix(b, "kafka3", "pub", sizedString(8*1024))
}

func Benchmark_Produce_32KBPayload_Kafka_3Brokers_Unix(b *testing.B) {
	benchProduceUnix(b, "kafka3", "pub", sizedString(32*1024))
}

// Nats benchmarks
func Benchmark_Produce_1BPayload_Nats_QUIC(b *testing.B) {
	benchProduceQUIC(b, "nats", "pub", sizedString(1))
}

func Benchmark_Produce_32BPayload_Nats_QUIC(b *testing.B) {
	benchProduceQUIC(b, "nats", "pub", sizedString(32))
}

func Benchmark_Produce_128BPayload_Nats_QUIC(b *testing.B) {
	benchProduceQUIC(b, "nats", "pub", sizedString(128))
}

func Benchmark_Produce_256BPayload_Nats_QUIC(b *testing.B) {
	benchProduceQUIC(b, "nats", "pub", sizedString(256))
}

func Benchmark_Produce_1KBPayload_Nats_QUIC(b *testing.B) {
	benchProduceQUIC(b, "nats", "pub", sizedString(1024))
}

func Benchmark_Produce_4KBPayload_Nats_QUIC(b *testing.B) {
	benchProduceQUIC(b, "nats", "pub", sizedString(4*1024))
}

func Benchmark_Produce_8KBPayload_Nats_QUIC(b *testing.B) {
	benchProduceQUIC(b, "nats", "pub", sizedString(8*1024))
}

func Benchmark_Produce_32KBPayload_Nats_QUIC(b *testing.B) {
	benchProduceQUIC(b, "nats", "pub", sizedString(32*1024))
}

// RabbitMQ benchmarks
func Benchmark_Produce_1BPayload_RabbitMQ_QUIC(b *testing.B) {
	benchProduceQUIC(b, "rabbitmq", "pub", sizedString(1))
}

func Benchmark_Produce_32KBPayload_RabbitMQ_QUIC(b *testing.B) {
	benchProduceQUIC(b, "rabbitmq", "pub", sizedString(32*1024))
}

// ArtemisMQ benchmarks
func Benchmark_Produce_1BPayload_ArtemisMQ_QUIC(b *testing.B) {
	benchProduceQUIC(b, "artemismq", "pub", sizedString(1))
}

func Benchmark_Produce_32KBPayload_ArtemisMQ_QUIC(b *testing.B) {
	benchProduceQUIC(b, "artemismq", "pub", sizedString(32*1024))
}

// Redis Pub/Sub benchmarks
func Benchmark_Produce_1BPayload_RedisPubSub_QUIC(b *testing.B) {
	benchProduceQUIC(b, "resp_pubsub", "pub", sizedString(1))
}

func Benchmark_Produce_32BPayload_RedisPubSub_QUIC(b *testing.B) {
	benchProduceQUIC(b, "resp_pubsub", "pub", sizedString(32))
}

func Benchmark_Produce_128BPayload_RedisPubSub_QUIC(b *testing.B) {
	benchProduceQUIC(b, "resp_pubsub", "pub", sizedString(128))
}

func Benchmark_Produce_256BPayload_RedisPubSub_QUIC(b *testing.B) {
	benchProduceQUIC(b, "resp_pubsub", "pub", sizedString(256))
}

func Benchmark_Produce_1KBPayload_RedisPubSub_QUIC(b *testing.B) {
	benchProduceQUIC(b, "resp_pubsub", "pub", sizedString(1024))
}

func Benchmark_Produce_4KBPayload_RedisPubSub_QUIC(b *testing.B) {
	benchProduceQUIC(b, "resp_pubsub", "pub", sizedString(4*1024))
}

func Benchmark_Produce_8KBPayload_RedisPubSub_QUIC(b *testing.B) {
	benchProduceQUIC(b, "resp_pubsub", "pub", sizedString(8*1024))
}

func Benchmark_Produce_32KBPayload_RedisPubSub_QUIC(b *testing.B) {
	benchProduceQUIC(b, "resp_pubsub", "pub", sizedString(32*1024))
}

// Redis Rueidis Streams benchmarks
func Benchmark_Produce_1BPayload_RedisStreams_QUIC(b *testing.B) {
	benchProduceQUIC(b, "redis_rueidis_streams", "pub", sizedString(1))
}

func Benchmark_Produce_32BPayload_RedisStreams_QUIC(b *testing.B) {
	benchProduceQUIC(b, "redis_rueidis_streams", "pub", sizedString(32))
}

func Benchmark_Produce_128BPayload_RedisStreams_QUIC(b *testing.B) {
	benchProduceQUIC(b, "redis_rueidis_streams", "pub", sizedString(128))
}

func Benchmark_Produce_256BPayload_RedisStreams_QUIC(b *testing.B) {
	benchProduceQUIC(b, "redis_rueidis_streams", "pub", sizedString(256))
}

func Benchmark_Produce_1KBPayload_RedisStreams_QUIC(b *testing.B) {
	benchProduceQUIC(b, "redis_rueidis_streams", "pub", sizedString(1024))
}

func Benchmark_Produce_4KBPayload_RedisStreams_QUIC(b *testing.B) {
	benchProduceQUIC(b, "redis_rueidis_streams", "pub", sizedString(4*1024))
}

func Benchmark_Produce_8KBPayload_RedisStreams_QUIC(b *testing.B) {
	benchProduceQUIC(b, "redis_rueidis_streams", "pub", sizedString(8*1024))
}

func Benchmark_Produce_32KBPayload_RedisStreams_QUIC(b *testing.B) {
	benchProduceQUIC(b, "redis_rueidis_streams", "pub", sizedString(32*1024))
}

// MQTT benchmarks
func Benchmark_Produce_1BPayload_MQTT_QUIC(b *testing.B) {
	benchProduceQUIC(b, "mqtt", "pub", sizedString(1))
}

func Benchmark_Produce_32BPayload_MQTT_QUIC(b *testing.B) {
	benchProduceQUIC(b, "mqtt", "pub", sizedString(32))
}

func Benchmark_Produce_128BPayload_MQTT_QUIC(b *testing.B) {
	benchProduceQUIC(b, "mqtt", "pub", sizedString(128))
}

func Benchmark_Produce_256BPayload_MQTT_QUIC(b *testing.B) {
	benchProduceQUIC(b, "mqtt", "pub", sizedString(256))
}

func Benchmark_Produce_1KBPayload_MQTT_QUIC(b *testing.B) {
	benchProduceQUIC(b, "mqtt", "pub", sizedString(1024))
}

func Benchmark_Produce_4KBPayload_MQTT_QUIC(b *testing.B) {
	benchProduceQUIC(b, "mqtt", "pub", sizedString(4*1024))
}

func Benchmark_Produce_8KBPayload_MQTT_QUIC(b *testing.B) {
	benchProduceQUIC(b, "mqtt", "pub", sizedString(8*1024))
}

func Benchmark_Produce_32KBPayload_MQTT_QUIC(b *testing.B) {
	benchProduceQUIC(b, "mqtt", "pub", sizedString(32*1024))
}

// NSQ benchmarks
func Benchmark_Produce_1BPayload_NSQ_QUIC(b *testing.B) {
	benchProduceQUIC(b, "nsq", "pub", sizedString(1))
}

func Benchmark_Produce_32BPayload_NSQ_QUIC(b *testing.B) {
	benchProduceQUIC(b, "nsq", "pub", sizedString(32))
}

func Benchmark_Produce_128BPayload_NSQ_QUIC(b *testing.B) {
	benchProduceQUIC(b, "nsq", "pub", sizedString(128))
}

func Benchmark_Produce_256BPayload_NSQ_QUIC(b *testing.B) {
	benchProduceQUIC(b, "nsq", "pub", sizedString(256))
}

func Benchmark_Produce_1KBPayload_NSQ_QUIC(b *testing.B) {
	benchProduceQUIC(b, "nsq", "pub", sizedString(1024))
}

func Benchmark_Produce_4KBPayload_NSQ_QUIC(b *testing.B) {
	benchProduceQUIC(b, "nsq", "pub", sizedString(4*1024))
}

func Benchmark_Produce_8KBPayload_NSQ_QUIC(b *testing.B) {
	benchProduceQUIC(b, "nsq", "pub", sizedString(8*1024))
}

func Benchmark_Produce_32KBPayload_NSQ_QUIC(b *testing.B) {
	benchProduceQUIC(b, "nsq", "pub", sizedString(32*1024))
}

func benchProduceQUIC(b *testing.B, typ, topic, payload string) {
	ctx, cancel := context.WithCancel(b.Context())
	defer cancel()

	var s *server.Server

	b.StopTimer()
	switch typ {
	case "nop":
		s = RunDefaultServerWithNopQUIC(ctx)
	case "kafka3":
		s = RunDefaultServerWithKafka3BrokersQUIC(ctx)
	case "nats":
		s = RunDefaultServerWithNatsQUIC(ctx)
	case "rabbitmq":
		s = RunDefaultServerWithAMQP091QUIC(ctx)
	case "artemismq":
		s = RunDefaultServerWithAMQP10QUIC(ctx)
	case "resp_pubsub":
		s = RunDefaultServerWithRedisPubSubQUIC(ctx)
	case "redis_rueidis_streams":
		s = RunDefaultServerWithRedisStreamsQUIC(ctx)
	case "mqtt":
		s = RunDefaultServerWithMQTTQUIC(ctx)
	case "nsq":
		s = RunDefaultServerWithNSQQUIC(ctx)
	default:
		panic("invalid typ")
	}

	defer func() {
		cancel()
		<-s.Done()
	}()

	c := createClientConn(ctx, PERF_ADDR)
	p := doDefaultBind(c)

	cmd := []byte{
		byte(v1.OP_CODE_PRODUCE),
		0, 0, 0, 0,
	}

	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(topic)))

	cmd = append(cmd, lenBuf...)
	cmd = append(cmd, []byte(topic)...)

	binary.BigEndian.PutUint32(lenBuf, uint32(len(payload)))

	cmd = append(cmd, lenBuf...)
	cmd = append(cmd, []byte(payload)...)

	b.SetBytes(int64(len(cmd)))
	bw := bufio.NewWriterSize(p, defaultSendBufSize)

	bytes := make(chan int)

	go drainStream(b, p, bytes)

	b.StartTimer()

	startTime := time.Now()
	for b.Loop() {
		bw.Write(cmd)
	}
	bw.Write([]byte{byte(v1.OP_CODE_DISCONNECT)})

	bw.Flush()
	elapsed := time.Since(startTime)
	fmt.Println("seconds to write full buf to quic stream:", elapsed.Seconds())
	res := <-bytes
	b.StopTimer()
	p.Close()
	_ = c.CloseWithError(0x0, "")
	expected := b.N*6 + 3
	if res != expected {
		panic(fmt.Errorf("Invalid number of bytes read: bytes: %d, expected: %d", res, expected))
	}
}

func benchProduceTCP(b *testing.B, typ, topic, payload string) {
	ctx, cancel := context.WithCancel(b.Context())
	defer cancel()

	var s *server.Server

	b.StopTimer()
	switch typ {
	case "nop":
		s = RunDefaultServerWithNopTCP(ctx)
	case "kafka3":
		s = RunDefaultServerWithKafka3BrokersTCP(ctx)
	default:
		panic("invalid typ")
	}

	defer func() {
		cancel()
		<-s.Done()
	}()

	c := createTCPClientConn(PERF_TCP_ADDR)
	doDefaultBindTCP(c)

	cmd := []byte{
		byte(v1.OP_CODE_PRODUCE),
		0, 0, 0, 0,
	}

	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(topic)))

	cmd = append(cmd, lenBuf...)
	cmd = append(cmd, []byte(topic)...)

	binary.BigEndian.PutUint32(lenBuf, uint32(len(payload)))

	cmd = append(cmd, lenBuf...)
	cmd = append(cmd, []byte(payload)...)

	b.SetBytes(int64(len(cmd)))
	bw := bufio.NewWriterSize(c, defaultSendBufSize)

	bytes := make(chan int)

	go drainTCPConn(b, c, bytes)

	b.StartTimer()

	startTime := time.Now()
	for b.Loop() {
		bw.Write(cmd)
	}
	bw.Write([]byte{byte(v1.OP_CODE_DISCONNECT)})

	bw.Flush()
	elapsed := time.Since(startTime)
	fmt.Println("seconds to write full buf to tcp conn:", elapsed.Seconds())
	res := <-bytes
	b.StopTimer()
	c.Close()
	expected := b.N*6 + 3
	if res != expected {
		panic(fmt.Errorf("Invalid number of bytes read: bytes: %d, expected: %d", res, expected))
	}
}

func benchProduceUnix(b *testing.B, typ, topic, payload string) {
	ctx, cancel := context.WithCancel(b.Context())
	defer cancel()

	var s *server.Server

	b.StopTimer()
	switch typ {
	case "nop":
		s = RunDefaultServerWithNopUnix(ctx)
	case "kafka3":
		s = RunDefaultServerWithKafka3BrokersUnix(ctx)
	default:
		panic("invalid typ")
	}

	defer func() {
		cancel()
		<-s.Done()
	}()

	c := createUnixClientConn(PERF_UNIX_PATH)
	doDefaultBindUnix(c)

	cmd := []byte{
		byte(v1.OP_CODE_PRODUCE),
		0, 0, 0, 0,
	}

	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(topic)))

	cmd = append(cmd, lenBuf...)
	cmd = append(cmd, []byte(topic)...)

	binary.BigEndian.PutUint32(lenBuf, uint32(len(payload)))

	cmd = append(cmd, lenBuf...)
	cmd = append(cmd, []byte(payload)...)

	b.SetBytes(int64(len(cmd)))
	bw := bufio.NewWriterSize(c, defaultSendBufSize)

	bytes := make(chan int)

	go drainUnixConn(b, c, bytes)

	b.StartTimer()

	startTime := time.Now()
	for b.Loop() {
		bw.Write(cmd)
	}
	bw.Write([]byte{byte(v1.OP_CODE_DISCONNECT)})

	bw.Flush()
	elapsed := time.Since(startTime)
	fmt.Println("seconds to write full buf to unix conn:", elapsed.Seconds())
	res := <-bytes
	b.StopTimer()
	c.Close()
	expected := b.N*6 + 3
	if res != expected {
		panic(fmt.Errorf("Invalid number of bytes read: bytes: %d, expected: %d", res, expected))
	}
}

var ch = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@$#%^&*()")

func sizedBytes(sz int) []byte {
	b := make([]byte, sz)
	for i := range b {
		b[i] = ch[rand.Intn(len(ch))]
	}
	return b
}

func sizedString(sz int) string {
	return string(sizedBytes(sz))
}
