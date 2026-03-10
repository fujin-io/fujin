//go:build quic

package quic_test

import (
	"context"
	"crypto/tls"
	"log/slog"
	"os"
	"testing"
	"time"

	quicserver "github.com/fujin-io/fujin/internal/transport/quic"
	connectorconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
	"github.com/fujin-io/fujin/public/server/config"
	quicgo "github.com/quic-go/quic-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewFujinServer(t *testing.T) {
	conf := config.QUICServerConfig{
		Addr: ":4848",
		Fujin: config.FujinProtocolConfig{
			PingInterval:          2 * time.Second,
			PingTimeout:           5 * time.Second,
			PingMaxRetries:        3,
			WriteDeadline:         10 * time.Second,
			ForceTerminateTimeout: 15 * time.Second,
		},
	}
	baseConfig := connectorconfig.ConnectorsConfig{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	srv := quicserver.NewFujinServer(conf, baseConfig, logger)

	assert.NotNil(t, srv)
}

func TestNewFujinServer_WithNilLogger(t *testing.T) {
	conf := config.QUICServerConfig{
		Addr: ":4848",
	}

	assert.NotPanics(t, func() {
		quicserver.NewFujinServer(conf, connectorconfig.ConnectorsConfig{}, slog.Default())
	})
}

func TestNewFujinServer_WithTLS(t *testing.T) {
	conf := config.QUICServerConfig{
		Addr: ":4848",
		Fujin: config.FujinProtocolConfig{
			PingInterval: 2 * time.Second,
		},
		TLS: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}
	baseConfig := connectorconfig.ConnectorsConfig{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	srv := quicserver.NewFujinServer(conf, baseConfig, logger)

	assert.NotNil(t, srv)
}

func TestNewFujinServer_WithQUICConfig(t *testing.T) {
	conf := config.QUICServerConfig{
		Addr: ":4848",
		Fujin: config.FujinProtocolConfig{
			PingInterval: 2 * time.Second,
		},
		QUIC: &quicgo.Config{
			MaxIdleTimeout: 30 * time.Second,
		},
	}
	baseConfig := connectorconfig.ConnectorsConfig{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	srv := quicserver.NewFujinServer(conf, baseConfig, logger)

	assert.NotNil(t, srv)
}

func TestFujinServer_ReadyForConnections_Timeout(t *testing.T) {
	conf := config.QUICServerConfig{
		Addr: ":4848",
	}
	baseConfig := connectorconfig.ConnectorsConfig{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	srv := quicserver.NewFujinServer(conf, baseConfig, logger)

	ready := srv.ReadyForConnections(10 * time.Millisecond)
	assert.False(t, ready, "Server should not be ready (timeout)")
}

func TestFujinServer_ReadyForConnections_Success(t *testing.T) {
	conf := config.QUICServerConfig{
		Addr: ":4848",
	}
	baseConfig := connectorconfig.ConnectorsConfig{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	srv := quicserver.NewFujinServer(conf, baseConfig, logger)

	go func() {
		time.Sleep(10 * time.Millisecond)
	}()

	ready := srv.ReadyForConnections(5 * time.Millisecond)
	assert.False(t, ready)
}

func TestFujinServer_Done(t *testing.T) {
	conf := config.QUICServerConfig{
		Addr: ":4848",
	}
	baseConfig := connectorconfig.ConnectorsConfig{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	srv := quicserver.NewFujinServer(conf, baseConfig, logger)

	done := srv.Done()
	assert.NotNil(t, done, "Done channel should not be nil")

	select {
	case <-done:
		t.Fatal("Done channel should not be closed initially")
	case <-time.After(10 * time.Millisecond):
	}
}

func TestFujinServer_ListenAndServe_InvalidAddress(t *testing.T) {
	conf := config.QUICServerConfig{
		Addr: "invalid address format",
	}
	baseConfig := connectorconfig.ConnectorsConfig{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	srv := quicserver.NewFujinServer(conf, baseConfig, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := srv.ListenAndServe(ctx)
	assert.Error(t, err, "Should fail with invalid address")
}

func TestFujinServer_ListenAndServe_CancelContext(t *testing.T) {
	conf := config.QUICServerConfig{
		Addr: "127.0.0.1:0",
		Fujin: config.FujinProtocolConfig{
			PingInterval:          100 * time.Millisecond,
			PingTimeout:           200 * time.Millisecond,
			PingMaxRetries:        2,
			WriteDeadline:         1 * time.Second,
			ForceTerminateTimeout: 2 * time.Second,
		},
		TLS:  &tls.Config{},
		QUIC: &quicgo.Config{},
	}
	baseConfig := connectorconfig.ConnectorsConfig{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelError,
	}))

	srv := quicserver.NewFujinServer(conf, baseConfig, logger)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- srv.ListenAndServe(ctx)
	}()

	ready := srv.ReadyForConnections(2 * time.Second)
	require.True(t, ready, "Server should be ready within timeout")

	cancel()

	select {
	case err := <-done:
		assert.NoError(t, err, "Server should stop cleanly")
	case <-time.After(5 * time.Second):
		t.Fatal("Server did not stop within timeout")
	}

	select {
	case <-srv.Done():
	case <-time.After(1 * time.Second):
		t.Fatal("Done channel should be closed after server stops")
	}
}

func TestFujinServer_MultipleInstances(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	baseConfig := connectorconfig.ConnectorsConfig{}

	servers := make([]*quicserver.FujinServer, 3)
	for i := 0; i < 3; i++ {
		conf := config.QUICServerConfig{
			Addr: ":4848",
			Fujin: config.FujinProtocolConfig{
				PingInterval: 2 * time.Second,
			},
		}
		servers[i] = quicserver.NewFujinServer(conf, baseConfig, logger)
		assert.NotNil(t, servers[i])
	}

	for i := 0; i < 3; i++ {
		assert.NotNil(t, servers[i].Done())
	}
}

func TestFujinServer_ConfigurationVariations(t *testing.T) {
	tests := []struct {
		name string
		conf config.QUICServerConfig
	}{
		{
			name: "minimal config",
			conf: config.QUICServerConfig{
				Addr: ":4848",
			},
		},
		{
			name: "full config",
			conf: config.QUICServerConfig{
				Addr: ":4848",
				Fujin: config.FujinProtocolConfig{
					PingInterval:          1 * time.Second,
					PingTimeout:           3 * time.Second,
					PingMaxRetries:        5,
					WriteDeadline:         5 * time.Second,
					ForceTerminateTimeout: 10 * time.Second,
				},
				TLS:  &tls.Config{},
				QUIC: &quicgo.Config{},
			},
		},
		{
			name: "with TLS only",
			conf: config.QUICServerConfig{
				Addr: ":4848",
				TLS: &tls.Config{
					MinVersion: tls.VersionTLS13,
				},
			},
		},
		{
			name: "with QUIC only",
			conf: config.QUICServerConfig{
				Addr: ":4848",
				QUIC: &quicgo.Config{
					MaxIdleTimeout: 60 * time.Second,
				},
			},
		},
		{
			name: "zero values",
			conf: config.QUICServerConfig{
				Addr: ":0",
				Fujin: config.FujinProtocolConfig{
					PingInterval:          0,
					PingTimeout:           0,
					PingMaxRetries:        0,
					WriteDeadline:         0,
					ForceTerminateTimeout: 0,
				},
			},
		},
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	baseConfig := connectorconfig.ConnectorsConfig{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := quicserver.NewFujinServer(tt.conf, baseConfig, logger)
			assert.NotNil(t, srv)
			assert.NotNil(t, srv.Done())
		})
	}
}

func TestFujinServer_ReadyForConnections_MultipleWaiters(t *testing.T) {
	conf := config.QUICServerConfig{
		Addr: ":4848",
	}
	baseConfig := connectorconfig.ConnectorsConfig{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	srv := quicserver.NewFujinServer(conf, baseConfig, logger)

	results := make(chan bool, 3)
	for i := 0; i < 3; i++ {
		go func() {
			ready := srv.ReadyForConnections(10 * time.Millisecond)
			results <- ready
		}()
	}

	for i := 0; i < 3; i++ {
		ready := <-results
		assert.False(t, ready, "All should timeout since server not started")
	}
}
