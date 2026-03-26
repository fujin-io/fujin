package server

import (
	"sync"
	"testing"

	connectorconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
)

func TestReloadConnectors(t *testing.T) {
	initial := connectorconfig.ConnectorsConfig{
		"conn1": {Type: "noop"},
	}

	conf := testConfig(initial)
	s, err := NewServer(conf, testLogger())
	if err != nil {
		t.Fatal(err)
	}

	// Verify initial config
	got := *s.connectorConfig.Load()
	if len(got) != 1 || got["conn1"].Type != "noop" {
		t.Fatalf("initial config mismatch: %v", got)
	}

	// Reload with new config
	updated := connectorconfig.ConnectorsConfig{
		"conn1": {Type: "kafka"},
		"conn2": {Type: "nats"},
	}
	s.ReloadConnectors(updated)

	got = *s.connectorConfig.Load()
	if len(got) != 2 {
		t.Fatalf("expected 2 connectors after reload, got %d", len(got))
	}
	if got["conn1"].Type != "kafka" {
		t.Fatalf("conn1 type should be kafka, got %s", got["conn1"].Type)
	}
	if got["conn2"].Type != "nats" {
		t.Fatalf("conn2 type should be nats, got %s", got["conn2"].Type)
	}
}

func TestReloadConnectors_Concurrent(t *testing.T) {
	initial := connectorconfig.ConnectorsConfig{
		"conn1": {Type: "v0"},
	}

	conf := testConfig(initial)
	s, err := NewServer(conf, testLogger())
	if err != nil {
		t.Fatal(err)
	}

	// Simulate concurrent reloads and reads
	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(2)
		go func(n int) {
			defer wg.Done()
			s.ReloadConnectors(connectorconfig.ConnectorsConfig{
				"conn1": {Type: "kafka"},
			})
		}(i)
		go func() {
			defer wg.Done()
			cfg := *s.connectorConfig.Load()
			_ = cfg["conn1"]
		}()
	}
	wg.Wait()
}
