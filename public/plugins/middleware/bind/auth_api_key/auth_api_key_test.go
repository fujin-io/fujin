package auth_api_key

import (
	"context"
	"log/slog"
	"testing"

	bmw "github.com/fujin-io/fujin/public/plugins/middleware/bind"
)

func TestAuthAPIKeyMiddleware(t *testing.T) {
	logger := slog.Default()

	tests := []struct {
		name          string
		config        map[string]any
		meta          map[string]string
		wantErr       bool
		expectedError string
	}{
		{
			name:   "valid api key",
			config: map[string]any{"api_key": "secret-key-123"},
			meta:   map[string]string{"api_key": "secret-key-123"},
			wantErr: false,
		},
		{
			name:          "missing api key in meta",
			config:        map[string]any{"api_key": "secret-key-123"},
			meta:          map[string]string{},
			wantErr:       true,
			expectedError: "authentication required: api_key missing in meta",
		},
		{
			name:          "invalid api key",
			config:        map[string]any{"api_key": "secret-key-123"},
			meta:          map[string]string{"api_key": "wrong-key"},
			wantErr:       true,
			expectedError: "authentication failed: invalid api_key",
		},
		{
			name:          "empty api key in config",
			config:        map[string]any{"api_key": ""},
			meta:          map[string]string{"api_key": "any-key"},
			wantErr:       true,
			expectedError: "api_key is required",
		},
		{
			name:          "nil config",
			config:        nil,
			meta:          map[string]string{"api_key": "any-key"},
			wantErr:       true,
			expectedError: "api_key is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Get the factory
			factory, ok := bmw.Get("auth_api_key")
			if !ok {
				t.Fatal("auth_api_key middleware not registered")
			}

			// Create middleware
			middleware, err := factory(tt.config, logger)
			if tt.expectedError == "api_key is required" {
				if err == nil {
					t.Fatal("expected error for empty api_key")
				}
				if err.Error() != tt.expectedError {
					t.Errorf("expected error %q, got %q", tt.expectedError, err.Error())
				}
				return
			}

			if err != nil {
				t.Fatalf("factory() error = %v", err)
			}

			// Process bind
			err = middleware.ProcessBind(context.Background(), tt.meta)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tt.expectedError != "" && err.Error() != tt.expectedError {
					t.Errorf("expected error %q, got %q", tt.expectedError, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("ProcessBind() error = %v, want nil", err)
				}
			}
		})
	}
}

func TestAuthAPIKeyMiddleware_Registration(t *testing.T) {
	// Verify the middleware is registered
	factory, ok := bmw.Get("auth_api_key")
	if !ok {
		t.Fatal("auth_api_key middleware not registered")
	}
	if factory == nil {
		t.Fatal("factory is nil")
	}

	// Verify it's in the list
	list := bmw.List()
	found := false
	for _, name := range list {
		if name == "auth_api_key" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("auth_api_key not found in middleware list")
	}
}

