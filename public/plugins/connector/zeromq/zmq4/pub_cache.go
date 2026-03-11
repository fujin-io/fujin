package zmq4

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/go-zeromq/zmq4"
)

// pubRef holds a PUB socket and refcount. Shared across writers for the same endpoint.
type pubRef struct {
	sock zmq4.Socket
	ref  int64
}

var (
	pubCache   = make(map[string]*pubRef)
	pubCacheMu sync.Mutex
)

// getOrCreatePUB returns a shared PUB socket for the given endpoint.
// bind true = Listen, false = Dial.
// Caller must call releasePUB when done.
func getOrCreatePUB(ctx context.Context, endpoint string, bind bool) (zmq4.Socket, error) {
	var key string
	if bind {
		key = "bind:" + endpoint
	} else {
		key = "connect:" + endpoint
	}

	pubCacheMu.Lock()
	defer pubCacheMu.Unlock()

	if ref, ok := pubCache[key]; ok {
		atomic.AddInt64(&ref.ref, 1)
		return ref.sock, nil
	}

	pub := zmq4.NewPub(ctx)
	if bind {
		if err := pub.Listen(endpoint); err != nil {
			return nil, fmt.Errorf("zeromq_zmq4: pub listen %q: %w", endpoint, err)
		}
	} else {
		if err := pub.Dial(endpoint); err != nil {
			pub.Close()
			return nil, fmt.Errorf("zeromq_zmq4: pub dial %q: %w", endpoint, err)
		}
	}

	ref := &pubRef{sock: pub, ref: 1}
	pubCache[key] = ref
	return pub, nil
}

// releasePUB decrements the refcount. Does not close the socket;
// the socket persists for the process lifetime.
func releasePUB(endpoint string, bind bool) {
	var key string
	if bind {
		key = "bind:" + endpoint
	} else {
		key = "connect:" + endpoint
	}

	pubCacheMu.Lock()
	defer pubCacheMu.Unlock()

	ref, ok := pubCache[key]
	if !ok {
		return
	}

	n := atomic.AddInt64(&ref.ref, -1)
	if n <= 0 {
		ref.sock.Close()
		delete(pubCache, key)
	}
}
