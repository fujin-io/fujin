package main

import (
	"context"
	"log"
	"os"

	zmq "github.com/go-zeromq/zmq4"
)

func main() {
	endpoint := os.Getenv("ZEROMQ_ENDPOINT")
	if endpoint == "" {
		endpoint = "tcp://*:5555"
	}

	ctx := context.Background()
	pub := zmq.NewPub(ctx)
	defer func() {
		if err := pub.Close(); err != nil {
			log.Printf("zeromq-socket: close error: %v", err)
		}
	}()

	if err := pub.Listen(endpoint); err != nil {
		log.Fatalf("zeromq-socket: listen on %s: %v", endpoint, err)
	}

	log.Printf("zeromq-socket: listening on %s", endpoint)

	// Block forever; this process only keeps the socket open.
	select {}
}

