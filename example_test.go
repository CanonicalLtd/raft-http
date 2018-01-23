package rafthttp_test

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/CanonicalLtd/raft-http"
	"github.com/CanonicalLtd/raft-membership"
	"github.com/CanonicalLtd/raft-test"
	"github.com/hashicorp/raft"
)

// Connect threed raft nodes using HTTP network layers.
func Example() {
	t := &testing.T{}

	// Create a set of transports using HTTP layers.
	handlers := make([]*rafthttp.Handler, 3)
	layers := make([]*rafthttp.Layer, 3)
	transports := make([]raft.Transport, 3)
	out := bytes.NewBuffer(nil)
	for i := range layers {
		handler := rafthttp.NewHandler()
		layer, cleanup := newLayer(handler)
		defer cleanup()

		transport := raft.NewNetworkTransport(layer, 2, time.Second, out)

		layers[i] = layer
		handlers[i] = handler
		transports[i] = transport
	}

	// Create a raft.Transport factory that uses the above layers.
	transport := rafttest.Transport(func(i int) raft.Transport { return transports[i] })
	servers := rafttest.Servers(0)

	// Create a 3-node cluster with default test configuration.
	rafts, cleanup := rafttest.Cluster(t, rafttest.FSMs(3), transport, servers)
	defer cleanup()

	// Start handling membership change requests on all nodes.
	for i, handler := range handlers {
		go raftmembership.HandleChangeRequests(rafts[i], handler.Requests())
	}

	rafttest.WaitLeader(t, rafts[0], time.Second)

	// Request that the second node joins the cluster.
	if err := layers[1].Join("1", transports[0].LocalAddr(), time.Second); err != nil {
		log.Fatalf("joining server 1 failed: %v", err)
	}

	// Request that the third node joins the cluster, contacting
	// the non-leader node 1. The request will be automatically
	// redirected to node 0.
	if err := layers[2].Join("2", transports[1].LocalAddr(), time.Second); err != nil {
		log.Fatal(err)
	}

	// Rquest that the third node leaves the cluster.
	if err := layers[2].Leave("2", transports[2].LocalAddr(), time.Second); err != nil {
		log.Fatal(err)
	}

	// Output:
	// true
	// 1
	fmt.Println(strings.Contains(out.String(), "accepted connection from"))
	fmt.Println(rafts[0].Stats()["num_peers"])
}

// Create a new Layer using a new Handler attached to a running HTTP
// server.
func newLayer(handler *rafthttp.Handler) (*rafthttp.Layer, func()) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("listening to local port failed: %v", err)
	}
	layer := rafthttp.NewLayer("/", listener.Addr(), handler, rafthttp.NewDialTCP())
	server := &http.Server{Handler: handler}
	go server.Serve(listener)

	cleanup := func() {
		listener.Close()
	}

	return layer, cleanup
}
