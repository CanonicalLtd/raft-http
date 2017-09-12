package rafthttp_test

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	rafthttp "github.com/CanonicalLtd/raft-http"
	raftmembership "github.com/CanonicalLtd/raft-membership"
	rafttest "github.com/CanonicalLtd/raft-test"
	"github.com/hashicorp/raft"
)

// Connect threed raft nodes using HTTP network layers.
func Example() {
	// Create a 3-node cluster with default test configuration.
	cluster := rafttest.NewCluster(3)

	// Turn off automatic transports connection.
	cluster.AutoConnectNodes = false

	node0 := cluster.Node(0)
	node1 := cluster.Node(1)
	node2 := cluster.Node(2)

	// Node 0 will self-elect
	node0.Config.EnableSingleNode = true

	// Replace the default in-memory transports with actual
	// network transports using HTTP layers.
	handlers := make([]*rafthttp.Handler, 3)
	layers := make([]*rafthttp.Layer, 3)
	for i := range layers {
		handler := rafthttp.NewHandler()
		layer, cleanup := newLayer(handler)
		defer cleanup()

		node := cluster.Node(i)
		node.Transport = raft.NewNetworkTransportWithLogger(
			layer, 2, time.Second, node.Config.Logger)

		layers[i] = layer
		handlers[i] = handler
	}
	cluster.Start()
	defer cluster.Shutdown()

	// Start handling membership change requests on all nodes.
	for i, handler := range handlers {
		node := cluster.Node(i)
		go raftmembership.HandleChangeRequests(node.Raft(), handler.Requests())
	}
	cluster.LeadershipAcquired()

	// Request that the second node joins the cluster.
	if err := layers[1].Join(node0.Transport.LocalAddr(), time.Second); err != nil {
		log.Fatal(err)
	}
	peers, _ := node0.Peers.Peers()
	if len(peers) != 2 {
		log.Fatalf("expected node 0 to have 2 peers, got %d", len(peers))
	}
	if peer := node1.Transport.LocalAddr(); peer != peers[0] {
		log.Fatalf("expected node 0 to have peer %s, got %s", peer, peers[0])
	}

	// Request that the third node joins the cluster, contacting
	// the non-leader node 1. The request will be automatically
	// redirected to node 0.
	if err := layers[2].Join(node1.Transport.LocalAddr(), time.Second); err != nil {
		log.Fatal(err)
	}
	peers, _ = node0.Peers.Peers()
	if len(peers) != 3 {
		log.Fatalf("expected node 0 to have 3 peers, got %d", len(peers))
	}
	if peer := node2.Transport.LocalAddr(); peer != peers[0] {
		log.Fatalf("expected node 0 to have peer %s, got %s", peer, peers[1])
	}

	// Rquest that the third node leaves the cluster.
	if err := layers[2].Leave(node0.Transport.LocalAddr(), time.Second); err != nil {
		log.Fatal(err)
	}
	peers, _ = node0.Peers.Peers()
	if len(peers) != 2 {
		log.Fatalf("expected node 0 to have 1 peers, got %d", len(peers))
	}
	if peer := node1.Transport.LocalAddr(); peer != peers[1] {
		log.Fatalf("expected node 0 to have peer %s, got %s", peer, peers[1])
	}

	// Output:
	// true
	fmt.Printf("%v", strings.Contains(cluster.LogOutput.String(), "entering Leader state"))
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
