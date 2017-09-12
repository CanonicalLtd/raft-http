package rafthttp

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/CanonicalLtd/raft-membership"
	"github.com/pkg/errors"
)

// Handler implements an HTTP handler that will look for an Upgrade
// header in the request to switch the HTTP connection to raw TCP
// mode, so it can be used as raft.NetworkTransport stream.
type Handler struct {
	requests    chan *raftmembership.ChangeRequest // Membership requests are pushed to this channel
	connections chan net.Conn                      // New Raft connections are pushed to this channel.
	shutdown    chan struct{}                      // Used to stop processing membership requests.
	timeout     time.Duration                      // Maximum time to wait for requests to be processed.
}

// NewHandler returns a new Handler.
//
// Incoming raft membership requests (received via POST and DELETE) are
// forwarded to the given channel, which is supposed to be processed using
// raftmembership.HandleChangeRequests().
func NewHandler(requests chan *raftmembership.ChangeRequest) *Handler {
	return &Handler{
		requests:    requests,
		connections: make(chan net.Conn, 0),
		shutdown:    make(chan struct{}),
		timeout:     10 * time.Second,
	}
}

// Timeout sets the maximum amount of time for a request to be processed. It
// defaults to 10 seconds if not set.
func (h *Handler) Timeout(timeout time.Duration) {
	h.timeout = timeout
}

// Close stops handling incoming requests.
func (h *Handler) Close() {
	close(h.shutdown)
	close(h.connections)
}

// ServerHTTP upgrades the given HTTP connection to a raw TCP one for
// use by raft.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Make sure we haven't been closed.
	switch r.Method {
	case "GET":
		h.handleGet(w, r)
	case "POST":
		h.handlePost(w, r)
	case "DELETE":
		h.handleDelete(w, r)
	default:
		http.Error(w, "unknown action", http.StatusMethodNotAllowed)
	}
}

func (h *Handler) handleGet(w http.ResponseWriter, r *http.Request) {
	// Fail immediately if we've been closed.
	select {
	case <-h.shutdown:
		http.Error(w, "raft transport closed", http.StatusForbidden)
		return
	default:
	}

	if r.Header.Get("Upgrade") != "raft" {
		http.Error(w, "missing or invalid upgrade header", http.StatusBadRequest)
		return
	}

	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "webserver doesn't support hijacking", http.StatusInternalServerError)
		return
	}

	conn, _, err := hijacker.Hijack()
	if err != nil {
		message := errors.Wrap(err, "failed to hijack connection").Error()
		http.Error(w, message, http.StatusInternalServerError)
		return
	}

	// Write the status line and upgrade header by hand since w.WriteHeader()
	// would fail after Hijack()
	data := []byte("HTTP/1.1 101 Switching Protocols\r\nUpgrade: raft\r\n\r\n")
	if n, err := conn.Write(data); err != nil || n != len(data) {
		conn.Close()
		return
	}

	select {
	case h.connections <- conn:
	case <-time.After(h.timeout):
		conn.Close()
	}
}

func (h *Handler) handlePost(w http.ResponseWriter, r *http.Request) {
	request := raftmembership.NewJoinRequest(r.URL.Query().Get("peer"))
	h.changeMembership(w, r, request)
}

func (h *Handler) handleDelete(w http.ResponseWriter, r *http.Request) {
	request := raftmembership.NewLeaveRequest(r.URL.Query().Get("peer"))
	h.changeMembership(w, r, request)
}

func (h *Handler) changeMembership(w http.ResponseWriter, r *http.Request, request *raftmembership.ChangeRequest) {
	// Sanity check before actually trying to process the request.
	if request.Peer() == "" {
		http.Error(w, "no peer address provided", http.StatusBadRequest)
		return
	}

	// Send the request to the channel for processing, unless we've been
	// closed (and in that case we bail out).
	select {
	case <-h.shutdown:
		http.Error(w, "raft transport closed", http.StatusForbidden)
		return
	case h.requests <- request:
	}

	err := request.Error(h.timeout)
	if err == nil {
		return
	}

	var code int

	switch err := err.(type) {
	case *raftmembership.ErrDifferentLeader:
		// If we fail because the current node is not the leader, send
		// a redirect.
		url := &url.URL{
			Scheme:   "http", // XXX TODO: handle HTTPS
			Path:     r.URL.Path,
			RawQuery: r.URL.RawQuery,
			Host:     err.Leader(),
		}
		http.Redirect(w, r, url.String(), StatusPermanentRedirect)
		return
	case *raftmembership.ErrUnknownLeader:
		// If we fail because we currently don't know the leader, hint
		// the client to retry.
		code = http.StatusServiceUnavailable
	default:
		code = http.StatusForbidden
	}

	message := errors.Wrap(err, fmt.Sprintf(
		"failed to %s peer %s", request.Kind(), request.Peer())).Error()
	http.Error(w, message, code)
}
