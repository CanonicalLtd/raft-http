package rafthttp

import (
	"fmt"
	"net"
	"net/http"
	"net/url"

	"github.com/dqlite/raft-membership"
	"github.com/pkg/errors"
)

// Handler implements an HTTP handler that will look for an Upgrade
// header in the request to switch the HTTP connection to raw TCP
// mode, so it can be used as raft.NetworkTransport stream.
type Handler struct {
	connections chan net.Conn
	requests    chan *raftmembership.ChangeRequest
	shutdown    chan struct{}
}

// NewHandler returns a new Handler.
func NewHandler() *Handler {
	return &Handler{
		connections: make(chan net.Conn, 0),
		requests:    make(chan *raftmembership.ChangeRequest, 0),
		shutdown:    make(chan struct{}),
	}
}

// Connections returns a channel of net.Conn objects that a Layer can
// receive from in order to establish new raft TCP connections.
func (h *Handler) Connections() chan net.Conn {
	return h.connections
}

// MembershipChangeRequests is a channel of raftmembership.ChangeRequest
// objects that can be passed to raftmembership.HandleChangeRequests to
// process join/leave requests.
func (h *Handler) MembershipChangeRequests() chan *raftmembership.ChangeRequest {
	return h.requests
}

// Close closes all channels and stops processing requests.
func (h *Handler) Close() {
	close(h.connections)
	close(h.requests)
	close(h.shutdown)
}

// ServerHTTP upgrades the given HTTP connection to a raw TCP one for
// use by raft.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Make sure we haven't been closed.
	select {
	case <-h.shutdown:
		http.Error(w, "raft transport closed", http.StatusForbidden)
		return
	default:
	}

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

	h.connections <- conn
}

func (h *Handler) handlePost(w http.ResponseWriter, r *http.Request) {
	request := raftmembership.NewJoinRequest(r.URL.Query().Get("peer"))
	h.processMembershipChangeRequest(w, r, request)
}

func (h *Handler) handleDelete(w http.ResponseWriter, r *http.Request) {
	request := raftmembership.NewLeaveRequest(r.URL.Query().Get("peer"))
	h.processMembershipChangeRequest(w, r, request)
}

func (h *Handler) processMembershipChangeRequest(w http.ResponseWriter, r *http.Request, request *raftmembership.ChangeRequest) {
	if request.Peer() == "" {
		http.Error(w, "no peer address provided", http.StatusBadRequest)
		return
	}

	h.requests <- request

	err := request.Error()
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
		http.Redirect(w, r, url.String(), http.StatusPermanentRedirect)
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
