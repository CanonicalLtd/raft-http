package rafthttp_test

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/dqlite/raft-http"
	"github.com/dqlite/raft-membership"
)

func TestHandler_NoUpgradeHeader(t *testing.T) {
	w := httptest.NewRecorder()

	// Make a request with no Upgrade header
	r := &http.Request{Method: "GET"}

	handler := rafthttp.NewHandler()
	handler.ServeHTTP(w, r)

	responseCheck(t, w, http.StatusBadRequest, "missing or invalid upgrade header\n")
}

func TestHandler_Closed(t *testing.T) {
	w := httptest.NewRecorder()
	r := &http.Request{Method: "GET"}
	handler := rafthttp.NewHandler()

	// Handle the request after the handler is closed
	handler.Close()
	handler.ServeHTTP(w, r)

	responseCheck(t, w, http.StatusForbidden, "raft transport closed\n")
}

func TestHandler_HijackerNotImplemented(t *testing.T) {
	// Use a response writer that doesn't implement http.Hijacker
	w := httptest.NewRecorder()

	r := &http.Request{Method: "GET", Header: make(http.Header)}
	r.Header.Set("Upgrade", "raft")
	handler := rafthttp.NewHandler()
	handler.ServeHTTP(w, r)

	responseCheck(t, w, http.StatusInternalServerError, "webserver doesn't support hijacking\n")
}

func TestHandler_HijackError(t *testing.T) {
	// Use a response writer that fails with an error when
	// invoking its Hijack() method.
	w := newResponseHijacker()
	w.HijackError = fmt.Errorf("boom")

	r := &http.Request{Method: "GET", Header: make(http.Header)}
	r.Header.Set("Upgrade", "raft")
	handler := rafthttp.NewHandler()
	handler.ServeHTTP(w, r)

	responseCheck(t, &w.ResponseRecorder, http.StatusInternalServerError, "failed to hijack connection: boom\n")
}

func TestHandler_HijackConnWriteError(t *testing.T) {
	w := newResponseHijacker()

	// Trigger an error when writing to the hijacked connection.
	w.HijackConn, _ = net.Pipe()
	w.HijackConn.Close()

	r := &http.Request{Method: "GET", Header: make(http.Header)}
	r.Header.Set("Upgrade", "raft")
	handler := rafthttp.NewHandler()
	handler.ServeHTTP(w, r)

	// The response has code 200 instead of 101 and the body is
	// empty.
	responseCheck(t, &w.ResponseRecorder, http.StatusOK, "")
}

func TestHandler_Connections(t *testing.T) {
	// Hijack the HTTP connection to a net.Pipe instance.
	server, client := net.Pipe()
	w := newResponseHijacker()
	w.HijackConn = server

	r := &http.Request{Method: "GET", Header: make(http.Header)}
	r.Header.Set("Upgrade", "raft")

	handler := rafthttp.NewHandler()
	go handler.ServeHTTP(w, r)

	reader := bufio.NewReader(client)

	// Check that the client is receiving the 101 status line.
	statusLine, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if statusLine != "HTTP/1.1 101 Switching Protocols\r\n" {
		t.Fatalf("Unexpected statusLine line: %s", statusLine)
	}

	// Check that the client is receiving the upgrade header.
	upgradeHeader, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if upgradeHeader != "Upgrade: raft\r\n" {
		t.Fatalf("Unexpected upgrade header: %s", statusLine)
	}

	// Check tha the handler notifies the hijacked connection.
	if <-handler.Connections() != server {
		t.Fatalf("Connection channel didn't return server connection")
	}
}

// If the peer is not provided as query parameter when joining or
// leaving, an error is returned.
func TestHandler_NoPeerProvided(t *testing.T) {
	w := httptest.NewRecorder()
	r := &http.Request{Method: "POST", URL: &url.URL{}}
	handler := rafthttp.NewHandler()
	handler.ServeHTTP(w, r)
	body := w.Body.String()
	if body != "no peer address provided\n" {
		t.Fatalf("Unexpected body content: '%s'", body)
	}
	if w.Code != http.StatusBadRequest {
		t.Fatalf("Got response code %d, expected 400", w.Code)
	}
}

func TestHandler_MembershipDeleteFailure(t *testing.T) {
	w := httptest.NewRecorder()
	r := &http.Request{Method: "DELETE", URL: &url.URL{RawQuery: "peer=abc"}}
	handler := rafthttp.NewHandler()

	// Spawn a memberships request handler that fails when trying
	// to leave the cluster.
	done := make(chan bool)
	go func() {
		request := <-handler.MembershipChangeRequests()
		if request.Peer() != "abc" {
			t.Errorf("unexpected peer: %s", request.Peer())
		}
		if kind := request.Kind(); kind != raftmembership.LeaveRequest {
			t.Errorf("request kind is not leave: %s", kind)
		}
		request.Done(fmt.Errorf("boom"))
		done <- true
	}()
	handler.ServeHTTP(w, r)
	if !<-done {
		t.Fatalf("memberships channel not triggered")
	}

	responseCheck(t, w, http.StatusForbidden, "failed to leave peer abc: boom\n")
}

func TestHandler_MembershipPostSuccess(t *testing.T) {
	w := httptest.NewRecorder()
	r := &http.Request{Method: "POST", URL: &url.URL{RawQuery: "peer=abc"}}
	handler := rafthttp.NewHandler()

	// Spawn a memberships request handler that succeeds when trying
	// to join the cluster.
	done := make(chan bool)
	go func() {
		request := <-handler.MembershipChangeRequests()
		if request.Peer() != "abc" {
			t.Errorf("unexpected peer: %s", request.Peer())
		}
		if kind := request.Kind(); kind != raftmembership.JoinRequest {
			t.Errorf("request kind is not join: %s", kind)
		}
		request.Done(nil)
		done <- true
	}()
	handler.ServeHTTP(w, r)
	if !<-done {
		t.Fatalf("memberships channel not triggered")
	}

	responseCheck(t, w, http.StatusOK, "")
}

func TestHandler_DifferentLeader(t *testing.T) {
	w := httptest.NewRecorder()
	r := &http.Request{Method: "POST", URL: &url.URL{RawQuery: "peer=abc"}}
	handler := rafthttp.NewHandler()

	// Spawn a memberships request handler that fails because the
	// node is not the leader and points to the currently known
	// leader.
	go func() {
		request := <-handler.MembershipChangeRequests()
		request.Done(&raftmembership.ErrDifferentLeader{})
	}()

	handler.ServeHTTP(w, r)

	responseCheck(t, w, http.StatusPermanentRedirect, "")
}

func TestHandler_UnknownLeader(t *testing.T) {
	w := httptest.NewRecorder()
	r := &http.Request{Method: "POST", URL: &url.URL{RawQuery: "peer=abc"}}
	handler := rafthttp.NewHandler()

	// Spawn a memberships request handler that fails because the
	// node is not the leader and also does not know who the current
	// leader is.
	go func() {
		request := <-handler.MembershipChangeRequests()
		request.Done(&raftmembership.ErrUnknownLeader{})
	}()
	handler.ServeHTTP(w, r)

	responseCheck(t, w, http.StatusServiceUnavailable,
		"failed to join peer abc: node is not leader, current leader unknown\n")
}

func TestHandler_UnsupportedMethod(t *testing.T) {
	w := httptest.NewRecorder()

	// Use an unsupported HTTP method.
	r := &http.Request{Method: "PATCH"}

	handler := rafthttp.NewHandler()
	handler.ServeHTTP(w, r)

	responseCheck(t, w, http.StatusMethodNotAllowed, "unknown action\n")
}

// A test response implementing the http.Hijacker interface.
type responseHijacker struct {
	httptest.ResponseRecorder
	HijackError error
	HijackConn  net.Conn
}

func newResponseHijacker() *responseHijacker {
	return &responseHijacker{
		ResponseRecorder: *httptest.NewRecorder(),
	}
}

func (r *responseHijacker) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return r.HijackConn, nil, r.HijackError
}

// Check that a response matches the given HTTP code and body.
func responseCheck(t *testing.T, w *httptest.ResponseRecorder, code int, body string) {
	if w.Code != code {
		t.Errorf("response code mismatch:\nwant: %d\n got: %d", w.Code, code)
	}
	if w.Body.String() != body {
		t.Errorf("response body mismatch:\nwant: '%s'\n got: '%s'", w.Body.String(), body)
	}
}
