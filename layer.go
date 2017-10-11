package rafthttp

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/CanonicalLtd/raft-membership"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

// NewLayer returns a new raft stream layer that initiates connections
// with HTTP and then uses Upgrade to switch them into raw TCP.
func NewLayer(path string, localAddr net.Addr, handler *Handler, dial Dial) *Layer {
	return &Layer{
		path:      path,
		localAddr: localAddr,
		handler:   handler,
		dial:      dial,
	}
}

// Layer represents the connection between raft nodes.
type Layer struct {
	path      string
	localAddr net.Addr
	handler   *Handler
	dial      Dial
}

// Accept waits for the next connection.
func (l *Layer) Accept() (net.Conn, error) {
	conn, more := <-l.handler.connections
	if !more {
		return nil, io.EOF
	}
	return conn, nil
}

// Close closes the layer.
func (l *Layer) Close() error {
	l.handler.Close()
	return nil
}

// Addr returns the local address for the layer.
func (l *Layer) Addr() net.Addr {
	return l.localAddr
}

// Dial creates a new network connection.
func (l *Layer) Dial(addr raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	url := l.url()
	request := &http.Request{
		Method:     "GET",
		URL:        url,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header),
		Host:       url.Host,
	}
	request.Header.Set("Upgrade", "raft")

	conn, err := l.dial(string(addr), timeout)
	if err != nil {
		return nil, errors.Wrap(err, "dialing failed")
	}

	if err := request.Write(conn); err != nil {
		return nil, errors.Wrap(err, "sending HTTP request failed")
	}

	response, err := http.ReadResponse(bufio.NewReader(conn), request)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read response")
	}
	if response.StatusCode != http.StatusSwitchingProtocols {
		return nil, fmt.Errorf("dialing fail: expected status code 101 got %d", response.StatusCode)
	}
	if response.Header.Get("Upgrade") != "raft" {
		return nil, fmt.Errorf("missing or unexpected Upgrade header in response")
	}
	return conn, err
}

// Join tries to join the cluster by contacting the leader at the given
// address. The raft node associated with this layer must have the given server
// identity.
func (l *Layer) Join(id raft.ServerID, addr raft.ServerAddress, timeout time.Duration) error {
	return l.changeMemberhip(raftmembership.JoinRequest, id, addr, timeout)
}

// Leave tries to leave the cluster by contacting the leader at the given
// address.  The raft node associated with this layer must have the given
// server identity.
func (l *Layer) Leave(id raft.ServerID, addr raft.ServerAddress, timeout time.Duration) error {
	return l.changeMemberhip(raftmembership.LeaveRequest, id, addr, timeout)
}

// Build a full url.URL object out of our path.
func (l *Layer) url() *url.URL {
	url, err := url.Parse(l.path)
	if err != nil {
		panic(fmt.Sprintf("invalid URL path %s", l.path))
	}
	return url
}

// Change the membership of the server associated with this layer.
func (l *Layer) changeMemberhip(kind raftmembership.ChangeRequestKind, id raft.ServerID, addr raft.ServerAddress, timeout time.Duration) error {
	url := l.url()
	url.RawQuery = fmt.Sprintf("id=%s", id)
	if kind == raftmembership.JoinRequest {
		url.RawQuery += fmt.Sprintf("&address=%s", l.Addr().String())
	}
	url.Host = string(addr)
	url.Scheme = "http"
	method := membershipChangeRequestKindToMethod[kind]
	request := &http.Request{
		Method:     method,
		URL:        url,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header),
	}

	remaining := timeout
	var response *http.Response
	var err error
	for remaining > 0 {
		start := time.Now()
		dial := func(network, addr string) (net.Conn, error) {
			return l.dial(addr, remaining)
		}
		client := &http.Client{
			Timeout:   remaining,
			Transport: &http.Transport{Dial: dial},
		}
		response, err = httpClientDo(client, request)

		// If we got a system or network error, just return it.
		if err != nil {
			break
		}

		// If we got an HTTP error, let's capture its details,
		// and possibly return it if it's not retriable or we
		// have hit our timeout.
		if response.StatusCode != http.StatusOK {
			body, _ := ioutil.ReadAll(response.Body)
			err = fmt.Errorf(
				"http code %d '%s'", response.StatusCode,
				strings.TrimSpace(string(body)))
		}
		// If there's a temporary failure, let's retry.
		if response.StatusCode == http.StatusServiceUnavailable {
			// XXX TODO: use an exponential backoff
			// relative to the timeout?
			time.Sleep(100 * time.Millisecond)

			remaining -= time.Since(start)
			continue
		}

		break
	}
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("server %s failed", kind))
	}
	return nil
}

// Map a membership ChangeRequest kind code to an HTTP method name.
var membershipChangeRequestKindToMethod = map[raftmembership.ChangeRequestKind]string{
	raftmembership.JoinRequest:  "POST",
	raftmembership.LeaveRequest: "DELETE",
}
