// Version-specific tweaks for Go 1.7

// +build go1.8

package rafthttp

import (
	"net/http"
)

const (
	// StatusPermanentRedirect has been introduced in Go 1.7
	StatusPermanentRedirect = http.StatusPermanentRedirect
)

// Support for redirecto of 308 was added in Go 1.8.
func httpClientDo(client *http.Client, request *http.Request) (*http.Response, error) {
	return client.Do(request)
}
