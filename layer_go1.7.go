// Version-specific tweaks for Go 1.7

// +build go1.7,!go1.8

package rafthttp

import (
	"net/http"
	"net/url"
)

const (
	// StatusPermanentRedirect has been introduced in Go 1.7
	StatusPermanentRedirect = http.StatusPermanentRedirect
)

// Poor man's redirect support, since 308 proper behavior was only
// added in Go 1.8 https://go-review.googlesource.com/c/29852/
func httpClientDo(client *http.Client, request *http.Request) (*http.Response, error) {
	var response *http.Response
	var err error
	for i := 0; i < 5; i++ {
		response, err = client.Do(request)
		if err != nil {
			break
		}
		if response.StatusCode == http.StatusOK {
			break
		}
		// The only non-200 status code we handle is 308.
		if response.StatusCode != StatusPermanentRedirect {
			break
		}
		if request.URL, err = url.Parse(response.Header.Get("Location")); err != nil {
			break
		}

	}
	return response, err
}
