package main

import "net/http"

var _ http.RoundTripper = &gitHubAdvancedSearchModder{}

type gitHubAdvancedSearchModder struct {
	base http.RoundTripper
}

func (g *gitHubAdvancedSearchModder) RoundTrip(req *http.Request) (*http.Response, error) {
	if req != nil && req.URL != nil {
		if req.URL.Path == "/search/issues" {
			values := req.URL.Query()
			values.Set("advanced_search", "true")
			req.URL.RawQuery = values.Encode()
		}
	}
	return g.base.RoundTrip(req)
}
