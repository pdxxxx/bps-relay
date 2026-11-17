package relay

import (
	"errors"
	"net/http"
	"net/url"
	"strings"
)

// The service is a fixed API relay, never a general URL proxy.
func allowedTarget(method, raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.User != nil || u.Fragment != "" || u.Opaque != "" || u.RawPath != "" || u.Host != u.Hostname() {
		return false
	}
	switch u.Host {
	case "bps.openai.com":
		if method == "POST" && (u.Path == "/basispoints/api/responses" || u.Path == "/basispoints/api/attachments") {
			return u.RawQuery == ""
		}
		return method == "GET" && u.Path == "/basispoints/api/responses/access" && (u.RawQuery == "include_models=true" || u.RawQuery == "")
	case "chatgpt.com":
		if method == "POST" && u.Path == "/backend-api/wham/rate-limit-reset-credits/consume" {
			return u.RawQuery == ""
		}
		return method == "GET" && (u.Path == "/backend-api/wham/usage" || u.Path == "/backend-api/wham/rate-limit-reset-credits") && u.RawQuery == ""
	case "auth.openai.com":
		return method == "POST" && u.Path == "/oauth/token" && u.RawQuery == ""
	}
	return false
}

func requestHeaders(values map[string]string) (http.Header, error) {
	h := make(http.Header)
	if len(values) > 40 {
		return nil, errors.New("invalid_headers")
	}
	total := 0
	for name, value := range values {
		total += len(name) + len(value)
		if total > 32768 || strings.ContainsAny(name+value, "\r\n") {
			return nil, errors.New("invalid_headers")
		}
		key := strings.ToLower(name)
		switch key {
		case "authorization", "chatgpt-account-id", "x-openai-account-id", "x-openai-account-user-id", "x-basispoints-auth-mode", "content-type", "accept", "origin", "user-agent", "accept-encoding", "x-openai-internal-basispoints-client-product", "x-openai-internal-basispoints-client-agent-profile", "openai-beta", "oai-language", "originator", "sec-fetch-site", "sec-fetch-mode", "sec-fetch-dest", "priority":
			h.Set(name, value)
		default:
			return nil, errors.New("unsupported_header")
		}
	}
	return h, nil
}

func responseHeaders(in http.Header) map[string]string {
	out := make(map[string]string)
	for _, key := range []string{"Content-Type", "Content-Encoding", "Cache-Control", "Retry-After", "X-Request-Id", "Openai-Processing-Ms", "X-Ratelimit-Remaining-Requests", "X-Ratelimit-Reset-Requests"} {
		if value := in.Get(key); value != "" && len(value) < 8192 {
			out[strings.ToLower(key)] = value
		}
	}
	return out
}
