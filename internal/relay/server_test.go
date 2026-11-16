package relay

import (
	"bytes"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

const testToken = "test-relay-token-012345678901234567890"

func testRelay(t *testing.T, handler http.HandlerFunc) (*Server, string, string) {
	t.Helper()
	origin := httptest.NewServer(handler)
	t.Cleanup(origin.Close)
	s, _ := New(testToken)
	s.allow = func(m, u string) bool { return strings.HasPrefix(u, origin.URL+"/") }
	server := httptest.NewServer(s)
	t.Cleanup(func() { s.Close(); server.Close() })
	return s, "ws" + strings.TrimPrefix(server.URL, "http") + "/relay", origin.URL
}
func dial(t *testing.T, url string) *websocket.Conn {
	t.Helper()
	c, _, e := websocket.DefaultDialer.Dial(url, http.Header{"Authorization": []string{"Bearer " + testToken}})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { c.Close() })
	var f frame
	if e = c.ReadJSON(&f); e != nil || f.Type != "hello" {
		t.Fatalf("hello %v %v", f, e)
	}
	return c
}
func write(t *testing.T, c *websocket.Conn, f frame) {
	t.Helper()
	if e := c.WriteJSON(f); e != nil {
		t.Fatal(e)
	}
}
func read(t *testing.T, c *websocket.Conn) frame {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	var f frame
	if e := c.ReadJSON(&f); e != nil {
		t.Fatal(e)
	}
	return f
}
func start(t *testing.T, c *websocket.Conn, id uint64, url, auth string, body []byte) {
	write(t, c, frame{Type: "start", ID: id, URL: url, Method: "POST", Length: int64(len(body)), Headers: map[string]string{"authorization": auth, "content-type": "application/json"}})
	if f := read(t, c); f.Type != "ready" {
		t.Fatal(f)
	}
	for off := 0; off < len(body); off += Chunk {
		end := min(len(body), off+Chunk)
		write(t, c, frame{Type: "upload", ID: id, Data: base64.StdEncoding.EncodeToString(body[off:end])})
		if f := read(t, c); f.Type != "upload_credit" {
			t.Fatal(f)
		}
	}
	write(t, c, frame{Type: "uploaded", ID: id})
}
func drain(t *testing.T, c *websocket.Conn, id uint64) (int, []byte) {
	t.Helper()
	var out []byte
	status := 0
	for {
		f := read(t, c)
		if f.ID != id {
			t.Fatal(f)
		}
		switch f.Type {
		case "head":
			status = f.Status
		case "data":
			b, e := base64.StdEncoding.DecodeString(f.Data)
			if e != nil {
				t.Fatal(e)
			}
			out = append(out, b...)
			write(t, c, frame{Type: "credit", ID: id, Bytes: len(b)})
		case "end":
			return status, out
		default:
			t.Fatal(f)
		}
	}
}

func TestRouteAndHeaders(t *testing.T) {
	for _, u := range []string{"https://127.0.0.1/", "https://bps.openai.com.evil.test/basispoints/api/responses", "https://bps.openai.com:443/basispoints/api/responses", "http://bps.openai.com/basispoints/api/responses", "https://auth.openai.com/oauth/token?x=1", "https://bps.openai.com/basispoints/api/responses#x"} {
		if allowedTarget("POST", u) {
			t.Fatal(u)
		}
	}
	if !allowedTarget("POST", "https://bps.openai.com/basispoints/api/responses") {
		t.Fatal("allowed target rejected")
	}
	for _, name := range []string{"proxy-authorization", "host", "cookie", "connection"} {
		if _, err := requestHeaders(map[string]string{name: "PRIVATE"}); err == nil {
			t.Fatal(name)
		}
	}
}
func TestAuthAndOrigin(t *testing.T) {
	_, url, _ := testRelay(t, func(http.ResponseWriter, *http.Request) { t.Fatal("upstream called") })
	for _, headers := range []http.Header{{}, {"Authorization": []string{"Bearer wrong"}}, {"Authorization": []string{"Bearer " + testToken}, "Origin": []string{"https://browser.invalid"}}} {
		c, r, e := websocket.DefaultDialer.Dial(url, headers)
		if c != nil {
			c.Close()
		}
		if e == nil || r == nil || (r.StatusCode != 401 && r.StatusCode != 403) {
			t.Fatal("unauthorized upgrade")
		}
	}
}
func TestPreservesBodyStatusAndReuse(t *testing.T) {
	var calls atomic.Int32
	_, url, origin := testRelay(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		b, _ := io.ReadAll(r.Body)
		if r.Header.Get("Authorization") != "Bearer private-test" {
			t.Error("wrong identity")
		}
		w.Header().Set("Retry-After", "7")
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(403)
		_, _ = w.Write(b)
	})
	c := dial(t, url)
	payload := bytes.Repeat([]byte("utf8-图片\n"), 40000)
	for i := uint64(1); i <= 3; i++ {
		start(t, c, i, origin+"/responses", "Bearer private-test", payload)
		status, got := drain(t, c, i)
		if status != 403 || !bytes.Equal(got, payload) {
			t.Fatal("changed response")
		}
	}
	if calls.Load() != 3 {
		t.Fatal(calls.Load())
	}
}
func TestMultipleComputersHaveNoGlobalFourLimit(t *testing.T) {
	arrived := make(chan string, 12)
	release := make(chan struct{})
	_, url, origin := testRelay(t, func(w http.ResponseWriter, r *http.Request) {
		arrived <- r.Header.Get("Authorization")
		select {
		case <-release:
			_, _ = w.Write([]byte(r.Header.Get("Authorization")))
		case <-r.Context().Done():
		}
	})
	clients := make([]*websocket.Conn, 12)
	for i := range clients {
		clients[i] = dial(t, url)
		identity := "same-account"
		if i%2 == 1 {
			identity = "other-account"
		}
		start(t, clients[i], 1, origin+"/responses", identity, nil)
	}
	for range clients {
		select {
		case <-arrived:
		case <-time.After(3 * time.Second):
			close(release)
			t.Fatal("server imposed a concurrency limit")
		}
	}
	// Disconnect one computer; the remaining requests still finish.
	clients[0].Close()
	close(release)
	for i, c := range clients {
		if i == 0 {
			continue
		}
		_, got := drain(t, c, 1)
		want := "same-account"
		if i%2 == 1 {
			want = "other-account"
		}
		if string(got) != want {
			t.Fatal("cross-account response")
		}
	}
}
func TestLargeDeclaredUploadAndCancellation(t *testing.T) {
	var calls atomic.Int32
	_, url, origin := testRelay(t, func(http.ResponseWriter, *http.Request) { calls.Add(1) })
	c := dial(t, url)
	write(t, c, frame{Type: "start", ID: 1, URL: origin + "/responses", Method: "POST", Length: 2 << 30})
	if f := read(t, c); f.Type != "ready" {
		t.Fatal(f)
	}
	write(t, c, frame{Type: "cancel", ID: 1})
	if f := read(t, c); f.Type != "error" || f.Code != "cancelled" {
		t.Fatal(f)
	}
	if calls.Load() != 0 {
		t.Fatal("partial upload forwarded")
	}
}
func TestFlowControlAndCancel(t *testing.T) {
	cancelled := make(chan struct{})
	_, url, origin := testRelay(t, func(w http.ResponseWriter, r *http.Request) {
		defer close(cancelled)
		buf := bytes.Repeat([]byte("x"), Chunk)
		for i := 0; i < 128; i++ {
			if _, err := w.Write(buf); err != nil {
				return
			}
			w.(http.Flusher).Flush()
		}
		<-r.Context().Done()
	})
	c := dial(t, url)
	start(t, c, 1, origin+"/responses", "test", nil)
	total := 0
	for total < Window {
		f := read(t, c)
		if f.Type == "data" {
			b, _ := base64.StdEncoding.DecodeString(f.Data)
			total += len(b)
		} else if f.Type != "head" {
			t.Fatal(f)
		}
	}
	if total != Window {
		t.Fatal("flow window exceeded")
	}
	write(t, c, frame{Type: "cancel", ID: 1})
	select {
	case <-cancelled:
	case <-time.After(3 * time.Second):
		t.Fatal("cancel did not reach upstream")
	}
}

func TestOneConnectionMultiplexesBeyondFourWithoutIdentityMixing(t *testing.T) {
	arrived := make(chan struct{}, 10)
	release := make(chan struct{})
	_, url, origin := testRelay(t, func(w http.ResponseWriter, r *http.Request) {
		arrived <- struct{}{}
		select {
		case <-release:
			_, _ = w.Write([]byte(r.Header.Get("Authorization")))
		case <-r.Context().Done():
		}
	})
	c := dial(t, url)
	for id := uint64(1); id <= 10; id++ {
		start(t, c, id, origin+"/responses", "Bearer "+string(rune('a'+id)), nil)
	}
	for i := 0; i < 10; i++ {
		select {
		case <-arrived:
		case <-time.After(3 * time.Second):
			close(release)
			t.Fatal("per-connection limit")
		}
	}
	close(release)
	output := make(map[uint64][]byte)
	ended := make(map[uint64]bool)
	for len(ended) < 10 {
		f := read(t, c)
		switch f.Type {
		case "head":
			if f.Status != 200 {
				t.Fatal(f)
			}
		case "data":
			b, err := base64.StdEncoding.DecodeString(f.Data)
			if err != nil {
				t.Fatal(err)
			}
			output[f.ID] = append(output[f.ID], b...)
			write(t, c, frame{Type: "credit", ID: f.ID, Bytes: len(b)})
		case "end":
			ended[f.ID] = true
		default:
			t.Fatal(f)
		}
	}
	for id, body := range output {
		if string(body) != "Bearer "+string(rune('a'+id)) {
			t.Fatal("request identities crossed")
		}
	}
}
