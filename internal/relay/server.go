package relay

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	Window = 256 << 10
	Chunk  = 32 << 10
)

type frame struct {
	Type    string            `json:"type"`
	ID      uint64            `json:"id,omitempty"`
	URL     string            `json:"url,omitempty"`
	Method  string            `json:"method,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	Length  int64             `json:"length,omitempty"`
	Data    string            `json:"data,omitempty"`
	Bytes   int               `json:"bytes,omitempty"`
	Status  int               `json:"status,omitempty"`
	Code    string            `json:"code,omitempty"`
}

type Server struct {
	token       string
	client      *http.Client
	allow       func(string, string) bool
	mu          sync.Mutex
	connections map[*websocket.Conn]bool
}

func New(token string) (*Server, error) {
	if len(token) < 32 || len(token) > 256 || strings.ContainsAny(token, "\r\n \t") {
		return nil, errors.New("relay token must contain 32-256 non-whitespace characters")
	}
	transport := &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: 60 * time.Second, KeepAlive: 30 * time.Second}).DialContext, ForceAttemptHTTP2: true, MaxIdleConns: 128, MaxIdleConnsPerHost: 64, IdleConnTimeout: 5 * time.Minute, TLSHandshakeTimeout: 60 * time.Second, DisableCompression: true}
	return &Server{token: token, client: &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, allow: allowedTarget, connections: make(map[*websocket.Conn]bool)}, nil
}

func (s *Server) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for c := range s.connections {
		_ = c.Close()
	}
	s.client.CloseIdleConnections()
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" && r.Method == "GET" {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true,"protocol":1}`)
		return
	}
	if r.URL.Path != "/relay" || r.Method != "GET" || r.URL.RawQuery != "" {
		http.NotFound(w, r)
		return
	}
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+s.token)) != 1 {
		http.Error(w, "unauthorized", 401)
		return
	}
	if r.Header.Get("Origin") != "" {
		http.Error(w, "browser origins are unsupported", 403)
		return
	}
	u := websocket.Upgrader{ReadBufferSize: 4096, WriteBufferSize: 4096, HandshakeTimeout: 60 * time.Second, CheckOrigin: func(*http.Request) bool { return true }}
	conn, err := u.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	s.mu.Lock()
	s.connections[conn] = true
	s.mu.Unlock()
	defer func() { _ = conn.Close(); s.mu.Lock(); delete(s.connections, conn); s.mu.Unlock() }()
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	p := &peer{server: s, conn: conn, ctx: ctx, cancel: cancel, requests: make(map[uint64]*request)}
	defer p.closeRequests()
	conn.SetReadLimit(128 << 10)
	_ = conn.SetReadDeadline(time.Now().Add(180 * time.Second))
	conn.SetPongHandler(func(string) error { return conn.SetReadDeadline(time.Now().Add(180 * time.Second)) })
	if p.send(frame{Type: "hello", Bytes: Window, Status: 1}) != nil {
		return
	}
	go func() {
		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(60*time.Second)) != nil {
					cancel()
					_ = conn.Close()
					return
				}
			}
		}
	}()
	for {
		kind, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(180 * time.Second))
		if kind != websocket.TextMessage {
			return
		}
		var f frame
		if json.Unmarshal(data, &f) != nil {
			return
		}
		if f.Type == "ping" {
			if p.send(frame{Type: "pong"}) != nil {
				return
			}
			continue
		}
		if f.ID == 0 || f.ID > 9007199254740991 {
			return
		}
		if f.Type == "start" {
			if !p.start(f) {
				return
			}
			continue
		}
		p.mu.Lock()
		req := p.requests[f.ID]
		p.mu.Unlock()
		if req == nil {
			continue
		}
		switch f.Type {
		case "upload":
			req.upload(f.Data)
		case "uploaded":
			req.begin()
		case "credit":
			if f.Bytes < 1 || f.Bytes > Window {
				return
			}
			req.addCredit(f.Bytes)
		case "cancel":
			req.cancel()
		default:
			return
		}
	}
}

type peer struct {
	server   *Server
	conn     *websocket.Conn
	ctx      context.Context
	cancel   context.CancelFunc
	write    sync.Mutex
	mu       sync.Mutex
	requests map[uint64]*request
	lastID   uint64
}

func (p *peer) send(f frame) error {
	p.write.Lock()
	defer p.write.Unlock()
	_ = p.conn.SetWriteDeadline(time.Now().Add(180 * time.Second))
	err := p.conn.WriteJSON(f)
	if err != nil {
		p.cancel()
		_ = p.conn.Close()
	}
	return err
}
func (p *peer) closeRequests() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, r := range p.requests {
		r.cancel()
	}
}
func (p *peer) start(f frame) bool {
	if f.ID <= p.lastID {
		return false
	}
	p.lastID = f.ID
	if !p.server.allow(f.Method, f.URL) || f.Length < 0 || (f.Method == "GET" && f.Length != 0) {
		_ = p.send(frame{Type: "error", ID: f.ID, Code: "invalid_request"})
		return true
	}
	headers, err := requestHeaders(f.Headers)
	if err != nil {
		_ = p.send(frame{Type: "error", ID: f.ID, Code: "invalid_headers"})
		return true
	}
	file, err := os.CreateTemp("", "bps-relay-*")
	if err != nil {
		_ = p.send(frame{Type: "error", ID: f.ID, Code: "unavailable"})
		return true
	}
	ctx, cancel := context.WithCancel(p.ctx)
	r := &request{peer: p, id: f.ID, method: f.Method, url: f.URL, headers: headers, expected: f.Length, file: file, ctx: ctx, cancel: cancel, credit: Window, wake: make(chan struct{}, 1), done: make(chan struct{})}
	p.mu.Lock()
	p.requests[f.ID] = r
	p.mu.Unlock()
	go func() {
		select {
		case <-ctx.Done():
			r.fail("cancelled")
		case <-r.done:
		}
	}()
	_ = p.send(frame{Type: "ready", ID: f.ID, Bytes: Window})
	return true
}

type request struct {
	peer               *peer
	id                 uint64
	method, url        string
	headers            http.Header
	expected, received int64
	file               *os.File
	ctx                context.Context
	cancel             context.CancelFunc
	mu                 sync.Mutex
	begun, finished    bool
	credit             int
	wake               chan struct{}
	once               sync.Once
	done               chan struct{}
}

func (r *request) finish() {
	r.terminate(nil)
}
func (r *request) terminate(terminal *frame) {
	r.once.Do(func() {
		r.mu.Lock()
		r.finished = true
		r.headers = nil
		_ = r.file.Close()
		_ = os.Remove(r.file.Name())
		r.mu.Unlock()
		r.cancel()
		if terminal != nil && r.peer.ctx.Err() == nil {
			_ = r.peer.send(*terminal)
		}
		r.peer.mu.Lock()
		delete(r.peer.requests, r.id)
		r.peer.mu.Unlock()
		close(r.done)
	})
}
func (r *request) fail(code string) {
	r.terminate(&frame{Type: "error", ID: r.id, Code: code})
}
func (r *request) upload(encoded string) {
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || len(data) == 0 || len(data) > Chunk {
		r.fail("invalid_upload")
		return
	}
	r.mu.Lock()
	if r.finished || r.begun || r.received+int64(len(data)) > r.expected {
		r.mu.Unlock()
		r.fail("invalid_upload")
		return
	}
	_, err = r.file.Write(data)
	r.received += int64(len(data))
	r.mu.Unlock()
	if err != nil {
		r.fail("upload_failed")
		return
	}
	_ = r.peer.send(frame{Type: "upload_credit", ID: r.id, Bytes: len(data)})
}
func (r *request) begin() {
	r.mu.Lock()
	if r.finished || r.begun || r.received != r.expected {
		r.mu.Unlock()
		r.fail("invalid_upload")
		return
	}
	r.begun = true
	_, err := r.file.Seek(0, 0)
	r.mu.Unlock()
	if err != nil {
		r.fail("upload_failed")
		return
	}
	go r.forward()
}
func (r *request) addCredit(n int) {
	r.mu.Lock()
	r.credit += n
	bad := r.credit > Window
	r.mu.Unlock()
	if bad {
		r.cancel()
		return
	}
	select {
	case r.wake <- struct{}{}:
	default:
	}
}
func (r *request) take(max int) (int, error) {
	for {
		r.mu.Lock()
		if r.credit > 0 {
			n := min(r.credit, max)
			r.credit -= n
			r.mu.Unlock()
			return n, nil
		}
		r.mu.Unlock()
		select {
		case <-r.ctx.Done():
			return 0, r.ctx.Err()
		case <-r.wake:
		}
	}
}
func (r *request) forward() {
	defer r.finish()
	r.mu.Lock()
	if r.finished {
		r.mu.Unlock()
		return
	}
	headers := r.headers
	r.headers = nil
	r.mu.Unlock()
	var body io.Reader
	if r.expected > 0 {
		body = io.NewSectionReader(r.file, 0, r.expected)
	}
	req, err := http.NewRequestWithContext(r.ctx, r.method, r.url, body)
	if err != nil {
		r.fail("invalid_request")
		return
	}
	req.Header = headers
	req.ContentLength = r.expected
	// A non-replayable body and no idempotency headers prevent automatic POST
	// replay. HTTP redirects and network retries are never performed here.
	res, err := r.peer.server.client.Do(req)
	if err != nil {
		r.fail("upstream_transport")
		return
	}
	defer res.Body.Close()
	if r.peer.send(frame{Type: "head", ID: r.id, Status: res.StatusCode, Headers: responseHeaders(res.Header)}) != nil {
		return
	}
	buf := make([]byte, Chunk)
	for {
		n, err := res.Body.Read(buf)
		if n > 0 {
			for off := 0; off < n; {
				size, e := r.take(n - off)
				if e != nil {
					return
				}
				if r.peer.send(frame{Type: "data", ID: r.id, Data: base64.StdEncoding.EncodeToString(buf[off : off+size])}) != nil {
					return
				}
				off += size
			}
		}
		if err == io.EOF {
			r.terminate(&frame{Type: "end", ID: r.id})
			return
		}
		if err != nil {
			r.fail("upstream_transport")
			return
		}
	}
}
