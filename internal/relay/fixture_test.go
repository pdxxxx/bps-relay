package relay

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Opt-in test process for the plugin's real Bun WebSocket client. Production
// builds cannot enable the arbitrary-origin test fixture.
func TestProtocolFixture(t *testing.T) {
	path := os.Getenv("BPS_RELAY_TEST_FIXTURE")
	if path == "" {
		t.Skip("integration fixture not requested")
	}
	_, ws, origin := testRelay(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/slow" {
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(200)
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			return
		}
		if r.URL.Path == "/large-response" {
			buf := make([]byte, Chunk)
			for i := 0; i < 4160; i++ {
				if _, e := w.Write(buf); e != nil {
					return
				}
			}
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = io.Copy(w, r.Body)
	})
	data, _ := json.Marshal(map[string]string{"ws": ws, "origin": origin, "token": testToken})
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(60 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			t.Fatal("fixture timeout")
		case <-ticker.C:
			if _, err := os.Stat(filepath.Clean(path) + ".done"); err == nil {
				return
			}
		}
	}
}
