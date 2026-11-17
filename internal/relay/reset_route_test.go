package relay

import "testing"

func TestResetConsumeRouteIsExact(t *testing.T) {
	endpoint := "https://chatgpt.com/backend-api/wham/rate-limit-reset-credits/consume"
	if !allowedTarget("POST", endpoint) {
		t.Fatal("reset POST must be forwarded")
	}
	for _, method := range []string{"GET", "PUT", "DELETE"} {
		if allowedTarget(method, endpoint) {
			t.Fatalf("unexpected method %s", method)
		}
	}
	for _, url := range []string{endpoint + "?retry=1", endpoint + "/", "https://chatgpt.com/backend-api/wham/other/consume", "https://chatgpt.com.evil.invalid/backend-api/wham/rate-limit-reset-credits/consume"} {
		if allowedTarget("POST", url) {
			t.Fatal("unexpected reset target")
		}
	}
}
