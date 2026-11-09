package rpc

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWebUIEndpoints(t *testing.T) {
	env := newEnv(t, Options{})
	h := env.server.Handler()

	tests := []struct {
		name         string
		url          string
		expectedCode int
		expectedType string
		bodyContains string
	}{
		{
			name:         "Root index page",
			url:          "http://127.0.0.1:9091/",
			expectedCode: http.StatusOK,
			expectedType: "text/html",
			bodyContains: "get-torrent | Transmission Web Control",
		},
		{
			name:         "WebUI index slash",
			url:          "http://127.0.0.1:9091/transmission/web/",
			expectedCode: http.StatusOK,
			expectedType: "text/html",
			bodyContains: "app-container",
		},
		{
			name:         "WebUI stylesheet",
			url:          "http://127.0.0.1:9091/transmission/web/style.css",
			expectedCode: http.StatusOK,
			expectedType: "text/css",
			bodyContains: "--bg-primary",
		},
		{
			name:         "WebUI javascript",
			url:          "http://127.0.0.1:9091/transmission/web/app.js",
			expectedCode: http.StatusOK,
			expectedType: "text/javascript",
			bodyContains: "X-Transmission-Session-Id",
		},
		{
			name:         "WebUI redirect without slash",
			url:          "http://127.0.0.1:9091/transmission/web",
			expectedCode: http.StatusMovedPermanently,
			expectedType: "",
			bodyContains: "/transmission/web/",
		},
		{
			name:         "WebUI favicon svg",
			url:          "http://127.0.0.1:9091/transmission/web/favicon.svg",
			expectedCode: http.StatusOK,
			expectedType: "image/svg+xml",
			bodyContains: "<svg",
		},
		{
			name:         "Root favicon ico redirect",
			url:          "http://127.0.0.1:9091/favicon.ico",
			expectedCode: http.StatusMovedPermanently,
			expectedType: "",
			bodyContains: "/transmission/web/favicon.svg",
		},
		{
			name:         "NotFound for unknown subpath",
			url:          "http://127.0.0.1:9091/nonexistent",
			expectedCode: http.StatusNotFound,
			expectedType: "",
			bodyContains: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.url, nil)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)

			if w.Code != tc.expectedCode {
				t.Fatalf("expected status %d, got %d", tc.expectedCode, w.Code)
			}
			if tc.expectedType != "" {
				contentType := w.Header().Get("Content-Type")
				if !strings.HasPrefix(contentType, tc.expectedType) {
					t.Errorf("expected Content-Type prefix %q, got %q", tc.expectedType, contentType)
				}
			}
			if tc.bodyContains != "" {
				body := w.Body.String()
				if !strings.Contains(body, tc.bodyContains) {
					t.Errorf("expected body to contain %q, got %q", tc.bodyContains, body)
				}
			}
		})
	}
}

func TestWebUIAuthorization(t *testing.T) {
	env := newEnv(t, Options{
		Username: "admin",
		Password: "secretpassword",
	})
	h := env.server.Handler()

	// 1. Unauthenticated request must be rejected with 401
	reqUnauth := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:9091/transmission/web/", nil)
	wUnauth := httptest.NewRecorder()
	h.ServeHTTP(wUnauth, reqUnauth)

	if wUnauth.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized, got %d", wUnauth.Code)
	}

	// 2. Authenticated request must succeed with 200
	reqAuth := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:9091/transmission/web/", nil)
	reqAuth.SetBasicAuth("admin", "secretpassword")
	wAuth := httptest.NewRecorder()
	h.ServeHTTP(wAuth, reqAuth)

	if wAuth.Code != http.StatusOK {
		t.Errorf("expected 200 OK with valid credentials, got %d", wAuth.Code)
	}
}
