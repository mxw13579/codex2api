package proxy

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/textproto"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codex2api/auth"
	"github.com/codex2api/config"
	"github.com/codex2api/database"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

const imagesKeepaliveTestInterval = 10 * time.Millisecond
const imagesKeepaliveTestKeyID int64 = 991037

type imagesKeepaliveFixture struct {
	handler  *Handler
	router   *gin.Engine
	server   *httptest.Server
	accounts []*auth.Account
	done     chan struct{}
}

// newImagesKeepaliveFixture exercises public handlers with local-only upstreams.
// These tests mutate runtime globals and must not use t.Parallel.
func newImagesKeepaliveFixture(t *testing.T, upstreamHandler http.HandlerFunc, policy database.ContinuousRetryPolicy, retries int) *imagesKeepaliveFixture {
	t.Helper()
	gin.SetMode(gin.TestMode)
	previousRuntime := CurrentRuntimeSettings()
	previousResin := resinCfg.Load()
	previousInterval := continuousRetryKeepaliveInterval
	t.Cleanup(func() {
		ApplyRuntimeSettings(previousRuntime)
		resinCfg.Store(previousResin)
		continuousRetryKeepaliveInterval = previousInterval
	})
	settings := DefaultRuntimeSettings()
	settings.CodexImagesMainModel = "gpt-5.4"
	settings.CodexForceWebsocket = false
	settings.CodexRequestCompression = false
	settings.CodexTelemetryEnabled = false
	settings.ContinuousRetryPolicy = policy
	ApplyRuntimeSettings(settings)
	continuousRetryKeepaliveInterval = imagesKeepaliveTestInterval

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		upstreamHandler(w, r)
	}))
	t.Cleanup(upstream.Close)
	SetResinConfig(&ResinConfig{BaseURL: upstream.URL, PlatformName: "images-keepalive-test"})
	store := auth.NewStore(nil, nil, &database.SystemSettings{
		MaxConcurrency: 1, MaxRetries: retries, MaxRateLimitRetries: 0, RetryIntervalMS: 0,
	})
	t.Cleanup(store.Stop)
	account := &auth.Account{DBID: 1, AccessToken: "image-test-token", PlanType: "plus", AccountID: "image-test-account", GroupIDs: []int64{10}}
	store.AddAccount(account)
	handler := NewHandler(store, nil, &config.Config{AllowAnonymousV1: true}, nil)
	fixture := &imagesKeepaliveFixture{handler: handler, accounts: []*auth.Account{account}, done: make(chan struct{}, 32)}
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set(contextAPIKeyID, imagesKeepaliveTestKeyID)
		c.Set(contextAPIKeyRow, &database.APIKeyRow{
			ID: imagesKeepaliveTestKeyID, Limits: database.APIKeyLimits{MaxConcurrency: 1},
		})
		c.Set(contextScopeBudgetGate, newScopeBudgetGate(imagesKeepaliveTestKeyID, []database.APIKeyScopeLimit{
			{ScopeType: database.APIKeyScopeTypeGroup, ScopeID: 10, MaxConcurrency: 2},
		}))
		c.Next()
		fixture.done <- struct{}{}
	})
	handler.RegisterRoutes(router)
	fixture.router = router
	fixture.server = httptest.NewServer(router)
	t.Cleanup(fixture.server.Close)
	return fixture
}

func (f *imagesKeepaliveFixture) assertReleased(t *testing.T) {
	t.Helper()
	select {
	case <-f.done:
	case <-time.After(3 * time.Second):
		t.Fatal("image handler did not return")
	}
	for _, account := range f.accounts {
		if got := account.GetActiveRequests(); got != 0 {
			t.Errorf("account %d still has %d active requests", account.ID(), got)
		}
	}
	if got := f.handler.apiKeyConcurrencyLimiter().counter(imagesKeepaliveTestKeyID).inflight.Load(); got != 0 {
		t.Errorf("API key concurrency still occupied: %d", got)
	}
	if got := APIKeyScopeInflight(imagesKeepaliveTestKeyID, database.APIKeyScopeTypeGroup, 10); got != 0 {
		t.Errorf("scope concurrency still occupied: %d", got)
	}
}

func imageKeepaliveRequest(t *testing.T, baseURL, kind string) *http.Request {
	t.Helper()
	endpoint := "/v1/images/generations"
	contentType := "application/json"
	body := []byte(`{"model":"gpt-image-2","prompt":"draw a test image"}`)
	switch kind {
	case "generations_false":
		body = []byte(`{"model":"gpt-image-2","prompt":"draw a test image","stream":false}`)
	case "generations_stream":
		body = []byte(`{"model":"gpt-image-2","prompt":"draw a test image","stream":true}`)
	case "edits_json":
		endpoint = "/v1/images/edits"
		body = []byte(`{"model":"gpt-image-2","prompt":"edit a test image","stream":false,"images":[{"image_url":"data:image/png;base64,` + tinyPNGBase64 + `"}]}`)
	case "edits_multipart":
		endpoint = "/v1/images/edits"
		var buffer bytes.Buffer
		writer := multipart.NewWriter(&buffer)
		for key, value := range map[string]string{"model": "gpt-image-2", "prompt": "edit a test image"} {
			if err := writer.WriteField(key, value); err != nil {
				t.Fatal(err)
			}
		}
		part, err := writer.CreateFormFile("image[]", "input.png")
		if err != nil {
			t.Fatal(err)
		}
		image, err := base64.StdEncoding.DecodeString(tinyPNGBase64)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = part.Write(image); err != nil {
			t.Fatal(err)
		}
		if err = writer.Close(); err != nil {
			t.Fatal(err)
		}
		body, contentType = buffer.Bytes(), writer.FormDataContentType()
	}
	request, err := http.NewRequest(http.MethodPost, baseURL+endpoint, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", contentType)
	return request
}

func writeKeepaliveImageResult(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/event-stream")
	_, _ = fmt.Fprint(w, `data: {"type":"response.completed","response":{"created_at":1710000000,"output":[{"type":"image_generation_call","result":"`+tinyPNGBase64+`","output_format":"png"}]}}`+"\n\n")
}

type imageKeepaliveHTTPResult struct {
	status      int
	contentType string
	body        []byte
	err         error
}

func startImageKeepaliveRequest(client *http.Client, request *http.Request, informational *atomic.Int32) (<-chan int, <-chan imageKeepaliveHTTPResult) {
	headers := make(chan int, 1)
	result := make(chan imageKeepaliveHTTPResult, 1)
	trace := &httptrace.ClientTrace{Got1xxResponse: func(int, textproto.MIMEHeader) error {
		informational.Add(1)
		return nil
	}}
	request = request.WithContext(httptrace.WithClientTrace(request.Context(), trace))
	go func() {
		response, err := client.Do(request)
		if err != nil {
			result <- imageKeepaliveHTTPResult{err: err}
			return
		}
		defer response.Body.Close()
		headers <- response.StatusCode
		body, err := io.ReadAll(response.Body)
		result <- imageKeepaliveHTTPResult{status: response.StatusCode, contentType: response.Header.Get("Content-Type"), body: body, err: err}
	}()
	return headers, result
}

func awaitImageKeepaliveResult(t *testing.T, result <-chan imageKeepaliveHTTPResult) imageKeepaliveHTTPResult {
	t.Helper()
	select {
	case response := <-result:
		return response
	case <-time.After(5 * time.Second):
		t.Fatal("image response did not complete")
		return imageKeepaliveHTTPResult{}
	}
}

func TestImagesNonStreamingKeepaliveDoesNotSendInformational(t *testing.T) {
	for _, kind := range []string{"generations_default", "generations_false", "edits_json", "edits_multipart"} {
		for _, phase := range []string{"headers", "sse_body", "error_headers", "error_body"} {
			t.Run(kind+"/"+phase, func(t *testing.T) {
				started, release := make(chan struct{}), make(chan struct{})
				var releaseOnce sync.Once
				unblock := func() { releaseOnce.Do(func() { close(release) }) }
				defer unblock()
				fixture := newImagesKeepaliveFixture(t, func(w http.ResponseWriter, r *http.Request) {
					switch phase {
					case "sse_body":
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = io.WriteString(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"image-test\"}}\n\n")
						w.(http.Flusher).Flush()
					case "error_body":
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(http.StatusTeapot)
						_, _ = io.WriteString(w, `{"error":`)
						w.(http.Flusher).Flush()
					}
					close(started)
					select {
					case <-release:
					case <-r.Context().Done():
						return
					}
					switch phase {
					case "error_headers":
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(http.StatusTeapot)
						_, _ = io.WriteString(w, `{"error":{"message":"image fixture rejected"}}`)
					case "error_body":
						_, _ = io.WriteString(w, `{"message":"image fixture rejected"}}`)
					default:
						writeKeepaliveImageResult(w)
					}
				}, database.ContinuousRetryPolicy{}, 0)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				request := imageKeepaliveRequest(t, fixture.server.URL, kind).WithContext(ctx)
				var informational atomic.Int32
				headers, result := startImageKeepaliveRequest(fixture.server.Client(), request, &informational)
				select {
				case <-started:
				case response := <-result:
					t.Fatalf("request ended before reaching fake upstream: %+v body=%s", response, response.body)
				case <-ctx.Done():
					t.Fatal("fake upstream did not start")
				}
				// A real socket observes 1xx without committing the final status.
				// The baseline emits several 102s during this controlled interval.
				select {
				case status := <-headers:
					t.Errorf("premature final response while upstream is blocked: %d", status)
				case <-time.After(4 * imagesKeepaliveTestInterval):
				}
				unblock()
				response := awaitImageKeepaliveResult(t, result)
				if response.err != nil {
					t.Fatal(response.err)
				}
				if got := informational.Load(); got != 0 {
					t.Errorf("received %d informational responses; want none for non-streaming Codex Images", got)
				}
				wantStatus := http.StatusOK
				if strings.HasPrefix(phase, "error_") {
					wantStatus = http.StatusTeapot
				}
				if response.status != wantStatus || !json.Valid(response.body) || !strings.HasPrefix(response.contentType, "application/json") {
					t.Fatalf("unexpected final response: status=%d content-type=%q body=%s", response.status, response.contentType, response.body)
				}
				if strings.Contains(string(response.body), ": keepalive") {
					t.Fatalf("SSE heartbeat leaked into JSON: %s", response.body)
				}
				if wantStatus == http.StatusOK && gjson.GetBytes(response.body, "data.0.b64_json").String() != tinyPNGBase64 {
					t.Errorf("image payload changed: %s", response.body)
				}
				if wantStatus != http.StatusOK && !strings.Contains(string(response.body), "image fixture rejected") {
					t.Errorf("final error lost: %s", response.body)
				}
				fixture.assertReleased(t)
			})
		}
	}
}

func TestImagesNonStreamingKeepaliveCancellationReleasesResources(t *testing.T) {
	for _, phase := range []string{"headers", "sse_body"} {
		t.Run(phase, func(t *testing.T) {
			started, canceled := make(chan struct{}), make(chan struct{})
			fixture := newImagesKeepaliveFixture(t, func(w http.ResponseWriter, r *http.Request) {
				if phase == "sse_body" {
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, "data: {\"type\":\"response.created\"}\n\n")
					w.(http.Flusher).Flush()
				}
				close(started)
				<-r.Context().Done()
				close(canceled)
			}, database.ContinuousRetryPolicy{}, 0)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var informational atomic.Int32
			_, result := startImageKeepaliveRequest(fixture.server.Client(), imageKeepaliveRequest(t, fixture.server.URL, "generations_default").WithContext(ctx), &informational)
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal("fake upstream did not start")
			}
			if fixture.accounts[0].GetActiveRequests() != 1 || APIKeyScopeInflight(imagesKeepaliveTestKeyID, database.APIKeyScopeTypeGroup, 10) != 1 {
				t.Fatal("request did not occupy account and scope concurrency")
			}
			cancel()
			if response := awaitImageKeepaliveResult(t, result); response.err == nil {
				t.Fatal("canceled client unexpectedly completed successfully")
			}
			select {
			case <-canceled:
			case <-time.After(3 * time.Second):
				t.Fatal("upstream request was not canceled")
			}
			fixture.assertReleased(t)
		})
	}
}

func TestImagesNonStreamingKeepaliveRetryAndDeadline(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name := "finite_retry"
		policy := database.ContinuousRetryPolicy{}
		if deadline {
			name = "continuous_retry_deadline"
			policy = database.ContinuousRetryPolicy{Enabled: true, CatchAll: true, MaxDurationSeconds: 1}
		}
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			started, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			fixture := newImagesKeepaliveFixture(t, func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) == 1 {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusServiceUnavailable)
					_, _ = io.WriteString(w, `{"error":{"message":"retry this image","code":"overloaded"}}`)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "data: {\"type\":\"response.created\"}\n\n")
				w.(http.Flusher).Flush()
				close(started)
				select {
				case <-r.Context().Done():
					close(canceled)
				case <-release:
					writeKeepaliveImageResult(w)
				}
			}, policy, 1)
			second := &auth.Account{DBID: 2, AccessToken: "second-test-token", AccountID: "second-image-test-account", PlanType: "plus", GroupIDs: []int64{10}}
			fixture.handler.store.AddAccount(second)
			fixture.accounts = append(fixture.accounts, second)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var informational atomic.Int32
			_, result := startImageKeepaliveRequest(fixture.server.Client(), imageKeepaliveRequest(t, fixture.server.URL, "generations_false").WithContext(ctx), &informational)
			select {
			case <-started:
			case response := <-result:
				t.Fatalf("second attempt did not start: %+v body=%s", response, response.body)
			case <-ctx.Done():
				t.Fatal("retry did not reach fake upstream")
			}
			if !deadline {
				time.Sleep(4 * imagesKeepaliveTestInterval)
				unblock()
			}
			response := awaitImageKeepaliveResult(t, result)
			if response.err != nil {
				t.Fatal(response.err)
			}
			wantStatus := http.StatusOK
			if deadline {
				wantStatus = http.StatusServiceUnavailable
				select {
				case <-canceled:
				case <-time.After(3 * time.Second):
					t.Fatal("retry deadline did not cancel upstream")
				}
			}
			if response.status != wantStatus || informational.Load() != 0 || calls.Load() != 2 {
				t.Errorf("status=%d want=%d, informational=%d want=0, upstream calls=%d want=2; body=%s", response.status, wantStatus, informational.Load(), calls.Load(), response.body)
			}
			if deadline && !strings.Contains(string(response.body), "retry this image") {
				t.Errorf("deadline lost last upstream failure: %s", response.body)
			}
			fixture.assertReleased(t)
		})
	}
}
