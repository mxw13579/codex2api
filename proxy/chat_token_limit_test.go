package proxy

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/codex2api/api"
	"github.com/codex2api/auth"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

func TestChatCompletionsTokenLimitValidation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	// An empty in-memory store and cancelled context ensure no upstream call is
	// possible. A 503/no-account response proves the request passed validation.
	store := auth.NewStore(nil, nil, nil)
	t.Cleanup(store.Stop)
	handler := NewHandler(store, nil, nil, nil)
	router := gin.New()
	router.POST("/v1/chat/completions", handler.ChatCompletions)

	for _, field := range []string{"max_tokens", "max_completion_tokens"} {
		for _, stream := range []bool{false, true} {
			for _, tokens := range []int{65536, 65537, 128000, 128001, 0, -1} {
				t.Run(fmt.Sprintf("%s/stream=%t/tokens=%d", field, stream, tokens), func(t *testing.T) {
					body := fmt.Sprintf(`{"model":"gpt-5.5","messages":[{"role":"user","content":"hello"}],"stream":%t,%q:%d}`, stream, field, tokens)
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body)).WithContext(ctx)
					req.Header.Set("Content-Type", "application/json")
					recorder := httptest.NewRecorder()
					router.ServeHTTP(recorder, req)

					if tokens >= 1 && tokens <= 128000 {
						if recorder.Code != http.StatusServiceUnavailable {
							t.Fatalf("status = %d, want 503 after validation passes; body=%s", recorder.Code, recorder.Body.String())
						}
						assertNoAvailableAccountResponse(t, recorder.Body.Bytes())
						return
					}
					if recorder.Code != http.StatusBadRequest {
						t.Fatalf("status = %d, want 400; body=%s", recorder.Code, recorder.Body.String())
					}
					if got := gjson.GetBytes(recorder.Body.Bytes(), "error.details.field").String(); got != field {
						t.Fatalf("validation error field = %q, want %q; body=%s", got, field, recorder.Body.String())
					}
					wantMessage := fmt.Sprintf("Field '%s' must be at least 1", field)
					if tokens > 128000 {
						wantMessage = fmt.Sprintf("Field '%s' must be at most 128000", field)
					}
					if got := gjson.GetBytes(recorder.Body.Bytes(), "error.message").String(); got != wantMessage {
						t.Fatalf("validation message = %q, want %q", got, wantMessage)
					}
				})
			}
		}
	}
}

func TestChatTokenLimitTranslationPreservesValueAndPrecedence(t *testing.T) {
	tests := []struct {
		name  string
		extra string
		want  int64
	}{
		{name: "omitted"},
		{name: "null completion alias", extra: `,"max_completion_tokens":null`},
		{name: "legacy 65536", extra: `,"max_tokens":65536`, want: 65536},
		{name: "legacy 128000", extra: `,"max_tokens":128000`, want: 128000},
		{name: "completion 65536", extra: `,"max_completion_tokens":65536`, want: 65536},
		{name: "completion 128000", extra: `,"max_completion_tokens":128000`, want: 128000},
		{name: "completion wins when larger", extra: `,"max_tokens":65536,"max_completion_tokens":128000`, want: 128000},
		{name: "completion wins when smaller", extra: `,"max_tokens":128000,"max_completion_tokens":65536`, want: 65536},
		{name: "null completion falls back", extra: `,"max_tokens":128000,"max_completion_tokens":null`, want: 128000},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			raw := []byte(`{"model":"gpt-5.5","messages":[{"role":"user","content":"hello"}]` + test.extra + `}`)
			original := bytes.Clone(raw)
			if result := api.ValidateChatCompletionsRequest(raw, []string{"gpt-5.5"}); !result.Valid {
				t.Fatalf("chat request failed validation: %#v", result.Errors)
			}
			translated, err := TranslateChatToResponsesForGrok(raw)
			if err != nil {
				t.Fatal(err)
			}
			if result := api.ValidateResponsesAPIRequest(translated, []string{"gpt-5.5"}); !result.Valid {
				t.Fatalf("translated request failed Responses validation: %#v", result.Errors)
			}
			for _, stage := range []struct {
				name string
				body []byte
			}{
				{"translated", translated},
				{"native Responses prepared", PrepareOpenAIResponsesBody(translated)},
			} {
				got := gjson.GetBytes(stage.body, "max_output_tokens")
				if test.want == 0 {
					if got.Exists() {
						t.Fatalf("%s added a token limit: %s", stage.name, got.Raw)
					}
				} else if got.Type != gjson.Number || got.Float() != float64(test.want) {
					t.Fatalf("%s max_output_tokens = %s, want %d", stage.name, got.Raw, test.want)
				}
				for _, alias := range []string{"max_tokens", "max_completion_tokens"} {
					if gjson.GetBytes(stage.body, alias).Exists() {
						t.Fatalf("%s retained Chat alias %s", stage.name, alias)
					}
				}
			}

			// Codex intentionally cannot enforce this requested output budget.
			codex, err := TranslateRequest(raw)
			if err != nil {
				t.Fatal(err)
			}
			for _, field := range []string{"max_tokens", "max_completion_tokens", "max_output_tokens"} {
				if gjson.GetBytes(codex, field).Exists() {
					t.Fatalf("Codex translation retained unsupported %s", field)
				}
			}
			if !bytes.Equal(raw, original) {
				t.Fatal("validation or translation mutated the client request")
			}
		})
	}
}
