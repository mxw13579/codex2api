package api

import (
	"bytes"
	"fmt"
	"testing"
)

func TestOutputTokenLimitValidation(t *testing.T) {
	fields := []struct {
		name      string
		base      string
		validate  func([]byte, []string) *ValidationResult
		nullValid bool
	}{
		{"max_tokens", `"messages":[{"role":"user","content":"hello"}]`, ValidateChatCompletionsRequest, false},
		{"max_completion_tokens", `"messages":[{"role":"user","content":"hello"}]`, ValidateChatCompletionsRequest, true},
		{"max_output_tokens", `"input":"hello"`, ValidateResponsesAPIRequest, false},
	}
	tests := []struct {
		name string
		raw  string
		code string
	}{
		{name: "omitted"},
		{name: "minimum", raw: "1"},
		{name: "old chat cap", raw: "65536"},
		{name: "above old chat cap", raw: "65537"},
		{name: "below cap", raw: "127999"},
		{name: "shared cap", raw: "128000"},
		{name: "integral decimal", raw: "128000.0"},
		{name: "integral exponent", raw: "1.28e5"},
		{name: "above cap", raw: "128001", code: "max_value"},
		{name: "zero", raw: "0", code: "min_value"},
		{name: "negative", raw: "-1", code: "min_value"},
		{name: "below minimum", raw: "0.5", code: "min_value"},
		{name: "fractional", raw: "1.5", code: "type_error"},
		{name: "fractional below cap", raw: "127999.5", code: "type_error"},
		{name: "fractional above cap", raw: "128000.5", code: "max_value"},
		{name: "overflow", raw: "1e1000", code: "max_value"},
		{name: "numeric string", raw: `"128000"`, code: "type_error"},
		{name: "true", raw: "true", code: "type_error"},
		{name: "false", raw: "false", code: "type_error"},
		{name: "object", raw: "{}", code: "type_error"},
		{name: "array", raw: "[]", code: "type_error"},
		{name: "null", raw: "null", code: "type_error"},
	}
	for _, model := range []string{"gpt-5.4", "gpt-5.5"} {
		for _, field := range fields {
			for _, test := range tests {
				t.Run(model+"/"+field.name+"/"+test.name, func(t *testing.T) {
					body := fmt.Sprintf(`{"model":%q,%s`, model, field.base)
					if test.raw != "" {
						body += fmt.Sprintf(`,%q:%s`, field.name, test.raw)
					}
					raw := []byte(body + "}")
					original := bytes.Clone(raw)
					result := field.validate(raw, []string{model})
					if !bytes.Equal(raw, original) {
						t.Fatal("validation changed the requested token limit")
					}
					wantCode := test.code
					if test.raw == "null" && field.nullValid {
						wantCode = ""
					}
					if wantCode == "" {
						if !result.Valid || len(result.Errors) != 0 {
							t.Fatalf("expected valid request; errors=%#v", result.Errors)
						}
						return
					}
					if result.Valid || len(result.Errors) != 1 {
						t.Fatalf("expected one validation error; result=%#v", result)
					}
					got := result.Errors[0]
					if got.Field != field.name || got.Code != wantCode {
						t.Fatalf("error = %#v, want field %q, code %q", got, field.name, wantCode)
					}
					if wantCode == "max_value" {
						want := fmt.Sprintf("Field '%s' must be at most 128000", field.name)
						if got.Message != want {
							t.Fatalf("message = %q, want %q", got.Message, want)
						}
					}
				})
			}
		}
	}
}

func TestChatTokenLimitAliasesValidateIndependently(t *testing.T) {
	tests := []struct {
		name  string
		extra string
		field string
		code  string
	}{
		{name: "both at cap", extra: `"max_tokens":128000,"max_completion_tokens":128000`},
		{name: "completion alias larger", extra: `"max_tokens":65536,"max_completion_tokens":128000`},
		{name: "completion alias smaller", extra: `"max_tokens":128000,"max_completion_tokens":65536`},
		{name: "null completion alias", extra: `"max_tokens":128000,"max_completion_tokens":null`},
		{name: "invalid legacy alias", extra: `"max_tokens":128001,"max_completion_tokens":1`, field: "max_tokens", code: "max_value"},
		{name: "invalid completion alias", extra: `"max_tokens":1,"max_completion_tokens":128001`, field: "max_completion_tokens", code: "max_value"},
		{name: "fractional completion alias", extra: `"max_tokens":1,"max_completion_tokens":1.5`, field: "max_completion_tokens", code: "type_error"},
		{name: "null legacy alias", extra: `"max_tokens":null,"max_completion_tokens":1`, field: "max_tokens", code: "type_error"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := []byte(`{"model":"gpt-5.5","messages":[{"role":"user","content":"hello"}],` + test.extra + `}`)
			result := ValidateChatCompletionsRequest(body, []string{"gpt-5.5"})
			if test.field == "" {
				if !result.Valid || len(result.Errors) != 0 {
					t.Fatalf("expected valid aliases; errors=%#v", result.Errors)
				}
				return
			}
			if result.Valid || len(result.Errors) != 1 || result.Errors[0].Field != test.field || result.Errors[0].Code != test.code {
				t.Fatalf("expected %s error for %s; result=%#v", test.code, test.field, result)
			}
		})
	}
}

func TestResponsesTokenLimitCapIsModelIndependent(t *testing.T) {
	for _, model := range []string{"", "gpt-5.4", "gpt-5.5", "synthetic-model"} {
		if got := ResponsesMaxOutputTokensForModel(model); got != 128000 {
			t.Errorf("ResponsesMaxOutputTokensForModel(%q) = %d, want 128000", model, got)
		}
	}
}
