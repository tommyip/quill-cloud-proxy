//go:build llm_multi

package llm

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	qtypes "github.com/Lore-Hex/quill-cloud-proxy/enclave-go/internal/types"
)

func TestNegbetaPrepaidStreaming(t *testing.T) {
	const model = "qwen/qwen3.8-27b"
	clients := newBootstrapDirectClients(map[string]string{"negbeta": "operator-test-key"})
	if clients["negbeta"] == nil {
		t.Fatal("Negbeta prepaid client missing")
	}
	calls := 0
	clients["negbeta"].httpc = &http.Client{Transport: byokRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.URL.String() != "https://inference.negbeta.com/v1/chat/completions" || r.Header.Get("Authorization") != "Bearer operator-test-key" {
			t.Fatal("incorrect Negbeta endpoint or credential")
		}
		var wire openAICompatibleRequest
		if err := json.NewDecoder(r.Body).Decode(&wire); err != nil {
			t.Fatal(err)
		}
		if wire.Model != model || !wire.Stream {
			t.Fatalf("incorrect upstream request: %+v", wire)
		}
		stream := "data: {\"choices\":[{\"delta\":{\"content\":\"PONG\"},\"finish_reason\":\"stop\"}]}\n\n" +
			"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":2,\"total_tokens\":14}}\n\n" +
			"data: [DONE]\n\n"
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(stream))}, nil
	})}
	var out bytes.Buffer
	multi := &multiClient{direct: clients}
	err := multi.InvokeStreaming(t.Context(), &qtypes.OpenAIChatRequest{Model: model},
		&qtypes.AnthropicMessagesRequest{MaxTokens: 512, Messages: []qtypes.AnthropicMessage{{Role: "user", Content: "PONG"}}},
		&out, InvokeOptions{Provider: "negbeta", UpstreamModel: model})
	if err != nil || calls != 1 {
		t.Fatalf("calls=%d error=%v", calls, err)
	}
	for _, want := range []string{"PONG", `"input_tokens":12`, `"output_tokens":2`, "message_stop"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("missing %s in stream", want)
		}
	}
}
