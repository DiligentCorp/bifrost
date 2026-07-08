package integrations

import (
	"bufio"
	"bytes"
	"io"
	"testing"
	"time"

	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/providers/bedrock"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

// These tests exercise the transport half of the streaming-liveness fix: once a
// stream is committed, handleStreaming's SSEStreamReader must emit keepalive frames
// during a silent gap between real chunks, and stop the instant real data flows.
// The core half (bounding the first-chunk peek so the stream commits before the
// first token) is covered by CheckFirstStreamChunkForError's unit tests.

// readerFromStreamingHandler runs handleStreaming for the given route type with a
// keepalive-enabled store, returns the committed body stream and the chunk channel
// the test producer feeds. keepaliveMS is the keepalive interval in whole seconds
// (the store's unit), so callers pass small values and the test stays fast.
func streamingBodyForRoute(t *testing.T, routeType RouteConfigType, keepaliveSeconds int, streamCfg *StreamConfig) (io.Reader, chan *schemas.BifrostStreamChunk) {
	t.Helper()
	stream := make(chan *schemas.BifrostStreamChunk, 4)
	store := &mockHandlerStore{streamKeepaliveIntervalS: keepaliveSeconds}
	router := NewGenericRouter(nil, store, nil, nil, bifrost.NewNoOpLogger())
	ctx := &fasthttp.RequestCtx{}

	router.handleStreaming(ctx, nil, RouteConfig{
		Type:         routeType,
		StreamConfig: streamCfg,
	}, stream, func() {})

	return ctx.Response.BodyStream(), stream
}

// TestHandleStreaming_KeepaliveEmittedDuringPreFirstChunkGap verifies that on an
// SSE (Anthropic-type) route with keepalives enabled, the reader emits at least one
// keepalive comment frame while the producer is silent, then the real chunk follows.
func TestHandleStreaming_KeepaliveEmittedDuringPreFirstChunkGap(t *testing.T) {
	body, stream := streamingBodyForRoute(t, RouteConfigTypeAnthropic, 1, &StreamConfig{
		ChatStreamResponseConverter: func(_ *schemas.BifrostContext, _ *schemas.BifrostChatResponse) (string, interface{}, error) {
			return "", "event: message_stop\ndata: {}\n\n", nil
		},
	})

	// Produce the first real chunk only after ~2 keepalive intervals of silence.
	go func() {
		time.Sleep(2200 * time.Millisecond)
		stream <- &schemas.BifrostStreamChunk{BifrostChatResponse: &schemas.BifrostChatResponse{ID: "1"}}
		close(stream)
	}()

	firstLine, elapsed := readFirstLine(t, body, 5*time.Second)
	require.True(t, bytes.HasPrefix(firstLine, []byte(":")),
		"first frame during the silent gap must be a keepalive comment, got %q", firstLine)
	require.Less(t, elapsed, 2*time.Second,
		"keepalive should arrive within ~1 interval, well before the 2.2s first chunk")
}

// TestHandleStreaming_BedrockKeepaliveIsBinaryFrame verifies that the Bedrock route
// emits its protocol-correct binary EventStream keepalive (not an SSE comment) during
// the gap, and that it decodes as an ignorable event.
func TestHandleStreaming_BedrockKeepaliveIsBinaryFrame(t *testing.T) {
	body, stream := streamingBodyForRoute(t, RouteConfigTypeBedrock, 1, &StreamConfig{
		ChatStreamResponseConverter: func(_ *schemas.BifrostContext, _ *schemas.BifrostChatResponse) (string, interface{}, error) {
			return "", &bedrock.BedrockStreamEvent{}, nil
		},
	})

	go func() {
		time.Sleep(1200 * time.Millisecond)
		close(stream) // no real chunk needed; we only assert the keepalive frame
	}()

	// The first bytes on the wire must NOT be an SSE comment (that would corrupt the
	// binary EventStream); they must be the pre-encoded keepalive frame.
	buf := make([]byte, len(bedrockKeepaliveFrame))
	deadline := time.Now().Add(5 * time.Second)
	done := make(chan struct{})
	var n int
	var readErr error
	go func() { n, readErr = io.ReadFull(body, buf); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Until(deadline)):
		t.Fatal("timed out waiting for the Bedrock keepalive frame")
	}
	require.NoError(t, readErr)
	require.Equal(t, bedrockKeepaliveFrame, buf[:n],
		"Bedrock gap frame must be the binary EventStream keepalive, not an SSE comment")
	require.False(t, bytes.HasPrefix(buf, []byte(":")), "must not be an SSE comment on the Bedrock path")
}

// TestHandleStreaming_NoKeepaliveWhenDisabled verifies that with keepalives off
// (interval 0), a silent gap produces no comment frames — the reader blocks, exactly
// as before this feature. Guards against spurious emission / behavior change.
func TestHandleStreaming_NoKeepaliveWhenDisabled(t *testing.T) {
	body, stream := streamingBodyForRoute(t, RouteConfigTypeAnthropic, 0, &StreamConfig{
		ChatStreamResponseConverter: func(_ *schemas.BifrostContext, _ *schemas.BifrostChatResponse) (string, interface{}, error) {
			return "", "event: message_stop\ndata: {}\n\n", nil
		},
	})

	go func() {
		time.Sleep(300 * time.Millisecond)
		stream <- &schemas.BifrostStreamChunk{BifrostChatResponse: &schemas.BifrostChatResponse{ID: "1"}}
		close(stream)
	}()

	firstLine, _ := readFirstLine(t, body, 5*time.Second)
	require.False(t, bytes.HasPrefix(firstLine, []byte(":")),
		"no keepalive frame should be emitted when the interval is 0, got %q", firstLine)
}

// readFirstLine reads the first non-empty line from an SSE body stream and reports
// how long it took to arrive.
func readFirstLine(t *testing.T, body io.Reader, timeout time.Duration) ([]byte, time.Duration) {
	t.Helper()
	type result struct {
		line    []byte
		elapsed time.Duration
	}
	ch := make(chan result, 1)
	start := time.Now()
	go func() {
		br := bufio.NewReader(body)
		for {
			line, err := br.ReadBytes('\n')
			trimmed := bytes.TrimRight(line, "\r\n")
			if len(trimmed) > 0 {
				ch <- result{trimmed, time.Since(start)}
				return
			}
			if err != nil {
				ch <- result{nil, time.Since(start)}
				return
			}
		}
	}()
	select {
	case r := <-ch:
		return r.line, r.elapsed
	case <-time.After(timeout):
		t.Fatal("timed out waiting for the first SSE line")
		return nil, 0
	}
}
