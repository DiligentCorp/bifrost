package utils

import (
	"context"
	"testing"
	"time"

	schemas "github.com/maximhq/bifrost/core/schemas"
)

func TestCheckFirstStreamChunk_ErrorInFirstChunk(t *testing.T) {
	stream := make(chan *schemas.BifrostStreamChunk, 2)
	stream <- &schemas.BifrostStreamChunk{
		BifrostError: &schemas.BifrostError{
			Error: &schemas.ErrorField{
				Code:    schemas.Ptr("limit_burst_rate"),
				Message: "Request rate increased too quickly",
			},
		},
	}
	close(stream)

	_, drainDone, err := CheckFirstStreamChunkForError(context.Background(), stream, 0)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	<-drainDone
	if err.Error.Message != "Request rate increased too quickly" {
		t.Errorf("unexpected error message: %s", err.Error.Message)
	}
	if err.Error.Code == nil || *err.Error.Code != "limit_burst_rate" {
		t.Errorf("unexpected error code: %v", err.Error.Code)
	}
}

func TestCheckFirstStreamChunk_ValidFirstChunk(t *testing.T) {
	stream := make(chan *schemas.BifrostStreamChunk, 3)
	chunk1 := &schemas.BifrostStreamChunk{
		BifrostChatResponse: &schemas.BifrostChatResponse{
			ID: "chatcmpl-123",
		},
	}
	chunk2 := &schemas.BifrostStreamChunk{
		BifrostChatResponse: &schemas.BifrostChatResponse{
			ID: "chatcmpl-123",
		},
	}
	stream <- chunk1
	stream <- chunk2
	close(stream)

	wrapped, _, err := CheckFirstStreamChunkForError(context.Background(), stream, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// First chunk should be re-injected
	got1 := <-wrapped
	if got1.BifrostChatResponse == nil || got1.BifrostChatResponse.ID != "chatcmpl-123" {
		t.Error("first chunk not re-injected correctly")
	}

	// Second chunk should follow
	got2 := <-wrapped
	if got2.BifrostChatResponse == nil || got2.BifrostChatResponse.ID != "chatcmpl-123" {
		t.Error("second chunk not forwarded correctly")
	}

	// Channel should be closed
	_, ok := <-wrapped
	if ok {
		t.Error("expected wrapped channel to be closed")
	}
}

func TestCheckFirstStreamChunk_EmptyStream(t *testing.T) {
	stream := make(chan *schemas.BifrostStreamChunk)
	close(stream)

	wrapped, drainDone, err := CheckFirstStreamChunkForError(context.Background(), stream, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Empty stream should return nil channel
	if wrapped != nil {
		t.Error("expected nil channel for empty stream")
	}

	// drainDone should be already closed
	select {
	case <-drainDone:
	default:
		t.Error("expected drainDone to be closed for empty stream")
	}
}

func TestCheckFirstStreamChunk_ErrorInSecondChunk(t *testing.T) {
	stream := make(chan *schemas.BifrostStreamChunk, 3)
	stream <- &schemas.BifrostStreamChunk{
		BifrostChatResponse: &schemas.BifrostChatResponse{
			ID: "chatcmpl-123",
		},
	}
	stream <- &schemas.BifrostStreamChunk{
		BifrostError: &schemas.BifrostError{
			Error: &schemas.ErrorField{
				Message: "some error in second chunk",
			},
		},
	}
	close(stream)

	// Should NOT return error — only first chunk matters for retry
	wrapped, _, err := CheckFirstStreamChunkForError(context.Background(), stream, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Read all chunks
	got1 := <-wrapped
	if got1.BifrostChatResponse == nil {
		t.Error("first chunk should be valid data")
	}
	got2 := <-wrapped
	if got2.BifrostError == nil {
		t.Error("second chunk should be the error")
	}

	_, ok := <-wrapped
	if ok {
		t.Error("expected wrapped channel to be closed")
	}
}

func TestCheckFirstStreamChunk_ErrorDrainsSource(t *testing.T) {
	stream := make(chan *schemas.BifrostStreamChunk, 5)
	stream <- &schemas.BifrostStreamChunk{
		BifrostError: &schemas.BifrostError{
			Error: &schemas.ErrorField{
				Message: "rate limit error",
			},
		},
	}
	// Add more chunks that should be drained
	stream <- &schemas.BifrostStreamChunk{
		BifrostChatResponse: &schemas.BifrostChatResponse{ID: "1"},
	}
	stream <- &schemas.BifrostStreamChunk{
		BifrostChatResponse: &schemas.BifrostChatResponse{ID: "2"},
	}
	close(stream)

	_, drainDone, err := CheckFirstStreamChunkForError(context.Background(), stream, 0)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	<-drainDone
	if err.Error.Message != "rate limit error" {
		t.Errorf("unexpected error message: %s", err.Error.Message)
	}
	if drainDone == nil {
		t.Fatal("expected drainDone channel, got nil")
	}
	// Wait for drain to complete — verifies the channel signals properly
	<-drainDone
}

func TestCheckFirstStreamChunk_ErrorWithEmptyMessage(t *testing.T) {
	// Error with empty message and no code/type should NOT be treated as an error
	stream := make(chan *schemas.BifrostStreamChunk, 2)
	stream <- &schemas.BifrostStreamChunk{
		BifrostError: &schemas.BifrostError{
			Error: &schemas.ErrorField{
				Message: "",
			},
		},
	}
	close(stream)

	wrapped, _, err := CheckFirstStreamChunkForError(context.Background(), stream, 0)
	if err != nil {
		t.Fatalf("unexpected error for empty message: %v", err)
	}
	// Should be treated as valid chunk
	<-wrapped
}

func TestCheckFirstStreamChunk_CtxCancelUnblocksWrapper(t *testing.T) {
	// Source with cap=1 so wrapped also has cap=1. wrapped is left full by
	// the re-injected first chunk, which makes the forwarder goroutine block
	// on its next send — the exact leak condition this test guards against.
	src := make(chan *schemas.BifrostStreamChunk, 1)
	src <- &schemas.BifrostStreamChunk{
		BifrostChatResponse: &schemas.BifrostChatResponse{ID: "1"},
	}

	ctx, cancel := context.WithCancel(context.Background())

	wrapped, drainDone, err := CheckFirstStreamChunkForError(ctx, src, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if wrapped == nil {
		t.Fatal("expected wrapped channel, got nil")
	}

	// Push a second chunk; forwarder will read it from src and then block
	// trying to send into the full wrapped channel (we intentionally never
	// read from wrapped).
	src <- &schemas.BifrostStreamChunk{
		BifrostChatResponse: &schemas.BifrostChatResponse{ID: "2"},
	}

	// Cancel — forwarder must stop trying to send to wrapped and drain src.
	cancel()

	// Simulate the upstream producer still emitting, then closing. The
	// drain loop should consume these and terminate.
	src <- &schemas.BifrostStreamChunk{
		BifrostChatResponse: &schemas.BifrostChatResponse{ID: "3"},
	}
	close(src)

	select {
	case <-drainDone:
	case <-time.After(time.Second):
		t.Fatal("drainDone did not close after ctx cancel; forwarder goroutine leaked")
	}
}

// --- First-chunk timeout context accessors ------------------------------------

func TestFirstChunkTimeout_UnsetReturnsZero(t *testing.T) {
	ctx := schemas.NewBifrostContext(context.Background(), schemas.NoDeadline)
	if got := GetFirstChunkTimeout(ctx); got != 0 {
		t.Errorf("unset timeout should be 0 (unbounded), got %v", got)
	}
}

func TestFirstChunkTimeout_SetThenGet(t *testing.T) {
	ctx := schemas.NewBifrostContext(context.Background(), schemas.NoDeadline)
	SetFirstChunkTimeoutIfEmpty(ctx, 15*time.Second)
	if got := GetFirstChunkTimeout(ctx); got != 15*time.Second {
		t.Errorf("got %v, want 15s", got)
	}
}

func TestFirstChunkTimeout_RespectsExistingValue(t *testing.T) {
	ctx := schemas.NewBifrostContext(context.Background(), schemas.NoDeadline)
	SetFirstChunkTimeoutIfEmpty(ctx, 30*time.Second) // e.g. an upstream/header value
	SetFirstChunkTimeoutIfEmpty(ctx, 15*time.Second) // must not overwrite
	if got := GetFirstChunkTimeout(ctx); got != 30*time.Second {
		t.Errorf("existing value must win: got %v, want 30s", got)
	}
}

func TestFirstChunkTimeout_NonPositiveIgnored(t *testing.T) {
	ctx := schemas.NewBifrostContext(context.Background(), schemas.NoDeadline)
	SetFirstChunkTimeoutIfEmpty(ctx, 0)
	SetFirstChunkTimeoutIfEmpty(ctx, -5*time.Second)
	if got := GetFirstChunkTimeout(ctx); got != 0 {
		t.Errorf("non-positive timeout must leave it unset (0), got %v", got)
	}
}

func TestAttachBilledUsageFromContext_KeepsUsageWithOnlyDetails(t *testing.T) {
	ctx := schemas.NewBifrostContext(context.Background(), schemas.NoDeadline)
	usage := &schemas.BifrostLLMUsage{
		// top-level counters all zero, but cache details are present
		PromptTokensDetails: &schemas.ChatPromptTokensDetails{CachedReadTokens: 5},
	}
	ctx.SetValue(schemas.BifrostContextKeyStreamAccumulatedUsage, usage)

	bifrostErr := &schemas.BifrostError{}
	attachBilledUsageFromContext(ctx, bifrostErr)

	if bifrostErr.ExtraFields.BilledUsage == nil {
		t.Fatal("BilledUsage should be attached when cache details are present")
	}
	if bifrostErr.ExtraFields.BilledUsage.PromptTokensDetails == nil ||
		bifrostErr.ExtraFields.BilledUsage.PromptTokensDetails.CachedReadTokens != 5 {
		t.Fatalf("cached read tokens not preserved: %+v", bifrostErr.ExtraFields.BilledUsage)
	}
}

func TestAttachBilledUsageFromContext_CopiesUsage(t *testing.T) {
	ctx := schemas.NewBifrostContext(context.Background(), schemas.NoDeadline)
	usage := &schemas.BifrostLLMUsage{
		PromptTokens: 10,
		TotalTokens:  10,
		PromptTokensDetails: &schemas.ChatPromptTokensDetails{
			CachedReadTokens: 3,
			CachedWriteTokenDetails: &schemas.ChatCachedWriteTokenDetails{
				CachedWriteTokens5m: 4,
			},
		},
	}
	ctx.SetValue(schemas.BifrostContextKeyStreamAccumulatedUsage, usage)

	bifrostErr := &schemas.BifrostError{}
	attachBilledUsageFromContext(ctx, bifrostErr)

	// Mutating the original (top-level AND nested pointers) must not change the
	// billed snapshot - BilledUsage is meant to be a fully decoupled record.
	usage.PromptTokens = 999
	usage.PromptTokensDetails.CachedReadTokens = 999
	usage.PromptTokensDetails.CachedWriteTokenDetails.CachedWriteTokens5m = 999

	billed := bifrostErr.ExtraFields.BilledUsage
	if billed.PromptTokens != 10 {
		t.Fatalf("BilledUsage aliases the context handle: got %d, want 10", billed.PromptTokens)
	}
	if billed.PromptTokensDetails.CachedReadTokens != 3 {
		t.Fatalf("BilledUsage aliases nested details: got %d, want 3", billed.PromptTokensDetails.CachedReadTokens)
	}
	if billed.PromptTokensDetails.CachedWriteTokenDetails.CachedWriteTokens5m != 4 {
		t.Fatalf("BilledUsage aliases deeply-nested details: got %d, want 4", billed.PromptTokensDetails.CachedWriteTokenDetails.CachedWriteTokens5m)
	}
}

func TestAttachBilledUsageFromContext_NoOpWhenEmpty(t *testing.T) {
	ctx := schemas.NewBifrostContext(context.Background(), schemas.NoDeadline)
	ctx.SetValue(schemas.BifrostContextKeyStreamAccumulatedUsage, &schemas.BifrostLLMUsage{})
	bifrostErr := &schemas.BifrostError{}
	attachBilledUsageFromContext(ctx, bifrostErr)
	if bifrostErr.ExtraFields.BilledUsage != nil {
		t.Fatal("BilledUsage should stay nil when nothing measurable accumulated")
	}
}

func TestCheckFirstStreamChunk_CodeOnlyError(t *testing.T) {
	// Error with code but no message should be treated as an error
	stream := make(chan *schemas.BifrostStreamChunk, 2)
	stream <- &schemas.BifrostStreamChunk{
		BifrostError: &schemas.BifrostError{
			Error: &schemas.ErrorField{
				Code: schemas.Ptr("limit_burst_rate"),
			},
		},
	}
	close(stream)

	_, drainDone, err := CheckFirstStreamChunkForError(context.Background(), stream, 0)
	if err == nil {
		t.Fatal("expected error for code-only error, got nil")
	}
	<-drainDone
	if err.Error.Code == nil || *err.Error.Code != "limit_burst_rate" {
		t.Errorf("unexpected error code: %v", err.Error.Code)
	}
}

// --- Bounded first-chunk peek (liveness timeout) ------------------------------
//
// These verify the firstChunkTimeout branch: when a caller supplies a positive
// deadline (a transport that must keep the connection warm during a slow first
// token), the peek must not block past it. The un-timed behavior is covered by
// every test above passing 0.

// TestCheckFirstStreamChunk_TimeoutForwardsPendingFirstChunk verifies that when the
// deadline elapses before the first chunk arrives, the stream is committed and the
// still-pending first chunk (and everything after) is forwarded verbatim — nothing
// is dropped and no error/retry is produced.
func TestCheckFirstStreamChunk_TimeoutForwardsPendingFirstChunk(t *testing.T) {
	stream := make(chan *schemas.BifrostStreamChunk, 2)

	wrapped, drainDone, err := CheckFirstStreamChunkForError(context.Background(), stream, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("timeout must not surface an error (retry is impossible post-commit): %v", err)
	}
	if wrapped == nil {
		t.Fatal("expected a forwarding channel on timeout, got nil")
	}

	// The first chunk arrives only after the deadline — simulating a slow first token.
	stream <- &schemas.BifrostStreamChunk{BifrostChatResponse: &schemas.BifrostChatResponse{ID: "late-first"}}
	stream <- &schemas.BifrostStreamChunk{BifrostChatResponse: &schemas.BifrostChatResponse{ID: "second"}}
	close(stream)

	got1 := <-wrapped
	if got1.BifrostChatResponse == nil || got1.BifrostChatResponse.ID != "late-first" {
		t.Errorf("late first chunk not forwarded verbatim: %+v", got1)
	}
	got2 := <-wrapped
	if got2.BifrostChatResponse == nil || got2.BifrostChatResponse.ID != "second" {
		t.Errorf("second chunk not forwarded: %+v", got2)
	}
	if _, ok := <-wrapped; ok {
		t.Error("expected wrapped channel to close after source drains")
	}
	<-drainDone
}

// TestCheckFirstStreamChunk_TimeoutThenErrorDeliveredInBand verifies that once the
// deadline commits the stream, a first chunk that turns out to be an error is
// delivered in-band as a normal chunk — NOT surfaced for retry (which is no longer
// possible after the response is committed).
func TestCheckFirstStreamChunk_TimeoutThenErrorDeliveredInBand(t *testing.T) {
	stream := make(chan *schemas.BifrostStreamChunk, 1)

	wrapped, drainDone, err := CheckFirstStreamChunkForError(context.Background(), stream, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("timeout path must not return a retryable error: %v", err)
	}
	if wrapped == nil {
		t.Fatal("expected a forwarding channel, got nil")
	}

	// After the deadline, the (late) first chunk is an error.
	stream <- &schemas.BifrostStreamChunk{
		BifrostError: &schemas.BifrostError{Error: &schemas.ErrorField{Message: "late upstream error"}},
	}
	close(stream)

	got := <-wrapped
	if got.BifrostError == nil || got.BifrostError.Error.Message != "late upstream error" {
		t.Errorf("post-commit error must be forwarded in-band, got %+v", got)
	}
	if _, ok := <-wrapped; ok {
		t.Error("expected wrapped channel to close")
	}
	<-drainDone
}

// TestCheckFirstStreamChunk_FastErrorStillCaughtUnderTimeout verifies that a
// generous deadline does not weaken error detection: an embedded error that arrives
// before the deadline is still returned for retry, exactly as with no timeout. This
// is the case the whole feature must preserve.
func TestCheckFirstStreamChunk_FastErrorStillCaughtUnderTimeout(t *testing.T) {
	stream := make(chan *schemas.BifrostStreamChunk, 2)
	stream <- &schemas.BifrostStreamChunk{
		BifrostError: &schemas.BifrostError{Error: &schemas.ErrorField{
			Code:    schemas.Ptr("limit_burst_rate"),
			Message: "Request rate increased too quickly",
		}},
	}
	close(stream)

	// A large deadline; the error is already queued, so the peek returns immediately.
	_, drainDone, err := CheckFirstStreamChunkForError(context.Background(), stream, 10*time.Second)
	if err == nil {
		t.Fatal("a fast embedded error must still be caught for retry even with a timeout set")
	}
	<-drainDone
	if err.Error.Code == nil || *err.Error.Code != "limit_burst_rate" {
		t.Errorf("unexpected error code: %v", err.Error.Code)
	}
}

// TestCheckFirstStreamChunk_FastDataUnderTimeoutBehavesAsUntimed verifies that a
// valid first chunk arriving before the deadline takes the normal re-inject path.
func TestCheckFirstStreamChunk_FastDataUnderTimeoutBehavesAsUntimed(t *testing.T) {
	stream := make(chan *schemas.BifrostStreamChunk, 2)
	stream <- &schemas.BifrostStreamChunk{BifrostChatResponse: &schemas.BifrostChatResponse{ID: "fast"}}
	stream <- &schemas.BifrostStreamChunk{BifrostChatResponse: &schemas.BifrostChatResponse{ID: "next"}}
	close(stream)

	wrapped, drainDone, err := CheckFirstStreamChunkForError(context.Background(), stream, 10*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got1 := <-wrapped
	if got1.BifrostChatResponse == nil || got1.BifrostChatResponse.ID != "fast" {
		t.Errorf("first chunk not re-injected: %+v", got1)
	}
	got2 := <-wrapped
	if got2.BifrostChatResponse == nil || got2.BifrostChatResponse.ID != "next" {
		t.Errorf("second chunk not forwarded: %+v", got2)
	}
	if _, ok := <-wrapped; ok {
		t.Error("expected wrapped channel to close")
	}
	<-drainDone
}

// TestCheckFirstStreamChunk_TimeoutEmptyStreamCloses verifies that a source which
// closes without ever producing a chunk terminates the forwarding channel cleanly
// even when a deadline is set.
func TestCheckFirstStreamChunk_TimeoutEmptyStreamCloses(t *testing.T) {
	stream := make(chan *schemas.BifrostStreamChunk, 1)

	wrapped, drainDone, err := CheckFirstStreamChunkForError(context.Background(), stream, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if wrapped == nil {
		t.Fatal("expected a forwarding channel on timeout, got nil")
	}

	// Deadline elapses, then the producer closes without emitting anything.
	close(stream)

	if _, ok := <-wrapped; ok {
		t.Error("expected wrapped channel to close for an empty stream")
	}
	select {
	case <-drainDone:
	case <-time.After(time.Second):
		t.Fatal("drainDone did not close; forwarder goroutine leaked")
	}
}

// TestCheckFirstStreamChunk_TimeoutCtxCancelDrains verifies the abandon path on the
// timeout branch: if the consumer stops reading the forwarding channel, ctx cancel
// makes the forwarder drain the source so the provider goroutine can exit.
func TestCheckFirstStreamChunk_TimeoutCtxCancelDrains(t *testing.T) {
	src := make(chan *schemas.BifrostStreamChunk, 1)
	ctx, cancel := context.WithCancel(context.Background())

	wrapped, drainDone, err := CheckFirstStreamChunkForError(ctx, src, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if wrapped == nil {
		t.Fatal("expected a forwarding channel, got nil")
	}

	// After the deadline, fill the pipeline so the forwarder blocks on send into
	// the unread (cap-1, soon full) wrapped channel.
	src <- &schemas.BifrostStreamChunk{BifrostChatResponse: &schemas.BifrostChatResponse{ID: "1"}}
	src <- &schemas.BifrostStreamChunk{BifrostChatResponse: &schemas.BifrostChatResponse{ID: "2"}}
	cancel()
	src <- &schemas.BifrostStreamChunk{BifrostChatResponse: &schemas.BifrostChatResponse{ID: "3"}}
	close(src)

	select {
	case <-drainDone:
	case <-time.After(time.Second):
		t.Fatal("drainDone did not close after ctx cancel; forwarder goroutine leaked")
	}
}
