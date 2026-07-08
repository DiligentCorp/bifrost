package utils

import (
	"context"
	"time"

	schemas "github.com/maximhq/bifrost/core/schemas"
)

// CheckFirstStreamChunkForError reads the first chunk from a streaming channel to detect
// errors returned inside HTTP 200 SSE streams (e.g., providers that send rate limit
// errors as SSE events instead of HTTP 429).
//
// If the first chunk is an error, it drains the source channel in the background
// (so the provider goroutine can exit cleanly) and returns the error for synchronous
// handling, enabling retries and fallbacks. The returned drainDone channel is closed
// once the drain completes — callers must wait on it before releasing any resources
// (e.g., plugin pipelines) that the provider goroutine's postHookRunner may still reference.
//
// If the first chunk is valid data, it returns a wrapped channel that re-emits
// the first chunk followed by all remaining chunks from the source. drainDone is
// closed when the wrapper goroutine finishes forwarding the source stream.
//
// If the source channel is closed immediately (empty stream), it returns a
// nil channel with nil error. drainDone is already closed.
//
// firstChunkTimeout bounds how long the first-chunk peek may block. It exists to
// resolve an unavoidable tension: detecting an embedded error requires holding the
// response uncommitted until the first chunk arrives, but a caller that must keep the
// connection warm during a slow first token (e.g. one emitting SSE keepalive frames)
// cannot wait indefinitely — a keepalive is a response-body frame and so forces the
// response to be committed. When the timeout elapses before the first chunk arrives,
// the stream is committed as-is: every chunk (including the still-pending first one) is
// forwarded verbatim, and a later error is delivered in-band rather than triggering a
// retry (which is no longer possible once bytes reach the client). A non-positive
// timeout waits unbounded, preserving full retry-on-first-chunk-error semantics for
// callers with no liveness deadline (direct SDK use, non-streaming intermediaries).
//
// The ctx argument cancels the background forwarding goroutine if the consumer
// abandons the returned wrapped channel. On ctx.Done the goroutine drains the
// source stream so the upstream provider's blocked send can exit cleanly.
func CheckFirstStreamChunkForError(
	ctx context.Context,
	stream chan *schemas.BifrostStreamChunk,
	firstChunkTimeout time.Duration,
) (chan *schemas.BifrostStreamChunk, <-chan struct{}, *schemas.BifrostError) {
	firstChunk, outcome := receiveFirstChunk(stream, firstChunkTimeout)
	switch outcome {
	case firstChunkClosed:
		// Source closed immediately (empty stream) — return nil so callers can
		// distinguish this from a live stream channel.
		done := make(chan struct{})
		close(done)
		return nil, done, nil
	case firstChunkTimedOut:
		// The liveness deadline elapsed before the first chunk arrived. We hold no
		// chunk (none was received, so none can be lost); commit the stream and
		// forward everything — including the still-pending first chunk — verbatim
		// as it arrives.
		wrapped, done := forwardStream(ctx, stream, nil)
		return wrapped, done, nil
	}

	// Check if first chunk is an error
	if firstChunk.BifrostError != nil && firstChunk.BifrostError.Error != nil &&
		(firstChunk.BifrostError.Error.Message != "" || firstChunk.BifrostError.Error.Code != nil || firstChunk.BifrostError.Error.Type != nil) {
		// Drain source channel to let the provider goroutine exit cleanly
		done := make(chan struct{})
		go func() {
			defer close(done)
			for range stream {
			}
		}()
		return nil, done, firstChunk.BifrostError
	}

	// First chunk is valid data — wrap channel to re-inject it, then forward the rest.
	wrapped, done := forwardStream(ctx, stream, firstChunk)
	return wrapped, done, nil
}

// firstChunkOutcome distinguishes the three ways the first-chunk peek can resolve,
// so the caller never has to receive from the stream a second time (which would
// risk dropping a first chunk that arrived between a timeout and a re-check).
type firstChunkOutcome int

const (
	firstChunkReceived firstChunkOutcome = iota // a chunk is in hand
	firstChunkClosed                            // source closed before any chunk (empty stream)
	firstChunkTimedOut                          // deadline elapsed with the first chunk still pending
)

// receiveFirstChunk waits for the first chunk, bounded by timeout when positive.
// A non-positive timeout waits unbounded and never returns firstChunkTimedOut.
func receiveFirstChunk(stream chan *schemas.BifrostStreamChunk, timeout time.Duration) (*schemas.BifrostStreamChunk, firstChunkOutcome) {
	if timeout <= 0 {
		chunk, ok := <-stream
		if !ok {
			return nil, firstChunkClosed
		}
		return chunk, firstChunkReceived
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case chunk, ok := <-stream:
		if !ok {
			return nil, firstChunkClosed
		}
		return chunk, firstChunkReceived
	case <-timer.C:
		return nil, firstChunkTimedOut
	}
}

// forwardStream returns a channel that re-emits first (when non-nil) followed by
// every chunk from stream, and a done channel closed once forwarding finishes. If
// the consumer abandons the returned channel, ctx cancellation drains the source so
// the provider's blocked send unblocks and its goroutine can exit.
func forwardStream(ctx context.Context, stream chan *schemas.BifrostStreamChunk, first *schemas.BifrostStreamChunk) (chan *schemas.BifrostStreamChunk, <-chan struct{}) {
	done := make(chan struct{})
	wrapped := make(chan *schemas.BifrostStreamChunk, max(cap(stream), 1))
	if first != nil {
		wrapped <- first
	}
	go func() {
		defer close(done)
		defer close(wrapped)
		for chunk := range stream {
			select {
			case wrapped <- chunk:
			case <-ctx.Done():
				for range stream {
				}
				return
			}
		}
	}()
	return wrapped, done
}
