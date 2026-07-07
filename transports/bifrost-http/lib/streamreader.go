package lib

import (
	"io"
	"sync"
	"time"
)

// sseKeepaliveFrame is an SSE comment line. Per the Server-Sent Events spec, any
// line beginning with a colon is a comment that clients ignore, so it keeps the
// connection warm — resetting idle timers on the client and on any intermediaries
// (load balancers, reverse proxies, API gateways) — without appearing in the
// parsed event stream. It is only ever read from, never mutated, so a single
// shared slice is safe.
var sseKeepaliveFrame = []byte(": keepalive\n\n")

// SSEStreamReader is an io.ReadCloser that delivers one event per Read call,
// bypassing fasthttp's internal pipe mechanism (fasthttputil.PipeConns) which
// batches multiple events into single TCP segments.
//
// Usage:
//  1. Create with NewSSEStreamReader()
//  2. Pass to ctx.Response.SetBodyStream(reader, -1)
//  3. Start a producer goroutine that calls Send()/SendEvent()/SendError() for each event
//  4. Producer calls Done() when finished (closes the event channel)
//  5. fasthttp calls Close() on write errors (signals producer to stop)
type SSEStreamReader struct {
	eventCh   chan []byte
	closeCh   chan struct{}
	closeOnce sync.Once
	current   []byte // remaining bytes from a partial read

	// keepaliveInterval, when > 0, is how long Read will wait for a real event
	// before emitting a keepalive frame instead of continuing to block. Zero
	// disables keepalives (the default), preserving the original blocking Read.
	keepaliveInterval time.Duration
	// keepaliveFrame is the payload written when the interval elapses. Defaults
	// to sseKeepaliveFrame (an SSE comment); the Bedrock binary EventStream path
	// overrides it via WithKeepaliveFrame so it stays protocol-correct.
	keepaliveFrame []byte
}

// SSEStreamReaderOption configures an SSEStreamReader at construction time.
type SSEStreamReaderOption func(*SSEStreamReader)

// KeepaliveOptions builds the reader options for the configured keepalive interval
// (in seconds). A non-positive value returns no options, leaving keepalives
// disabled. This is the single place SSE producers translate the client config
// field into reader options.
func KeepaliveOptions(intervalSeconds int) []SSEStreamReaderOption {
	if intervalSeconds <= 0 {
		return nil
	}
	return []SSEStreamReaderOption{WithKeepalive(time.Duration(intervalSeconds) * time.Second)}
}

// WithKeepalive enables periodic keepalive frames. While the producer emits no
// real event, Read returns a keepalive frame every interval so the connection
// stays warm through idle-timeout enforcing intermediaries. A non-positive
// interval leaves keepalives disabled.
func WithKeepalive(interval time.Duration) SSEStreamReaderOption {
	return func(r *SSEStreamReader) {
		if interval > 0 {
			r.keepaliveInterval = interval
		}
	}
}

// WithKeepaliveFrame overrides the bytes emitted as a keepalive. Use it for
// non-SSE streams (e.g. the Bedrock binary EventStream) where the default SSE
// comment would corrupt the wire framing. An empty frame is ignored.
func WithKeepaliveFrame(frame []byte) SSEStreamReaderOption {
	return func(r *SSEStreamReader) {
		if len(frame) > 0 {
			r.keepaliveFrame = frame
		}
	}
}

// NewSSEStreamReader creates a new SSEStreamReader with a buffered event channel.
// Channel capacity of 1 allows one event of pipeline parallelism between
// the producer goroutine and fasthttp's writeBodyChunked loop.
func NewSSEStreamReader(opts ...SSEStreamReaderOption) *SSEStreamReader {
	r := &SSEStreamReader{
		eventCh:        make(chan []byte, 1),
		closeCh:        make(chan struct{}),
		keepaliveFrame: sseKeepaliveFrame,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Read implements io.Reader. It blocks until an event is available, then returns
// that event's bytes. If the caller's buffer is smaller than the event, remaining
// bytes are stored and returned on subsequent calls. Returns io.EOF when Done()
// has been called and all events have been consumed.
//
// When keepalives are enabled, a real event resets the interval; if the interval
// elapses with no event, Read returns a keepalive frame instead of blocking
// further. Read is only ever called by fasthttp's single write loop, so the
// keepalive timer needs no additional synchronization.
func (r *SSEStreamReader) Read(p []byte) (int, error) {
	if len(r.current) == 0 {
		event, err := r.next()
		if err != nil {
			return 0, err
		}
		r.current = event
	}
	n := copy(p, r.current)
	r.current = r.current[n:]
	return n, nil
}

// next returns the next frame to write: a real event, a keepalive frame if the
// keepalive interval elapses first, or io.EOF once the stream is done. A fresh
// timer is started per call, so the interval is measured from the previous
// frame — a steady flow of real events never triggers a keepalive.
func (r *SSEStreamReader) next() ([]byte, error) {
	if r.keepaliveInterval <= 0 {
		event, ok := <-r.eventCh
		if !ok {
			return nil, io.EOF
		}
		return event, nil
	}

	timer := time.NewTimer(r.keepaliveInterval)
	defer timer.Stop()
	select {
	case event, ok := <-r.eventCh:
		if !ok {
			return nil, io.EOF
		}
		return event, nil
	case <-timer.C:
		return r.keepaliveFrame, nil
	}
}

// Close implements io.Closer. Called by fasthttp when writeBodyChunked encounters
// a write error (client disconnect). Signals the producer goroutine to stop via closeCh.
// Safe to call multiple times.
func (r *SSEStreamReader) Close() error {
	r.closeOnce.Do(func() {
		close(r.closeCh)
	})
	return nil
}

// Send delivers a pre-formatted event to the reader. Returns false if the reader
// has been closed (client disconnected), in which case the producer should stop.
func (r *SSEStreamReader) Send(event []byte) bool {
	// Check closeCh first (non-blocking) to avoid sending after Close
	select {
	case <-r.closeCh:
		return false
	default:
	}
	select {
	case r.eventCh <- event:
		return true
	case <-r.closeCh:
		return false
	}
}

// SendEvent sends an SSE-framed event. If eventType is empty, it sends "data: <data>\n\n".
// If eventType is non-empty, it sends "event: <eventType>\ndata: <data>\n\n".
// Returns false if the reader has been closed (client disconnected).
func (r *SSEStreamReader) SendEvent(eventType string, data []byte) bool {
	var buf []byte
	if eventType != "" {
		buf = make([]byte, 0, 7+len(eventType)+7+len(data)+2)
		buf = append(buf, "event: "...)
		buf = append(buf, eventType...)
		buf = append(buf, "\ndata: "...)
	} else {
		buf = make([]byte, 0, 6+len(data)+2)
		buf = append(buf, "data: "...)
	}
	buf = append(buf, data...)
	buf = append(buf, '\n', '\n')
	return r.Send(buf)
}

// SendError sends an SSE error event: "event: error\ndata: <data>\n\n".
// Returns false if the reader has been closed (client disconnected).
func (r *SSEStreamReader) SendError(data []byte) bool {
	return r.SendEvent("error", data)
}

// SendDone sends the standard SSE done marker: "data: [DONE]\n\n".
// Returns false if the reader has been closed (client disconnected).
func (r *SSEStreamReader) SendDone() bool {
	return r.Send([]byte("data: [DONE]\n\n"))
}

// Done closes the event channel, signaling to Read that the stream is finished.
// Must be called exactly once by the producer goroutine when streaming is complete.
func (r *SSEStreamReader) Done() {
	close(r.eventCh)
}
