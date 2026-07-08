package integrations

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream"
)

// TestBedrockKeepaliveFrameDecodes verifies that the pre-encoded Bedrock keepalive
// frame is a well-formed AWS EventStream message that decodes cleanly with the same
// decoder the SDK clients use. A malformed frame (bad CRC / truncated prelude) would
// make every downstream decoder error before reading headers, so this guards the
// framing itself.
func TestBedrockKeepaliveFrameDecodes(t *testing.T) {
	if len(bedrockKeepaliveFrame) == 0 {
		t.Fatal("bedrockKeepaliveFrame is empty; encoding failed at init")
	}

	decoder := eventstream.NewDecoder()
	msg, err := decoder.Decode(bytes.NewReader(bedrockKeepaliveFrame), nil)
	if err != nil {
		t.Fatalf("failed to decode keepalive frame: %v", err)
	}

	// :message-type MUST be "event" — this is what makes SDK decoders treat an
	// unknown :event-type as an ignorable/unmodeled event rather than routing it to
	// the exception/error path (which returns a 400 / synthesizes an API error).
	if got := msg.Headers.Get(":message-type").String(); got != "event" {
		t.Errorf(":message-type = %q, want \"event\"", got)
	}

	// :event-type MUST be a proprietary name outside the Bedrock response union so it
	// can never collide with a current or future AWS-modeled event.
	if got := msg.Headers.Get(":event-type").String(); got != "bifrost-ping" {
		t.Errorf(":event-type = %q, want \"bifrost-ping\"", got)
	}

	if got := string(msg.Payload); got != "{}" {
		t.Errorf("payload = %q, want \"{}\"", got)
	}
}

// TestBedrockKeepaliveFrameIsNotAnException guards the critical safety property: the
// keepalive must never carry an :exception-type or an error/exception :message-type,
// which would cause botocore to surface a 400 and go-v2 to synthesize an API error.
func TestBedrockKeepaliveFrameIsNotAnException(t *testing.T) {
	decoder := eventstream.NewDecoder()
	msg, err := decoder.Decode(bytes.NewReader(bedrockKeepaliveFrame), nil)
	if err != nil {
		t.Fatalf("failed to decode keepalive frame: %v", err)
	}

	if mt := msg.Headers.Get(":message-type").String(); mt == "exception" || mt == "error" {
		t.Errorf(":message-type = %q must not be an error/exception type", mt)
	}
	if exc := msg.Headers.Get(":exception-type"); exc != nil {
		t.Errorf(":exception-type header present (%q); keepalive must not look like an exception", exc.String())
	}
}
