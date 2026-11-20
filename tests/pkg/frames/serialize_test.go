package frames_test

import (
	"testing"

	"github.com/Voxray-AI/Voxray/pkg/frames"
	"github.com/Voxray-AI/Voxray/pkg/frames/serialize"
)

// TestEncoderDecoderRoundTrip verifies that frames can be encoded to the JSON
// envelope format and decoded back while preserving their type and key fields.
func TestEncoderDecoderRoundTrip(t *testing.T) {
	text := frames.NewTextFrame("hello world")
	data, err := serialize.Encoder(text)
	if err != nil {
		t.Fatalf("Encoder returned error: %v", err)
	}

	decoded, err := serialize.Decoder(data)
	if err != nil {
		t.Fatalf("Decoder returned error: %v", err)
	}

	tf, ok := decoded.(*frames.TextFrame)
	if !ok {
		t.Fatalf("expected *frames.TextFrame, got %T", decoded)
	}
	if tf.Text != text.Text {
		t.Fatalf("expected text %q, got %q", text.Text, tf.Text)
	}
	if !tf.AppendToContext {
		t.Fatalf("expected AppendToContext to be true")
	}
}

// TestProtoBinaryRoundTrip verifies binary protobuf frame encode/decode.
func TestProtoBinaryRoundTrip(t *testing.T) {
	text := frames.NewTextFrame("wire test")
	data, err := serialize.ProtoEncode(text)
	if err != nil {
		t.Fatalf("ProtoEncode: %v", err)
	}
	if data == nil {
		t.Fatal("ProtoEncode returned nil")
	}
	decoded, err := serialize.ProtoDecode(data)
	if err != nil {
		t.Fatalf("ProtoDecode: %v", err)
	}
	tf, ok := decoded.(*frames.TextFrame)
	if !ok {
		t.Fatalf("expected *frames.TextFrame, got %T", decoded)
	}
	if tf.Text != text.Text {
		t.Fatalf("expected text %q, got %q", text.Text, tf.Text)
	}
}

// TestProtoEnvelopeRoundTrip verifies binary envelope (wire.FrameEnvelope) encode/decode.
func TestProtoEnvelopeRoundTrip(t *testing.T) {
	text := frames.NewTextFrame("envelope test")
	data, err := serialize.ProtoEncoder(text)
	if err != nil {
		t.Fatalf("ProtoEncoder: %v", err)
	}
	decoded, err := serialize.ProtoDecoder(data)
	if err != nil {
		t.Fatalf("ProtoDecoder: %v", err)
	}
	tf, ok := decoded.(*frames.TextFrame)
	if !ok {
		t.Fatalf("expected *frames.TextFrame, got %T", decoded)
	}
	if tf.Text != text.Text {
		t.Fatalf("expected text %q, got %q", text.Text, tf.Text)
	}
}

// TestTransportMessageFrameRoundTrip verifies TransportMessageFrame in JSON envelope.
func TestTransportMessageFrameRoundTrip(t *testing.T) {
	msg := frames.NewTransportMessageFrame(map[string]any{"event": "media", "payload": "base64data"})
	data, err := serialize.Encoder(msg)
	if err != nil {
		t.Fatalf("Encoder: %v", err)
	}
	decoded, err := serialize.Decoder(data)
	if err != nil {
		t.Fatalf("Decoder: %v", err)
	}
	tm, ok := decoded.(*frames.TransportMessageFrame)
	if !ok {
		t.Fatalf("expected *frames.TransportMessageFrame, got %T", decoded)
	}
	if tm.Message["event"] != "media" {
		t.Fatalf("expected event=media, got %v", tm.Message["event"])
	}
}

// TestInterruptionFrameRoundTrip verifies InterruptionFrame encode/decode.
func TestInterruptionFrameRoundTrip(t *testing.T) {
	f := frames.NewInterruptionFrame()
	data, err := serialize.Encoder(f)
	if err != nil {
		t.Fatalf("Encoder: %v", err)
	}
	decoded, err := serialize.Decoder(data)
	if err != nil {
		t.Fatalf("Decoder: %v", err)
	}
	if _, ok := decoded.(*frames.InterruptionFrame); !ok {
		t.Fatalf("expected *frames.InterruptionFrame, got %T", decoded)
	}
}

// TestInputDTMFFrameRoundTrip verifies InputDTMFFrame encode/decode.
func TestInputDTMFFrameRoundTrip(t *testing.T) {
	kp, _ := frames.ParseKeypadEntry("5")
	f, err := frames.NewInputDTMFFrame(kp)
	if err != nil {
		t.Fatalf("NewInputDTMFFrame: %v", err)
	}
	data, err := serialize.Encoder(f)
	if err != nil {
		t.Fatalf("Encoder: %v", err)
	}
	decoded, err := serialize.Decoder(data)
	if err != nil {
		t.Fatalf("Decoder: %v", err)
	}
	in, ok := decoded.(*frames.InputDTMFFrame)
	if !ok {
		t.Fatalf("expected *frames.InputDTMFFrame, got %T", decoded)
	}
	if in.Digit != kp {
		t.Fatalf("expected digit %q, got %q", kp, in.Digit)
	}
}

func TestDecodeByType_UnknownType(t *testing.T) {
	_, err := serialize.DecodeByType("UnknownFrameType", []byte("{}"))
	if err == nil {
		t.Error("DecodeByType(unknown type) should return error")
	}
}

// TestCaptionFrameJSONRoundTrip verifies every caption event type survives the
// JSON envelope wire format with session, sequence and timing fields intact.
func TestCaptionFrameJSONRoundTrip(t *testing.T) {
	cases := []struct {
		event string
		text  string
		final bool
	}{
		{frames.CaptionEventStarted, "full interim sentence", false},
		{frames.CaptionEventDelta, "partial ", false},
		{frames.CaptionEventFinalized, "played words", true},
		{frames.CaptionEventRetracted, "unplayed suffix", true},
		{frames.CaptionEventError, "tts unavailable", true},
		{frames.CaptionEventReset, "", true},
	}
	for _, tc := range cases {
		f := frames.NewCaptionFrame("sess-42", "tts-7", 99, tc.event, tc.text)
		f.PlayedText = "played "
		f.StartTime = 1_700_000_000_000_000_000
		f.EndTime = 1_700_000_000_500_000_000
		f.AudioFrameID = 555
		f.Final = tc.final

		data, err := serialize.Encoder(f)
		if err != nil {
			t.Fatalf("%s: Encoder: %v", tc.event, err)
		}
		decoded, err := serialize.Decoder(data)
		if err != nil {
			t.Fatalf("%s: Decoder: %v", tc.event, err)
		}
		cf, ok := decoded.(*frames.CaptionFrame)
		if !ok {
			t.Fatalf("%s: expected *CaptionFrame, got %T", tc.event, decoded)
		}
		if cf.SessionID != "sess-42" || cf.SegmentID != "tts-7" || cf.Seq != 99 {
			t.Fatalf("%s: identity fields lost: %+v", tc.event, cf)
		}
		if cf.Event != tc.event || cf.Text != tc.text || cf.Final != tc.final {
			t.Fatalf("%s: payload mismatch: %+v", tc.event, cf)
		}
		if cf.StartTime != f.StartTime || cf.EndTime != f.EndTime || cf.AudioFrameID != 555 {
			t.Fatalf("%s: timing fields lost: %+v", tc.event, cf)
		}
	}
}

// TestCaptionFrameSkippedByProtobuf ensures the binary protobuf path skips
// caption frames gracefully (captions travel over JSON, never breaking the
// shared binary audio wire format).
func TestCaptionFrameSkippedByProtobuf(t *testing.T) {
	f := frames.NewCaptionFrame("s", "seg", 1, frames.CaptionEventDelta, "x")
	data, err := serialize.ProtoEncode(f)
	if err != nil {
		t.Fatalf("ProtoEncode: %v", err)
	}
	if data != nil {
		t.Fatalf("caption frames must be skipped by protobuf, got %d bytes", len(data))
	}
}
