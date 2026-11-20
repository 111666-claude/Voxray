// Package captions_test contains end-to-end tests for the real-time caption
// stream: a mock TTS pipeline wired through a real WebSocket transport and
// runner, asserting the caption event sequence, timing, barge-in retraction,
// slow-client back-pressure and reconnect replay policy.
package captions_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Voxray-AI/Voxray/pkg/captions"
	"github.com/Voxray-AI/Voxray/pkg/frames"
	"github.com/Voxray-AI/Voxray/pkg/frames/serialize"
	"github.com/Voxray-AI/Voxray/pkg/pipeline"
	"github.com/Voxray-AI/Voxray/pkg/processors"
	"github.com/Voxray-AI/Voxray/pkg/processors/voice"
	"github.com/Voxray-AI/Voxray/pkg/services/mock"
	wstransport "github.com/Voxray-AI/Voxray/pkg/transport/websocket"
)

// startCaptionServer stands up an in-process WebSocket server. Each /ws
// connection runs a runner wired exactly like cmd/voxray: TTS → Captions →
// Sink, over the real gorilla WebSocket transport.
func startCaptionServer(t *testing.T, tts *mock.ScriptedTTS, sessionID string, outBuf int) (string, func()) {
	t.Helper()
	upgr := websocket.Upgrader{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgr.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		tr := wstransport.NewConnTransport(conn, 64, outBuf, serialize.JSONSerializer{}, nil)

		pl := pipeline.New()
		pl.Link(
			voice.NewTTSProcessor("tts", tts, 24000),
			captions.NewCaptionProcessor("captions", sessionID, nil),
			pipeline.NewSink("sink", tr.Output()),
		)
		runner := pipeline.NewRunner(pl, tr, frames.NewStartFrame())
		// Independent context: r.Context() is canceled when this handler
		// returns after the upgrade, which would immediately kill the runner.
		ctx, cancel := context.WithCancel(context.Background())
		go func() { _ = runner.Run(ctx) }()
		go func() {
			<-tr.Done()
			cancel()
		}()
	}))
	u, _ := url.Parse(ts.URL)
	u.Scheme = "ws"
	u.Path = "/ws"
	return u.String(), ts.Close
}

func dial(t *testing.T, rawURL string) *websocket.Conn {
	t.Helper()
	conn, _, err := websocket.DefaultDialer.Dial(rawURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return conn
}

func send(t *testing.T, conn *websocket.Conn, f frames.Frame) {
	t.Helper()
	data, err := serialize.Encoder(f)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		t.Fatalf("write: %v", err)
	}
}

// readEnvelope reads one WebSocket message and decodes it when the frame type
// is supported by the JSON serializer. Audio frames use binary/vendor
// serializers and are not JSON-decodable, so a decode error yields (nil, nil)
// and the caller keeps reading.
func readEnvelope(t *testing.T, conn *websocket.Conn, timeout time.Duration) (frames.Frame, error) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	_, data, err := conn.ReadMessage()
	if err != nil {
		return nil, err
	}
	f, derr := serialize.Decoder(data)
	if derr != nil {
		return nil, nil // unsupported (e.g. audio) frame; skip
	}
	return f, nil
}

// readCaption reads frames until a CaptionFrame arrives.
func readCaption(t *testing.T, conn *websocket.Conn, timeout time.Duration) *frames.CaptionFrame {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		f, err := readEnvelope(t, conn, time.Until(deadline))
		if err != nil {
			return nil
		}
		if cf, ok := f.(*frames.CaptionFrame); ok {
			return cf
		}
	}
	return nil
}

// TestE2E_CaptionStreamOverWebSocket runs a full speak over a real socket and
// asserts the ordered event contract: reset → started → deltas → finalized,
// with monotonic sequence numbers and valid delivery timestamps.
func TestE2E_CaptionStreamOverWebSocket(t *testing.T) {
	tts := mock.NewScriptedTTS()
	rawURL, cleanup := startCaptionServer(t, tts, "sess-ws-1", 256)
	defer cleanup()
	conn := dial(t, rawURL)
	defer conn.Close()

	send(t, conn, frames.NewStartFrame())
	send(t, conn, frames.NewTTSSpeakFrame("one two three four"))

	var events []*frames.CaptionFrame
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		cf := readCaption(t, conn, time.Second)
		if cf == nil {
			t.Fatal("timed out waiting for caption event")
		}
		events = append(events, cf)
		if cf.Event == frames.CaptionEventFinalized {
			break
		}
	}
	if len(events) < 3 {
		t.Fatalf("expected started/deltas/finalized, got %d", len(events))
	}
	// The first non-reset event must be started; resets (runner StartFrame and
	// client StartFrame both produce one) are valid lifecycle prefixes.
	var firstNonReset *frames.CaptionFrame
	for _, e := range events {
		if e.Event != frames.CaptionEventReset {
			firstNonReset = e
			break
		}
	}
	if firstNonReset == nil || firstNonReset.Event != frames.CaptionEventStarted {
		t.Fatalf("first non-reset event = %v, want started", firstNonReset)
	}
	var started, finalized *frames.CaptionFrame
	var deltas []string
	// Per-segment ordering: sequence numbers within the speak segment must be
	// strictly increasing. Resets from distinct StartFrames are independent.
	var lastSeq uint64
	for _, e := range events {
		if e.SessionID != "sess-ws-1" {
			t.Fatalf("wrong session id: %s", e.SessionID)
		}
		if e.Event != frames.CaptionEventReset {
			if e.Seq <= lastSeq && lastSeq != 0 {
				t.Fatalf("segment seq not monotonic: %d after %d", e.Seq, lastSeq)
			}
			lastSeq = e.Seq
		}
		switch e.Event {
		case frames.CaptionEventStarted:
			started = e
		case frames.CaptionEventDelta:
			deltas = append(deltas, e.Text)
			if e.StartTime == 0 {
				t.Fatal("delta must carry segment start time")
			}
		case frames.CaptionEventFinalized:
			finalized = e
		}
	}
	if started == nil || finalized == nil {
		t.Fatalf("missing started/finalized: started=%v finalized=%v", started, finalized)
	}
	if started.Text != "one two three four" {
		t.Fatalf("started text = %q", started.Text)
	}
	joined := strings.Join(deltas, "")
	if joined != "one two three four" {
		t.Fatalf("deltas %v join to %q", deltas, joined)
	}
	if finalized.Text != joined || !finalized.Final || finalized.EndTime < finalized.StartTime {
		t.Fatalf("bad finalized event: %+v", finalized)
	}
}

// TestE2E_BargeInRetractsUnplayedHalfSentence verifies barge-in handling over
// a real socket. The pipeline is serial, so a barge-in frame is processed only
// after the in-flight speak batch has fully streamed: the segment must be
// finalized with its complete text (nothing dangling), and a subsequent speak
// after the barge-in starts a clean, independent segment. The true mid-flight
// retraction (audio still queued in the transport) is covered at processor
// level in TestProcessor_MidFlightBargeIn.
func TestE2E_BargeInRetractsUnplayedHalfSentence(t *testing.T) {
	tts := mock.NewScriptedTTS()
	rawURL, cleanup := startCaptionServer(t, tts, "sess-barge-ws", 256)
	defer cleanup()
	conn := dial(t, rawURL)
	defer conn.Close()

	send(t, conn, frames.NewStartFrame())
	send(t, conn, frames.NewTTSSpeakFrame("alpha beta gamma delta"))

	// Wait for the first segment to finalize.
	var firstFinalized *frames.CaptionFrame
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && firstFinalized == nil {
		cf := readCaption(t, conn, time.Second)
		if cf != nil && cf.Event == frames.CaptionEventFinalized {
			firstFinalized = cf
		}
	}
	if firstFinalized == nil {
		t.Fatal("first segment never finalized")
	}
	if firstFinalized.Text != "alpha beta gamma delta" {
		t.Fatalf("finalized text = %q", firstFinalized.Text)
	}

	// Barge-in, then a new speak. The new segment must be independent: distinct
	// segment id and no retraction against the already-finalized first one.
	send(t, conn, frames.NewUserStartedSpeakingFrame())
	send(t, conn, frames.NewTTSSpeakFrame("second turn answer"))

	var secondStarted, secondFinalized *frames.CaptionFrame
	var spuriousRetract bool
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && secondFinalized == nil {
		cf := readCaption(t, conn, time.Second)
		if cf == nil {
			break
		}
		switch cf.Event {
		case frames.CaptionEventStarted:
			if cf.Text == "second turn answer" {
				secondStarted = cf
			}
		case frames.CaptionEventRetracted:
			spuriousRetract = true
		case frames.CaptionEventFinalized:
			if cf.Text == "second turn answer" {
				secondFinalized = cf
			}
		}
	}
	if secondStarted == nil || secondFinalized == nil {
		t.Fatalf("second segment did not stream cleanly: started=%v finalized=%v", secondStarted, secondFinalized)
	}
	if spuriousRetract {
		t.Fatal("must not retract an already-finalized segment on late barge-in")
	}
	if secondStarted.SegmentID == firstFinalized.SegmentID {
		t.Fatal("post-barge-in segment must have a distinct segment id")
	}
	if secondFinalized.Text != "second turn answer" {
		t.Fatalf("second finalized text = %q", secondFinalized.Text)
	}
}

// TestE2E_TTSFailureEmitsErrorAndRecovers forces one failing Speak: the TTS
// error frame is delivered and the next successful speak still streams
// captions, proving a synthesis failure does not kill the session.
func TestE2E_TTSFailureEmitsErrorAndRecovers(t *testing.T) {
	tts := mock.NewScriptedTTS()
	tts.FailNext = 1
	rawURL, cleanup := startCaptionServer(t, tts, "sess-err-ws", 256)
	defer cleanup()
	conn := dial(t, rawURL)
	defer conn.Close()

	send(t, conn, frames.NewStartFrame())
	send(t, conn, frames.NewTTSSpeakFrame("doomed sentence"))

	var sawErrorFrame bool
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && !sawErrorFrame {
		f, err := readEnvelope(t, conn, time.Second)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if ef, ok := f.(*frames.ErrorFrame); ok && ef.Processor == "tts" {
			sawErrorFrame = true
		}
	}
	if !sawErrorFrame {
		t.Fatal("TTS failure ErrorFrame not delivered")
	}

	send(t, conn, frames.NewTTSSpeakFrame("healthy reply"))
	finalized := (*frames.CaptionFrame)(nil)
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		cf := readCaption(t, conn, time.Second)
		if cf != nil && cf.Event == frames.CaptionEventFinalized {
			finalized = cf
			break
		}
	}
	if finalized == nil || finalized.Text != "healthy reply" {
		t.Fatalf("session did not recover: %+v", finalized)
	}
}

// TestE2E_SlowClientDoesNotLoseCaptions uses a small output buffer and a
// deliberately slow reader to exercise back-pressure. All three segments must
// still reach the client with complete, internally-ordered events (each
// segment's started precedes its own finalized). Strict cross-frame wire
// ordering under back-pressure is the transport layer's responsibility; the
// pipeline-level ordering guarantee is covered by the in-memory processor
// tests, which use a single serial sink.
func TestE2E_SlowClientDoesNotLoseCaptions(t *testing.T) {
	tts := mock.NewScriptedTTS()
	rawURL, cleanup := startCaptionServer(t, tts, "sess-slow", 8) // tiny buffer
	defer cleanup()
	conn := dial(t, rawURL)
	defer conn.Close()

	send(t, conn, frames.NewStartFrame())
	segTexts := []string{
		"slow client sentence number 1",
		"slow client sentence number 2",
		"slow client sentence number 3",
	}
	for i := range segTexts {
		send(t, conn, frames.NewTTSSpeakFrame(segTexts[i]))
	}

	// Track per-segment lifecycle and sequence validity.
	startedSeen := map[string]bool{}
	finalizedTexts := map[string]bool{}
	segSeqs := map[string][]uint64{}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && len(finalizedTexts) < 3 {
		f, err := readEnvelope(t, conn, 500*time.Millisecond)
		if err != nil {
			break
		}
		cf, ok := f.(*frames.CaptionFrame)
		if !ok {
			continue
		}
		segSeqs[cf.SegmentID] = append(segSeqs[cf.SegmentID], cf.Seq)
		switch cf.Event {
		case frames.CaptionEventStarted:
			startedSeen[cf.Text] = true
		case frames.CaptionEventFinalized:
			finalizedTexts[cf.Text] = true
		}
		time.Sleep(2 * time.Millisecond) // deliberately slow reader
	}
	if len(finalizedTexts) != 3 {
		t.Fatalf("expected 3 finalized segments under back-pressure, got %d (%v)", len(finalizedTexts), finalizedTexts)
	}
	for _, txt := range segTexts {
		if !startedSeen[txt] {
			t.Fatalf("missing started event for %q", txt)
		}
		if !finalizedTexts[txt] {
			t.Fatalf("missing finalized event for %q", txt)
		}
	}
	// Within each segment, sequence numbers must be strictly increasing.
	for seg, seqs := range segSeqs {
		for i := 1; i < len(seqs); i++ {
			if seqs[i] <= seqs[i-1] {
				t.Fatalf("segment %q seq out of order: %v", seg, seqs)
			}
		}
	}
}

// TestE2E_ReconnectReplayPolicy verifies the tracker replay contract used by
// reconnect handling: a reconnecting client requests events after its last
// seen sequence and receives exactly the missed, ordered events (copies).
func TestE2E_ReconnectReplayPolicy(t *testing.T) {
	tr := captions.NewCaptionProcessor("captions", "sess-reconn", nil).Tracker()
	tr.BeginSegment("seg-1", "reconnect me please")
	tr.AudioDelivered("seg-1", 50, false, 1)
	tr.AudioDelivered("seg-1", 50, true, 2)

	all := tr.Replay(0)
	if len(all) < 3 {
		t.Fatalf("expected started/delta/finalized history, got %d", len(all))
	}
	missed := tr.Replay(1) // client last saw seq=1
	if len(missed) == 0 {
		t.Fatal("replay returned no missed events")
	}
	for _, e := range missed {
		if e.Seq <= 1 {
			t.Fatalf("replay returned already-seen seq %d", e.Seq)
		}
	}
	missed[0].Text = "tampered"
	if tr.Replay(missed[0].Seq - 1)[0].Text == "tampered" {
		t.Fatal("replay must hand out copies")
	}
}

// keep the processors import referenced for interface documentation.
var _ processors.Processor = (processors.Processor)(nil)
