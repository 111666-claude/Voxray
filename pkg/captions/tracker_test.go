package captions

import (
	"testing"
	"time"

	"github.com/Voxray-AI/Voxray/pkg/frames"
)

// pinnedClock returns a controllable time for deterministic timestamp tests.
type pinnedClock struct{ t time.Time }

func (c *pinnedClock) now() time.Time { return c.t }

func (c *pinnedClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestTracker() (*Tracker, *pinnedClock) {
	clk := &pinnedClock{t: time.Unix(1_700_000_000, 0)}
	return NewTracker("sess-1", clk.now), clk
}

func collect(events []*frames.CaptionFrame) []string {
	out := make([]string, 0, len(events))
	for _, e := range events {
		out = append(out, e.Event)
	}
	return out
}

func TestTracker_FullLifecycleEventsAndTiming(t *testing.T) {
	tr, clk := newTestTracker()

	start := tr.BeginSegment("seg-1", "one two three four")
	if start.Event != frames.CaptionEventStarted || start.Text != "one two three four" {
		t.Fatalf("bad started event: %+v", start)
	}
	if start.Seq != 1 || start.SessionID != "sess-1" || start.SegmentID != "seg-1" {
		t.Fatalf("bad event identity: %+v", start)
	}
	wantStart := clk.t.UnixNano()
	if start.StartTime != wantStart {
		t.Fatalf("start time = %d want %d", start.StartTime, wantStart)
	}

	// Deliver four 50 ms chunks. Timestamps must reflect each delivery instant.
	clk.advance(10 * time.Millisecond)
	e1 := tr.AudioDelivered("seg-1", 50, false, 101)
	clk.advance(10 * time.Millisecond)
	e2 := tr.AudioDelivered("seg-1", 50, false, 102)
	clk.advance(10 * time.Millisecond)
	e3 := tr.AudioDelivered("seg-1", 50, false, 103)
	clk.advance(10 * time.Millisecond)
	e4 := tr.AudioDelivered("seg-1", 50, true, 104)

	var deltas string
	for _, evs := range [][]*frames.CaptionFrame{e1, e2, e3} {
		for _, e := range evs {
			if e.Event != frames.CaptionEventDelta {
				t.Fatalf("expected delta only mid-segment, got %s", e.Event)
			}
			deltas += e.Text
		}
	}
	// Final chunk returns delta + finalized.
	if len(e4) != 2 {
		t.Fatalf("final chunk should yield delta+finalized, got %d events", len(e4))
	}
	if e4[0].Event != frames.CaptionEventDelta || e4[1].Event != frames.CaptionEventFinalized {
		t.Fatalf("bad final-chunk events: %v", collect(e4))
	}
	deltas += e4[0].Text
	if deltas != "one two three four" {
		t.Fatalf("delta join mismatch: %q", deltas)
	}

	final := e4[1]
	if !final.Final || final.Text != "one two three four" {
		t.Fatalf("bad finalized event: %+v", final)
	}
	if final.EndTime != clk.t.UnixNano() {
		t.Fatalf("end time should be the last delivery instant")
	}
	if final.StartTime != wantStart {
		t.Fatalf("start time should be preserved on finalize")
	}
	// Sequence numbers are strictly increasing: started(1), three mid
	// deltas(2-4), final delta(5), finalized(6).
	if final.Seq != 6 {
		t.Fatalf("expected seq 6, got %d", final.Seq)
	}
}

func TestTracker_InterruptFinalizesPlayedAndRetractsRest(t *testing.T) {
	tr, _ := newTestTracker()
	tr.BeginSegment("seg-1", "alpha beta gamma delta")
	// Deliver half the audio (2 of 4 chunks) before barge-in.
	tr.AudioDelivered("seg-1", 50, false, 1)
	tr.AudioDelivered("seg-1", 50, false, 2)

	finalEv, retractEv := tr.Interrupt(99)
	if finalEv == nil || retractEv == nil {
		t.Fatalf("expected both finalize and retract events, got final=%v retract=%v", finalEv, retractEv)
	}
	if finalEv.Event != frames.CaptionEventFinalized || !finalEv.Final {
		t.Fatalf("bad finalize: %+v", finalEv)
	}
	if retractEv.Event != frames.CaptionEventRetracted || !retractEv.Final {
		t.Fatalf("bad retract: %+v", retractEv)
	}
	// Played prefix plus retracted suffix must reconstruct the full sentence.
	if finalEv.Text+retractEv.Text != "alpha beta gamma delta" {
		t.Fatalf("played=%q retracted=%q", finalEv.Text, retractEv.Text)
	}
	if retractEv.PlayedText != finalEv.Text {
		t.Fatalf("retract should echo played text, got %q", retractEv.PlayedText)
	}
	// A second interrupt on the finished segment is a no-op.
	if f, r := tr.Interrupt(100); f != nil || r != nil {
		t.Fatal("interrupt after finish must be a no-op")
	}
}

func TestTracker_InterruptBeforeAnyAudioRetractsWholeSegment(t *testing.T) {
	tr, _ := newTestTracker()
	tr.BeginSegment("seg-1", "nothing played yet")
	finalEv, retractEv := tr.Interrupt(1)
	if finalEv != nil {
		t.Fatalf("no finalize expected before audio, got %+v", finalEv)
	}
	if retractEv == nil || retractEv.Text != "nothing played yet" {
		t.Fatalf("whole interim text must be retracted, got %+v", retractEv)
	}
}

func TestTracker_TTSFailedRetractsInterim(t *testing.T) {
	tr, _ := newTestTracker()
	tr.BeginSegment("seg-1", "partial sentence here")
	tr.AudioDelivered("seg-1", 50, false, 1)
	errEv := tr.TTSFailed("provider unavailable")
	if errEv == nil || errEv.Event != frames.CaptionEventError {
		t.Fatalf("expected error event, got %+v", errEv)
	}
	if !errEv.Final {
		t.Fatal("error event must be final")
	}
	// History must contain the retraction before the error.
	hist := tr.Replay(0)
	sawRetract := false
	for _, h := range hist {
		if h.Event == frames.CaptionEventRetracted {
			sawRetract = true
		}
		if h.Event == frames.CaptionEventError && !sawRetract {
			t.Fatal("retraction must precede error event")
		}
	}
}

func TestTracker_UnknownAndLateSegmentAudioIgnored(t *testing.T) {
	tr, _ := newTestTracker()
	// No active segment.
	if evs := tr.AudioDelivered("ghost", 50, false, 1); evs != nil {
		t.Fatal("audio for unknown segment must be ignored")
	}
	tr.BeginSegment("seg-1", "a b")
	tr.AudioDelivered("seg-1", 50, true, 2) // finishes the segment
	if evs := tr.AudioDelivered("seg-1", 50, false, 3); evs != nil {
		t.Fatal("audio after segment end must be ignored")
	}
	if evs := tr.AudioDelivered("other", 50, false, 4); evs != nil {
		t.Fatal("audio for a different segment must be ignored")
	}
}

func TestTracker_ResetEmitsResetAndClearsState(t *testing.T) {
	tr, _ := newTestTracker()
	tr.BeginSegment("seg-1", "stale")
	reset := tr.Reset()
	if reset == nil || reset.Event != frames.CaptionEventReset || !reset.Final {
		t.Fatalf("bad reset event: %+v", reset)
	}
	// The old segment is gone: interrupt produces nothing.
	if f, r := tr.Interrupt(1); f != nil || r != nil {
		t.Fatal("reset must clear active segment")
	}
	// A fresh segment starts a new sequence after the reset.
	start := tr.BeginSegment("seg-2", "fresh")
	if start.Seq <= reset.Seq {
		t.Fatal("new segment sequence must continue after reset")
	}
}

func TestTracker_ReplayReturnsEventsAfterSeq(t *testing.T) {
	tr, _ := newTestTracker()
	tr.BeginSegment("seg-1", "a b c")
	tr.AudioDelivered("seg-1", 50, false, 1)
	tr.AudioDelivered("seg-1", 50, false, 2)
	tr.AudioDelivered("seg-1", 50, true, 3)

	all := tr.Replay(0)
	if len(all) < 3 {
		t.Fatalf("expected at least 3 historical events, got %d", len(all))
	}
	// Replay from a middle seq.
	mid := all[1].Seq
	rest := tr.Replay(mid)
	for _, e := range rest {
		if e.Seq <= mid {
			t.Fatalf("replay returned event with seq %d <= %d", e.Seq, mid)
		}
	}
	// Replayed frames are copies, not shared pointers.
	rest[0].Text = "mutated"
	if tr.Replay(rest[0].Seq - 1)[0].Text == "mutated" {
		t.Fatal("replay must return copies")
	}
}

func TestTracker_HistoryBounded(t *testing.T) {
	tr, _ := newTestTracker()
	tr.maxHist = 4
	for i := 0; i < 10; i++ {
		tr.Reset()
	}
	hist := tr.Replay(0)
	if len(hist) > 4 {
		t.Fatalf("history must be bounded to 4, got %d", len(hist))
	}
}
