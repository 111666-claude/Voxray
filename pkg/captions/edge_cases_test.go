package captions

import (
	"testing"

	"github.com/Voxray-AI/Voxray/pkg/frames"
)

// TestTracker_EmptyTextSegmentIsHarmless verifies that a segment with no
// speakable text still finalizes cleanly without panicking or emitting
// malformed deltas (the TTS processor normally suppresses empty text before
// synthesis, but the tracker must remain safe on its own).
func TestTracker_EmptyTextSegmentIsHarmless(t *testing.T) {
	tr, _ := newTestTracker()
	start := tr.BeginSegment("seg-empty", "")
	if start == nil || start.Event != frames.CaptionEventStarted {
		t.Fatalf("empty-text segment should still emit started: %+v", start)
	}
	evs := tr.AudioDelivered("seg-empty", 50, true, 1)
	var hasFinal bool
	for _, e := range evs {
		if e.Event == frames.CaptionEventFinalized {
			hasFinal = true
			if e.Text != "" {
				t.Fatalf("finalized empty segment must have empty text, got %q", e.Text)
			}
		}
		if e.Event == frames.CaptionEventDelta && e.Text != "" {
			t.Fatal("empty segment must not emit non-empty deltas")
		}
	}
	if !hasFinal {
		t.Fatal("empty segment must finalize")
	}
}

// TestTracker_NewSegmentClosesPrevious ensures an interleaved/overlapping
// segment (e.g. a new Speak while an old one is somehow still open) settles
// the previous segment instead of corrupting its state.
func TestTracker_NewSegmentClosesPrevious(t *testing.T) {
	tr, _ := newTestTracker()
	tr.BeginSegment("seg-1", "first segment words")
	tr.AudioDelivered("seg-1", 50, false, 1)

	// A second segment begins before seg-1 explicitly ends.
	tr.BeginSegment("seg-2", "second segment words")

	// Late audio for the old segment must be ignored.
	if evs := tr.AudioDelivered("seg-1", 50, true, 2); evs != nil {
		t.Fatal("late audio for the superseded segment must be ignored")
	}
	// The new segment works independently and finalizes with its own text.
	evs := tr.AudioDelivered("seg-2", 50, true, 3)
	var final *frames.CaptionFrame
	for _, e := range evs {
		if e.Event == frames.CaptionEventFinalized {
			final = e
		}
	}
	if final == nil || final.SegmentID != "seg-2" || final.Text != "second segment words" {
		t.Fatalf("second segment not independent: %+v", final)
	}
}

// TestTracker_SequenceMonotonicAcrossSegments verifies seq keeps increasing
// across segment boundaries and lifecycle events (clients rely on one global
// per-session ordering, not per-segment numbering).
func TestTracker_SequenceMonotonicAcrossSegments(t *testing.T) {
	tr, _ := newTestTracker()
	tr.BeginSegment("a", "a one")
	tr.AudioDelivered("a", 50, true, 1)
	tr.BeginSegment("b", "b one")
	tr.AudioDelivered("b", 50, true, 2)
	tr.Reset()
	tr.BeginSegment("c", "c one")
	tr.AudioDelivered("c", 50, true, 3)

	hist := tr.Replay(0)
	for i := 1; i < len(hist); i++ {
		if hist[i].Seq <= hist[i-1].Seq {
			t.Fatalf("seq not globally monotonic at %d: %d -> %d", i, hist[i-1].Seq, hist[i].Seq)
		}
	}
}

// TestTracker_InterruptEmitsNoEventsWhenIdle verifies barge-in with no active
// segment is a quiet no-op (it must not fabricate empty finalize/retract
// events that would confuse the client).
func TestTracker_InterruptEmitsNoEventsWhenIdle(t *testing.T) {
	tr, _ := newTestTracker()
	if f, r := tr.Interrupt(1); f != nil || r != nil {
		t.Fatal("interrupt while idle must emit nothing")
	}
	// And after a clean finalize, another interrupt is also quiet.
	tr.BeginSegment("s", "done words")
	tr.AudioDelivered("s", 50, true, 2)
	if f, r := tr.Interrupt(3); f != nil || r != nil {
		t.Fatal("interrupt after finalize must emit nothing")
	}
}

// TestTracker_TTSFailedWithNoSegmentIsQuiet verifies an error with nothing
// active does not create a phantom segment.
func TestTracker_TTSFailedWithNoSegmentIsQuiet(t *testing.T) {
	tr, _ := newTestTracker()
	if ev := tr.TTSFailed("boom"); ev != nil {
		t.Fatalf("error with no active segment must be quiet, got %+v", ev)
	}
}
