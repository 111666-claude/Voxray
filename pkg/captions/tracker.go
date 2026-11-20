package captions

import (
	"sync"
	"time"

	"github.com/Voxray-AI/Voxray/pkg/frames"
)

// segmentState is one active caption segment.
type segmentState struct {
	id        string
	fullText  string
	alloc     *allocator
	startTime time.Time
	endTime   time.Time
	// playedRunes counts runes whose audio has been delivered (revealed text).
	playedRunes int
	// deltaText accumulates the exact deltas already emitted (byte-exact join
	// of the source text prefix that has been delivered as audio).
	playedText string
	finished   bool
}

// historyEntry is one CaptionFrame kept for reconnect replay.
type historyEntry struct {
	frame *frames.CaptionFrame
}

// Tracker is the per-session caption state machine. It is safe for concurrent
// use because the pipeline is serial, but the mutex also covers history reads
// from reconnect handling on other goroutines.
type Tracker struct {
	sessionID string
	clock     Clock

	mu      sync.Mutex
	seq     uint64
	active  *segmentState
	history []historyEntry // bounded ring of the most recent events
	maxHist int
}

// defaultHistoryLimit bounds reconnect-replay memory per session.
const defaultHistoryLimit = 256

// NewTracker creates a Tracker for sessionID. A nil clock uses time.Now.
func NewTracker(sessionID string, clock Clock) *Tracker {
	if clock == nil {
		clock = defaultClock
	}
	return &Tracker{
		sessionID: sessionID,
		clock:     clock,
		maxHist:   defaultHistoryLimit,
	}
}

// SessionID returns the tracker's session id.
func (t *Tracker) SessionID() string { return t.sessionID }

// nextSeq returns the next per-session sequence number.
func (t *Tracker) nextSeq() uint64 {
	t.seq++
	return t.seq
}

// emit records and returns a new caption event.
func (t *Tracker) emit(segmentID, event, text, playedText string, start, end time.Time, audioFrameID uint64, final bool) *frames.CaptionFrame {
	cf := frames.NewCaptionFrame(t.sessionID, segmentID, t.nextSeq(), event, text)
	cf.PlayedText = playedText
	if !start.IsZero() {
		cf.StartTime = start.UnixNano()
	}
	if !end.IsZero() {
		cf.EndTime = end.UnixNano()
	}
	cf.AudioFrameID = audioFrameID
	cf.Final = final
	t.history = append(t.history, historyEntry{frame: cf})
	if len(t.history) > t.maxHist {
		// Drop the oldest entries to keep memory bounded.
		t.history = t.history[len(t.history)-t.maxHist:]
	}
	return cf
}

// BeginSegment opens a new segment for a TTS speak batch. fullText is the text
// being synthesized. The total audio duration is unknown at this point
// (streaming reveals it chunk by chunk) so the allocator starts conservative
// and the final chunk flushes the remainder. It emits the "started" event
// carrying the full interim text.
func (t *Tracker) BeginSegment(segmentID, fullText string) *frames.CaptionFrame {
	t.mu.Lock()
	defer t.mu.Unlock()
	// Defensive: a new segment implicitly closes any still-active one.
	if t.active != nil && !t.active.finished {
		_ = t.finalizeLocked(t.clock(), 0)
	}
	seg := &segmentState{
		id:       segmentID,
		fullText: fullText,
		alloc:    newAllocator(fullText),
	}
	t.active = seg
	now := t.clock()
	seg.startTime = now
	return t.emit(segmentID, frames.CaptionEventStarted, fullText, "", now, time.Time{}, 0, false)
}

// AudioDelivered records that one TTS audio chunk of the segment has been
// handed to the transport at time now. chunkMs is its PCM duration; isEnd
// marks the segment's last chunk. It returns the events to send in order
// (a delta, plus a finalized event when isEnd closes the segment). It returns
// nil for audio belonging to an unknown or already finished segment.
func (t *Tracker) AudioDelivered(segmentID string, chunkMs int64, isEnd bool, audioFrameID uint64) []*frames.CaptionFrame {
	t.mu.Lock()
	defer t.mu.Unlock()
	seg := t.active
	if seg == nil || seg.id != segmentID || seg.finished {
		return nil
	}
	now := t.clock()
	// Streaming segments never know their true total duration up front; only
	// the end marker does. Feed each chunk and let the conservative
	// one-unit-per-chunk rule (or final flush) govern reveal timing.
	delta := seg.alloc.Feed(chunkMs, isEnd)
	if delta != "" {
		seg.playedText += delta
		seg.playedRunes += runeCount(delta)
	}
	if isEnd {
		seg.endTime = now
	}
	var out []*frames.CaptionFrame
	if delta != "" && !isEnd {
		out = append(out, t.emit(seg.id, frames.CaptionEventDelta, delta, "", seg.startTime, time.Time{}, audioFrameID, false))
	}
	if isEnd {
		// Final chunk: flush any remainder then finalize so clients never see
		// a gap between the last revealed text and finalization.
		if rem := seg.alloc.Flush(); rem != "" {
			delta += rem
			seg.playedText += rem
			seg.playedRunes += runeCount(rem)
		}
		if delta != "" {
			out = append(out, t.emit(seg.id, frames.CaptionEventDelta, delta, "", seg.startTime, now, audioFrameID, false))
		}
		seg.finished = true
		out = append(out, t.emit(seg.id, frames.CaptionEventFinalized, seg.playedText, "", seg.startTime, now, audioFrameID, true))
	}
	return out
}

// Finalize closes the active segment normally (e.g. BotStoppedSpeakingFrame
// without an explicit end marker) and returns the finalized event, or nil.
func (t *Tracker) Finalize(audioFrameID uint64) *frames.CaptionFrame {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.active == nil {
		return nil
	}
	return t.finalizeLocked(t.clock(), audioFrameID)
}

func (t *Tracker) finalizeLocked(now time.Time, audioFrameID uint64) *frames.CaptionFrame {
	seg := t.active
	if seg == nil || seg.finished {
		return nil
	}
	if rem := seg.alloc.Flush(); rem != "" {
		seg.playedText += rem
		seg.playedRunes += runeCount(rem)
	}
	if seg.endTime.IsZero() {
		seg.endTime = now
	}
	seg.finished = true
	return t.emit(seg.id, frames.CaptionEventFinalized, seg.playedText, "", seg.startTime, seg.endTime, audioFrameID, true)
}

// Interrupt performs barge-in handling: the already-played prefix is finalized
// and the not-yet-played suffix is retracted. It returns the events to send in
// order (a finalized event for the played prefix and a retracted event for the
// unplayed remainder); either may be nil.
func (t *Tracker) Interrupt(audioFrameID uint64) (finalEv, retractEv *frames.CaptionFrame) {
	t.mu.Lock()
	defer t.mu.Unlock()
	seg := t.active
	if seg == nil || seg.finished {
		return nil, nil
	}
	now := t.clock()
	if seg.endTime.IsZero() {
		seg.endTime = now
	}
	played := seg.playedText
	// The retracted suffix is everything in the source text not yet delivered
	// as audio, computed byte-exactly against the full segment text.
	unplayed := ""
	if len(played) <= len(seg.fullText) && seg.fullText[:len(played)] == played {
		unplayed = seg.fullText[len(played):]
	} else {
		// Allocation should always be a prefix; fall back to flushing the
		// allocator remainder if the invariant ever fails.
		unplayed = seg.alloc.Flush()
		played = seg.fullText[:len(seg.fullText)-len(unplayed)]
	}
	seg.finished = true
	if played != "" {
		finalEv = t.emit(seg.id, frames.CaptionEventFinalized, played, "", seg.startTime, seg.endTime, audioFrameID, true)
	}
	if unplayed != "" {
		retractEv = t.emit(seg.id, frames.CaptionEventRetracted, unplayed, played, seg.startTime, seg.endTime, audioFrameID, true)
	}
	// If nothing was played yet, still retract the whole sentence so the
	// client clears the interim "started" text.
	if played == "" && unplayed == "" && seg.fullText != "" {
		retractEv = t.emit(seg.id, frames.CaptionEventRetracted, seg.fullText, "", seg.startTime, seg.endTime, audioFrameID, true)
	}
	return finalEv, retractEv
}

// TTSFailed reports a synthesis error for the active segment: interim text is
// withdrawn and an error event is emitted. It returns nil when no segment is
// active (nothing to retract on the caption channel).
func (t *Tracker) TTSFailed(errMsg string) *frames.CaptionFrame {
	t.mu.Lock()
	defer t.mu.Unlock()
	seg := t.active
	if seg == nil || seg.finished {
		return nil
	}
	now := t.clock()
	played := seg.playedText
	unplayed := seg.fullText
	if len(played) <= len(seg.fullText) && seg.fullText[:len(played)] == played {
		unplayed = seg.fullText[len(played):]
	}
	seg.finished = true
	// Retract whatever was only interim, then surface the error.
	if unplayed != "" {
		_ = t.emit(seg.id, frames.CaptionEventRetracted, unplayed, played, seg.startTime, now, 0, true)
	}
	return t.emit(seg.id, frames.CaptionEventError, errMsg, played, seg.startTime, now, 0, true)
}

// Reset invalidates all caption state and emits a reset event (session
// (re)start). Any active segment is abandoned without finalization.
func (t *Tracker) Reset() *frames.CaptionFrame {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.active = nil
	return t.emit("", frames.CaptionEventReset, "", "", time.Time{}, time.Time{}, 0, true)
}

// LastSeq returns the highest emitted sequence number (0 if none).
func (t *Tracker) LastSeq() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.seq
}

// Replay returns copies of historical events with seq > afterSeq for
// reconnect catch-up. The returned slice is ordered by sequence.
func (t *Tracker) Replay(afterSeq uint64) []*frames.CaptionFrame {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]*frames.CaptionFrame, 0)
	for _, h := range t.history {
		if h.frame.Seq > afterSeq {
			cp := *h.frame
			out = append(out, &cp)
		}
	}
	return out
}
