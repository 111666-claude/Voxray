package frames

// Caption event types carried by CaptionFrame. They describe the lifecycle of
// one assistant (TTS) caption segment on the web client:
//
//   - CaptionEventStarted: a new caption segment begins; Text is its full
//     intended text (interim, may be retracted later on barge-in/TTS failure).
//   - CaptionEventDelta: an incremental slice of text whose audio chunk has just
//     been handed to the transport. Text is the delta only.
//   - CaptionEventFinalized: the segment (or the played prefix of it) is final
//     and must be kept verbatim. Text is the final full segment text.
//   - CaptionEventRetracted: the not-yet-played remainder of the segment is
//     withdrawn (barge-in). Text is the withdrawn suffix; PlayedText is the
//     prefix already delivered as audio, which stays on screen.
//   - CaptionEventError: TTS failed for the segment; any interim text is
//     withdrawn. Text carries the error message.
//   - CaptionEventReset: all caption state is invalidated (session (re)start).
const (
	CaptionEventStarted   = "started"
	CaptionEventDelta     = "delta"
	CaptionEventFinalized = "finalized"
	CaptionEventRetracted = "retracted"
	CaptionEventError     = "error"
	CaptionEventReset     = "reset"
)

// CaptionFrame is an incremental real-time caption event for assistant speech.
//
// Every event carries the pipeline SessionID and a per-session monotonically
// increasing Seq so clients can order events, detect gaps after reconnects,
// and deduplicate. SegmentID groups the started/delta/finalized/retracted
// events belonging to one TTS speak batch.
//
// Time fields are Unix nanoseconds captured when the corresponding TTS audio
// chunk is actually handed to the transport (i.e. the delivery instant, not
// synthesis time), so captions stay aligned with what the listener hears.
type CaptionFrame struct {
	ControlFrame
	// SessionID identifies the pipeline session (one WebSocket/WebRTC connection).
	SessionID string `json:"session_id"`
	// SegmentID identifies one TTS speak batch within the session.
	SegmentID string `json:"segment_id"`
	// Seq is the per-session monotonically increasing event sequence (1-based).
	Seq uint64 `json:"seq"`
	// Event is one of the CaptionEvent* constants.
	Event string `json:"event"`
	// Text is the event payload: full text for started/finalized, a delta for
	// delta, the withdrawn suffix for retracted, the error message for error.
	Text string `json:"text,omitempty"`
	// PlayedText is set on retracted events: the prefix already delivered as
	// audio that must remain visible (and is separately finalized).
	PlayedText string `json:"played_text,omitempty"`
	// StartTime is the delivery timestamp (Unix ns) of the segment's first
	// audio chunk; zero on non-segment events.
	StartTime int64 `json:"start_time,omitempty"`
	// EndTime is the delivery timestamp (Unix ns) of the segment's (or played
	// prefix's) last audio chunk; zero until the segment finishes/interrupts.
	EndTime int64 `json:"end_time,omitempty"`
	// AudioFrameID is the ID of the TTSAudioRawFrame whose delivery produced
	// this event (0 for lifecycle events not tied to a single chunk).
	AudioFrameID uint64 `json:"audio_frame_id,omitempty"`
	// Final marks segments that will never change again (finalized/retracted).
	Final bool `json:"final,omitempty"`
}

// FrameType implements Frame.
func (*CaptionFrame) FrameType() string { return "CaptionFrame" }

// NewCaptionFrame creates a CaptionFrame with a fresh ID.
func NewCaptionFrame(sessionID, segmentID string, seq uint64, event, text string) *CaptionFrame {
	return &CaptionFrame{
		ControlFrame: ControlFrame{Base: NewBase()},
		SessionID:    sessionID,
		SegmentID:    segmentID,
		Seq:          seq,
		Event:        event,
		Text:         text,
	}
}
