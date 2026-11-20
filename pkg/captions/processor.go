package captions

import (
	"context"

	"github.com/Voxray-AI/Voxray/pkg/frames"
	"github.com/Voxray-AI/Voxray/pkg/logger"
	"github.com/Voxray-AI/Voxray/pkg/processors"
)

// CaptionProcessor converts tagged TTS audio into an ordered CaptionFrame
// stream alongside the unchanged audio/control frames. It never alters,
// drops, or reorders the underlying call frames: every input frame is
// forwarded exactly once, and caption events are injected into the same
// downstream position as the audio chunk that produced them.
//
// Placement: directly after the TTS processor (and InterruptionController,
// if present) and before the sink.
type CaptionProcessor struct {
	*processors.BaseProcessor
	tracker *Tracker
}

// Ensure CaptionProcessor implements Processor.
var _ processors.Processor = (*CaptionProcessor)(nil)

// NewCaptionProcessor builds a processor emitting captions for sessionID.
// A nil clock uses wall-clock time.
func NewCaptionProcessor(name, sessionID string, clock Clock) *CaptionProcessor {
	if name == "" {
		name = "Captions"
	}
	return &CaptionProcessor{
		BaseProcessor: processors.NewBaseProcessor(name),
		tracker:       NewTracker(sessionID, clock),
	}
}

// Tracker exposes the underlying tracker (e.g. for reconnect replay).
func (p *CaptionProcessor) Tracker() *Tracker { return p.tracker }

// ProcessFrame implements processors.Processor.
func (p *CaptionProcessor) ProcessFrame(ctx context.Context, f frames.Frame, dir processors.Direction) error {
	if dir != processors.Downstream {
		if p.Prev() != nil {
			return p.Prev().ProcessFrame(ctx, f, dir)
		}
		return nil
	}

	switch t := f.(type) {
	case *frames.StartFrame:
		// (Re)start: invalidate any stale caption state first, then forward.
		if ev := p.tracker.Reset(); ev != nil {
			if err := p.PushDownstream(ctx, ev); err != nil {
				return err
			}
		}
		return p.PushDownstream(ctx, f)

	case *frames.TTSAudioRawFrame:
		return p.handleTTSAudio(ctx, t)

	case *frames.BotStoppedSpeakingFrame:
		// Safety net for TTS implementations that emit no end marker on the
		// last audio frame: finalize an open segment before forwarding.
		if ev := p.tracker.Finalize(0); ev != nil {
			if err := p.PushDownstream(ctx, ev); err != nil {
				return err
			}
		}
		return p.PushDownstream(ctx, f)

	case *frames.UserStartedSpeakingFrame, *frames.InterruptionFrame:
		// Barge-in: finalize the played prefix and retract the unplayed suffix.
		// This runs at the point the barge-in signal reaches us in the serial
		// pipeline, i.e. after every audio chunk emitted before it has already
		// been handed to the transport.
		finalEv, retractEv := p.tracker.Interrupt(f.ID())
		if finalEv != nil {
			if err := p.PushDownstream(ctx, finalEv); err != nil {
				return err
			}
		}
		if retractEv != nil {
			if err := p.PushDownstream(ctx, retractEv); err != nil {
				return err
			}
		}
		return p.PushDownstream(ctx, f)

	case *frames.ErrorFrame:
		// TTS synthesis failures: retract interim text and emit an error event.
		if t.Processor == "TTS" || t.Processor == "tts" {
			if ev := p.tracker.TTSFailed(t.Error); ev != nil {
				if err := p.PushDownstream(ctx, ev); err != nil {
					return err
				}
			}
		}
		return p.PushDownstream(ctx, f)

	case *frames.CancelFrame, *frames.StopFrame, *frames.EndFrame:
		// Session teardown: settle any open segment so clients are not left
		// showing interim text, then forward the control frame.
		if ev := p.tracker.Finalize(f.ID()); ev != nil {
			if err := p.PushDownstream(ctx, ev); err != nil {
				return err
			}
		}
		return p.PushDownstream(ctx, f)

	default:
		return p.PushDownstream(ctx, f)
	}
}

// handleTTSAudio forwards the audio chunk unchanged, emitting the caption
// events that correspond to its delivery. Events are emitted before the audio
// frame is forwarded so the client always receives the caption first and can
// render it at the same instant playback begins.
func (p *CaptionProcessor) handleTTSAudio(ctx context.Context, af *frames.TTSAudioRawFrame) error {
	meta := af.Metadata()
	segmentID, _ := meta[MetadataKeySegmentID].(string)
	text, _ := meta[MetadataKeyText].(string)
	_, isEnd := meta[MetadataKeySegmentEnd].(bool)

	// Defensive: an untagged TTS frame cannot be correlated to text. Forward it
	// unchanged and log once per occurrence; it must not break the audio path.
	if segmentID == "" {
		logger.Debug("captions: TTS audio frame id=%d without segment tag; forwarding without caption", af.ID())
		return p.PushDownstream(ctx, af)
	}

	// First chunk of a segment carries the source text.
	if text != "" {
		if ev := p.tracker.BeginSegment(segmentID, text); ev != nil {
			if err := p.PushDownstream(ctx, ev); err != nil {
				return err
			}
		}
	}

	events := p.tracker.AudioDelivered(segmentID, audioDurationMs(af), isEnd, af.ID())
	for _, ev := range events {
		if err := p.PushDownstream(ctx, ev); err != nil {
			return err
		}
	}
	return p.PushDownstream(ctx, af)
}

// audioDurationMs computes a PCM chunk's duration in milliseconds from its
// sample rate and frame count. Returns 0 when the duration cannot be derived
// (the allocator then reveals text on the first chunk instead of stalling).
func audioDurationMs(af *frames.TTSAudioRawFrame) int64 {
	sr := af.SampleRate
	if sr <= 0 {
		return 0
	}
	n := af.NumFrames
	if n <= 0 && af.NumChannels > 0 {
		n = len(af.Audio) / (af.NumChannels * 2)
	}
	if n <= 0 {
		return 0
	}
	return int64(n) * 1000 / int64(sr)
}
