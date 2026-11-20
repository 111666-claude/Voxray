package mock

import (
	"context"
	"sync"

	"github.com/Voxray-AI/Voxray/pkg/frames"
)

// ScriptedTTS is a configurable TTSService for caption and interruption tests.
// It returns a fixed number of audio frames per Speak call (so tests can
// exercise multi-chunk segments), can fail on demand, and can return empty
// (silent) frames. It records every text it is asked to speak.
type ScriptedTTS struct {
	mu sync.Mutex

	// FramesPerSpeak is the number of TTSAudioRawFrame returned per Speak.
	FramesPerSpeak int
	// BytesPerFrame is the PCM payload size per returned frame.
	BytesPerFrame int
	// SampleRate of the returned frames.
	SampleRate int
	// FailNext, when > 0, makes the next N Speak calls return an error and
	// decrements the counter.
	FailNext int
	// FailErr is the error returned by failing Speak calls.
	FailErr error
	// IncludeEmpty, when true, prepends a zero-byte audio frame to each
	// response to exercise the empty-audio filtering policy.
	IncludeEmpty bool

	// Spoken records every text passed to Speak, in call order.
	Spoken []string
	// Calls counts Speak invocations.
	Calls int
}

// NewScriptedTTS returns a ScriptedTTS producing 4 frames of 2400 bytes
// (50 ms each at 24 kHz mono 16-bit) per Speak call.
func NewScriptedTTS() *ScriptedTTS {
	return &ScriptedTTS{
		FramesPerSpeak: 4,
		BytesPerFrame:  2400,
		SampleRate:     24000,
	}
}

// Speak implements services.TTSService.
func (m *ScriptedTTS) Speak(ctx context.Context, text string, sampleRate int) ([]*frames.TTSAudioRawFrame, error) {
	m.mu.Lock()
	m.Calls++
	m.Spoken = append(m.Spoken, text)
	if m.FailNext > 0 {
		m.FailNext--
		err := m.FailErr
		m.mu.Unlock()
		if err == nil {
			err = context.DeadlineExceeded
		}
		return nil, err
	}
	n := m.FramesPerSpeak
	if n <= 0 {
		n = 1
	}
	b := m.BytesPerFrame
	if b <= 0 {
		b = 2400
	}
	sr := m.SampleRate
	if sr <= 0 {
		sr = 24000
	}
	if sampleRate > 0 {
		sr = sampleRate
	}
	includeEmpty := m.IncludeEmpty
	m.mu.Unlock()

	out := make([]*frames.TTSAudioRawFrame, 0, n+1)
	if includeEmpty {
		out = append(out, frames.NewTTSAudioRawFrame([]byte{}, sr))
	}
	for i := 0; i < n; i++ {
		// Distinct payload per frame so tests can correlate ordering.
		audio := make([]byte, b)
		audio[0] = byte(i + 1)
		out = append(out, frames.NewTTSAudioRawFrame(audio, sr))
	}
	return out, nil
}

// SpokenTexts returns a copy of the recorded Speak inputs.
func (m *ScriptedTTS) SpokenTexts() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]string, len(m.Spoken))
	copy(cp, m.Spoken)
	return cp
}

// Ensure ScriptedTTS satisfies the TTS interface.
var _ interface {
	Speak(ctx context.Context, text string, sampleRate int) ([]*frames.TTSAudioRawFrame, error)
} = (*ScriptedTTS)(nil)
