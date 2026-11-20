// Package captions implements real-time, incremental caption events for
// assistant (TTS) speech.
//
// The CaptionProcessor sits between the TTS processor and the pipeline sink.
// TTS tags the audio frames it emits with the source text and a segment ID
// (see MetadataKeySegmentID / MetadataKeyText); CaptionProcessor turns those
// tags, together with the actual hand-off of each audio chunk to the
// transport, into an ordered stream of CaptionFrame events:
//
//	started     – segment begins (full intended text, interim)
//	delta       – slice of text whose audio chunk was just delivered
//	finalized   – played text is final and must be kept
//	retracted   – not-yet-played suffix withdrawn on barge-in
//	error       – TTS synthesis failed, interim text withdrawn
//	reset       – all caption state invalidated (session (re)start)
//
// Timestamps are taken at audio delivery time (the moment the chunk is handed
// to the transport sink), not at synthesis time, so captions track what the
// listener actually hears.
package captions

import (
	"strings"
	"time"
	"unicode/utf8"
)

// Metadata keys attached by the TTS processor to TTSAudioRawFrame metadata.
// Metadata is not serialized on the JSON/protobuf audio wire payload, so these
// stay internal to the pipeline.
const (
	// MetadataKeySegmentID groups the audio frames of one TTS speak batch.
	MetadataKeySegmentID = "caption_segment_id"
	// MetadataKeyText is the source text spoken by the segment's audio. It is
	// attached to the segment's first audio frame.
	MetadataKeyText = "caption_text"
	// MetadataKeySegmentEnd marks the last audio frame of a speak batch. The
	// TTS processor already emits BotStoppedSpeakingFrame, but an explicit
	// marker makes segments robust even if control frames are filtered.
	MetadataKeySegmentEnd = "caption_segment_end"
)

// Clock returns the current time. It is a function so tests can pin time.
type Clock func() time.Time

// defaultClock uses wall-clock time.
func defaultClock() time.Time { return time.Now() }

// textUnit is one whitespace-delimited token with its trailing whitespace
// preserved, so concatenating units reconstructs the original text exactly.
type textUnit struct {
	text string
}

// splitTextUnits breaks text into word-ish units while preserving every byte
// of the original (including leading/trailing whitespace) so that the deltas
// emitted for audio chunks concatenate back to the exact source text.
//
// Each word carries its trailing whitespace (one unit ≈ one spoken word plus
// its spacing); leading whitespace forms a unit of its own.
// Example: "one two" -> ["one ", "two"]; "  hi" -> ["  ", "hi"].
func splitTextUnits(text string) []textUnit {
	if text == "" {
		return nil
	}
	isSpaceRune := func(r rune) bool {
		return r == ' ' || r == '\t' || r == '\n' || r == '\r'
	}
	var units []textUnit
	runes := []rune(text)
	for i := 0; i < len(runes); {
		// Read one run of whitespace or non-whitespace.
		space := isSpaceRune(runes[i])
		j := i
		for j < len(runes) && isSpaceRune(runes[j]) == space {
			j++
		}
		unit := string(runes[i:j])
		if space {
			if len(units) == 0 {
				// Leading whitespace stands alone.
				units = append(units, textUnit{text: unit})
			} else {
				// Interior/trailing whitespace attaches to the previous unit.
				units[len(units)-1].text += unit
			}
		} else {
			units = append(units, textUnit{text: unit})
		}
		i = j
	}
	return units
}

// allocator distributes the units of a segment's source text across its audio
// chunks in proportion to each chunk's share of the total audio duration.
// It is stateful: Feed is called once per delivered audio chunk in order and
// returns the text that should be revealed with that chunk's delta event.
//
// Streaming TTS reveals the total duration only as chunks arrive, so the
// allocator starts with no total (totalMs == 0). In that mode it cannot use a
// proportional target; instead it reveals at most one unit per chunk, which
// keeps captions at or behind the delivered audio even when the segment length
// is unknown up front. When more chunks than units exist, some chunks simply
// reveal nothing; the final chunk always flushes the remainder.
type allocator struct {
	units      []textUnit
	next       int   // index of the next unallocated unit
	consumedMs int64 // duration (ms) of audio already fed
	totalMs    int64 // known total segment duration (ms); 0 = still unknown
}

// newAllocator builds an allocator over text. It starts with an unknown total
// duration (streaming); the total can later be supplied via updateTotal.
func newAllocator(text string) *allocator {
	return &allocator{
		units: splitTextUnits(text),
	}
}

// updateTotal extends the known total duration (streams reveal total length
// only as chunks arrive). It never shrinks below already-consumed duration.
func (a *allocator) updateTotal(totalMs int64) {
	if totalMs > a.totalMs {
		a.totalMs = totalMs
	}
}

// remaining returns the not-yet-allocated text.
func (a *allocator) remaining() string {
	var b strings.Builder
	for i := a.next; i < len(a.units); i++ {
		b.WriteString(a.units[i].text)
	}
	return b.String()
}

// Feed records a delivered chunk of chunkMs milliseconds and returns the text
// delta to reveal now. On the final chunk (isEnd) all remaining text is
// flushed so the concatenation of every delta equals the source text exactly.
func (a *allocator) Feed(chunkMs int64, isEnd bool) string {
	if a.next >= len(a.units) {
		return ""
	}
	if isEnd {
		delta := a.remaining()
		a.next = len(a.units)
		return delta
	}
	a.consumedMs += chunkMs

	// Total duration unknown (streaming, before the end marker): reveal at most
	// one unit per chunk. Conservative by construction — captions can never
	// run ahead of the delivered chunk count — and the first chunk still makes
	// progress so the screen is not left empty after "started".
	if a.totalMs <= 0 {
		delta := a.units[a.next].text
		a.next++
		return delta
	}

	// Total known: reveal proportionally to delivered duration. Never overshoot
	// the proportional target and always make at least one unit of progress.
	target := int(int64(len(a.units)) * a.consumedMs / a.totalMs)
	if target <= a.next {
		target = a.next + 1
	}
	if target > len(a.units) {
		target = len(a.units)
	}
	var b strings.Builder
	for ; a.next < target; a.next++ {
		b.WriteString(a.units[a.next].text)
	}
	return b.String()
}

// Flush returns any text still held back (used on finalize/retract).
func (a *allocator) Flush() string {
	delta := a.remaining()
	a.next = len(a.units)
	return delta
}

// runeCount returns the rune count of s (0 for empty strings).
func runeCount(s string) int { return utf8.RuneCountInString(s) }
