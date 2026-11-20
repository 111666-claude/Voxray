# Real-time Caption Stream

Real-time, incremental captions for assistant (TTS) speech. The web client
shows what the assistant is saying word-by-word, aligned to the audio the
listener actually hears, and — on barge-in — withdraws any text whose audio
was never played, so a half-spoken sentence never lingers on screen.

## 1. Where it lives in the pipeline

```
LLM → TTS → [Captions] → (InterruptionController) → Sink → Transport
```

- `pkg/processors/voice/tts.go` tags each speak batch's audio frames with
  internal metadata (`captions.MetadataKey*`): a segment id, the source text,
  and an end marker. Metadata never appears on the wire.
- `pkg/captions/CaptionProcessor` turns those tagged audio chunks into
  `frames.CaptionFrame` events, timestamped at the moment each chunk is handed
  toward the transport (delivery time, not synthesis time).
- The processor forwards **every** input frame unchanged and in order; it only
  injects caption events alongside them. Recording, transcript logging and the
  rest of the call are unaffected.
- One `CaptionProcessor` is created per session in `cmd/voxray/main.go`.

## 2. Event types and wire format

Captions travel as JSON-enveloped `CaptionFrame` messages on the same
WebSocket as the rest of the frames:

```json
{
  "type": "CaptionFrame",
  "data": {
    "session_id": "uuid",
    "segment_id": "tts-12",
    "seq": 7,
    "event": "delta",
    "text": "word ",
    "played_text": "",
    "start_time": 1700000000000000000,
    "end_time": 0,
    "audio_frame_id": 555,
    "final": false
  }
}
```

| Field | Meaning |
|---|---|
| `session_id` | Pipeline session (one connection). |
| `segment_id` | One TTS speak batch; groups its events. |
| `seq` | Per-session monotonic event counter (1-based); used for ordering and reconnect gap detection. |
| `event` | One of the six events below. |
| `text` | Full text (`started`/`finalized`), a delta (`delta`), withdrawn suffix (`retracted`), or message (`error`). |
| `played_text` | On `retracted`: the already-played prefix that stays visible. |
| `start_time` / `end_time` | Unix nanoseconds of first/last **delivered** audio chunk for the segment. |
| `audio_frame_id` | ID of the `TTSAudioRawFrame` whose delivery produced the event. |
| `final` | True once the segment will never change again. |

### Event lifecycle

| Event | When | Client action |
|---|---|---|
| `started` | A segment's first audio chunk is delivered; carries the full interim text. | Open an interim line. |
| `delta` | Each subsequent delivered audio chunk reveals its text slice. | Append `text`. |
| `finalized` | Segment finished (or barge-in settles the played prefix). | Freeze the line verbatim. |
| `retracted` | Barge-in withdraws the not-yet-played suffix. | Remove `text`; keep `played_text` (which is separately finalized). |
| `error` | TTS synthesis failed for an active segment. | Withdraw interim text; show the error note. |
| `reset` | `StartFrame` / session (re)start. | Clear all caption state. |

The concatenation of a segment's `delta` texts always equals the exact source
string; `finalized.text` equals the played prefix; and on a retraction
`finalized.text + retracted.text` reconstructs the full sentence.

## 3. Timing and text allocation

- Timestamps use a pluggable `Clock` (wall clock in production, pinned in
  tests) sampled inside `AudioDelivered`, i.e. at delivery time. A segment's
  `start_time` is the first chunk's delivery instant; `end_time` is the last
  delivered chunk's instant.
- Streaming TTS reveals total audio duration only as chunks arrive, so the
  allocator does **not** assume a total up front. It reveals at most one
  whitespace-delimited word unit per delivered chunk (conservative — captions
  never run ahead of the audio), and the final chunk flushes the remainder.
  Chunks that reveal nothing still do not break ordering; the final flush
  guarantees the exact-join invariant.

## 4. Edge-case event policies

| Situation | Policy |
|---|---|
| **Out-of-order / foreign audio packets** | Audio is correlated by `segment_id`. Frames for an unknown segment, a different segment, or an already-finished segment are ignored for captions (audio itself is still forwarded). Late/duplicate packets therefore cannot create phantom text. |
| **Silence / empty audio** | The TTS processor drops zero-byte frames before they leave it; they carry neither playback nor caption timing and would only stall allocation. A batch that is entirely empty emits a non-fatal `ErrorFrame` so any interim state can be withdrawn. |
| **Empty text** | Whitespace-only/empty speak text is suppressed before synthesis. If an empty segment ever reaches the tracker, it still opens and finalizes cleanly with empty deltas — no malformed events, no panic. |
| **TTS failure** | On a `Speak` error the TTS processor emits a non-fatal `ErrorFrame` and the pipeline stays alive. The caption processor retracts the active segment's interim text, emits an `error` event, and the next successful speak starts a fresh segment. With no active segment, the error is quiet (no phantom event). |
| **Slow client (back-pressure)** | The pipeline `Sink` forwards through a single ordered writer over a bounded queue, so a slow transport blocks the pipeline rather than dropping or reordering frames; caption `seq` stays monotonic and all segments still arrive. Memory stays bounded (`sinkQueueCap`). |
| **Disconnect / reconnect** | The tracker keeps a bounded ring of recent events (`Replay(afterSeq)`) and emits `reset` on a new `StartFrame`. A reconnecting client requests events with `seq > lastSeen`, receives ordered copies, and the client store flags any remaining gap as a transcript boundary rather than splicing mismatched text. |
| **Barge-in while idle / after finalize** | A no-op: no empty `finalized`/`retracted` events are fabricated. |
| **Interleaved segments** | Opening a new segment implicitly settles any still-open previous one; late audio for the old segment id is then ignored. |

## 5. Serializer behavior

- **JSON** (`JSONSerializer`, the WebSocket default): full `CaptionFrame`
  encode/decode via `serialize.DecodeByType("CaptionFrame", …)`.
- **Binary protobuf** and telephony serializers (Twilio, Telnyx, Plivo,
  Vonage, Exotel, Genesys): caption frames are not part of those wire
  protocols. `ProtoEncode` returns `(nil, nil)` and vendor serializers skip
  unknown frames, so they are safely omitted without affecting audio or
  telephony control messages.
- **WebRTC**: the barge-in drain
  (`smallwebrtc.drainTTSFramesUntilNonTTS`) skips caption frames together with
  the drained TTS audio and bot-turn control frames, so withdrawn audio and its
  captions are discarded together.

## 6. Tests

- `pkg/captions/allocator_test.go`, `tracker_test.go`, `edge_cases_test.go`,
  `processor_test.go` — unit and processor-level coverage including delivery
  timestamps, exact text joins, mid-flight barge-in retraction, TTS failure
  recovery, empty audio/text and untagged-frame resilience.
- `tests/pkg/captions/e2e_test.go` — end-to-end over a real WebSocket:
  full event contract, barge-in settlement, TTS-failure recovery, slow-client
  back-pressure and reconnect replay.
- `tests/pkg/frames/serialize_test.go` — JSON round-trip for every event and
  protobuf skip behavior.
- Mock TTS: `pkg/services/mock/scripted_tts.go` (multi-chunk, fail-on-demand,
  empty-frame injection).
