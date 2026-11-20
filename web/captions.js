// Voxray real-time caption stream client.
//
// Consumes CaptionFrame events from the server (JSON envelope over WebSocket)
// and maintains the exact caption state the UI should render:
//
//   started    -> a new interim segment appears
//   delta      -> incremental text is appended as its audio chunk is delivered
//   finalized  -> the played text becomes permanent (kept verbatim)
//   retracted  -> the un-played suffix is removed immediately on barge-in, so
//                 a half-spoken sentence never lingers on screen
//   error      -> interim text is withdrawn and an error note is shown
//   reset      -> all caption state is cleared (session (re)start)
//
// Reconnect safety: events carry a per-session monotonic `seq`. A gap after
// reconnect is detected and surfaced via onGap so the UI can mark the
// transcript boundary rather than silently splice mismatched text.
(function (global) {
  "use strict";

  function createCaptionStore(handlers) {
    handlers = handlers || {};
    // segments: segmentID -> { id, status, interim, final, startedAt, updatedAt }
    const segments = new Map();
    let order = [];
    let lastSeq = 0;
    let sessionID = null;

    const emit = () => {
      if (typeof handlers.onChange === "function") {
        handlers.onChange(snapshot());
      }
    };

    function snapshot() {
      return order
        .map((id) => segments.get(id))
        .filter(Boolean)
        .map((s) => ({
          segmentId: s.id,
          status: s.status, // "interim" | "final" | "retracted" | "error"
          text: s.status === "interim" ? s.interim : s.final,
          interim: s.interim,
          startedAt: s.startedAt,
          updatedAt: s.updatedAt,
        }));
    }

    function handleEvent(ev) {
      if (!ev || typeof ev !== "object") return;

      // Session identity: a different session id means a fresh connection.
      if (ev.session_id && sessionID && ev.session_id !== sessionID) {
        reset();
      }
      if (ev.session_id) sessionID = ev.session_id;

      // Sequence gap detection for reconnect diagnostics.
      if (typeof ev.seq === "number" && ev.seq > 0) {
        if (lastSeq !== 0 && ev.seq !== lastSeq + 1 && ev.event !== "reset") {
          if (typeof handlers.onGap === "function") {
            handlers.onGap({ expected: lastSeq + 1, got: ev.seq });
          }
        }
        lastSeq = ev.seq;
      }

      switch (ev.event) {
        case "reset":
          reset();
          break;
        case "started": {
          const seg = {
            id: ev.segment_id,
            status: "interim",
            interim: ev.text || "",
            final: "",
            startedAt: ev.start_time || 0,
            updatedAt: Date.now(),
          };
          if (!segments.has(seg.id)) {
            segments.set(seg.id, seg);
            order.push(seg.id);
          } else {
            Object.assign(segments.get(seg.id), seg);
          }
          break;
        }
        case "delta": {
          const seg = segments.get(ev.segment_id);
          if (!seg) return;
          seg.interim = (seg.interim || "") + (ev.text || "");
          seg.updatedAt = Date.now();
          break;
        }
        case "finalized": {
          const seg = segments.get(ev.segment_id);
          if (!seg) break;
          seg.final = ev.text || seg.interim;
          seg.interim = seg.final;
          seg.status = "final";
          seg.updatedAt = Date.now();
          break;
        }
        case "retracted": {
          // Drop the un-played suffix; keep only the already-played prefix.
          const seg = segments.get(ev.segment_id);
          if (!seg) break;
          const played = ev.played_text || "";
          seg.interim = played;
          seg.final = played;
          seg.status = played ? "final" : "retracted";
          seg.updatedAt = Date.now();
          if (!played) {
            // Nothing was played: remove the segment entirely.
            segments.delete(seg.id);
            order = order.filter((id) => id !== seg.id);
          }
          break;
        }
        case "error": {
          const seg = segments.get(ev.segment_id);
          if (seg) {
            seg.status = "error";
            seg.final = seg.interim;
            seg.updatedAt = Date.now();
          }
          if (typeof handlers.onError === "function") {
            handlers.onError(ev.text || "caption error");
          }
          break;
        }
        default:
          break;
      }
      emit();
    }

    function reset() {
      segments.clear();
      order = [];
      lastSeq = 0;
      emit();
    }

    return { handleEvent, reset, snapshot, get lastSeq() { return lastSeq; } };
  }

  global.VoxrayCaptions = { createCaptionStore };
})(window);
