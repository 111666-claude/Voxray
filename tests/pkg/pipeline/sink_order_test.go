package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/Voxray-AI/Voxray/pkg/frames"
	"github.com/Voxray-AI/Voxray/pkg/pipeline"
	"github.com/Voxray-AI/Voxray/pkg/processors"
)

// TestSink_PreservesOrderUnderBackPressure is a regression guard for the
// real-time caption stream: caption events must reach the transport in the
// same order the pipeline emitted them, even when the transport reads slowly.
// The previous per-frame goroutine fan-out could reorder frames here.
func TestSink_PreservesOrderUnderBackPressure(t *testing.T) {
	// Tiny output buffer forces the writer to block, exercising the internal
	// ordered queue and back-pressure path.
	out := make(chan frames.Frame, 2)
	sink := pipeline.NewSink("sink", out)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const n = 200
	// Push frames through the sink from the pipeline goroutine.
	pushErr := make(chan error, 1)
	go func() {
		for i := 0; i < n; i++ {
			f := frames.NewTextFrame("")
			// Tag each frame with its ordinal in metadata for order checking.
			f.Metadata()["ord"] = i
			if err := sink.ProcessFrame(ctx, f, processors.Downstream); err != nil {
				pushErr <- err
				return
			}
		}
		pushErr <- nil
	}()

	// Read slowly (deliberately slower than production) and verify ordering.
	received := 0
	deadline := time.Now().Add(10 * time.Second)
	for received < n {
		select {
		case f := <-out:
			ord, ok := f.Metadata()["ord"].(int)
			if !ok {
				t.Fatalf("frame %d missing ordinal metadata", received)
			}
			if ord != received {
				t.Fatalf("order broken at position %d: got ord %d", received, ord)
			}
			received++
			time.Sleep(20 * time.Microsecond) // slow client
		case <-time.After(time.Until(deadline)):
			t.Fatalf("timed out after %d/%d frames", received, n)
		}
	}
	if err := <-pushErr; err != nil {
		t.Fatalf("push failed: %v", err)
	}
	if err := sink.Cleanup(ctx); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	// Cleanup must be idempotent (pipeline teardown can call it more than once).
	if err := sink.Cleanup(ctx); err != nil {
		t.Fatalf("second cleanup: %v", err)
	}
}
