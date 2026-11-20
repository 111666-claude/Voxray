package pipeline

import (
	"context"
	"sync"

	"github.com/Voxray-AI/Voxray/pkg/frames"
	"github.com/Voxray-AI/Voxray/pkg/logger"
	"github.com/Voxray-AI/Voxray/pkg/processors"
)

// DownstreamCallback is called when a frame is emitted downstream (e.g. from a sink).
type DownstreamCallback func(ctx context.Context, f frames.Frame) error

// UpstreamCallback is called when a frame is emitted upstream (e.g. from a source).
type UpstreamCallback func(ctx context.Context, f frames.Frame) error

// Source is a processor that reads frames from a channel and pushes them downstream.
type Source struct {
	*processors.BaseProcessor
	In <-chan frames.Frame
}

// NewSource returns a Source that reads from ch.
func NewSource(name string, ch <-chan frames.Frame) *Source {
	if name == "" {
		name = "Source"
	}
	return &Source{BaseProcessor: processors.NewBaseProcessor(name), In: ch}
}

// Run reads from In and pushes to next until context is done or channel closed.
func (s *Source) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case f, ok := <-s.In:
			if !ok {
				return
			}
			_ = s.PushDownstream(ctx, f)
		}
	}
}

// Sink is a processor that forwards all frames to a channel (for transport output).
//
// ORDERING: frames are handed to a single writer goroutine through an internal
// bounded queue, so the order in which frames reach the transport output
// channel exactly matches the pipeline's downstream order. This matters for
// caption events, which must stay aligned with their audio chunks. The queue
// decouples the pipeline from a slow transport (the writer blocks on the
// transport, not the processor) while bounding buffering: when the queue is
// full the pipeline applies back-pressure instead of spawning unbounded
// goroutines.
type Sink struct {
	*processors.BaseProcessor
	Out        chan<- frames.Frame
	ttsLogOnce sync.Once

	queue   chan frames.Frame
	started bool
	closed  bool
	startMu sync.Mutex
	done    chan struct{}
}

// sinkQueueCap bounds the in-process frame backlog between the pipeline and
// the transport writer. It mirrors the runner's input-queue philosophy: large
// enough to absorb transport jitter, small enough to avoid unbounded memory.
const sinkQueueCap = 256

// NewSink returns a Sink that writes to ch.
func NewSink(name string, ch chan<- frames.Frame) *Sink {
	if name == "" {
		name = "Sink"
	}
	return &Sink{
		BaseProcessor: processors.NewBaseProcessor(name),
		Out:           ch,
		queue:         make(chan frames.Frame, sinkQueueCap),
		done:          make(chan struct{}),
	}
}

// ensureWriter starts the single ordered writer goroutine once. The writer
// drains the internal queue into Out in FIFO order until the queue is closed
// (Cleanup) or the context is canceled.
func (s *Sink) ensureWriter(ctx context.Context) {
	s.startMu.Lock()
	defer s.startMu.Unlock()
	if s.started {
		return
	}
	s.started = true
	go func() {
		defer close(s.done)
		for {
			select {
			case <-ctx.Done():
				return
			case frame, ok := <-s.queue:
				if !ok {
					return
				}
				// A single goroutine performs all sends, so transport ordering
				// matches pipeline ordering even with a slow client. The
				// transport may close its output channel during shutdown; the
				// recover keeps that race from crashing the process.
				func() {
					defer func() { _ = recover() }()
					select {
					case s.Out <- frame:
					case <-ctx.Done():
					}
				}()
			}
		}
	}()
}

// ProcessFrame forwards the frame to the transport via the ordered queue and
// does not call next (end of chain).
func (s *Sink) ProcessFrame(ctx context.Context, f frames.Frame, dir processors.Direction) error {
	if dir != processors.Downstream {
		if s.Prev() != nil {
			return s.Prev().ProcessFrame(ctx, f, dir)
		}
		return nil
	}
	if f != nil {
		if _, ok := f.(*frames.TTSAudioRawFrame); ok {
			s.ttsLogOnce.Do(func() { logger.Info("pipeline (sink): first TTS audio frame to transport") })
		} else {
			logger.Info("pipeline (sink): forwarding to transport frame type=%s id=%d", f.FrameType(), f.ID())
		}
	}
	if s.Out == nil || f == nil {
		return nil
	}
	s.ensureWriter(ctx)
	// Enqueue in pipeline order. Back-pressure: a slow transport eventually
	// fills the queue and the pipeline blocks here rather than dropping or
	// reordering frames. A closed queue (after Cleanup) means teardown is in
	// progress; recover and drop rather than panic.
	defer func() { _ = recover() }()
	select {
	case s.queue <- f:
	case <-ctx.Done():
	}
	return nil
}

// Cleanup stops the ordered writer. It is idempotent: concurrent or repeated
// calls after the first are no-ops. Queued frames are delivered best-effort
// until the context is canceled.
func (s *Sink) Cleanup(ctx context.Context) error {
	s.startMu.Lock()
	started := s.started
	if !s.closed {
		s.closed = true
		close(s.queue)
	}
	s.startMu.Unlock()
	if !started {
		return nil
	}
	select {
	case <-s.done:
	case <-ctx.Done():
	}
	return s.BaseProcessor.Cleanup(ctx)
}

// PipelineSource is a processor that acts as the entry point of a pipeline; it forwards
// downstream frames to the next processor and upstream frames to the provided callback.
type PipelineSource struct {
	*processors.BaseProcessor
	OnUpstream UpstreamCallback
	name       string
}

// NewPipelineSource returns a source that forwards upstream frames to the callback.
func NewPipelineSource(name string, onUpstream UpstreamCallback) *PipelineSource {
	if name == "" {
		name = "PipelineSource"
	}
	return &PipelineSource{BaseProcessor: processors.NewBaseProcessor(name), OnUpstream: onUpstream, name: name}
}

// ProcessFrame implements Processor. Downstream goes to next; upstream goes to OnUpstream.
func (s *PipelineSource) ProcessFrame(ctx context.Context, f frames.Frame, dir processors.Direction) error {
	if dir == processors.Downstream {
		if s.Next() != nil {
			return s.Next().ProcessFrame(ctx, f, dir)
		}
		return nil
	}
	if dir == processors.Upstream && s.OnUpstream != nil {
		return s.OnUpstream(ctx, f)
	}
	return nil
}

// PipelineSinkCallback is a processor that acts as the exit point of a pipeline; it forwards
// upstream frames to the previous processor and downstream frames to the provided callback.
type PipelineSinkCallback struct {
	*processors.BaseProcessor
	OnDownstream DownstreamCallback
	name         string
}

// NewPipelineSinkCallback returns a sink that forwards downstream frames to the callback.
func NewPipelineSinkCallback(name string, onDownstream DownstreamCallback) *PipelineSinkCallback {
	if name == "" {
		name = "PipelineSink"
	}
	return &PipelineSinkCallback{BaseProcessor: processors.NewBaseProcessor(name), OnDownstream: onDownstream, name: name}
}

// ProcessFrame implements Processor. Upstream goes to prev; downstream goes to OnDownstream.
func (s *PipelineSinkCallback) ProcessFrame(ctx context.Context, f frames.Frame, dir processors.Direction) error {
	if dir == processors.Upstream {
		if s.Prev() != nil {
			return s.Prev().ProcessFrame(ctx, f, dir)
		}
		return nil
	}
	if dir == processors.Downstream && s.OnDownstream != nil {
		return s.OnDownstream(ctx, f)
	}
	return nil
}
