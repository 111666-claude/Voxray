package captions

import (
	"strings"
	"testing"
)

func TestSplitTextUnits_PreservesSource(t *testing.T) {
	cases := []string{
		"Hello world.",
		"  leading spaces",
		"trailing  ",
		"one\ttwo\nthree",
		"",
		"single",
	}
	for _, in := range cases {
		units := splitTextUnits(in)
		var joined strings.Builder
		for _, u := range units {
			joined.WriteString(u.text)
		}
		if joined.String() != in {
			t.Errorf("split/join mismatch: got %q want %q", joined.String(), in)
		}
	}
}

func TestAllocator_ProportionalRevealAndExactJoin(t *testing.T) {
	text := "one two three four five six seven eight"
	// 8 words; simulate 4 equal 50 ms chunks totaling 200 ms.
	a := newAllocator(text)
	a.updateTotal(200)
	var revealed string
	for i := 0; i < 4; i++ {
		delta := a.Feed(50, i == 3)
		revealed += delta
	}
	if revealed != text {
		t.Fatalf("deltas do not reconstruct source:\n got=%q\nwant=%q", revealed, text)
	}
	if a.remaining() != "" {
		t.Errorf("allocator should be exhausted, remaining=%q", a.remaining())
	}
}

func TestAllocator_EndFlushesRemainder(t *testing.T) {
	text := "alpha beta gamma delta epsilon"
	a := newAllocator(text)
	a.updateTotal(1000)
	// A tiny first chunk would normally reveal nothing proportionally, but the
	// final chunk must flush everything regardless of estimates.
	first := a.Feed(1, false)
	if first == "" {
		t.Fatal("first chunk must make forward progress (no empty initial caption)")
	}
	rest := a.Feed(1, true)
	if first+rest != text {
		t.Fatalf("exact join broken: got=%q want=%q", first+rest, text)
	}
}

func TestAllocator_UnknownTotalRevealsOneUnitPerChunk(t *testing.T) {
	text := "no duration info available yet"
	a := newAllocator(text) // total unknown: streaming conservative mode
	// Whitespace attaches to the preceding word unit: "no ", "duration ", ...
	first := a.Feed(0, false)
	if first != "no " {
		t.Fatalf("unknown-total first chunk should reveal one unit, got %q", first)
	}
	second := a.Feed(0, false)
	if second != "duration " {
		t.Fatalf("second chunk should reveal the next unit, got %q", second)
	}
	rest := a.Feed(0, true)
	if first+second+rest != text {
		t.Fatalf("exact join broken: got %q", first+second+rest)
	}
}

func TestAllocator_EarlyTotalEstimateRefined(t *testing.T) {
	text := "a b c d e f g h"
	a := newAllocator(text)
	a.updateTotal(100) // underestimate; refined as chunks arrive
	a.updateTotal(400)
	if a.totalMs != 400 {
		t.Fatalf("total estimate not updated: %d", a.totalMs)
	}
	// Shrinking the estimate must be ignored.
	a.updateTotal(50)
	if a.totalMs != 400 {
		t.Fatalf("total estimate must never shrink: %d", a.totalMs)
	}
}
