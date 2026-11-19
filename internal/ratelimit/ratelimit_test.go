package ratelimit

import (
	"testing"
	"time"
)

func TestBurstThenDenyThenRefill(t *testing.T) {
	now := time.Unix(1000, 0)
	l := New(1, 3)
	l.now = func() time.Time { return now }
	for i := 0; i < 3; i++ {
		if ok, _ := l.Allow("a"); !ok {
			t.Fatalf("request %d within burst denied", i)
		}
	}
	ok, retry := l.Allow("a")
	if ok || retry <= 0 || retry > time.Second {
		t.Fatalf("want denial with retry<=1s, got ok=%v retry=%s", ok, retry)
	}
	if ok, _ := l.Allow("b"); !ok {
		t.Fatal("keys must have independent buckets")
	}
	now = now.Add(time.Second)
	if ok, _ := l.Allow("a"); !ok {
		t.Fatal("token should refill after 1s")
	}
}

func TestIdleKeysAreEvicted(t *testing.T) {
	now := time.Unix(1000, 0)
	l := New(1, 1)
	l.now = func() time.Time { return now }
	l.Allow("old")
	now = now.Add(time.Hour)
	l.Allow("new")
	if _, ok := l.buckets["old"]; ok {
		t.Fatal("idle bucket not evicted")
	}
}
