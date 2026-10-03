package main

import (
	"context"
	"testing"
	"time"
)

func TestParseBytes(t *testing.T) {
	cases := []struct {
		in      string
		want    int64
		wantErr bool
	}{
		{"", 0, false},
		{"1024", 1024, false},
		{"400G", 400e9, false},
		{"400GB", 400e9, false},
		{"400Gi", 400 * (1 << 30), false},
		{"400GiB", 400 * (1 << 30), false},
		{"22GB", 22e9, false},
		{"1T", 1e12, false},
		{"1Ti", 1 << 40, false},
		{"500M", 500e6, false},
		{"64Ki", 64 << 10, false},
		{" 10 G ", 10e9, false},
		{"-5G", 0, true},
		{"banana", 0, true},
		{"12X", 0, true},
	}
	for _, c := range cases {
		got, err := parseBytes(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("parseBytes(%q): want error, got %d", c.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseBytes(%q): unexpected error %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("parseBytes(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestWatchLogCapFires(t *testing.T) {
	logUncompressedBytes.Store(0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stopped := make(chan int64, 1)
	// perPodCap tiny so the first tick trips it.
	logUncompressedBytes.Store(1000)
	go watchLogCap(ctx, 500, cancel, func(total int64) { stopped <- total })

	select {
	case total := <-stopped:
		if total < 500 {
			t.Fatalf("onCap total = %d, want >= 500", total)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cap did not fire within 3s")
	}
	if ctx.Err() == nil {
		t.Fatal("stop() was not called (context still live)")
	}
}

func TestWatchLogCapDisabled(t *testing.T) {
	logUncompressedBytes.Store(1 << 60)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// perPodCap 0 => disabled: returns immediately, never cancels.
	watchLogCap(ctx, 0, cancel, func(int64) { t.Fatal("onCap called while disabled") })
	if ctx.Err() != nil {
		t.Fatal("disabled cap must not cancel the context")
	}
}
