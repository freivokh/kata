package httpurl

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ParseRetryAfter parses a delay or HTTP date, bounding both to maxDelay.
func ParseRetryAfter(value string, now time.Time, maxDelay, overflow time.Duration) time.Duration {
	if value == "" || value != strings.TrimSpace(value) || strings.HasPrefix(value, "+") || strings.HasPrefix(value, "-") {
		return 0
	}
	if seconds, err := strconv.ParseUint(value, 10, 64); err == nil {
		if seconds > uint64(maxDelay/time.Second) {
			return maxDelay
		}
		return time.Duration(seconds) * time.Second
	} else if strings.IndexFunc(value, func(c rune) bool { return c < '0' || c > '9' }) == -1 {
		return overflow
	}
	if delay, ok := RetryAfterWait(value, now); ok {
		return min(max(delay, 0), maxDelay)
	}
	return 0
}

// RetryAfterWait parses the unbounded header and reports whether it was valid.
func RetryAfterWait(value string, now time.Time) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		const maxDurationSeconds = int64(1<<63-1) / int64(time.Second)
		if seconds > maxDurationSeconds {
			return time.Duration(1<<63 - 1), true
		}
		return time.Duration(seconds) * time.Second, true
	}
	at, err := http.ParseTime(value)
	if err != nil {
		return 0, false
	}
	return max(at.Sub(now), 0), true
}
