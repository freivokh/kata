package httpurl

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ParseRetryAfter parses a delay or HTTP date, bounding both to maxDelay.
func ParseRetryAfter(value string, now time.Time, maxDelay, overflow time.Duration) time.Duration {
	if value == "" {
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
	if at, err := http.ParseTime(value); err == nil {
		return min(max(at.Sub(now), 0), maxDelay)
	}
	return 0
}
