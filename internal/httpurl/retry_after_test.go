package httpurl

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseRetryAfterValid(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		value string
		delay time.Duration
		valid bool
	}{
		{"0", 0, true},
		{"5", 5 * time.Second, true},
		{now.Add(3 * time.Second).Format(http.TimeFormat), 3 * time.Second, true},
		{"+5", 0, false},
		{" 5", 0, false},
		{"invalid", 0, false},
	} {
		t.Run(tc.value, func(t *testing.T) {
			delay, valid := ParseRetryAfterValid(tc.value, now, time.Minute, time.Minute)
			require.Equal(t, tc.valid, valid)
			require.Equal(t, tc.delay, delay)
		})
	}
}
