package cache_test

import (
	"log/slog"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/cache/cachetest"
	"github.com/block/cachew/internal/logging"
)

func TestMemoryCache(t *testing.T) {
	cachetest.Suite(t, func(t *testing.T) cache.Cache {
		_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
		c, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: 100 * time.Millisecond})
		assert.NoError(t, err)
		return c
	})
}
