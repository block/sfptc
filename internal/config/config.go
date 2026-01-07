// Package config loads HCL configuration and uses that to construct the cache backend, and proxy strategies.
package config

import (
	"context"
	"io"
	"net/http"
	"strings"

	"github.com/alecthomas/errors"
	"github.com/alecthomas/hcl/v2"

	"github.com/block/sfptc/internal/cache"
	"github.com/block/sfptc/internal/logging"
	"github.com/block/sfptc/internal/strategy"
)

// Load HCL configuration and uses that to construct the cache backend, and proxy strategies.
func Load(ctx context.Context, r io.Reader, mux *http.ServeMux) error {
	logger := logging.FromContext(ctx)
	ast, err := hcl.Parse(r)
	if err != nil {
		return errors.WithStack(err)
	}

	strategyCandidates := []*hcl.Block{
		// Always enable the default strategy
		{Name: "default", Labels: []string{"/api/v1/"}},
	}

	// First pass, instantiate caches
	var caches []cache.Cache
	for _, node := range ast.Entries {
		switch node := node.(type) {
		case *hcl.Block:
			c, err := cache.Create(ctx, node.Name, node)
			if errors.Is(err, cache.ErrNotFound) {
				strategyCandidates = append(strategyCandidates, node)
				continue
			} else if err != nil {
				return errors.Errorf("%s: %w", node.Pos, err)
			}
			caches = append(caches, c)

		case *hcl.Attribute:
			return errors.Errorf("%s: attributes are not allowed", node.Pos)
		}
	}
	if len(caches) != 1 {
		return errors.Errorf("%s: expected exactly one cache backend, got %d", ast.Pos, len(caches))
	}

	cache := caches[0]

	logger.DebugContext(ctx, "Cache backend", "cache", cache)

	// Second pass, instantiate strategies and bind them to the mux.
	for _, block := range strategyCandidates {
		if len(block.Labels) != 1 {
			return errors.Errorf("%s: block must have exactly one label defining the server mount point", block.Pos)
		}
		pattern := block.Labels[0]
		block.Labels = nil
		s, err := strategy.Create(ctx, block.Name, block, cache)
		if err != nil {
			return errors.Errorf("%s: %w", block.Pos, err)
		}

		logger.DebugContext(ctx, "Adding strategy", "strategy", s, "pattern", pattern)

		mux.Handle(pattern, http.StripPrefix(strings.TrimSuffix(pattern, "/"), s))
	}
	return nil
}
