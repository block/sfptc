// Package strategy provides a framework for implementing and registering different caching strategies.
package strategy

import (
	"context"
	"net/http"

	"github.com/alecthomas/errors"
	"github.com/alecthomas/hcl"
)

var registry = map[string]func(config *hcl.Block) (Strategy, error){}

type Factory[Config any] func(ctx context.Context, config Config) (Strategy, error)

// Register a new caching strategy.
func Register[Config any](id string, factory Factory[Config]) {
	registry[id] = func(config *hcl.Block) (Strategy, error) {
		var cfg Config
		if err := hcl.UnmarshalBlock(config, &cfg); err != nil {
			return nil, errors.WithStack(err)
		}
		return factory(context.Background(), cfg)
	}
}

type Strategy interface {
	Register(mux *http.ServeMux)
}
