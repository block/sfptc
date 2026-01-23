package strategy

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"

	"github.com/goproxy/goproxy"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/jobscheduler"
	"github.com/block/cachew/internal/logging"
)

func init() {
	Register("gomod", "Caches Go module proxy requests.", NewGoMod)
}

type GoModConfig struct {
	Proxy        string   `hcl:"proxy,optional" help:"Upstream Go module proxy URL (defaults to proxy.golang.org)" default:"https://proxy.golang.org"`
	PrivatePaths []string `hcl:"private-paths,optional" help:"Module path patterns for private repositories"`
}

type GoMod struct {
	config      GoModConfig
	cache       cache.Cache
	logger      *slog.Logger
	proxy       *url.URL
	goproxy     *goproxy.Goproxy
	gitStrategy GitStrategy // Reference to git strategy for private repo access
}

var _ Strategy = (*GoMod)(nil)

func NewGoMod(ctx context.Context, config GoModConfig, _ jobscheduler.Scheduler, cache cache.Cache, mux Mux) (*GoMod, error) {
	parsedURL, err := url.Parse(config.Proxy)
	if err != nil {
		return nil, fmt.Errorf("invalid proxy URL: %w", err)
	}

	g := &GoMod{
		config: config,
		cache:  cache,
		logger: logging.FromContext(ctx),
		proxy:  parsedURL,
	}

	publicFetcher := &goproxy.GoFetcher{
		Env: []string{
			"GOPROXY=" + config.Proxy,
			"GOSUMDB=off", // Disable checksum database validation in fetcher, to prevent unneccessary double validation
		},
	}

	var fetcher goproxy.Fetcher = publicFetcher

	if len(config.PrivatePaths) > 0 {
		gitStrat := GetStrategy("git")
		if gitStrat == nil {
			g.logger.WarnContext(ctx, "Private paths configured but git strategy not found, private module support disabled")
		} else {
			gitStrategy, ok := gitStrat.(GitStrategy)
			if !ok {
				g.logger.WarnContext(ctx, "Git strategy does not implement GitStrategy interface, private module support disabled")
			} else {
				g.gitStrategy = gitStrategy
				privateFetcher := newPrivateFetcher(g, gitStrategy)
				fetcher = newCompositeFetcher(publicFetcher, privateFetcher, config.PrivatePaths)

				g.logger.InfoContext(ctx, "Configured private module support",
					slog.Any("private_paths", config.PrivatePaths))
			}
		}
	}

	g.goproxy = &goproxy.Goproxy{
		Logger:  g.logger,
		Fetcher: fetcher,
		Cacher: &goproxyCacher{
			cache: cache,
		},
		ProxiedSumDBs: []string{
			"sum.golang.org https://sum.golang.org",
		},
	}

	g.logger.InfoContext(ctx, "Initialized Go module proxy strategy",
		slog.String("proxy", g.proxy.String()))

	mux.Handle("GET /gomod/{path...}", http.StripPrefix("/gomod", g.goproxy))

	return g, nil
}

func (g *GoMod) String() string {
	return "gomod:" + g.proxy.Host
}
