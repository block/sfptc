package strategy

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"
	"strings"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/jobscheduler"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/strategy/handler"
)

func init() {
	Register("hermit", "Caches Hermit package downloads.", NewHermit)
}

type HermitConfig struct {
	BaseURL string `hcl:"base-url" help:"Base URL for internal redirects to github-releases strategy" default:"${CACHEW_URL}"`
}

// Hermit caches Hermit package downloads.
// Acts as a smart router: GitHub releases redirect to github-releases strategy,
// all other sources are handled directly.
type Hermit struct {
	config          HermitConfig
	cache           cache.Cache
	client          *http.Client
	logger          *slog.Logger
	mux             Mux
	redirectHandler http.Handler
	directHandler   http.Handler
}

var _ Strategy = (*Hermit)(nil)

func NewHermit(ctx context.Context, config HermitConfig, _ jobscheduler.Scheduler, c cache.Cache, mux Mux) (*Hermit, error) {
	logger := logging.FromContext(ctx)

	logger.DebugContext(ctx, "Hermit strategy config received",
		slog.String("base_url_raw", config.BaseURL))

	s := &Hermit{
		config: config,
		cache:  c,
		client: http.DefaultClient,
		logger: logger,
		mux:    mux,
	}

	s.directHandler = s.createDirectHandler(c)
	mux.Handle("GET /hermit/{host}/{path...}", s.directHandler)

	if config.BaseURL != "" {
		s.redirectHandler = s.createRedirectHandler()
		mux.Handle("GET /hermit/github.com/{path...}", s.redirectHandler)
		logger.InfoContext(ctx, "Hermit strategy initialized", slog.String("base_url", config.BaseURL))
	} else {
		logger.WarnContext(ctx, "Hermit strategy initialized without base-url - GitHub releases will fail")
	}

	return s, nil
}

func (s *Hermit) String() string { return "hermit" }

func (s *Hermit) createDirectHandler(c cache.Cache) http.Handler {
	return handler.New(s.client, c).
		CacheKey(func(r *http.Request) string {
			return s.buildOriginalURL(r)
		}).
		Transform(func(r *http.Request) (*http.Request, error) {
			return s.buildDirectRequest(r)
		})
}

func (s *Hermit) createRedirectHandler() http.Handler {
	return handler.New(s.client, cache.NoOpCache()).
		Transform(func(r *http.Request) (*http.Request, error) {
			return s.buildRedirectRequest(r)
		})
}

func (s *Hermit) buildRedirectRequest(r *http.Request) (*http.Request, error) {
	path := r.PathValue("path")
	newPath := "/github.com/" + path

	internalURL := s.config.BaseURL + newPath
	if r.URL.RawQuery != "" {
		internalURL += "?" + r.URL.RawQuery
	}

	req, err := http.NewRequestWithContext(r.Context(), r.Method, internalURL, nil)
	if err != nil {
		return nil, errors.Wrap(err, "create internal redirect request")
	}

	req.Header = r.Header.Clone()
	return req, nil
}

func (s *Hermit) buildDirectRequest(r *http.Request) (*http.Request, error) {
	originalURL := s.buildOriginalURL(r)

	s.logger.DebugContext(r.Context(), "Fetching Hermit package",
		slog.String("url", originalURL))

	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, originalURL, nil)
	if err != nil {
		return nil, errors.Wrap(err, "create request")
	}
	return req, nil
}

func (s *Hermit) buildOriginalURL(r *http.Request) string {
	host := r.PathValue("host")
	path := r.PathValue("path")

	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	u := &url.URL{
		Scheme:   "https",
		Host:     host,
		Path:     path,
		RawQuery: r.URL.RawQuery,
	}
	return u.String()
}
