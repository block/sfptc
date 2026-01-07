package strategy

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/textproto"
	"net/url"
	"os"

	"github.com/alecthomas/errors"

	"github.com/block/sfptc/internal/cache"
	"github.com/block/sfptc/internal/logging"
)

func init() {
	Register("host", NewHost)
}

// HostConfig represents the configuration for the Host strategy.
//
// In HCL it looks something like this:
//
//	host "/github/" {
//		target = "https://github.com/"
//	}
//
// In this example, the strategy will be mounted under "/github".
type HostConfig struct {
	Target string `hcl:"target" help:"The target URL to proxy requests to."`
}

// The Host [Strategy] forwards all GET requests to the specified host, caching the response payloads.
type Host struct {
	target *url.URL
	cache  cache.Cache
	client *http.Client
	logger *slog.Logger
}

var _ Strategy = (*Host)(nil)

func NewHost(ctx context.Context, config HostConfig, cache cache.Cache) (*Host, error) {
	u, err := url.Parse(config.Target)
	if err != nil {
		return nil, fmt.Errorf("invalid target URL: %w", err)
	}
	return &Host{
		target: u,
		cache:  cache,
		client: &http.Client{},
		logger: logging.FromContext(ctx),
	}, nil
}

func (d *Host) String() string { return "host:" + d.target.Host + d.target.Path }

func (d *Host) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	targetURL := *d.target
	targetURL.Path = r.URL.Path
	targetURL.RawQuery = r.URL.RawQuery
	fullURL := targetURL.String()

	key := cache.NewKey(fullURL)

	cr, headers, err := d.cache.Open(r.Context(), key)
	if err == nil {
		defer cr.Close()
		maps.Copy(w.Header(), headers)
		if _, err := io.Copy(w, cr); err != nil {
			d.logger.Error("Failed to copy cached response", slog.String("error", err.Error()), slog.String("url", fullURL))
		}
		return
	}

	if !errors.Is(err, os.ErrNotExist) {
		d.logger.Error("Failed to open cache", slog.String("error", err.Error()), slog.String("url", fullURL))
	}

	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, fullURL, nil)
	if err != nil {
		d.httpError(w, http.StatusInternalServerError, err, "Failed to create request", slog.String("url", fullURL))
		return
	}

	resp, err := d.client.Do(req)
	if err != nil {
		d.httpError(w, http.StatusBadGateway, err, "Failed to fetch from target", slog.String("url", fullURL))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		w.WriteHeader(resp.StatusCode)
		if _, err := io.Copy(w, resp.Body); err != nil {
			d.logger.Error("Failed to copy error response", slog.String("error", err.Error()), slog.String("url", fullURL))
		}
		return
	}

	responseHeaders := textproto.MIMEHeader(maps.Clone(resp.Header))
	cw, err := d.cache.Create(r.Context(), key, responseHeaders, 0)
	if err != nil {
		d.httpError(w, http.StatusInternalServerError, err, "Failed to create cache entry", slog.String("url", fullURL))
		return
	}

	mw := io.MultiWriter(w, cw)
	_, copyErr := io.Copy(mw, resp.Body)
	closeErr := cw.Close()
	if err := errors.Join(copyErr, closeErr); err != nil {
		d.logger.Error("Failed to write to cache", slog.String("error", err.Error()), slog.String("url", fullURL))
	}
}

func (d *Host) httpError(w http.ResponseWriter, code int, err error, message string, args ...any) {
	args = append(args, slog.String("error", err.Error()))
	d.logger.Error(message, args...)
	http.Error(w, message, code)
}
