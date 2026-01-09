package strategy

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/url"

	"github.com/alecthomas/errors"

	"github.com/block/sfptc/internal/cache"
	"github.com/block/sfptc/internal/httputil"
	"github.com/block/sfptc/internal/logging"
)

func init() {
	Register("host", NewHost)
}

// HostConfig represents the configuration for the Host strategy.
//
// In HCL it looks something like this:
//
//	host {
//		target = "https://github.com/"
//	}
//
// In this example, the strategy will be mounted under "/github.com".
type HostConfig struct {
	Target string `hcl:"target,label" help:"The target URL to proxy requests to."`
}

// The Host [Strategy] forwards all GET requests to the specified host, caching the response payloads.
type Host struct {
	target *url.URL
	cache  cache.Cache
	client *http.Client
	logger *slog.Logger
	prefix string
}

var _ Strategy = (*Host)(nil)

func NewHost(ctx context.Context, config HostConfig, cache cache.Cache, mux Mux) (*Host, error) {
	u, err := url.Parse(config.Target)
	if err != nil {
		return nil, fmt.Errorf("invalid target URL: %w", err)
	}
	prefix := "/" + u.Host + u.EscapedPath()
	h := &Host{
		target: u,
		cache:  cache,
		client: &http.Client{},
		logger: logging.FromContext(ctx),
		prefix: prefix,
	}
	mux.HandleFunc("GET "+prefix+"/", h.serveHTTP)
	return h, nil
}

func (d *Host) String() string { return "host:" + d.target.Host + d.target.Path }

func (d *Host) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Strip the prefix from the request path
	path := r.URL.Path
	if len(path) >= len(d.prefix) {
		path = path[len(d.prefix):]
	}
	if path == "" {
		path = "/"
	}

	targetURL, err := url.Parse(d.target.String())
	if err != nil {
		httputil.ErrorResponse(w, r, http.StatusInternalServerError, "Failed to parse target URL", "error", err.Error(), "upstream", d.target.String())
		return
	}
	targetURL.Path = path
	targetURL.RawQuery = r.URL.RawQuery
	fullURL := targetURL.String()

	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, fullURL, nil)
	if err != nil {
		httputil.ErrorResponse(w, r, http.StatusInternalServerError, "Failed to create request", "error", err.Error(), "upstream", fullURL)
		return
	}

	resp, err := cache.Fetch(d.client, req, d.cache)
	if err != nil {
		if httpErr, ok := errors.AsType[httputil.HTTPError](err); ok {
			httputil.ErrorResponse(w, r, httpErr.StatusCode(), httpErr.Error(), "error", httpErr.Error(), "upstream", fullURL)
		} else {
			httputil.ErrorResponse(w, r, http.StatusInternalServerError, "Failed to fetch", "error", err.Error(), "upstream", fullURL)
		}
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		w.WriteHeader(resp.StatusCode)
		if _, err := io.Copy(w, resp.Body); err != nil {
			d.logger.Error("Failed to copy error response", "error", err.Error(), "upstream", fullURL)
		}
		return
	}

	maps.Copy(w.Header(), resp.Header)
	if _, err := io.Copy(w, resp.Body); err != nil {
		d.logger.Error("Failed to copy response", "error", err.Error(), "upstream", fullURL)
	}
}
