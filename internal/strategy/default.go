package strategy

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/block/sfptc/internal/cache"
	"github.com/block/sfptc/internal/logging"
)

func init() {
	Register("default", NewDefault)
}

type DefaultConfig struct{}

var _ Strategy = (*Default)(nil)

// The Default strategy represents v1 of the proxy API.
type Default struct {
	cache  cache.Cache
	logger *slog.Logger
	mux    *http.ServeMux
}

var _ http.Handler = (*Default)(nil)

func NewDefault(ctx context.Context, _ DefaultConfig, cache cache.Cache) (*Default, error) {
	s := &Default{
		logger: logging.FromContext(ctx),
		cache:  cache,
		mux:    http.NewServeMux(),
	}
	s.mux.Handle("GET /{key}", http.HandlerFunc(s.getObject))
	s.mux.Handle("POST /{key}", http.HandlerFunc(s.putObject))
	s.mux.Handle("DELETE /{key}", http.HandlerFunc(s.deleteObject))
	return s, nil
}

func (d *Default) String() string { return "default" }

func (d *Default) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	d.mux.ServeHTTP(w, r)
}

func (d *Default) getObject(w http.ResponseWriter, r *http.Request) {
	key, err := cache.ParseKey(r.PathValue("key"))
	if err != nil {
		d.httpError(w, http.StatusBadRequest, err, "Invalid key")
		return
	}

	cr, err := d.cache.Open(r.Context(), key)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			d.httpError(w, http.StatusNotFound, err, "Cache object not found", slog.String("key", key.String()))
			return
		}
		d.httpError(w, http.StatusInternalServerError, err, "Failed to open cache object", slog.String("key", key.String()))
		return
	}

	_, err = io.Copy(w, cr)
	if err != nil {
		d.logger.Error("Failed to copy cache object to response", slog.String("error", err.Error()), slog.String("key", key.String()))
	}
	if cerr := cr.Close(); cerr != nil {
		d.logger.Error("Failed to close cache reader", slog.String("error", cerr.Error()), slog.String("key", key.String()))
	}
}

func (d *Default) putObject(w http.ResponseWriter, r *http.Request) {
	key, err := cache.ParseKey(r.PathValue("key"))
	if err != nil {
		d.httpError(w, http.StatusBadRequest, err, "Invalid key")
		return
	}

	var ttl time.Duration
	ttlh := r.Header.Get("Time-To-Live")
	if ttlh != "" {
		ttl, err = time.ParseDuration(ttlh)
		if err != nil {
			d.httpError(w, http.StatusBadRequest, err, "Invalid Time-To-Live header format, must be in Go duration format eg. 1h")
			return
		}
	}

	cw, err := d.cache.Create(r.Context(), key, ttl)
	if err != nil {
		d.httpError(w, http.StatusInternalServerError, err, "Failed to create cache writer", slog.String("key", key.String()))
		return
	}

	if _, err := io.Copy(cw, r.Body); err != nil {
		d.httpError(w, http.StatusInternalServerError, err, "Failed to copy request body to cache writer")
		return
	}

	if err := cw.Close(); err != nil {
		d.httpError(w, http.StatusInternalServerError, err, "Failed to close cache writer")
		return
	}
}

func (d *Default) deleteObject(w http.ResponseWriter, r *http.Request) {
	key, err := cache.ParseKey(r.PathValue("key"))
	if err != nil {
		d.httpError(w, http.StatusBadRequest, err, "Invalid key")
		return
	}

	err = d.cache.Delete(r.Context(), key)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			d.httpError(w, http.StatusNotFound, err, "Cache object not found", slog.String("key", key.String()))
			return
		}
		d.httpError(w, http.StatusInternalServerError, err, "Failed to delete cache object", slog.String("key", key.String()))
		return
	}
}

func (d *Default) httpError(w http.ResponseWriter, code int, err error, message string, args ...any) {
	args = append(args, slog.String("error", err.Error()))
	d.logger.Error(message, args...)
	http.Error(w, message, code)
}
