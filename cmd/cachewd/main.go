package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/alecthomas/chroma/v2/quick"
	"github.com/alecthomas/hcl/v2"
	"github.com/alecthomas/kong"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/config"
	"github.com/block/cachew/internal/httputil"
	"github.com/block/cachew/internal/jobscheduler"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/metrics"
	"github.com/block/cachew/internal/strategy"
	"github.com/block/cachew/internal/strategy/git"
	"github.com/block/cachew/internal/strategy/gomod"
)

type GlobalConfig struct {
	Bind            string              `hcl:"bind" default:"127.0.0.1:8080" help:"Bind address for the server."`
	URL             string              `hcl:"url" default:"http://127.0.0.1:8080/" help:"Base URL for cachewd."`
	SchedulerConfig jobscheduler.Config `embed:"" hcl:"scheduler,block" prefix:"scheduler-"`
	LoggingConfig   logging.Config      `embed:"" hcl:"logging,block" prefix:"log-"`
	MetricsConfig   metrics.Config      `embed:"" hcl:"metrics,block" prefix:"metrics-"`
}

var cli struct {
	Schema bool `help:"Print the configuration file schema." xor:"command"`

	Config *os.File `hcl:"-" help:"Configuration file path." placeholder:"PATH" required:"" default:"cachew.hcl"`

	// GlobalConfig accepts command-line, but can also be parsed from HCL.
	GlobalConfig
}

func main() {
	kctx, providersConfig := parseConfig()

	ctx := context.Background()
	logger, ctx := logging.Configure(ctx, cli.LoggingConfig)

	startServer(ctx, logger, kctx, providersConfig)
}

func parseConfig() (*kong.Context, *hcl.AST) {
	// 1. Get defaults
	defaults := struct{ GlobalConfig }{}
	_, err := kong.New(&defaults, kong.Exit(func(int) {}))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting defaults: %v\n", err)
		os.Exit(1)
	}

	// 2. Parse CLI/env
	kctx := kong.Parse(&cli, kong.DefaultEnvars("CACHEW"))

	// 3. Save CLI/env values that differ from defaults (these take precedence)
	saved := saveNonDefaultValues(&cli.GlobalConfig, &defaults.GlobalConfig)

	// 4. Parse and unmarshal HCL (this overwrites cli.GlobalConfig)
	ast, err := hcl.Parse(cli.Config)
	kctx.FatalIfErrorf(err)

	globalConfig, providersConfig := config.Split[GlobalConfig](ast)

	err = hcl.UnmarshalAST(globalConfig, &cli.GlobalConfig)
	kctx.FatalIfErrorf(err)

	// 5. Restore CLI/env values (precedence: defaults < HCL < env < CLI)
	restoreValues(&cli.GlobalConfig, saved)

	return kctx, providersConfig
}

func startServer(ctx context.Context, logger *slog.Logger, kctx *kong.Context, providersConfig *hcl.AST) {
	scheduler := jobscheduler.New(ctx, cli.SchedulerConfig)

	cr := cache.NewRegistry()
	cache.RegisterMemory(cr)
	cache.RegisterDisk(cr)
	cache.RegisterS3(cr)

	sr := strategy.NewRegistry()
	strategy.RegisterAPIV1(sr)
	strategy.RegisterArtifactory(sr)
	strategy.RegisterGitHubReleases(sr)
	strategy.RegisterHermit(sr, cli.URL)
	strategy.RegisterHost(sr)
	git.Register(sr, scheduler)
	gomod.Register(sr)

	// Commands
	switch { //nolint:gocritic
	case cli.Schema:
		schema := config.Schema[GlobalConfig](cr, sr)
		slices.SortStableFunc(schema.Entries, func(a, b hcl.Entry) int {
			return strings.Compare(a.EntryKey(), b.EntryKey())
		})
		text, err := hcl.MarshalAST(schema)
		kctx.FatalIfErrorf(err)

		if fileInfo, err := os.Stdout.Stat(); err == nil && (fileInfo.Mode()&os.ModeCharDevice) != 0 {
			err = quick.Highlight(os.Stdout, string(text), "terraform", "terminal256", "solarized")
			kctx.FatalIfErrorf(err)
		} else {
			fmt.Printf("%s\n", text) //nolint:forbidigo
		}
		return
	}

	mux := http.NewServeMux()

	// Health check endpoints
	mux.HandleFunc("GET /_liveness", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK")) //nolint:errcheck
	})

	mux.HandleFunc("GET /_readiness", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK")) //nolint:errcheck
	})

	err := config.Load(ctx, cr, sr, providersConfig, mux, parseEnvars())
	kctx.FatalIfErrorf(err)

	metricsClient, metricsErr := metrics.New(ctx, cli.MetricsConfig)
	kctx.FatalIfErrorf(metricsErr, "failed to create metrics client")
	defer func() {
		if err := metricsClient.Close(); err != nil {
			logger.ErrorContext(ctx, "failed to close metrics client", "error", err)
		}
	}()

	if err := metricsClient.ServeMetrics(ctx); err != nil {
		kctx.FatalIfErrorf(err, "failed to start metrics server")
	}

	logger.InfoContext(ctx, "Starting cachewd", slog.String("bind", cli.Bind))

	var handler http.Handler = mux

	handler = otelhttp.NewMiddleware(cli.MetricsConfig.ServiceName,
		otelhttp.WithMeterProvider(otel.GetMeterProvider()),
		otelhttp.WithTracerProvider(otel.GetTracerProvider()),
	)(handler)

	handler = httputil.LoggingMiddleware(handler)

	server := &http.Server{
		Addr:              cli.Bind,
		Handler:           handler,
		ReadTimeout:       30 * time.Minute,
		WriteTimeout:      30 * time.Minute,
		ReadHeaderTimeout: 30 * time.Second,
		BaseContext: func(net.Listener) context.Context {
			return ctx
		},
		ConnContext: func(ctx context.Context, c net.Conn) context.Context {
			return logging.ContextWithLogger(ctx, logger.With("client", c.RemoteAddr().String()))
		},
	}

	err = server.ListenAndServe()
	kctx.FatalIfErrorf(err)
}

func parseEnvars() map[string]string {
	envars := map[string]string{}
	for _, env := range os.Environ() {
		if key, value, ok := strings.Cut(env, "="); ok {
			envars[key] = value
		}
	}
	return envars
}

// buildFieldPath constructs a dot-separated field path.
func buildFieldPath(path, fieldName string) string {
	if path != "" {
		return path + "." + fieldName
	}
	return fieldName
}

// saveNonDefaultValues recursively saves field values that differ from defaults.
// Returns a map of field paths to their values.
func saveNonDefaultValues(target, defaults *GlobalConfig) map[string]any {
	saved := make(map[string]any)
	saveFieldValues(reflect.ValueOf(target).Elem(), reflect.ValueOf(defaults).Elem(), "", saved)
	return saved
}

func saveFieldValues(targetVal, defaultsVal reflect.Value, path string, saved map[string]any) {
	targetType := targetVal.Type()

	for i := range targetVal.NumField() {
		field := targetType.Field(i)
		targetField := targetVal.Field(i)
		defaultField := defaultsVal.Field(i)

		// Skip unexported fields
		if !targetField.CanSet() {
			continue
		}

		fieldPath := buildFieldPath(path, field.Name)

		// If the field is a struct, recurse into it
		if targetField.Kind() == reflect.Struct {
			saveFieldValues(targetField, defaultField, fieldPath, saved)
			continue
		}

		// If the field differs from default, save it
		if !reflect.DeepEqual(targetField.Interface(), defaultField.Interface()) {
			saved[fieldPath] = targetField.Interface()
		}
	}
}

// restoreValues recursively restores saved values back into the target struct.
func restoreValues(target *GlobalConfig, saved map[string]any) {
	restoreFieldValues(reflect.ValueOf(target).Elem(), "", saved)
}

func restoreFieldValues(targetVal reflect.Value, path string, saved map[string]any) {
	targetType := targetVal.Type()

	for i := range targetVal.NumField() {
		field := targetType.Field(i)
		targetField := targetVal.Field(i)

		// Skip unexported fields
		if !targetField.CanSet() {
			continue
		}

		fieldPath := buildFieldPath(path, field.Name)

		// If the field is a struct, recurse into it
		if targetField.Kind() == reflect.Struct {
			restoreFieldValues(targetField, fieldPath, saved)
			continue
		}

		// If we have a saved value for this field, restore it
		if savedValue, ok := saved[fieldPath]; ok {
			targetField.Set(reflect.ValueOf(savedValue))
		}
	}
}
