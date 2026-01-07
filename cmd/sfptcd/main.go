package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/alecthomas/kong"

	"github.com/block/sfptc/internal/config"
	"github.com/block/sfptc/internal/logging"
)

var cli struct {
	Config        *os.File       `hcl:"-" help:"Configuration file path." placeholder:"PATH" required:""`
	Bind          string         `hcl:"bind" default:"127.0.0.1:8080" help:"Bind address for the server."`
	LoggingConfig logging.Config `embed:"" prefix:"log-"`
}

func main() {
	kctx := kong.Parse(&cli)

	ctx := context.Background()
	logger, ctx := logging.Configure(ctx, cli.LoggingConfig)

	mux := http.NewServeMux()

	err := config.Load(ctx, cli.Config, mux)
	kctx.FatalIfErrorf(err)

	logger.InfoContext(ctx, "Starting sfptcd", slog.String("bind", cli.Bind))

	server := &http.Server{
		Addr:              cli.Bind,
		Handler:           mux,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		ReadHeaderTimeout: 10 * time.Second,
	}
	err = server.ListenAndServe()
	kctx.FatalIfErrorf(err)
}
