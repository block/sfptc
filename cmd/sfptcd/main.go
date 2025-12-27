package main

import (
	"context"

	"github.com/alecthomas/kong"

	"github.com/block/sfptc/internal/logging"
)

var cli struct {
	logging.Config `prefix:"log-"`
}

func main() {
	kong.Parse(&cli)

	ctx := context.Background()
	logger, ctx := logging.Configure(ctx, cli.Config)

	logger.InfoContext(ctx, "Starting sfptcd")
}
