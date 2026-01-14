package main

import (
	"github.com/alecthomas/kong"

	"github.com/block/cachew/internal/logging"
)

var cli struct {
	logging.Config
}

func main() {
	kong.Parse(&cli)
}
