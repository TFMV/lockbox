package main

import (
	"os"

	"github.com/TFMV/lockbox/cmd/lockbox/cmd"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	// Configure zerolog
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	// Execute the root command
	if err := cmd.Execute(); err != nil {
		log.Fatal().Err(err).Msg("Failed to execute command")
		os.Exit(1)
	}
}
