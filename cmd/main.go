package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/S1riyS/os-course-lab-4/server/internal/config"
	"github.com/S1riyS/os-course-lab-4/server/pkg/database/postgresql"
	"github.com/S1riyS/os-course-lab-4/server/pkg/logging"
	"github.com/S1riyS/os-course-lab-4/server/pkg/logging/slogpretty"
)

const configPath = "configs/config.yaml"

func main() {
	cfg := config.MustLoad(configPath)

	prettyLogger := setupPrettySlog()

	// Root context
	ctx := context.Background()
	ctx = logging.MakeContextWithLogger(ctx, prettyLogger)

	// Dependencies
	db := postgresql.MustNewClient(ctx, cfg.Database)
	_ = db

	fmt.Println("Hello World!")
}

func setupPrettySlog() *slog.Logger {
	opts := slogpretty.PrettyHandlerOptions{
		SlogOpts: &slog.HandlerOptions{
			Level: slog.LevelDebug,
		},
	}

	handler := opts.NewPrettyHandler(os.Stdout)

	return slog.New(handler)
}
