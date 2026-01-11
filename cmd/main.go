package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/S1riyS/os-course-lab-4/server/internal/config"
	"github.com/S1riyS/os-course-lab-4/server/internal/handler"
	"github.com/S1riyS/os-course-lab-4/server/internal/middleware"
	"github.com/S1riyS/os-course-lab-4/server/internal/repository"
	"github.com/S1riyS/os-course-lab-4/server/internal/service"
	"github.com/S1riyS/os-course-lab-4/server/pkg/database/postgresql"
	"github.com/S1riyS/os-course-lab-4/server/pkg/logging"
	"github.com/S1riyS/os-course-lab-4/server/pkg/logging/slogext"
	"github.com/S1riyS/os-course-lab-4/server/pkg/logging/slogpretty"
)

const configPath = "configs/config.yaml"

func main() {
	cfg := config.MustLoad(configPath)

	prettyLogger := setupPrettySlog()

	// Root context
	ctx := context.Background()
	ctx = logging.MakeContextWithLogger(ctx, prettyLogger)

	logger := logging.GetLoggerFromContextWithOp(ctx, "main")

	// Database
	db := postgresql.MustNewClient(ctx, cfg.Database)

	// Repositories
	fsRepo := repository.NewFilesystemRepository(db)
	inodeRepo := repository.NewInodeRepository(db)
	dirRepo := repository.NewDirectoryRepository(db)
	contentRepo := repository.NewContentRepository(db)

	// Service
	fsService := service.NewFileSystemService(db, fsRepo, inodeRepo, dirRepo, contentRepo)

	// Handler
	h := handler.NewHandler(fsService)

	// Router
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	// Middlewares
	handler := middleware.RequestIDMiddleware(mux)

	// HTTP Server
	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.App.Port),
		Handler:      handler,
		ReadTimeout:  cfg.App.DefaultTimeout,
		WriteTimeout: cfg.App.DefaultTimeout,
		BaseContext:  func(_ net.Listener) context.Context { return ctx },
	}

	// Graceful shutdown
	go func() {
		logger.Info("Starting HTTP server", slog.Int("port", cfg.App.Port))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("Failed to start server", slogext.Err(err))
			panic(err)
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down server...")

	// Graceful shutdown with timeout
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("Server forced to shutdown", slogext.Err(err))
	} else {
		logger.Info("Server exited gracefully")
	}
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
