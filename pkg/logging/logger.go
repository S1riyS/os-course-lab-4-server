package logging

import (
	"context"
	"log/slog"
	"os"
)

type ctxLoggerKey struct {
	Key string
}

var (
	cKey   = ctxLoggerKey{Key: "logger"}
	reqKey = ctxLoggerKey{Key: "request_id"}
)

func GetLoggerFromContext(ctx context.Context) *slog.Logger {
	var l *slog.Logger

	logger := ctx.Value(cKey)
	if logger != nil {
		l = logger.(*slog.Logger)
	} else {
		// Default stdout logger
		l = slog.New(slog.NewJSONHandler(os.Stdout, nil))
	}

	// Always attach request ID from context if available
	requestID := GetRequestIDFromCtx(ctx)
	if requestID != "" {
		// Check if logger already has request_id to avoid duplication
		// If request_id is in context, always use it
		l = l.With(slog.String("request_id", requestID))
	}

	return l
}

// Returns logger from context and attaches operation name
func GetLoggerFromContextWithOp(ctx context.Context, op string) *slog.Logger {
	l := GetLoggerFromContext(ctx)

	// Attach operation
	l = l.With(slog.String("op", op))

	return l
}

func MakeContextWithLogger(ctx context.Context, logger *slog.Logger) context.Context {
	ctx = context.WithValue(ctx, cKey, logger)
	return ctx
}
