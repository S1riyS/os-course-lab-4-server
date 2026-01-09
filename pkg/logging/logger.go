package logging

import (
	"context"
	"log/slog"
	"os"

	"github.com/google/uuid"
)

type ctxLoggerKey struct {
	Key string
}

var (
	cKey   = ctxLoggerKey{Key: "logger"}
	reqKey = ctxLoggerKey{Key: "request_id"}
)

func GetLoggerFromContext(ctx context.Context) *slog.Logger {
	logger := ctx.Value(cKey)
	if logger != nil {
		l := logger.(*slog.Logger)
		return l
	}

	// Default stdout logger
	l := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	return l
}

// Returns logger from context and attaches operation name
func GetLoggerFromContextWithOp(ctx context.Context, op string) *slog.Logger {
	logger := ctx.Value(cKey)
	var l *slog.Logger

	if logger != nil {
		l = logger.(*slog.Logger)
	} else {
		l = slog.New(slog.NewJSONHandler(os.Stdout, nil))
	}

	l = l.With(slog.String("op", op))
	return l
}

func GetLoggerFromContextWithReqID(ctx context.Context) *slog.Logger {
	logger := ctx.Value(cKey)
	if logger != nil {
		l := logger.(*slog.Logger)
		return l
	}

	l := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	requestID := GetRequestIDFromCtx(ctx)

	if requestID == "" {
		l = slogWithReqId(l)
	} else {
		l = l.With(slog.String("request_id", requestID))
	}

	return l
}

func MakeContextWithLogger(ctx context.Context, logger *slog.Logger) context.Context {
	ctx = context.WithValue(ctx, cKey, logger)
	return ctx
}

func slogWithReqId(l *slog.Logger) *slog.Logger {
	l = l.With(slog.String("request_id", uuid.New().String()))
	return l
}
