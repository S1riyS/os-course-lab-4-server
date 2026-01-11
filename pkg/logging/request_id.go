package logging

import (
	"context"

	"github.com/google/uuid"
)

func GetRequestIDFromCtx(ctx context.Context) string {
	if v := ctx.Value(reqKey); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func MakeContextWithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, reqKey, requestID)
}

func MakeContextWithNewRequestID(ctx context.Context) context.Context {
	return MakeContextWithRequestID(ctx, uuid.New().String())
}
