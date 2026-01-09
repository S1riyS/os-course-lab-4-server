package logging

import "context"

func GetRequestIDFromCtx(ctx context.Context) string {
	if v := ctx.Value(reqKey); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}
