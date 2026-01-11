package middleware

import (
	"net/http"

	"github.com/S1riyS/os-course-lab-4/server/pkg/logging"
)

func RequestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// Check if request_id is already in context
		requestID := logging.GetRequestIDFromCtx(ctx)

		// If not in context, check X-Request-ID header
		if requestID == "" {
			requestID = r.Header.Get("X-Request-ID")
		}

		// If still no request_id, generate a new one
		if requestID == "" {
			ctx = logging.MakeContextWithNewRequestID(ctx)
		} else {
			ctx = logging.MakeContextWithRequestID(ctx, requestID)
		}

		// Create a new request with the updated context
		r = r.WithContext(ctx)

		// Call the next handler
		next.ServeHTTP(w, r)
	})
}
