package nest

import (
	"context"

	"go.uber.org/zap"
)

type nestContextKey string

const (
	ctxLoggerKey nestContextKey = "logger"
)

func StoreLogger(ctx context.Context, l *zap.Logger) context.Context {
	return context.WithValue(ctx, ctxLoggerKey, l)
}

func L(ctx context.Context) *zap.Logger {
	return ctx.Value(ctxLoggerKey).(*zap.Logger)
}
func AddFields(ctx context.Context, fields ...zap.Field) context.Context {
	return StoreLogger(ctx, L(ctx).With(fields...))
}
