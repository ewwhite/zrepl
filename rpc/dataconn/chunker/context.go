package chunker

import (
	"context"

	"github.com/zrepl/zrepl/logger"
)

type contextKey int

const (
	contextKeyLog = 1 + iota
)

type Logger = logger.Logger

func WithLogger(ctx context.Context, logger Logger) context.Context {
	return context.WithValue(ctx, contextKeyLog, logger)
}

func getLog(ctx context.Context) Logger {
	if l, ok := ctx.Value(contextKeyLog).(Logger); ok {
		return l
	}
	return logger.NewNullLogger()
}
