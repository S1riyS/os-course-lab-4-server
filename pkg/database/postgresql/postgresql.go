package postgresql

import (
	"context"
	"sync"

	"github.com/S1riyS/os-course-lab-4/server/internal/config"
	"github.com/S1riyS/os-course-lab-4/server/pkg/logging"
	"github.com/S1riyS/os-course-lab-4/server/pkg/logging/slogext"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Client interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Begin(ctx context.Context) (pgx.Tx, error)
}

var (
	instance *pgxpool.Pool
	once     sync.Once
)

func MustNewClient(ctx context.Context, cfg config.DatabaseConfig) *pgxpool.Pool {
	once.Do(func() {
		const op = "postgresql.MustNewClient"

		logger := logging.GetLoggerFromContextWithOp(ctx, op)

		dsn := cfg.DSN()
		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			logger.Error("Failed to create connection pool", slogext.Err(err))
			panic(err)
		}

		if err = pool.Ping(ctx); err != nil {
			logger.Error("Failed to connect to database", slogext.Err(err))
			panic(err)
		}

		logger.Info("Connected to database")
		instance = pool
	})

	return instance
}
