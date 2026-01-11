package postgresql

import (
	"context"

	"github.com/jackc/pgx/v5"
)

type txKey struct{}

// WithTransaction executes function inside a transaction
func WithTransaction(ctx context.Context, db Client, fn func(context.Context) error) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}

	txCtx := context.WithValue(ctx, txKey{}, tx)

	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback(ctx)
			panic(p)
		} else if err != nil {
			_ = tx.Rollback(ctx)
		} else {
			err = tx.Commit(ctx)
		}
	}()

	err = fn(txCtx)
	return err
}

// postgresql.GetDBClient returns transaction from context if present, otherwise returns the default client
func GetDBClient(ctx context.Context, defaultClient Client) Client {
	if tx, ok := ctx.Value(txKey{}).(pgx.Tx); ok {
		return tx
	}
	return defaultClient
}
