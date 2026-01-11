package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/S1riyS/os-course-lab-4/server/pkg/database/postgresql"
	"github.com/jackc/pgx/v5"
)

type ContentRepository interface {
	Get(ctx context.Context, token string, ino int64) ([]byte, error)
	GetRange(ctx context.Context, token string, ino int64, offset int64, length int64) ([]byte, error)
	Set(ctx context.Context, token string, ino int64, data []byte) error
	Delete(ctx context.Context, token string, ino int64) error
}

type contentRepository struct {
	db postgresql.Client
}

func NewContentRepository(db postgresql.Client) ContentRepository {
	return &contentRepository{db: db}
}

func (r *contentRepository) Get(ctx context.Context, token string, ino int64) ([]byte, error) {
	const op = "repository.contentRepository.Get"

	query := `
		SELECT data
		FROM file_contents
		WHERE token = $1 AND ino = $2
	`

	var data []byte
	db := postgresql.GetDBClient(ctx, r.db)
	err := db.QueryRow(ctx, query, token, ino).Scan(&data)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return []byte{}, nil
		}
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return data, nil
}

func (r *contentRepository) GetRange(ctx context.Context, token string, ino int64, offset int64, length int64) ([]byte, error) {
	const op = "repository.contentRepository.GetRange"

	data, err := r.Get(ctx, token, ino)
	if err != nil {
		return nil, err
	}

	dataLen := int64(len(data))
	if offset >= dataLen {
		return []byte{}, nil
	}

	available := dataLen - offset
	toRead := length
	if toRead > available {
		toRead = available
	}

	return data[offset : offset+toRead], nil
}

func (r *contentRepository) Set(ctx context.Context, token string, ino int64, data []byte) error {
	const op = "repository.contentRepository.Set"

	query := `
		INSERT INTO file_contents (token, ino, data)
		VALUES ($1, $2, $3)
		ON CONFLICT (token, ino)
		DO UPDATE SET data = EXCLUDED.data
	`

	db := postgresql.GetDBClient(ctx, r.db)
	_, err := db.Exec(ctx, query, token, ino, data)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func (r *contentRepository) Delete(ctx context.Context, token string, ino int64) error {
	const op = "repository.contentRepository.Delete"

	query := `
		DELETE FROM file_contents
		WHERE token = $1 AND ino = $2
	`

	db := postgresql.GetDBClient(ctx, r.db)
	_, err := db.Exec(ctx, query, token, ino)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}
