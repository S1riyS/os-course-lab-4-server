package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/S1riyS/os-course-lab-4/server/internal/models"
	"github.com/S1riyS/os-course-lab-4/server/pkg/database/postgresql"
	"github.com/jackc/pgx/v5"
)

type InodeRepository interface {
	Get(ctx context.Context, token string, ino int64) (*models.Inode, error)
	Create(ctx context.Context, inode *models.Inode) error
	UpdateSize(ctx context.Context, token string, ino int64, size int64) error
	UpdateRefCount(ctx context.Context, token string, ino int64, delta int) error
	Delete(ctx context.Context, token string, ino int64) error
	IsDir(ctx context.Context, token string, ino int64) (bool, error)
	IsFile(ctx context.Context, token string, ino int64) (bool, error)
}

type inodeRepository struct {
	db postgresql.Client
}

func NewInodeRepository(db postgresql.Client) InodeRepository {
	return &inodeRepository{db: db}
}

func (r *inodeRepository) Get(ctx context.Context, token string, ino int64) (*models.Inode, error) {
	const op = "repository.inodeRepository.Get"

	query := `
		SELECT ino, token, type, mode, size, ref_count
		FROM inodes
		WHERE token = $1 AND ino = $2
	`

	var inode models.Inode
	db := postgresql.GetDBClient(ctx, r.db)
	err := db.QueryRow(ctx, query, token, ino).Scan(
		&inode.Ino,
		&inode.Token,
		&inode.Type,
		&inode.Mode,
		&inode.Size,
		&inode.RefCount,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return &inode, nil
}

func (r *inodeRepository) Create(ctx context.Context, inode *models.Inode) error {
	const op = "repository.inodeRepository.Create"

	query := `
		INSERT INTO inodes (ino, token, type, mode, size, ref_count)
		VALUES ($1, $2, $3, $4, $5, $6)
	`

	db := postgresql.GetDBClient(ctx, r.db)
	_, err := db.Exec(ctx, query,
		inode.Ino,
		inode.Token,
		inode.Type,
		inode.Mode,
		inode.Size,
		inode.RefCount,
	)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func (r *inodeRepository) UpdateSize(ctx context.Context, token string, ino int64, size int64) error {
	const op = "repository.inodeRepository.UpdateSize"

	query := `
		UPDATE inodes
		SET size = $1, updated_at = NOW()
		WHERE token = $2 AND ino = $3
	`

	db := postgresql.GetDBClient(ctx, r.db)
	_, err := db.Exec(ctx, query, size, token, ino)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func (r *inodeRepository) UpdateRefCount(ctx context.Context, token string, ino int64, delta int) error {
	const op = "repository.inodeRepository.UpdateRefCount"

	query := `
		UPDATE inodes
		SET ref_count = ref_count + $1
		WHERE token = $2 AND ino = $3
	`

	db := postgresql.GetDBClient(ctx, r.db)
	_, err := db.Exec(ctx, query, delta, token, ino)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func (r *inodeRepository) Delete(ctx context.Context, token string, ino int64) error {
	const op = "repository.inodeRepository.Delete"

	query := `
		DELETE FROM inodes
		WHERE token = $1 AND ino = $2
	`

	db := postgresql.GetDBClient(ctx, r.db)
	_, err := db.Exec(ctx, query, token, ino)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func (r *inodeRepository) IsDir(ctx context.Context, token string, ino int64) (bool, error) {
	const op = "repository.inodeRepository.IsDir"

	query := `
		SELECT type
		FROM inodes
		WHERE token = $1 AND ino = $2
	`

	var nodeType int16
	db := postgresql.GetDBClient(ctx, r.db)
	err := db.QueryRow(ctx, query, token, ino).Scan(&nodeType)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("%s: %w", op, err)
	}

	return nodeType == int16(models.NodeTypeDir), nil
}

func (r *inodeRepository) IsFile(ctx context.Context, token string, ino int64) (bool, error) {
	const op = "repository.inodeRepository.IsFile"

	query := `
		SELECT type
		FROM inodes
		WHERE token = $1 AND ino = $2
	`

	var nodeType int16
	db := postgresql.GetDBClient(ctx, r.db)
	err := db.QueryRow(ctx, query, token, ino).Scan(&nodeType)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("%s: %w", op, err)
	}

	return nodeType == int16(models.NodeTypeFile), nil
}
