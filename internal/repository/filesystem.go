package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/S1riyS/os-course-lab-4/server/internal/models"
	"github.com/S1riyS/os-course-lab-4/server/pkg/database/postgresql"
	"github.com/S1riyS/os-course-lab-4/server/pkg/logging"
	"github.com/S1riyS/os-course-lab-4/server/pkg/logging/slogext"
	"github.com/jackc/pgx/v5"
)

const (
	VTFS_ROOT_INO  = 1000
	VTFS_ROOT_MODE = 0777
)

type FilesystemRepository interface {
	Create(ctx context.Context, token string) error
	Get(ctx context.Context, token string) (*models.Filesystem, error)
	GetOrCreate(ctx context.Context, token string) (*models.Filesystem, error)
	GetNextIno(ctx context.Context, token string) (int64, error)
	IncrementNextIno(ctx context.Context, token string) error
}

type filesystemRepository struct {
	db postgresql.Client
}

func NewFilesystemRepository(db postgresql.Client) FilesystemRepository {
	return &filesystemRepository{db: db}
}

func (r *filesystemRepository) Create(ctx context.Context, token string) error {
	const op = "repository.filesystemRepository.Create"

	logger := logging.GetLoggerFromContextWithOp(ctx, op)

	query := `
		INSERT INTO filesystems (token, root_ino, next_ino)
		VALUES ($1, $2, $3)
		ON CONFLICT (token) DO NOTHING
	`

	db := postgresql.GetDBClient(ctx, r.db)
	_, err := db.Exec(ctx, query, token, VTFS_ROOT_INO, VTFS_ROOT_INO+1)
	if err != nil {
		logger.Error("Failed to create filesystem", slogext.Err(err), "token", token)
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func (r *filesystemRepository) Get(ctx context.Context, token string) (*models.Filesystem, error) {
	const op = "repository.filesystemRepository.Get"

	query := `
		SELECT token, root_ino, next_ino, created_at
		FROM filesystems
		WHERE token = $1
	`

	var fs models.Filesystem
	db := postgresql.GetDBClient(ctx, r.db)
	err := db.QueryRow(ctx, query, token).Scan(
		&fs.Token,
		&fs.RootIno,
		&fs.NextIno,
		&fs.CreateAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return &fs, nil
}

func (r *filesystemRepository) GetOrCreate(ctx context.Context, token string) (*models.Filesystem, error) {
	fs, err := r.Get(ctx, token)
	if err != nil {
		return nil, err
	}

	if fs != nil {
		return fs, nil
	}

	err = postgresql.WithTransaction(ctx, r.db, func(ctx context.Context) error {
		fs, err = r.Get(ctx, token)
		if err != nil {
			return err
		}
		if fs != nil {
			return nil
		}

		fsQuery := `
			INSERT INTO filesystems (token, root_ino, next_ino)
			VALUES ($1, $2, $3)
			ON CONFLICT (token) DO NOTHING
		`
		db := postgresql.GetDBClient(ctx, r.db)
		_, err = db.Exec(ctx, fsQuery, token, VTFS_ROOT_INO, VTFS_ROOT_INO+1)
		if err != nil {
			return err
		}

		inodeQuery := `
			INSERT INTO inodes (ino, token, type, mode, size, ref_count)
			VALUES ($1, $2, $3, $4, $5, $6)
		`
		_, err = db.Exec(ctx, inodeQuery,
			VTFS_ROOT_INO,
			token,
			int16(models.NodeTypeDir),
			VTFS_ROOT_MODE,
			0, // size
			1, // ref_count
		)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return r.Get(ctx, token)
}

func (r *filesystemRepository) GetNextIno(ctx context.Context, token string) (int64, error) {
	const op = "repository.filesystemRepository.GetNextIno"

	query := `
		SELECT next_ino
		FROM filesystems
		WHERE token = $1
	`

	var nextIno int64
	db := postgresql.GetDBClient(ctx, r.db)
	err := db.QueryRow(ctx, query, token).Scan(&nextIno)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", op, err)
	}

	return nextIno, nil
}

func (r *filesystemRepository) IncrementNextIno(ctx context.Context, token string) error {
	const op = "repository.filesystemRepository.IncrementNextIno"

	query := `
		UPDATE filesystems
		SET next_ino = next_ino + 1
		WHERE token = $1
	`

	db := postgresql.GetDBClient(ctx, r.db)
	_, err := db.Exec(ctx, query, token)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}
