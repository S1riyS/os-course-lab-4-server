package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/S1riyS/os-course-lab-4/server/internal/models"
	"github.com/S1riyS/os-course-lab-4/server/pkg/database/postgresql"
	"github.com/jackc/pgx/v5"
)

type DirectoryRepository interface {
	Lookup(ctx context.Context, token string, parentIno int64, name string) (int64, error)
	CreateEntry(ctx context.Context, token string, parentIno int64, name string, ino int64) error
	DeleteEntry(ctx context.Context, token string, parentIno int64, name string) error
	GetEntries(ctx context.Context, token string, parentIno int64) ([]models.Dirent, error)
	GetEntryByOffset(ctx context.Context, token string, parentIno int64, offset uint64) (*models.Dirent, error)
	IsEmpty(ctx context.Context, token string, dirIno int64) (bool, error)
	Exists(ctx context.Context, token string, parentIno int64, name string) (bool, error)
}

type directoryRepository struct {
	db postgresql.Client
}

func NewDirectoryRepository(db postgresql.Client) DirectoryRepository {
	return &directoryRepository{db: db}
}

func (r *directoryRepository) Lookup(ctx context.Context, token string, parentIno int64, name string) (int64, error) {
	const op = "repository.directoryRepository.Lookup"

	query := `
		SELECT ino
		FROM directory_entries
		WHERE token = $1 AND parent_ino = $2 AND name = $3
	`

	var ino int64
	db := postgresql.GetDBClient(ctx, r.db)
	err := db.QueryRow(ctx, query, token, parentIno, name).Scan(&ino)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("%s: %w", op, err)
	}

	return ino, nil
}

func (r *directoryRepository) CreateEntry(ctx context.Context, token string, parentIno int64, name string, ino int64) error {
	const op = "repository.directoryRepository.CreateEntry"

	query := `
		INSERT INTO directory_entries (token, parent_ino, name, ino)
		VALUES ($1, $2, $3, $4)
	`

	db := postgresql.GetDBClient(ctx, r.db)
	_, err := db.Exec(ctx, query, token, parentIno, name, ino)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func (r *directoryRepository) DeleteEntry(ctx context.Context, token string, parentIno int64, name string) error {
	const op = "repository.directoryRepository.DeleteEntry"

	query := `
		DELETE FROM directory_entries
		WHERE token = $1 AND parent_ino = $2 AND name = $3
	`

	db := postgresql.GetDBClient(ctx, r.db)
	_, err := db.Exec(ctx, query, token, parentIno, name)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func (r *directoryRepository) GetEntries(ctx context.Context, token string, parentIno int64) ([]models.Dirent, error) {
	const op = "repository.directoryRepository.GetEntries"

	query := `
		SELECT de.name, de.ino, i.type
		FROM directory_entries de
		JOIN inodes i ON de.token = i.token AND de.ino = i.ino
		WHERE de.token = $1 AND de.parent_ino = $2
		ORDER BY de.name
	`

	db := postgresql.GetDBClient(ctx, r.db)
	rows, err := db.Query(ctx, query, token, parentIno)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	defer rows.Close()

	var entries []models.Dirent
	for rows.Next() {
		var dirent models.Dirent
		var nodeType int16
		err := rows.Scan(&dirent.Name, &dirent.Ino, &nodeType)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", op, err)
		}
		dirent.Type = models.NodeType(nodeType)
		entries = append(entries, dirent)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return entries, nil
}

func (r *directoryRepository) GetEntryByOffset(ctx context.Context, token string, parentIno int64, offset uint64) (*models.Dirent, error) {
	const op = "repository.directoryRepository.GetEntryByOffset"

	query := `
		SELECT de.name, de.ino, i.type
		FROM directory_entries de
		JOIN inodes i ON de.token = i.token AND de.ino = i.ino
		WHERE de.token = $1 AND de.parent_ino = $2
		ORDER BY de.name
		LIMIT 1 OFFSET $3
	`

	var dirent models.Dirent
	var nodeType int16
	db := postgresql.GetDBClient(ctx, r.db)
	err := db.QueryRow(ctx, query, token, parentIno, offset).Scan(&dirent.Name, &dirent.Ino, &nodeType)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	dirent.Type = models.NodeType(nodeType)
	return &dirent, nil
}

func (r *directoryRepository) IsEmpty(ctx context.Context, token string, dirIno int64) (bool, error) {
	const op = "repository.directoryRepository.IsEmpty"

	query := `
		SELECT COUNT(*)
		FROM directory_entries
		WHERE token = $1 AND parent_ino = $2
	`

	var count int
	db := postgresql.GetDBClient(ctx, r.db)
	err := db.QueryRow(ctx, query, token, dirIno).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("%s: %w", op, err)
	}

	return count == 0, nil
}

func (r *directoryRepository) Exists(ctx context.Context, token string, parentIno int64, name string) (bool, error) {
	const op = "repository.directoryRepository.Exists"

	query := `
		SELECT EXISTS(
			SELECT 1
			FROM directory_entries
			WHERE token = $1 AND parent_ino = $2 AND name = $3
		)
	`

	var exists bool
	db := postgresql.GetDBClient(ctx, r.db)
	err := db.QueryRow(ctx, query, token, parentIno, name).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("%s: %w", op, err)
	}

	return exists, nil
}
