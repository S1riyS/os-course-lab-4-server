package service

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/S1riyS/os-course-lab-4/server/internal/models"
	"github.com/S1riyS/os-course-lab-4/server/internal/pkg/kerrors"
	"github.com/S1riyS/os-course-lab-4/server/internal/repository"
	"github.com/S1riyS/os-course-lab-4/server/pkg/database/postgresql"
	"github.com/S1riyS/os-course-lab-4/server/pkg/logging"
	"github.com/S1riyS/os-course-lab-4/server/pkg/logging/slogext"
	"github.com/lib/pq"
)

const (
	VTFS_ROOT_INO = 1000

	S_IFDIR = 0o040000 // Directory
	S_IFREG = 0o100000 // Regular file

	S_IRWXUGO = 0o0777 // Read, write, execute for owner, group, others
)

type FileSystemService interface {
	Init(ctx context.Context, token string) error
	GetRoot(ctx context.Context, token string) (*models.NodeMeta, error)
	Lookup(ctx context.Context, token string, parentIno int64, name string) (*models.NodeMeta, error)
	IterateDir(ctx context.Context, token string, dirIno int64, offset *uint64) (*models.Dirent, error)
	CreateFile(ctx context.Context, token string, parentIno int64, name string, mode uint32) (*models.NodeMeta, error)
	Unlink(ctx context.Context, token string, parentIno int64, name string) error
	CreateDir(ctx context.Context, token string, parentIno int64, name string, mode uint32) (*models.NodeMeta, error)
	Rmdir(ctx context.Context, token string, parentIno int64, name string) error
	Read(ctx context.Context, token string, ino int64, buffer []byte, offset int64) (int64, error)
	Write(ctx context.Context, token string, ino int64, data []byte, length uint64, offset int64) (int64, error)
	Link(ctx context.Context, token string, targetIno int64, parentIno int64, name string) error
	CountLinks(ctx context.Context, token string, ino int64) (uint32, error)
}

type fileSystemService struct {
	db          postgresql.Client
	fsRepo      repository.FilesystemRepository
	inodeRepo   repository.InodeRepository
	dirRepo     repository.DirectoryRepository
	contentRepo repository.ContentRepository
}

func NewFileSystemService(
	db postgresql.Client,
	fsRepo repository.FilesystemRepository,
	inodeRepo repository.InodeRepository,
	dirRepo repository.DirectoryRepository,
	contentRepo repository.ContentRepository,
) FileSystemService {
	return &fileSystemService{
		db:          db,
		fsRepo:      fsRepo,
		inodeRepo:   inodeRepo,
		dirRepo:     dirRepo,
		contentRepo: contentRepo,
	}
}

func (s *fileSystemService) Init(ctx context.Context, token string) error {
	const op = "service.fileSystemService.Init"

	logger := logging.GetLoggerFromContextWithOp(ctx, op)
	logger.Debug("Init filesystem", slog.String("token", token))

	fs, err := s.fsRepo.Get(ctx, token)
	if err != nil {
		logger.Error("Failed to get filesystem", slogext.Err(err), slog.String("token", token))
		return fmt.Errorf("%s: %w", op, err)
	}

	if fs != nil {
		logger.Debug("Filesystem already exists", slog.String("token", token))
		return &ServiceError{Code: kerrors.EEXIST, Message: "filesystem already exists"}
	}

	logger.Debug("Creating new filesystem", slog.String("token", token))
	_, err = s.fsRepo.GetOrCreate(ctx, token)
	if err != nil {
		logger.Error("Failed to create filesystem", slogext.Err(err), slog.String("token", token))
		return fmt.Errorf("%s: %w", op, err)
	}

	logger.Debug("Filesystem initialized successfully", slog.String("token", token))
	return nil
}

func (s *fileSystemService) GetRoot(ctx context.Context, token string) (*models.NodeMeta, error) {
	const op = "service.fileSystemService.GetRoot"

	logger := logging.GetLoggerFromContextWithOp(ctx, op)
	logger.Debug("GetRoot", slog.String("token", token))

	_, err := s.fsRepo.GetOrCreate(ctx, token)
	if err != nil {
		logger.Error("Failed to get or create filesystem", slogext.Err(err), slog.String("token", token))
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	logger.Debug("Getting root inode", slog.String("token", token), slog.Int64("root_ino", VTFS_ROOT_INO))
	inode, err := s.inodeRepo.Get(ctx, token, VTFS_ROOT_INO)
	if err != nil {
		logger.Error("Failed to get root inode", slogext.Err(err), slog.String("token", token))
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	if inode == nil {
		logger.Debug("Root inode not found", slog.String("token", token))
		return nil, &ServiceError{Code: kerrors.ENOENT, Message: "root inode not found"}
	}

	mode := inode.Mode
	switch inode.Type {
	case models.NodeTypeDir:
		mode = S_IFDIR | (mode & S_IRWXUGO)
	case models.NodeTypeFile:
		mode = S_IFREG | (mode & S_IRWXUGO)
	}

	meta := &models.NodeMeta{
		Ino:       inode.Ino,
		ParentIno: VTFS_ROOT_INO,
		Type:      inode.Type,
		Mode:      mode,
		Size:      inode.Size,
	}

	logger.Debug("Root retrieved successfully",
		slog.String("token", token),
		slog.Int64("ino", meta.Ino),
		slog.Int64("parent_ino", meta.ParentIno),
		slog.Int64("size", meta.Size),
	)

	return meta, nil
}

func (s *fileSystemService) Lookup(ctx context.Context, token string, parentIno int64, name string) (*models.NodeMeta, error) {
	const op = "service.fileSystemService.Lookup"

	logger := logging.GetLoggerFromContextWithOp(ctx, op)
	logger.Debug("Lookup",
		slog.String("token", token),
		slog.Int64("parent_ino", parentIno),
		slog.String("name", name),
	)

	isDir, err := s.inodeRepo.IsDir(ctx, token, parentIno)
	if err != nil {
		logger.Error("Failed to check if parent is directory", slogext.Err(err), slog.Int64("parent_ino", parentIno))
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if !isDir {
		logger.Debug("Parent is not a directory", slog.Int64("parent_ino", parentIno))
		return nil, &ServiceError{Code: kerrors.ENOTDIR, Message: "parent is not a directory"}
	}

	logger.Debug("Looking up directory entry", slog.Int64("parent_ino", parentIno), slog.String("name", name))
	ino, err := s.dirRepo.Lookup(ctx, token, parentIno, name)
	if err != nil {
		logger.Error("Failed to lookup directory entry", slogext.Err(err))
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	if ino == 0 {
		logger.Debug("Entry not found", slog.Int64("parent_ino", parentIno), slog.String("name", name))
		return nil, &ServiceError{Code: kerrors.ENOENT, Message: "file not found"}
	}

	logger.Debug("Found entry, getting inode", slog.Int64("ino", ino))
	inode, err := s.inodeRepo.Get(ctx, token, ino)
	if err != nil {
		logger.Error("Failed to get inode", slogext.Err(err), slog.Int64("ino", ino))
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	if inode == nil {
		logger.Debug("Inode not found", slog.Int64("ino", ino))
		return nil, &ServiceError{Code: kerrors.ENOENT, Message: "inode not found"}
	}

	mode := inode.Mode
	switch inode.Type {
	case models.NodeTypeDir:
		mode = S_IFDIR | (mode & S_IRWXUGO)
	case models.NodeTypeFile:
		mode = S_IFREG | (mode & S_IRWXUGO)
	}

	meta := &models.NodeMeta{
		Ino:       inode.Ino,
		ParentIno: parentIno,
		Type:      inode.Type,
		Mode:      mode,
		Size:      inode.Size,
	}

	logger.Debug("Lookup successful",
		slog.String("name", name),
		slog.Int64("ino", meta.Ino),
		slog.Int64("parent_ino", meta.ParentIno),
		slog.Int("type", int(meta.Type)),
		slog.Int64("size", meta.Size),
	)

	return meta, nil
}

func (s *fileSystemService) IterateDir(ctx context.Context, token string, dirIno int64, offset *uint64) (*models.Dirent, error) {
	const op = "service.fileSystemService.IterateDir"

	logger := logging.GetLoggerFromContextWithOp(ctx, op)
	logger.Debug("IterateDir",
		slog.String("token", token),
		slog.Int64("dir_ino", dirIno),
		slog.Uint64("offset", *offset),
	)

	isDir, err := s.inodeRepo.IsDir(ctx, token, dirIno)
	if err != nil {
		logger.Error("Failed to check if dir_ino is directory", slogext.Err(err), slog.Int64("dir_ino", dirIno))
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if !isDir {
		logger.Debug("dir_ino is not a directory", slog.Int64("dir_ino", dirIno))
		return nil, &ServiceError{Code: kerrors.ENOTDIR, Message: "not a directory"}
	}

	dirent, err := s.dirRepo.GetEntryByOffset(ctx, token, dirIno, *offset)
	if err != nil {
		logger.Error("Failed to get entry by offset", slogext.Err(err), slog.Uint64("offset", *offset))
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	if dirent == nil {
		logger.Debug("No more entries", slog.Int64("dir_ino", dirIno), slog.Uint64("offset", *offset))
		return nil, &ServiceError{Code: kerrors.ENOENT, Message: "no more entries"}
	}

	*offset++

	logger.Debug("IterateDir successful",
		slog.String("name", dirent.Name),
		slog.Int64("ino", dirent.Ino),
		slog.Int("type", int(dirent.Type)),
		slog.Uint64("next_offset", *offset),
	)

	return dirent, nil
}

func (s *fileSystemService) CreateFile(ctx context.Context, token string, parentIno int64, name string, mode uint32) (*models.NodeMeta, error) {
	const op = "service.fileSystemService.CreateFile"

	logger := logging.GetLoggerFromContextWithOp(ctx, op)
	logger.Debug("CreateFile",
		slog.String("token", token),
		slog.Int64("parent_ino", parentIno),
		slog.String("name", name),
		slog.Uint64("mode", uint64(mode)),
	)

	isDir, err := s.inodeRepo.IsDir(ctx, token, parentIno)
	if err != nil {
		logger.Error("Failed to check if parent is directory", slogext.Err(err), slog.Int64("parent_ino", parentIno))
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if !isDir {
		logger.Debug("Parent is not a directory", slog.Int64("parent_ino", parentIno))
		return nil, &ServiceError{Code: kerrors.ENOTDIR, Message: "parent is not a directory"}
	}

	exists, err := s.dirRepo.Exists(ctx, token, parentIno, name)
	if err != nil {
		logger.Error("Failed to check if file exists", slogext.Err(err))
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if exists {
		logger.Debug("File already exists", slog.String("name", name), slog.Int64("parent_ino", parentIno))
		return nil, &ServiceError{Code: kerrors.EEXIST, Message: "file already exists"}
	}

	var newIno int64
	var inode *models.Inode

	logger.Debug("Creating file in transaction", slog.String("name", name))
	err = postgresql.WithTransaction(ctx, s.db, func(ctx context.Context) error {
		nextIno, err := s.fsRepo.GetNextIno(ctx, token)
		if err != nil {
			return err
		}

		newIno = nextIno
		logger.Debug("Allocated new ino", slog.Int64("new_ino", newIno))

		inode = &models.Inode{
			Ino:      newIno,
			Token:    token,
			Type:     models.NodeTypeFile,
			Mode:     mode,
			Size:     0,
			RefCount: 1,
		}

		if err := s.inodeRepo.Create(ctx, inode); err != nil {
			return err
		}

		logger.Debug("Created inode", slog.Int64("ino", newIno))

		if err := s.dirRepo.CreateEntry(ctx, token, parentIno, name, newIno); err != nil {
			return err
		}

		logger.Debug("Created directory entry", slog.String("name", name), slog.Int64("ino", newIno))

		if err := s.fsRepo.IncrementNextIno(ctx, token); err != nil {
			return err
		}

		if err := s.contentRepo.Set(ctx, token, newIno, []byte{}); err != nil {
			return err
		}

		logger.Debug("Created empty file contents", slog.Int64("ino", newIno))

		return nil
	})

	if err != nil {
		if pqErr, ok := err.(*pq.Error); ok {
			if pqErr.Code == "23505" {
				logger.Debug("File already exists (unique violation)", slog.String("name", name))
				return nil, &ServiceError{Code: kerrors.EEXIST, Message: "file already exists"}
			}
		}
		logger.Error("Failed to create file", slogext.Err(err), slog.String("name", name))
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	modeVal := inode.Mode
	switch inode.Type {
	case models.NodeTypeDir:
		modeVal = S_IFDIR | (modeVal & S_IRWXUGO)
	case models.NodeTypeFile:
		modeVal = S_IFREG | (modeVal & S_IRWXUGO)
	}

	meta := &models.NodeMeta{
		Ino:       inode.Ino,
		ParentIno: parentIno,
		Type:      inode.Type,
		Mode:      modeVal,
		Size:      inode.Size,
	}

	logger.Debug("File created successfully",
		slog.String("name", name),
		slog.Int64("ino", meta.Ino),
		slog.Int64("parent_ino", meta.ParentIno),
	)

	return meta, nil
}

func (s *fileSystemService) Unlink(ctx context.Context, token string, parentIno int64, name string) error {
	const op = "service.fileSystemService.Unlink"

	logger := logging.GetLoggerFromContextWithOp(ctx, op)
	logger.Debug("Unlink",
		slog.String("token", token),
		slog.Int64("parent_ino", parentIno),
		slog.String("name", name),
	)

	ino, err := s.dirRepo.Lookup(ctx, token, parentIno, name)
	if err != nil {
		logger.Error("Failed to lookup entry", slogext.Err(err), slog.String("name", name))
		return fmt.Errorf("%s: %w", op, err)
	}

	if ino == 0 {
		logger.Debug("File not found", slog.String("name", name), slog.Int64("parent_ino", parentIno))
		return &ServiceError{Code: kerrors.ENOENT, Message: "file not found"}
	}

	logger.Debug("Found entry", slog.Int64("ino", ino))

	isFile, err := s.inodeRepo.IsFile(ctx, token, ino)
	if err != nil {
		logger.Error("Failed to check if ino is file", slogext.Err(err), slog.Int64("ino", ino))
		return fmt.Errorf("%s: %w", op, err)
	}
	if !isFile {
		logger.Debug("Cannot unlink directory", slog.Int64("ino", ino))
		return &ServiceError{Code: kerrors.EPERM, Message: "cannot unlink directory"}
	}

	logger.Debug("Unlinking file in transaction", slog.Int64("ino", ino))
	err = postgresql.WithTransaction(ctx, s.db, func(ctx context.Context) error {
		if err := s.dirRepo.DeleteEntry(ctx, token, parentIno, name); err != nil {
			return err
		}

		logger.Debug("Deleted directory entry", slog.String("name", name))

		if err := s.inodeRepo.UpdateRefCount(ctx, token, ino, -1); err != nil {
			return err
		}

		logger.Debug("Decremented ref_count", slog.Int64("ino", ino))

		inode, err := s.inodeRepo.Get(ctx, token, ino)
		if err != nil {
			return err
		}

		if inode != nil && inode.RefCount == 0 {
			logger.Debug("Ref count reached zero, deleting inode and contents", slog.Int64("ino", ino))
			if err := s.contentRepo.Delete(ctx, token, ino); err != nil {
				return err
			}
			if err := s.inodeRepo.Delete(ctx, token, ino); err != nil {
				return err
			}
			logger.Debug("Deleted inode and contents", slog.Int64("ino", ino))
		} else if inode != nil {
			logger.Debug("Inode still has references, keeping it", slog.Int64("ino", ino), slog.Int("ref_count", inode.RefCount))
		}

		return nil
	})

	if err != nil {
		logger.Error("Failed to unlink file", slogext.Err(err), slog.String("name", name))
		return fmt.Errorf("%s: %w", op, err)
	}

	logger.Debug("File unlinked successfully", slog.String("name", name), slog.Int64("ino", ino))
	return nil
}

func (s *fileSystemService) CreateDir(ctx context.Context, token string, parentIno int64, name string, mode uint32) (*models.NodeMeta, error) {
	const op = "service.fileSystemService.CreateDir"

	logger := logging.GetLoggerFromContextWithOp(ctx, op)
	logger.Debug("CreateDir",
		slog.String("token", token),
		slog.Int64("parent_ino", parentIno),
		slog.String("name", name),
		slog.Uint64("mode", uint64(mode)),
	)

	isDir, err := s.inodeRepo.IsDir(ctx, token, parentIno)
	if err != nil {
		logger.Error("Failed to check if parent is directory", slogext.Err(err), slog.Int64("parent_ino", parentIno))
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if !isDir {
		logger.Debug("Parent is not a directory", slog.Int64("parent_ino", parentIno))
		return nil, &ServiceError{Code: kerrors.ENOTDIR, Message: "parent is not a directory"}
	}

	exists, err := s.dirRepo.Exists(ctx, token, parentIno, name)
	if err != nil {
		logger.Error("Failed to check if directory exists", slogext.Err(err))
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if exists {
		logger.Debug("Directory already exists", slog.String("name", name), slog.Int64("parent_ino", parentIno))
		return nil, &ServiceError{Code: kerrors.EEXIST, Message: "directory already exists"}
	}

	var newIno int64
	var inode *models.Inode

	logger.Debug("Creating directory in transaction", slog.String("name", name))
	err = postgresql.WithTransaction(ctx, s.db, func(ctx context.Context) error {
		nextIno, err := s.fsRepo.GetNextIno(ctx, token)
		if err != nil {
			return err
		}

		newIno = nextIno
		logger.Debug("Allocated new ino", slog.Int64("new_ino", newIno))

		inode = &models.Inode{
			Ino:      newIno,
			Token:    token,
			Type:     models.NodeTypeDir,
			Mode:     mode,
			Size:     0,
			RefCount: 1,
		}

		if err := s.inodeRepo.Create(ctx, inode); err != nil {
			return err
		}

		logger.Debug("Created inode", slog.Int64("ino", newIno))

		if err := s.dirRepo.CreateEntry(ctx, token, parentIno, name, newIno); err != nil {
			return err
		}

		logger.Debug("Created directory entry", slog.String("name", name), slog.Int64("ino", newIno))

		if err := s.fsRepo.IncrementNextIno(ctx, token); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		if pqErr, ok := err.(*pq.Error); ok {
			if pqErr.Code == "23505" {
				logger.Debug("Directory already exists (unique violation)", slog.String("name", name))
				return nil, &ServiceError{Code: kerrors.EEXIST, Message: "directory already exists"}
			}
		}
		logger.Error("Failed to create directory", slogext.Err(err), slog.String("name", name))
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	modeVal := inode.Mode
	switch inode.Type {
	case models.NodeTypeDir:
		modeVal = S_IFDIR | (modeVal & S_IRWXUGO)
	case models.NodeTypeFile:
		modeVal = S_IFREG | (modeVal & S_IRWXUGO)
	}

	meta := &models.NodeMeta{
		Ino:       inode.Ino,
		ParentIno: parentIno,
		Type:      inode.Type,
		Mode:      modeVal,
		Size:      inode.Size,
	}

	logger.Debug("Directory created successfully",
		slog.String("name", name),
		slog.Int64("ino", meta.Ino),
		slog.Int64("parent_ino", meta.ParentIno),
	)

	return meta, nil
}

func (s *fileSystemService) Rmdir(ctx context.Context, token string, parentIno int64, name string) error {
	const op = "service.fileSystemService.Rmdir"

	logger := logging.GetLoggerFromContextWithOp(ctx, op)
	logger.Debug("Rmdir",
		slog.String("token", token),
		slog.Int64("parent_ino", parentIno),
		slog.String("name", name),
	)

	ino, err := s.dirRepo.Lookup(ctx, token, parentIno, name)
	if err != nil {
		logger.Error("Failed to lookup entry", slogext.Err(err), slog.String("name", name))
		return fmt.Errorf("%s: %w", op, err)
	}

	if ino == 0 {
		logger.Debug("Directory not found", slog.String("name", name), slog.Int64("parent_ino", parentIno))
		return &ServiceError{Code: kerrors.ENOENT, Message: "directory not found"}
	}

	logger.Debug("Found entry", slog.Int64("ino", ino))

	isDir, err := s.inodeRepo.IsDir(ctx, token, ino)
	if err != nil {
		logger.Error("Failed to check if ino is directory", slogext.Err(err), slog.Int64("ino", ino))
		return fmt.Errorf("%s: %w", op, err)
	}
	if !isDir {
		logger.Debug("Not a directory", slog.Int64("ino", ino))
		return &ServiceError{Code: kerrors.ENOTDIR, Message: "not a directory"}
	}

	if ino == VTFS_ROOT_INO {
		logger.Debug("Attempt to remove root directory", slog.Int64("ino", ino))
		return &ServiceError{Code: kerrors.EPERM, Message: "cannot remove root directory"}
	}

	isEmpty, err := s.dirRepo.IsEmpty(ctx, token, ino)
	if err != nil {
		logger.Error("Failed to check if directory is empty", slogext.Err(err), slog.Int64("ino", ino))
		return fmt.Errorf("%s: %w", op, err)
	}
	if !isEmpty {
		logger.Debug("Directory not empty", slog.Int64("ino", ino))
		return &ServiceError{Code: kerrors.ENOTEMPTY, Message: "directory not empty"}
	}

	logger.Debug("Removing directory in transaction", slog.Int64("ino", ino))
	err = postgresql.WithTransaction(ctx, s.db, func(ctx context.Context) error {
		if err := s.dirRepo.DeleteEntry(ctx, token, parentIno, name); err != nil {
			return err
		}

		logger.Debug("Deleted directory entry", slog.String("name", name))

		if err := s.inodeRepo.Delete(ctx, token, ino); err != nil {
			return err
		}

		logger.Debug("Deleted inode", slog.Int64("ino", ino))

		return nil
	})

	if err != nil {
		logger.Error("Failed to remove directory", slogext.Err(err), slog.String("name", name))
		return fmt.Errorf("%s: %w", op, err)
	}

	logger.Debug("Directory removed successfully", slog.String("name", name), slog.Int64("ino", ino))
	return nil
}

func (s *fileSystemService) Read(ctx context.Context, token string, ino int64, buffer []byte, offset int64) (int64, error) {
	const op = "service.fileSystemService.Read"

	logger := logging.GetLoggerFromContextWithOp(ctx, op)
	logger.Debug("Read",
		slog.String("token", token),
		slog.Int64("ino", ino),
		slog.Int64("offset", offset),
		slog.Int("buffer_len", len(buffer)),
	)

	inode, err := s.inodeRepo.Get(ctx, token, ino)
	if err != nil {
		logger.Error("Failed to get inode", slogext.Err(err), slog.Int64("ino", ino))
		return 0, fmt.Errorf("%s: %w", op, err)
	}

	if inode == nil {
		logger.Debug("File not found", slog.Int64("ino", ino))
		return 0, &ServiceError{Code: kerrors.ENOENT, Message: "file not found"}
	}

	if inode.Type != models.NodeTypeFile {
		logger.Debug("Is a directory, not a file", slog.Int64("ino", ino))
		return 0, &ServiceError{Code: kerrors.EISDIR, Message: "is a directory"}
	}

	if offset < 0 {
		logger.Debug("Invalid offset", slog.Int64("offset", offset))
		return 0, &ServiceError{Code: kerrors.EINVAL, Message: "invalid offset"}
	}

	available := inode.Size - offset
	if available <= 0 {
		logger.Debug("EOF reached", slog.Int64("file_size", inode.Size), slog.Int64("offset", offset))
		return 0, nil // EOF
	}

	toRead := int64(len(buffer))
	if toRead > available {
		toRead = available
	}

	logger.Debug("Reading from file",
		slog.Int64("file_size", inode.Size),
		slog.Int64("available", available),
		slog.Int64("to_read", toRead),
	)

	data, err := s.contentRepo.GetRange(ctx, token, ino, offset, toRead)
	if err != nil {
		logger.Error("Failed to read file content", slogext.Err(err), slog.Int64("ino", ino))
		return 0, fmt.Errorf("%s: %w", op, err)
	}

	copy(buffer, data)

	logger.Debug("Read successful",
		slog.Int64("ino", ino),
		slog.Int("bytes_read", len(data)),
		slog.Int64("offset", offset),
	)

	return int64(len(data)), nil
}

func (s *fileSystemService) Write(ctx context.Context, token string, ino int64, data []byte, length uint64, offset int64) (int64, error) {
	const op = "service.fileSystemService.Write"

	logger := logging.GetLoggerFromContextWithOp(ctx, op)
	logger.Debug("Write",
		slog.String("token", token),
		slog.Int64("ino", ino),
		slog.Int64("offset", offset),
		slog.Uint64("length", length),
		slog.Int("data_buffer_len", len(data)),
	)

	if length > uint64(len(data)) {
		logger.Debug("Length exceeds buffer size",
			slog.Uint64("length", length),
			slog.Int("buffer_size", len(data)))
		return 0, &ServiceError{Code: kerrors.EINVAL, Message: "length exceeds buffer size"}
	}

	if offset < 0 {
		logger.Debug("Invalid offset", slog.Int64("offset", offset))
		return 0, &ServiceError{Code: kerrors.EINVAL, Message: "invalid offset"}
	}

	writeData := data[:length]
	logger.Debug("Using data slice",
		slog.Uint64("length", length),
		slog.Int("slice_len", len(writeData)))

	inode, err := s.inodeRepo.Get(ctx, token, ino)
	if err != nil {
		logger.Error("Failed to get inode", slogext.Err(err), slog.Int64("ino", ino))
		return 0, fmt.Errorf("%s: %w", op, err)
	}

	if inode == nil {
		logger.Debug("File not found", slog.Int64("ino", ino))
		return 0, &ServiceError{Code: kerrors.ENOENT, Message: "file not found"}
	}

	if inode.Type != models.NodeTypeFile {
		logger.Debug("Is a directory, not a file", slog.Int64("ino", ino))
		return 0, &ServiceError{Code: kerrors.EISDIR, Message: "is a directory"}
	}

	logger.Debug("Writing to file in transaction",
		slog.Int64("current_size", inode.Size),
		slog.Int64("new_size", offset+int64(length)),
		slog.Uint64("bytes_to_write", length),
	)
	err = postgresql.WithTransaction(ctx, s.db, func(ctx context.Context) error {
		currentData, err := s.contentRepo.Get(ctx, token, ino)
		if err != nil {
			return err
		}

		oldSize := int64(len(currentData))

		newSize := offset + int64(length)
		if newSize > int64(len(currentData)) {
			extended := make([]byte, newSize)
			copy(extended, currentData)
			currentData = extended
			logger.Debug("Extended file size", slog.Int64("old_size", oldSize), slog.Int64("new_size", newSize))
		}

		copy(currentData[offset:], writeData)

		if err := s.contentRepo.Set(ctx, token, ino, currentData); err != nil {
			return err
		}

		logger.Debug("Saved file content", slog.Int64("new_size", int64(len(currentData))))

		if err := s.inodeRepo.UpdateSize(ctx, token, ino, int64(len(currentData))); err != nil {
			return err
		}

		logger.Debug("Updated file size in inode", slog.Int64("new_size", int64(len(currentData))))

		return nil
	})

	if err != nil {
		logger.Error("Failed to write file", slogext.Err(err), slog.Int64("ino", ino))
		return 0, fmt.Errorf("%s: %w", op, err)
	}

	logger.Debug("Write successful",
		slog.Int64("ino", ino),
		slog.Uint64("bytes_written", length),
		slog.Int64("offset", offset),
	)

	return int64(length), nil
}

func (s *fileSystemService) Link(ctx context.Context, token string, targetIno int64, parentIno int64, name string) error {
	const op = "service.fileSystemService.Link"

	logger := logging.GetLoggerFromContextWithOp(ctx, op)
	logger.Debug("Link",
		slog.String("token", token),
		slog.Int64("target_ino", targetIno),
		slog.Int64("parent_ino", parentIno),
		slog.String("name", name),
	)

	targetInode, err := s.inodeRepo.Get(ctx, token, targetIno)
	if err != nil {
		logger.Error("Failed to get target inode", slogext.Err(err), slog.Int64("target_ino", targetIno))
		return fmt.Errorf("%s: %w", op, err)
	}

	if targetInode == nil {
		logger.Debug("Target not found", slog.Int64("target_ino", targetIno))
		return &ServiceError{Code: kerrors.ENOENT, Message: "target not found"}
	}

	if targetInode.Type != models.NodeTypeFile {
		logger.Debug("Cannot link directory", slog.Int64("target_ino", targetIno))
		return &ServiceError{Code: kerrors.EISDIR, Message: "cannot link directory"}
	}

	logger.Debug("Target found", slog.Int64("target_ino", targetIno), slog.Int("current_ref_count", targetInode.RefCount))

	exists, err := s.dirRepo.Exists(ctx, token, parentIno, name)
	if err != nil {
		logger.Error("Failed to check if name exists", slogext.Err(err))
		return fmt.Errorf("%s: %w", op, err)
	}
	if exists {
		logger.Debug("Name already exists", slog.String("name", name), slog.Int64("parent_ino", parentIno))
		return &ServiceError{Code: kerrors.EEXIST, Message: "name already exists"}
	}

	logger.Debug("Creating hard link in transaction", slog.Int64("target_ino", targetIno))
	err = postgresql.WithTransaction(ctx, s.db, func(ctx context.Context) error {
		if err := s.dirRepo.CreateEntry(ctx, token, parentIno, name, targetIno); err != nil {
			return err
		}

		logger.Debug("Created directory entry for link", slog.String("name", name), slog.Int64("target_ino", targetIno))

		if err := s.inodeRepo.UpdateRefCount(ctx, token, targetIno, 1); err != nil {
			return err
		}

		logger.Debug("Incremented ref_count", slog.Int64("target_ino", targetIno), slog.Int("new_ref_count", targetInode.RefCount+1))

		return nil
	})

	if err != nil {
		if pqErr, ok := err.(*pq.Error); ok {
			if pqErr.Code == "23505" { // unique_violation
				logger.Debug("Name already exists (unique violation)", slog.String("name", name))
				return &ServiceError{Code: kerrors.EEXIST, Message: "name already exists"}
			}
		}
		logger.Error("Failed to create hard link", slogext.Err(err), slog.String("name", name))
		return fmt.Errorf("%s: %w", op, err)
	}

	logger.Debug("Hard link created successfully",
		slog.String("name", name),
		slog.Int64("target_ino", targetIno),
		slog.Int64("parent_ino", parentIno),
	)

	return nil
}

func (s *fileSystemService) CountLinks(ctx context.Context, token string, ino int64) (uint32, error) {
	const op = "service.fileSystemService.CountLinks"

	logger := logging.GetLoggerFromContextWithOp(ctx, op)
	logger.Debug("CountLinks",
		slog.String("token", token),
		slog.Int64("ino", ino),
	)

	inode, err := s.inodeRepo.Get(ctx, token, ino)
	if err != nil {
		logger.Error("Failed to get inode", slogext.Err(err), slog.Int64("ino", ino))
		return 0, fmt.Errorf("%s: %w", op, err)
	}

	if inode == nil {
		logger.Debug("Inode not found", slog.Int64("ino", ino))
		return 0, &ServiceError{Code: kerrors.ENOENT, Message: "inode not found"}
	}

	count := uint32(inode.RefCount)
	logger.Debug("CountLinks successful",
		slog.Int64("ino", ino),
		slog.Uint64("ref_count", uint64(count)),
	)

	return count, nil
}

type ServiceError struct {
	Code    int64
	Message string
}

func (e *ServiceError) Error() string {
	return e.Message
}

func (e *ServiceError) GetCode() int64 {
	return e.Code
}
