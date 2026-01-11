package handler

import (
	"encoding/base64"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/S1riyS/os-course-lab-4/server/internal/pkg/kerrors"
	"github.com/S1riyS/os-course-lab-4/server/internal/service"
	"github.com/S1riyS/os-course-lab-4/server/pkg/binary"
	"github.com/S1riyS/os-course-lab-4/server/pkg/logging"
	"github.com/S1riyS/os-course-lab-4/server/pkg/logging/slogext"
)

type Handler struct {
	service service.FileSystemService
}

func NewHandler(service service.FileSystemService) *Handler {
	return &Handler{service: service}
}

func (h *Handler) HandleInit(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	token := r.URL.Query().Get("token")
	if token == "" {
		binary.WriteResponse(w, kerrors.EINVAL_NEG, nil)
		return
	}

	err := h.service.Init(ctx, token)
	if err != nil {
		code := mapErrorToCode(err)
		binary.WriteResponse(w, code, nil)
		return
	}

	binary.WriteResponse(w, 0, nil)
}

func (h *Handler) HandleGetRoot(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	const op = "handler.HandleGetRoot"

	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	token := r.URL.Query().Get("token")
	if token == "" {
		binary.WriteResponse(w, kerrors.EINVAL_NEG, nil)
		return
	}

	meta, err := h.service.GetRoot(ctx, token)
	if err != nil {
		code := mapErrorToCode(err)
		binary.WriteResponse(w, code, nil)
		return
	}

	data, err := binary.EncodeNodeMeta(meta)
	if err != nil {
		binary.WriteResponse(w, kerrors.ENOMEM_NEG, nil)
		return
	}

	binary.WriteResponse(w, 0, data)
}

func (h *Handler) HandleLookup(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	const op = "handler.HandleLookup"

	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	token := r.URL.Query().Get("token")
	parentStr := r.URL.Query().Get("parent")
	name := r.URL.Query().Get("name")

	if token == "" || parentStr == "" || name == "" {
		binary.WriteResponse(w, kerrors.EINVAL_NEG, nil)
		return
	}

	parent, err := strconv.ParseInt(parentStr, 10, 64)
	if err != nil {
		binary.WriteResponse(w, kerrors.EINVAL_NEG, nil)
		return
	}

	meta, err := h.service.Lookup(ctx, token, parent, name)
	if err != nil {
		code := mapErrorToCode(err)
		binary.WriteResponse(w, code, nil)
		return
	}

	data, err := binary.EncodeNodeMeta(meta)
	if err != nil {
		binary.WriteResponse(w, kerrors.ENOMEM_NEG, nil)
		return
	}

	binary.WriteResponse(w, 0, data)
}

func (h *Handler) HandleIterateDir(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	const op = "handler.HandleIterateDir"

	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	token := r.URL.Query().Get("token")
	dirInoStr := r.URL.Query().Get("dir_ino")
	offsetStr := r.URL.Query().Get("offset")

	if token == "" || dirInoStr == "" || offsetStr == "" {
		binary.WriteResponse(w, kerrors.EINVAL_NEG, nil)
		return
	}

	dirIno, err := strconv.ParseInt(dirInoStr, 10, 64)
	if err != nil {
		binary.WriteResponse(w, kerrors.EINVAL_NEG, nil)
		return
	}

	offset, err := strconv.ParseUint(offsetStr, 10, 64)
	if err != nil {
		binary.WriteResponse(w, kerrors.EINVAL_NEG, nil)
		return
	}

	dirent, err := h.service.IterateDir(ctx, token, dirIno, &offset)
	if err != nil {
		code := mapErrorToCode(err)
		binary.WriteResponse(w, code, nil)
		return
	}

	data, err := binary.EncodeDirent(dirent)
	if err != nil {
		binary.WriteResponse(w, kerrors.ENOMEM_NEG, nil)
		return
	}

	binary.WriteResponse(w, 0, data)
}

func (h *Handler) HandleCreateFile(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	const op = "handler.HandleCreateFile"

	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	token := r.URL.Query().Get("token")
	parentStr := r.URL.Query().Get("parent")
	name := r.URL.Query().Get("name")
	modeStr := r.URL.Query().Get("mode")

	if token == "" || parentStr == "" || name == "" || modeStr == "" {
		binary.WriteResponse(w, kerrors.EINVAL_NEG, nil)
		return
	}

	parent, err := strconv.ParseInt(parentStr, 10, 64)
	if err != nil {
		binary.WriteResponse(w, kerrors.EINVAL_NEG, nil)
		return
	}

	mode, err := strconv.ParseUint(modeStr, 10, 32)
	if err != nil {
		binary.WriteResponse(w, kerrors.EINVAL_NEG, nil)
		return
	}

	meta, err := h.service.CreateFile(ctx, token, parent, name, uint32(mode))
	if err != nil {
		code := mapErrorToCode(err)
		binary.WriteResponse(w, code, nil)
		return
	}

	data, err := binary.EncodeNodeMeta(meta)
	if err != nil {
		binary.WriteResponse(w, kerrors.ENOMEM_NEG, nil)
		return
	}

	binary.WriteResponse(w, 0, data)
}

func (h *Handler) HandleUnlink(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	const op = "handler.HandleUnlink"

	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	token := r.URL.Query().Get("token")
	parentStr := r.URL.Query().Get("parent")
	name := r.URL.Query().Get("name")

	if token == "" || parentStr == "" || name == "" {
		binary.WriteResponse(w, kerrors.EINVAL_NEG, nil)
		return
	}

	parent, err := strconv.ParseInt(parentStr, 10, 64)
	if err != nil {
		binary.WriteResponse(w, kerrors.EINVAL_NEG, nil)
		return
	}

	err = h.service.Unlink(ctx, token, parent, name)
	if err != nil {
		code := mapErrorToCode(err)
		binary.WriteResponse(w, code, nil)
		return
	}

	binary.WriteResponse(w, 0, nil)
}

func (h *Handler) HandleMkdir(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	const op = "handler.HandleMkdir"

	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	token := r.URL.Query().Get("token")
	parentStr := r.URL.Query().Get("parent")
	name := r.URL.Query().Get("name")
	modeStr := r.URL.Query().Get("mode")

	if token == "" || parentStr == "" || name == "" || modeStr == "" {
		binary.WriteResponse(w, kerrors.EINVAL_NEG, nil)
		return
	}

	parent, err := strconv.ParseInt(parentStr, 10, 64)
	if err != nil {
		binary.WriteResponse(w, kerrors.EINVAL_NEG, nil)
		return
	}

	mode, err := strconv.ParseUint(modeStr, 10, 32)
	if err != nil {
		binary.WriteResponse(w, kerrors.EINVAL_NEG, nil)
		return
	}

	meta, err := h.service.CreateDir(ctx, token, parent, name, uint32(mode))
	if err != nil {
		code := mapErrorToCode(err)
		binary.WriteResponse(w, code, nil)
		return
	}

	data, err := binary.EncodeNodeMeta(meta)
	if err != nil {
		binary.WriteResponse(w, kerrors.ENOMEM_NEG, nil)
		return
	}

	binary.WriteResponse(w, 0, data)
}

func (h *Handler) HandleRmdir(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	const op = "handler.HandleRmdir"

	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	token := r.URL.Query().Get("token")
	parentStr := r.URL.Query().Get("parent")
	name := r.URL.Query().Get("name")

	if token == "" || parentStr == "" || name == "" {
		binary.WriteResponse(w, kerrors.EINVAL_NEG, nil)
		return
	}

	parent, err := strconv.ParseInt(parentStr, 10, 64)
	if err != nil {
		binary.WriteResponse(w, kerrors.EINVAL_NEG, nil)
		return
	}

	err = h.service.Rmdir(ctx, token, parent, name)
	if err != nil {
		code := mapErrorToCode(err)
		binary.WriteResponse(w, code, nil)
		return
	}

	binary.WriteResponse(w, 0, nil)
}

func (h *Handler) HandleRead(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	const op = "handler.HandleRead"

	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	token := r.URL.Query().Get("token")
	inoStr := r.URL.Query().Get("ino")
	lenStr := r.URL.Query().Get("len")
	offsetStr := r.URL.Query().Get("offset")

	if token == "" || inoStr == "" || lenStr == "" || offsetStr == "" {
		binary.WriteResponse(w, kerrors.EINVAL_NEG, nil)
		return
	}

	ino, err := strconv.ParseInt(inoStr, 10, 64)
	if err != nil {
		binary.WriteResponse(w, kerrors.EINVAL_NEG, nil)
		return
	}

	length, err := strconv.ParseUint(lenStr, 10, 64)
	if err != nil {
		binary.WriteResponse(w, kerrors.EINVAL_NEG, nil)
		return
	}

	offset, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil {
		binary.WriteResponse(w, kerrors.EINVAL_NEG, nil)
		return
	}

	buffer := make([]byte, length)
	read, err := h.service.Read(ctx, token, ino, buffer, offset)
	if err != nil {
		code := mapErrorToCode(err)
		binary.WriteResponse(w, code, nil)
		return
	}

	// Возвращаем только прочитанные байты
	binary.WriteResponse(w, 0, buffer[:read])
}

func (h *Handler) HandleWrite(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	const op = "handler.HandleWrite"

	logger := logging.GetLoggerFromContextWithOp(ctx, op)
	logger.Info("Write request received",
		slog.String("method", r.Method),
		slog.String("path", r.URL.Path),
		slog.String("query", r.URL.RawQuery),
		slog.String("remote_addr", r.RemoteAddr))

	if r.Method != http.MethodGet {
		logger.Warn("Method not allowed", slog.String("method", r.Method))
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	token := r.URL.Query().Get("token")
	inoStr := r.URL.Query().Get("ino")
	lenStr := r.URL.Query().Get("len")
	offsetStr := r.URL.Query().Get("offset")
	dataBase64 := r.URL.Query().Get("data")

	logger.Debug("Parsed query parameters",
		slog.String("token", token),
		slog.String("ino", inoStr),
		slog.String("len", lenStr),
		slog.String("offset", offsetStr),
		slog.Bool("has_data", dataBase64 != ""),
		slog.Int("data_base64_len", len(dataBase64)))

	if token == "" || inoStr == "" || lenStr == "" || offsetStr == "" || dataBase64 == "" {
		logger.Warn("Missing required parameters",
			slog.Bool("has_token", token != ""),
			slog.Bool("has_ino", inoStr != ""),
			slog.Bool("has_len", lenStr != ""),
			slog.Bool("has_offset", offsetStr != ""),
			slog.Bool("has_data", dataBase64 != ""))
		binary.WriteResponse(w, kerrors.EINVAL_NEG, nil)
		return
	}

	ino, err := strconv.ParseInt(inoStr, 10, 64)
	if err != nil {
		binary.WriteResponse(w, kerrors.EINVAL_NEG, nil)
		return
	}

	length, err := strconv.ParseUint(lenStr, 10, 64)
	if err != nil {
		binary.WriteResponse(w, kerrors.EINVAL_NEG, nil)
		return
	}

	offset, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil {
		binary.WriteResponse(w, kerrors.EINVAL_NEG, nil)
		return
	}

	data, err := base64.StdEncoding.DecodeString(dataBase64)
	if err != nil {
		logger.Warn("Failed to decode base64 data", slogext.Err(err))
		binary.WriteResponse(w, kerrors.EINVAL_NEG, nil)
		return
	}

	// Проверка, что длина буфера достаточна для запроса
	if uint64(len(data)) < length {
		logger.Warn("Buffer size is less than requested length",
			slog.Uint64("requested_length", length),
			slog.Int("buffer_size", len(data)))
		binary.WriteResponse(w, kerrors.EINVAL_NEG, nil)
		return
	}

	logger.Debug("Calling service.Write",
		slog.Int64("ino", ino),
		slog.Uint64("length", length),
		slog.Int64("offset", offset),
		slog.Int("data_buffer_size", len(data)))

	written, err := h.service.Write(ctx, token, ino, data, length, offset)
	if err != nil {
		logger.Error("Service.Write failed", slogext.Err(err),
			slog.Int64("ino", ino),
			slog.Int64("offset", offset),
			slog.Uint64("length", length))
		code := mapErrorToCode(err)
		logger.Debug("Returning error code", slog.Int64("error_code", code))
		binary.WriteResponse(w, code, nil)
		return
	}

	logger.Info("Write successful",
		slog.Int64("ino", ino),
		slog.Int64("bytes_written", written),
		slog.Int64("offset", offset),
		slog.Uint64("requested_length", length))
	// Возвращаем количество записанных байт
	binary.WriteInt64Response(w, 0, written)
}

func (h *Handler) HandleLink(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	const op = "handler.HandleLink"

	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	token := r.URL.Query().Get("token")
	targetInoStr := r.URL.Query().Get("target_ino")
	parentStr := r.URL.Query().Get("parent")
	name := r.URL.Query().Get("name")

	if token == "" || targetInoStr == "" || parentStr == "" || name == "" {
		binary.WriteResponse(w, kerrors.EINVAL_NEG, nil)
		return
	}

	targetIno, err := strconv.ParseInt(targetInoStr, 10, 64)
	if err != nil {
		binary.WriteResponse(w, kerrors.EINVAL_NEG, nil)
		return
	}

	parent, err := strconv.ParseInt(parentStr, 10, 64)
	if err != nil {
		binary.WriteResponse(w, kerrors.EINVAL_NEG, nil)
		return
	}

	err = h.service.Link(ctx, token, targetIno, parent, name)
	if err != nil {
		code := mapErrorToCode(err)
		binary.WriteResponse(w, code, nil)
		return
	}

	binary.WriteResponse(w, 0, nil)
}

func (h *Handler) HandleCountLinks(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	const op = "handler.HandleCountLinks"

	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	token := r.URL.Query().Get("token")
	inoStr := r.URL.Query().Get("ino")

	if token == "" || inoStr == "" {
		binary.WriteResponse(w, kerrors.EINVAL_NEG, nil)
		return
	}

	ino, err := strconv.ParseInt(inoStr, 10, 64)
	if err != nil {
		binary.WriteResponse(w, kerrors.EINVAL_NEG, nil)
		return
	}

	count, err := h.service.CountLinks(ctx, token, ino)
	if err != nil {
		code := mapErrorToCode(err)
		binary.WriteResponse(w, code, nil)
		return
	}

	binary.WriteUint32Response(w, 0, count)
}

func (h *Handler) HandleHealthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	// fmt.Fprintf(w, `{"status":"ok","service":"vtfs-server"}`) // TODO: remove
	response := `{"status":"ok","service":"vtfs-server"}`
	w.Write([]byte(response))
}

func mapErrorToCode(err error) int64 {
	if serviceErr, ok := err.(*service.ServiceError); ok {
		return serviceErr.Code
	}
	// По умолчанию возвращаем ENOMEM
	return kerrors.ENOMEM_NEG
}
