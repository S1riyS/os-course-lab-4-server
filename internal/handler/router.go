package handler

import (
	"net/http"
)

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	// System endpoints
	mux.HandleFunc("/health", h.HandleHealthCheck)

	// API endpoints
	mux.HandleFunc("/api/init", h.HandleInit)
	mux.HandleFunc("/api/get_root", h.HandleGetRoot)
	mux.HandleFunc("/api/lookup", h.HandleLookup)
	mux.HandleFunc("/api/iterate_dir", h.HandleIterateDir)
	mux.HandleFunc("/api/create_file", h.HandleCreateFile)
	mux.HandleFunc("/api/unlink", h.HandleUnlink)
	mux.HandleFunc("/api/mkdir", h.HandleMkdir)
	mux.HandleFunc("/api/rmdir", h.HandleRmdir)
	mux.HandleFunc("/api/read", h.HandleRead)
	mux.HandleFunc("/api/write", h.HandleWrite)
	mux.HandleFunc("/api/link", h.HandleLink)
	mux.HandleFunc("/api/count_links", h.HandleCountLinks)
}
