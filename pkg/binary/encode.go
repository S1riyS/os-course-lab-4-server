package binary

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net/http"

	"github.com/S1riyS/os-course-lab-4/server/internal/models"
)

func EncodeNodeMeta(meta *models.NodeMeta) ([]byte, error) {
	buf := new(bytes.Buffer)

	// ino (int64, 8 bytes)
	if err := binary.Write(buf, binary.LittleEndian, meta.Ino); err != nil {
		return nil, fmt.Errorf("failed to encode ino: %w", err)
	}

	// parent_ino (int64, 8 bytes)
	if err := binary.Write(buf, binary.LittleEndian, meta.ParentIno); err != nil {
		return nil, fmt.Errorf("failed to encode parent_ino: %w", err)
	}

	// type (int16, 2 bytesа)
	if err := binary.Write(buf, binary.LittleEndian, int16(meta.Type)); err != nil {
		return nil, fmt.Errorf("failed to encode type: %w", err)
	}

	// mode (uint32, 4 bytesа)
	if err := binary.Write(buf, binary.LittleEndian, meta.Mode); err != nil {
		return nil, fmt.Errorf("failed to encode mode: %w", err)
	}

	// size (int64, 8 bytes)
	if err := binary.Write(buf, binary.LittleEndian, meta.Size); err != nil {
		return nil, fmt.Errorf("failed to encode size: %w", err)
	}

	return buf.Bytes(), nil
}

func EncodeDirent(dirent *models.Dirent) ([]byte, error) {
	buf := new(bytes.Buffer)

	// name (char[256], null-terminated, padded with zeros)
	nameBytes := make([]byte, 256)
	copy(nameBytes, dirent.Name)
	if _, err := buf.Write(nameBytes); err != nil {
		return nil, fmt.Errorf("failed to encode name: %w", err)
	}

	// ino (int64, 8 bytes)
	if err := binary.Write(buf, binary.LittleEndian, dirent.Ino); err != nil {
		return nil, fmt.Errorf("failed to encode ino: %w", err)
	}

	// type (int16, 2 bytesа)
	if err := binary.Write(buf, binary.LittleEndian, int16(dirent.Type)); err != nil {
		return nil, fmt.Errorf("failed to encode type: %w", err)
	}

	return buf.Bytes(), nil
}

func WriteResponse(w http.ResponseWriter, code int64, data []byte) error {
	response := new(bytes.Buffer)

	// Код возврата (int64, 8 bytes)
	if err := binary.Write(response, binary.LittleEndian, code); err != nil {
		return fmt.Errorf("failed to write response code: %w", err)
	}

	// Данные (если есть)
	if data != nil {
		if _, err := response.Write(data); err != nil {
			return fmt.Errorf("failed to write response data: %w", err)
		}
	}

	body := response.Bytes()

	// Set headers
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
	w.Header().Set("Connection", "close")
	w.WriteHeader(http.StatusOK)

	_, err := w.Write(body)
	return err
}

func WriteUint32Response(w http.ResponseWriter, code int64, value uint32) error {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, value); err != nil {
		return err
	}
	return WriteResponse(w, code, buf.Bytes())
}

func WriteInt64Response(w http.ResponseWriter, code int64, value int64) error {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, value); err != nil {
		return err
	}
	return WriteResponse(w, code, buf.Bytes())
}
