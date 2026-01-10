package models

import "time"

type NodeType int16

const (
	NodeTypeDir  NodeType = 0 // VTFS_NODE_DIR
	NodeTypeFile NodeType = 1 // VTFS_NODE_FILE
)

type NodeMeta struct {
	Ino       int64    `json:"ino"`
	ParentIno int64    `json:"parent_ino"`
	Type      NodeType `json:"type"`
	Mode      uint32   `json:"mode"` // umode_t
	Size      int64    `json:"size"`
}

type Dirent struct {
	Name string   `json:"name"`
	Ino  int64    `json:"ino"`
	Type NodeType `json:"type"`
}

type Inode struct {
	Ino      int64
	Token    string
	Type     NodeType
	Mode     uint32
	Size     int64
	RefCount int
}

type Filesystem struct {
	Token    string
	RootIno  int64
	NextIno  int64
	CreateAt time.Time
}
