package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// Store 调度状态的持久化抽象。保存必须是原子的：
// 先写同目录临时文件再 rename，保证进程崩溃后不会读到半个快照。
type Store interface {
	Save(ctx context.Context, snap *snapshot) error
	Load(ctx context.Context) (*snapshot, bool, error)
}

// FileStore 基于本地 JSON 文件的持久化实现。
type FileStore struct {
	path string
}

// NewFileStore 创建一个写入 path 的文件存储。
func NewFileStore(path string) *FileStore {
	return &FileStore{path: path}
}

func (s *FileStore) Save(ctx context.Context, snap *snapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create state dir: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".snap-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp snapshot: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("write snapshot: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("fsync snapshot: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		return fmt.Errorf("commit snapshot: %w", err)
	}
	return nil
}

func (s *FileStore) Load(ctx context.Context) (*snapshot, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("read snapshot: %w", err)
	}
	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, false, fmt.Errorf("parse snapshot: %w", err)
	}
	return &snap, true, nil
}

// MemoryStore 仅用于测试的内存存储。
type MemoryStore struct {
	snap *snapshot
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{}
}

func (s *MemoryStore) Save(ctx context.Context, snap *snapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// 深拷贝一次，模拟落盘后的隔离性。
	data, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	var cp snapshot
	if err := json.Unmarshal(data, &cp); err != nil {
		return err
	}
	s.snap = &cp
	return nil
}

func (s *MemoryStore) Load(ctx context.Context) (*snapshot, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if s.snap == nil {
		return nil, false, nil
	}
	data, err := json.Marshal(s.snap)
	if err != nil {
		return nil, false, err
	}
	var cp snapshot
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, false, err
	}
	return &cp, true, nil
}
