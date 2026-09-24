package scheduler

import (
	"log/slog"
	"strings"
	"sync"
	"testing"
)

// testWriter 把 slog 输出桥接到 t.Log，使关键调度决策与资源变更依据
// 出现在 `go test -v` 日志中。
type testWriter struct {
	mu sync.Mutex
	t  *testing.T
}

func (w *testWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

// testLogger 构造写入测试日志的结构化 logger。
func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	w := &testWriter{t: t}
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelInfo}))
}
