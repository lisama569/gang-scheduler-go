package scheduler

import (
	"log"
	"os"
)

// Logger 调度器使用的最小日志接口，便于测试替换与捕获关键决策。
type Logger interface {
	Printf(format string, args ...any)
}

// defaultLogger 输出到 stderr 的标准日志实现。
type defaultLogger struct {
	l *log.Logger
}

func newDefaultLogger() Logger {
	return &defaultLogger{l: log.New(os.Stderr, "[scheduler] ", log.LstdFlags|log.Lmicroseconds)}
}

func (d *defaultLogger) Printf(format string, args ...any) {
	d.l.Printf(format, args...)
}

// nopLogger 丢弃所有日志。
type nopLogger struct{}

func (nopLogger) Printf(string, ...any) {}

// sortStrings 单独包一层，避免 types.go 再引入 sort 造成关注点分散。
func sortStrings(x []string) {
	for i := 1; i < len(x); i++ {
		for j := i; j > 0 && x[j-1] > x[j]; j-- {
			x[j-1], x[j] = x[j], x[j-1]
		}
	}
}
