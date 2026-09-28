package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/lisama569/gang-scheduler-go/pkg/api"
)

// Store 是基于原子快照替换 + 文件持久化的作业状态存储。
//
// 注意：Store 自身不加锁。它设计为由上层（scheduler）的单把互斥锁统一
// 串行化所有访问。这样“检查-写入”（如 Submit 的查重+Add）与 Recover 的
// 整表原子替换之间由同一把锁保护，避免并发恢复用旧快照覆盖刚刚提交的作业，
// 也避免恢复与调度同时发生时资源重复占用或作业丢失。
type Store struct {
	path    string
	counter int // 已持久化版本号，每次写盘 +1
	jobs    map[string]*api.JobRuntime
	// draining 记录处于维护/排空中的节点 ID，随快照一起落盘，
	// 保证重启恢复后排空中的节点不会被当成健康机器重新投入调度。
	draining map[string]bool
}

func New(path string) *Store {
	return &Store{path: path, jobs: map[string]*api.JobRuntime{}, draining: map[string]bool{}}
}

func cloneTask(t api.TaskRuntime) api.TaskRuntime {
	return api.TaskRuntime{
		Spec: api.TaskSpec{
			Name:      t.Spec.Name,
			Resources: t.Spec.Resources,
			Duration:  t.Spec.Duration,
		},
		State:   t.State,
		NodeID:  t.NodeID,
		Attempt: t.Attempt,
	}
}

func cloneJob(j *api.JobRuntime) *api.JobRuntime {
	c := &api.JobRuntime{
		Spec: api.JobSpec{
			ID:       j.Spec.ID,
			Priority: j.Spec.Priority,
			Tasks:    make([]api.TaskSpec, len(j.Spec.Tasks)),
		},
		State:  j.State,
		Reason: j.Reason,
		Tasks:  make([]api.TaskRuntime, len(j.Tasks)),
	}
	for i, t := range j.Spec.Tasks {
		c.Spec.Tasks[i] = api.TaskSpec{Name: t.Name, Resources: t.Resources, Duration: t.Duration}
	}
	for i, t := range j.Tasks {
		c.Tasks[i] = cloneTask(t)
	}
	return c
}

// Add 接纳一个新作业；作业 ID 已存在（含已结束作业）即拒绝，结束作业不会复活。
// 调用方必须持有上层串行化锁。
func (s *Store) Add(j *api.JobRuntime) error {
	if _, ok := s.jobs[j.Spec.ID]; ok {
		return &api.RejectError{Kind: api.ErrDuplicateJob, Msg: "job id " + j.Spec.ID}
	}
	s.jobs[j.Spec.ID] = cloneJob(j)
	return s.Persist()
}

// Get 返回作业的深拷贝，外部修改不会影响存储。
func (s *Store) Get(id string) (*api.JobRuntime, bool) {
	j, ok := s.jobs[id]
	if !ok {
		return nil, false
	}
	return cloneJob(j), true
}

// Update 对作业做一次原子变更并立即落盘；mut 直接作用于存储内部对象，
// 因此调用方必须保证同一时刻没有其他访问者。
func (s *Store) Update(id string, mut func(*api.JobRuntime)) (*api.JobRuntime, error) {
	j, ok := s.jobs[id]
	if !ok {
		return nil, api.ErrJobNotFound
	}
	mut(j)
	if err := s.Persist(); err != nil {
		return nil, err
	}
	return cloneJob(j), nil
}

func (s *Store) List() []*api.JobRuntime {
	out := make([]*api.JobRuntime, 0, len(s.jobs))
	for _, j := range s.jobs {
		out = append(out, cloneJob(j))
	}
	return out
}

func (s *Store) Counter() int { return s.counter }

// SetDraining 记录/清除一个节点的排空标记并立即落盘。
// 调用方必须持有上层串行化锁。
func (s *Store) SetDraining(nodeID string, on bool) error {
	if on {
		s.draining[nodeID] = true
	} else {
		delete(s.draining, nodeID)
	}
	return s.Persist()
}

// IsDraining 查询节点是否被标记为排空中。
func (s *Store) IsDraining(nodeID string) bool { return s.draining[nodeID] }

// Draining 返回全部排空中节点 ID 的确定性排序列表。
func (s *Store) Draining() []string {
	out := make([]string, 0, len(s.draining))
	for id := range s.draining {
		out = append(out, id)
	}
	sortStrings(out)
	return out
}

// Persist 将当前内存状态原子写盘：先写临时文件再 rename，并推进版本号。
func (s *Store) Persist() error {
	if s.path == "" {
		s.counter++
		return nil
	}
	snap := api.Snapshot{Counter: s.counter + 1, Draining: s.Draining()}
	ids := make([]string, 0, len(s.jobs))
	for id := range s.jobs {
		ids = append(ids, id)
	}
	sortStrings(ids)
	for _, id := range ids {
		j := s.jobs[id]
		rec := api.JobRecord{
			Spec:   j.Spec,
			State:  j.State,
			Reason: j.Reason,
			Tasks:  make([]api.TaskRecord, len(j.Tasks)),
		}
		for i, t := range j.Tasks {
			rec.Tasks[i] = api.TaskRecord{Spec: t.Spec, State: t.State, NodeID: t.NodeID, Attempt: t.Attempt}
		}
		snap.Jobs = append(snap.Jobs, rec)
	}

	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("create state dir: %w", err)
	}
	tmp := s.path + ".tmp"
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return err
	}
	s.counter++
	return nil
}

// Recover 从状态文件恢复，并与当前内存表做合并而非整表替换：
//   - 内存中已存在的作业（恢复与提交/调度并发时可能已经包含更新的状态）
//     一律保留，磁盘快照不覆盖它们，避免用陈旧快照冲掉刚刚的提交；
//   - 仅快照中存在而内存中没有的作业并入内存（正常重启时内存为空，效果等同加载）；
//   - 计数器取内存与磁盘的较大值，保证版本号单调。
//
// 已结束作业原样并入（Submit 时按重复提交拒绝，不会复活）。
func (s *Store) Recover() (int, []*api.JobRuntime, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return s.counter, nil, nil
		}
		return 0, nil, err
	}
	var snap api.Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return 0, nil, fmt.Errorf("decode snapshot: %w", err)
	}

	added := make([]*api.JobRuntime, 0)
	for _, rec := range snap.Jobs {
		if _, exists := s.jobs[rec.Spec.ID]; exists {
			// 内存里的作业代表本进程更新的事实（含并发提交/推进），不被旧快照回退。
			continue
		}
		j := &api.JobRuntime{
			Spec:   rec.Spec,
			State:  rec.State,
			Reason: rec.Reason,
			Tasks:  make([]api.TaskRuntime, len(rec.Tasks)),
		}
		for i, tr := range rec.Tasks {
			j.Tasks[i] = api.TaskRuntime{Spec: tr.Spec, State: tr.State, NodeID: tr.NodeID, Attempt: tr.Attempt}
		}
		s.jobs[rec.Spec.ID] = j
		added = append(added, cloneJob(j))
	}
	if snap.Counter > s.counter {
		s.counter = snap.Counter
	}
	// 排空标记按并集合并：磁盘快照中的维护中节点不因重启而丢失，
	// 内存中已有的标记（恢复与并发排空交错时更新）也不被旧快照回退。
	for _, id := range snap.Draining {
		s.draining[id] = true
	}
	return snap.Counter, added, nil
}

func sortStrings(x []string) {
	for i := 1; i < len(x); i++ {
		for j := i; j > 0 && x[j-1] > x[j]; j-- {
			x[j-1], x[j] = x[j], x[j-1]
		}
	}
}
