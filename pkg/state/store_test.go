package state

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/lisama569/gang-scheduler-go/pkg/api"
)

func sampleJob(id, state string) *api.JobRuntime {
	return &api.JobRuntime{
		Spec: api.JobSpec{ID: id, Priority: 3, Tasks: []api.TaskSpec{
			{Name: "t1", Duration: 2, Resources: api.Resource{CPU: 2, Memory: 1}},
		}},
		State: state,
		Tasks: []api.TaskRuntime{{
			Spec:    api.TaskSpec{Name: "t1", Duration: 2, Resources: api.Resource{CPU: 2, Memory: 1}},
			State:   api.TaskRunning,
			NodeID:  "n9",
			Attempt: 2,
		}},
	}
}

func TestStoreAddDuplicateAndGetClone(t *testing.T) {
	s := New("")
	if err := s.Add(sampleJob("j", api.JobRunning)); err != nil {
		t.Fatal(err)
	}
	err := s.Add(sampleJob("j", api.JobRunning))
	if !errors.Is(err, api.ErrDuplicateJob) {
		t.Fatalf("duplicate add must be ErrDuplicateJob, got %v", err)
	}
	// Get 返回深拷贝：外部修改不得影响存储。
	j, ok := s.Get("j")
	if !ok {
		t.Fatal("job missing")
	}
	j.Tasks[0].State = api.TaskSucceeded
	j.State = api.JobSucceeded
	again, _ := s.Get("j")
	if again.State != api.JobRunning || again.Tasks[0].State != api.TaskRunning {
		t.Fatalf("Get must return a defensive copy, store mutated: %+v", again)
	}
}

func TestStorePersistRecoverRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "snapshot.json")
	s1 := New(path)
	if err := s1.Add(sampleJob("running", api.JobRunning)); err != nil {
		t.Fatal(err)
	}
	if err := s1.Add(sampleJob("done", api.JobSucceeded)); err != nil {
		t.Fatal(err)
	}
	_, err := s1.Update("done", func(j *api.JobRuntime) {
		j.Reason = ""
		j.Tasks[0].State = api.TaskSucceeded
	})
	if err != nil {
		t.Fatal(err)
	}
	ver := s1.Counter()
	if ver < 3 {
		t.Fatalf("counter should advance per write, got %d", ver)
	}

	s2 := New(path)
	gotVer, jobs, err := s2.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if gotVer != ver || len(jobs) != 2 {
		t.Fatalf("recover ver=%d jobs=%d want ver=%d count=2", gotVer, len(jobs), ver)
	}
	byID := map[string]*api.JobRuntime{}
	for _, j := range jobs {
		byID[j.Spec.ID] = j
	}
	if byID["done"].State != api.JobSucceeded || byID["done"].Tasks[0].State != api.TaskSucceeded {
		t.Fatalf("terminal job not preserved: %+v", byID["done"])
	}
	if byID["running"].Tasks[0].NodeID != "n9" || byID["running"].Tasks[0].Attempt != 2 {
		t.Fatalf("runtime fields not preserved: %+v", byID["running"].Tasks[0])
	}
}

func TestStoreRecoverMissingFile(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), "absent.json"))
	ver, jobs, err := s.Recover()
	if err != nil || ver != 0 || jobs != nil {
		t.Fatalf("missing file must mean empty state: ver=%d jobs=%v err=%v", ver, jobs, err)
	}
}

func TestStoreUpdateMissing(t *testing.T) {
	s := New("")
	_, err := s.Update("ghost", func(*api.JobRuntime) {})
	if !errors.Is(err, api.ErrJobNotFound) {
		t.Fatalf("missing update must return ErrJobNotFound, got %v", err)
	}
}
