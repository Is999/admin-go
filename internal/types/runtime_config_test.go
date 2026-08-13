package types

import (
	"testing"

	tasklimits "admin/internal/task/limits"
)

// TestRuntimeConfigQueryReqAllowsBoundedLargePages 验证两个草稿列表允许五百条，但不会接受更大的单页请求。
func TestRuntimeConfigQueryReqAllowsBoundedLargePages(t *testing.T) {
	periodicReq := &RuntimeTaskPeriodicQueryReq{GetPageReq: GetPageReq{Page: 0, PageSize: runtimeConfigMaxPageSize}}
	if err := periodicReq.Validate(); err != nil {
		t.Fatalf("周期任务查询参数校验失败: %v", err)
	}
	if periodicReq.Page != defaultPageNumber || periodicReq.PageSize != runtimeConfigMaxPageSize {
		t.Fatalf("周期任务分页 = %d/%d", periodicReq.Page, periodicReq.PageSize)
	}

	archiveReq := &RuntimeArchiveJobQueryReq{GetPageReq: GetPageReq{Page: 2, PageSize: runtimeConfigMaxPageSize + 1}}
	if err := archiveReq.Validate(); err != nil {
		t.Fatalf("归档任务查询参数校验失败: %v", err)
	}
	if archiveReq.Page != 2 || archiveReq.PageSize != runtimeConfigMaxPageSize {
		t.Fatalf("归档任务分页 = %d/%d", archiveReq.Page, archiveReq.PageSize)
	}
}

// TestSaveRuntimeTaskPeriodicReqValidateNormalizesTargets 验证对应场景符合预期。
func TestSaveRuntimeTaskPeriodicReqValidateNormalizesTargets(t *testing.T) {
	req := &SaveRuntimeTaskPeriodicReq{
		Name:         "daily",
		Workflow:     "user_tag.refresh",
		EverySeconds: 60,
		Targets:      []string{" uid:1 ", "", "uid:1", " uid:2 "},
	}

	if err := req.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	want := []string{"uid:1", "uid:2"}
	if len(req.Targets) != len(want) {
		t.Fatalf("Targets = %#v, want %#v", req.Targets, want)
	}
	for i := range want {
		if req.Targets[i] != want[i] {
			t.Fatalf("Targets = %#v, want %#v", req.Targets, want)
		}
	}
}

// TestSaveRuntimeTaskPeriodicReqValidateKeepsEmptyTargetsNil 验证对应场景符合预期。
func TestSaveRuntimeTaskPeriodicReqValidateKeepsEmptyTargetsNil(t *testing.T) {
	req := &SaveRuntimeTaskPeriodicReq{
		Name:         "daily",
		Workflow:     "user_tag.refresh",
		EverySeconds: 60,
		Targets:      []string{" ", ""},
	}

	if err := req.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	if req.Targets != nil {
		t.Fatalf("Targets = %#v, want nil", req.Targets)
	}
}

// TestSaveRuntimeTaskPeriodicReqRejectsResourceOverridesAboveHardLimits 校验周期任务草稿入口拒绝无界资源参数。
func TestSaveRuntimeTaskPeriodicReqRejectsResourceOverridesAboveHardLimits(t *testing.T) {
	tests := []struct {
		name string                     // 测试场景
		req  SaveRuntimeTaskPeriodicReq // 待校验请求
	}{
		{
			name: "retry",
			req:  SaveRuntimeTaskPeriodicReq{Retry: tasklimits.MaxRetry + 1},
		},
		{
			name: "timeout",
			req:  SaveRuntimeTaskPeriodicReq{TimeoutSeconds: tasklimits.MaxTimeoutSeconds + 1},
		},
		{
			name: "shard total",
			req:  SaveRuntimeTaskPeriodicReq{ShardTotal: tasklimits.MaxShardTotal + 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := tt.req
			req.Name = "daily"
			req.Workflow = "demo.workflow"
			req.EverySeconds = 60
			if err := req.Validate(); err == nil {
				t.Fatal("期望超过任务资源硬上限的周期任务草稿被拒绝")
			}
		})
	}
}

// TestSaveRuntimeTaskPeriodicReqRejectsInvalidScheduleBounds 校验草稿保存阶段直接拒绝运行态无法接受的调度参数。
func TestSaveRuntimeTaskPeriodicReqRejectsInvalidScheduleBounds(t *testing.T) {
	tests := []struct {
		name string                     // 测试场景
		req  SaveRuntimeTaskPeriodicReq // 待校验请求
	}{
		{
			name: "every seconds below minimum",
			req: SaveRuntimeTaskPeriodicReq{
				EverySeconds: tasklimits.MinPeriodicEverySeconds - 1,
			},
		},
		{
			name: "invalid deadline",
			req: SaveRuntimeTaskPeriodicReq{
				EverySeconds: tasklimits.MinPeriodicEverySeconds,
				Deadline:     "2026-08-02 12:00:00",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := tt.req
			req.Name = "daily"
			req.Workflow = "demo.workflow"
			if err := req.Validate(); err == nil {
				t.Fatal("期望无效周期任务调度参数被拒绝")
			}
		})
	}
}
