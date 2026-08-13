package runtimefile

import (
	"os"
	"path/filepath"
	"testing"

	"admin/internal/config"
)

// TestSectionSpecsValid 确保运行期外部配置段规格完整且唯一。
func TestSectionSpecsValid(t *testing.T) {
	specs := sectionSpecs()
	if len(specs) == 0 {
		t.Fatal("运行期外部配置段规格不能为空")
	}
	seen := make(map[string]struct{}, len(specs))
	for _, spec := range specs {
		if spec.Key == "" {
			t.Fatalf("运行期外部配置段存在空 key: %+v", spec)
		}
		if spec.apply == nil {
			t.Fatalf("运行期外部配置段缺少合并逻辑: %s", spec.Key)
		}
		if _, ok := seen[spec.Key]; ok {
			t.Fatalf("运行期外部配置段重复: %s", spec.Key)
		}
		seen[spec.Key] = struct{}{}
	}
	for _, key := range []string{sectionWorkflows} {
		if _, ok := seen[key]; !ok {
			t.Fatalf("运行期外部配置段缺少 key=%s", key)
		}
	}
}

// TestApplyIgnoresLegacyTaskAndArchiveSections 验证外部文件不能再覆盖数据库管理的周期任务和归档任务。
func TestApplyIgnoresLegacyTaskAndArchiveSections(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime.yaml")
	content := []byte("task_periodic:\n  - name: external-task\narchive_jobs:\n  - name: external-archive\n")
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("写入测试运行配置失败: %v", err)
	}
	cfg := config.Config{
		Task:    config.TaskQueueConfig{Periodic: []config.TaskPeriodicConfig{{Name: "database-task"}}},
		Archive: config.ArchiveConfig{Jobs: []config.ArchiveJobConfig{{Name: "database-archive"}}},
	}
	if err := apply(path, &cfg); err != nil {
		t.Fatalf("apply() error = %v", err)
	}
	if len(cfg.Task.Periodic) != 1 || cfg.Task.Periodic[0].Name != "database-task" {
		t.Fatalf("周期任务被外部文件覆盖: %+v", cfg.Task.Periodic)
	}
	if len(cfg.Archive.Jobs) != 1 || cfg.Archive.Jobs[0].Name != "database-archive" {
		t.Fatalf("归档任务被外部文件覆盖: %+v", cfg.Archive.Jobs)
	}
}
