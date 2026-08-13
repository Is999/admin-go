package configload

import (
	"os"
	"path/filepath"
	"testing"
)

// TestLoadConfigOnlyMergesRuntimeWorkflowFile 确保外部运行文件只覆盖 workflows，不再读取周期任务和归档任务。
func TestLoadConfigOnlyMergesRuntimeWorkflowFile(t *testing.T) {
	mainFile := writeRuntimeConfigFiles(t, "", `
task_periodic:
  - name: "external-periodic"
    cron: "0 2 * * *"
    workflow: "external.workflow"
archive_jobs:
  - name: "external_archive"
    enabled: true
    database: "main"
    table_name: "admin_log"
workflows:
  user_tag:
    enabled: true
    default_shard_total: 16
`)
	cfg, err := Load(mainFile)
	if err != nil {
		t.Fatalf("加载配置失败: %v", err)
	}
	if got := len(cfg.Task.Periodic); got != 0 {
		t.Fatalf("外部运行文件不应加载周期任务，实际为 %d", got)
	}
	if got := len(cfg.Archive.Jobs); got != 0 {
		t.Fatalf("外部运行文件不应加载归档任务，实际为 %d", got)
	}
	if !cfg.Workflows.UserTag.Enabled || cfg.Workflows.UserTag.DefaultShardTotal != 16 {
		t.Fatalf("期望外部 workflows.user_tag 覆盖主配置，实际为 %+v", cfg.Workflows.UserTag)
	}
}

// TestLoadConfigIgnoresUnknownRuntimeConfigKeys 验证运行期外部配置会忽略当前版本不认识的顶层配置。
func TestLoadConfigIgnoresUnknownRuntimeConfigKeys(t *testing.T) {
	mainFile := writeRuntimeConfigFiles(t, "", `
user_tag:
  enabled: true
  default_shard_total: 12
future_feature:
  enabled: true
`)
	cfg, err := Load(mainFile)
	if err != nil {
		t.Fatalf("未知顶层配置不应导致启动失败: %v", err)
	}
	if cfg.Workflows.UserTag.DefaultShardTotal != 0 {
		t.Fatalf("未知顶层 user_tag 不应被隐式合并，实际为 %+v", cfg.Workflows.UserTag)
	}
}

// TestLoadConfigIgnoresUnknownRuntimeConfigBlock 确保未知运行期配置块不会误合并。
func TestLoadConfigIgnoresUnknownRuntimeConfigBlock(t *testing.T) {
	mainFile := writeRuntimeConfigFiles(t, "", `
obsolete_feature:
  enabled: true
  database: "main"
`)
	if _, err := Load(mainFile); err != nil {
		t.Fatalf("未知运行期配置块应被忽略: %v", err)
	}
}

// TestLoadConfigAllowsMainRuntimeConfigSections 验证主配置运行期块不影响基础加载。
func TestLoadConfigAllowsMainRuntimeConfigSections(t *testing.T) {
	mainFile := writeRuntimeConfigFiles(t, `
task:
  periodic:
    - name: "main-periodic"
      cron: "0 1 * * *"
      workflow: "main.workflow"
`, "task_periodic: []\n")
	cfg, err := Load(mainFile)
	if err != nil {
		t.Fatalf("主配置 task.periodic 不应额外扫描拒绝: %v", err)
	}
	if got := len(cfg.Task.Periodic); got != 1 {
		t.Fatalf("期望保留主配置中的 1 个周期任务，实际为 %d", got)
	}
}

// TestLoadConfigIgnoresMainTopLevelRuntimeConfig 验证主配置未知顶层块会被忽略。
func TestLoadConfigIgnoresMainTopLevelRuntimeConfig(t *testing.T) {
	mainFile := writeRuntimeConfigFiles(t, `
task_periodic:
  - name: "main-top-periodic"
    cron: "0 1 * * *"
    workflow: "main.workflow"
`, "task_periodic: []\n")
	cfg, err := Load(mainFile)
	if err != nil {
		t.Fatalf("主配置未知顶层 task_periodic 不应额外扫描拒绝: %v", err)
	}
	if got := len(cfg.Task.Periodic); got != 0 {
		t.Fatalf("未知顶层 task_periodic 不应隐式写入 task.periodic，实际为 %d", got)
	}
}

// TestLoadConfigResolvesRuntimeRelativeToMainFile 确保外部配置相对路径固定按主配置目录解析。
func TestLoadConfigResolvesRuntimeRelativeToMainFile(t *testing.T) {
	dir := t.TempDir()
	mainDir := filepath.Join(dir, "main")
	workingDir := filepath.Join(dir, "working")
	if err := os.MkdirAll(filepath.Join(mainDir, "config.d"), 0o755); err != nil {
		t.Fatalf("创建主配置外部目录失败: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(workingDir, "config.d"), 0o755); err != nil {
		t.Fatalf("创建工作目录外部目录失败: %v", err)
	}
	mainFile := filepath.Join(mainDir, "config.yaml")
	if err := os.WriteFile(mainFile, []byte(minimalConfigYAML(`
config_files:
  runtime: "config.d/runtime.sample.yaml"
`)), 0o644); err != nil {
		t.Fatalf("写入主配置失败: %v", err)
	}
	if err := os.WriteFile(filepath.Join(mainDir, "config.d", "runtime.sample.yaml"), []byte(`
workflows:
  user_tag:
    enabled: true
    default_shard_total: 16
`), 0o644); err != nil {
		t.Fatalf("写入主配置目录 runtime 失败: %v", err)
	}
	if err := os.WriteFile(filepath.Join(workingDir, "config.d", "runtime.sample.yaml"), []byte(`
workflows:
  user_tag:
    enabled: true
    default_shard_total: 99
`), 0o644); err != nil {
		t.Fatalf("写入工作目录 runtime 失败: %v", err)
	}
	oldWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("读取当前工作目录失败: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chdir(oldWD)
	})
	if err = os.Chdir(workingDir); err != nil {
		t.Fatalf("切换工作目录失败: %v", err)
	}
	cfg, err := Load(mainFile)
	if err != nil {
		t.Fatalf("加载配置失败: %v", err)
	}
	if cfg.Workflows.UserTag.DefaultShardTotal != 16 {
		t.Fatalf("期望读取主配置目录 runtime，实际 workflows.user_tag.default_shard_total=%d", cfg.Workflows.UserTag.DefaultShardTotal)
	}
}

// TestLoadConfigIgnoresNestedRuntimeTaskBlock 确保运行期文件不会误合并非白名单任务块。
func TestLoadConfigIgnoresNestedRuntimeTaskBlock(t *testing.T) {
	mainFile := writeRuntimeConfigFiles(t, "", `
task:
  periodic:
    - name: "ignored-periodic"
      cron: "0 2 * * *"
      workflow: "ignored.workflow"
`)
	cfg, err := Load(mainFile)
	if err != nil {
		t.Fatalf("非白名单任务块不应导致启动失败: %v", err)
	}
	if len(cfg.Task.Periodic) != 0 {
		t.Fatalf("非白名单 task.periodic 不应被运行期文件隐式合并，实际数量=%d", len(cfg.Task.Periodic))
	}
}
