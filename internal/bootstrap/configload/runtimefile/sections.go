package runtimefile

import "admin/internal/config"

// 运行期外部配置支持的顶层配置段；周期任务和归档任务只从数据库发布快照读取。
const (
	sectionWorkflows = "workflows" // 工作流类配置聚合入口
)

// file 描述外部运行期工作流配置文件；周期任务和归档任务字段即使出现也不会被当前进程读取。
type file struct {
	Workflows config.WorkflowsConfig `json:"workflows,optional"` // 工作流类配置聚合入口
}

// sectionSpec 描述一个允许运行期外置的配置段。
type sectionSpec struct {
	Key   string                                                  // 外部运行期配置文件中的顶层键
	apply func(cfg *config.Config, ext file, source string) error // 将该配置段合并到主配置
}

// sectionSpecs 返回运行期外部配置段规格。
func sectionSpecs() []sectionSpec {
	return []sectionSpec{
		{
			Key:   sectionWorkflows,
			apply: applyWorkflows,
		},
	}
}

// sectionKeys 返回当前版本会主动读取的运行期外部配置顶层键。
func sectionKeys() map[string]struct{} {
	specs := sectionSpecs()
	keys := make(map[string]struct{}, len(specs))
	for _, spec := range specs {
		keys[spec.Key] = struct{}{}
	}
	return keys
}

// applyWorkflows 覆盖外部 workflows 配置块。
func applyWorkflows(cfg *config.Config, ext file, _ string) error {
	// workflows 是工作流类配置统一入口，运行期文件显式声明后整体覆盖。
	cfg.Workflows = ext.Workflows
	return nil
}
