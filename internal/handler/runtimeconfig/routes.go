package runtimeconfig

import (
	"net/http"

	"admin/internal/handler/shared"
)

// RouteSpecs 返回运行配置管理路由规格。
func RouteSpecs() []shared.RouteSpec {
	return []shared.RouteSpec{
		{
			Method:      http.MethodGet,
			Path:        "/api/runtime-config/overview", // 查询运行配置概览。
			Access:      shared.RouteAccessAuth,
			Meta:        shared.RuntimeConfigOverview,
			Description: shared.RuntimeConfigOverview.Describe,
			Handler:     GetOverviewHandler,
		},
		{
			Method:      http.MethodGet,
			Path:        "/api/runtime-config/periodic", // 查询周期任务草稿。
			Access:      shared.RouteAccessAuth,
			Meta:        shared.RuntimeConfigList,
			Description: shared.RuntimeConfigList.Describe,
			Handler:     ListPeriodicTasksHandler,
		},
		{
			Method:      http.MethodPost,
			Path:        "/api/runtime-config/periodic", // 保存周期任务草稿。
			Access:      shared.RouteAccessAuth,
			Meta:        shared.RuntimeConfigSave,
			Description: shared.RuntimeConfigSave.Describe,
			Handler:     SavePeriodicTaskHandler,
		},
		{
			Method:      http.MethodDelete,
			Path:        "/api/runtime-config/periodic/:id", // 删除周期任务草稿。
			Access:      shared.RouteAccessAuth,
			Meta:        shared.RuntimeConfigSave,
			Description: shared.RuntimeConfigSave.Describe,
			Handler:     DeletePeriodicTaskHandler,
		},
		{
			Method:      http.MethodGet,
			Path:        "/api/runtime-config/archive-jobs", // 查询归档任务草稿。
			Access:      shared.RouteAccessAuth,
			Meta:        shared.RuntimeConfigList,
			Description: shared.RuntimeConfigList.Describe,
			Handler:     ListArchiveJobsHandler,
		},
		{
			Method:        http.MethodGet,
			Path:          "/api/runtime-config/archive-jobs/:id/progress", // 查询归档任务执行详情。
			Access:        shared.RouteAccessAuth,
			Alias:         shared.RuntimeConfigList.Alias,
			Description:   "查询归档任务执行详情",
			SkipAccessLog: true,
			Handler:       GetArchiveJobProgressHandler,
		},
		{
			Method:      http.MethodPost,
			Path:        "/api/runtime-config/archive-jobs", // 保存归档任务草稿。
			Access:      shared.RouteAccessAuth,
			Meta:        shared.RuntimeConfigSave,
			Description: shared.RuntimeConfigSave.Describe,
			Handler:     SaveArchiveJobHandler,
		},
		{
			Method:      http.MethodDelete,
			Path:        "/api/runtime-config/archive-jobs/:id", // 删除归档任务草稿。
			Access:      shared.RouteAccessAuth,
			Meta:        shared.RuntimeConfigSave,
			Description: shared.RuntimeConfigSave.Describe,
			Handler:     DeleteArchiveJobHandler,
		},
		{
			Method:      http.MethodPost,
			Path:        "/api/runtime-config/validate", // 预检运行配置草稿。
			Access:      shared.RouteAccessAuth,
			Meta:        shared.RuntimeConfigValidate,
			Description: shared.RuntimeConfigValidate.Describe,
			Handler:     ValidateDraftHandler,
		},
		{
			Method:      http.MethodPost,
			Path:        "/api/runtime-config/publish", // 发布运行配置草稿。
			Access:      shared.RouteAccessAuth,
			Meta:        shared.RuntimeConfigPublish,
			Description: shared.RuntimeConfigPublish.Describe,
			Handler:     PublishHandler,
		},
		{
			Method:      http.MethodGet,
			Path:        "/api/runtime-config/releases", // 查询运行配置发布历史。
			Access:      shared.RouteAccessAuth,
			Meta:        shared.RuntimeConfigList,
			Description: shared.RuntimeConfigList.Describe,
			Handler:     ListReleasesHandler,
		},
		{
			Method:      http.MethodGet,
			Path:        "/api/runtime-config/releases/:releaseId", // 查询运行配置发布快照。
			Access:      shared.RouteAccessAuth,
			Meta:        shared.RuntimeConfigList,
			Description: shared.RuntimeConfigList.Describe,
			Handler:     GetReleaseHandler,
		},
		{
			Method:      http.MethodPost,
			Path:        "/api/runtime-config/rollback", // 回滚运行配置发布快照。
			Access:      shared.RouteAccessAuth,
			Meta:        shared.RuntimeConfigRollback,
			Description: shared.RuntimeConfigRollback.Describe,
			Handler:     RollbackHandler,
		},
	}
}
