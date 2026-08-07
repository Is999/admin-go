package builtin

import (
	"context"
	"strings"

	core "admin/internal/bootstrap/components"
	"admin/internal/bootstrap/configload"
	"admin/internal/bootstrap/register"
	"admin/internal/bootstrap/runmode"
	"admin/internal/config"
	"admin/internal/infra/redisx"
	"admin/internal/jobs"
	"admin/internal/jobs/taskalert"
	"admin/internal/task/queue"
	taskruntime "admin/internal/task/runtime"

	"github.com/Is999/go-utils/errors"
	"github.com/redis/go-redis/v9"
)

// newTaskRuntime 创建任务队列与任务运行时启动组件。
func newTaskRuntime() core.Component {
	return core.NewFunc(NameTaskRuntime, func(ctx context.Context, state *core.State) error {
		if state == nil || state.ServiceContext == nil {
			return errors.Errorf("任务运行时组件缺少服务上下文")
		}
		if !state.Config.Task.Enabled {
			return nil
		}
		if err := taskqueue.RegisterMetrics(); err != nil {
			return errors.Wrap(err, "注册任务队列指标失败")
		}

		// 任务配置注入 AppID，保证队列、调度锁和幂等键按站点隔离。
		taskCfg := TaskConfigWithAppID(state.Config)
		taskRedis, taskRedisOwned, err := resolveTaskRedisClient(ctx, state, taskCfg)
		if err != nil {
			return errors.Tag(err)
		}
		state.TaskRedis = taskRedis
		state.TaskRedisOwned = taskRedisOwned

		manager := taskqueue.New(taskCfg, taskRedis)
		if manager == nil {
			return nil
		}
		state.ServiceContext.Task = manager
		state.TaskManager = manager

		// 任务插件统一走注册规格收口，避免内置插件列表散落在不同启动文件。
		defaultPlugins := taskruntime.PluginsFromSpecs(DefaultTaskPluginSpecs())
		plugins := taskruntime.ComposePlugins(defaultPlugins, state.TaskPlugins)
		if err := register.ValidateNamesUnique(register.KindTaskPlugin, register.PluginNames(plugins)); err != nil {
			return errors.Tag(err)
		}
		taskRuntime, err := taskruntime.Register(state.ServiceContext, manager, plugins...)
		if err != nil {
			return errors.Tag(err)
		}
		state.TaskRuntime = taskRuntime
		if err := taskalert.RegisterLark(state.Config, manager); err != nil {
			return errors.Tag(err)
		}
		if err := registerTaskRuntimeLifecycle(state, manager); err != nil {
			return errors.Tag(err)
		}
		return nil
	})
}

// TaskConfigWithAppID 把顶层 app_id 注入任务队列配置，保证 Redis key 和队列名统一隔离。
func TaskConfigWithAppID(c config.Config) config.TaskQueueConfig {
	cfg := c.Task
	cfg.AppID = strings.TrimSpace(c.AppID)
	return cfg
}

// DefaultTaskPluginSpecs 返回项目内置任务运行时插件规格，顺序即注册顺序。
func DefaultTaskPluginSpecs() []taskruntime.PluginSpec {
	return taskruntime.ComposePluginSpecs(taskruntime.CorePluginSpecs(), jobs.PluginSpecs())
}

// resolveTaskRedisClient 返回任务系统实际使用的 Redis 客户端和 App 托管标记。
func resolveTaskRedisClient(ctx context.Context, state *core.State, taskCfg config.TaskQueueConfig) (redis.UniversalClient, bool, error) {
	if err := configload.ValidateTaskRedisConfig(taskCfg.Redis); err != nil {
		return nil, false, errors.Tag(err)
	}
	if !configload.HasTaskRedisConfig(taskCfg.Redis) {
		return state.ServiceContext.Rds, false, nil
	}
	rdb, err := redisx.New(ctx, taskCfg.Redis, state.Config.Observability)
	if err != nil {
		return nil, false, errors.Tag(err)
	}
	return rdb, true, nil
}

// registerTaskRuntimeLifecycle 注册任务 Worker 和 Scheduler 后台生命周期。
func registerTaskRuntimeLifecycle(state *core.State, manager *taskqueue.Manager) error {
	if !runmode.ShouldStartTask(state.Mode) {
		return nil
	}
	// 启动期校验 workflow 注册完整性，避免配置漂移后反复失败。
	if err := manager.ValidatePeriodicTaskDefinitions(); err != nil {
		return errors.Tag(err)
	}
	// 任务后台生命周期只在 Worker 或 Scheduler 模式启动；API-only 仅保留任务投递和查询能力。
	return state.AddLifecycleHooks(NameTaskRuntime, func(ctx context.Context) error {
		if runmode.Has(state.Mode, runmode.Worker) {
			if err := manager.StartWorker(); err != nil {
				return errors.Wrap(err, "启动任务 worker 失败")
			}
		}
		if runmode.Has(state.Mode, runmode.Scheduler) {
			if err := manager.StartScheduler(); err != nil {
				if stopErr := manager.Stop(ctx); stopErr != nil {
					return errors.Wrapf(err, "启动任务调度器失败; 回滚任务运行时失败: %v", stopErr)
				}
				return errors.Wrap(err, "启动任务调度器失败")
			}
		}
		return nil
	}, func(ctx context.Context) error {
		return errors.Tag(manager.Stop(ctx))
	})
}
