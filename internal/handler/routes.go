package handler

import (
	"strings"

	adminhandler "admin/internal/handler/admin"
	authhandler "admin/internal/handler/auth"
	cachehandler "admin/internal/handler/cachemanage"
	collectorhandler "admin/internal/handler/collector"
	confighandler "admin/internal/handler/config"
	docshandler "admin/internal/handler/docs"
	healthhandler "admin/internal/handler/health"
	messagehandler "admin/internal/handler/message"
	profilehandler "admin/internal/handler/profile"
	rbachandler "admin/internal/handler/rbac"
	runtimeconfighandler "admin/internal/handler/runtimeconfig"
	secretkeyhandler "admin/internal/handler/secretkey"
	securitydebughandler "admin/internal/handler/securitydebug"
	"admin/internal/handler/shared"
	taskhandler "admin/internal/handler/task"
	transferhandler "admin/internal/handler/transfer"
	userhandler "admin/internal/handler/user"
	usertaghandler "admin/internal/handler/usertag"
	"admin/internal/middleware"
	"admin/internal/svc"

	"github.com/Is999/go-utils/errors"
	"github.com/zeromicro/go-zero/rest"
)

// RouteModule 描述一个可插拔 HTTP 路由模块。
type RouteModule interface {
	Name() string               // Name 返回路由模块名称
	Routes() []shared.RouteSpec // Routes 返回模块路由规格
}

// RouteModuleFunc 允许通过函数快速声明路由模块。
type RouteModuleFunc struct {
	name   string                    // 路由模块名称
	routes func() []shared.RouteSpec // 路由规格函数
}

// RouteModuleSpec 描述内置 HTTP 路由模块的装配和注册清单信息。
type RouteModuleSpec struct {
	Name        string                    // 模块名称，必须在内置模块中唯一
	File        string                    // 模块路由规格所在文件
	Method      string                    // 模块路由规格入口
	Description string                    // 模块中文说明
	Routes      func() []shared.RouteSpec // 模块内置路由规格
}

// NewRouteModuleFunc 创建函数式路由模块。
func NewRouteModuleFunc(name string, routes func() []shared.RouteSpec) RouteModule {
	return RouteModuleFunc{name: strings.TrimSpace(name), routes: routes}
}

// Name 返回路由模块名称。
func (m RouteModuleFunc) Name() string {
	return m.name
}

// Routes 返回当前模块的路由规格。
func (m RouteModuleFunc) Routes() []shared.RouteSpec {
	if m.routes == nil {
		return nil
	}
	return m.routes()
}

// ComposeRouteModules 合并多组路由模块，保持注册顺序。
func ComposeRouteModules(groups ...[]RouteModule) []RouteModule {
	total := 0
	for _, group := range groups {
		total += len(group)
	}
	modules := make([]RouteModule, 0, total)
	for _, group := range groups {
		modules = append(modules, group...)
	}
	return modules
}

// BuiltinRouteModules 返回当前进程默认启用的路由模块集合。
// 该方法由内置模块规格派生，供启动装配、测试和显式装配入口复用。
func BuiltinRouteModules() []RouteModule {
	specs := BuiltinRouteModuleSpecs()
	modules := make([]RouteModule, 0, len(specs))
	for _, spec := range specs {
		if spec.Routes == nil {
			continue
		}
		modules = append(modules, newRouteModule(spec))
	}
	return modules
}

// BuiltinRouteModuleSpecs 返回内置 HTTP 路由模块规格，供启动装配和注册清单复用。
func BuiltinRouteModuleSpecs() []RouteModuleSpec {
	out := make([]RouteModuleSpec, len(builtinRouteModuleSpecs))
	copy(out, builtinRouteModuleSpecs)
	return out
}

// newRouteModule 从内置模块规格派生真实路由模块。
func newRouteModule(spec RouteModuleSpec) RouteModule {
	return NewRouteModuleFunc(spec.Name, func() []shared.RouteSpec {
		if spec.Routes == nil {
			return nil
		}
		return spec.Routes()
	})
}

// DefaultRouteSpecs 返回内置 HTTP 路由规格，顺序与内置模块注册顺序保持一致。
func DefaultRouteSpecs() []shared.RouteSpec {
	moduleSpecs := BuiltinRouteModuleSpecs()
	groups := make([][]shared.RouteSpec, 0, len(moduleSpecs))
	total := 0
	for _, moduleSpec := range moduleSpecs {
		if moduleSpec.Routes == nil {
			continue
		}
		group := routeSpecsWithModule(moduleSpec.Name, moduleSpec.Routes())
		total += len(group)
		groups = append(groups, group)
	}
	specs := make([]shared.RouteSpec, 0, total)
	for _, group := range groups {
		specs = append(specs, group...)
	}
	return specs
}

// routeSpecsWithModule 返回写入模块名称后的路由规格快照。
func routeSpecsWithModule(module string, specs []shared.RouteSpec) []shared.RouteSpec {
	out := make([]shared.RouteSpec, len(specs))
	copy(out, specs)
	for index := range out {
		if out[index].Module == "" {
			out[index].Module = module
		}
	}
	return out
}

// routeSpecsForServer 按监听器边界筛选路由，公网与内网路由不在同一 Server 注册。
func routeSpecsForServer(specs []shared.RouteSpec, internal bool) []shared.RouteSpec {
	filtered := make([]shared.RouteSpec, 0, len(specs))
	for _, spec := range specs {
		if (spec.Access == shared.RouteAccessInternal) != internal {
			continue
		}
		filtered = append(filtered, spec)
	}
	return filtered
}

// RegisterPublicHandlers 统一注册公网监听器的全局中间件和各领域路由模块。
// 自动组合内置路由和额外模块；启动链使用 bootstrap 统一清单调用 RegisterPublicHandlersWithModules。
// 中间件顺序固定为 outer recover -> trace -> access log -> inner recover：
// 1. outer recover 兜底保护入口中间件自身异常；
// 2. trace 创建上下文和 span；
// 3. access log 使用 defer 在请求结束时统一收口；
// 4. inner recover 最靠近业务 handler，把 panic 转成标准响应后交回上层记录。
func RegisterPublicHandlers(server *rest.Server, serverCtx *svc.ServiceContext, modules ...RouteModule) error {
	moduleGroups := [][]RouteModule{BuiltinRouteModules(), modules}
	return RegisterPublicHandlersWithModules(server, serverCtx, ComposeRouteModules(moduleGroups...)...)
}

// RegisterInternalHandlers 统一注册内网监听器的全局中间件和各领域路由模块。
func RegisterInternalHandlers(server *rest.Server, serverCtx *svc.ServiceContext, modules ...RouteModule) error {
	moduleGroups := [][]RouteModule{BuiltinRouteModules(), modules}
	return RegisterInternalHandlersWithModules(server, serverCtx, ComposeRouteModules(moduleGroups...)...)
}

// RegisterPublicHandlersWithModules 按完整模块清单注册公网路由。
func RegisterPublicHandlersWithModules(server *rest.Server, serverCtx *svc.ServiceContext, modules ...RouteModule) error {
	return registerHandlersWithModules(server, serverCtx, false, modules...)
}

// RegisterInternalHandlersWithModules 按完整模块清单注册内网路由。
func RegisterInternalHandlersWithModules(server *rest.Server, serverCtx *svc.ServiceContext, modules ...RouteModule) error {
	return registerHandlersWithModules(server, serverCtx, true, modules...)
}

// registerHandlersWithModules 注册公共中间件，并按监听器边界筛选所有模块路由。
func registerHandlersWithModules(server *rest.Server, serverCtx *svc.ServiceContext, internal bool, modules ...RouteModule) error {
	if server == nil {
		return errors.Errorf("注册 HTTP 路由时 Server 为空 internal=%t", internal)
	}
	if serverCtx == nil {
		return errors.Errorf("注册 HTTP 路由时 ServiceContext 为空 internal=%t", internal)
	}
	server.Use(middleware.NewRecoverMiddleware().Handle)
	server.Use(middleware.NewTraceMiddleware(serverCtx).Handle)
	server.Use(middleware.NewAccessLogMiddleware().Handle)
	server.Use(middleware.NewRecoverMiddleware().Handle)

	// 统一在这里构造领域级中间件，避免各模块重复创建。
	authMw := middleware.NewAuthMiddleware(serverCtx)
	opsMw := middleware.NewOpsMiddleware(serverCtx)
	for _, module := range modules {
		if module == nil {
			continue
		}
		routes := routeSpecsWithModule(module.Name(), module.Routes())
		routes = routeSpecsForServer(routes, internal)
		if err := shared.AddRouteSpecs(server, serverCtx, authMw, opsMw, routes); err != nil {
			return errors.Wrapf(err, "注册路由模块失败 module=%s internal=%t", module.Name(), internal)
		}
	}
	return nil
}

// builtinRouteModuleSpecs 是内置 HTTP 路由模块的单一装配规格。
var builtinRouteModuleSpecs = []RouteModuleSpec{
	{
		Name:        "health",
		File:        "internal/handler/health/routes.go",
		Method:      "health.RouteSpecs",
		Description: "注册健康检查路由",
		Routes:      healthhandler.RouteSpecs,
	},
	{
		Name:        "auth",
		File:        "internal/handler/auth/routes.go",
		Method:      "auth.RouteSpecs",
		Description: "注册后台认证路由",
		Routes:      authhandler.RouteSpecs,
	},
	{
		Name:        "admin",
		File:        "internal/handler/admin/routes.go",
		Method:      "admin.RouteSpecs",
		Description: "注册管理员管理路由",
		Routes:      adminhandler.RouteSpecs,
	},
	{
		Name:        "user",
		File:        "internal/handler/user/routes.go",
		Method:      "user.RouteSpecs",
		Description: "注册前台用户和 API 运行态管理路由",
		Routes:      userhandler.RouteSpecs,
	},
	{
		Name:        "profile",
		File:        "internal/handler/profile/routes.go",
		Method:      "profile.RouteSpecs",
		Description: "注册个人中心和账号安全路由",
		Routes:      profilehandler.RouteSpecs,
	},
	{
		Name:        "rbac",
		File:        "internal/handler/rbac/routes.go",
		Method:      "rbac.RouteSpecs",
		Description: "注册角色权限路由",
		Routes:      rbachandler.RouteSpecs,
	},
	{
		Name:        "config",
		File:        "internal/handler/config/routes.go",
		Method:      "config.RouteSpecs",
		Description: "注册系统配置路由",
		Routes:      confighandler.RouteSpecs,
	},
	{
		Name:        "cache",
		File:        "internal/handler/cachemanage/routes.go",
		Method:      "cachemanage.RouteSpecs",
		Description: "注册缓存管理路由",
		Routes:      cachehandler.RouteSpecs,
	},
	{
		Name:        "admin_log",
		File:        "internal/handler/admin/routes.go",
		Method:      "admin.LogRouteSpecs",
		Description: "注册管理员审计日志路由",
		Routes:      adminhandler.LogRouteSpecs,
	},
	{
		Name:        "secret_key",
		File:        "internal/handler/secretkey/routes.go",
		Method:      "secretkey.RouteSpecs",
		Description: "注册秘钥管理路由",
		Routes:      secretkeyhandler.RouteSpecs,
	},
	{
		Name:        "security_debug",
		File:        "internal/handler/securitydebug/routes.go",
		Method:      "securitydebug.RouteSpecs",
		Description: "注册安全调试路由",
		Routes:      securitydebughandler.RouteSpecs,
	},
	{
		Name:        "message",
		File:        "internal/handler/message/routes.go",
		Method:      "message.RouteSpecs",
		Description: "注册消息中心路由",
		Routes:      messagehandler.RouteSpecs,
	},
	{
		Name:        "transfer",
		File:        "internal/handler/transfer/routes.go",
		Method:      "transfer.RouteSpecs",
		Description: "注册文件传输路由",
		Routes:      transferhandler.RouteSpecs,
	},
	{
		Name:        "task",
		File:        "internal/handler/task/routes.go",
		Method:      "task.RouteSpecs",
		Description: "注册任务系统路由",
		Routes:      taskhandler.RouteSpecs,
	},
	{
		Name:        "runtime_config",
		File:        "internal/handler/runtimeconfig/routes.go",
		Method:      "runtimeconfig.RouteSpecs",
		Description: "注册运行配置管理路由",
		Routes:      runtimeconfighandler.RouteSpecs,
	},
	{
		Name:        "collector",
		File:        "internal/handler/collector/routes.go",
		Method:      "collector.RouteSpecs",
		Description: "注册 Collector 管理路由",
		Routes:      collectorhandler.RouteSpecs,
	},
	{
		Name:        "user_tag",
		File:        "internal/handler/usertag/routes.go",
		Method:      "usertag.RouteSpecs",
		Description: "注册用户标签业务路由",
		Routes:      usertaghandler.RouteSpecs,
	},
	{
		Name:        "internal_user_tag",
		File:        "internal/handler/usertag/routes.go",
		Method:      "usertag.InternalRouteSpecs",
		Description: "注册内网用户标签路由",
		Routes:      usertaghandler.InternalRouteSpecs,
	},
	{
		Name:        "internal_auth",
		File:        "internal/handler/auth/routes.go",
		Method:      "auth.InternalRouteSpecs",
		Description: "注册内网认证自举路由",
		Routes:      authhandler.InternalRouteSpecs,
	},
	{
		Name:        "docs",
		File:        "internal/handler/docs/routes.go",
		Method:      "docs.RouteSpecs",
		Description: "注册 API 文档路由",
		Routes:      docshandler.RouteSpecs,
	},
}
