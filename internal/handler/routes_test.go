package handler

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"admin/internal/config"
	"admin/internal/handler/shared"
	"admin/internal/middleware"
	"admin/internal/svc"

	"github.com/Is999/go-utils/errors"
	"github.com/zeromicro/go-zero/rest"
)

// TestRegisterHandlersRegistersExpectedRoutes 确认裁剪业务模块后，主体框架路由仍然完整注册。
func TestRegisterHandlersRegistersExpectedRoutes(t *testing.T) {
	publicServer := rest.MustNewServer(rest.RestConf{
		Host: "127.0.0.1",
		Port: 0,
	})
	defer publicServer.Stop()
	internalServer := rest.MustNewServer(rest.RestConf{
		Host: "127.0.0.1",
		Port: 0,
	})
	defer internalServer.Stop()

	svcCtx := svc.NewServiceContext(config.Config{}, svc.Dependencies{})
	if err := RegisterPublicHandlers(publicServer, svcCtx); err != nil {
		t.Fatalf("注册公网路由失败: %v", err)
	}
	if err := RegisterInternalHandlers(internalServer, svcCtx); err != nil {
		t.Fatalf("注册内网路由失败: %v", err)
	}
	publicRoutes := publicServer.Routes()
	internalRoutes := internalServer.Routes()
	routes := append(publicRoutes, internalRoutes...)
	contracts := DefaultRouteContracts()
	if len(routes) != len(contracts) {
		t.Fatalf("expected %d routes, got %d", len(contracts), len(routes))
	}
	for _, route := range publicRoutes {
		if strings.HasPrefix(route.Path, "/internal/") {
			t.Fatalf("公网监听器注册了内网路由: %s %s", route.Method, route.Path)
		}
	}
	for _, route := range internalRoutes {
		if !strings.HasPrefix(route.Path, "/internal/") {
			t.Fatalf("内网监听器注册了公网路由: %s %s", route.Method, route.Path)
		}
	}

	routeSet := make(map[string]struct{}, len(routes))
	for _, route := range routes {
		key := route.Method + " " + route.Path
		if _, ok := routeSet[key]; ok {
			t.Fatalf("路由重复注册: %s", key)
		}
		routeSet[key] = struct{}{}
	}
	for _, contract := range contracts {
		if _, ok := routeSet[contract.Key()]; !ok {
			t.Fatalf("契约路由未注册: %s", contract.Key())
		}
	}

	taskDetailRoute := http.MethodGet + " /api/tasks/:taskId"
	for _, fixedRoute := range []string{
		http.MethodGet + " /api/tasks/overview",
		http.MethodGet + " /api/tasks/workflows",
		http.MethodGet + " /api/tasks/history",
		http.MethodGet + " /api/tasks/history/:id",
		http.MethodGet + " /api/tasks/failures",
		http.MethodGet + " /api/tasks/observability",
		http.MethodGet + " /api/tasks/queues",
		http.MethodGet + " /api/tasks/registry/task-types",
		http.MethodGet + " /api/tasks/registry/workflows",
		http.MethodGet + " /api/tasks/config-reload",
		http.MethodGet + " /api/tasks/config-reload/items",
	} {
		if routeIndex(routes, fixedRoute) > routeIndex(routes, taskDetailRoute) {
			t.Fatalf("固定任务路由 %s 必须排在参数路由 %s 之前", fixedRoute, taskDetailRoute)
		}
	}
}

// TestBuiltinRouteModuleSpecsValid 确保内置路由模块规格字段完整且能派生真实模块。
func TestBuiltinRouteModuleSpecsValid(t *testing.T) {
	specs := BuiltinRouteModuleSpecs()
	if len(specs) == 0 {
		t.Fatal("内置路由模块规格不能为空")
	}
	seen := make(map[string]struct{}, len(specs))
	for _, spec := range specs {
		if strings.TrimSpace(spec.Name) == "" {
			t.Fatalf("内置路由模块规格缺少名称: %+v", spec)
		}
		if strings.TrimSpace(spec.File) == "" || strings.TrimSpace(spec.Method) == "" || strings.TrimSpace(spec.Description) == "" {
			t.Fatalf("内置路由模块规格说明不完整: %+v", spec)
		}
		if !strings.HasPrefix(spec.File, "internal/handler/") || !strings.HasSuffix(spec.File, "/routes.go") || strings.Contains(spec.File, " + ") {
			t.Fatalf("内置路由模块规格文件说明不合法: %+v", spec)
		}
		if spec.Routes == nil {
			t.Fatalf("内置路由模块规格缺少路由规格函数: %s", spec.Name)
		}
		if len(spec.Routes()) == 0 {
			t.Fatalf("内置路由模块规格缺少路由: %s", spec.Name)
		}
		if _, ok := seen[spec.Name]; ok {
			t.Fatalf("内置路由模块规格名称重复: %s", spec.Name)
		}
		seen[spec.Name] = struct{}{}
	}
}

// TestDefaultRouteContractsAreSelfConsistent 确保契约清单具备最小治理信息。
func TestDefaultRouteContractsAreSelfConsistent(t *testing.T) {
	seen := make(map[string]struct{})
	for _, contract := range DefaultRouteContracts() {
		if contract.Module == "" {
			t.Fatalf("路由契约缺少模块: %+v", contract)
		}
		if contract.Method == "" || contract.Path == "" {
			t.Fatalf("路由契约缺少 method/path: %+v", contract)
		}
		if contract.Description == "" {
			t.Fatalf("路由契约缺少描述: %s", contract.Key())
		}
		if _, ok := seen[contract.Key()]; ok {
			t.Fatalf("路由契约重复: %s", contract.Key())
		}
		seen[contract.Key()] = struct{}{}
		if strings.HasPrefix(contract.Path, "/internal/") && contract.Access != RouteAccessInternal {
			t.Fatalf("内网路由必须标记 internal: %+v", contract)
		}
		if contract.Access == RouteAccessInternal && !strings.HasPrefix(contract.Path, "/internal/") {
			t.Fatalf("internal 契约路径必须使用 /internal 前缀: %+v", contract)
		}
		if contract.Access == RouteAccessDocs && !strings.HasPrefix(contract.Path, "/api/docs") {
			t.Fatalf("docs 契约路径必须使用 /api/docs 前缀: %+v", contract)
		}
	}
}

// TestDefaultRouteContractsAreDocumented 确保真实路由路径在接口文档和生成清单中可检索。
func TestDefaultRouteContractsAreDocumented(t *testing.T) {
	docs := readDocsSiteContent(t, filepath.Join("..", "..", "docs", "site"))
	for _, contract := range DefaultRouteContracts() {
		if !strings.Contains(docs, contract.Path) {
			t.Fatalf("接口文档缺少路由模板: %s", contract.Key())
		}
	}
}

// readDocsSiteContent 读取 Markdown 文档和由后端生成的 JSON 路由清单。
func readDocsSiteContent(t *testing.T, root string) string {
	t.Helper()
	var builder strings.Builder
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return errors.Tag(err)
		}
		extension := filepath.Ext(path)
		if entry.IsDir() || (extension != ".md" && extension != ".json") {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return errors.Tag(err)
		}
		builder.Write(body)
		builder.WriteByte('\n')
		return nil
	}); err != nil {
		t.Fatalf("读取接口文档失败: %v", err)
	}
	return builder.String()
}

// TestDefaultRouteContractsHaveAccessPolicyAliases 确保受保护路由具备明确权限或豁免边界。
func TestDefaultRouteContractsHaveAccessPolicyAliases(t *testing.T) {
	for _, contract := range DefaultRouteContracts() {
		switch contract.Access {
		case RouteAccessAuth, RouteAccessInternal:
			if strings.TrimSpace(contract.Alias) == "" {
				t.Fatalf("受保护路由缺少权限/审计别名: %+v", contract)
			}
		}
		if contract.Alias == string(middleware.Ignore) {
			continue
		}
		if strings.ContainsAny(contract.Alias, " \t\r\n") {
			t.Fatalf("路由别名不能包含空白字符: %+v", contract)
		}
	}
}

// TestDefaultRouteContractsSkipAccessLog 确保高频低价值路由才标记跳过普通访问日志。
func TestDefaultRouteContractsSkipAccessLog(t *testing.T) {
	skipRoutes := map[string]struct{}{
		http.MethodGet + " /api/live":                                     {},
		http.MethodGet + " /api/ready":                                    {},
		http.MethodGet + " /api/metrics":                                  {},
		http.MethodGet + " /api/admin-messages/notifications":             {},
		http.MethodGet + " /api/runtime-config/archive-jobs/:id/progress": {},
	}
	seen := make(map[string]struct{}, len(skipRoutes))
	for _, contract := range DefaultRouteContracts() {
		_, wantSkip := skipRoutes[contract.Key()]
		if contract.SkipAccessLog != wantSkip {
			t.Fatalf("路由 %s skip_access_log = %v, want %v", contract.Key(), contract.SkipAccessLog, wantSkip)
		}
		if wantSkip {
			seen[contract.Key()] = struct{}{}
		}
	}
	if len(seen) != len(skipRoutes) {
		t.Fatalf("跳过访问日志路由覆盖不完整: got=%v want=%v", seen, skipRoutes)
	}
}

// TestAuditRouteMetasAreContracted 确保带审计动作的路由元数据已进入公开路由契约。
func TestAuditRouteMetasAreContracted(t *testing.T) {
	aliases := make(map[string]struct{}, len(DefaultRouteContracts()))
	for _, contract := range DefaultRouteContracts() {
		if contract.Alias == "" || contract.Alias == string(middleware.Ignore) {
			continue
		}
		aliases[contract.Alias] = struct{}{}
	}
	for _, meta := range shared.DefaultRouteMetas() {
		if meta.Action == "" {
			continue
		}
		alias := string(meta.Alias)
		if _, ok := aliases[alias]; !ok {
			t.Fatalf("审计路由元数据未进入契约清单: %s", alias)
		}
	}
}

// routeIndex 返回路由在 go-zero 注册顺序中的下标，用于保护固定路由优先于参数路由。
func routeIndex(routes []rest.Route, routeKey string) int {
	for index, route := range routes {
		if route.Method+" "+route.Path == routeKey {
			return index
		}
	}
	return len(routes) + 1
}

// TestRegisterHandlersAppendsRouteModules 确保外部路由模块可以通过统一入口追加注册。
func TestRegisterHandlersAppendsRouteModules(t *testing.T) {
	server := rest.MustNewServer(rest.RestConf{
		Host: "127.0.0.1",
		Port: 0,
	})
	defer server.Stop()

	module := NewRouteModuleFunc("custom", func() []shared.RouteSpec {
		return []shared.RouteSpec{{
			Method: http.MethodGet,
			Path:   "/api/custom",
			Access: shared.RouteAccessPublic,
			Handler: func(*svc.ServiceContext) http.HandlerFunc {
				return func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusNoContent)
				}
			},
		}}
	})
	if err := RegisterPublicHandlers(server, svc.NewServiceContext(config.Config{}, svc.Dependencies{}), module); err != nil {
		t.Fatalf("注册扩展路由失败: %v", err)
	}

	routeSet := make(map[string]struct{}, len(server.Routes()))
	for _, route := range server.Routes() {
		routeSet[route.Method+" "+route.Path] = struct{}{}
	}
	if _, ok := routeSet[http.MethodGet+" /api/custom"]; !ok {
		t.Fatal("期望外部路由模块已注册")
	}
}

// TestCustomRouteModuleIsPartitionedByAccess 确保扩展模块也不能把内网路由注册到公网监听器。
func TestCustomRouteModuleIsPartitionedByAccess(t *testing.T) {
	publicServer := rest.MustNewServer(rest.RestConf{Host: "127.0.0.1", Port: 0})
	defer publicServer.Stop()
	internalServer := rest.MustNewServer(rest.RestConf{Host: "127.0.0.1", Port: 0})
	defer internalServer.Stop()
	module := NewRouteModuleFunc("custom-internal", func() []shared.RouteSpec {
		return []shared.RouteSpec{{
			Method:  http.MethodPost,
			Path:    "/internal/custom",
			Access:  shared.RouteAccessInternal,
			Handler: func(*svc.ServiceContext) http.HandlerFunc { return func(http.ResponseWriter, *http.Request) {} },
		}}
	})
	svcCtx := svc.NewServiceContext(config.Config{}, svc.Dependencies{})
	if err := RegisterPublicHandlersWithModules(publicServer, svcCtx, module); err != nil {
		t.Fatalf("注册公网扩展路由失败: %v", err)
	}
	if err := RegisterInternalHandlersWithModules(internalServer, svcCtx, module); err != nil {
		t.Fatalf("注册内网扩展路由失败: %v", err)
	}
	if routeExists(publicServer.Routes(), http.MethodPost+" /internal/custom") {
		t.Fatal("扩展内网路由不能注册到公网监听器")
	}
	if !routeExists(internalServer.Routes(), http.MethodPost+" /internal/custom") {
		t.Fatal("扩展内网路由应注册到内网监听器")
	}
}

// TestCustomRouteModuleRejectsUnknownAccess 确保扩展路由访问类型拼写错误时返回启动错误，而不是 panic 或降级为匿名路由。
func TestCustomRouteModuleRejectsUnknownAccess(t *testing.T) {
	server := rest.MustNewServer(rest.RestConf{Host: "127.0.0.1", Port: 0})
	defer server.Stop()
	module := NewRouteModuleFunc("custom-invalid-access", func() []shared.RouteSpec {
		return []shared.RouteSpec{{
			Method: http.MethodGet,
			Path:   "/api/custom-invalid-access",
			Access: shared.RouteAccess("auth-typo"),
			Handler: func(*svc.ServiceContext) http.HandlerFunc {
				return func(http.ResponseWriter, *http.Request) {}
			},
		}}
	})

	err := RegisterPublicHandlersWithModules(server, svc.NewServiceContext(config.Config{}, svc.Dependencies{}), module)
	if err == nil || !strings.Contains(err.Error(), "未知路由访问类型") {
		t.Fatalf("未知路由访问类型错误 = %v", err)
	}
	if routeExists(server.Routes(), http.MethodGet+" /api/custom-invalid-access") {
		t.Fatal("无效访问类型路由不能写入 HTTP Server")
	}
}

// routeExists 判断路由集合是否包含指定 method/path。
func routeExists(routes []rest.Route, want string) bool {
	for _, route := range routes {
		if route.Method+" "+route.Path == want {
			return true
		}
	}
	return false
}
