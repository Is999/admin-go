package shared

import (
	"net/http"

	"admin/internal/middleware"
	"admin/internal/svc"

	"github.com/Is999/go-utils/errors"
	"github.com/zeromicro/go-zero/rest"
)

// RouteAccess 描述路由的入口鉴权类型，供注册、契约和测试统一理解路由边界。
type RouteAccess string

const (
	// RouteAccessPublic 表示匿名可访问，或由业务自行完成临时凭证校验。
	RouteAccessPublic RouteAccess = "public"
	// RouteAccessAuth 表示后台登录态鉴权。
	RouteAccessAuth RouteAccess = "auth"
	// RouteAccessInternal 表示独立内网监听器的来源、Ops HMAC 和 nonce 鉴权入口。
	RouteAccessInternal RouteAccess = "internal"
	// RouteAccessDocs 表示文档会话 JWT 鉴权。
	RouteAccessDocs RouteAccess = "docs"
	// RouteAccessHealth 表示健康检查与指标入口。
	RouteAccessHealth RouteAccess = "health"
)

// RouteHandler 根据服务上下文构造真实 HTTP Handler。
type RouteHandler func(*svc.ServiceContext) http.HandlerFunc

// RouteSpec 是路由注册、访问边界、契约和访问日志策略的单一规格。
type RouteSpec struct {
	Module        string                // 路由模块名称，由默认模块聚合时写入
	Method        string                // HTTP 请求方法
	Path          string                // go-zero 路由路径
	Access        RouteAccess           // 入口鉴权类型
	Meta          RouteMeta             // 业务路由元数据，空值表示该路由不进入业务鉴权链
	Alias         middleware.RouteAlias // 非业务 RouteMeta 路由使用的权限/审计别名
	Description   string                // 路由中文说明
	SkipAccessLog bool                  // 是否跳过普通访问日志
	Handler       RouteHandler          // 真实 Handler 构造函数
}

// RouteAlias 返回路由鉴权和审计使用的稳定别名。
func (s RouteSpec) RouteAlias() middleware.RouteAlias {
	if s.Meta.Alias != "" {
		return s.Meta.Alias
	}
	return s.Alias
}

// Describe 返回路由中文说明。
func (s RouteSpec) Describe() string {
	if s.Meta.Describe != "" {
		return s.Meta.Describe
	}
	return s.Description
}

// RestRoute 将路由规格转换为 go-zero 路由；规格错误返回启动错误，禁止用 panic 终止进程。
func (s RouteSpec) RestRoute(svcCtx *svc.ServiceContext, authMw *middleware.AuthMiddleware, opsMw *middleware.OpsMiddleware) (rest.Route, error) {
	if s.Handler == nil {
		return rest.Route{}, errors.Errorf("路由规格缺少 Handler: %s %s", s.Method, s.Path)
	}
	if svcCtx == nil {
		return rest.Route{}, errors.Errorf("路由规格缺少 ServiceContext: %s %s", s.Method, s.Path)
	}
	handler := s.Handler(svcCtx)
	if handler == nil {
		return rest.Route{}, errors.Errorf("路由规格构造出空 Handler: %s %s", s.Method, s.Path)
	}
	alias := s.RouteAlias()
	switch s.Access {
	case RouteAccessAuth:
		if authMw == nil {
			return rest.Route{}, errors.Errorf("登录态路由缺少 AuthMiddleware: %s %s", s.Method, s.Path)
		}
		handler = authMw.Handle(handler, alias)
	case RouteAccessPublic:
		if alias != "" {
			if authMw == nil {
				return rest.Route{}, errors.Errorf("公开安全路由缺少 AuthMiddleware: %s %s", s.Method, s.Path)
			}
			handler = authMw.PublicHandle(handler, alias)
		}
	case RouteAccessInternal:
		if opsMw == nil {
			return rest.Route{}, errors.Errorf("内网路由缺少 OpsMiddleware: %s %s", s.Method, s.Path)
		}
		handler = opsMw.Handle(handler, alias)
	case RouteAccessDocs:
		handler = middleware.DocsJwtMiddleware(svcCtx)(handler)
	case RouteAccessHealth:
		// 健康探针由部署网络边界控制，不进入登录态或业务签名链。
	default:
		// 未知访问类型必须拒绝启动，避免拼写错误把受保护路由降级为匿名公网入口。
		return rest.Route{}, errors.Errorf("未知路由访问类型: %s", s.Access)
	}
	if s.SkipAccessLog {
		handler = middleware.SkipAccessLog(handler)
	}
	return rest.Route{Method: s.Method, Path: s.Path, Handler: handler}, nil
}

// AddRouteSpecs 按声明顺序转换并注册一组路由规格；任一规格无效时整组不写入 Server。
func AddRouteSpecs(server *rest.Server, svcCtx *svc.ServiceContext, authMw *middleware.AuthMiddleware, opsMw *middleware.OpsMiddleware, specs []RouteSpec) error {
	if server == nil {
		return errors.Errorf("注册路由规格时 HTTP Server 为空")
	}
	routes := make([]rest.Route, 0, len(specs))
	for _, spec := range specs {
		route, err := spec.RestRoute(svcCtx, authMw, opsMw)
		if err != nil {
			return errors.Tag(err)
		}
		routes = append(routes, route)
	}
	server.AddRoutes(routes)
	return nil
}
