package builtin

import (
	"context"

	core "admin/internal/bootstrap/components"
	"admin/internal/bootstrap/register"
	"admin/internal/bootstrap/runmode"
	"admin/internal/handler"

	"github.com/Is999/go-utils/errors"
	"github.com/zeromicro/go-zero/rest"
)

// newHTTPServer 创建 HTTP 服务与路由启动组件。
func newHTTPServer() core.Component {
	return core.NewFunc(NameHTTPServer, func(ctx context.Context, state *core.State) error {
		_ = ctx
		if state == nil || state.ServiceContext == nil {
			return errors.Errorf("HTTP 服务组件缺少服务上下文")
		}
		if !runmode.Has(state.Mode, runmode.API) {
			return nil
		}

		restConf := state.Config.RestConf
		// 项目已接入自定义 access log 中间件，关闭 go-zero 默认 HTTP 日志，避免高频路由产生双份日志。
		restConf.Middlewares.Log = false
		server, err := rest.NewServer(restConf)
		if err != nil {
			return errors.Wrapf(err, "创建 HTTP 服务失败 host=%s port=%d", restConf.Host, restConf.Port)
		}
		internalServer, err := newInternalServer(state.Config)
		if err != nil {
			return errors.Tag(err)
		}
		// HTTP 路由模块从统一规格解析，handler 只负责按给定模块完成注册。
		routeModules := handler.ComposeRouteModules(handler.BuiltinRouteModules(), state.RouteModules)
		if err := register.ValidateNamesUnique(register.KindRoute, register.RouteModuleNames(routeModules)); err != nil {
			return errors.Tag(err)
		}
		if err := handler.RegisterPublicHandlersWithModules(server, state.ServiceContext, routeModules...); err != nil {
			return errors.Wrap(err, "注册公网 HTTP 路由失败")
		}
		if err := handler.RegisterInternalHandlersWithModules(internalServer, state.ServiceContext, routeModules...); err != nil {
			return errors.Wrap(err, "注册内网 HTTP 路由失败")
		}
		state.Server = server
		state.InternalServer = internalServer
		return nil
	})
}
