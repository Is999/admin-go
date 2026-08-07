package auth

import (
	"admin/internal/handler/shared"
	"context"
	"net/http"

	i18n "admin/common/i18n"
	"admin/internal/audit"
	"admin/internal/infra/loggerx"
	adminlogic "admin/internal/logic/admin"
	messagelogic "admin/internal/logic/message"
	"admin/internal/model"
	"admin/internal/requestctx"
	"admin/internal/svc"
	"admin/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/rest/httpx"
)

// LoginCaptchaHandler 返回登录图形验证码。
func LoginCaptchaHandler(sCtx *svc.ServiceContext) http.HandlerFunc {
	handler := shared.RespHandlerFunc(func(r *http.Request) (shared.LogicObj, *types.BizResult) {
		requestctx.SetRoute(r.Context(), string(shared.AuthCaptcha.Alias))
		logicObj := adminlogic.NewAdminLogic(r, sCtx)
		return logicObj, logicObj.BuildLoginCaptcha()
	})
	return func(w http.ResponseWriter, r *http.Request) {
		setCaptchaNoStoreHeaders(w)
		handler(w, r)
	}
}

// setCaptchaNoStoreHeaders 禁止浏览器和代理缓存一次性登录验证码。
func setCaptchaNoStoreHeaders(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
}

// LoginHandler 处理管理员登录请求，并在成功后补齐当前请求的审计与用户上下文。
func LoginHandler(sCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 登录接口没有经过 auth 中间件，这里手动补齐统一路由别名，方便 access log 与审计对齐。
		requestctx.SetRoute(r.Context(), string(shared.AuthLogin.Alias))

		var req types.LoginReq
		if err := httpx.Parse(r, &req); err != nil {
			shared.WriteBizResponse(w, r, nil, types.ParamErrorResult(err), nil)
			return
		}
		req.IP = sCtx.ClientIP(r)
		logicObj := adminlogic.NewAdminLogic(r, sCtx)
		if captchaResp := logicObj.VerifyLoginCaptcha(req.Key, req.Captcha); captchaResp.IsFailure() {
			message := captchaResp.ResolveMessage(r.Context())
			recordAuthAudit(logicObj.Audit(), r.Context(), audit.Event{
				Action:   model.ActionAdminLogin,
				Route:    string(shared.AuthLogin.Alias),
				Method:   "LoginHandler",
				Describe: shared.AuthLogin.Describe,
				Data:     req,
				UserName: req.Username,
				IP:       req.IP,
			}, captchaResp, http.StatusOK, message)
			shared.WriteBizResponse(w, r, logicObj, captchaResp.WithReq(&req), nil)
			return
		}
		resp := logicObj.Login(&req).WithReq(&req)
		message := resp.ResolveMessage(r.Context())

		if resp.IsSuccess() {
			// 登录成功后把用户信息写回 request meta，保证本次请求的访问日志也能带上 user_id。
			if loginUser, ok := resp.Data.(*types.ProfileLoginResp); ok && loginUser.User != nil {
				requestctx.SetUser(r.Context(), loginUser.User.ID, req.Username, req.IP)
				// 登录成功后异步投递“管理员登录”消息；任务纳入 ServiceContext 停机等待，避免数据库先关闭导致消息丢失。
				sCtx.GoBackground(func() {
					messagelogic.EmitAdminLoginMessage(r.Context(), sCtx, loginUser.User.ID, req.Username, req.IP)
				})
			}
		}
		recordAuthAudit(logicObj.Audit(), r.Context(), audit.Event{
			Action:   model.ActionAdminLogin,
			Route:    string(shared.AuthLogin.Alias),
			Method:   "LoginHandler",
			Describe: shared.AuthLogin.Describe,
			Data:     req,
			UserName: req.Username,
			IP:       req.IP,
		}, resp, http.StatusOK, message)

		shared.WriteBizResponse(w, r, logicObj, resp, nil)
	}
}

// LogoutHandler 处理管理员登出请求，并统一记录登出审计事件。
func LogoutHandler(sCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 登出接口已经过 auth 中间件，但仍显式设置 route，避免后续审计依赖 handler 名称推断。
		requestctx.SetRoute(r.Context(), string(shared.AuthLogout.Alias))

		logicObj := adminlogic.NewAdminLogic(r, sCtx)
		ctxAdmin := logicObj.GetCtxAdmin()
		if ctxAdmin == nil || ctxAdmin.ID == 0 {
			shared.WriteBizResponse(w, r, logicObj, types.Unauthorized(i18n.MsgKeyNeedLogin).ToBizResult(), nil)
			return
		}

		resp := logicObj.Logout(ctxAdmin).WithReq(map[string]any{"username": ctxAdmin.Name})
		message := resp.ResolveMessage(r.Context())
		recordAuthAudit(logicObj.Audit(), r.Context(), audit.Event{
			Action:   model.ActionAdminLogout,
			Route:    string(shared.AuthLogout.Alias),
			Method:   "LogoutHandler",
			Describe: shared.AuthLogout.Describe,
			Data:     map[string]any{"username": ctxAdmin.Name},
			UserID:   ctxAdmin.ID,
			UserName: ctxAdmin.Name,
			IP:       ctxAdmin.IP,
		}, resp, http.StatusOK, message)

		shared.WriteBizResponse(w, r, logicObj, resp, nil)
	}
}

// recordAuthAudit 统一记录登录/登出的审计事件。
// 登录链路会先落审计再写响应，因此这里把 HTTP 状态码和业务码显式补齐，避免审计丢字段。
func recordAuthAudit(recorder *audit.Recorder, ctx context.Context, event audit.Event, resp *types.BizResult, httpStatus int, errorMessage string) {
	if recorder == nil || resp == nil {
		return
	}
	success := resp.IsSuccess()
	event.Success = &success
	event.HTTPStatus = httpStatus
	event.BizCode = resp.Code
	if !success {
		event.ErrorMessage = errorMessage
	}
	if err := recorder.Record(ctx, event); err != nil {
		loggerx.Errorw(ctx, "管理员认证审计投递失败", err,
			logx.Field("action", event.Action),
			logx.Field("route", event.Route),
		)
	}
}
