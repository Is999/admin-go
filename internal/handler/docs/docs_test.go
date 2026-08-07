package docs

import (
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	sitedocs "admin/docs"
	"admin/internal/config"
	"admin/internal/handler/shared"
	"admin/internal/middleware"
	"admin/internal/requestctx"
	"admin/internal/routealias"
	"admin/internal/svc"
)

// TestDocsSessionRouteUsesLoginOnlyPermission 校验文档会话只要求登录态，具体文档权限由资源路由校验。
func TestDocsSessionRouteUsesLoginOnlyPermission(t *testing.T) {
	for _, spec := range RouteSpecs() {
		if spec.Method != http.MethodPost || spec.Path != "/api/docs/session" {
			continue
		}
		if spec.Access != shared.RouteAccessAuth {
			t.Fatalf("docs session access = %q, want %q", spec.Access, shared.RouteAccessAuth)
		}
		if spec.RouteAlias() != routealias.Ignore {
			t.Fatalf("docs session alias = %q, want %q", spec.RouteAlias(), routealias.Ignore)
		}
		return
	}
	t.Fatal("docs session route not found")
}

// TestDocsSessionCookieLimitsScope 校验文档会话 cookie 只挂在文档路径下。
func TestDocsSessionCookieLimitsScope(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api/docs/session", nil)
	cookie := docsSessionCookie(req, "token-1")

	if cookie.Name != middleware.DocsSessionCookieName {
		t.Fatalf("cookie name = %q, want %q", cookie.Name, middleware.DocsSessionCookieName)
	}
	if cookie.Path != middleware.DocsSessionCookiePath {
		t.Fatalf("cookie path = %q, want %q", cookie.Path, middleware.DocsSessionCookiePath)
	}
	if !cookie.HttpOnly {
		t.Fatal("docs session cookie must be HttpOnly")
	}
	if cookie.SameSite != http.SameSiteLaxMode {
		t.Fatalf("cookie SameSite = %v, want %v", cookie.SameSite, http.SameSiteLaxMode)
	}
}

// TestDocsSessionCookieSecureBehindProxy 校验 HTTPS 代理头会启用 Secure 属性。
func TestDocsSessionCookieSecureBehindProxy(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api/docs/session", nil)
	req.Header.Set("X-Forwarded-Proto", "https")

	if !docsSessionCookie(req, "token-1").Secure {
		t.Fatal("docs session cookie should be Secure when X-Forwarded-Proto=https")
	}
}

// TestDocsSessionHandlerSetsCookie 校验已鉴权请求会写入文档会话 cookie。
func TestDocsSessionHandlerSetsCookie(t *testing.T) {
	ctx, _ := requestctx.New(httptest.NewRequest(http.MethodPost, "/api/docs/session", nil).Context())
	requestctx.SetAccessToken(ctx, "token-1")
	req := httptest.NewRequest(http.MethodPost, "/api/docs/session", nil).WithContext(ctx)
	recorder := httptest.NewRecorder()

	DocsSessionHandler()(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("http status = %d, want %d", recorder.Code, http.StatusOK)
	}
	cookies := recorder.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatalf("cookie count = %d, want 1", len(cookies))
	}
	if cookies[0].Name != middleware.DocsSessionCookieName || cookies[0].Value != "token-1" {
		t.Fatalf("unexpected docs cookie: %+v", cookies[0])
	}
}

// TestDocsIndexSearchUsesFilteredNavigation 校验后台文档搜索只使用已过滤导航里的文档路径。
func TestDocsIndexSearchUsesFilteredNavigation(t *testing.T) {
	content, err := fs.ReadFile(sitedocs.FS, "site/index.html")
	if err != nil {
		t.Fatalf("docs index asset missing: %v", err)
	}
	if strings.Contains(string(content), "docs.unshift({ path: '文档首页.md'") {
		t.Fatal("docs search index must not reinsert filtered homepage doc")
	}
}

// TestAPIDocsProxyPath 校验后台文档站 API 前缀会转换为 API 内网文档路径。
func TestAPIDocsProxyPath(t *testing.T) {
	tests := []struct {
		name     string // name 表示测试场景名称。
		path     string // path 表示请求路径。
		wantPath string // wantPath 表示期望重定向路径。
		wantOK   bool   // wantOK 表示期望是否成功。
	}{
		{name: "代理根路径", path: "/api/docs/api", wantOK: false},
		{name: "代理侧边栏", path: "/api/docs/api/_sidebar.md", wantPath: "/_sidebar.md", wantOK: true},
		{name: "接口规范", path: "/api/docs/api/接口文档/接口文档统一规范.md", wantPath: "/接口文档/接口文档统一规范.md", wantOK: true},
		{name: "编码路径", path: "/api/docs/api/%E6%8E%A5%E5%8F%A3%E6%96%87%E6%A1%A3/%E5%89%8D%E5%8F%B0%E7%B3%BB%E7%BB%9F/%E8%AE%A4%E8%AF%81%E6%8E%A5%E5%8F%A3.md", wantPath: "/接口文档/前台系统/认证接口.md", wantOK: true},
		{name: "角色文档", path: "/api/docs/api/角色文档/后端开发/AI开发规范.md", wantPath: "/角色文档/后端开发/AI开发规范.md", wantOK: true},
		{name: "非代理路径", path: "/api/docs/接口文档/后台系统/权限管理接口.md", wantOK: false},
		{name: "路径穿越", path: "/api/docs/api/../角色文档/后端开发/AI开发规范.md", wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPath, gotOK := apiDocsProxyPath(tt.path)
			if gotOK != tt.wantOK || gotPath != tt.wantPath {
				t.Fatalf("apiDocsProxyPath() = (%q, %v), want (%q, %v)", gotPath, gotOK, tt.wantPath, tt.wantOK)
			}
		})
	}
}

// TestAPIDocsIndexStoredAsStaticAsset 校验前台 API 文档入口由 docs 静态资源维护。
func TestAPIDocsIndexStoredAsStaticAsset(t *testing.T) {
	content, err := fs.ReadFile(sitedocs.FS, "site/"+apiDocsIndexAssetPath)
	if err != nil {
		t.Fatalf("api docs index asset missing: %v", err)
	}
	for _, text := range []string{
		"前台 API 文档",
		"basePath: '/api/docs/api/'",
		"docsify.min.js",
		"installLazySearchBox",
		"startSearchIndex",
		"window.fetch(docFetchURL",
		"return docsBasePath +",
		"ai-prompt-copy-button",
		"copyTextToClipboard",
		"handlePromptCopyClick",
		"document.addEventListener('click', handlePromptCopyClick, true)",
	} {
		if !strings.Contains(string(content), text) {
			t.Fatalf("api docs index asset missing %q", text)
		}
	}
	for _, text := range []string{"search.min.js", "cdn.jsdelivr.net"} {
		if strings.Contains(string(content), text) {
			t.Fatalf("api docs index must not depend on blocking search plugin or CDN %q", text)
		}
	}
	if strings.Contains(string(content), "docs.unshift({ path: homeDocPath") {
		t.Fatal("api docs search index must not reinsert filtered homepage doc")
	}
}

// TestAPIDocsIndexStandalone 校验前台 API 文档入口不复用 Admin 文档导航。
func TestAPIDocsIndexStandalone(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/docs/api", nil)
	recorder := httptest.NewRecorder()

	DocsSiteHandler(nil)(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("http status = %d, want %d", recorder.Code, http.StatusOK)
	}
	body := recorder.Body.String()
	for _, text := range []string{"前台 API 文档", "basePath: '/api/docs/api/'", "文档首页.md"} {
		if !strings.Contains(body, text) {
			t.Fatalf("api docs index missing %q", text)
		}
	}
	for _, text := range []string{"后台系统", "角色文档", "功能模块"} {
		if strings.Contains(body, text) {
			t.Fatalf("api docs index should not contain admin nav %q", text)
		}
	}
}

// TestAPIDocsSidebarRequiresAPIProxy 校验前台 API 侧边栏不再由 Admin 本地硬编码输出。
func TestAPIDocsSidebarRequiresAPIProxy(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/docs/api/_sidebar.md", nil)
	recorder := httptest.NewRecorder()

	DocsSiteHandler(nil)(recorder, req)
	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("http status = %d, want %d", recorder.Code, http.StatusBadGateway)
	}
}

// TestDocsAccessForRequestNonProdAnonymousDenied 校验开发和测试环境无登录身份时不会输出全量导航。
func TestDocsAccessForRequestNonProdAnonymousDenied(t *testing.T) {
	for _, mode := range []string{"dev", "test"} {
		t.Run(mode, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/docs/_sidebar.md", nil)
			cfg := config.Config{}
			cfg.Mode = mode
			svcCtx := svc.NewServiceContext(cfg, svc.Dependencies{})

			access, err := docsAccessForRequest(req, svcCtx)
			if err != nil {
				t.Fatalf("docsAccessForRequest returned error: %v", err)
			}
			if len(access.resources) != 0 {
				t.Fatalf("%s anonymous docs request should be denied, got %+v", mode, access)
			}
		})
	}
}

// TestDocsAccessForRequestProdAnonymousDenied 校验生产环境无登录身份时不会输出全量导航。
func TestDocsAccessForRequestProdAnonymousDenied(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/docs/_sidebar.md", nil)
	cfg := config.Config{}
	cfg.Mode = "pro"
	svcCtx := svc.NewServiceContext(cfg, svc.Dependencies{})

	access, err := docsAccessForRequest(req, svcCtx)
	if err != nil {
		t.Fatalf("docsAccessForRequest returned error: %v", err)
	}
	if len(access.resources) != 0 {
		t.Fatalf("prod anonymous docs request should be denied, got %+v", access)
	}
}

// TestDocsAccessForRequestFailsWithoutServiceContext 校验文档权限依赖缺失时失败关闭。
func TestDocsAccessForRequestFailsWithoutServiceContext(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/docs/_sidebar.md", nil)
	if _, err := docsAccessForRequest(req, nil); err == nil {
		t.Fatal("docsAccessForRequest() error = nil, want service context error")
	}
}

// TestFilterDocsNavigationByAccess 校验后台文档导航只展示当前账号有单篇权限的文件。
func TestFilterDocsNavigationByAccess(t *testing.T) {
	content, err := fs.ReadFile(sitedocs.FS, "site/"+docsSidebarAssetPath)
	if err != nil {
		t.Fatalf("read sidebar asset: %v", err)
	}
	access := docsAccessSet{resources: map[routealias.DocResource]struct{}{
		{Site: routealias.DocSiteAdmin, Path: "文档首页.md"}:             {},
		{Site: routealias.DocSiteAdmin, Path: "接口文档/接口文档统一规范.md"}:    {},
		{Site: routealias.DocSiteAdmin, Path: "角色文档/后端开发/AI开发规范.md"}: {},
	}}

	body := string(filterDocsNavigation(content, "", access))
	for _, text := range []string{"文档首页", "接口文档统一规范", "AI开发规范"} {
		if !strings.Contains(body, text) {
			t.Fatalf("filtered sidebar missing allowed text %q\n%s", text, body)
		}
	}
	for _, text := range []string{"部署发布指南", "运行与操作手册", "后台系统接口总览", "用户标签接口总览"} {
		if strings.Contains(body, text) {
			t.Fatalf("filtered sidebar contains forbidden text %q\n%s", text, body)
		}
	}
}

// TestFilterAPIDocsNavigationByAccess 校验前台 API 代理文档导航按 API 文档权限裁剪。
func TestFilterAPIDocsNavigationByAccess(t *testing.T) {
	content := []byte(`- 前台 API 文档
  - 接口文档
    - [接口文档统一规范](接口文档/接口文档统一规范.md)
    - 前台系统
      - [系统接口](接口文档/前台系统/系统接口.md)
  - 角色文档
    - 后端开发
      - [AI开发规范](角色文档/后端开发/AI开发规范.md)
`)
	access := docsAccessSet{resources: map[routealias.DocResource]struct{}{
		{Site: routealias.DocSiteAPI, Path: "接口文档/接口文档统一规范.md"}:    {},
		{Site: routealias.DocSiteAPI, Path: "角色文档/后端开发/AI开发规范.md"}: {},
	}}

	body := string(filterDocsNavigation(content, apiDocsProxyBasePath, access))
	for _, text := range []string{"接口文档统一规范", "AI开发规范"} {
		if !strings.Contains(body, text) {
			t.Fatalf("filtered api sidebar missing allowed text %q\n%s", text, body)
		}
	}
	if strings.Contains(body, "系统接口") {
		t.Fatalf("filtered api sidebar contains forbidden front api doc\n%s", body)
	}
}
