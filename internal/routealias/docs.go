package routealias

import (
	"net/url"
	pathpkg "path"
	"strings"
)

const (
	// DocSiteAdmin 表示 Admin 后台文档站。
	DocSiteAdmin = "admin"
	// DocSiteAPI 表示 API 服务文档站。
	DocSiteAPI = "api"
	// docResourceKeySeparator 分隔文档站点与路径；文件路径不能包含 NUL，因而可以无歧义解析。
	docResourceKeySeparator = "\x00"
)

// DocResource 是文档权限使用的稳定资源键。
type DocResource struct {
	Site string // 文档所属站点
	Path string // 站点内 Markdown 相对路径
}

// Key 返回文档权限使用的稳定资源键，非法资源返回空字符串。
func (r DocResource) Key() string {
	site := strings.TrimSpace(r.Site)
	path := strings.TrimSpace(r.Path)
	if (site != DocSiteAdmin && site != DocSiteAPI) || path == "" || strings.ContainsRune(path, '\x00') {
		return ""
	}
	return site + docResourceKeySeparator + path
}

// ParseDocResourceKey 解析稳定文档资源键，格式或站点非法时返回 false。
func ParseDocResourceKey(value string) (DocResource, bool) {
	site, path, ok := strings.Cut(value, docResourceKeySeparator)
	resource := DocResource{Site: strings.TrimSpace(site), Path: strings.TrimSpace(path)}
	if !ok || resource.Key() != value {
		return DocResource{}, false
	}
	return resource, true
}

// docsResources 维护当前文档站可直接阅读的 Markdown 文档；docsify 公共资源不进入正文授权。
var docsResources = []DocResource{
	{Site: DocSiteAdmin, Path: "功能模块/任务系统/任务系统使用手册.md"},
	{Site: DocSiteAdmin, Path: "功能模块/用户标签/任务系统与用户标签排障手册.md"},
	{Site: DocSiteAdmin, Path: "功能模块/用户标签/用户标签实现与调度说明.md"},
	{Site: DocSiteAdmin, Path: "功能模块/用户标签/用户标签操作手册.md"},
	{Site: DocSiteAdmin, Path: "接口文档/任务系统/任务列表接口.md"},
	{Site: DocSiteAdmin, Path: "接口文档/任务系统/任务总控接口.md"},
	{Site: DocSiteAdmin, Path: "接口文档/任务系统/任务注册表接口.md"},
	{Site: DocSiteAdmin, Path: "接口文档/任务系统/任务监控接口.md"},
	{Site: DocSiteAdmin, Path: "接口文档/任务系统/任务系统接口.md"},
	{Site: DocSiteAdmin, Path: "接口文档/任务系统/任务队列接口.md"},
	{Site: DocSiteAdmin, Path: "接口文档/任务系统/收集器任务接口.md"},
	{Site: DocSiteAdmin, Path: "接口文档/任务系统/运行配置管理接口.md"},
	{Site: DocSiteAdmin, Path: "接口文档/后台系统/个人信息接口.md"},
	{Site: DocSiteAdmin, Path: "接口文档/后台系统/内网初始化管理员接口.md"},
	{Site: DocSiteAdmin, Path: "接口文档/后台系统/前台用户管理接口.md"},
	{Site: DocSiteAdmin, Path: "接口文档/后台系统/后台系统接口.md"},
	{Site: DocSiteAdmin, Path: "接口文档/后台系统/基础规范与认证接口.md"},
	{Site: DocSiteAdmin, Path: "接口文档/后台系统/安全调试接口.md"},
	{Site: DocSiteAdmin, Path: "接口文档/后台系统/接口文档访问接口.md"},
	{Site: DocSiteAdmin, Path: "接口文档/后台系统/文件传输接口.md"},
	{Site: DocSiteAdmin, Path: "接口文档/后台系统/日志管理接口.md"},
	{Site: DocSiteAdmin, Path: "接口文档/后台系统/权限管理接口.md"},
	{Site: DocSiteAdmin, Path: "接口文档/后台系统/消息中心接口.md"},
	{Site: DocSiteAdmin, Path: "接口文档/后台系统/秘钥管理接口.md"},
	{Site: DocSiteAdmin, Path: "接口文档/后台系统/管理员管理接口.md"},
	{Site: DocSiteAdmin, Path: "接口文档/后台系统/系统配置接口.md"},
	{Site: DocSiteAdmin, Path: "接口文档/后台系统/缓存管理接口.md"},
	{Site: DocSiteAdmin, Path: "接口文档/后台系统/角色管理接口.md"},
	{Site: DocSiteAdmin, Path: "接口文档/接口文档统一规范.md"},
	{Site: DocSiteAdmin, Path: "接口文档/接口文档首页.md"},
	{Site: DocSiteAdmin, Path: "接口文档/用户标签/内网接口.md"},
	{Site: DocSiteAdmin, Path: "接口文档/用户标签/指定标签重算接口.md"},
	{Site: DocSiteAdmin, Path: "接口文档/用户标签/用户标签工作流接口.md"},
	{Site: DocSiteAdmin, Path: "接口文档/用户标签/用户标签接口.md"},
	{Site: DocSiteAdmin, Path: "文档首页.md"},
	{Site: DocSiteAdmin, Path: "角色文档/前端与测试/前端权限码与管理员权限映射说明.md"},
	{Site: DocSiteAdmin, Path: "角色文档/前端与测试/接口联调与验收说明.md"},
	{Site: DocSiteAdmin, Path: "角色文档/后端开发/AI开发提示词.md"},
	{Site: DocSiteAdmin, Path: "角色文档/后端开发/AI开发规范.md"},
	{Site: DocSiteAdmin, Path: "角色文档/后端开发/任务队列与工作流指南.md"},
	{Site: DocSiteAdmin, Path: "角色文档/后端开发/多因素认证与管理员角色规则.md"},
	{Site: DocSiteAdmin, Path: "角色文档/后端开发/库表路由规范.md"},
	{Site: DocSiteAdmin, Path: "角色文档/后端开发/开发扩展指南.md"},
	{Site: DocSiteAdmin, Path: "角色文档/后端开发/系统组件功能说明.md"},
	{Site: DocSiteAdmin, Path: "角色文档/后端开发/组件注册清单.md"},
	{Site: DocSiteAdmin, Path: "角色文档/后端开发/配置字段说明.md"},
	{Site: DocSiteAdmin, Path: "角色文档/运维/数据库迁移治理.md"},
	{Site: DocSiteAdmin, Path: "角色文档/运维/部署发布指南.md"},
	{Site: DocSiteAPI, Path: "文档首页.md"},
	{Site: DocSiteAPI, Path: "接口文档/前台系统/健康检查接口.md"},
	{Site: DocSiteAPI, Path: "接口文档/前台系统/用户接口.md"},
	{Site: DocSiteAPI, Path: "接口文档/前台系统/系统接口.md"},
	{Site: DocSiteAPI, Path: "接口文档/前台系统/认证接口.md"},
	{Site: DocSiteAPI, Path: "接口文档/接口文档统一规范.md"},
	{Site: DocSiteAPI, Path: "角色文档/后端开发/AI开发提示词.md"},
	{Site: DocSiteAPI, Path: "角色文档/后端开发/AI开发规范.md"},
	{Site: DocSiteAPI, Path: "角色文档/后端开发/前端安全清单同步.md"},
	{Site: DocSiteAPI, Path: "角色文档/后端开发/开发扩展指南.md"},
	{Site: DocSiteAPI, Path: "角色文档/后端开发/组件注册清单.md"},
	{Site: DocSiteAPI, Path: "角色文档/后端开发/认证安全指标与告警.md"},
	{Site: DocSiteAPI, Path: "角色文档/运维/数据库迁移治理.md"},
	{Site: DocSiteAPI, Path: "角色文档/运维/部署发布指南.md"},
}

// DocsResources 返回全部受保护文档资源。
func DocsResources() []DocResource {
	result := make([]DocResource, len(docsResources))
	copy(result, docsResources)
	return result
}

// DocsResourceForPath 按文档站请求路径返回精确文档资源；公共资源和未知文档返回 false。
func DocsResourceForPath(requestPath string) (DocResource, bool) {
	docsPath, ok := NormalizeDocsRequestPath(requestPath)
	if !ok {
		return DocResource{}, false
	}
	return docsResourceFromAssetPath(docsPath)
}

// DocsResourceForAssetPath 按 docsify 站点基路径和资源路径返回精确文档资源。
func DocsResourceForAssetPath(basePath string, assetPath string) (DocResource, bool) {
	parts := []string{
		strings.Trim(strings.TrimSpace(basePath), "/"),
		strings.Trim(strings.TrimSpace(assetPath), "/"),
	}
	return docsResourceFromAssetPath(strings.Trim(strings.Join(parts, "/"), "/"))
}

// DocsEntryAliasForPath 返回请求所属文档站的入口路由权限。
func DocsEntryAliasForPath(requestPath string) Alias {
	docsPath, _ := NormalizeDocsRequestPath(requestPath)
	// Admin 与 API 文档站共用 docsify 静态资源，只校验登录安全态，不能反向要求 Admin 文档入口权限。
	if strings.HasPrefix(docsPath, "vendor/") {
		return Ignore
	}
	if docsPath == "api" || strings.HasPrefix(docsPath, "api/") {
		return DocsAPIServiceIndex
	}
	return DocsIndex
}

// DocsPathNeedsResourcePermission 判断请求是否是必须精确授权的 Markdown 正文。
func DocsPathNeedsResourcePermission(requestPath string) bool {
	docsPath, ok := NormalizeDocsRequestPath(requestPath)
	if !ok {
		// 非法路径必须进入精确资源校验并失败关闭，不能降级成文档站公共资源。
		return true
	}
	if docsPath == "" {
		return false
	}
	name := pathpkg.Base(docsPath)
	switch strings.ToLower(name) {
	case "_sidebar.md", "_navbar.md", "404.md":
		return false
	default:
		return strings.EqualFold(pathpkg.Ext(name), ".md")
	}
}

// docsResourceFromAssetPath 把聚合文档路径转换为 site + path 资源键。
func docsResourceFromAssetPath(assetPath string) (DocResource, bool) {
	docsPath := normalizedDocsAssetPath(assetPath)
	if docsPath == "" {
		return DocResource{}, false
	}
	resource := DocResource{Site: DocSiteAdmin, Path: docsPath}
	if strings.HasPrefix(docsPath, "api/") {
		resource.Site = DocSiteAPI
		resource.Path = strings.TrimPrefix(docsPath, "api/")
	}
	for _, item := range docsResources {
		if item == resource {
			return resource, true
		}
	}
	return DocResource{}, false
}

// NormalizeDocsRequestPath 返回 /api/docs 下的规范相对路径，非法路径返回 false。
func NormalizeDocsRequestPath(requestPath string) (string, bool) {
	if strings.TrimSpace(requestPath) == "" {
		return "", false
	}
	cleanPath, ok := cleanDocsPath(requestPath)
	if !ok {
		return "", false
	}
	if cleanPath == "/api/docs" || cleanPath == "/api/docs/" {
		return "", true
	}
	if !strings.HasPrefix(cleanPath, "/api/docs/") {
		return "", false
	}
	return strings.TrimPrefix(cleanPath, "/api/docs/"), true
}

// normalizedDocsAssetPath 规整文档站内部资源路径。
func normalizedDocsAssetPath(assetPath string) string {
	cleanPath, ok := cleanDocsPath(assetPath)
	if !ok || cleanPath == "/" {
		return ""
	}
	return strings.TrimPrefix(cleanPath, "/")
}

// cleanDocsPath 解码并规整文档路径，拒绝点段、反斜杠和空字节。
func cleanDocsPath(value string) (string, bool) {
	decoded, err := url.PathUnescape(strings.TrimSpace(value))
	if err != nil {
		return "", false
	}
	decoded = strings.TrimSpace(decoded)
	if strings.Contains(decoded, "\\") || strings.ContainsRune(decoded, '\x00') {
		return "", false
	}
	for _, segment := range strings.Split(decoded, "/") {
		if segment == "." || segment == ".." {
			return "", false
		}
	}
	return pathpkg.Clean("/" + strings.TrimLeft(decoded, "/")), true
}
