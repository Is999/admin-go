package routealias

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
)

// TestDocResourceKeyRoundTrip 验证文档权限资源键可无歧义编码和解析。
func TestDocResourceKeyRoundTrip(t *testing.T) {
	resource := DocResource{Site: DocSiteAdmin, Path: "接口文档/后台系统/角色管理接口.md"}
	key := resource.Key()
	parsed, ok := ParseDocResourceKey(key)
	if !ok || parsed != resource {
		t.Fatalf("ParseDocResourceKey(%q) = %v, %v; want %v, true", key, parsed, ok, resource)
	}
	for _, invalid := range []string{"", "unknown\x00path.md", DocSiteAdmin + "\x00"} {
		if _, ok := ParseDocResourceKey(invalid); ok {
			t.Fatalf("ParseDocResourceKey(%q) ok = true, want false", invalid)
		}
	}
}

// TestDocsEntryAliasForPath 验证后台与 API 文档分别使用自己的入口路由权限。
func TestDocsEntryAliasForPath(t *testing.T) {
	cases := []struct {
		name string // name 表示测试场景名称。
		path string // path 表示请求路径。
		want Alias  // want 表示期望入口权限。
	}{
		{name: "后台首页", path: "/api/docs", want: DocsIndex},
		{name: "后台公共资源", path: "/api/docs/_sidebar.md", want: DocsIndex},
		{name: "后台正文", path: "/api/docs/文档首页.md", want: DocsIndex},
		{name: "共享静态资源", path: "/api/docs/vendor/docsify/docsify.min.js", want: Ignore},
		{name: "API 入口", path: "/api/docs/api", want: DocsAPIServiceIndex},
		{name: "API 公共资源", path: "/api/docs/api/_sidebar.md", want: DocsAPIServiceIndex},
		{name: "API 正文", path: "/api/docs/api/接口文档/前台系统/认证接口.md", want: DocsAPIServiceIndex},
		{name: "路径穿越回退", path: "/api/docs/../secret.md", want: DocsIndex},
	}
	for _, tc := range cases {
		if got := DocsEntryAliasForPath(tc.path); got != tc.want {
			t.Fatalf("%s DocsEntryAliasForPath(%q) = %q, want %q", tc.name, tc.path, got, tc.want)
		}
	}
}

// TestDocsResourceForPath 验证只有清单内 Markdown 才映射为精确文档资源。
func TestDocsResourceForPath(t *testing.T) {
	cases := []struct {
		name string      // name 表示测试场景名称。
		path string      // path 表示请求路径。
		want DocResource // want 表示期望文档资源。
		ok   bool        // ok 表示是否应该命中资源。
	}{
		{name: "后台文档", path: "/api/docs/角色文档/后端开发/AI开发规范.md", want: DocResource{Site: DocSiteAdmin, Path: "角色文档/后端开发/AI开发规范.md"}, ok: true},
		{name: "API 文档", path: "/api/docs/api/接口文档/前台系统/认证接口.md", want: DocResource{Site: DocSiteAPI, Path: "接口文档/前台系统/认证接口.md"}, ok: true},
		{name: "编码路径", path: "/api/docs/%E6%8E%A5%E5%8F%A3%E6%96%87%E6%A1%A3/%E5%90%8E%E5%8F%B0%E7%B3%BB%E7%BB%9F/%E6%9D%83%E9%99%90%E7%AE%A1%E7%90%86%E6%8E%A5%E5%8F%A3.md", want: DocResource{Site: DocSiteAdmin, Path: "接口文档/后台系统/权限管理接口.md"}, ok: true},
		{name: "公共资源", path: "/api/docs/_sidebar.md", ok: false},
		{name: "未知正文", path: "/api/docs/不存在.md", ok: false},
		{name: "编码路径穿越", path: "/api/docs/%2e%2e/文档首页.md", ok: false},
	}
	for _, tc := range cases {
		got, ok := DocsResourceForPath(tc.path)
		if ok != tc.ok || got != tc.want {
			t.Fatalf("%s DocsResourceForPath(%q) = (%+v, %t), want (%+v, %t)", tc.name, tc.path, got, ok, tc.want, tc.ok)
		}
	}
}

// TestNormalizeDocsRequestPathRejectsDoubleEncodedTraversal 验证 HTTP 首次解码后的二次编码点段会被拒绝。
func TestNormalizeDocsRequestPathRejectsDoubleEncodedTraversal(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/docs/%252e%252e/文档首页.md", nil)
	if _, ok := NormalizeDocsRequestPath(req.URL.Path); ok {
		t.Fatalf("NormalizeDocsRequestPath(%q) ok = true, want false", req.URL.Path)
	}
	if !DocsPathNeedsResourcePermission(req.URL.Path) {
		t.Fatalf("DocsPathNeedsResourcePermission(%q) = false, want fail-closed true", req.URL.Path)
	}
}

// TestDocsResourceForAssetPath 验证聚合导航能够按站点基路径解析文档资源。
func TestDocsResourceForAssetPath(t *testing.T) {
	got, ok := DocsResourceForAssetPath("api", "接口文档/接口文档统一规范.md")
	want := DocResource{Site: DocSiteAPI, Path: "接口文档/接口文档统一规范.md"}
	if !ok || got != want {
		t.Fatalf("DocsResourceForAssetPath() = (%+v, %t), want (%+v, true)", got, ok, want)
	}
}

// TestDocsResourceForAPIHomepage 验证 API 独立首页使用精确正文权限，而不是只依赖入口权限。
func TestDocsResourceForAPIHomepage(t *testing.T) {
	got, ok := DocsResourceForAssetPath("api", "文档首页.md")
	want := DocResource{Site: DocSiteAPI, Path: "文档首页.md"}
	if !ok || got != want {
		t.Fatalf("DocsResourceForAssetPath() = (%+v, %t), want (%+v, true)", got, ok, want)
	}
}

// TestDocsResourcesMatchWorkspace 确保两个仓库的 Markdown 文档都纳入精确权限清单。
func TestDocsResourcesMatchWorkspace(t *testing.T) {
	adminRoot := routealiasTestAdminRoot(t)
	apiRoot := routealiasTestSiblingRoot(t, adminRoot, "api-go")
	got := docResourceKeys(DocsResources())
	want := docResourceKeys(append(
		workspaceDocsResources(t, filepath.Join(adminRoot, "docs/site"), DocSiteAdmin),
		workspaceDocsResources(t, filepath.Join(apiRoot, "docs/site"), DocSiteAPI)...,
	))
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("DocsResources() mismatch\n got=%v\nwant=%v", got, want)
	}
}

// docResourceKeys 把文档资源转换为可稳定比较的排序键。
func docResourceKeys(resources []DocResource) []string {
	keys := make([]string, 0, len(resources))
	for _, resource := range resources {
		keys = append(keys, resource.Site+"\x00"+resource.Path)
	}
	sort.Strings(keys)
	return keys
}

// routealiasTestAdminRoot 返回 admin 仓库根目录，避免测试依赖执行目录。
func routealiasTestAdminRoot(t *testing.T) string {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(currentFile), "../.."))
}

// routealiasTestSiblingRoot 从普通克隆或 Git worktree 中定位工作区兄弟仓库。
func routealiasTestSiblingRoot(t *testing.T, start string, project string) string {
	t.Helper()
	dir := filepath.Clean(start)
	for {
		projectDir := filepath.Join(dir, project)
		if info, err := os.Stat(projectDir); err == nil && info.IsDir() {
			return projectDir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("workspace sibling project not found: %s", project)
		}
		dir = parent
	}
}

// workspaceDocsResources 返回指定文档站目录下需要授权的 Markdown 文档。
func workspaceDocsResources(t *testing.T, root string, site string) []DocResource {
	t.Helper()
	if _, err := os.Stat(root); err != nil {
		t.Fatalf("docs root %s stat error: %v", root, err)
	}
	resources := make([]DocResource, 0)
	err := filepath.WalkDir(root, func(itemPath string, item os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if item.IsDir() || !strings.EqualFold(filepath.Ext(item.Name()), ".md") {
			return nil
		}
		switch item.Name() {
		case "_sidebar.md", "_navbar.md", "404.md":
			return nil
		}
		relativePath, err := filepath.Rel(root, itemPath)
		if err != nil {
			return err
		}
		resources = append(resources, DocResource{Site: site, Path: filepath.ToSlash(relativePath)})
		return nil
	})
	if err != nil {
		t.Fatalf("WalkDir(%s) error = %v", root, err)
	}
	return resources
}
