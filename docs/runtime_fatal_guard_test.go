package docs

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// allowedInitPanicFiles 只允许发布物固定资产在包级匿名初始化器中校验失败时终止启动。
// 新增文件例外必须同时证明输入固定且不受请求、消息、任务、热加载或外部依赖影响。
var allowedInitPanicFiles = map[string]struct{}{
	"common/i18n/catalog.go": {},
}

// TestProductionCodeRejectsRuntimeFatalCalls 扫描项目自有生产 Go 文件，阻止运行路径重新引入不可恢复退出。
func TestProductionCodeRejectsRuntimeFatalCalls(t *testing.T) {
	moduleRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatalf("解析项目根目录失败: %v", err)
	}

	err = filepath.WalkDir(moduleRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path != moduleRoot && (entry.Name() == ".agents" || entry.Name() == ".git" || entry.Name() == "data" || entry.Name() == "vendor") {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		relPath, err := filepath.Rel(moduleRoot, path)
		if err != nil {
			return err
		}
		relPath = filepath.ToSlash(relPath)
		fileSet := token.NewFileSet()
		parsed, err := parser.ParseFile(fileSet, path, nil, 0)
		if err != nil {
			return err
		}
		imports := importPathByAlias(parsed)
		allowedFatalCalls := packageInitializerFatalCalls(parsed, relPath, imports)
		allowedStartupExits := commandMainExitCalls(parsed, relPath, imports)
		for _, imported := range parsed.Imports {
			path, pathErr := strconv.Unquote(imported.Path.Value)
			if pathErr == nil && imported.Name != nil && imported.Name.Name == "." && isFatalDotImportPackage(path) {
				position := fileSet.Position(imported.Pos())
				t.Errorf("生产代码禁止点导入含致命调用的包 %s: %s:%d", path, relPath, position.Line)
			}
		}

		ast.Inspect(parsed, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			position := fileSet.Position(call.Pos())
			switch target := call.Fun.(type) {
			case *ast.Ident:
				if target.Name == "panic" {
					if _, allowed := allowedFatalCalls[call.Pos()]; !allowed {
						t.Errorf("运行路径禁止 panic: %s:%d", relPath, position.Line)
					}
				}
			case *ast.SelectorExpr:
				if isFatalSelector(target) {
					if _, allowed := allowedFatalCalls[call.Pos()]; !allowed {
						t.Errorf("生产代码禁止不可恢复调用 %s: %s:%d", target.Sel.Name, relPath, position.Line)
					}
				}
				if packagePath, terminates := processTerminatorPackage(target, imports); terminates {
					if _, startupExit := allowedStartupExits[call.Pos()]; !startupExit {
						t.Errorf("服务运行路径禁止进程终止调用 %s.%s: %s:%d", packagePath, target.Sel.Name, relPath, position.Line)
					}
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("扫描生产 Go 文件失败: %v", err)
	}
}

// packageInitializerFatalCalls 返回允许在包初始化阶段执行的固定资产校验调用位置。
func packageInitializerFatalCalls(file *ast.File, relPath string, imports map[string]string) map[token.Pos]struct{} {
	positions := packageInitializerPanicCalls(file, relPath)
	for _, declaration := range file.Decls {
		group, ok := declaration.(*ast.GenDecl)
		if !ok || group.Tok != token.VAR {
			continue
		}
		for _, spec := range group.Specs {
			values, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for _, value := range values.Values {
				ast.Inspect(value, func(node ast.Node) bool {
					// 函数字面量可被保存到运行期调用；其内部 Must* 不能借包级变量声明获得例外。
					if _, function := node.(*ast.FuncLit); function {
						return false
					}
					call, ok := node.(*ast.CallExpr)
					if !ok {
						return true
					}
					if isFixedInitializerMustCall(call, imports) {
						positions[call.Pos()] = struct{}{}
					}
					return true
				})
			}
		}
	}
	return positions
}

// commandMainExitCalls 只放行 cmd 主函数直接执行的退出候选；仍需沿主调用链确认服务尚未监听或已完整停机并清理资源。
func commandMainExitCalls(file *ast.File, relPath string, imports map[string]string) map[token.Pos]struct{} {
	positions := make(map[token.Pos]struct{})
	if file.Name.Name != "main" || !strings.HasPrefix(relPath, "cmd/") {
		return positions
	}
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != "main" || function.Body == nil {
			continue
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			// 保存到变量或异步执行的闭包不属于主函数直接退出边界。
			if _, nestedFunction := node.(*ast.FuncLit); nestedFunction {
				return false
			}
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			packagePath, terminates := processTerminatorPackage(selector, imports)
			if terminates && (packagePath == "os" || packagePath == "syscall") {
				positions[call.Pos()] = struct{}{}
			}
			return true
		})
	}
	return positions
}

// packageInitializerPanicCalls 只返回包级变量直接调用的匿名函数内 panic，命名 helper 和未调用函数值不会被放行。
func packageInitializerPanicCalls(file *ast.File, relPath string) map[token.Pos]struct{} {
	positions := make(map[token.Pos]struct{})
	if _, ok := allowedInitPanicFiles[relPath]; !ok {
		return positions
	}
	for _, declaration := range file.Decls {
		group, ok := declaration.(*ast.GenDecl)
		if !ok || group.Tok != token.VAR {
			continue
		}
		for _, spec := range group.Specs {
			values, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for _, value := range values.Values {
				call, ok := value.(*ast.CallExpr)
				if !ok {
					continue
				}
				function, ok := call.Fun.(*ast.FuncLit)
				if !ok {
					continue
				}
				ast.Inspect(function.Body, func(node ast.Node) bool {
					// 初始化器内保存或异步执行的嵌套函数可能在 main 启动后运行，其 panic 不属于包初始化例外。
					if _, nestedFunction := node.(*ast.FuncLit); nestedFunction {
						return false
					}
					panicCall, ok := node.(*ast.CallExpr)
					if !ok {
						return true
					}
					if target, ok := panicCall.Fun.(*ast.Ident); ok && target.Name == "panic" {
						positions[panicCall.Pos()] = struct{}{}
					}
					return true
				})
			}
		}
	}
	return positions
}

// TestPackageInitializerPanicCallsRejectsCallableFunctions 校验只放行立即执行的包级匿名初始化器，命名函数和未调用函数值仍属运行期风险。
func TestPackageInitializerPanicCallsRejectsCallableFunctions(t *testing.T) {
	fileSet := token.NewFileSet()
	parsed, err := parser.ParseFile(fileSet, "common/i18n/catalog.go", `package i18n
var initialized = func() int { panic("build asset"); return 1 }()
var callable = func() { panic("runtime value") }
var nested = func() int { deferred := func() { panic("runtime nested") }; _ = deferred; return 1 }()
func runtimePath() { panic("runtime function") }
`, 0)
	if err != nil {
		t.Fatalf("解析测试源码失败: %v", err)
	}
	if got := len(packageInitializerPanicCalls(parsed, "common/i18n/catalog.go")); got != 1 {
		t.Fatalf("允许的包初始化 panic 数量 = %d，期望 1", got)
	}
}

// TestFatalCallClassification 校验固定包初始化例外与运行期终止调用分类不会因标准库别名失效。
func TestFatalCallClassification(t *testing.T) {
	fileSet := token.NewFileSet()
	parsed, err := parser.ParseFile(fileSet, "common/i18n/catalog.go", `package i18n
import (
    re "regexp"
    run "runtime"
    sys "syscall"
)
var pattern = re.MustCompile("fixed")
var dynamicValue = "runtime"
var dynamicPattern = re.MustCompile(dynamicValue)
func runtimePath() { _ = re.MustCompile("runtime"); run.Goexit(); sys.Exit(1) }
`, 0)
	if err != nil {
		t.Fatalf("解析测试源码失败: %v", err)
	}
	imports := importPathByAlias(parsed)
	allowed := packageInitializerFatalCalls(parsed, "common/i18n/catalog.go", imports)
	if len(allowed) != 1 {
		t.Fatalf("允许的固定包初始化调用数量 = %d，期望 1", len(allowed))
	}

	var runtimeTerminators int
	ast.Inspect(parsed, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if _, terminates := processTerminatorPackage(selector, imports); terminates {
			runtimeTerminators++
		}
		return true
	})
	if runtimeTerminators != 2 {
		t.Fatalf("识别的进程终止调用数量 = %d，期望 2", runtimeTerminators)
	}

	mainFile, err := parser.ParseFile(fileSet, "cmd/example/main.go", `package main
import process "os"
func main() { process.Exit(1) }
func helper() { process.Exit(1) }
`, 0)
	if err != nil {
		t.Fatalf("解析命令入口测试源码失败: %v", err)
	}
	if got := len(commandMainExitCalls(mainFile, "cmd/example/main.go", importPathByAlias(mainFile))); got != 1 {
		t.Fatalf("允许的命令入口退出调用数量 = %d，期望 1", got)
	}

	for _, name := range []string{"Fatal", "Fatalw", "Panic", "Panicw", "MustCompile", "MustRegister"} {
		if !isFatalSelector(&ast.SelectorExpr{Sel: ast.NewIdent(name)}) {
			t.Fatalf("未识别致命调用名称 %s", name)
		}
	}
}

// isFatalSelector 识别会直接终止进程或在重复指标注册时触发 panic 的调用。
func isFatalSelector(selector *ast.SelectorExpr) bool {
	name := selector.Sel.Name
	return strings.HasPrefix(name, "Fatal") || strings.HasPrefix(name, "Panic") || strings.HasPrefix(name, "Must")
}

// importPathByAlias 返回源码实际使用的导入别名到完整包路径映射。
func importPathByAlias(file *ast.File) map[string]string {
	imports := make(map[string]string, len(file.Imports))
	for _, imported := range file.Imports {
		path, err := strconv.Unquote(imported.Path.Value)
		if err != nil {
			continue
		}
		alias := filepath.Base(path)
		if imported.Name != nil {
			alias = imported.Name.Name
		}
		if alias != "_" && alias != "." {
			imports[alias] = path
		}
	}
	return imports
}

// isFixedInitializerMustCall 仅放行包级变量初始化时使用固定字符串字面量编译正则的标准库调用。
func isFixedInitializerMustCall(call *ast.CallExpr, imports map[string]string) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || len(call.Args) != 1 {
		return false
	}
	ident, ok := selector.X.(*ast.Ident)
	if !ok || imports[ident.Name] != "regexp" || selector.Sel.Name != "MustCompile" {
		return false
	}
	literal, ok := call.Args[0].(*ast.BasicLit)
	return ok && literal.Kind == token.STRING
}

// processTerminatorPackage 返回会终止进程或当前 goroutine 的标准库包路径。
func processTerminatorPackage(selector *ast.SelectorExpr, imports map[string]string) (string, bool) {
	ident, ok := selector.X.(*ast.Ident)
	if !ok {
		return "", false
	}
	packagePath := imports[ident.Name]
	switch {
	case (packagePath == "os" || packagePath == "syscall") && selector.Sel.Name == "Exit":
		return packagePath, true
	case packagePath == "runtime" && selector.Sel.Name == "Goexit":
		return packagePath, true
	default:
		return "", false
	}
}

// isFatalDotImportPackage 判断点导入是否会隐藏标准库进程终止函数的来源。
func isFatalDotImportPackage(path string) bool {
	return path == "log" || path == "os" || path == "syscall" || path == "runtime"
}
