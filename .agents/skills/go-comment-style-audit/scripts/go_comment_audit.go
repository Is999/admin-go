package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// chineseTextPattern 识别注释中至少一个中文字符。
var chineseTextPattern = regexp.MustCompile(`[\p{Han}]`)

// finding 描述一处需要人工复核的注释缺口。
type finding struct {
	// path 是缺口所在文件路径。
	path string
	// line 是缺口所在源码行号。
	line int
	// msg 是面向审阅者的缺口说明。
	msg string
}

func main() {
	includeTests := flag.Bool("include-tests", false, "scan *_test.go files")
	exitZero := flag.Bool("advisory-exit-zero", false, "print findings but exit 0")
	flag.Parse()

	root := "."
	if flag.NArg() > 0 {
		root = flag.Arg(0)
	}

	var findings []finding
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if shouldSkipDir(d.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		if !*includeTests && strings.HasSuffix(path, "_test.go") {
			return nil
		}
		if isGenerated(path) {
			return nil
		}
		file, parseErr := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if parseErr != nil {
			findings = append(findings, finding{path: path, line: 1, msg: parseErr.Error()})
			return nil
		}
		findings = append(findings, auditFile(fset, path, file)...)
		return nil
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	for _, f := range findings {
		fmt.Printf("%s:%d: %s\n", f.path, f.line, f.msg)
	}
	if len(findings) > 0 && !*exitZero {
		os.Exit(1)
	}
}

// shouldSkipDir 过滤常见依赖、构建产物和版本控制目录。
func shouldSkipDir(name string) bool {
	switch name {
	case ".git", "vendor", "node_modules", "dist", "build", "bin", ".turbo", "coverage":
		return true
	default:
		return false
	}
}

// isGenerated 只识别文件头的生成代码标记，避免普通字符串误触发。
func isGenerated(path string) bool {
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	if len(data) > 4096 {
		data = data[:4096]
	}
	for _, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if strings.HasPrefix(trimmed, "// Code generated ") && strings.HasSuffix(trimmed, " DO NOT EDIT.") {
			return true
		}
		if strings.HasPrefix(trimmed, "//") {
			continue
		}
		return false
	}
	return false
}

// auditFile 检查单个 Go 文件的声明和匿名结构体注释覆盖。
func auditFile(fset *token.FileSet, path string, file *ast.File) []finding {
	var findings []finding
	namedStructs := map[token.Pos]bool{}

	for _, decl := range file.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			if d.Name.Name == "init" || d.Name.Name == "main" {
				continue
			}
			if !hasDoc(d.Doc, nil) {
				findings = append(findings, at(fset, path, d.Pos(), "missing Chinese comment for function or method "+d.Name.Name))
			}
		case *ast.GenDecl:
			findings = append(findings, auditGenDecl(fset, path, d, namedStructs)...)
		}
	}

	ast.Inspect(file, func(n ast.Node) bool {
		st, ok := n.(*ast.StructType)
		if !ok || namedStructs[st.Pos()] {
			return true
		}
		findings = append(findings, auditStructFields(fset, path, st, "anonymous struct")...)
		return true
	})

	return findings
}

// auditGenDecl 检查 type、const、var 声明及命名结构体字段。
func auditGenDecl(fset *token.FileSet, path string, d *ast.GenDecl, namedStructs map[token.Pos]bool) []finding {
	var findings []finding
	multi := len(d.Specs) > 1

	for _, spec := range d.Specs {
		switch s := spec.(type) {
		case *ast.TypeSpec:
			if !hasDoc(s.Doc, nil) && (multi || !hasDoc(d.Doc, nil)) {
				findings = append(findings, at(fset, path, s.Pos(), "missing Chinese comment for type "+s.Name.Name))
			}
			if st, ok := s.Type.(*ast.StructType); ok {
				namedStructs[st.Pos()] = true
				findings = append(findings, auditStructFields(fset, path, st, "struct "+s.Name.Name)...)
			}
		case *ast.ValueSpec:
			if d.Tok != token.CONST && d.Tok != token.VAR {
				continue
			}
			if !hasDoc(s.Doc, s.Comment) && (multi || !hasDoc(d.Doc, nil)) {
				findings = append(findings, at(fset, path, s.Pos(), "missing Chinese comment for "+strings.ToLower(d.Tok.String())+" "+valueNames(s.Names)))
			}
		}
	}
	return findings
}

// auditStructFields 检查结构体字段是否具备可维护说明。
func auditStructFields(fset *token.FileSet, path string, st *ast.StructType, owner string) []finding {
	var findings []finding
	if st.Fields == nil {
		return findings
	}
	for _, field := range st.Fields.List {
		if hasDoc(field.Doc, field.Comment) {
			continue
		}
		names := fieldNames(field)
		if names == "" {
			names = "embedded field"
		}
		findings = append(findings, at(fset, path, field.Pos(), "missing Chinese comment for "+owner+" field "+names))
	}
	return findings
}

// hasDoc 判断声明或行尾是否存在中文说明。
func hasDoc(doc *ast.CommentGroup, line *ast.CommentGroup) bool {
	if doc != nil && chineseTextPattern.MatchString(doc.Text()) {
		return true
	}
	if line != nil && chineseTextPattern.MatchString(line.Text()) {
		return true
	}
	return false
}

// valueNames 将一组标识符压缩成人类可读的名称列表。
func valueNames(names []*ast.Ident) string {
	var out []string
	for _, name := range names {
		out = append(out, name.Name)
	}
	return strings.Join(out, ",")
}

// fieldNames 返回普通字段名，匿名嵌入字段由调用方描述。
func fieldNames(field *ast.Field) string {
	if len(field.Names) == 0 {
		return ""
	}
	return valueNames(field.Names)
}

// at 生成带源码位置的审计结果。
func at(fset *token.FileSet, path string, pos token.Pos, msg string) finding {
	p := fset.Position(pos)
	return finding{path: path, line: p.Line, msg: msg}
}
