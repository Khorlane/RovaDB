package rovadb

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

const archModulePath = "github.com/Khorlane/RovaDB"

func TestArchitectureDependencyDirectionGuardrails(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name             string
		dir              string
		forbiddenImports []string
	}{
		{
			name: "parser stays free of execution and storage dependencies",
			dir:  filepath.Join("internal", "parser"),
			forbiddenImports: []string{
				archModulePath + "/internal/bufferpool",
				archModulePath + "/internal/executor",
				archModulePath + "/internal/planner",
				archModulePath + "/internal/storage",
				archModulePath + "/internal/txn",
			},
		},
		{
			name: "planner stays free of storage concrete details",
			dir:  filepath.Join("internal", "planner"),
			forbiddenImports: []string{
				archModulePath + "/internal/bufferpool",
				archModulePath + "/internal/executor",
				archModulePath + "/internal/storage",
				archModulePath + "/internal/txn",
			},
		},
		{
			name: "storage stays below parser planner and execution",
			dir:  filepath.Join("internal", "storage"),
			forbiddenImports: []string{
				archModulePath + "/internal/executor",
				archModulePath + "/internal/parser",
				archModulePath + "/internal/planner",
			},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			files := packageGoFiles(t, tc.dir)
			for _, file := range files {
				imports := fileImports(t, file)
				for _, forbidden := range tc.forbiddenImports {
					if slicesContains(imports, forbidden) {
						t.Fatalf("%s imports forbidden package %q", file, forbidden)
					}
				}
			}
		})
	}
}

func TestArchitectureRootInternalImportAllowlist(t *testing.T) {
	t.Parallel()

	allowed := map[string]struct{}{
		"api_errors.go":              {},
		"catalog_api.go":             {},
		"db.go":                      {},
		"physical_table_mutation.go": {},
		"query_trace_api.go":         {},
		"status_api.go":              {},
		"storage_value_adapter.go":   {},
		"system_catalog.go":          {},
		"tx.go":                      {},
	}

	rootFiles := packageGoFiles(t, ".")
	for _, file := range rootFiles {
		imports := fileImports(t, file)
		if !hasInternalImport(imports) {
			continue
		}
		if _, ok := allowed[filepath.Base(file)]; !ok {
			t.Fatalf("%s imports internal packages but is not in the root-layer allowlist", file)
		}
	}
}

func TestArchitectureIndexedReadBoundaryGuardrails(t *testing.T) {
	t.Parallel()

	dbFile := parseGoFile(t, "db.go")

	for _, tc := range []struct {
		funcName           string
		requiredSelectors  []string
		forbiddenSelectors []string
	}{
		{
			funcName:          "lookupIndexedLocators",
			requiredSelectors: []string{"LookupSimpleIndexExact"},
			forbiddenSelectors: []string{
				"DecodeIndexInternalEntry",
				"DecodeIndexLeafEntry",
				"EncodeIndexInternalEntry",
				"EncodeIndexKey",
				"FindIndexLeafPage",
				"IndexInternalEntry",
				"IndexLeafEntry",
				"IndexPageEntry",
				"IndexPageEntryCount",
				"IndexPageEntryPayload",
				"LookupIndexExact",
				"ReadAllIndexInternalRecords",
				"ReadAllIndexLeafRecords",
			},
		},
		{
			funcName:          "countAllRowsFromIndexOnly",
			requiredSelectors: []string{"CountAllSimpleIndexEntries"},
			forbiddenSelectors: []string{
				"DecodeIndexInternalEntry",
				"DecodeIndexLeafEntry",
				"EncodeIndexKey",
				"IndexInternalEntry",
				"IndexLeafEntry",
				"IndexPageEntry",
				"IndexPageEntryCount",
				"LookupIndexExact",
				"ReadAllIndexInternalRecords",
				"ReadAllIndexLeafRecords",
			},
		},
		{
			funcName:          "projectAllRowsFromIndexOnly",
			requiredSelectors: []string{"ReadAllSimpleIndexValuesInOrder"},
			forbiddenSelectors: []string{
				"DecodeIndexInternalEntry",
				"DecodeIndexLeafEntry",
				"EncodeIndexKey",
				"IndexInternalEntry",
				"IndexLeafEntry",
				"IndexPageEntry",
				"IndexPageEntryCount",
				"LookupIndexExact",
				"ReadAllIndexInternalRecords",
				"ReadAllIndexLeafRecords",
			},
		},
		{
			funcName:          "validateIndexLookupMetadata",
			requiredSelectors: []string{"ValidateIndexRoot"},
			forbiddenSelectors: []string{
				"DecodeIndexInternalEntry",
				"DecodeIndexLeafEntry",
				"EncodeIndexKey",
				"IndexInternalEntry",
				"IndexLeafEntry",
				"IndexPageEntry",
				"IndexPageEntryCount",
				"LookupIndexExact",
				"ReadAllIndexInternalRecords",
				"ReadAllIndexLeafRecords",
			},
		},
	} {
		tc := tc
		t.Run(tc.funcName, func(t *testing.T) {
			t.Parallel()

			selectors := storageSelectorsInFunc(t, dbFile, tc.funcName)
			for _, required := range tc.requiredSelectors {
				if _, ok := selectors[required]; !ok {
					t.Fatalf("db.go:%s no longer calls storage.%s; update the guardrail if the boundary intentionally changed", tc.funcName, required)
				}
			}
			for _, forbidden := range tc.forbiddenSelectors {
				if _, ok := selectors[forbidden]; ok {
					t.Fatalf("db.go:%s calls forbidden low-level storage helper storage.%s", tc.funcName, forbidden)
				}
			}
		})
	}
}

func TestPlannerExecutionBoundaryPlanTypeGuardrails(t *testing.T) {
	t.Parallel()

	scanFile := parseGoFile(t, filepath.Join("internal", "planner", "scan.go"))
	indexScan := findStructType(t, scanFile, "IndexScan")
	assertStructFieldType(
		t,
		indexScan,
		"LookupValue",
		func(expr ast.Expr) bool {
			ident, ok := expr.(*ast.Ident)
			return ok && ident.Name == "Value"
		},
		"planner.IndexScan.LookupValue must stay planner.Value instead of a parser-owned payload",
	)
	assertStructHasNoSelectorType(
		t,
		indexScan,
		"parser",
		"Value",
		"planner.IndexScan must not carry parser.Value directly",
	)

	selectPlanFile := parseGoFile(t, filepath.Join("internal", "planner", "select_plan.go"))
	selectPlan := findStructType(t, selectPlanFile, "SelectPlan")
	assertStructHasNoSelectorType(
		t,
		selectPlan,
		"parser",
		"SelectExpr",
		"planner.SelectPlan must not carry parser.SelectExpr directly",
	)
}

func TestPlannerExecutionBoundaryExecutorHotPathGuardrails(t *testing.T) {
	t.Parallel()

	for _, path := range []string{
		filepath.Join("internal", "executor", "select.go"),
		filepath.Join("internal", "executor", "select_join.go"),
	} {
		path := path
		t.Run(path, func(t *testing.T) {
			t.Parallel()

			file := parseGoFile(t, path)
			assertFileHasNoSelectorExpr(t, file, "planner", "SelectQuery", path+" must not directly operate on planner.SelectQuery in hot SELECT paths")
			assertFileHasNoSelectorExpr(t, file, "planner", "ValueExpr", path+" must not directly operate on planner.ValueExpr in hot SELECT paths")
			assertFileHasNoSelectorExpr(t, file, "planner", "PredicateExpr", path+" must not directly operate on planner.PredicateExpr in hot SELECT paths")
			assertFileHasNoSelectorExpr(t, file, "planner", "WhereClause", path+" must not directly operate on planner.WhereClause in hot SELECT paths")

			assertFileHasNoSelectorExpr(t, file, "planner", "ScanTypeTable", path+" must not branch on raw planner scan constants in hot SELECT paths")
			assertFileHasNoSelectorExpr(t, file, "planner", "ScanTypeIndex", path+" must not branch on raw planner scan constants in hot SELECT paths")
			assertFileHasNoSelectorExpr(t, file, "planner", "TableScan", path+" must not depend on planner.TableScan in hot SELECT paths")
			assertFileHasNoSelectorExpr(t, file, "planner", "IndexScan", path+" must not depend on planner.IndexScan in hot SELECT paths")
		})
	}
}

func TestArchitectureOuterSeamRootSelectGuardrails(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name               string
		path               string
		funcName           string
		requiredSelectors  []string
		forbiddenSelectors []string
	}{
		{
			name:     "db query stays handoff-first",
			path:     "db.go",
			funcName: "query",
			requiredSelectors: []string{
				"NewIndexOnlyExecutionHandoff",
				"FallbackSelectHandoff",
				"SelectWithHandoff",
				"ProjectedColumnNamesForHandoff",
			},
			forbiddenSelectors: []string{
				"Select",
				"SelectCandidateRows",
				"ProjectedColumnNames",
			},
		},
		{
			name:     "tx query stays handoff-first",
			path:     "tx.go",
			funcName: "Query",
			requiredSelectors: []string{
				"NewSelectExecutionHandoff",
				"SelectWithHandoff",
				"ProjectedColumnNamesForHandoff",
			},
			forbiddenSelectors: []string{
				"Select",
				"SelectCandidateRows",
				"ProjectedColumnNames",
			},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			file := parseGoFile(t, tc.path)
			selectors := selectorNamesInFunc(t, file, tc.funcName)
			for _, required := range tc.requiredSelectors {
				if _, ok := selectors[required]; !ok {
					t.Fatalf("%s:%s no longer references %s; update the guardrail only if the outer seam intentionally changed", tc.path, tc.funcName, required)
				}
			}
			for _, forbidden := range tc.forbiddenSelectors {
				if _, ok := selectors[forbidden]; ok {
					t.Fatalf("%s:%s references raw executor.%s plan wrapper; root SELECT entry must stay handoff-first", tc.path, tc.funcName, forbidden)
				}
			}
		})
	}
}

func TestArchitectureOuterSeamRootDoesNotPeekIndexOnlyPayload(t *testing.T) {
	t.Parallel()

	dbFile := parseGoFile(t, "db.go")
	selectors := selectorNamesInFunc(t, dbFile, "query")
	if _, ok := selectors["IndexOnlyScan"]; ok {
		t.Fatal("db.go:query must not directly inspect planner.IndexOnlyScan payload; keep index-only entry mediated through executor handoffs")
	}
}

func TestArchitectureOuterSeamPlannerHelperTranslationGuardrails(t *testing.T) {
	t.Parallel()

	plannerFile := parseGoFile(t, filepath.Join("internal", "planner", "planner.go"))
	for _, tc := range []struct {
		funcName       string
		requiredParams []string
	}{
		{funcName: "chooseIndexOnlyScan", requiredParams: []string{"*SelectQuery", "map[string]*TableMetadata"}},
		{funcName: "simpleIndexOnlyProjectionColumn", requiredParams: []string{"*SelectQuery"}},
		{funcName: "chooseJoinScan", requiredParams: []string{"*SelectQuery"}},
		{funcName: "chooseIndexScan", requiredParams: []string{"*SelectQuery", "map[string]*TableMetadata"}},
		{funcName: "valueExprOperandShape", requiredParams: []string{"*ValueExpr"}},
		{funcName: "normalizePlannerColumnName", requiredParams: []string{"string", "*TableRef"}},
	} {
		tc := tc
		t.Run(tc.funcName, func(t *testing.T) {
			t.Parallel()

			assertFuncParamTypes(t, plannerFile, tc.funcName, tc.requiredParams)
			assertFuncBodyHasNoSelectorExpr(t, plannerFile, tc.funcName, "parser", "", "internal/planner/"+tc.funcName+" must stay on planner-owned translated types after PlanSelect entry translation")
		})
	}
}

func packageGoFiles(t *testing.T, dir string) []string {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir(%q) error = %v", dir, err)
	}

	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		files = append(files, filepath.Join(dir, name))
	}
	sort.Strings(files)
	return files
}

func fileImports(t *testing.T, path string) []string {
	t.Helper()

	file := parseGoFile(t, path)
	imports := make([]string, 0, len(file.Imports))
	for _, imp := range file.Imports {
		imports = append(imports, strings.Trim(imp.Path.Value, `"`))
	}
	sort.Strings(imports)
	return imports
}

func parseGoFile(t *testing.T, path string) *ast.File {
	t.Helper()

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("ParseFile(%q) error = %v", path, err)
	}
	return file
}

func findStructType(t *testing.T, file *ast.File, typeName string) *ast.StructType {
	t.Helper()

	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.TYPE {
			continue
		}
		for _, spec := range gen.Specs {
			typeSpec, ok := spec.(*ast.TypeSpec)
			if !ok || typeSpec.Name == nil || typeSpec.Name.Name != typeName {
				continue
			}
			structType, ok := typeSpec.Type.(*ast.StructType)
			if !ok {
				t.Fatalf("type %s is not a struct", typeName)
			}
			return structType
		}
	}

	t.Fatalf("struct type %s not found", typeName)
	return nil
}

func assertStructFieldType(t *testing.T, st *ast.StructType, fieldName string, match func(ast.Expr) bool, message string) {
	t.Helper()

	for _, field := range st.Fields.List {
		for _, name := range field.Names {
			if name.Name != fieldName {
				continue
			}
			if !match(field.Type) {
				t.Fatal(message)
			}
			return
		}
	}

	t.Fatalf("field %s not found", fieldName)
}

func assertStructHasNoSelectorType(t *testing.T, st *ast.StructType, pkgName, typeName, message string) {
	t.Helper()

	for _, field := range st.Fields.List {
		if selectorExprMatches(field.Type, pkgName, typeName) {
			t.Fatal(message)
		}
	}
}

func assertFileHasNoSelectorExpr(t *testing.T, file *ast.File, pkgName, selectorName, message string) {
	t.Helper()

	found := false
	ast.Inspect(file, func(node ast.Node) bool {
		if found {
			return false
		}
		sel, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if selectorExprMatches(sel, pkgName, selectorName) {
			found = true
			return false
		}
		return true
	})
	if found {
		t.Fatal(message)
	}
}

func assertFuncBodyHasNoSelectorExpr(t *testing.T, file *ast.File, funcName, pkgName, selectorName, message string) {
	t.Helper()

	found := false
	fn := findFuncDecl(t, file, funcName)
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if found {
			return false
		}
		sel, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if selectorName == "" {
			ident, ok := sel.X.(*ast.Ident)
			if ok && ident.Name == pkgName {
				found = true
				return false
			}
			return true
		}
		if selectorExprMatches(sel, pkgName, selectorName) {
			found = true
			return false
		}
		return true
	})
	if found {
		t.Fatal(message)
	}
}

func selectorExprMatches(expr ast.Expr, pkgName, selectorName string) bool {
	switch node := expr.(type) {
	case *ast.SelectorExpr:
		ident, ok := node.X.(*ast.Ident)
		return ok && ident.Name == pkgName && node.Sel != nil && node.Sel.Name == selectorName
	case *ast.StarExpr:
		return selectorExprMatches(node.X, pkgName, selectorName)
	case *ast.ArrayType:
		return selectorExprMatches(node.Elt, pkgName, selectorName)
	default:
		return false
	}
}

func hasInternalImport(imports []string) bool {
	for _, imp := range imports {
		if strings.HasPrefix(imp, archModulePath+"/internal/") {
			return true
		}
	}
	return false
}

func findFuncDecl(t *testing.T, file *ast.File, funcName string) *ast.FuncDecl {
	t.Helper()

	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if ok && fn.Name != nil && fn.Name.Name == funcName {
			if fn.Body == nil {
				t.Fatalf("function %s has no body", funcName)
			}
			return fn
		}
	}

	t.Fatalf("function %s not found", funcName)
	return nil
}

func selectorNamesInFunc(t *testing.T, file *ast.File, funcName string) map[string]struct{} {
	t.Helper()

	fn := findFuncDecl(t, file, funcName)
	selectors := map[string]struct{}{}
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		sel, ok := node.(*ast.SelectorExpr)
		if !ok || sel.Sel == nil {
			return true
		}
		selectors[sel.Sel.Name] = struct{}{}
		return true
	})
	return selectors
}

func assertFuncParamTypes(t *testing.T, file *ast.File, funcName string, want []string) {
	t.Helper()

	fn := findFuncDecl(t, file, funcName)
	got := make([]string, 0, len(want))
	if fn.Type.Params != nil {
		for _, field := range fn.Type.Params.List {
			typeName := exprString(field.Type)
			count := len(field.Names)
			if count == 0 {
				count = 1
			}
			for i := 0; i < count; i++ {
				got = append(got, typeName)
			}
		}
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("%s params = %v, want %v", funcName, got, want)
	}
}

func exprString(expr ast.Expr) string {
	switch node := expr.(type) {
	case *ast.Ident:
		return node.Name
	case *ast.StarExpr:
		return "*" + exprString(node.X)
	case *ast.SelectorExpr:
		return exprString(node.X) + "." + node.Sel.Name
	case *ast.ArrayType:
		return "[]" + exprString(node.Elt)
	case *ast.MapType:
		return "map[" + exprString(node.Key) + "]" + exprString(node.Value)
	default:
		return ""
	}
}

func storageSelectorsInFunc(t *testing.T, file *ast.File, funcName string) map[string]struct{} {
	t.Helper()

	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name == nil || fn.Name.Name != funcName {
			continue
		}
		if fn.Body == nil {
			t.Fatalf("function %s has no body", funcName)
		}

		selectors := map[string]struct{}{}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			sel, ok := node.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			ident, ok := sel.X.(*ast.Ident)
			if !ok || ident.Name != "storage" {
				return true
			}
			selectors[sel.Sel.Name] = struct{}{}
			return true
		})
		return selectors
	}

	t.Fatalf("function %s not found in db.go", funcName)
	return nil
}

func slicesContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
