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

func hasInternalImport(imports []string) bool {
	for _, imp := range imports {
		if strings.HasPrefix(imp, archModulePath+"/internal/") {
			return true
		}
	}
	return false
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
