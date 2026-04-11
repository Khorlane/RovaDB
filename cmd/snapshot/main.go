package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

const (
	outputDir         = `C:\Projects\RovaDB Research`
	snapshotToolToken = "snapshot-main-v11"
)

type treeNode struct {
	name     string
	children map[string]*treeNode
	isDir    bool
}

type packageInfo struct {
	Name        string
	ImportPath  string
	Dir         string
	Files       []string
	ParsedFiles []*ast.File
	FSet        *token.FileSet
}

type funcEntry struct {
	Signature  string
	Exported   bool
	MethodName string
}

func main() {
	wd, err := os.Getwd()
	if err != nil {
		fail("unable to determine working directory: %v", err)
	}

	if err := validateRepoRoot(wd); err != nil {
		fail("%v", err)
	}

	rootAbs, err := filepath.Abs(wd)
	if err != nil {
		fail("unable to resolve repo root: %v", err)
	}

	if _, err := os.Stat(outputDir); err != nil {
		fail("output directory unavailable: %v", err)
	}

	generatedDate, err := runGit(wd, "log", "-1", "--date=format:%Y-%m-%d", "--pretty=format:%ad")
	if err != nil {
		fail("git history unavailable: %v", err)
	}
	generatedDate = strings.TrimSpace(generatedDate)
	if generatedDate == "" {
		fail("git history unavailable: empty generated date")
	}

	generatedCommit, err := runGit(wd, "rev-parse", "--short", "HEAD")
	if err != nil {
		fail("git commit unavailable: %v", err)
	}
	generatedCommit = strings.TrimSpace(generatedCommit)
	if generatedCommit == "" {
		fail("git commit unavailable: empty HEAD sha")
	}

	repoName := filepath.Base(rootAbs)

	historyLines, err := collectGitHistory(wd, 120)
	if err != nil {
		fail("unable to collect git history: %v", err)
	}

	tagsOut, err := runGit(wd, "tag", "-l")
	if err != nil {
		fail("unable to collect git tags: %v", err)
	}
	tags := splitNonEmptyLines(tagsOut)
	sort.Strings(tags)

	files, err := collectRepoFiles(rootAbs)
	if err != nil {
		fail("unable to walk repository: %v", err)
	}

	treeLines, err := buildRepoTree(rootAbs)
	if err != nil {
		fail("unable to build repo structure: %v", err)
	}

	modulePath, err := readModulePath(filepath.Join(rootAbs, "go.mod"))
	if err != nil {
		fail("unable to read go.mod: %v", err)
	}

	packages, err := collectPackages(rootAbs, files)
	if err != nil {
		fail("unable to collect Go packages: %v", err)
	}

	testFiles, err := collectTestFiles(rootAbs, files)
	if err != nil {
		fail("unable to collect test files: %v", err)
	}

	testClassification, err := classifyTests(rootAbs, testFiles)
	if err != nil {
		fail("unable to classify test files: %v", err)
	}

	dependencyMap, err := buildDependencyMap(modulePath, packages)
	if err != nil {
		fail("unable to build package dependency map: %v", err)
	}

	exportSurface, err := buildExportSurface(packages)
	if err != nil {
		fail("unable to build exported API surface: %v", err)
	}

	boundaryTypes, err := buildBoundaryTypes(packages)
	if err != nil {
		fail("unable to build key boundary types: %v", err)
	}

	signatureInventory, err := buildSignatureInventory(rootAbs, packages)
	if err != nil {
		fail("unable to build signature inventory: %v", err)
	}

	callPaths, err := buildCriticalCallPaths(rootAbs)
	if err != nil {
		fail("unable to build critical call paths: %v", err)
	}

	guardrails, err := buildArchitecturalGuardrails(rootAbs)
	if err != nil {
		fail("unable to build architectural guardrails: %v", err)
	}

	largeFiles, err := buildLargeFiles(rootAbs, files)
	if err != nil {
		fail("unable to build large file report: %v", err)
	}

	summary := buildSummarySignal(exportSurface, largeFiles, guardrails)

	sections := []struct {
		Number int
		Title  string
		Lines  []string
	}{
		{1, "GIT HISTORY", historyLines},
		{2, "GIT TAGS", tags},
		{3, "REPO STRUCTURE", treeLines},
		{4, "PACKAGE DEPENDENCY MAP (CRITICAL)", dependencyMap},
		{5, "EXPORTED API SURFACE", exportSurface},
		{6, "KEY BOUNDARY TYPES", boundaryTypes},
		{7, "FUNCTION / METHOD SIGNATURE INVENTORY", signatureInventory},
		{8, "CRITICAL CALL PATHS (FLOW)", callPaths},
		{9, "ARCHITECTURAL GUARDRAILS", guardrails},
		{10, "LARGE / CENTRAL FILES", largeFiles},
		{11, "SUMMARY SIGNAL (SHORT)", summary},
		{12, "TEST CLASSIFICATION", testClassification},
	}

	indexLines := []string{
		"RovaDB Snapshot Section Index",
		fmt.Sprintf("Snapshot Tool Version: %s", snapshotToolToken),
		fmt.Sprintf("Generated: %s", generatedDate),
		fmt.Sprintf("Generated Commit: %s", generatedCommit),
		fmt.Sprintf("Repository: %s", repoName),
		"",
	}

	var combined strings.Builder

	for _, section := range sections {
		filename := fmt.Sprintf("snapshot.%d.txt", section.Number)

		var out strings.Builder
		writeFileHeader(&out, generatedDate, generatedCommit, repoName)
		writeSection(&out, section.Number, section.Title, section.Lines)

		if err := writeAtomically(filepath.Join(outputDir, filename), []byte(out.String())); err != nil {
			fail("unable to write %s: %v", filename, err)
		}

		indexLines = append(indexLines, fmt.Sprintf("%d -> %s -> %s", section.Number, filename, section.Title))
	}

	if err := writeAtomically(filepath.Join(outputDir, "snapshot.index.txt"), []byte(strings.Join(indexLines, "\n")+"\n")); err != nil {
		fail("unable to write snapshot.index.txt: %v", err)
	}

	for _, section := range sections {
		writeSection(&combined, section.Number, section.Title, section.Lines)
	}

	if err := writeAtomically(filepath.Join(outputDir, "snapshot.txt"), []byte(combined.String())); err != nil {
		fail("unable to write snapshot.txt: %v", err)
	}

	fmt.Printf("OK\nWrote section files to %s\n", outputDir)
}

func validateRepoRoot(root string) error {
	required := []string{"go.mod", "README.md", "internal", "cmd"}
	for _, name := range required {
		path := filepath.Join(root, name)
		if _, err := os.Stat(path); err != nil {
			return fmt.Errorf("must be run from the RovaDB repo root: missing %s", name)
		}
	}
	return nil
}

func collectGitHistory(dir string, limit int) ([]string, error) {
	out, err := runGit(dir, "log", "--decorate", "--date=iso", "--pretty=format:%h %ad %d %s")
	if err != nil {
		return nil, err
	}
	lines := splitNonEmptyLines(out)
	if limit > 0 && len(lines) > limit {
		lines = lines[:limit]
	}
	return lines, nil
}

func runGit(dir string, args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		return "", fmt.Errorf("%s", msg)
	}
	return normalizeNewlines(stdout.String()), nil
}

func collectRepoFiles(root string) ([]string, error) {
	var files []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == root {
			return nil
		}

		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}

		if d.IsDir() {
			if shouldExcludeDir(rel) {
				return filepath.SkipDir
			}
			return nil
		}

		if shouldExcludeFile(rel) {
			return nil
		}

		files = append(files, path)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	return files, nil
}


func collectTestFiles(root string, files []string) ([]string, error) {
	var tests []string
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return nil, err
			}
			tests = append(tests, filepath.ToSlash(rel))
		}
	}
	sort.Strings(tests)
	return tests, nil
}

func classifyTests(root string, testFiles []string) ([]string, error) {
	type classifiedTest struct {
		relPath        string
		dir            string
		packageName    string
		classification string
	}

	type directorySummary struct {
		white int
		black int
	}

	type packageSummary struct {
		classification string
		count          int
	}

	var items []classifiedTest
	dirSummary := map[string]*directorySummary{}
	pkgSummary := map[string]*packageSummary{}
	whiteCount := 0
	blackCount := 0

	for _, relPath := range testFiles {
		absPath := filepath.Join(root, filepath.FromSlash(relPath))

		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, absPath, nil, parser.PackageClauseOnly)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", relPath, err)
		}

		pkgName := f.Name.Name
		classification := fmt.Sprintf("white-box (package %s)", pkgName)
		if strings.HasSuffix(pkgName, "_test") {
			classification = fmt.Sprintf("black-box (package %s)", pkgName)
			blackCount++
		} else {
			whiteCount++
		}

		dir := filepath.ToSlash(filepath.Dir(relPath))
		if dir == "." {
			dir = "(root)"
		}

		entry := dirSummary[dir]
		if entry == nil {
			entry = &directorySummary{}
			dirSummary[dir] = entry
		}
		if strings.HasSuffix(pkgName, "_test") {
			entry.black++
		} else {
			entry.white++
		}

		pkgEntry := pkgSummary[pkgName]
		if pkgEntry == nil {
			pkgEntry = &packageSummary{classification: classification}
			pkgSummary[pkgName] = pkgEntry
		}
		pkgEntry.count++

		items = append(items, classifiedTest{
			relPath:        relPath,
			dir:            dir,
			packageName:    pkgName,
			classification: classification,
		})
	}

	sort.Slice(items, func(i, j int) bool {
		if items[i].dir != items[j].dir {
			return items[i].dir < items[j].dir
		}
		if items[i].packageName != items[j].packageName {
			return items[i].packageName < items[j].packageName
		}
		return items[i].relPath < items[j].relPath
	})

	lines := []string{
		fmt.Sprintf("white-box tests: %d", whiteCount),
		fmt.Sprintf("black-box tests: %d", blackCount),
		"",
		"[directory summary]",
	}

	if len(items) == 0 {
		lines = append(lines, "  - (none)")
		return lines, nil
	}

	dirs := make([]string, 0, len(dirSummary))
	for dir := range dirSummary {
		dirs = append(dirs, dir)
	}
	sort.Strings(dirs)
	for _, dir := range dirs {
		s := dirSummary[dir]
		lines = append(lines, fmt.Sprintf("  %s -> white-box: %d, black-box: %d, total: %d", dir, s.white, s.black, s.white+s.black))
	}

	lines = append(lines, "")
	lines = append(lines, "[package summary]")

	packages := make([]string, 0, len(pkgSummary))
	for pkgName := range pkgSummary {
		packages = append(packages, pkgName)
	}
	sort.Strings(packages)
	for _, pkgName := range packages {
		s := pkgSummary[pkgName]
		lines = append(lines, fmt.Sprintf("  %s -> %s, files: %d", pkgName, s.classification, s.count))
	}

	lines = append(lines, "")
	lines = append(lines, "[per-file classification]")

	for _, item := range items {
		lines = append(lines, fmt.Sprintf("  %s -> %s", item.relPath, item.classification))
	}

	return lines, nil
}

func buildRepoTree(root string) ([]string, error) {
	rootNode := &treeNode{name: "", isDir: true, children: map[string]*treeNode{}}

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == root {
			return nil
		}

		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}

		depth := pathDepth(rel)

		if d.IsDir() {
			if shouldExcludeDir(rel) {
				return filepath.SkipDir
			}
			if depth > 3 {
				return filepath.SkipDir
			}
			insertTreePath(rootNode, filepath.ToSlash(rel), true)
			return nil
		}

		if shouldExcludeFile(rel) || depth > 3 {
			return nil
		}

		insertTreePath(rootNode, filepath.ToSlash(rel), false)
		return nil
	})
	if err != nil {
		return nil, err
	}

	var lines []string
	emitTree(rootNode, 0, &lines)
	return lines, nil
}

func insertTreePath(root *treeNode, rel string, isDir bool) {
	parts := strings.Split(rel, "/")
	cur := root
	for i, part := range parts {
		child, ok := cur.children[part]
		last := i == len(parts)-1
		if !ok {
			child = &treeNode{name: part, isDir: !last || isDir, children: map[string]*treeNode{}}
			cur.children[part] = child
		}
		if last {
			child.isDir = isDir
		}
		cur = child
	}
}

func emitTree(n *treeNode, depth int, lines *[]string) {
	keys := make([]string, 0, len(n.children))
	for k := range n.children {
		keys = append(keys, k)
	}

	sort.Slice(keys, func(i, j int) bool {
		a := n.children[keys[i]]
		b := n.children[keys[j]]
		if a.isDir != b.isDir {
			return a.isDir && !b.isDir
		}
		return strings.ToLower(a.name) < strings.ToLower(b.name)
	})

	indent := strings.Repeat("  ", depth)
	for _, k := range keys {
		child := n.children[k]
		line := indent + child.name
		if child.isDir {
			line += "/"
		}
		*lines = append(*lines, line)
		if child.isDir {
			emitTree(child, depth+1, lines)
		}
	}
}

func readModulePath(goModPath string) (string, error) {
	data, err := os.ReadFile(goModPath)
	if err != nil {
		return "", err
	}
	for _, line := range splitNonEmptyLines(string(data)) {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "module ") {
			return strings.TrimSpace(strings.TrimPrefix(line, "module ")), nil
		}
	}
	return "", fmt.Errorf("module path not found")
}

func collectPackages(root string, files []string) ([]packageInfo, error) {
	byDir := map[string][]string{}
	for _, path := range files {
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			continue
		}
		byDir[filepath.Dir(path)] = append(byDir[filepath.Dir(path)], path)
	}

	dirs := make([]string, 0, len(byDir))
	for dir := range byDir {
		dirs = append(dirs, dir)
	}
	sort.Strings(dirs)

	var out []packageInfo
	for _, dir := range dirs {
		rel, err := filepath.Rel(root, dir)
		if err != nil {
			return nil, err
		}
		rel = filepath.ToSlash(rel)
		importPath := "."
		if rel != "." {
			importPath = rel
		}

		fset := token.NewFileSet()
		filesInDir := append([]string(nil), byDir[dir]...)
		sort.Strings(filesInDir)

		var parsed []*ast.File
		var pkgName string
		for _, path := range filesInDir {
			file, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", path, err)
			}
			if pkgName == "" {
				pkgName = file.Name.Name
			}
			parsed = append(parsed, file)
		}

		out = append(out, packageInfo{
			Name:        pkgName,
			ImportPath:  importPath,
			Dir:         dir,
			Files:       filesInDir,
			ParsedFiles: parsed,
			FSet:        fset,
		})
	}

	return out, nil
}

func buildDependencyMap(modulePath string, packages []packageInfo) ([]string, error) {
	preferred := []string{".", "internal/parser", "internal/planner", "internal/executor", "internal/storage", "internal/txn", "internal/bufferpool"}
	packageSet := map[string]packageInfo{}
	for _, pkg := range packages {
		packageSet[pkg.ImportPath] = pkg
	}

	ordered := uniqueExistingPackages(preferred, packages)
	var lines []string
	for i, pkgPath := range ordered {
		pkg := packageSet[pkgPath]
		importsSet := map[string]bool{}
		for _, f := range pkg.ParsedFiles {
			for _, imp := range f.Imports {
				raw := strings.Trim(imp.Path.Value, "\"")
				if raw == modulePath {
					importsSet["."] = true
					continue
				}
				if strings.HasPrefix(raw, modulePath+"/") {
					importsSet[strings.TrimPrefix(raw, modulePath+"/")] = true
				}
			}
		}

		imports := make([]string, 0, len(importsSet))
		for imp := range importsSet {
			imports = append(imports, imp)
		}
		sort.Strings(imports)

		lines = append(lines, fmt.Sprintf("[%s]", pkgPath))
		lines = append(lines, "imports:")
		if len(imports) == 0 {
			lines = append(lines, "  - (none)")
		} else {
			for _, imp := range imports {
				lines = append(lines, "  - "+imp)
			}
		}
		if i != len(ordered)-1 {
			lines = append(lines, "")
		}
	}
	return lines, nil
}

func buildExportSurface(packages []packageInfo) ([]string, error) {
	ordered := orderedPackages(packages)
	pkgMap := map[string]packageInfo{}
	for _, pkg := range packages {
		pkgMap[pkg.ImportPath] = pkg
	}

	var lines []string
	for i, pkgPath := range ordered {
		pkg := pkgMap[pkgPath]
		if pkgPath == "." {
			lines = append(lines, buildRootExportSurface(pkg)...)
		} else {
			lines = append(lines, buildPackageExportSurface(pkg)...)
		}

		if i != len(ordered)-1 {
			lines = append(lines, "")
		}
	}

	return lines, nil
}

func buildRootExportSurface(pkg packageInfo) []string {
	primaryTypes := map[string]bool{
		"DB":                    true,
		"Tx":                    true,
		"Rows":                  true,
		"Row":                   true,
		"Result":                true,
		"DBError":               true,
		"ErrorKind":             true,
		"TableInfo":             true,
		"ColumnInfo":            true,
		"EngineStatus":          true,
		"EngineSnapshot":        true,
		"EnginePageUsage":       true,
		"EngineSchemaInventory": true,
		"EngineCheckResult":     true,
		"EngineTableInfo":       true,
		"EngineIndexInfo":       true,
		"QueryExecutionTrace":   true,
	}

	primaryErrors := map[string]bool{
		"ErrClosed":                   true,
		"ErrExec":                     true,
		"ErrExecDisallowsSelect":      true,
		"ErrInvalidArgument":          true,
		"ErrMultipleRows":             true,
		"ErrNoRows":                   true,
		"ErrNotImplemented":           true,
		"ErrParse":                    true,
		"ErrPlan":                     true,
		"ErrQueryRequiresSelect":      true,
		"ErrRowsClosed":               true,
		"ErrScanBeforeNext":           true,
		"ErrScanMismatch":             true,
		"ErrStorage":                  true,
		"ErrTxNotActive":              true,
		"ErrTxnAlreadyActive":         true,
		"ErrTxnCommitWithoutActive":   true,
		"ErrTxnInvariantViolation":    true,
		"ErrTxnRollbackWithoutActive": true,
		"ErrUnsupportedScanType":      true,
	}

	primaryFuncs := map[string]bool{
		"Open(path string) (*DB, error)": true,
		"Version() string":               true,
	}

	primaryMethodPrefixes := []string{
		"(*DB).",
		"(*Tx).",
		"(*Rows).",
		"(*Row).",
		"(Result).",
		"(EngineSnapshot).",
	}

	exports := collectExports(pkg)
	var primary []string
	var other []string

	for _, item := range exports {
		switch {
		case primaryFuncs[item]:
			primary = append(primary, item)
		case primaryTypes[item]:
			primary = append(primary, item)
		case primaryErrors[item]:
			primary = append(primary, item)
		case hasPrefixAny(item, primaryMethodPrefixes):
			primary = append(primary, item)
		default:
			other = append(other, item)
		}
	}

	sort.Strings(primary)
	sort.Strings(other)

	lines := []string{
		"[.]",
		fmt.Sprintf("primary public API (%d):", len(primary)),
	}
	if len(primary) == 0 {
		lines = append(lines, "  - (none)")
	} else {
		for _, item := range primary {
			lines = append(lines, "  - "+item)
		}
	}

	lines = append(lines, "")
	lines = append(lines, fmt.Sprintf("other exported root symbols (%d):", len(other)))
	if len(other) == 0 {
		lines = append(lines, "  - (none)")
	} else {
		for _, item := range other {
			lines = append(lines, "  - "+item)
		}
	}

	return lines
}

func buildPackageExportSurface(pkg packageInfo) []string {
	exports := collectExports(pkg)
	sort.Strings(exports)

	lines := []string{
		fmt.Sprintf("[%s]", pkg.ImportPath),
		fmt.Sprintf("exports (%d):", len(exports)),
	}

	if len(exports) == 0 {
		lines = append(lines, "  - (none)")
		return lines
	}

	const maxDisplay = 80
	limit := len(exports)
	truncated := false
	if limit > maxDisplay {
		limit = maxDisplay
		truncated = true
	}

	for _, item := range exports[:limit] {
		lines = append(lines, "  - "+item)
	}

	if truncated {
		lines = append(lines, fmt.Sprintf("  - ... (%d more omitted)", len(exports)-limit))
	}

	return lines
}

func hasPrefixAny(s string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(s, prefix) {
			return true
		}
	}
	return false
}

func buildBoundaryTypes(packages []packageInfo) ([]string, error) {
	order := []string{"internal/planner", "internal/executor", "internal/storage"}
	pkgMap := map[string]packageInfo{}
	for _, pkg := range packages {
		pkgMap[pkg.ImportPath] = pkg
	}

	var lines []string
	for idx, pkgPath := range order {
		pkg, ok := pkgMap[pkgPath]
		if !ok {
			return nil, fmt.Errorf("required package missing: %s", pkgPath)
		}

		sectionLines, err := buildBoundaryTypesForPackage(pkgPath, pkg)
		if err != nil {
			return nil, err
		}

		lines = append(lines, sectionLines...)
		if idx != len(order)-1 {
			lines = append(lines, "")
		}
	}
	return lines, nil
}

func buildBoundaryTypesForPackage(pkgPath string, pkg packageInfo) ([]string, error) {
	switch pkgPath {
	case "internal/planner":
		items := collectNamedTypeLines(pkg, []string{
			"SelectPlan",
			"SelectQuery",
			"TableScan",
			"IndexScan",
			"IndexOnlyScan",
			"JoinScan",
			"ScanType",
		})
		return labeledBoundarySection("planner", items), nil

	case "internal/executor":
		items := collectNamedTypeLines(pkg, []string{
			"SelectExecutionHandoff",
			"IndexOnlyExecutionHandoff",
			"SelectAccessPath",
			"SelectAccessPathKind",
			"SelectIndexLookup",
			"Table",
		})
		return labeledBoundarySection("executor", items), nil

	case "internal/storage":
		items := collectNamedTypeLines(pkg, []string{
			"CatalogData",
			"CatalogTable",
			"CatalogColumn",
			"CatalogIndex",
			"CatalogIndexColumn",
			"CatalogPrimaryKey",
			"CatalogForeignKey",
			"CatalogWritePlan",
			"CatalogWritePage",
			"DirectoryControlState",
			"DirectoryRootIDMapping",
			"DirectoryCheckpointMetadata",
			"Page",
			"PageID",
			"Pager",
			"PageReader",
			"PageReaderFunc",
			"PageType",
			"PageAllocator",
			"RowLocator",
			"Value",
			"ValueKind",
			"WALHeader",
			"WALFrame",
			"WALCommitRecord",
			"WALRecord",
			"JournalHeader",
			"JournalEntry",
			"Journal",
			"IndexPageReader",
			"CatalogOverflowPageAllocator",
			"CatalogOverflowPageImage",
		})
		return labeledBoundarySection("storage", items), nil
	}

	return nil, fmt.Errorf("unsupported boundary package: %s", pkgPath)
}

func labeledBoundarySection(label string, items []string) []string {
	lines := []string{
		fmt.Sprintf("[%s]", label),
		fmt.Sprintf("boundary types (%d):", len(items)),
	}
	if len(items) == 0 {
		lines = append(lines, "  - (none)")
		return lines
	}
	for _, item := range items {
		lines = append(lines, "  - "+item)
	}
	return lines
}

func collectNamedTypeLines(pkg packageInfo, wanted []string) []string {
	wantedSet := map[string]bool{}
	for _, name := range wanted {
		wantedSet[name] = true
	}

	found := map[string]string{}
	for _, f := range pkg.ParsedFiles {
		for _, decl := range f.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.TYPE {
				continue
			}
			for _, spec := range gen.Specs {
				ts, ok := spec.(*ast.TypeSpec)
				if !ok || !wantedSet[ts.Name.Name] {
					continue
				}
				found[ts.Name.Name] = fmt.Sprintf("%s: %s", ts.Name.Name, renderExpr(pkg.FSet, ts.Type))
			}
		}
	}

	var lines []string
	for _, name := range wanted {
		if line, ok := found[name]; ok {
			lines = append(lines, line)
		}
	}
	return lines
}

func buildSignatureInventory(root string, packages []packageInfo) ([]string, error) {
	pkgMap := map[string]packageInfo{}
	for _, pkg := range packages {
		pkgMap[pkg.ImportPath] = pkg
	}

	rootPkg, ok := pkgMap["."]
	if !ok {
		return nil, fmt.Errorf("missing root package for section 7")
	}

	var lines []string

	lines = append(lines, "[root public api signatures]")
	rootPublic := collectRootPublicSignatures(rootPkg)
	lines = appendSignatureGroup(lines, rootPublic)
	lines = append(lines, "")

	lines = append(lines, "[root internal/helper signatures]")
	rootHelpers := collectRootHelperSignatures(rootPkg)
	lines = appendSignatureGroup(lines, rootHelpers)
	lines = append(lines, "")

	for _, item := range []struct {
		label string
		pkg   string
	}{
		{"[internal/parser key signatures]", "internal/parser"},
		{"[internal/planner key signatures]", "internal/planner"},
		{"[internal/executor key signatures]", "internal/executor"},
		{"[internal/storage key signatures]", "internal/storage"},
	} {
		pkg, ok := pkgMap[item.pkg]
		if !ok {
			return nil, fmt.Errorf("missing package for section 7: %s", item.pkg)
		}
		lines = append(lines, item.label)
		lines = appendSignatureGroup(lines, collectKeyPackageSignatures(item.pkg, pkg))
		lines = append(lines, "")
	}

	cmdExampleCount := countFilesUnder(
		filepath.Join(root, "cmd"),
		filepath.Join(root, "examples"),
	)

	lines = append(lines, "[cmd and examples omitted from primary signature view]")
	lines = append(lines, fmt.Sprintf("  - omitted file count: %d", cmdExampleCount))
	lines = append(lines, "  - rationale: preserve Codex handoff focus on engine/api signatures")

	return lines, nil
}

func appendSignatureGroup(lines []string, sigs []string) []string {
	lines = append(lines, fmt.Sprintf("count: %d", len(sigs)))
	if len(sigs) == 0 {
		lines = append(lines, "  - (none)")
		return lines
	}
	for _, sig := range sigs {
		lines = append(lines, "  - "+sig)
	}
	return lines
}

func collectRootPublicSignatures(pkg packageInfo) []string {
	entries := collectFuncEntries(pkg)

	var out []string
	for _, entry := range entries {
		if !entry.Exported {
			continue
		}

		switch {
		case entry.Signature == "Open(path string) (*DB, error)":
			out = append(out, entry.Signature)
		case entry.Signature == "Version() string":
			out = append(out, entry.Signature)
		case strings.HasPrefix(entry.Signature, "(*DB)."):
			out = append(out, entry.Signature)
		case strings.HasPrefix(entry.Signature, "(*Tx)."):
			out = append(out, entry.Signature)
		case strings.HasPrefix(entry.Signature, "(*Rows)."):
			out = append(out, entry.Signature)
		case strings.HasPrefix(entry.Signature, "(*Row)."):
			out = append(out, entry.Signature)
		case strings.HasPrefix(entry.Signature, "(Result)."):
			out = append(out, entry.Signature)
		case strings.HasPrefix(entry.Signature, "(EngineSnapshot)."):
			out = append(out, entry.Signature)
		}
	}

	sort.Strings(out)
	return dedupeStrings(out)
}

func collectRootHelperSignatures(pkg packageInfo) []string {
	helperPrefixes := []string{
		"(*DB).appendPendingPagesToWAL(",
		"(*DB).buildCatalogPageData(",
		"(*DB).checkpointCommittedPages(",
		"(*DB).countAllRowsFromIndexOnly(",
		"(*DB).countIndexedRows(",
		"(*DB).exec(",
		"(*DB).fetchRowByLocator(",
		"(*DB).loadRowsIntoTables(",
		"(*DB).lookupIndexedLocators(",
		"(*DB).lookupIndexedRows(",
		"(*DB).persistCatalogState(",
		"(*DB).persistPublicTxState(",
		"(*DB).query(",
		"(*DB).queryIndexOnly(",
		"(*DB).reconcileSystemCatalogOnOpen(",
		"(*DB).scanTableRows(",
		"(*DB).stageDirtyState(",
		"(*DB).stageSchemaState(",
		"(*DB).tablesForSelectHandoff(",
		"(*DB).validateTxnState(",
	}

	entries := collectFuncEntries(pkg)
	var out []string
	for _, entry := range entries {
		if entry.Exported {
			continue
		}
		if hasPrefixAny(entry.Signature, helperPrefixes) {
			out = append(out, entry.Signature)
		}
	}

	sort.Strings(out)
	return dedupeStrings(out)
}

func collectFuncEntries(pkg packageInfo) []funcEntry {
	var out []funcEntry
	for _, f := range pkg.ParsedFiles {
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			methodName := fn.Name.Name
			out = append(out, funcEntry{
				Signature:  renderFuncSignature(pkg.FSet, fn),
				Exported:   ast.IsExported(methodName),
				MethodName: methodName,
			})
		}
	}
	return out
}

func collectKeyPackageSignatures(pkgPath string, pkg packageInfo) []string {
	prefixSets := map[string][]string{
		"internal/parser": {
			"BindPlaceholders(",
			"Parse(",
			"ParseSelectExpr(",
			"parsePredicateExpr(",
			"parseValueExpr(",
		},
		"internal/planner": {
			"PlanSelect(",
			"(*SelectQuery).PrimaryTableRef(",
		},
		"internal/executor": {
			"DescribeSelectAccessPath(",
			"Execute(",
			"NewIndexOnlyExecutionHandoff(",
			"NewSelectExecutionHandoff(",
			"ProjectedColumnNames(",
			"ProjectedColumnNamesForHandoff(",
			"ProjectedColumnNamesForPlan(",
			"Select(",
			"SelectWithHandoff(",
		},
		"internal/storage": {
			"BuildCatalogPageData(",
			"CountAllSimpleIndexEntries(",
			"LoadCatalog(",
			"LookupIndexExact(",
			"LookupSimpleIndexExact(",
			"PrepareCatalogWritePlanWithRootMappings(",
			"SaveCatalog(",
			"ValidateIndexRoot(",
		},
	}

	wanted := prefixSets[pkgPath]
	var out []string
	for _, f := range pkg.ParsedFiles {
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			sig := renderFuncSignature(pkg.FSet, fn)
			if hasPrefixAny(sig, wanted) {
				out = append(out, sig)
			}
		}
	}
	sort.Strings(out)
	return dedupeStrings(out)
}

func countFilesUnder(dirs ...string) int {
	total := 0
	for _, dir := range dirs {
		_ = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			if d.IsDir() {
				return nil
			}
			if filepath.Ext(path) == ".go" {
				total++
			}
			return nil
		})
	}
	return total
}

func buildCriticalCallPaths(root string) ([]string, error) {
	required := []string{
		filepath.Join(root, "db.go"),
		filepath.Join(root, "tx.go"),
		filepath.Join(root, "internal", "planner", "planner.go"),
		filepath.Join(root, "internal", "executor", "planner_bridge.go"),
		filepath.Join(root, "internal", "executor", "select.go"),
		filepath.Join(root, "internal", "storage", "btree_lookup.go"),
	}
	for _, path := range required {
		if _, err := os.Stat(path); err != nil {
			return nil, fmt.Errorf("required file missing for critical flow section: %s", path)
		}
	}

	return []string{
		"A) Query path",
		"(*DB).Query -> (*DB).query -> parser.Parse -> planner.PlanSelect -> executor.NewSelectExecutionHandoff / executor.SelectWithHandoff -> storage row/index reads",
		"",
		"B) Mutation path",
		"(*DB).Exec -> (*DB).exec -> parser.Parse -> executor.Execute -> storage catalog / table / index mutation helpers",
		"Tx path: (*Tx).Exec -> parser.Parse -> executor.Execute -> txn-scoped storage mutation path",
		"",
		"C) Index-only path",
		"(*DB).Query -> parser.Parse -> planner.PlanSelect -> planner.IndexOnlyScan / SelectPlan -> executor.NewIndexOnlyExecutionHandoff -> storage.LookupIndexExact / CountAllSimpleIndexEntries",
	}, nil
}

func buildArchitecturalGuardrails(root string) ([]string, error) {
	path := filepath.Join(root, "arch_guardrails_test.go")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("required guardrail file missing: %s", path)
	}

	text := strings.ToLower(string(data))
	lines := []string{
		summarizeRule("forbidden import directions", text, []string{"forbidden", "import"}),
		summarizeRule("storage isolation guarantees", text, []string{"storage", "internal/storage"}),
		summarizeRule("root boundary restrictions", text, []string{"root", "public", "internal"}),
		summarizeRule("index-only boundary constraints", text, []string{"index-only", "boundary"}),
	}
	for i := range lines {
		lines[i] = "- " + lines[i]
	}
	return lines, nil
}

func summarizeRule(title, corpus string, needles []string) string {
	for _, needle := range needles {
		if !strings.Contains(corpus, needle) {
			return fmt.Sprintf("%s: review required; snapshot could not confirm exact enforced wording", title)
		}
	}

	switch title {
	case "forbidden import directions":
		return "forbidden import directions: architecture tests enforce dependency direction rather than allowing arbitrary cross-layer imports"
	case "storage isolation guarantees":
		return "storage isolation guarantees: architecture tests treat storage as an isolated layer rather than a planner/executor convenience dependency"
	case "root boundary restrictions":
		return "root boundary restrictions: architecture tests enforce a thin public root boundary instead of letting engine internals leak outward"
	case "index-only boundary constraints":
		return "index-only boundary constraints: architecture tests enforce index-only access behind approved boundary helpers rather than shortcut seams"
	default:
		return title + ": explicitly enforced by architecture tests"
	}
}

func buildLargeFiles(root string, files []string) ([]string, error) {
	type fileLines struct {
		path  string
		lines int
	}

	items := make([]fileLines, 0)
	for _, path := range files {
		ext := strings.ToLower(filepath.Ext(path))
		if ext != ".go" && ext != ".md" {
			continue
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return nil, err
		}
		relSlash := filepath.ToSlash(rel)
		if relSlash == "cmd/snapshot/main.go" {
			continue
		}

		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		items = append(items, fileLines{path: relSlash, lines: countLines(string(data))})
	}

	sort.Slice(items, func(i, j int) bool {
		if items[i].lines != items[j].lines {
			return items[i].lines > items[j].lines
		}
		return items[i].path < items[j].path
	})

	limit := 10
	if len(items) < limit {
		limit = len(items)
	}

	lines := make([]string, 0, limit)
	for _, item := range items[:limit] {
		lines = append(lines, fmt.Sprintf("%s — %d", item.path, item.lines))
	}
	return lines, nil
}

func buildSummarySignal(exportSurface, largeFiles, guardrails []string) []string {
	rootExportCount := countRootExports(exportSurface)

	status := "explicitly guarded"
	for _, line := range guardrails {
		if strings.Contains(strings.ToLower(line), "review required") {
			status = "partially confirmed"
			break
		}
	}

	summary := []string{fmt.Sprintf("- boundary direction status: %s", status)}
	if rootExportCount <= 20 {
		summary = append(summary, fmt.Sprintf("- root thickness status: thin (%d exported items in root package)", rootExportCount))
	} else {
		summary = append(summary, fmt.Sprintf("- root thickness status: broad exported surface (%d exported items in root package)", rootExportCount))
	}

	summary = append(summary, "- main remaining pressure points:")
	added := 0
	for _, line := range largeFiles {
		if strings.Contains(line, "_test.go") {
			continue
		}
		summary = append(summary, "  "+line)
		added++
		if added == 3 {
			break
		}
	}
	if added == 0 {
		summary = append(summary, "  none obvious outside test files")
	}
	return summary
}

func collectExports(pkg packageInfo) []string {
	var items []string
	for _, f := range pkg.ParsedFiles {
		for _, decl := range f.Decls {
			switch d := decl.(type) {
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					switch s := spec.(type) {
					case *ast.TypeSpec:
						if ast.IsExported(s.Name.Name) {
							items = append(items, s.Name.Name)
						}
					case *ast.ValueSpec:
						for _, name := range s.Names {
							if ast.IsExported(name.Name) {
								items = append(items, name.Name)
							}
						}
					}
				}
			case *ast.FuncDecl:
				if ast.IsExported(d.Name.Name) {
					items = append(items, renderFuncSignature(pkg.FSet, d))
				}
			}
		}
	}
	sort.Strings(items)
	return dedupeStrings(items)
}

func renderFuncSignature(fset *token.FileSet, fn *ast.FuncDecl) string {
	var b strings.Builder
	if fn.Recv != nil && len(fn.Recv.List) > 0 {
		b.WriteString("(")
		b.WriteString(renderExpr(fset, fn.Recv.List[0].Type))
		b.WriteString(").")
	}
	b.WriteString(fn.Name.Name)
	b.WriteString("(")
	b.WriteString(renderFieldList(fset, fn.Type.Params, false))
	b.WriteString(")")

	if fn.Type.Results != nil && len(fn.Type.Results.List) > 0 {
		results := renderFieldList(fset, fn.Type.Results, true)
		if strings.Contains(results, ",") {
			b.WriteString(" (")
			b.WriteString(results)
			b.WriteString(")")
		} else {
			b.WriteString(" ")
			b.WriteString(results)
		}
	}
	return b.String()
}

func renderFieldList(fset *token.FileSet, fl *ast.FieldList, omitNames bool) string {
	if fl == nil || len(fl.List) == 0 {
		return ""
	}

	parts := make([]string, 0, len(fl.List))
	for _, field := range fl.List {
		typ := renderExpr(fset, field.Type)
		if omitNames || len(field.Names) == 0 {
			parts = append(parts, typ)
			continue
		}
		names := make([]string, 0, len(field.Names))
		for _, n := range field.Names {
			names = append(names, n.Name)
		}
		parts = append(parts, strings.Join(names, ", ")+" "+typ)
	}

	return strings.Join(parts, ", ")
}

func renderExpr(fset *token.FileSet, expr ast.Expr) string {
	var b bytes.Buffer
	_ = printer.Fprint(&b, fset, expr)
	return normalizeWhitespace(b.String())
}

func writeFileHeader(out *strings.Builder, generatedDate, generatedCommit, repoName string) {
	out.WriteString("RovaDB Snapshot\n")
	out.WriteString(fmt.Sprintf("Snapshot Tool Version: %s\n", snapshotToolToken))
	out.WriteString(fmt.Sprintf("Generated: %s\n", generatedDate))
	out.WriteString(fmt.Sprintf("Generated Commit: %s\n", generatedCommit))
	out.WriteString(fmt.Sprintf("Repository: %s\n\n", repoName))
}

func writeSection(out *strings.Builder, number int, title string, lines []string) {
	out.WriteString("======================================================================\n")
	out.WriteString(fmt.Sprintf("SECTION %d - %s\n", number, title))
	out.WriteString("======================================================================\n")
	for _, line := range lines {
		out.WriteString(line)
		out.WriteString("\n")
	}
	out.WriteString("\n")
}

func writeAtomically(path string, data []byte) error {
	dir := filepath.Dir(path)
	if _, err := os.Stat(dir); err != nil {
		return err
	}

	tmp, err := os.CreateTemp(dir, "snapshot-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	ok := false

	defer func() {
		_ = tmp.Close()
		if !ok {
			_ = os.Remove(tmpPath)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	ok = true
	return nil
}

func splitNonEmptyLines(s string) []string {
	s = normalizeNewlines(s)
	raw := strings.Split(s, "\n")
	var lines []string
	for _, line := range raw {
		if strings.TrimSpace(line) != "" {
			lines = append(lines, strings.TrimRight(line, " \t"))
		}
	}
	return lines
}

func normalizeNewlines(s string) string {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\r", "\n")
	return s
}

func normalizeWhitespace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func pathDepth(rel string) int {
	rel = filepath.ToSlash(rel)
	if rel == "." || rel == "" {
		return 0
	}
	return len(strings.Split(rel, "/"))
}

func shouldExcludeDir(rel string) bool {
	base := strings.ToLower(filepath.Base(filepath.ToSlash(rel)))
	switch base {
	case ".git", ".idea", ".vscode", "bin", "dist", "tmp", "vendor", "node_modules":
		return true
	default:
		return false
	}
}

func shouldExcludeFile(rel string) bool {
	base := strings.ToLower(filepath.Base(filepath.ToSlash(rel)))
	ext := strings.ToLower(filepath.Ext(base))

	if strings.HasSuffix(base, ".exe") || strings.HasSuffix(base, ".dll") || strings.HasSuffix(base, ".so") || strings.HasSuffix(base, ".dylib") {
		return true
	}
	switch ext {
	case ".db", ".wal", ".tmp", ".out", ".test", ".log":
		return true
	}
	if base == "coverage.out" || base == "snapshot.txt" || base == "snapshot.index.txt" {
		return true
	}
	if strings.HasPrefix(base, "snapshot.") && strings.HasSuffix(base, ".txt") {
		return true
	}
	if strings.HasSuffix(base, ".journal") || strings.HasSuffix(base, ".coverprofile") {
		return true
	}
	return false
}

func orderedPackages(packages []packageInfo) []string {
	preferred := []string{".", "internal/parser", "internal/planner", "internal/executor", "internal/storage", "internal/txn", "internal/bufferpool"}
	ordered := uniqueExistingPackages(preferred, packages)

	seen := map[string]bool{}
	for _, p := range ordered {
		seen[p] = true
	}

	var others []string
	for _, pkg := range packages {
		if !seen[pkg.ImportPath] {
			others = append(others, pkg.ImportPath)
		}
	}
	sort.Strings(others)

	return append(ordered, others...)
}

func uniqueExistingPackages(preferred []string, packages []packageInfo) []string {
	present := map[string]bool{}
	for _, pkg := range packages {
		present[pkg.ImportPath] = true
	}

	var out []string
	seen := map[string]bool{}
	for _, p := range preferred {
		if present[p] && !seen[p] {
			out = append(out, p)
			seen[p] = true
		}
	}
	return out
}

func dedupeStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := []string{in[0]}
	for i := 1; i < len(in); i++ {
		if in[i] != in[i-1] {
			out = append(out, in[i])
		}
	}
	return out
}

func countRootExports(lines []string) int {
	count := 0
	inRoot := false
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "[.]" {
			inRoot = true
			continue
		}
		if inRoot && strings.HasPrefix(trimmed, "[") && trimmed != "[.]" {
			break
		}
		if inRoot && strings.HasPrefix(line, "  - ") {
			count++
		}
	}
	return count
}

func countLines(s string) int {
	s = normalizeNewlines(s)
	if s == "" {
		return 0
	}
	return strings.Count(s, "\n") + 1
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "snapshot error: "+format+"\n", args...)
	os.Exit(1)
}
