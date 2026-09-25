// Copyright (c) 0xCarbon
// SPDX-License-Identifier: MPL-2.0

// Command checkdeps enforces the dependency policy:
//
//   - The root module (the library, its tests and the in-module tools)
//     requires no module at all: its go.mod has no require line, and the
//     import closure of every package, tests included, holds only the
//     standard library and this module.
//   - A tool that needs more lives in its own module under tools/; such a
//     module may depend, transitively, only on golang.org/x/ and
//     github.com/0xCarbon/ modules, without replacements.
//   - No project-authored unsafe or cgo imports, assembly, object or
//     cgo source files.
//
// testdata directories are skipped, as the go command skips them: they may
// hold nested modules that pin the implementations Mori is checked against
// (internal/wire/testdata/oracle runs go-msgpack).
//
// Usage: go run ./tools/checkdeps (from the repository root).
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

// modFile is the subset of `go mod edit -json` the audit reads.
type modFile struct {
	Module  struct{ Path string }
	Require []struct{ Path string }
	Replace []struct{ Old, New struct{ Path string } }
}

// module is one entry of `go list -m -json all`.
type module struct {
	Path    string
	Main    bool
	Replace *module
}

// approvedForTools reports whether a tool module may depend on path.
func approvedForTools(path string) bool {
	return strings.HasPrefix(path, "golang.org/x/") || strings.HasPrefix(path, "github.com/0xCarbon/")
}

func goCmd(dir string, args ...string) ([]byte, error) {
	cmd := exec.Command("go", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOWORK=off", "GOTOOLCHAIN=local", "GOFLAGS=-mod=readonly")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("go %s failed: %w\n%s", strings.Join(args, " "), err, stderr.String())
	}
	return out, nil
}

// auditRootRequirements refuses any require or replace in the root go.mod
// and returns the module path.
func auditRootRequirements(data []byte) (string, error) {
	var mf modFile
	if err := json.Unmarshal(data, &mf); err != nil {
		return "", fmt.Errorf("go.mod: %w", err)
	}
	for _, r := range mf.Replace {
		return "", fmt.Errorf("prohibited replacement for %s: %s", r.Old.Path, r.New.Path)
	}
	for _, r := range mf.Require {
		return "", fmt.Errorf("prohibited dependency %s: the root module requires only the standard library", r.Path)
	}
	return mf.Module.Path, nil
}

// auditRootPackages checks that every package of the root module, tests
// included, reaches only the standard library and this module.
func auditRootPackages(root, modPath string) error {
	out, err := goCmd(root, "list", "-deps", "-test", "-f", "{{if not .Standard}}{{.ImportPath}}|{{with .Module}}{{.Path}}{{end}}{{end}}", "./...")
	if err != nil {
		return err
	}
	for line := range strings.Lines(string(out)) {
		pkg, mod, _ := strings.Cut(strings.TrimSpace(line), "|")
		if pkg == "" || mod == modPath || (mod == "" && strings.HasSuffix(pkg, ".test")) {
			continue // this module, or a synthesized test main
		}
		return fmt.Errorf("package dependency %s (module %s) is not standard library", pkg, mod)
	}
	return nil
}

// auditToolGraph checks a `go list -m -json all` stream of a tool module.
func auditToolGraph(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	count := 0
	for {
		var m module
		if err := dec.Decode(&m); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("module graph: %w", err)
		}
		count++
		if m.Main {
			continue
		}
		if m.Replace != nil {
			return fmt.Errorf("prohibited replacement for %s: %s", m.Path, m.Replace.Path)
		}
		if !approvedForTools(m.Path) {
			return fmt.Errorf("prohibited tool dependency %s", m.Path)
		}
	}
	if count == 0 {
		return errors.New("empty module graph")
	}
	return nil
}

// walk audits maintained sources and collects nested tool module
// directories.
func walk(root string) (toolModules []string, err error) {
	err = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		slash := filepath.ToSlash(rel)
		if d.IsDir() {
			if slash == ".git" || slash == "dist" || d.Name() == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		switch filepath.Ext(d.Name()) {
		case ".s", ".S", ".sx", ".syso":
			return fmt.Errorf("%s: assembly or object files are prohibited", slash)
		case ".c", ".cc", ".cpp", ".cxx", ".h", ".hh", ".hpp", ".m", ".f", ".F", ".for", ".f90":
			return fmt.Errorf("%s: cgo sources are prohibited", slash)
		case ".go":
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
			if err != nil {
				return fmt.Errorf("%s: parse imports: %w", slash, err)
			}
			for _, spec := range file.Imports {
				name, err := strconv.Unquote(spec.Path.Value)
				if err != nil {
					return fmt.Errorf("%s: import path: %w", slash, err)
				}
				switch name {
				case "unsafe":
					return fmt.Errorf("%s: project-authored unsafe import prohibited", slash)
				case "C":
					return fmt.Errorf("%s: cgo (import \"C\") prohibited", slash)
				}
			}
		}
		if d.Name() == "go.mod" && slash != "go.mod" {
			if !strings.HasPrefix(slash, "tools/") {
				return fmt.Errorf("%s: nested modules are allowed only under tools/", slash)
			}
			toolModules = append(toolModules, filepath.Dir(path))
		}
		return nil
	})
	return toolModules, err
}

func audit(root string) error {
	toolModules, err := walk(root)
	if err != nil {
		return err
	}
	edit, err := goCmd(root, "mod", "edit", "-json")
	if err != nil {
		return err
	}
	modPath, err := auditRootRequirements(edit)
	if err != nil {
		return err
	}
	if err := auditRootPackages(root, modPath); err != nil {
		return err
	}
	for _, dir := range toolModules {
		graph, err := goCmd(dir, "list", "-m", "-json", "all")
		if err != nil {
			return err
		}
		if err := auditToolGraph(graph); err != nil {
			rel, _ := filepath.Rel(root, dir)
			return fmt.Errorf("%s: %w", filepath.ToSlash(rel), err)
		}
	}
	fmt.Printf("check-deps: passed (standard library only; %d tool module(s))\n", len(toolModules))
	return nil
}

func main() {
	root, err := os.Getwd()
	if err == nil {
		err = audit(root)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "check-deps:", err)
		os.Exit(1)
	}
}
