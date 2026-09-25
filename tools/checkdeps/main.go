// Copyright (c) 0xCarbon
// SPDX-License-Identifier: MPL-2.0

// Command checkdeps enforces the dependency policy:
//
//   - Library packages (every package outside tools/) depend only on the
//     standard library and on packages of this module.
//   - Every requirement in go.mod (direct or indirect, including those
//     only tests need) is an exception listed in the transition allowlist
//     (project/deps-transition.txt). With module graph pruning, go.mod
//     lists exactly the modules whose packages the module's packages and
//     tests build. The allowlist is a ratchet: an entry that is no longer
//     required fails the audit, so removing a dependency forces removing
//     its entry, and the file ends empty. Replacements are refused.
//   - Tools may only use golang.org/x/ and github.com/0xCarbon/ modules.
//   - No project-authored unsafe imports, assembly or object files.
//
// Frozen evidence under project/evidence/ (nested modules that pin the
// codecs being compared) is not maintained source and is skipped.
//
// Usage: go run ./tools/checkdeps (from the repository root).
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// allowlistPath is the transition allowlist, relative to the root.
const allowlistPath = "project/deps-transition.txt"

// modFile is the subset of `go mod edit -json` the audit reads.
type modFile struct {
	Require []struct{ Path string }
	Replace []struct{ Old, New struct{ Path string } }
}

// approvedForTools reports whether a tool may depend on path.
func approvedForTools(path string) bool {
	return strings.HasPrefix(path, "golang.org/x/") || strings.HasPrefix(path, "github.com/0xCarbon/")
}

// readAllowlist parses "<module path> <reason...>" lines; '#' starts a
// comment. A missing file is an empty allowlist.
func readAllowlist(root string) (map[string]bool, error) {
	f, err := os.Open(filepath.Join(root, allowlistPath))
	if errors.Is(err, os.ErrNotExist) {
		return map[string]bool{}, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	allowed := map[string]bool{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line, _, _ := strings.Cut(sc.Text(), "#")
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if len(fields) < 2 {
			return nil, fmt.Errorf("%s: entry %q needs a reason", allowlistPath, fields[0])
		}
		allowed[fields[0]] = true
	}
	return allowed, sc.Err()
}

// auditRequirements checks a `go mod edit -json` document. allowed holds
// the transition exceptions; used collects every allowed module required.
func auditRequirements(data []byte, allowed, used map[string]bool) error {
	var mf modFile
	if err := json.Unmarshal(data, &mf); err != nil {
		return fmt.Errorf("go.mod: %w", err)
	}
	for _, r := range mf.Replace {
		return fmt.Errorf("prohibited replacement for %s: %s", r.Old.Path, r.New.Path)
	}
	for _, r := range mf.Require {
		if !allowed[r.Path] {
			return fmt.Errorf("prohibited dependency %s", r.Path)
		}
		used[r.Path] = true
	}
	return nil
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

// auditPackages checks the non-test import closure of every package: a
// library package may reach only the standard library, this module and
// allowlisted modules; a tool may additionally reach approved modules.
func auditPackages(root, modPath string, allowed, used map[string]bool) error {
	out, err := goCmd(root, "list", "-deps", "-test", "-f", "{{if not .Standard}}{{.ImportPath}}|{{with .Module}}{{.Path}}{{end}}{{end}}", "./...")
	if err != nil {
		return err
	}
	// The closure of tool packages is audited separately so a tool-only
	// dependency is not mistaken for a library one.
	libOut, err := goCmd(root, "list", "-f", "{{.ImportPath}}", "./...")
	if err != nil {
		return err
	}
	var libs []string
	for line := range strings.Lines(string(libOut)) {
		p := strings.TrimSpace(line)
		if p != "" && !strings.HasPrefix(p, modPath+"/tools/") {
			libs = append(libs, p)
		}
	}
	libDeps := map[string]bool{}
	if len(libs) > 0 {
		deps, err := goCmd(root, append([]string{"list", "-deps", "-test", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}"}, libs...)...)
		if err != nil {
			return err
		}
		for line := range strings.Lines(string(deps)) {
			if p := strings.TrimSpace(line); p != "" {
				libDeps[p] = true
			}
		}
	}
	for line := range strings.Lines(string(out)) {
		pkg, mod, _ := strings.Cut(strings.TrimSpace(line), "|")
		if pkg == "" || mod == modPath || (mod == "" && strings.HasSuffix(pkg, ".test")) {
			continue // this module, or a synthesized test main
		}
		switch {
		case allowed[mod]:
			used[mod] = true
		case libDeps[pkg]:
			return fmt.Errorf("library package dependency %s (module %s) is not standard library", pkg, mod)
		case !approvedForTools(mod):
			return fmt.Errorf("tool dependency %s (module %s) is not golang.org/x or 0xCarbon", pkg, mod)
		}
	}
	return nil
}

// auditSources refuses unsafe imports and non-Go machine code in
// maintained source.
func auditSources(root string) error {
	return filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch filepath.ToSlash(rel) {
			case ".git", "project/evidence", "dist":
				return filepath.SkipDir
			}
			return nil
		}
		switch filepath.Ext(d.Name()) {
		case ".s", ".syso":
			return fmt.Errorf("%s: assembly or object files are prohibited", filepath.ToSlash(rel))
		case ".go":
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
			if err != nil {
				return fmt.Errorf("%s: parse imports: %w", filepath.ToSlash(rel), err)
			}
			for _, spec := range file.Imports {
				name, err := strconv.Unquote(spec.Path.Value)
				if err != nil {
					return fmt.Errorf("%s: import path: %w", filepath.ToSlash(rel), err)
				}
				if name == "unsafe" {
					return fmt.Errorf("%s: project-authored unsafe import prohibited", filepath.ToSlash(rel))
				}
			}
		}
		return nil
	})
}

func audit(root string) error {
	if err := auditSources(root); err != nil {
		return err
	}
	allowed, err := readAllowlist(root)
	if err != nil {
		return err
	}
	modOut, err := goCmd(root, "list", "-m")
	if err != nil {
		return err
	}
	modPath := strings.TrimSpace(string(modOut))
	requirements, err := goCmd(root, "mod", "edit", "-json")
	if err != nil {
		return err
	}
	used := map[string]bool{}
	if err := auditRequirements(requirements, allowed, used); err != nil {
		return err
	}
	if err := auditPackages(root, modPath, allowed, used); err != nil {
		return err
	}
	var stale []string
	for path := range allowed {
		if !used[path] {
			stale = append(stale, path)
		}
	}
	if len(stale) > 0 {
		slices.Sort(stale)
		return fmt.Errorf("%s lists modules that are no longer required; remove them: %s", allowlistPath, strings.Join(stale, ", "))
	}
	if len(allowed) > 0 {
		fmt.Printf("check-deps: passed with %d transition exception(s) in %s\n", len(allowed), allowlistPath)
		return nil
	}
	fmt.Println("check-deps: passed (standard library only)")
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
