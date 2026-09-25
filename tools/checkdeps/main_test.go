// Copyright (c) 0xCarbon
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeTree(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for rel, content := range files {
		path := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

const testModFile = "module github.com/0xCarbon/example\n\ngo 1.27\n"

func TestAuditRootRequirements(t *testing.T) {
	for _, tc := range []struct {
		name, doc string
		valid     bool
	}{
		{"none", `{"Module":{"Path":"m"}}`, true},
		{"any-require", `{"Module":{"Path":"m"},"Require":[{"Path":"golang.org/x/net"}]}`, false},
		{"0xcarbon-require", `{"Module":{"Path":"m"},"Require":[{"Path":"github.com/0xCarbon/decimal"}]}`, false},
		{"replacement", `{"Module":{"Path":"m"},"Replace":[{"Old":{"Path":"a"},"New":{"Path":"../a"}}]}`, false},
		{"malformed", `{`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := auditRootRequirements([]byte(tc.doc))
			if (err == nil) != tc.valid {
				t.Fatalf("auditRootRequirements = %v, want valid %v", err, tc.valid)
			}
		})
	}
}

func TestAuditToolGraph(t *testing.T) {
	for _, tc := range []struct {
		name, graph string
		valid       bool
	}{
		{"main-only", `{"Path":"github.com/0xCarbon/example/tools/x","Main":true}`, true},
		{"official", `{"Path":"t","Main":true} {"Path":"golang.org/x/tools"}`, true},
		{"sibling", `{"Path":"t","Main":true} {"Path":"github.com/0xCarbon/decimal"}`, true},
		{"transitive-third-party", `{"Path":"t","Main":true} {"Path":"golang.org/x/perf"} {"Path":"github.com/aclements/go-moremath"}`, false},
		{"lookalike", `{"Path":"t","Main":true} {"Path":"golang.org/xevil/tools"}`, false},
		{"replacement", `{"Path":"t","Main":true} {"Path":"golang.org/x/tools","Replace":{"Path":"../tools"}}`, false},
		{"empty", ``, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := auditToolGraph([]byte(tc.graph)); (err == nil) != tc.valid {
				t.Fatalf("auditToolGraph = %v, want valid %v", err, tc.valid)
			}
		})
	}
}

func TestStdlibOnlyModulePasses(t *testing.T) {
	root := writeTree(t, map[string]string{
		"go.mod":             testModFile,
		"a.go":               "package example\nimport \"strings\"\nvar _ = strings.Repeat\n",
		"a_test.go":          "package example\nimport \"testing\"\nfunc TestA(t *testing.T) {}\n",
		"internal/x/x.go":    "package x\n",
		"tools/tool/main.go": "package main\nfunc main() {}\n",
	})
	if err := audit(root); err != nil {
		t.Fatalf("std-only module refused: %v", err)
	}
}

func TestRootRequirementFails(t *testing.T) {
	root := writeTree(t, map[string]string{
		"go.mod":        testModFile + "\nrequire github.com/example/legacy v0.0.0\n\nreplace github.com/example/legacy => ./legacy\n",
		"a.go":          "package example\n",
		"legacy/go.mod": "module github.com/example/legacy\n\ngo 1.27\n",
		"legacy/l.go":   "package legacy\n",
	})
	if err := audit(root); err == nil {
		t.Fatal("root module with a requirement accepted")
	}
}

func TestNestedModuleOutsideToolsFails(t *testing.T) {
	root := writeTree(t, map[string]string{
		"go.mod":       testModFile,
		"a.go":         "package example\n",
		"sub/go.mod":   "module github.com/0xCarbon/example/sub\n\ngo 1.27\n",
		"sub/sub.go":   "package sub\n",
		"tools/t/t.go": "package main\nfunc main() {}\n",
	})
	if err := audit(root); err == nil || !strings.Contains(err.Error(), "only under tools/") {
		t.Fatalf("nested module outside tools/ accepted: %v", err)
	}
}

func TestSourcePolicy(t *testing.T) {
	for _, tc := range []struct{ file, content, want string }{
		{"k.go", "package example\nimport \"unsafe\"\nvar _ = unsafe.Sizeof(0)\n", "unsafe"},
		{"tools/dist/k.go", "package dist\nimport \"unsafe\"\nvar _ = unsafe.Sizeof(0)\n", "tools/dist/k.go"},
		{"add_amd64.s", "TEXT ·add(SB),4,$0\n", "assembly"},
		{"blob.syso", "x", "assembly"},
		{"add_arm64.S", "TEXT ·add(SB),4,$0\n", "assembly"},
		{"helper.c", "int x;\n", "cgo sources"},
		{"c.go", "package example\n// int x;\nimport \"C\"\nvar _ = C.x\n", "cgo"},
	} {
		t.Run(tc.file, func(t *testing.T) {
			root := writeTree(t, map[string]string{"go.mod": testModFile, "a.go": "package example\n", tc.file: tc.content})
			if err := audit(root); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("%s accepted: %v", tc.file, err)
			}
		})
	}

	root := writeTree(t, map[string]string{
		"go.mod":              testModFile,
		"a.go":                "package example\n// import \"unsafe\" is only a comment\n",
		"x/testdata/p/p.go":   "package p\nimport \"unsafe\"\nvar _ = unsafe.Sizeof(0)\n",
		"x/testdata/p/go.mod": "module github.com/0xCarbon/example/x/testdata/p\n\ngo 1.27\n\nrequire github.com/hashicorp/go-msgpack/v2 v2.1.5\n",
	})
	if err := audit(root); err != nil {
		t.Fatalf("a testdata module or a comment was refused: %v", err)
	}
}
