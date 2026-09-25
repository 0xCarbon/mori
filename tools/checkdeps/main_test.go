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

func TestAuditRequirements(t *testing.T) {
	allowed := map[string]bool{"github.com/example/legacy": true}
	for _, tc := range []struct {
		name, doc string
		valid     bool
	}{
		{"no-requirements", `{}`, true},
		{"allowlisted", `{"Require":[{"Path":"github.com/example/legacy"}]}`, true},
		{"not-allowlisted", `{"Require":[{"Path":"golang.org/x/net"}]}`, false},
		{"replacement", `{"Replace":[{"Old":{"Path":"github.com/example/legacy"},"New":{"Path":"../legacy"}}]}`, false},
		{"malformed", `{`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := auditRequirements([]byte(tc.doc), allowed, map[string]bool{})
			if (err == nil) != tc.valid {
				t.Fatalf("auditRequirements = %v, want valid %v", err, tc.valid)
			}
		})
	}
}

func TestStdlibOnlyModulePasses(t *testing.T) {
	root := writeTree(t, map[string]string{
		"go.mod":             testModFile,
		"a.go":               "package example\nimport \"strings\"\nvar _ = strings.Repeat\n",
		"internal/x/x.go":    "package x\n",
		"tools/tool/main.go": "package main\nfunc main() {}\n",
	})
	if err := audit(root); err != nil {
		t.Fatalf("std-only module refused: %v", err)
	}
}

func TestStaleAllowlistEntryFails(t *testing.T) {
	root := writeTree(t, map[string]string{
		"go.mod":                      testModFile,
		"a.go":                        "package example\n",
		"project/deps-transition.txt": "github.com/example/gone removed in W9\n",
	})
	err := audit(root)
	if err == nil || !strings.Contains(err.Error(), "github.com/example/gone") {
		t.Fatalf("stale allowlist entry accepted: %v", err)
	}
}

func TestAllowlistEntryNeedsReason(t *testing.T) {
	root := writeTree(t, map[string]string{
		"go.mod":                      testModFile,
		"a.go":                        "package example\n",
		"project/deps-transition.txt": "github.com/example/legacy\n",
	})
	if err := audit(root); err == nil || !strings.Contains(err.Error(), "needs a reason") {
		t.Fatalf("allowlist entry without a reason accepted: %v", err)
	}
}

func TestReplacedDependencyFails(t *testing.T) {
	root := writeTree(t, map[string]string{
		"go.mod":                      testModFile + "\nrequire github.com/example/legacy v0.0.0\n\nreplace github.com/example/legacy => ./legacy\n",
		"a.go":                        "package example\nimport _ \"github.com/example/legacy\"\n",
		"legacy/go.mod":               "module github.com/example/legacy\n\ngo 1.27\n",
		"legacy/l.go":                 "package legacy\n",
		"project/deps-transition.txt": "github.com/example/legacy test fixture\n",
	})
	if err := audit(root); err == nil || !strings.Contains(err.Error(), "prohibited replacement") {
		t.Fatalf("replaced dependency accepted: %v", err)
	}
}

func TestSourcePolicy(t *testing.T) {
	for _, tc := range []struct{ file, content, want string }{
		{"k.go", "package example\nimport \"unsafe\"\nvar _ = unsafe.Sizeof(0)\n", "unsafe"},
		{"tools/dist/k.go", "package dist\nimport \"unsafe\"\nvar _ = unsafe.Sizeof(0)\n", "tools/dist/k.go"},
		{"add_amd64.s", "TEXT ·add(SB),4,$0\n", "assembly"},
		{"blob.syso", "x", "assembly"},
	} {
		t.Run(tc.file, func(t *testing.T) {
			root := writeTree(t, map[string]string{"go.mod": testModFile, "a.go": "package example\n", tc.file: tc.content})
			if err := audit(root); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("%s accepted: %v", tc.file, err)
			}
		})
	}

	root := writeTree(t, map[string]string{
		"go.mod":                    testModFile,
		"a.go":                      "package example\n// import \"unsafe\" is only a comment\n",
		"project/evidence/p/p.go":   "package p\nimport \"unsafe\"\nvar _ = unsafe.Sizeof(0)\n",
		"project/evidence/p/go.mod": "module github.com/0xCarbon/example/project/evidence/p\n\ngo 1.27\n",
	})
	if err := audit(root); err != nil {
		t.Fatalf("frozen evidence or a comment was refused: %v", err)
	}
}
