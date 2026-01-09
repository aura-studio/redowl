package redowl

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Redis Cluster does not support PSUBSCRIBE.
// This regression test ensures redowl's non-test code does not start using PSubscribe.
func TestNoPSubscribeUsage(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, ".go") {
			continue
		}
		if strings.HasSuffix(name, "_test.go") {
			continue
		}

		b, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("ReadFile %s: %v", name, err)
		}

		// We intentionally check for the Go-redis method call form.
		if bytes.Contains(b, []byte(".PSubscribe(")) {
			t.Fatalf("%s uses PSubscribe; Redis Cluster does not support PSUBSCRIBE", filepath.Base(name))
		}
	}
}
