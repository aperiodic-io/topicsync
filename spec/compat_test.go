package spec

import (
	"os"
	"path/filepath"
	"testing"
)

// The hash names the Kubernetes Job that applies a file. A topicsync release that hashed an unchanged file differently
// would rerun every cluster's Job, and one that read an old file differently would change clusters on upgrade. These
// files, written for apiVersion v1, must keep parsing to the same hash in every later release: change a recorded hash
// only together with a new apiVersion.
var compatHashes = map[string]string{
	"e2e-adopt.json": "26740685e15c97b909b7297dcf62e66a40b09507dbe29654b8e9b9f80dd93c23",
	"full.json":      "541ccfc765547ab74525d955d0687f04a1ea80d7b02202cdf71997b4d3a92566",
}

func TestV1FilesKeepTheirHashAcrossReleases(t *testing.T) {
	files, err := filepath.Glob("testdata/compat/v1/*.json")
	if err != nil || len(files) != len(compatHashes) {
		t.Fatalf("%d fixtures for %d recorded hashes (%v)", len(files), len(compatHashes), err)
	}
	for _, f := range files {
		d, err := Load(f)
		if err != nil {
			t.Errorf("%s no longer parses: %v", f, err)
			continue
		}
		if want := compatHashes[filepath.Base(f)]; d.Hash != want {
			t.Errorf("%s hashes %s, recorded %s", f, d.Hash, want)
		}
		if d.ShortHash() != d.Hash[:10] {
			t.Error("ShortHash")
		}
	}
	if _, err := os.Stat("testdata/compat/v1"); err != nil {
		t.Fatal(err)
	}
}
