package spec

import (
	"bytes"
	"encoding/json"
	"testing"
)

// Parse never panics, and a file it accepts resolves to the same hash when re-encoded from its resolved form: the
// hash depends on the meaning only.
func FuzzParse(f *testing.F) {
	f.Add([]byte(valid))
	f.Add([]byte(`{"apiVersion": "topicsync.aperiodic.io/v1", "cluster": "c", "topics": []}`))
	f.Add([]byte(`{"apiVersion": "topicsync.aperiodic.io/v1", "cluster": "c", "defaults": {"partitions": 3},` +
		`"topics": [{"name": "a", "replicationFactor": 2, "configs": {"x": "y"}}], "prune": {"enabled": false}}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		d, err := Parse(bytes.NewReader(data))
		if err != nil {
			if !IsValidationError(err) {
				t.Fatalf("a non-validation error from Parse: %v", err)
			}
			return
		}
		file := File{APIVersion: APIVersion, Cluster: d.Cluster, Prune: PruneRule{Enabled: d.Prune.Enabled}}
		file.Protected = append(file.Protected, d.ProtectedSource[1:]...)
		file.Prune.Patterns = append(file.Prune.Patterns, d.Prune.Source...)
		for _, n := range d.Names() {
			t := d.Topics[n]
			file.Topics = append(file.Topics, Topic{Name: n, Partitions: t.Partitions, ReplicationFactor: t.ReplicationFactor, Configs: t.Configs})
		}
		b, err := json.Marshal(file)
		if err != nil {
			t.Fatal(err)
		}
		again, err := Parse(bytes.NewReader(b))
		if err != nil {
			t.Fatalf("the resolved form of an accepted file is refused: %v\n%s", err, b)
		}
		if again.Hash != d.Hash {
			t.Fatalf("hash %s, re-encoded %s", d.Hash, again.Hash)
		}
	})
}
