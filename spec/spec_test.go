package spec

import (
	"errors"
	"strings"
	"testing"
)

const valid = `{
  "apiVersion": "topicsync.aperiodic.io/v1",
  "cluster": "metrics",
  "defaults": {"partitions": 1, "replicationFactor": 3, "configs": {"cleanup.policy": "delete", "segment.bytes": "16777216"}},
  "topics": [
    {"name": "ohlcv.v1.binance-futures.1m", "configs": {"retention.bytes": "40316198"}},
    {"name": "trades.raw", "partitions": 6, "replicationFactor": 1, "configs": {"segment.bytes": "1048576"}}
  ],
  "protected": ["^brrrrr\\.leases$"],
  "prune": {"enabled": true, "patterns": ["^[a-z0-9_]+\\.v[0-9]+\\.[a-z0-9-]+\\.[0-9]+[smhd]$"]}
}`

func mustParse(t *testing.T, s string) *Desired {
	t.Helper()
	d, err := Parse(strings.NewReader(s))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	return d
}

func problems(t *testing.T, s string) []string {
	t.Helper()
	_, err := Parse(strings.NewReader(s))
	if err == nil {
		t.Fatalf("Parse accepted an invalid file:\n%s", s)
	}
	var v *ValidationError
	if !errors.As(err, &v) {
		t.Fatalf("error %T %v is not a ValidationError", err, err)
	}
	if !IsValidationError(err) {
		t.Fatal("IsValidationError is false for a ValidationError")
	}
	return v.Problems
}

func hasProblem(ps []string, sub string) bool {
	for _, p := range ps {
		if strings.Contains(p, sub) {
			return true
		}
	}
	return false
}

// The defaults fill what a topic leaves out, and a topic's own configs override the defaults' keys one by one, so
// a topic keeps every default key it does not set itself.
func TestDefaultsFillAndTopicsOverrideKeyByKey(t *testing.T) {
	d := mustParse(t, valid)
	o := d.Topics["ohlcv.v1.binance-futures.1m"]
	if o.Partitions != 1 || o.ReplicationFactor != 3 {
		t.Errorf("ohlcv: partitions %d rf %d, want the defaults 1 and 3", o.Partitions, o.ReplicationFactor)
	}
	want := map[string]string{"cleanup.policy": "delete", "segment.bytes": "16777216", "retention.bytes": "40316198"}
	if len(o.Configs) != len(want) {
		t.Errorf("ohlcv configs %v, want %v", o.Configs, want)
	}
	for k, v := range want {
		if o.Configs[k] != v {
			t.Errorf("ohlcv %s = %q, want %q", k, o.Configs[k], v)
		}
	}
	r := d.Topics["trades.raw"]
	if r.Partitions != 6 || r.ReplicationFactor != 1 {
		t.Errorf("trades.raw: partitions %d rf %d, want its own 6 and 1", r.Partitions, r.ReplicationFactor)
	}
	if r.Configs["segment.bytes"] != "1048576" || r.Configs["cleanup.policy"] != "delete" {
		t.Errorf("trades.raw configs %v: its own segment.bytes and the default cleanup.policy", r.Configs)
	}
	if d.Cluster != "metrics" || len(d.Topics) != 2 {
		t.Errorf("cluster %q, %d topics", d.Cluster, len(d.Topics))
	}
}

// A misspelt key must not silently fall back to a default: unknown fields are refused, anywhere in the file.
func TestUnknownFieldsAreRefused(t *testing.T) {
	for _, s := range []string{
		strings.Replace(valid, `"replicationFactor": 3`, `"replicationfactor": 3`, 1),
		strings.Replace(valid, `"partitions": 6`, `"partition": 6`, 1),
		strings.Replace(valid, `"prune": {`, `"pruning": {`, 1),
	} {
		if ps := problems(t, s); !hasProblem(ps, "unknown field") {
			t.Errorf("problems %v, want an unknown field", ps)
		}
	}
}

func TestEveryProblemIsReportedAtOnce(t *testing.T) {
	ps := problems(t, `{
	  "apiVersion": "topicsync.aperiodic.io/v2",
	  "cluster": " ",
	  "defaults": {"partitions": -1, "replicationFactor": -1},
	  "topics": [
	    {"name": ""},
	    {"name": "a/b"},
	    {"name": "ok"},
	    {"name": "ok"},
	    {"name": ".."},
	    {"name": "`+strings.Repeat("x", 250)+`"},
	    {"name": "c", "configs": {" k": "v", "k2": " v"}}
	  ],
	  "protected": ["("],
	  "prune": {"enabled": true, "patterns": ["unanchored", "^(bad$"]}
	}`)
	for _, want := range []string{
		`apiVersion "topicsync.aperiodic.io/v2"`, "cluster is empty", "defaults.partitions -1", "defaults.replicationFactor -1",
		"topics[0]: name is empty", "topics[1] (a/b): name may contain only", "topics[3] (ok): listed more than once",
		"cannot be . or ..", "250 characters", `config key " k"`, `config k2 value " v"`, "protected[0]",
		`prune.patterns[0] "unanchored": must be anchored`, `prune.patterns[1] "^(bad$"`,
		"topics[2] (ok): partitions must be positive", "replicationFactor must be positive",
	} {
		if !hasProblem(ps, want) {
			t.Errorf("no problem mentioning %q in:\n%s", want, strings.Join(ps, "\n"))
		}
	}
}

func TestPruneEnabledNeedsPatterns(t *testing.T) {
	ps := problems(t, `{"apiVersion": "topicsync.aperiodic.io/v1", "cluster": "c", "defaults": {"partitions": 1, "replicationFactor": 1},
	  "topics": [{"name": "a"}], "prune": {"enabled": true}}`)
	if !hasProblem(ps, "prune.enabled without prune.patterns") {
		t.Errorf("problems %v", ps)
	}
}

func TestMalformedJSONIsAValidationError(t *testing.T) {
	for _, s := range []string{``, `{`, `[]`, `{"apiVersion": 1}`, valid + `{}`} {
		problems(t, s)
	}
}

// The hash names the Kubernetes Job that applies a file: it must change with any change to the resolved state and
// with nothing else, so that a reformatted or reordered file does not rerun the Job, and a real change always does.
func TestHashIsTheResolvedStateNotTheBytes(t *testing.T) {
	a := mustParse(t, valid)
	reordered := mustParse(t, `{
	  "prune": {"patterns": ["^[a-z0-9_]+\\.v[0-9]+\\.[a-z0-9-]+\\.[0-9]+[smhd]$"], "enabled": true},
	  "protected": ["^brrrrr\\.leases$"],
	  "topics": [
	    {"configs": {"segment.bytes": "1048576", "cleanup.policy": "delete"}, "replicationFactor": 1, "partitions": 6, "name": "trades.raw"},
	    {"name": "ohlcv.v1.binance-futures.1m", "partitions": 1, "replicationFactor": 3,
	     "configs": {"retention.bytes": "40316198", "segment.bytes": "16777216", "cleanup.policy": "delete"}}
	  ],
	  "cluster": "metrics", "apiVersion": "topicsync.aperiodic.io/v1"
	}`)
	if a.Hash != reordered.Hash {
		t.Errorf("the same resolved state hashes %s and %s", a.Hash, reordered.Hash)
	}
	if len(a.Hash) != 64 {
		t.Errorf("hash %q is not a hex SHA-256", a.Hash)
	}
	for name, edit := range map[string][2]string{
		"a config value":    {`"40316198"`, `"40316199"`},
		"a partition count": {`"partitions": 6`, `"partitions": 7`},
		"the cluster":       {`"cluster": "metrics"`, `"cluster": "metrics2"`},
		"a topic name":      {`"trades.raw"`, `"trades.raw2"`},
		"prune":             {`"enabled": true`, `"enabled": false`},
		"protected":         {`"^brrrrr\\.leases$"`, `"^brrrrr\\.lease$"`},
		"a default":         {`"replicationFactor": 3`, `"replicationFactor": 2`},
	} {
		b := mustParse(t, strings.Replace(valid, edit[0], edit[1], 1))
		if b.Hash == a.Hash {
			t.Errorf("changing %s left the hash unchanged", name)
		}
	}
}

func TestInternalTopicsAreAlwaysProtected(t *testing.T) {
	d := mustParse(t, valid)
	for _, n := range []string{"__consumer_offsets", "_schemas", "_redpanda.audit_log", "brrrrr.leases"} {
		if !d.IsProtected(n) {
			t.Errorf("%s is not protected", n)
		}
	}
	if d.IsProtected("ohlcv.v1.okx-perps.1m") {
		t.Error("an ordinary topic is protected")
	}
}

func TestPrunableNeedsEnabledPatternUnlistedUnprotected(t *testing.T) {
	d := mustParse(t, valid)
	cases := map[string]bool{
		"ohlcv.v1.okx-perps.1m":       true,  // matches, not listed
		"ohlcv.v1.binance-futures.1m": false, // listed
		"trades.raw":                  false, // no pattern match
		"_ohlcv.v1.okx-perps.1m":      false, // protected (and no match)
		"brrrrr.leases":               false, // protected
	}
	for n, want := range cases {
		if got := d.Prunable(n); got != want {
			t.Errorf("Prunable(%s) = %v, want %v", n, got, want)
		}
	}
	off := mustParse(t, strings.Replace(valid, `"enabled": true`, `"enabled": false`, 1))
	if off.Prunable("ohlcv.v1.okx-perps.1m") {
		t.Error("a topic is prunable with prune disabled")
	}
}

func TestNamesAreSorted(t *testing.T) {
	d := mustParse(t, valid)
	n := d.Names()
	if len(n) != 2 || n[0] != "ohlcv.v1.binance-futures.1m" || n[1] != "trades.raw" {
		t.Errorf("Names() = %v", n)
	}
}

func TestLoadReadsAFile(t *testing.T) {
	p := t.TempDir() + "/topics.json"
	if err := writeFile(p, valid); err != nil {
		t.Fatal(err)
	}
	d, err := Load(p)
	if err != nil || len(d.Topics) != 2 {
		t.Fatalf("Load: %v", err)
	}
	if _, err := Load(p + ".missing"); err == nil {
		t.Error("Load of a missing file succeeded")
	}
}

func TestAnEmptyTopicListIsValidHere(t *testing.T) {
	// the planner refuses an empty list unless asked; the file itself may be empty
	d := mustParse(t, `{"apiVersion": "topicsync.aperiodic.io/v1", "cluster": "c", "topics": []}`)
	if len(d.Topics) != 0 {
		t.Fatal(len(d.Topics))
	}
}

// A `|` at the top level escapes ^...$: "^tmp\.a|tmp\.b$" would match "tmp.a-PRODUCTION-DATA". Patterns are compiled
// as a whole-name match, whatever their own anchors.
func TestPrunePatternsMatchWholeNamesOnly(t *testing.T) {
	d := mustParse(t, `{"apiVersion": "topicsync.aperiodic.io/v1", "cluster": "c", "defaults": {"partitions": 1, "replicationFactor": 1},
	  "topics": [{"name": "keep"}], "protected": ["^brrrrr\\.leases|other$"],
	  "prune": {"enabled": true, "patterns": ["^tmp\\.a|tmp\\.b$"]}}`)
	for name, want := range map[string]bool{"tmp.a": true, "tmp.b": true, "tmp.a-PRODUCTION-DATA": false, "prod.ohlcv.tmp.b": false} {
		if got := d.Prunable(name); got != want {
			t.Errorf("Prunable(%q) = %v, want %v", name, got, want)
		}
	}
	// protected patterns match as written: matching more protects more, the safe direction
	if !d.IsProtected("brrrrr.leases") || !d.IsProtected("brrrrr.leases-old") || !d.IsProtected("x.other") {
		t.Error("protected patterns do not match as written")
	}
}

// Internal topics (__consumer_offsets, _schemas, _redpanda.*) are the broker's: listing one would put the file's
// defaults on it (cleanup.policy=delete on a compacted _schemas loses the schemas). They are refused.
func TestInternalTopicsCannotBeListed(t *testing.T) {
	ps := problems(t, `{"apiVersion": "topicsync.aperiodic.io/v1", "cluster": "c", "defaults": {"partitions": 1, "replicationFactor": 1},
	  "topics": [{"name": "_schemas"}, {"name": "__consumer_offsets"}]}`)
	if !hasProblem(ps, "topics[0] (_schemas): names starting with _ are internal") || !hasProblem(ps, "topics[1] (__consumer_offsets)") {
		t.Errorf("%v", ps)
	}
}

// A reviewer reads the first "prune"; JSON's last-one-wins would apply the second.
func TestDuplicateKeysAreRefused(t *testing.T) {
	for _, s := range []string{
		`{"apiVersion": "topicsync.aperiodic.io/v1", "cluster": "c", "prune": {"enabled": false}, "topics": [{"name": "a", "partitions": 1, "replicationFactor": 1}], "prune": {"enabled": true, "patterns": ["^.*$"]}}`,
		`{"apiVersion": "topicsync.aperiodic.io/v1", "cluster": "c", "topics": [{"name": "a", "name": "b", "partitions": 1, "replicationFactor": 1}]}`,
		`{"apiVersion": "topicsync.aperiodic.io/v1", "cluster": "c", "topics": [{"name": "a", "partitions": 1, "replicationFactor": 1, "configs": {"retention.ms": "1", "retention.ms": "2"}}]}`,
	} {
		if ps := problems(t, s); !hasProblem(ps, "duplicate key") {
			t.Errorf("problems %v for %s", ps, s)
		}
	}
}

func TestEmptyOrNullConfigValuesAreRefused(t *testing.T) {
	for _, v := range []string{`null`, `""`} {
		ps := problems(t, `{"apiVersion": "topicsync.aperiodic.io/v1", "cluster": "c", "topics": [{"name": "a", "partitions": 1, "replicationFactor": 1, "configs": {"retention.ms": `+v+`}}]}`)
		if !hasProblem(ps, "retention.ms") {
			t.Errorf("%s: %v", v, ps)
		}
	}
}

func TestHashIgnoresTheOrderOfPatterns(t *testing.T) {
	a := mustParse(t, `{"apiVersion": "topicsync.aperiodic.io/v1", "cluster": "c", "topics": [{"name": "a", "partitions": 1, "replicationFactor": 1}],
	  "protected": ["^x$", "^y$"], "prune": {"enabled": true, "patterns": ["^m\\..*$", "^n\\..*$"]}}`)
	b := mustParse(t, `{"apiVersion": "topicsync.aperiodic.io/v1", "cluster": "c", "topics": [{"name": "a", "partitions": 1, "replicationFactor": 1}],
	  "protected": ["^y$", "^x$"], "prune": {"enabled": true, "patterns": ["^n\\..*$", "^m\\..*$"]}}`)
	if a.Hash != b.Hash {
		t.Error("reordering patterns changed the hash")
	}
}

// Pruning is off unless the file turns it on: a file without a prune section, or with prune disabled, prunes nothing.
func TestPruningIsOffByDefault(t *testing.T) {
	d := mustParse(t, `{"apiVersion": "topicsync.aperiodic.io/v1", "cluster": "c", "topics": [{"name": "a", "partitions": 1, "replicationFactor": 1}]}`)
	if d.Prune.Enabled || d.Prunable("anything.else") {
		t.Error("a file without a prune section prunes")
	}
}
