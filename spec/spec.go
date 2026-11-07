// Package spec is topicsync's desired-state file: the topics one Kafka cluster must have, with their partition
// counts, replication factors and configs, and the opt-in rules under which topicsync may delete a topic.
//
// The file is JSON, rendered by a generator and reviewed in a pull request:
//
//	{
//	  "apiVersion": "topicsync.aperiodic.io/v1",
//	  "cluster": "redpanda-metrics-out",
//	  "defaults": {"partitions": 1, "replicationFactor": 3, "configs": {"cleanup.policy": "delete"}},
//	  "topics": [
//	    {"name": "ohlcv.v1.binance-futures.1m", "configs": {"retention.bytes": "40316198"}}
//	  ],
//	  "protected": ["^_"],
//	  "prune": {"enabled": false, "patterns": []}
//	}
//
// A topic's partitions, replication factor and configs fall back to the defaults; a topic's own config keys
// override the defaults' keys one by one. Only the keys a topic ends up with are managed: any other config of the
// topic on the cluster is left as it is.
package spec

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
)

// APIVersion is the only apiVersion this build reads. A file of another version is refused rather than misread.
const APIVersion = "topicsync.aperiodic.io/v1"

// MaxNameLength is Kafka's limit on a topic name.
const MaxNameLength = 249

var validName = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

// File is the desired-state file as written.
type File struct {
	APIVersion string    `json:"apiVersion"`
	Cluster    string    `json:"cluster"`
	Defaults   Defaults  `json:"defaults"`
	Topics     []Topic   `json:"topics"`
	Protected  []string  `json:"protected,omitempty"`
	Prune      PruneRule `json:"prune"`
}

// Defaults apply to every topic that does not set its own value.
type Defaults struct {
	Partitions        int32             `json:"partitions,omitempty"`
	ReplicationFactor int16             `json:"replicationFactor,omitempty"`
	Configs           map[string]string `json:"configs,omitempty"`
}

// Topic is one topic as written; zero values fall back to the defaults.
type Topic struct {
	Name              string            `json:"name"`
	Partitions        int32             `json:"partitions,omitempty"`
	ReplicationFactor int16             `json:"replicationFactor,omitempty"`
	Configs           map[string]string `json:"configs,omitempty"`
}

// PruneRule says which topics the cluster may lose when they are not listed. It is off unless enabled, and even
// then `apply` deletes only when also run with --prune: two opt-ins, one in the reviewed file and one in the
// command. A topic is pruned only if its name matches one of Patterns and no protected pattern.
type PruneRule struct {
	Enabled  bool     `json:"enabled"`
	Patterns []string `json:"patterns,omitempty"`
}

// Desired is the validated file: every topic resolved against the defaults.
type Desired struct {
	Cluster string
	Topics  map[string]DesiredTopic
	// Protected are the protected patterns compiled as whole-name matches (the built-in one first); ProtectedSource
	// and Prune.Source are the patterns as written, which the hash and re-encoding use.
	Protected       []*regexp.Regexp
	ProtectedSource []string
	Prune           Prune
	// Hash is the SHA-256 of the canonical form of the resolved state: two files that mean the same topics have the
	// same hash, whatever their key order or whitespace. A Kubernetes Job named after it runs once per change.
	Hash string
}

// DesiredTopic is a topic with its defaults applied.
type DesiredTopic struct {
	Name              string
	Partitions        int32
	ReplicationFactor int16
	Configs           map[string]string
}

// Prune is the compiled prune rule.
type Prune struct {
	Enabled  bool
	Patterns []*regexp.Regexp
	Source   []string
}

// BuiltinProtected are never deleted, whatever the file says: Kafka's and Redpanda's own topics (`__consumer_offsets`,
// `_schemas`, `_redpanda.*`), all of which start with an underscore.
var BuiltinProtected = []*regexp.Regexp{regexp.MustCompile(`^_`)}

const builtinProtectedSource = `^_`

// ValidationError lists every problem found in a file, so that one run reports them all.
type ValidationError struct {
	Problems []string
}

func (e *ValidationError) Error() string {
	return "invalid desired-state file:\n  - " + strings.Join(e.Problems, "\n  - ")
}

// Load reads and validates the file at path.
func Load(path string) (*Desired, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return Parse(f)
}

// Parse reads and validates a desired-state file. Unknown fields are refused: a misspelt key (`replicationfactor`)
// must not silently fall back to a default.
func Parse(r io.Reader) (*Desired, error) {
	data, err := io.ReadAll(io.LimitReader(r, 64<<20))
	if err != nil {
		return nil, err
	}
	// encoding/json matches field names case-insensitively, so "replicationfactor" would fill replicationFactor:
	// check every key's exact spelling first
	if problems := checkKeys(data); len(problems) > 0 {
		return nil, &ValidationError{Problems: problems}
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var f File
	if err := dec.Decode(&f); err != nil {
		return nil, &ValidationError{Problems: []string{"not valid JSON for this apiVersion: " + err.Error()}}
	}
	if dec.More() {
		return nil, &ValidationError{Problems: []string{"trailing data after the JSON document"}}
	}
	return f.Resolve()
}

// Resolve validates the file and applies the defaults.
func (f File) Resolve() (*Desired, error) {
	var problems []string
	add := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }

	if f.APIVersion != APIVersion {
		add("apiVersion %q is not %q", f.APIVersion, APIVersion)
	}
	if strings.TrimSpace(f.Cluster) == "" {
		add("cluster is empty: name the cluster this file is for (it labels logs and metrics)")
	}
	if f.Defaults.Partitions < 0 {
		add("defaults.partitions %d is negative", f.Defaults.Partitions)
	}
	if f.Defaults.ReplicationFactor < 0 {
		add("defaults.replicationFactor %d is negative", f.Defaults.ReplicationFactor)
	}
	for k, v := range f.Defaults.Configs {
		if msg := checkConfig(k, v); msg != "" {
			add("defaults.configs: %s", msg)
		}
	}

	d := &Desired{Cluster: f.Cluster, Topics: make(map[string]DesiredTopic, len(f.Topics))}
	for i, t := range f.Topics {
		where := fmt.Sprintf("topics[%d]", i)
		if t.Name != "" {
			where = fmt.Sprintf("topics[%d] (%s)", i, t.Name)
		}
		if msg := checkName(t.Name); msg != "" {
			add("%s: %s", where, msg)
			continue
		}
		if strings.HasPrefix(t.Name, "_") {
			// listing one would put the file's defaults on it: cleanup.policy=delete on a compacted _schemas
			add("%s: names starting with _ are internal topics (the broker's own), which topicsync does not manage", where)
			continue
		}
		if _, dup := d.Topics[t.Name]; dup {
			add("%s: listed more than once", where)
			continue
		}
		dt := DesiredTopic{Name: t.Name, Partitions: t.Partitions, ReplicationFactor: t.ReplicationFactor}
		if dt.Partitions == 0 {
			dt.Partitions = f.Defaults.Partitions
		}
		if dt.ReplicationFactor == 0 {
			dt.ReplicationFactor = f.Defaults.ReplicationFactor
		}
		if dt.Partitions <= 0 {
			add("%s: partitions must be positive (set it on the topic or in defaults)", where)
		}
		if dt.ReplicationFactor <= 0 {
			add("%s: replicationFactor must be positive (set it on the topic or in defaults)", where)
		}
		dt.Configs = make(map[string]string, len(f.Defaults.Configs)+len(t.Configs))
		for k, v := range f.Defaults.Configs {
			dt.Configs[k] = v
		}
		for k, v := range t.Configs {
			if msg := checkConfig(k, v); msg != "" {
				add("%s: configs: %s", where, msg)
				continue
			}
			dt.Configs[k] = v
		}
		d.Topics[t.Name] = dt
	}

	d.Protected = append(d.Protected, BuiltinProtected...)
	d.ProtectedSource = append(d.ProtectedSource, builtinProtectedSource)
	for i, p := range f.Protected {
		// as written: a protected pattern that matches more protects more (the safe direction), unlike a prune pattern
		re, err := regexp.Compile(p)
		if err != nil {
			add("protected[%d] %q: %v", i, p, err)
			continue
		}
		d.Protected = append(d.Protected, re)
		d.ProtectedSource = append(d.ProtectedSource, p)
	}

	d.Prune.Enabled = f.Prune.Enabled
	for i, p := range f.Prune.Patterns {
		re, err := wholeName(p)
		if err != nil {
			add("prune.patterns[%d] %q: %v", i, p, err)
			continue
		}
		if !strings.HasPrefix(p, "^") || !strings.HasSuffix(p, "$") {
			add("prune.patterns[%d] %q: must be anchored with ^ and $, so that it cannot match more than intended", i, p)
			continue
		}
		d.Prune.Patterns = append(d.Prune.Patterns, re)
		d.Prune.Source = append(d.Prune.Source, p)
	}
	if f.Prune.Enabled && len(f.Prune.Patterns) == 0 {
		add("prune.enabled without prune.patterns: name the topics that may be pruned")
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		return nil, &ValidationError{Problems: problems}
	}
	d.Hash = d.canonicalHash()
	return d, nil
}

// wholeName compiles a pattern to match whole names only: "^a|b$" written by hand matches "a..." or "...b", so the
// pattern is wrapped in ^(?:...)$ whatever its own anchors.
func wholeName(p string) (*regexp.Regexp, error) {
	if _, err := regexp.Compile(p); err != nil {
		return nil, err
	}
	return regexp.Compile("^(?:" + p + ")$")
}

func checkName(name string) string {
	switch {
	case name == "":
		return "name is empty"
	case name == "." || name == "..":
		return "name cannot be . or .."
	case len(name) > MaxNameLength:
		return fmt.Sprintf("name is %d characters, more than Kafka's %d", len(name), MaxNameLength)
	case !validName.MatchString(name):
		return "name may contain only ASCII letters, digits, '.', '_' and '-'"
	}
	return ""
}

func checkConfig(key, value string) string {
	switch {
	case strings.TrimSpace(key) == "" || key != strings.TrimSpace(key):
		return fmt.Sprintf("config key %q is empty or has surrounding whitespace", key)
	case value == "":
		return fmt.Sprintf("config %s has an empty (or null) value: leave the key out to keep the broker's", key)
	case value != strings.TrimSpace(value):
		return fmt.Sprintf("config %s value %q has surrounding whitespace", key, value)
	}
	return ""
}

// IsProtected reports whether a topic may never be deleted by topicsync.
func (d *Desired) IsProtected(name string) bool {
	for _, re := range d.Protected {
		if re.MatchString(name) {
			return true
		}
	}
	return false
}

// Prunable reports whether a topic the file does not list may be pruned: prune enabled, the name matched by a prune
// pattern, not protected, not listed.
func (d *Desired) Prunable(name string) bool {
	if !d.Prune.Enabled || d.IsProtected(name) {
		return false
	}
	if _, listed := d.Topics[name]; listed {
		return false
	}
	for _, re := range d.Prune.Patterns {
		if re.MatchString(name) {
			return true
		}
	}
	return false
}

// MatchesPrunePattern reports whether a name is matched by a prune pattern, listed or not.
func (d *Desired) MatchesPrunePattern(name string) bool {
	for _, re := range d.Prune.Patterns {
		if re.MatchString(name) {
			return true
		}
	}
	return false
}

// Names returns the desired topics' names, sorted.
func (d *Desired) Names() []string {
	names := make([]string, 0, len(d.Topics))
	for n := range d.Topics {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func (d *Desired) canonicalHash() string {
	type ct struct {
		Name    string     `json:"n"`
		P       int32      `json:"p"`
		RF      int16      `json:"r"`
		Configs [][]string `json:"c"`
	}
	type canon struct {
		Cluster   string   `json:"cluster"`
		Topics    []ct     `json:"topics"`
		Protected []string `json:"protected"`
		Prune     bool     `json:"prune"`
		Patterns  []string `json:"patterns"`
	}
	c := canon{Cluster: d.Cluster, Prune: d.Prune.Enabled}
	for _, n := range d.Names() {
		t := d.Topics[n]
		keys := make([]string, 0, len(t.Configs))
		for k := range t.Configs {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		entry := ct{Name: n, P: t.Partitions, RF: t.ReplicationFactor}
		for _, k := range keys {
			entry.Configs = append(entry.Configs, []string{k, t.Configs[k]})
		}
		c.Topics = append(c.Topics, entry)
	}
	c.Protected = append(c.Protected, d.ProtectedSource...)
	c.Patterns = append(c.Patterns, d.Prune.Source...)
	// a set of patterns, whatever order the file lists them in
	sort.Strings(c.Protected)
	sort.Strings(c.Patterns)
	b, err := json.Marshal(c)
	if err != nil {
		panic(err) // plain values only
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// IsValidationError reports whether err is a problem with the file rather than with the cluster.
func IsValidationError(err error) bool {
	var v *ValidationError
	return errors.As(err, &v)
}

// schema is the file's structure: the keys allowed at each level; a nil map allows any key (configs).
type schema map[string]schema

var (
	topicSchema = schema{"name": {}, "partitions": {}, "replicationFactor": {}, "configs": nil}
	fileSchema  = schema{
		"apiVersion": {}, "cluster": {},
		"defaults":  {"partitions": {}, "replicationFactor": {}, "configs": nil},
		"topics":    topicSchema,
		"protected": {},
		"prune":     {"enabled": {}, "patterns": {}},
	}
)

// checkKeys reports every object key whose spelling is not exactly one the schema allows. Malformed JSON is left to
// the decoder, which reports it.
func checkKeys(data []byte) []string {
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		return nil
	}
	problems := duplicateKeys(data)
	var walk func(v any, s schema, path string)
	walk = func(v any, s schema, path string) {
		switch x := v.(type) {
		case map[string]any:
			if s == nil {
				return
			}
			for k, child := range x {
				sub, ok := s[k]
				if !ok {
					problems = append(problems, fmt.Sprintf("%sunknown field %q", path, k))
					continue
				}
				walk(child, sub, path+k+".")
			}
		case []any:
			for i, child := range x {
				walk(child, s, fmt.Sprintf("%s[%d].", strings.TrimSuffix(path, "."), i))
			}
		}
	}
	walk(v, fileSchema, "")
	sort.Strings(problems)
	return problems
}

// ShortHash is the first 10 hex digits of Hash: enough to tell versions of one file apart, short enough for a
// Kubernetes object name (topicsync-<cluster>-<short hash>).
func (d *Desired) ShortHash() string { return d.Hash[:10] }

// duplicateKeys reports every object key given twice in one object: JSON decoders keep the last, while a reviewer
// reads the first.
func duplicateKeys(data []byte) []string {
	dec := json.NewDecoder(bytes.NewReader(data))
	var problems []string
	var walk func(path string) error
	walk = func(path string) error {
		tok, err := dec.Token()
		if err != nil {
			return err
		}
		switch tok {
		case json.Delim('{'):
			seen := map[string]bool{}
			for dec.More() {
				kt, err := dec.Token()
				if err != nil {
					return err
				}
				k, _ := kt.(string)
				if seen[k] {
					problems = append(problems, fmt.Sprintf("%sduplicate key %q", path, k))
				}
				seen[k] = true
				if err := walk(path + k + "."); err != nil {
					return err
				}
			}
			_, err = dec.Token()
			return err
		case json.Delim('['):
			for i := 0; dec.More(); i++ {
				if err := walk(fmt.Sprintf("%s[%d].", strings.TrimSuffix(path, "."), i)); err != nil {
					return err
				}
			}
			_, err = dec.Token()
			return err
		}
		return nil
	}
	if err := walk(""); err != nil {
		return nil // malformed: the decoder reports it
	}
	return problems
}
