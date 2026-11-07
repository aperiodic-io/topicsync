// Package plan computes what topicsync would change on a cluster: a pure function of the desired state and the
// observed one, so that every rule can be tested without a broker.
//
// The rules:
//   - a listed topic that is missing is created with its partitions, replication factor and configs;
//   - a listed topic's config key whose value differs is set; config keys the file does not name are never touched;
//   - a listed topic with fewer partitions than desired gets more; one with more is reported, never shrunk (Kafka
//     cannot);
//   - a replication factor that differs is reported, never changed;
//   - a topic that is not listed is left alone, unless pruning is enabled in the file AND requested by the caller,
//     its name matches a prune pattern and no protected pattern, and the number and share of deletions stay under
//     the caller's caps.
package plan

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/aperiodic-io/topicsync/internal/cluster"
	"github.com/aperiodic-io/topicsync/spec"
)

// Kind is an action's kind.
type Kind string

const (
	Create        Kind = "create"
	AlterConfigs  Kind = "alter_configs"
	AddPartitions Kind = "add_partitions"
	Delete        Kind = "delete"
)

// Kinds lists every kind, in execution order.
var Kinds = []Kind{Create, AlterConfigs, AddPartitions, Delete}

// ConfigChange is one config key of an AlterConfigs action.
type ConfigChange struct {
	Key  string `json:"key"`
	From string `json:"from"`
	// FromSource says whether the old value was set on the topic or came from a default.
	FromSource string `json:"fromSource"`
	To         string `json:"to"`
}

// Action is one change to the cluster.
type Action struct {
	Kind  Kind   `json:"kind"`
	Topic string `json:"topic"`
	// Create: what is created.
	Partitions        int32             `json:"partitions,omitempty"`
	ReplicationFactor int16             `json:"replicationFactor,omitempty"`
	Configs           map[string]string `json:"configs,omitempty"`
	// AlterConfigs: what changes, key by key.
	Changes []ConfigChange `json:"changes,omitempty"`
	// AddPartitions: from how many to how many.
	FromPartitions int32 `json:"fromPartitions,omitempty"`
	ToPartitions   int32 `json:"toPartitions,omitempty"`
}

// WarningKind is a warning's kind.
type WarningKind string

const (
	// PartitionsAboveDesired: the topic has more partitions than desired; Kafka cannot remove partitions.
	PartitionsAboveDesired WarningKind = "partitions_above_desired"
	// ReplicationFactorDiffers: the topic's replication factor is not the desired one; topicsync does not reassign
	// replicas.
	ReplicationFactorDiffers WarningKind = "replication_factor_differs"
	// SensitiveConfig: a desired config is sensitive on the broker, so its value cannot be compared.
	SensitiveConfig WarningKind = "sensitive_config"
	// PruneNotRequested: a topic would be pruned, but pruning was not requested on this run.
	PruneNotRequested WarningKind = "prune_not_requested"
	// UnreportedConfig: the broker does not report a desired config of an existing topic (it ignores the key, or does
	// not know it): setting it would be planned again on every run, so it is reported instead.
	UnreportedConfig WarningKind = "unreported_config"
)

// Warning is something topicsync will not change but reports.
type Warning struct {
	Kind    WarningKind `json:"kind"`
	Topic   string      `json:"topic"`
	Message string      `json:"message"`
}

// Plan is what a run changes.
type Plan struct {
	Cluster   string    `json:"cluster"`
	Hash      string    `json:"specHash"`
	Actions   []Action  `json:"actions"`
	Warnings  []Warning `json:"warnings"`
	Unchanged int       `json:"unchanged"`
	Desired   int       `json:"desired"`
	Observed  int       `json:"observed"`
	// Unmanaged counts the cluster's topics the file does not list, other than internal and protected ones: what
	// topicsync leaves alone (or prunes, when asked).
	Unmanaged int `json:"unmanaged"`
}

// Options are the caller's choices for a plan.
type Options struct {
	// Prune requests deletion of prunable topics (the file must enable pruning too).
	Prune bool
	// PruneMaxDeletes caps how many topics one run may prune; above it the plan is refused. 0 means no deletes.
	PruneMaxDeletes int
	// PruneMaxFraction caps the share of the cluster's topics matching a prune pattern that one run may prune
	// (0 < f <= 1); above it the plan is refused. It catches a truncated or wrong file.
	PruneMaxFraction float64
	// AllowEmpty accepts a file that lists no topic. Without it such a file is refused: an empty file is far more
	// likely a rendering bug than an intent.
	AllowEmpty bool
}

// DefaultOptions are the options of a run that does not prune.
func DefaultOptions() Options {
	return Options{PruneMaxDeletes: 50, PruneMaxFraction: 0.25}
}

var (
	// ErrEmpty is the error for a file that lists no topic.
	ErrEmpty = errors.New("the desired-state file lists no topic (pass --allow-empty if that is intended)")
	// ErrPruneNotEnabled is the error for a prune requested by the caller but not enabled in the file.
	ErrPruneNotEnabled = errors.New("--prune was given, but the desired-state file does not enable pruning (prune.enabled)")
)

// PruneLimitError is the error for a prune larger than the caller's caps.
type PruneLimitError struct {
	Deletes, Max, Matching int
	MaxFraction            float64
	Topics                 []string
}

func (e *PruneLimitError) Error() string {
	shown := e.Topics
	if len(shown) > 10 {
		shown = shown[:10]
	}
	return fmt.Sprintf("refusing to prune %d topics (cap %d, or %.0f%% of the %d topics matching a prune pattern): "+
		"is the desired-state file complete? First ones: %s", e.Deletes, e.Max, e.MaxFraction*100, e.Matching, strings.Join(shown, ", "))
}

// Compute plans the changes that make the observed cluster match the desired state.
func Compute(d *spec.Desired, s cluster.State, o Options) (*Plan, error) {
	if len(d.Topics) == 0 && !o.AllowEmpty {
		return nil, ErrEmpty
	}
	if o.Prune && !d.Prune.Enabled {
		return nil, ErrPruneNotEnabled
	}
	p := &Plan{Cluster: d.Cluster, Hash: d.Hash, Desired: len(d.Topics), Observed: len(s.Topics), Actions: []Action{}, Warnings: []Warning{}}

	var creates, alters, adds, deletes []Action
	for _, name := range d.Names() {
		want := d.Topics[name]
		have, exists := s.Topics[name]
		if !exists {
			creates = append(creates, Action{
				Kind: Create, Topic: name, Partitions: want.Partitions, ReplicationFactor: want.ReplicationFactor,
				Configs: copyMap(want.Configs),
			})
			continue
		}
		changed := false
		var changes []ConfigChange
		for _, key := range sortedKeys(want.Configs) {
			wantV := want.Configs[key]
			haveC, known := have.Configs[key]
			switch {
			case !known:
				p.Warnings = append(p.Warnings, Warning{Kind: UnreportedConfig, Topic: name,
					Message: fmt.Sprintf("the broker does not report config %s of this topic (it ignores or does not know the key): not set", key)})
			case haveC.Sensitive:
				p.Warnings = append(p.Warnings, Warning{Kind: SensitiveConfig, Topic: name,
					Message: fmt.Sprintf("config %s is sensitive on the broker: its value cannot be compared, so it is not set", key)})
			case SameValue(haveC.Value, wantV):
			default:
				changes = append(changes, ConfigChange{Key: key, To: wantV, From: haveC.Value, FromSource: sourceName(haveC.Source)})
			}
		}
		if len(changes) > 0 {
			alters = append(alters, Action{Kind: AlterConfigs, Topic: name, Changes: changes})
			changed = true
		}
		switch {
		case have.Partitions < want.Partitions:
			adds = append(adds, Action{Kind: AddPartitions, Topic: name, FromPartitions: have.Partitions, ToPartitions: want.Partitions})
			changed = true
		case have.Partitions > want.Partitions:
			p.Warnings = append(p.Warnings, Warning{Kind: PartitionsAboveDesired, Topic: name,
				Message: fmt.Sprintf("has %d partitions, %d desired: Kafka cannot remove partitions", have.Partitions, want.Partitions)})
		}
		if have.ReplicationFactor != want.ReplicationFactor {
			p.Warnings = append(p.Warnings, Warning{Kind: ReplicationFactorDiffers, Topic: name,
				Message: fmt.Sprintf("replication factor %d, %d desired: topicsync does not reassign replicas", have.ReplicationFactor, want.ReplicationFactor)})
		}
		if !changed {
			p.Unchanged++
		}
	}

	for name, t := range s.Topics {
		if _, listed := d.Topics[name]; !listed && !t.Internal && !d.IsProtected(name) {
			p.Unmanaged++
		}
	}

	if d.Prune.Enabled {
		matching := 0
		var prunable []string
		for name, t := range s.Topics {
			if t.Internal || !d.MatchesPrunePattern(name) || d.IsProtected(name) {
				continue
			}
			matching++
			if d.Prunable(name) {
				prunable = append(prunable, name)
			}
		}
		sort.Strings(prunable)
		if !o.Prune {
			for _, name := range prunable {
				p.Warnings = append(p.Warnings, Warning{Kind: PruneNotRequested, Topic: name,
					Message: "not listed and matches a prune pattern: deleted only by a run with --prune"})
			}
		} else if len(prunable) > 0 {
			if len(prunable) > o.PruneMaxDeletes || float64(len(prunable)) > o.PruneMaxFraction*float64(matching) {
				return nil, &PruneLimitError{Deletes: len(prunable), Max: o.PruneMaxDeletes, Matching: matching, MaxFraction: o.PruneMaxFraction, Topics: prunable}
			}
			for _, name := range prunable {
				deletes = append(deletes, Action{Kind: Delete, Topic: name})
			}
		}
	}

	p.Actions = append(p.Actions, creates...)
	p.Actions = append(p.Actions, alters...)
	p.Actions = append(p.Actions, adds...)
	p.Actions = append(p.Actions, deletes...)
	sort.SliceStable(p.Warnings, func(i, j int) bool {
		if p.Warnings[i].Topic != p.Warnings[j].Topic {
			return p.Warnings[i].Topic < p.Warnings[j].Topic
		}
		return p.Warnings[i].Kind < p.Warnings[j].Kind
	})
	return p, nil
}

// Empty reports whether the plan changes nothing.
func (p *Plan) Empty() bool { return len(p.Actions) == 0 }

// Count returns how many actions of a kind the plan has.
func (p *Plan) Count(k Kind) int {
	n := 0
	for _, a := range p.Actions {
		if a.Kind == k {
			n++
		}
	}
	return n
}

// WarningCount returns how many warnings of a kind the plan has.
func (p *Plan) WarningCount(k WarningKind) int {
	n := 0
	for _, w := range p.Warnings {
		if w.Kind == k {
			n++
		}
	}
	return n
}

// Text renders the plan for people: one line per action and warning, then a summary.
func (p *Plan) Text() string {
	var b strings.Builder
	for _, a := range p.Actions {
		switch a.Kind {
		case Create:
			fmt.Fprintf(&b, "+ create %s (partitions %d, replication factor %d", a.Topic, a.Partitions, a.ReplicationFactor)
			for _, k := range sortedKeys(a.Configs) {
				fmt.Fprintf(&b, ", %s=%s", k, a.Configs[k])
			}
			b.WriteString(")\n")
		case AlterConfigs:
			fmt.Fprintf(&b, "~ alter %s:", a.Topic)
			for _, c := range a.Changes {
				fmt.Fprintf(&b, " %s %q (%s) -> %q;", c.Key, c.From, c.FromSource, c.To)
			}
			b.WriteString("\n")
		case AddPartitions:
			fmt.Fprintf(&b, "~ partitions %s: %d -> %d\n", a.Topic, a.FromPartitions, a.ToPartitions)
		case Delete:
			fmt.Fprintf(&b, "- delete %s\n", a.Topic)
		}
	}
	for _, w := range p.Warnings {
		fmt.Fprintf(&b, "! %s %s: %s\n", w.Kind, w.Topic, w.Message)
	}
	fmt.Fprintf(&b, "cluster %s, spec %.12s: %d desired, %d observed; %d to create, %d to alter, %d to repartition, %d to delete, %d unchanged, %d warnings\n",
		p.Cluster, p.Hash, p.Desired, p.Observed, p.Count(Create), p.Count(AlterConfigs), p.Count(AddPartitions), p.Count(Delete), p.Unchanged, len(p.Warnings))
	return b.String()
}

// SameValue reports whether a value the broker reports means the desired one. Brokers normalize what they store:
// booleans in lower case (Kafka reports "False" as "false"), list values in their own order (Redpanda reports
// cleanup.policy "delete,compact" as "compact,delete"). Comparing the strings as given would plan the same change on
// every run and never converge.
func SameValue(have, want string) bool {
	if have == want {
		return true
	}
	if b1, b2 := strings.ToLower(have), strings.ToLower(want); (b1 == "true" || b1 == "false") && b1 == b2 {
		return true
	}
	if strings.Contains(have, ",") || strings.Contains(want, ",") {
		set := func(v string) string {
			parts := strings.Split(v, ",")
			for i := range parts {
				parts[i] = strings.TrimSpace(parts[i])
			}
			sort.Strings(parts)
			return strings.Join(parts, ",")
		}
		return set(have) == set(want)
	}
	return false
}

func sourceName(s cluster.Source) string {
	switch s {
	case cluster.SourceDynamicTopic:
		return "topic"
	case cluster.SourceDefault:
		return "default"
	default:
		return "unknown"
	}
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func copyMap(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
