// Package labels identifies series: a sorted set of name="value" pairs, with
// the metric name stored as __name__.
package labels

import (
	"fmt"
	"hash/fnv"
	"regexp"
	"sort"
	"strings"
)

// MetricName is the label holding the metric name.
const MetricName = "__name__"

// Label is one name/value pair.
type Label struct{ Name, Value string }

// Labels is sorted by name, with unique names.
type Labels []Label

// FromMap builds sorted labels.
func FromMap(m map[string]string) Labels {
	ls := make(Labels, 0, len(m))
	for k, v := range m {
		ls = append(ls, Label{k, v})
	}
	sort.Slice(ls, func(i, j int) bool { return ls[i].Name < ls[j].Name })
	return ls
}

// FromStrings builds labels from name, value, name, value...
func FromStrings(kv ...string) Labels {
	m := map[string]string{}
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}
	return FromMap(m)
}

// Get returns a label's value ("" if absent).
func (ls Labels) Get(name string) string {
	for _, l := range ls {
		if l.Name == name {
			return l.Value
		}
	}
	return ""
}

// Hash identifies the label set (collisions are checked by callers).
func (ls Labels) Hash() uint64 {
	h := fnv.New64a()
	for _, l := range ls {
		h.Write([]byte(l.Name))
		h.Write([]byte{0xff})
		h.Write([]byte(l.Value))
		h.Write([]byte{0xff})
	}
	return h.Sum64()
}

// Equal compares two label sets.
func (ls Labels) Equal(o Labels) bool {
	if len(ls) != len(o) {
		return false
	}
	for i := range ls {
		if ls[i] != o[i] {
			return false
		}
	}
	return true
}

// Compare orders label sets (for sorted query results).
func Compare(a, b Labels) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if c := strings.Compare(a[i].Name, b[i].Name); c != 0 {
			return c
		}
		if c := strings.Compare(a[i].Value, b[i].Value); c != 0 {
			return c
		}
	}
	return len(a) - len(b)
}

func (ls Labels) String() string {
	var b strings.Builder
	name := ls.Get(MetricName)
	b.WriteString(name)
	b.WriteByte('{')
	first := true
	for _, l := range ls {
		if l.Name == MetricName {
			continue
		}
		if !first {
			b.WriteString(", ")
		}
		first = false
		fmt.Fprintf(&b, "%s=%q", l.Name, l.Value)
	}
	b.WriteByte('}')
	return b.String()
}

// Without returns the labels minus the named ones (and minus the metric name).
func (ls Labels) Without(names ...string) Labels {
	out := Labels{}
	for _, l := range ls {
		if l.Name == MetricName || contains(names, l.Name) {
			continue
		}
		out = append(out, l)
	}
	return out
}

// Only keeps the named labels.
func (ls Labels) Only(names ...string) Labels {
	out := Labels{}
	for _, l := range ls {
		if contains(names, l.Name) {
			out = append(out, l)
		}
	}
	return out
}

func contains(s []string, x string) bool {
	for _, v := range s {
		if v == x {
			return true
		}
	}
	return false
}

// MatchType is =, !=, =~ or !~.
type MatchType int

const (
	Equal MatchType = iota
	NotEqual
	Regexp
	NotRegexp
)

func (t MatchType) String() string { return [...]string{"=", "!=", "=~", "!~"}[t] }

// Matcher selects series by one label.
type Matcher struct {
	Type  MatchType
	Name  string
	Value string
	re    *regexp.Regexp
}

// NewMatcher builds a matcher. Regular expressions are anchored at both
// ends, as in Prometheus.
func NewMatcher(t MatchType, name, value string) (*Matcher, error) {
	m := &Matcher{Type: t, Name: name, Value: value}
	if t == Regexp || t == NotRegexp {
		re, err := regexp.Compile("^(?:" + value + ")$")
		if err != nil {
			return nil, err
		}
		m.re = re
	}
	return m, nil
}

// MustMatcher is NewMatcher that panics (for tests).
func MustMatcher(t MatchType, name, value string) *Matcher {
	m, err := NewMatcher(t, name, value)
	if err != nil {
		panic(err)
	}
	return m
}

// Matches tests one label value ("" for a missing label).
func (m *Matcher) Matches(v string) bool {
	switch m.Type {
	case Equal:
		return v == m.Value
	case NotEqual:
		return v != m.Value
	case Regexp:
		return m.re.MatchString(v)
	default:
		return !m.re.MatchString(v)
	}
}

func (m *Matcher) String() string { return fmt.Sprintf("%s%s%q", m.Name, m.Type, m.Value) }

// MatchesAll tests a whole label set.
func MatchesAll(ls Labels, ms []*Matcher) bool {
	for _, m := range ms {
		if !m.Matches(ls.Get(m.Name)) {
			return false
		}
	}
	return true
}
