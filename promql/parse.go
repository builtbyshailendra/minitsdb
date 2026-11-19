// Package promql evaluates a subset of Prometheus's query language:
//
//	selectors     metric{label="v", other!="v", re=~"a.*", nre!~"b.*"}
//	ranges        metric[5m]
//	functions     rate, increase, avg/sum/min/max/count_over_time
//	aggregations  sum, avg, min, max, count, with by (...) or without (...)
//	arithmetic    + - * / between numbers and vectors, and between vectors
//	              whose label sets match exactly
//
// Semantics follow Prometheus 3: an instant selector takes the newest sample
// in (t - 5m, t]; a range selector takes the samples in (t - range, t]; rate
// extrapolates to the edges of the range the way Prometheus does.
package promql

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/builtbyshailendra/minitsdb/labels"
)

// Expr is a parsed expression.
type Expr interface{ expr() }

// NumberLiteral is a constant.
type NumberLiteral struct{ Val float64 }

// VectorSelector selects series by matchers.
type VectorSelector struct {
	Matchers []*labels.Matcher
}

// MatrixSelector is a vector selector with a time range.
type MatrixSelector struct {
	VS    *VectorSelector
	Range time.Duration
}

// Call is a function call.
type Call struct {
	Func string
	Args []Expr
}

// AggregateExpr is sum/avg/min/max/count with optional grouping.
type AggregateExpr struct {
	Op       string
	Grouping []string
	Without  bool
	Expr     Expr
}

// BinaryExpr is arithmetic.
type BinaryExpr struct {
	Op       string
	LHS, RHS Expr
}

func (*NumberLiteral) expr()  {}
func (*VectorSelector) expr() {}
func (*MatrixSelector) expr() {}
func (*Call) expr()           {}
func (*AggregateExpr) expr()  {}
func (*BinaryExpr) expr()     {}

var functions = map[string]bool{
	"rate": true, "increase": true,
	"avg_over_time": true, "sum_over_time": true, "min_over_time": true, "max_over_time": true, "count_over_time": true,
}

var aggregations = map[string]bool{"sum": true, "avg": true, "min": true, "max": true, "count": true}

// Parse parses a query.
func Parse(q string) (Expr, error) {
	p := &parser{lx: lexer{src: q}}
	p.next()
	e, err := p.parseExpr(0)
	if err != nil {
		return nil, err
	}
	if p.tok.kind != tEOF {
		return nil, p.errf("unexpected %q", p.tok.text)
	}
	return e, nil
}

type tokKind int

const (
	tEOF tokKind = iota
	tIdent
	tNumber
	tString
	tDuration
	tOp // + - * /
	tLParen
	tRParen
	tLBrace
	tRBrace
	tLBracket
	tRBracket
	tComma
	tMatch // = != =~ !~
)

type token struct {
	kind tokKind
	text string
	pos  int
}

type lexer struct {
	src string
	pos int
}

func (l *lexer) next() (token, error) {
	for l.pos < len(l.src) && unicode.IsSpace(rune(l.src[l.pos])) {
		l.pos++
	}
	if l.pos >= len(l.src) {
		return token{kind: tEOF, pos: l.pos}, nil
	}
	start := l.pos
	c := l.src[l.pos]
	two := ""
	if l.pos+1 < len(l.src) {
		two = l.src[l.pos : l.pos+2]
	}
	switch {
	case two == "!=" || two == "=~" || two == "!~":
		l.pos += 2
		return token{tMatch, two, start}, nil
	case c == '=':
		l.pos++
		return token{tMatch, "=", start}, nil
	case strings.ContainsRune("+-*/", rune(c)):
		l.pos++
		return token{tOp, string(c), start}, nil
	case c == '(':
		l.pos++
		return token{tLParen, "(", start}, nil
	case c == ')':
		l.pos++
		return token{tRParen, ")", start}, nil
	case c == '{':
		l.pos++
		return token{tLBrace, "{", start}, nil
	case c == '}':
		l.pos++
		return token{tRBrace, "}", start}, nil
	case c == '[':
		l.pos++
		return token{tLBracket, "[", start}, nil
	case c == ']':
		l.pos++
		return token{tRBracket, "]", start}, nil
	case c == ',':
		l.pos++
		return token{tComma, ",", start}, nil
	case c == '"' || c == '\'':
		quote := c
		l.pos++
		var b strings.Builder
		for l.pos < len(l.src) && l.src[l.pos] != quote {
			if l.src[l.pos] == '\\' && l.pos+1 < len(l.src) {
				l.pos++
			}
			b.WriteByte(l.src[l.pos])
			l.pos++
		}
		if l.pos >= len(l.src) {
			return token{}, fmt.Errorf("unterminated string at %d", start)
		}
		l.pos++
		return token{tString, b.String(), start}, nil
	case c >= '0' && c <= '9' || c == '.':
		for l.pos < len(l.src) && (isDigit(l.src[l.pos]) || l.src[l.pos] == '.' || l.src[l.pos] == 'e' || l.src[l.pos] == 'E') {
			l.pos++
		}
		// A duration is a number followed by unit letters (5m, 1h30m, 250ms).
		if l.pos < len(l.src) && strings.ContainsRune("smhdwy", rune(l.src[l.pos])) {
			for l.pos < len(l.src) && (isDigit(l.src[l.pos]) || strings.ContainsRune("smhdwy", rune(l.src[l.pos]))) {
				l.pos++
			}
			return token{tDuration, l.src[start:l.pos], start}, nil
		}
		return token{tNumber, l.src[start:l.pos], start}, nil
	case isIdentStart(c):
		for l.pos < len(l.src) && (isIdentStart(l.src[l.pos]) || isDigit(l.src[l.pos]) || l.src[l.pos] == ':') {
			l.pos++
		}
		return token{tIdent, l.src[start:l.pos], start}, nil
	}
	return token{}, fmt.Errorf("unexpected character %q at %d", c, start)
}

func isDigit(c byte) bool      { return c >= '0' && c <= '9' }
func isIdentStart(c byte) bool { return c == '_' || c == ':' || unicode.IsLetter(rune(c)) }

type parser struct {
	lx  lexer
	tok token
	err error
}

func (p *parser) next() {
	if p.err != nil {
		return
	}
	t, err := p.lx.next()
	if err != nil {
		p.err = err
		p.tok = token{kind: tEOF}
		return
	}
	p.tok = t
}

func (p *parser) errf(format string, args ...any) error {
	if p.err != nil {
		return p.err
	}
	return fmt.Errorf("parse error at %d: %s", p.tok.pos, fmt.Sprintf(format, args...))
}

func precedence(op string) int {
	if op == "*" || op == "/" {
		return 2
	}
	return 1
}

// parseExpr is precedence climbing over + - * /.
func (p *parser) parseExpr(minPrec int) (Expr, error) {
	lhs, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for p.tok.kind == tOp && precedence(p.tok.text) > minPrec {
		op := p.tok.text
		p.next()
		rhs, err := p.parseExpr(precedence(op))
		if err != nil {
			return nil, err
		}
		lhs = &BinaryExpr{Op: op, LHS: lhs, RHS: rhs}
	}
	return lhs, p.err
}

func (p *parser) parseUnary() (Expr, error) {
	if p.tok.kind == tOp && (p.tok.text == "-" || p.tok.text == "+") {
		neg := p.tok.text == "-"
		p.next()
		e, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		if neg {
			return &BinaryExpr{Op: "*", LHS: &NumberLiteral{-1}, RHS: e}, nil
		}
		return e, nil
	}
	return p.parsePrimary()
}

func (p *parser) parsePrimary() (Expr, error) {
	switch p.tok.kind {
	case tNumber:
		v, err := strconv.ParseFloat(p.tok.text, 64)
		if err != nil {
			return nil, p.errf("bad number %q", p.tok.text)
		}
		p.next()
		return &NumberLiteral{v}, nil
	case tLParen:
		p.next()
		e, err := p.parseExpr(0)
		if err != nil {
			return nil, err
		}
		if p.tok.kind != tRParen {
			return nil, p.errf("expected )")
		}
		p.next()
		return e, nil
	case tLBrace:
		return p.parseSelector("")
	case tIdent:
		name := p.tok.text
		if aggregations[name] {
			return p.parseAggregation()
		}
		p.next()
		if p.tok.kind == tLParen {
			if !functions[name] {
				return nil, p.errf("unknown function %q", name)
			}
			return p.parseCall(name)
		}
		return p.parseSelector(name)
	}
	return nil, p.errf("unexpected %q", p.tok.text)
}

func (p *parser) parseCall(name string) (Expr, error) {
	p.next() // (
	arg, err := p.parseExpr(0)
	if err != nil {
		return nil, err
	}
	if p.tok.kind != tRParen {
		return nil, p.errf("expected ) after argument")
	}
	p.next()
	if _, ok := arg.(*MatrixSelector); !ok {
		return nil, p.errf("%s needs a range vector, like x[5m]", name)
	}
	return &Call{Func: name, Args: []Expr{arg}}, nil
}

// parseAggregation accepts both "sum by (a) (x)" and "sum(x) by (a)".
func (p *parser) parseAggregation() (Expr, error) {
	agg := &AggregateExpr{Op: p.tok.text}
	p.next()
	if err := p.parseGrouping(agg); err != nil {
		return nil, err
	}
	if p.tok.kind != tLParen {
		return nil, p.errf("expected ( after %s", agg.Op)
	}
	p.next()
	e, err := p.parseExpr(0)
	if err != nil {
		return nil, err
	}
	if p.tok.kind != tRParen {
		return nil, p.errf("expected )")
	}
	p.next()
	agg.Expr = e
	if err := p.parseGrouping(agg); err != nil {
		return nil, err
	}
	return agg, nil
}

func (p *parser) parseGrouping(agg *AggregateExpr) error {
	if p.tok.kind != tIdent || (p.tok.text != "by" && p.tok.text != "without") {
		return nil
	}
	agg.Without = p.tok.text == "without"
	p.next()
	if p.tok.kind != tLParen {
		return p.errf("expected ( after by/without")
	}
	p.next()
	for p.tok.kind != tRParen {
		if p.tok.kind != tIdent {
			return p.errf("expected a label name")
		}
		agg.Grouping = append(agg.Grouping, p.tok.text)
		p.next()
		if p.tok.kind == tComma {
			p.next()
		}
	}
	p.next()
	return nil
}

func (p *parser) parseSelector(name string) (Expr, error) {
	vs := &VectorSelector{}
	if name != "" {
		vs.Matchers = append(vs.Matchers, labels.MustMatcher(labels.Equal, labels.MetricName, name))
	}
	if p.tok.kind == tLBrace {
		p.next()
		for p.tok.kind != tRBrace {
			if p.tok.kind != tIdent {
				return nil, p.errf("expected a label name")
			}
			lname := p.tok.text
			p.next()
			if p.tok.kind != tMatch {
				return nil, p.errf("expected = != =~ or !~")
			}
			op := map[string]labels.MatchType{"=": labels.Equal, "!=": labels.NotEqual, "=~": labels.Regexp, "!~": labels.NotRegexp}[p.tok.text]
			p.next()
			if p.tok.kind != tString {
				return nil, p.errf("expected a quoted value")
			}
			m, err := labels.NewMatcher(op, lname, p.tok.text)
			if err != nil {
				return nil, p.errf("%v", err)
			}
			vs.Matchers = append(vs.Matchers, m)
			p.next()
			if p.tok.kind == tComma {
				p.next()
			}
		}
		p.next()
	}
	if len(vs.Matchers) == 0 {
		return nil, p.errf("a selector needs a metric name or a matcher")
	}
	if p.tok.kind == tLBracket {
		p.next()
		if p.tok.kind != tDuration {
			return nil, p.errf("expected a duration like 5m")
		}
		d, err := ParseDuration(p.tok.text)
		if err != nil {
			return nil, p.errf("%v", err)
		}
		p.next()
		if p.tok.kind != tRBracket {
			return nil, p.errf("expected ]")
		}
		p.next()
		return &MatrixSelector{VS: vs, Range: d}, nil
	}
	return vs, p.err
}

// ParseDuration reads Prometheus durations: 30s, 5m, 1h30m, 2d, 250ms.
func ParseDuration(s string) (time.Duration, error) {
	units := map[string]time.Duration{"ms": time.Millisecond, "s": time.Second, "m": time.Minute, "h": time.Hour,
		"d": 24 * time.Hour, "w": 7 * 24 * time.Hour, "y": 365 * 24 * time.Hour}
	var total time.Duration
	for s != "" {
		i := 0
		for i < len(s) && isDigit(s[i]) {
			i++
		}
		if i == 0 {
			return 0, fmt.Errorf("bad duration %q", s)
		}
		n, _ := strconv.Atoi(s[:i])
		s = s[i:]
		j := 0
		for j < len(s) && !isDigit(s[j]) {
			j++
		}
		u, ok := units[s[:j]]
		if !ok {
			return 0, fmt.Errorf("bad duration unit %q", s[:j])
		}
		total += time.Duration(n) * u
		s = s[j:]
	}
	return total, nil
}
