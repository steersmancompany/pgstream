// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"fmt"
	"maps"
	"path"
	"slices"
	"strings"
)

type SchemaTableMap map[string]map[string]struct{}

const (
	PublicSchema = "public"

	wildcard = "*"

	// patternMeta are the characters that turn a table entry from a literal
	// name into a pattern. They match psql's own object-name pattern syntax,
	// which is what pg_dump -T consumes, so an entry means the same thing
	// whether it is matched here or handed to pg_dump.
	patternMeta = "*?["
)

var ErrInvalidTableName = errors.New("invalid table name format")

// IsPattern reports whether a table entry is a pattern rather than a literal
// name. Every caller that has to tell the two apart - matching here, quoting
// for pg_dump - asks this, so they cannot drift apart.
func IsPattern(table string) bool {
	return strings.ContainsAny(table, patternMeta)
}

func NewSchemaTableMap(tables []string) (SchemaTableMap, error) {
	schemaTablesMap := make(SchemaTableMap, len(tables))
	for _, table := range tables {
		schemaName, tableName, err := parseTableName(table)
		if err != nil {
			return nil, err
		}
		if _, found := schemaTablesMap[schemaName]; !found {
			schemaTablesMap[schemaName] = make(map[string]struct{})
		}
		schemaTablesMap[schemaName][tableName] = struct{}{}
	}
	return schemaTablesMap, nil
}

// NewSchemaTableMapFromSchemaTables builds the map from an already schema-keyed
// table list, the shape snapshot requests carry. Same type and same matching
// rules as NewSchemaTableMap, so a list means the same thing wherever it came
// from.
func NewSchemaTableMapFromSchemaTables(schemaTables map[string][]string) SchemaTableMap {
	m := make(SchemaTableMap, len(schemaTables))
	for schema, tables := range schemaTables {
		m[schema] = make(map[string]struct{}, len(tables))
		for _, table := range tables {
			m[schema][table] = struct{}{}
		}
	}
	return m
}

func (t SchemaTableMap) ContainsSchemaTable(schema, table string) bool {
	if len(t) == 0 {
		return false
	}

	containsTable := func(tables map[string]struct{}) bool {
		// Literal names resolve on the map itself, so a list of thousands of
		// exact tables still costs one lookup. Only entries carrying pattern
		// metacharacters have to be walked, and there are typically a handful.
		if _, found := tables[table]; found {
			return true
		}
		for entry := range tables {
			if !IsPattern(entry) {
				continue
			}
			// path.Match only errors on a malformed pattern, which can never
			// match anything, so a bad entry is skipped rather than promoted
			// into a match.
			if matched, err := path.Match(entry, table); err == nil && matched {
				return true
			}
		}
		return false
	}
	return containsTable(t[schema]) || containsTable(t[wildcard])
}

// Patterns returns the pattern entries for a schema, and Exact returns the
// literal ones. Callers that delegate matching to something else - pg_dump has
// its own pattern engine - need to tell them apart to hand each over correctly.
func (t SchemaTableMap) Patterns(schema string) []string {
	return t.partition(schema, true)
}

func (t SchemaTableMap) Exact(schema string) []string {
	return t.partition(schema, false)
}

func (t SchemaTableMap) partition(schema string, patterns bool) []string {
	out := []string{}
	for entry := range t[schema] {
		if IsPattern(entry) == patterns {
			out = append(out, entry)
		}
	}
	slices.Sort(out)
	return out
}

// ContainsExactSchemaTable returns true only if the table is listed by its
// exact name under the exact schema. Wildcard entries do not match.
func (t SchemaTableMap) ContainsExactSchemaTable(schema, table string) bool {
	_, found := t[schema][table]
	return found
}

func (t SchemaTableMap) GetSchemaTables(schema string) map[string]struct{} {
	tables, found := t[schema]
	if !found {
		return t[wildcard]
	}

	if len(t[wildcard]) == 0 {
		return tables
	}

	// merge with the wildcard schema tables into a copy, so the map itself is
	// never mutated by lookups
	merged := make(map[string]struct{}, len(tables)+len(t[wildcard]))
	maps.Copy(merged, tables)
	maps.Copy(merged, t[wildcard])
	return merged
}

// ValidateWildcardSchema returns an error when the wildcard schema entry lists
// anything other than the wildcard table ("*.*"): the snapshot generators
// can't resolve a specific table name across all schemas.
func (t SchemaTableMap) ValidateWildcardSchema() error {
	tables, found := t[wildcard]
	if !found {
		return nil
	}
	tableNames := make([]string, 0, len(tables))
	for table := range tables {
		tableNames = append(tableNames, table)
	}
	slices.Sort(tableNames)
	return ValidateWildcardSchemaTables(map[string][]string{wildcard: tableNames})
}

// ValidateWildcardSchemaTables is the schema->table-list counterpart of
// ValidateWildcardSchema, for callers operating on map[string][]string.
func ValidateWildcardSchemaTables(schemaTables map[string][]string) error {
	tables, found := schemaTables[wildcard]
	if !found || (len(tables) == 1 && tables[0] == wildcard) {
		return nil
	}
	return fmt.Errorf("wildcard schema must be used with wildcard table, got %q", tables)
}

func (t SchemaTableMap) Add(table string) error {
	schema, table, err := parseTableName(table)
	if err != nil {
		return err
	}
	_, found := t[schema]
	if !found {
		t[schema] = map[string]struct{}{}
	}
	t[schema][table] = struct{}{}
	return nil
}

// ParseTableName splits a possibly schema-qualified table name, defaulting to
// the public schema. Exported so that every caller splits names the same way,
// rather than each growing its own strings.Split.
func ParseTableName(qualifiedTableName string) (string, string, error) {
	return parseTableName(qualifiedTableName)
}

func parseTableName(qualifiedTableName string) (string, string, error) {
	parts := strings.Split(qualifiedTableName, ".")
	switch len(parts) {
	case 1:
		return PublicSchema, parts[0], nil
	case 2:
		return parts[0], parts[1], nil
	default:
		return "", "", ErrInvalidTableName
	}
}
