// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// SchemaTableMap backs every table include/exclude list in the project, and the
// configuration documents all of them as supporting wildcards. These cases pin
// down what a wildcard actually buys today: the bare "*" entry, matched as a
// literal map key. A prefix pattern is stored and compared verbatim, so it
// never matches the tables it is meant to name.
func TestSchemaTableMap_ContainsSchemaTable_patterns(t *testing.T) {
	t.Parallel()

	const generatedTable = "website_es_data_product_prices_1_29209"

	tests := []struct {
		name    string
		entries []string
		schema  string
		table   string
		want    bool
	}{
		{
			name:    "exact table name matches",
			entries: []string{"public." + generatedTable},
			schema:  "public",
			table:   generatedTable,
			want:    true,
		},
		{
			name:    "bare wildcard covers every table in the schema",
			entries: []string{"public.*"},
			schema:  "public",
			table:   generatedTable,
			want:    true,
		},
		{
			name:    "prefix pattern matches a table carrying that prefix",
			entries: []string{"public.website_es_data_*"},
			schema:  "public",
			table:   generatedTable,
			want:    true,
		},
		{
			name:    "prefix pattern does not match outside its prefix",
			entries: []string{"public.website_es_data_*"},
			schema:  "public",
			table:   "product_template",
			want:    false,
		},
		{
			name:    "prefix pattern is scoped to its schema",
			entries: []string{"public.website_es_data_*"},
			schema:  "other",
			table:   generatedTable,
			want:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			m, err := NewSchemaTableMap(tc.entries)
			require.NoError(t, err)
			require.Equal(t, tc.want, m.ContainsSchemaTable(tc.schema, tc.table))
		})
	}
}

// ContainsExactSchemaTable drives include-list precedence, so it must stay an
// exact-name lookup even once patterns are understood elsewhere: a pattern is
// not an explicit listing.
func TestSchemaTableMap_ContainsExactSchemaTable_patternsAreNotExact(t *testing.T) {
	t.Parallel()

	m, err := NewSchemaTableMap([]string{"public.website_es_data_*"})
	require.NoError(t, err)

	require.False(t, m.ContainsExactSchemaTable("public", "website_es_data_product_prices_1_29209"))
	require.True(t, m.ContainsExactSchemaTable("public", "website_es_data_*"))
}
