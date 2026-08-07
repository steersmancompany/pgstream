// SPDX-License-Identifier: Apache-2.0

package pgdumprestore

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
)

// Snapshot DDL: exclusions become pg_dump --exclude-table arguments. pg_dump
// matches those against its own pattern syntax, where "*" is a wildcard - but
// only for unquoted names. Quoting makes pg_dump treat the argument literally,
// so a quoted pattern excludes nothing.
//
// The wildcard-table case is handled separately (it becomes an excluded schema)
// and is covered by the existing pgdumpOptions tests, so this pins the pattern
// case only.
func TestOptionsGenerator_snapshotDDL_patternExclusion(t *testing.T) {
	t.Parallel()

	og := &optionGenerator{
		sourceURL: "source-url",
		// Every table is in scope via the wildcard, so no per-table excludes
		// are computed from the catalogue and the connection is never used.
		querier: nil,
	}

	opts, err := og.pgdumpOptions(
		context.Background(),
		map[string][]string{"public": {"*"}},
		map[string][]string{},
		map[string][]string{"public": {"website_es_data_*"}},
	)
	require.NoError(t, err)

	require.Contains(t, opts.ExcludeTables, pglib.QuoteIdentifier("public")+".website_es_data_*",
		"pattern must reach pg_dump unquoted for its wildcard matching to apply")
}

// Control: an exactly-named exclusion still has to be quoted, since a literal
// name may contain characters pg_dump would otherwise interpret.
func TestOptionsGenerator_snapshotDDL_exactExclusionStaysQuoted(t *testing.T) {
	t.Parallel()

	og := &optionGenerator{sourceURL: "source-url", querier: nil}

	opts, err := og.pgdumpOptions(
		context.Background(),
		map[string][]string{"public": {"*"}},
		map[string][]string{},
		map[string][]string{"public": {"audit_log"}},
	)
	require.NoError(t, err)

	require.Contains(t, opts.ExcludeTables, pglib.QuoteQualifiedIdentifier("public", "audit_log"))
}
