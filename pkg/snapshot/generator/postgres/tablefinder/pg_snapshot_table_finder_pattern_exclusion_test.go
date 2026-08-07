// SPDX-License-Identifier: Apache-2.0

package tablefinder

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/snapshot/generator/mocks"
)

const generatedMatview = "website_es_data_product_prices_1_29209"

// newFinder wires a table finder whose discovery returns a fixed table list, so
// the exclusion logic can be exercised without a database.
func newFinder(t *testing.T, discovered []string, captured *snapshot.Snapshot) *SnapshotSchemaTableFinder {
	t.Helper()

	return &SnapshotSchemaTableFinder{
		wrapped: &mocks.Generator{
			CreateSnapshotFn: func(_ context.Context, ss *snapshot.Snapshot) error {
				*captured = *ss
				return nil
			},
		},
		schemaDiscoveryFn: func(context.Context, pglib.Querier) ([]string, error) {
			return []string{"public"}, nil
		},
		tableDiscoveryFn: func(context.Context, pglib.Querier, string) ([]string, error) {
			return discovered, nil
		},
	}
}

// Snapshot DML: excluded tables are removed from the data snapshot scope by
// exact string comparison (slices.Contains), so a pattern never matches.
func TestSnapshotSchemaTableFinder_snapshotDML_patternExclusion(t *testing.T) {
	t.Parallel()

	var captured snapshot.Snapshot
	finder := newFinder(t, []string{"product_template", generatedMatview}, &captured)

	err := finder.CreateSnapshot(context.Background(), &snapshot.Snapshot{
		SchemaTables:         map[string][]string{"public": {"*"}},
		SchemaExcludedTables: map[string][]string{"public": {"website_es_data_*"}},
	})
	require.NoError(t, err)

	require.Equal(t, []string{"product_template"}, captured.SchemaTables["public"],
		"excluded table stayed in the data snapshot scope")
}

// Control: an exactly-named excluded table is dropped from the scope today.
func TestSnapshotSchemaTableFinder_snapshotDML_exactExclusion(t *testing.T) {
	t.Parallel()

	var captured snapshot.Snapshot
	finder := newFinder(t, []string{"product_template", generatedMatview}, &captured)

	err := finder.CreateSnapshot(context.Background(), &snapshot.Snapshot{
		SchemaTables:         map[string][]string{"public": {"*"}},
		SchemaExcludedTables: map[string][]string{"public": {generatedMatview}},
	})
	require.NoError(t, err)

	require.Equal(t, []string{"product_template"}, captured.SchemaTables["public"])
}

// The bare wildcard is the one form of exclusion the rest of the project
// understands, and the data snapshot scope should honour it too: excluding
// "public.*" should leave no tables to copy.
func TestSnapshotSchemaTableFinder_snapshotDML_wildcardExclusion(t *testing.T) {
	t.Parallel()

	var captured snapshot.Snapshot
	finder := newFinder(t, []string{"product_template", generatedMatview}, &captured)

	err := finder.CreateSnapshot(context.Background(), &snapshot.Snapshot{
		SchemaTables:         map[string][]string{"public": {"*"}},
		SchemaExcludedTables: map[string][]string{"public": {"*"}},
	})
	require.NoError(t, err)

	require.Empty(t, captured.SchemaTables["public"],
		"wildcard exclusion left tables in the data snapshot scope")
}
