// SPDX-License-Identifier: Apache-2.0

package filter

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	log "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor/mocks"
)

// Odoo generates these at runtime with a numeric suffix, so they can only ever
// be excluded by pattern - the exact set is unbounded and changes as the
// application runs.
const (
	generatedMatview = "website_es_data_product_prices_1_29209"
	excludePattern   = "public.website_es_data_*"
)

func newExcludingFilter(t *testing.T, processor *mocks.Processor, exclude []string) *Filter {
	t.Helper()

	excluded, err := pglib.NewSchemaTableMap(exclude)
	require.NoError(t, err)

	return &Filter{
		processor:          processor,
		excludeTableMap:    excluded,
		logger:             log.NewNoopLogger(),
		walEventToDDLEvent: wal.WalDataToDDLEvent,
	}
}

func newDDLWALEvent(t *testing.T, event *wal.DDLEvent) *wal.Event {
	t.Helper()

	content, err := json.Marshal(event)
	require.NoError(t, err)

	return &wal.Event{
		Data: &wal.Data{
			Action:  wal.LogicalMessageAction,
			Prefix:  wal.DDLPrefix,
			Content: string(content),
		},
	}
}

// Replication DML: a data event for a table matched by an exclusion pattern
// must not reach the target.
func TestFilter_replicationDML_patternExclusion(t *testing.T) {
	t.Parallel()

	processor := &mocks.Processor{
		ProcessWALEventFn: func(context.Context, *wal.Event) error { return nil },
	}
	f := newExcludingFilter(t, processor, []string{excludePattern})

	err := f.ProcessWALEvent(context.Background(), &wal.Event{
		Data: &wal.Data{Schema: "public", Table: generatedMatview},
	})
	require.NoError(t, err)
	require.Equal(t, uint(0), processor.GetProcessCalls(), "excluded table's DML reached the target")
}

// Replication DDL: this is the statement that halted the Great American
// replica. CREATE MATERIALIZED VIEW emits objects of type "materialized view",
// and skipDDLEvent only inspects "table" and "table column" objects, so the
// event is never considered for exclusion.
func TestFilter_replicationDDL_materializedViewExclusion(t *testing.T) {
	t.Parallel()

	processor := &mocks.Processor{
		ProcessWALEventFn: func(context.Context, *wal.Event) error { return nil },
	}
	f := newExcludingFilter(t, processor, []string{excludePattern})

	event := newDDLWALEvent(t, &wal.DDLEvent{
		DDL:        `CREATE MATERIALIZED VIEW "` + generatedMatview + `" AS SELECT eval_js_formula('x', '{}'::jsonb)`,
		SchemaName: "public",
		CommandTag: "CREATE MATERIALIZED VIEW",
		Objects: []wal.DDLObject{
			{
				Type:     "materialized view",
				Identity: "public." + generatedMatview,
				Schema:   "public",
			},
		},
	})

	err := f.ProcessWALEvent(context.Background(), event)
	require.NoError(t, err)
	require.Equal(t, uint(0), processor.GetProcessCalls(), "excluded materialized view's DDL reached the target")
}

// Isolates the object-type gap from the pattern gap: even named exactly, a
// materialized view's DDL is not filtered, because skipDDLEvent never looks at
// "materialized view" objects.
func TestFilter_replicationDDL_materializedViewExactExclusion(t *testing.T) {
	t.Parallel()

	processor := &mocks.Processor{
		ProcessWALEventFn: func(context.Context, *wal.Event) error { return nil },
	}
	f := newExcludingFilter(t, processor, []string{"public." + generatedMatview})

	event := newDDLWALEvent(t, &wal.DDLEvent{
		DDL:        `DROP MATERIALIZED VIEW "` + generatedMatview + `";`,
		SchemaName: "public",
		CommandTag: "DROP MATERIALIZED VIEW",
		Objects: []wal.DDLObject{
			{Type: "materialized view", Identity: "public." + generatedMatview, Schema: "public"},
		},
	})

	err := f.ProcessWALEvent(context.Background(), event)
	require.NoError(t, err)
	require.Equal(t, uint(0), processor.GetProcessCalls(),
		"exactly-named materialized view's DDL reached the target")
}

// Control: an exactly-named table object is filtered today. This is the one
// combination that already works, and it must keep working.
func TestFilter_replicationDDL_exactTableExclusion(t *testing.T) {
	t.Parallel()

	processor := &mocks.Processor{
		ProcessWALEventFn: func(context.Context, *wal.Event) error { return nil },
	}
	f := newExcludingFilter(t, processor, []string{"public.audit_log"})

	event := newDDLWALEvent(t, &wal.DDLEvent{
		DDL:        "DROP TABLE public.audit_log;",
		SchemaName: "public",
		CommandTag: "DROP TABLE",
		Objects: []wal.DDLObject{
			{Type: "table", Identity: "public.audit_log", Schema: "public"},
		},
	})

	err := f.ProcessWALEvent(context.Background(), event)
	require.NoError(t, err)
	require.Equal(t, uint(0), processor.GetProcessCalls())
}

// Replication DDL by pattern, on a plain table object. Isolates pattern
// matching from the materialized-view object-type gap above.
func TestFilter_replicationDDL_patternExclusion(t *testing.T) {
	t.Parallel()

	processor := &mocks.Processor{
		ProcessWALEventFn: func(context.Context, *wal.Event) error { return nil },
	}
	f := newExcludingFilter(t, processor, []string{excludePattern})

	event := newDDLWALEvent(t, &wal.DDLEvent{
		DDL:        "DROP TABLE public." + generatedMatview + ";",
		SchemaName: "public",
		CommandTag: "DROP TABLE",
		Objects: []wal.DDLObject{
			{Type: "table", Identity: "public." + generatedMatview, Schema: "public"},
		},
	})

	err := f.ProcessWALEvent(context.Background(), event)
	require.NoError(t, err)
	require.Equal(t, uint(0), processor.GetProcessCalls(), "excluded table's DDL reached the target")
}
