// SPDX-License-Identifier: Apache-2.0

package filter

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor/mocks"
)

func TestFilter_indexAndRenameDDL(t *testing.T) {
	t.Parallel()

	ddlEvent := func(t *testing.T, tag, ddl string, obj wal.DDLObject) *wal.Event {
		t.Helper()
		content, err := json.Marshal(&wal.DDLEvent{DDL: ddl, SchemaName: "public", CommandTag: tag, Objects: []wal.DDLObject{obj}})
		require.NoError(t, err)
		return &wal.Event{Data: &wal.Data{Action: wal.LogicalMessageAction, Prefix: wal.DDLPrefix, Content: string(content)}}
	}
	dmlEvent := func(table string) *wal.Event {
		return &wal.Event{Data: &wal.Data{Action: "I", Schema: "public", Table: table}}
	}
	index := func(name string) wal.DDLObject {
		return wal.DDLObject{Type: wal.ObjectTypeIndex, Identity: "public." + name, Schema: "public"}
	}
	table := func(name string) wal.DDLObject {
		return wal.DDLObject{Type: wal.ObjectTypeTable, Identity: "public." + name, Schema: "public"}
	}
	newFilter := func(processor *mocks.Processor) *Filter {
		return &Filter{
			logger:             log.NewNoopLogger(),
			processor:          processor,
			excludeTableMap:    pglib.SchemaTableMap{"public": {"excluded": {}, "wizard_*": {}}},
			walEventToDDLEvent: wal.WalDataToDDLEvent,
			renamedExcluded:    pglib.SchemaTableMap{},
		}
	}

	t.Run("index on an excluded table is skipped", func(t *testing.T) {
		t.Parallel()
		p := &mocks.Processor{ProcessWALEventFn: func(context.Context, *wal.Event) error { return nil }}
		f := newFilter(p)
		require.NoError(t, f.ProcessWALEvent(context.Background(),
			ddlEvent(t, "CREATE INDEX", `CREATE INDEX "excluded_a_index" ON "excluded" ("a")`, index("excluded_a_index"))))
		require.NoError(t, f.ProcessWALEvent(context.Background(),
			ddlEvent(t, "CREATE INDEX", `CREATE UNIQUE INDEX "w_uniq" ON "wizard_x" ("a")`, index("w_uniq"))))
		require.Equal(t, uint(0), p.GetProcessCalls())
	})

	t.Run("index on a replicated table passes, whatever the index is called", func(t *testing.T) {
		t.Parallel()
		p := &mocks.Processor{ProcessWALEventFn: func(context.Context, *wal.Event) error { return nil }}
		f := newFilter(p)
		require.NoError(t, f.ProcessWALEvent(context.Background(),
			ddlEvent(t, "CREATE INDEX", `CREATE INDEX "excluded_lookalike" ON "orders" ("a")`, index("excluded_lookalike"))))
		require.Equal(t, uint(1), p.GetProcessCalls())
	})

	t.Run("renaming an excluded table is skipped and its new name stays excluded", func(t *testing.T) {
		t.Parallel()
		p := &mocks.Processor{ProcessWALEventFn: func(context.Context, *wal.Event) error { return nil }}
		f := newFilter(p)
		require.NoError(t, f.ProcessWALEvent(context.Background(),
			ddlEvent(t, "ALTER TABLE", `ALTER TABLE "wizard_old" RENAME TO "catalog_new"`, table("catalog_new"))))
		require.Equal(t, uint(0), p.GetProcessCalls())

		require.NoError(t, f.ProcessWALEvent(context.Background(), dmlEvent("catalog_new")))
		require.NoError(t, f.ProcessWALEvent(context.Background(),
			ddlEvent(t, "ALTER TABLE", `ALTER TABLE "catalog_new" ADD COLUMN "b" text`, table("catalog_new"))))
		require.Equal(t, uint(0), p.GetProcessCalls())

		require.NoError(t, f.ProcessWALEvent(context.Background(), dmlEvent("orders")))
		require.Equal(t, uint(1), p.GetProcessCalls())
	})

	t.Run("renaming a replicated table passes", func(t *testing.T) {
		t.Parallel()
		p := &mocks.Processor{ProcessWALEventFn: func(context.Context, *wal.Event) error { return nil }}
		f := newFilter(p)
		require.NoError(t, f.ProcessWALEvent(context.Background(),
			ddlEvent(t, "ALTER TABLE", `ALTER TABLE "product_image" RENAME TO "product_catalog_image"`, table("product_catalog_image"))))
		require.NoError(t, f.ProcessWALEvent(context.Background(), dmlEvent("product_catalog_image")))
		require.Equal(t, uint(2), p.GetProcessCalls())
	})
}

func TestFilter_renameIntoIncludeList(t *testing.T) {
	t.Parallel()

	content, err := json.Marshal(&wal.DDLEvent{
		DDL: `ALTER TABLE "staging" RENAME TO "orders"`, SchemaName: "public", CommandTag: "ALTER TABLE",
		Objects: []wal.DDLObject{{Type: wal.ObjectTypeTable, Identity: "public.orders", Schema: "public"}},
	})
	require.NoError(t, err)
	p := &mocks.Processor{ProcessWALEventFn: func(context.Context, *wal.Event) error { return nil }}
	f := &Filter{
		logger:             log.NewNoopLogger(),
		processor:          p,
		includeTableMap:    pglib.SchemaTableMap{"public": {"orders": {}}},
		walEventToDDLEvent: wal.WalDataToDDLEvent,
		renamedExcluded:    pglib.SchemaTableMap{},
	}
	require.NoError(t, f.ProcessWALEvent(context.Background(),
		&wal.Event{Data: &wal.Data{Action: wal.LogicalMessageAction, Prefix: wal.DDLPrefix, Content: string(content)}}))
	require.NoError(t, f.ProcessWALEvent(context.Background(), &wal.Event{Data: &wal.Data{Action: "I", Schema: "public", Table: "orders"}}))
	require.Equal(t, uint(2), p.GetProcessCalls())
}

func TestFilter_renamedExcludedNameIsUnquoted(t *testing.T) {
	t.Parallel()

	content, err := json.Marshal(&wal.DDLEvent{
		DDL: `ALTER TABLE "excluded" RENAME TO "NewName"`, SchemaName: "public", CommandTag: "ALTER TABLE",
		Objects: []wal.DDLObject{{Type: wal.ObjectTypeTable, Identity: `public."NewName"`, Schema: "public"}},
	})
	require.NoError(t, err)
	p := &mocks.Processor{ProcessWALEventFn: func(context.Context, *wal.Event) error { return nil }}
	f := &Filter{
		logger:             log.NewNoopLogger(),
		processor:          p,
		excludeTableMap:    pglib.SchemaTableMap{"public": {"excluded": {}}},
		walEventToDDLEvent: wal.WalDataToDDLEvent,
		renamedExcluded:    pglib.SchemaTableMap{},
	}
	require.NoError(t, f.ProcessWALEvent(context.Background(),
		&wal.Event{Data: &wal.Data{Action: wal.LogicalMessageAction, Prefix: wal.DDLPrefix, Content: string(content)}}))
	require.NoError(t, f.ProcessWALEvent(context.Background(), &wal.Event{Data: &wal.Data{Action: "I", Schema: "public", Table: "NewName"}}))
	require.Equal(t, uint(0), p.GetProcessCalls())
}
