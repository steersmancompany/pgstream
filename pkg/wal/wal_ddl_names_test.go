// SPDX-License-Identifier: Apache-2.0

package wal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDDLEvent_IndexedTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ddl        string
		wantSchema string
		wantTable  string
		wantOK     bool
	}{
		{ddl: `CREATE INDEX "x_backend_id_index" ON "integration_sync_shipment_confirmation" ("backend_id")`, wantTable: "integration_sync_shipment_confirmation", wantOK: true},
		{ddl: `CREATE UNIQUE INDEX "x_uniq" ON "Mixed""Case" (a)`, wantTable: `Mixed"Case`, wantOK: true},
		{ddl: "create index if not exists idx on only Public.Orders using btree (id)", wantSchema: "public", wantTable: "orders", wantOK: true},
		{ddl: "CREATE INDEX CONCURRENTLY idx_on\n  ON sales.order_line (order_id)", wantSchema: "sales", wantTable: "order_line", wantOK: true},
		{ddl: `CREATE INDEX ON "t" (a)`, wantTable: "t", wantOK: true},
		{ddl: `ALTER INDEX "x_pkey" RENAME TO "y_pkey"`},
		{ddl: `DROP INDEX IF EXISTS "x_index"`},
		{ddl: `CREATE TABLE t (a int)`},
	}

	for _, tc := range tests {
		t.Run(tc.ddl, func(t *testing.T) {
			t.Parallel()
			schema, table, ok := (&DDLEvent{DDL: tc.ddl}).IndexedTable()
			require.Equal(t, tc.wantOK, ok)
			require.Equal(t, tc.wantSchema, schema)
			require.Equal(t, tc.wantTable, table)
		})
	}
}

func TestDDLEvent_RenamedTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ddl        string
		wantSchema string
		wantTable  string
		wantOK     bool
	}{
		{ddl: `ALTER TABLE "unlink_medium_confirmation_wizard" RENAME TO "product_catalog_unlink_medium_confirmation_wizard"`, wantTable: "unlink_medium_confirmation_wizard", wantOK: true},
		{ddl: `alter table if exists only "public"."Old" rename to new_name`, wantSchema: "public", wantTable: "Old", wantOK: true},
		{ddl: "ALTER TABLE Old_Name\n RENAME TO new_name", wantTable: "old_name", wantOK: true},
		{ddl: `ALTER TABLE "t" RENAME COLUMN "a" TO "b"`},
		{ddl: `ALTER TABLE "t" RENAME CONSTRAINT "a" TO "b"`},
		{ddl: `ALTER TABLE "t" ADD COLUMN "rename" text`},
		{ddl: `ALTER INDEX "t_pkey" RENAME TO "u_pkey"`},
	}

	for _, tc := range tests {
		t.Run(tc.ddl, func(t *testing.T) {
			t.Parallel()
			schema, table, ok := (&DDLEvent{DDL: tc.ddl}).RenamedTable()
			require.Equal(t, tc.wantOK, ok)
			require.Equal(t, tc.wantSchema, schema)
			require.Equal(t, tc.wantTable, table)
		})
	}
}
