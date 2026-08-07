// SPDX-License-Identifier: Apache-2.0

package adapter

import (
	pglib "github.com/xataio/pgstream/internal/postgres"
)

const publicSchema = pglib.PublicSchema

type SnapshotConfig struct {
	Tables         []string
	ExcludedTables []string
	// SchemaOnlyTables are included in the schema snapshot but skipped by the
	// data snapshot.
	SchemaOnlyTables []string
}

// schemaTableMap groups a table list by schema. Names are split by
// pglib.ParseTableName so a list means the same thing here as it does wherever
// else it is parsed. Malformed entries are rejected during config validation,
// which builds a pglib.SchemaTableMap from these same lists, so an entry that
// fails to parse here is kept whole rather than dropped.
func schemaTableMap(tables []string) map[string][]string {
	schemaTableMap := make(map[string][]string, len(tables))
	for _, table := range tables {
		schemaName, tableName, err := pglib.ParseTableName(table)
		if err != nil {
			schemaName, tableName = publicSchema, table
		}
		schemaTableMap[schemaName] = append(schemaTableMap[schemaName], tableName)
	}
	return schemaTableMap
}
