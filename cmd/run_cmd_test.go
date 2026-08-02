// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

// snapshotFlagCmd builds a command carrying the flags initialSnapshotFlagBinding
// reads, wired the way rootCmd wires them. An empty snapshotTables leaves the
// flag unset.
func snapshotFlagCmd(t *testing.T, snapshotTables string, dataOnly bool, target string) *cobra.Command {
	t.Helper()

	cmd := &cobra.Command{Use: "run"}
	cmd.Flags().StringSlice("snapshot-tables", nil, "")
	cmd.Flags().Bool("data-only", false, "")
	cmd.Flags().String("target", "", "")
	cmd.Flags().Bool("reset", false, "")
	cmd.Flags().String("dump-file", "", "")

	if snapshotTables != "" {
		require.NoError(t, cmd.Flags().Set("snapshot-tables", snapshotTables))
	}
	if dataOnly {
		require.NoError(t, cmd.Flags().Set("data-only", "true"))
	}
	require.NoError(t, cmd.Flags().Set("target", target))

	return cmd
}

func TestInitialSnapshotFlagBindingSetsSnapshotMode(t *testing.T) {
	tests := []struct {
		name     string
		dataOnly bool
		wantMode string
	}{
		{name: "data only", dataOnly: true, wantMode: "data"},
		{name: "full", dataOnly: false, wantMode: "full"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(viper.Reset)
			viper.Reset()

			initialSnapshotFlagBinding(snapshotFlagCmd(t, "public.*", tc.dataOnly, postgres))

			// the yaml configuration is read when a config file is provided, and
			// the env one when it is not, so the flag has to reach both keys
			require.Equal(t, tc.wantMode, viper.GetString("source.postgres.snapshot.mode"))
			require.Equal(t, tc.wantMode, viper.GetString("PGSTREAM_POSTGRES_SNAPSHOT_MODE"))
		})
	}
}

func TestInitialSnapshotFlagBindingLeavesSnapshotModeUnsetWithoutSnapshotTables(t *testing.T) {
	t.Cleanup(viper.Reset)
	viper.Reset()

	initialSnapshotFlagBinding(snapshotFlagCmd(t, "", false, postgres))

	require.Empty(t, viper.GetString("source.postgres.snapshot.mode"))
	require.Empty(t, viper.GetString("PGSTREAM_POSTGRES_SNAPSHOT_MODE"))
}

func TestInitialSnapshotFlagBindingEnablesBulkIngest(t *testing.T) {
	t.Cleanup(viper.Reset)
	viper.Reset()

	viper.Set("PGSTREAM_POSTGRES_LISTENER_URL", "postgres://source/db")
	viper.Set("PGSTREAM_POSTGRES_WRITER_TARGET_URL", "postgres://target/db")

	initialSnapshotFlagBinding(snapshotFlagCmd(t, "public.*", false, postgres))

	// the snapshot tables are bound to a stringSlice flag, so reading them as a
	// string returns "" and previously skipped this block, leaving the snapshot
	// on row by row inserts with triggers still enabled
	require.True(t, viper.GetBool("PGSTREAM_POSTGRES_WRITER_BULK_INGEST_ENABLED"))
	require.True(t, viper.GetBool("PGSTREAM_POSTGRES_WRITER_DISABLE_TRIGGERS"))
}

func TestInitialSnapshotFlagBindingLeavesBulkIngestAloneWithoutSnapshotTables(t *testing.T) {
	t.Cleanup(viper.Reset)
	viper.Reset()

	viper.Set("PGSTREAM_POSTGRES_LISTENER_URL", "postgres://source/db")
	viper.Set("PGSTREAM_POSTGRES_WRITER_TARGET_URL", "postgres://target/db")

	initialSnapshotFlagBinding(snapshotFlagCmd(t, "", false, postgres))

	require.False(t, viper.GetBool("PGSTREAM_POSTGRES_WRITER_BULK_INGEST_ENABLED"))
	require.False(t, viper.GetBool("PGSTREAM_POSTGRES_WRITER_DISABLE_TRIGGERS"))
}

func TestInitialSnapshotFlagBindingRespectsExplicitBulkIngestSetting(t *testing.T) {
	t.Cleanup(viper.Reset)
	viper.Reset()

	viper.Set("PGSTREAM_POSTGRES_LISTENER_URL", "postgres://source/db")
	viper.Set("PGSTREAM_POSTGRES_WRITER_TARGET_URL", "postgres://target/db")
	viper.Set("PGSTREAM_POSTGRES_WRITER_BULK_INGEST_ENABLED", "false")

	initialSnapshotFlagBinding(snapshotFlagCmd(t, "public.*", false, postgres))

	require.False(t, viper.GetBool("PGSTREAM_POSTGRES_WRITER_BULK_INGEST_ENABLED"))
}
