// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"regexp"

	"github.com/jackc/pgx/v5/pgconn"
	pglib "github.com/xataio/pgstream/internal/postgres"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
)

// The checkpoint only advances once a whole batch is sent, so a restart
// replays every DDL statement sent since, and replayed DDL fails: the column
// already exists, the table was already renamed. Each statement commits in the
// same transaction as a row naming its position in the source WAL, and a
// statement whose position is already recorded is skipped.
//
// Positions are kept as a set, not a high-water mark: concurrent transactions
// commit out of LSN order, so a later DDL can carry a lower LSN than one
// already applied. They only mean anything against the source they came from,
// so a target re-pointed at a restored source must drop pgstream.applied_ddl.
const (
	createAppliedDDLQuery = `CREATE SCHEMA IF NOT EXISTS pgstream;
CREATE TABLE IF NOT EXISTS pgstream.applied_ddl (
	lsn pg_lsn NOT NULL,
	ordinal integer NOT NULL,
	applied_at timestamptz NOT NULL DEFAULT now(),
	PRIMARY KEY (lsn, ordinal)
);
DELETE FROM pgstream.applied_ddl WHERE applied_at < now() - interval '30 days'`
	// Held until COMMIT, so a retry after a lost COMMIT waits for the orphaned
	// transaction to finish and then sees its row, instead of racing it.
	lockAppliedDDLQuery = `SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('pgstream.applied_ddl'))`
	ddlAppliedQuery     = `SELECT EXISTS (SELECT 1 FROM pgstream.applied_ddl WHERE lsn = $1::pg_lsn AND ordinal = $2)`
	recordDDLQuery      = `INSERT INTO pgstream.applied_ddl (lsn, ordinal) VALUES ($1::pg_lsn, $2) ON CONFLICT DO NOTHING`
	setSearchPathQuery  = `SELECT pg_catalog.set_config('search_path', $1, false)`
	indexValidQuery     = `SELECT (SELECT indisvalid FROM pg_catalog.pg_index WHERE indexrelid = pg_catalog.to_regclass($1))`
)

// CONCURRENTLY refuses to run in a transaction block, as do the statements
// cannotRunInTx recognises by their error. They run bare and are recorded
// after, so a crash between the two still replays them.
var nonTransactionalDDLRegex = regexp.MustCompile(`(?i)\bCONCURRENTLY\b`)

// ddlPosition identifies a DDL statement in the source WAL: the LSN of the
// logical message that carried it, and its index among the statements built
// from that message. An empty lsn is untracked.
type ddlPosition struct {
	lsn     string
	ordinal int
}

// The schema snapshot replayed through the WAL carries no position: every one
// of its statements is at wal.ZeroLSN.
func newDDLPosition(lsn string, ordinal int) ddlPosition {
	if lsn == wal.ZeroLSN {
		lsn = ""
	}
	return ddlPosition{lsn: lsn, ordinal: ordinal}
}

// ddlEventInfo is what the writer needs from the event beyond the statement.
type ddlEventInfo struct {
	commandTag string
	// identity of the index a CREATE INDEX builds, schema-qualified and quoted
	index string
}

func newDDLEventInfo(d *wal.Data) ddlEventInfo {
	ddlEvent, err := wal.WalDataToDDLEvent(d)
	if err != nil {
		return ddlEventInfo{}
	}
	info := ddlEventInfo{commandTag: ddlEvent.CommandTag}
	if indexes := ddlEvent.GetObjectsByType(wal.ObjectTypeIndex); len(indexes) == 1 {
		info.index = indexes[0].Identity
	}
	return info
}

type rowQuerier interface {
	QueryRow(ctx context.Context, dest []any, query string, args ...any) error
}

// execDDL applies a replicated DDL statement. A connection lost since the last
// statement - the target restarted while no DDL was flowing - is redialled and
// the statement retried once; that is safe even when the first attempt's
// COMMIT was lost, since a committed statement is recorded and skipped.
func (w *BatchWriter) execDDL(ctx context.Context, q *query, pos ddlPosition, info ddlEventInfo) error {
	err := w.applyDDL(ctx, q, pos, info)
	if err != nil && w.ddlConn.Lost() {
		w.logger.Warn(err, "DDL connection lost, reconnecting", loglib.Fields{"query_sql": q.sql})
		err = w.applyDDL(ctx, q, pos, info)
	}
	if err == nil {
		return nil
	}

	// Indexes and sequences carry no replicated data - rows bring their own
	// ids - so one built on, renamed for or dropped from a relation the target
	// never had (its table is excluded) is already in the state the statement
	// asks for.
	if isDatalessDDL(info.commandTag) && isUndefinedRelation(err) {
		w.logger.Warn(err, "DDL names a relation the target does not have, nothing to apply", loglib.Fields{"query_sql": q.sql})
		return w.recordDDL(ctx, pos)
	}

	w.logger.Error(err, "running DDL query", loglib.Fields{"query_sql": q.sql, "query_args": q.args})
	if w.isInternalError(err) {
		return err
	}
	if w.strictMode {
		return fmt.Errorf("strict mode: stopping on non-internal DDL failure: %w", err)
	}
	w.recordDroppedQuery(q, err)
	return w.recordDDL(ctx, pos)
}

func (w *BatchWriter) applyDDL(ctx context.Context, q *query, pos ddlPosition, info ddlEventInfo) error {
	conn, err := w.ddlConn.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquiring DDL connection: %w", err)
	}

	// The default search path is "$user",public, so a role sharing its name
	// with pgstream's own internal schema silently creates the object there
	// instead of in the target schema. Pin it to the schema the event was
	// captured in. Both statements run on the same connection, so the setting
	// is guaranteed to apply to the statement it is meant for; nothing else
	// uses that connection, so there is nothing to restore it to afterwards.
	if q.schema != "" {
		if _, err := conn.Exec(ctx, setSearchPathQuery, q.schema); err != nil {
			return fmt.Errorf("setting search path for DDL query: %w", err)
		}
	}

	if pos.lsn == "" {
		_, err := conn.Exec(ctx, q.sql, q.args...)
		return err
	}
	if err := w.ensureAppliedDDL(ctx, conn); err != nil {
		return err
	}

	if nonTransactionalDDLRegex.MatchString(q.sql) {
		return w.applyDDLOutsideTx(ctx, conn, q, pos, info)
	}

	err = conn.ExecInTx(ctx, func(tx pglib.Tx) error {
		if _, err := tx.Exec(ctx, lockAppliedDDLQuery); err != nil {
			return err
		}
		if applied, err := w.ddlApplied(ctx, tx, q, pos); err != nil || applied {
			return err
		}
		if _, err := tx.Exec(ctx, q.sql, q.args...); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, recordDDLQuery, pos.lsn, pos.ordinal)
		return err
	})
	if cannotRunInTx(err) {
		return w.applyDDLOutsideTx(ctx, conn, q, pos, info)
	}
	return err
}

func (w *BatchWriter) applyDDLOutsideTx(ctx context.Context, conn pglib.Querier, q *query, pos ddlPosition, info ddlEventInfo) error {
	if applied, err := w.ddlApplied(ctx, conn, q, pos); err != nil || applied {
		return err
	}
	if info.commandTag == "CREATE INDEX" && info.index != "" {
		built, err := w.resumeConcurrentIndex(ctx, conn, info.index)
		if err != nil {
			return err
		}
		if built {
			return w.recordDDL(ctx, pos)
		}
	}
	if _, err := conn.Exec(ctx, q.sql, q.args...); err != nil {
		return err
	}
	_, err := conn.Exec(ctx, recordDDLQuery, pos.lsn, pos.ordinal)
	return err
}

// resumeConcurrentIndex deals with a CREATE INDEX CONCURRENTLY a lost
// connection cut short. That leaves an invalid index under the same name, and
// rerunning the statement fails on it as a duplicate. Reports whether the
// index is already built, in which case the statement must not run again.
func (w *BatchWriter) resumeConcurrentIndex(ctx context.Context, conn pglib.Querier, index string) (bool, error) {
	var valid *bool
	if err := conn.QueryRow(ctx, []any{&valid}, indexValidQuery, index); err != nil {
		return false, fmt.Errorf("checking for an earlier attempt at the index: %w", err)
	}
	switch {
	case valid == nil:
		return false, nil
	case *valid:
		w.logger.Info("index already built by an earlier attempt", loglib.Fields{"index": index})
		return true, nil
	}
	w.logger.Warn(nil, "dropping the invalid index an interrupted attempt left behind", loglib.Fields{"index": index})
	if _, err := conn.Exec(ctx, "DROP INDEX CONCURRENTLY IF EXISTS "+index); err != nil {
		return false, fmt.Errorf("dropping invalid index: %w", err)
	}
	return false, nil
}

func (w *BatchWriter) ddlApplied(ctx context.Context, rq rowQuerier, q *query, pos ddlPosition) (bool, error) {
	var applied bool
	if err := rq.QueryRow(ctx, []any{&applied}, ddlAppliedQuery, pos.lsn, pos.ordinal); err != nil {
		return false, fmt.Errorf("checking for applied DDL: %w", err)
	}
	if applied {
		w.logger.Info("skipping DDL already applied", loglib.Fields{"lsn": pos.lsn, "ordinal": pos.ordinal, "query_sql": q.sql})
	}
	return applied, nil
}

// recordDDL marks a statement handled without having applied it, so a replay
// doesn't try it again.
func (w *BatchWriter) recordDDL(ctx context.Context, pos ddlPosition) error {
	if pos.lsn == "" {
		return nil
	}
	conn, err := w.ddlConn.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquiring DDL connection: %w", err)
	}
	if err := w.ensureAppliedDDL(ctx, conn); err != nil {
		return err
	}
	if _, err := conn.Exec(ctx, recordDDLQuery, pos.lsn, pos.ordinal); err != nil {
		return fmt.Errorf("recording applied DDL: %w", err)
	}
	return nil
}

func (w *BatchWriter) ensureAppliedDDL(ctx context.Context, conn pglib.Querier) error {
	if w.appliedDDLReady {
		return nil
	}
	if _, err := conn.Exec(ctx, createAppliedDDLQuery); err != nil {
		return fmt.Errorf("creating applied DDL table: %w", err)
	}
	w.appliedDDLReady = true
	return nil
}

func isDatalessDDL(commandTag string) bool {
	switch commandTag {
	case "CREATE INDEX", "ALTER INDEX", "DROP INDEX", "ALTER SEQUENCE", "DROP SEQUENCE":
		return true
	}
	return false
}

// isUndefinedRelation matches undefined_table, and undefined_object, which is
// what DROP INDEX raises for a missing index.
func isUndefinedRelation(err error) bool {
	var e *pglib.ErrRelationDoesNotExist
	return errors.As(err, &e) && (e.Code == "42P01" || e.Code == "42704")
}

// cannotRunInTx matches active_sql_transaction, from statements such as
// REINDEX SCHEMA that refuse a transaction block, and
// invalid_transaction_termination, from a procedure that commits. Either way
// the transaction was rolled back and nothing was applied.
func cannotRunInTx(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && (pgErr.Code == "25001" || pgErr.Code == "2D000")
}
