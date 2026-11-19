// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	pgmocks "github.com/xataio/pgstream/internal/postgres/mocks"
	loglib "github.com/xataio/pgstream/pkg/log"
)

// lossyDDLConn hands out conns in order, moving to the next once the current
// one is reported lost.
type lossyDDLConn struct {
	conns []*pgmocks.Querier
	idx   int
	lost  bool
}

func (c *lossyDDLConn) Acquire(context.Context) (pglib.Querier, error) {
	if c.lost {
		c.idx++
		c.lost = false
	}
	return c.conns[c.idx], nil
}
func (c *lossyDDLConn) Lost() bool                  { return c.lost }
func (c *lossyDDLConn) Close(context.Context) error { return nil }

// fakeDDLTarget records every statement run on it, in and out of a
// transaction, and answers the applied and index-validity lookups.
type fakeDDLTarget struct {
	q          *pgmocks.Querier
	ran        []string
	inTx       []string
	applied    bool
	indexValid *bool
	failOn     map[string]error
	txErr      error
}

func newFakeDDLTarget() *fakeDDLTarget {
	c := &fakeDDLTarget{failOn: map[string]error{}}
	answer := func(dest []any, sql string) error {
		switch d := dest[0].(type) {
		case *bool:
			*d = c.applied
		case **bool:
			*d = c.indexValid
		default:
			return errors.New("unexpected destination")
		}
		return nil
	}
	c.q = &pgmocks.Querier{
		ExecFn: func(_ context.Context, _ uint, sql string, _ ...any) (pglib.CommandTag, error) {
			c.ran = append(c.ran, sql)
			return pglib.CommandTag{}, c.failOn[sql]
		},
		QueryRowFn: func(_ context.Context, dest []any, sql string, _ ...any) error {
			c.ran = append(c.ran, sql)
			return answer(dest, sql)
		},
		ExecInTxFn: func(ctx context.Context, f func(tx pglib.Tx) error) error {
			if c.txErr != nil {
				return c.txErr
			}
			return f(&pgmocks.Tx{
				ExecFn: func(_ context.Context, _ uint, sql string, _ ...any) (pglib.CommandTag, error) {
					c.inTx = append(c.inTx, sql)
					return pglib.CommandTag{}, c.failOn[sql]
				},
				QueryRowFn: func(_ context.Context, dest []any, sql string, _ ...any) error {
					c.inTx = append(c.inTx, sql)
					return answer(dest, sql)
				},
			})
		},
	}
	return c
}

func newDDLTestWriter(strict bool, targets ...*fakeDDLTarget) *BatchWriter {
	qs := make([]*pgmocks.Querier, 0, len(targets))
	for _, c := range targets {
		qs = append(qs, c.q)
	}
	return &BatchWriter{
		Writer:  &Writer{logger: loglib.NewNoopLogger(), strictMode: strict},
		ddlConn: &lossyDDLConn{conns: qs},
	}
}

func TestBatchWriter_execDDL(t *testing.T) {
	t.Parallel()

	const ddl = `ALTER TABLE "payment_transaction" ADD COLUMN "custom_payment_ref" VARCHAR`
	pos := newDDLPosition("45/10D67ED0", 0)
	q := &query{schema: "public", sql: ddl, isDDL: true}
	alterTable := ddlEventInfo{commandTag: "ALTER TABLE"}
	ctx := context.Background()

	t.Run("applies in a transaction with its position", func(t *testing.T) {
		t.Parallel()
		c := newFakeDDLTarget()
		require.NoError(t, newDDLTestWriter(true, c).execDDL(ctx, q, pos, alterTable))
		require.Equal(t, []string{setSearchPathQuery, createAppliedDDLQuery}, c.ran)
		require.Equal(t, []string{lockAppliedDDLQuery, ddlAppliedQuery, ddl, recordDDLQuery}, c.inTx)
	})

	t.Run("skips a statement already applied", func(t *testing.T) {
		t.Parallel()
		c := newFakeDDLTarget()
		c.applied = true
		require.NoError(t, newDDLTestWriter(true, c).execDDL(ctx, q, pos, alterTable))
		require.Equal(t, []string{lockAppliedDDLQuery, ddlAppliedQuery}, c.inTx)
	})

	t.Run("untracked statements run as before", func(t *testing.T) {
		t.Parallel()
		for _, p := range []ddlPosition{newDDLPosition("", 0), newDDLPosition("0/0", 0), newDDLPosition("0/0", 1)} {
			c := newFakeDDLTarget()
			require.NoError(t, newDDLTestWriter(true, c).execDDL(ctx, q, p, alterTable))
			require.Equal(t, []string{setSearchPathQuery, ddl}, c.ran)
			require.Empty(t, c.inTx)
		}
	})

	t.Run("redials a lost connection and retries", func(t *testing.T) {
		t.Parallel()
		dead, live := newFakeDDLTarget(), newFakeDDLTarget()
		w := newDDLTestWriter(true, dead, live)
		lossy, ok := w.ddlConn.(*lossyDDLConn)
		require.True(t, ok)
		dead.q.ExecFn = func(context.Context, uint, string, ...any) (pglib.CommandTag, error) {
			lossy.lost = true
			return pglib.CommandTag{}, errors.New("write failed: broken pipe")
		}
		require.NoError(t, w.execDDL(ctx, q, pos, alterTable))
		require.Equal(t, []string{lockAppliedDDLQuery, ddlAppliedQuery, ddl, recordDDLQuery}, live.inTx)
	})

	t.Run("a connection error that isn't a loss is returned", func(t *testing.T) {
		t.Parallel()
		c := newFakeDDLTarget()
		errTest := errors.New("boom")
		c.failOn[setSearchPathQuery] = errTest
		require.ErrorIs(t, newDDLTestWriter(false, c).execDDL(ctx, q, pos, alterTable), errTest)
	})

	t.Run("statements refusing a transaction block run bare", func(t *testing.T) {
		t.Parallel()
		for _, code := range []string{"25001", "2D000"} {
			c := newFakeDDLTarget()
			c.txErr = &pgconn.PgError{Code: code}
			rq := &query{sql: "REINDEX SCHEMA public", isDDL: true}
			require.NoError(t, newDDLTestWriter(true, c).execDDL(ctx, rq, pos, ddlEventInfo{commandTag: "REINDEX"}))
			require.Equal(t, []string{createAppliedDDLQuery, ddlAppliedQuery, rq.sql, recordDDLQuery}, c.ran)
		}
	})

	t.Run("dataless DDL on a missing relation is recorded, even in strict mode", func(t *testing.T) {
		t.Parallel()
		for _, tc := range []struct{ tag, code string }{
			{"ALTER INDEX", "42P01"},
			{"CREATE INDEX", "42P01"},
			{"DROP INDEX", "42704"},
			{"ALTER SEQUENCE", "42P01"},
			{"DROP SEQUENCE", "42P01"},
		} {
			c := newFakeDDLTarget()
			c.failOn[ddl] = &pglib.ErrRelationDoesNotExist{Code: tc.code}
			require.NoError(t, newDDLTestWriter(true, c).execDDL(ctx, q, pos, ddlEventInfo{commandTag: tc.tag}), tc.tag)
			require.Equal(t, []string{setSearchPathQuery, createAppliedDDLQuery, recordDDLQuery}, c.ran, tc.tag)
		}
	})

	t.Run("a missing relation on table DDL still stops strict mode", func(t *testing.T) {
		t.Parallel()
		c := newFakeDDLTarget()
		c.failOn[ddl] = &pglib.ErrRelationDoesNotExist{Code: "42P01"}
		require.ErrorContains(t, newDDLTestWriter(true, c).execDDL(ctx, q, pos, alterTable), "strict mode")
	})

	t.Run("a missing column is not treated as a missing relation", func(t *testing.T) {
		t.Parallel()
		c := newFakeDDLTarget()
		c.failOn[ddl] = &pglib.ErrRelationDoesNotExist{Code: "42703"}
		require.ErrorContains(t, newDDLTestWriter(true, c).execDDL(ctx, q, pos, ddlEventInfo{commandTag: "CREATE INDEX"}), "strict mode")
	})

	t.Run("strict mode stops on a non-internal failure without recording it", func(t *testing.T) {
		t.Parallel()
		c := newFakeDDLTarget()
		c.failOn[ddl] = &pglib.ErrRelationAlreadyExists{}
		require.ErrorContains(t, newDDLTestWriter(true, c).execDDL(ctx, q, pos, alterTable), "strict mode")
		require.NotContains(t, c.ran, recordDDLQuery)
	})

	t.Run("relaxed mode drops the statement and records it", func(t *testing.T) {
		t.Parallel()
		c := newFakeDDLTarget()
		c.failOn[ddl] = &pglib.ErrRelationAlreadyExists{}
		w := newDDLTestWriter(false, c)
		require.NoError(t, w.execDDL(ctx, q, pos, alterTable))
		require.Equal(t, uint64(1), w.DroppedQueries())
		require.Contains(t, c.ran, recordDDLQuery)
	})

	cic := &query{sql: `CREATE INDEX CONCURRENTLY "i" ON "t" (a)`, isDDL: true}
	cicInfo := ddlEventInfo{commandTag: "CREATE INDEX", index: "public.i"}

	t.Run("CONCURRENTLY runs outside a transaction", func(t *testing.T) {
		t.Parallel()
		c := newFakeDDLTarget()
		require.NoError(t, newDDLTestWriter(true, c).execDDL(ctx, cic, pos, cicInfo))
		require.Equal(t, []string{createAppliedDDLQuery, ddlAppliedQuery, indexValidQuery, cic.sql, recordDDLQuery}, c.ran)
		require.Empty(t, c.inTx)
	})

	t.Run("an index an interrupted attempt left invalid is rebuilt", func(t *testing.T) {
		t.Parallel()
		c := newFakeDDLTarget()
		invalid := false
		c.indexValid = &invalid
		require.NoError(t, newDDLTestWriter(true, c).execDDL(ctx, cic, pos, cicInfo))
		require.Equal(t, []string{createAppliedDDLQuery, ddlAppliedQuery, indexValidQuery, "DROP INDEX CONCURRENTLY IF EXISTS public.i", cic.sql, recordDDLQuery}, c.ran)
	})

	t.Run("an index an earlier attempt finished is only recorded", func(t *testing.T) {
		t.Parallel()
		c := newFakeDDLTarget()
		valid := true
		c.indexValid = &valid
		require.NoError(t, newDDLTestWriter(true, c).execDDL(ctx, cic, pos, cicInfo))
		require.Equal(t, []string{createAppliedDDLQuery, ddlAppliedQuery, indexValidQuery, recordDDLQuery}, c.ran)
	})
}
