// SPDX-License-Identifier: Apache-2.0

package pgdumprestore

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// pg_dump breaks a circular view dependency by emitting a dummy view - a bare
// SELECT of NULL casts with no FROM - into the main schema dump, and the real
// definition later. Skipping only the views dump therefore leaves the dummy
// behind: a view that exists and returns nothing. skip_views has to drop both.
const dumpWithDummyView = `CREATE TABLE public.account_move (
    id integer NOT NULL
);

CREATE VIEW public.stock_quant_error AS
 SELECT
    NULL::integer AS id,
    NULL::integer AS product_id;

CREATE OR REPLACE VIEW public.stock_quant_error AS
 SELECT q.id,
    q.product_id
   FROM public.stock_quant q;
`

func TestParseDump_skipViewsDropsDummyAndReal(t *testing.T) {
	t.Parallel()

	kept := (&SnapshotGenerator{}).parseDump([]byte(dumpWithDummyView))
	require.Contains(t, string(kept.filtered), "NULL::integer AS id",
		"precondition: the dummy view belongs to the main schema dump")
	require.Contains(t, string(kept.views), "CREATE OR REPLACE VIEW")

	skipped := (&SnapshotGenerator{skipViews: true}).parseDump([]byte(dumpWithDummyView))
	require.NotContains(t, string(skipped.filtered), "stock_quant_error",
		"dummy view survived into the main schema dump")
	require.NotContains(t, string(skipped.views), "stock_quant_error")
	require.Contains(t, string(skipped.filtered), "CREATE TABLE public.account_move",
		"skip_views must not disturb tables")
}

func TestParseDump_skipViewsDropsReturnRule(t *testing.T) {
	t.Parallel()

	const dumpWithRule = `CREATE TABLE public.legacy_view (
    id integer
);

CREATE RULE "_RETURN" AS
    ON SELECT TO public.legacy_view DO INSTEAD  SELECT t.id
   FROM public.real_table t;
`
	skipped := (&SnapshotGenerator{skipViews: true}).parseDump([]byte(dumpWithRule))
	require.NotContains(t, string(skipped.views), "_RETURN", "view-defining rule survived")
	require.False(t, strings.Contains(string(skipped.views), "legacy_view"))
}
