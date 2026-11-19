// SPDX-License-Identifier: Apache-2.0

package postgres

import "context"

// AcquireFunc lazily yields a Postgres connection. Useful when a set of
// related callers want to share a single TCP connection without each one
// having to think about lifecycle.
type AcquireFunc func(ctx context.Context) (Querier, error)

// LazyConn memoises a single *Conn (or its dial error) for a URL. Cheap to
// construct: nothing is opened until Acquire is called for the first time.
// Not safe for concurrent use — designed for the sequential case (e.g. a
// preflight check engine that runs checks one after another).
type LazyConn struct {
	url  string
	conn *Conn
	err  error
}

// NewLazyConn returns a LazyConn that will open a connection to url on first
// Acquire.
func NewLazyConn(url string) *LazyConn {
	return &LazyConn{url: url}
}

// Acquire returns the cached conn, opening it on the first call and again
// once it has been lost. A dial failure is cached — subsequent calls return
// the same error without retrying.
func (l *LazyConn) Acquire(ctx context.Context) (Querier, error) {
	if l.err != nil {
		return nil, l.err
	}
	if l.conn == nil || l.conn.IsClosed() {
		conn, err := NewConn(ctx, l.url)
		if err != nil {
			l.conn, l.err = nil, err
			return nil, err
		}
		l.conn = conn
	}
	return l.conn, nil
}

// Lost reports whether a connection was opened and has since been closed by a
// network error or by the server going away.
func (l *LazyConn) Lost() bool {
	return l.conn != nil && l.conn.IsClosed()
}

// Close releases the underlying connection if one was opened.
func (l *LazyConn) Close(ctx context.Context) error {
	if l.conn == nil {
		return nil
	}
	c := l.conn
	l.conn = nil
	return c.Close(ctx)
}
