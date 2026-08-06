// SPDX-License-Identifier: Apache-2.0

package batch

import (
	"github.com/xataio/pgstream/pkg/wal"
)

type Batch[T Message] struct {
	messages   []T
	positions  []wal.CommitPosition
	totalBytes int
	// acquiredBytes is what the sender charged the queue semaphore for the
	// messages in this batch. It is tracked separately from totalBytes because
	// the semaphore is acquired for every message, while totalBytes only counts
	// the ones that carry data: releasing totalBytes would strand the
	// difference on every batch, and the queue would eventually deadlock.
	acquiredBytes int64
}

func NewBatch[T Message](messages []T, positions []wal.CommitPosition) *Batch[T] {
	return &Batch[T]{
		messages:  messages,
		positions: positions,
	}
}

func (b *Batch[T]) GetMessages() []T {
	return b.messages
}

func (b *Batch[T]) GetCommitPositions() []wal.CommitPosition {
	return b.positions
}

func (b *Batch[T]) add(m *WALMessage[T]) {
	// charged against the queue semaphore for every message, data or not
	b.acquiredBytes += int64(m.Size())

	if !m.message.IsEmpty() {
		b.messages = append(b.messages, m.message)
		b.totalBytes += m.message.Size()
	}

	if m.position != "" && m.position != wal.ZeroLSN {
		b.positions = append(b.positions, m.position)
	}
}

func (b *Batch[T]) drain() *Batch[T] {
	batch := &Batch[T]{
		messages:      b.messages,
		positions:     b.positions,
		totalBytes:    b.totalBytes,
		acquiredBytes: b.acquiredBytes,
	}

	b.messages = []T{}
	b.totalBytes = 0
	b.acquiredBytes = 0
	b.positions = []wal.CommitPosition{}
	return batch
}

func (b *Batch[T]) isEmpty() bool {
	return len(b.messages) == 0 && len(b.positions) == 0
}

func (b *Batch[T]) maxBatchBytesReached(maxBatchBytes int64, msg T) bool {
	return maxBatchBytes > 0 && b.totalBytes+msg.Size() >= int(maxBatchBytes)
}

// AcquiredBytes returns the queue-semaphore weight charged for the messages in
// this batch, which is what must be released once the batch has been sent.
func (b *Batch[T]) AcquiredBytes() int64 {
	return b.acquiredBytes
}
