package groupindexer

import (
	"fmt"
	"iter"
)

type Indexer struct {
	size      int
	groups    int
	remainder int
}

func NewIndexer(n, div int) (Indexer, error) {
	if n < 1 || div < 1 {
		return Indexer{}, fmt.Errorf(
			"invalid input: n=%d and div=%d must be positive",
			n, div,
		)
	}

	if n <= div {
		return Indexer{
			size:      1,
			groups:    n,
			remainder: 0,
		}, nil
	}

	return Indexer{
		size:      div,
		groups:    n / div,
		remainder: n % div,
	}, nil
}

func (idx Indexer) Iterate() iter.Seq[int] {
	return func(yield func(int) bool) {
		for i := range idx.groups {
			for range idx.size {
				if !yield(i) {
					return
				}
			}
		}

		for range idx.remainder {
			if !yield(idx.groups) {
				return
			}
		}
	}
}

func IterateWithFunc[T any](idx Indexer, fn func() T) iter.Seq2[int, T] {
	return func(yield func(int, T) bool) {
		next, stop := iter.Pull(idx.Iterate())
		defer stop()
		lastIdx, ok := next()
		idx := lastIdx

		if !ok {
			return
		}

		lastVal := fn()

		for ok {
			if idx != lastIdx {
				lastVal = fn()
				lastIdx = idx
			}

			if !yield(idx, lastVal) {
				return
			}

			idx, ok = next()
		}
	}
}

// Iterator defines the interface for pull-style iteration
type Iterator[T any] interface {
	// Next returns the next value in the sequence
	// If done is false, there are no more values and value should be ignored
	Next() (value T, done bool)

	// Close cleans up any resources used by the iterator
	Close()
}

// iteratorImpl is the concrete type that implements Iterator[T]
type iteratorImpl[T any] struct {
	next func() (T, bool)
	stop func()
}

// These are the interface implementation methods:

func (it *iteratorImpl[T]) Next() (value T, done bool) {
	if it.next == nil {
		return value, false
	}
	value, ok := it.next()
	if !ok {
		it.Close()
		return value, false
	}
	return value, true
}

func (it *iteratorImpl[T]) Close() {
	if it.stop != nil {
		it.stop()
		it.stop = nil
		it.next = nil
	}
}

// NewIterator creates an Iterator from an iter.Seq
// It returns the interface type, but internally creates an iteratorImpl
func NewIterator[T any](seq iter.Seq[T]) Iterator[T] {
	next, stop := iter.Pull(seq)
	// Create the concrete implementation
	impl := &iteratorImpl[T]{
		next: next,
		stop: stop,
	}
	// Return it as the interface type
	return impl
}
