package groupindexer

import (
	"iter"
	"strconv"
)

type Indexer struct {
	size      int64
	groups    int64
	remainder int64
}

func NewIndexer(n, div int64) Indexer {
	if n < 1 || div < 1 {
		return Indexer{
			size:      0,
			groups:    0,
			remainder: 0,
		}
	} else if n <= div {
		return Indexer{
			size:      1,
			groups:    n,
			remainder: 0,
		}
	} else {
		return Indexer{
			size:      div,
			groups:    n / div,
			remainder: n % div,
		}
	}
}

func (idx Indexer) Iterate() iter.Seq[int64] {
	return func(yield func(int64) bool) {
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

func (idx Indexer) IterateStr() iter.Seq[string] {
	return func(yield func(string) bool) {
		for num := range idx.Iterate() {
			if !yield(strconv.FormatInt(num, 10)) {
				return
			}
		}
	}
}

func IterateWithFunc[T any](idx Indexer, fn func() T) iter.Seq2[int64, T] {
	return func(yield func(int64, T) bool) {
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

func IterateWithMap2[T any](idx Indexer, mapFn func(int64) T) iter.Seq2[int64, T] {
	return func(yield func(int64, T) bool) {
		next, stop := iter.Pull(idx.Iterate())
		defer stop()
		lastIdx, ok := next()
		idx := lastIdx

		if !ok {
			return
		}

		lastVal := mapFn(idx)

		for ok {
			if idx != lastIdx {
				lastVal = mapFn(idx)
				lastIdx = idx
			}

			if !yield(idx, lastVal) {
				return
			}

			idx, ok = next()
		}
	}
}

func IterateWithMap[T any](idx Indexer, mapFn func(int64) T) iter.Seq[T] {
	return func(yield func(T) bool) {
		for _, val := range IterateWithMap2(idx, mapFn) {
			if !yield(val) {
				return
			}
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

func NewIndexerIterator(n, div int64) Iterator[int64] {
	return NewIterator(NewIndexer(n, div).Iterate())
}

func NewIndexerIteratorStr(n, div int64) Iterator[string] {
	return NewIterator(NewIndexer(n, div).IterateStr())
}

func NewIndexerIteratorWithMap[T any](n, div int64, mapFn func(int64) T) Iterator[T] {
	return NewIterator(IterateWithMap(NewIndexer(n, div), mapFn))
}
