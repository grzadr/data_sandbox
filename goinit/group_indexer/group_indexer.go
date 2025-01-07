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
