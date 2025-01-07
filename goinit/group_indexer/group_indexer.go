package group_indexer

import (
	"fmt"
	"iter"
)

type Indexer struct {
	size     int
	groups   int
	reminder int
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
			size:     1,
			groups:   n,
			reminder: 0,
		}, nil
	}

	return Indexer{
		size:     div,
		groups:   n / div,
		reminder: n % div,
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

		for range idx.reminder {
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
		last_i, ok := next()
		i := last_i

		if !ok {
			return
		}

		last_val := fn()

		for ok {
			if i != last_i {
				last_val = fn()
				last_i = i
			}

			if !yield(i, last_val) {
				return
			}

			i, ok = next()
		}
	}
}
