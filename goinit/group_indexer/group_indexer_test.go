package groupindexer

import (
	"reflect"
	"slices"
	"testing"
)

func TestNewIndexer(t *testing.T) {
	tests := []struct {
		name        string
		n           int
		div         int
		want        Indexer
		errContains string // Empty string means we don't expect an error
	}{
		{
			name: "basic case 1:1",
			n:    1,
			div:  1,
			want: Indexer{size: 1, groups: 1, remainder: 0},
		},
		{
			name: "n larger than 1, div is 1",
			n:    10,
			div:  1,
			want: Indexer{size: 1, groups: 10, remainder: 0},
		},
		{
			name: "n is 1, div larger than 1",
			n:    1,
			div:  10,
			want: Indexer{size: 1, groups: 1, remainder: 0},
		},
		{
			name: "equal division",
			n:    100,
			div:  10,
			want: Indexer{size: 10, groups: 10, remainder: 0},
		},
		{
			name: "with reminder case 1",
			n:    13,
			div:  3,
			want: Indexer{size: 3, groups: 4, remainder: 1},
		},
		{
			name: "with reminder case 2",
			n:    7,
			div:  3,
			want: Indexer{size: 3, groups: 2, remainder: 1},
		},
		{
			name:        "negative n",
			n:           -1,
			div:         3,
			errContains: "invalid input: n=-1 and div=3 must be positive",
		},
		{
			name:        "negative div",
			n:           5,
			div:         -3,
			errContains: "invalid input: n=5 and div=-3 must be positive",
		},
		{
			name:        "zero n",
			n:           0,
			div:         3,
			errContains: "invalid input: n=0 and div=3 must be positive",
		},
		{
			name:        "zero div",
			n:           5,
			div:         0,
			errContains: "invalid input: n=5 and div=0 must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewIndexer(tt.n, tt.div)

			if tt.errContains != "" {
				if err == nil {
					t.Errorf("NewIndexer(%d, %d) expected error containing %q, got nil",
						tt.n, tt.div, tt.errContains)
					return
				}
				if err.Error() != tt.errContains {
					t.Errorf("NewIndexer(%d, %d) error = %v, want error containing %q",
						tt.n, tt.div, err, tt.errContains)
				}
				return
			}

			// At this point, we expect no error
			if err != nil {
				t.Errorf("NewIndexer(%d, %d) unexpected error: %v",
					tt.n, tt.div, err)
				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewIndexer(%d, %d) = %+v, want %+v",
					tt.n, tt.div, got, tt.want)
			}
		})
	}
}

func TestIndexerIterate(t *testing.T) {
	tests := []struct {
		name     string
		n        int
		div      int
		expected []int
	}{
		{
			name:     "single element",
			n:        1,
			div:      1,
			expected: []int{0},
		},
		{
			name:     "seven elements divided into three groups",
			n:        7,
			div:      3,
			expected: []int{0, 0, 0, 1, 1, 1, 2},
		},
		{
			name:     "ten elements in single group",
			n:        10,
			div:      1,
			expected: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			indexer, err := NewIndexer(tt.n, tt.div)
			if err != nil {
				t.Fatalf("NewIndexer(%d, %d) returned unexpected error: %v",
					tt.n, tt.div, err)
			}

			var result []int
			for val := range indexer.Iterate() {
				result = append(result, val)
			}

			if !slices.Equal(result, tt.expected) {
				t.Errorf("Iterate() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestIterateWithFunc(t *testing.T) {
	tests := []struct {
		name     string
		n        int
		div      int
		input    []string
		expected []string
	}{
		{
			name:     "single element",
			n:        1,
			div:      1,
			input:    []string{"A", "B", "C"},
			expected: []string{"A"},
		},
		{
			name:     "multiple groups with reminder",
			n:        7,
			div:      3,
			input:    []string{"A", "B", "C"},
			expected: []string{"A", "A", "A", "B", "B", "B", "C"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx, err := NewIndexer(tt.n, tt.div)
			if err != nil {
				t.Fatalf("failed to create indexer: %v", err)
			}

			// Create an iterator over input labels
			pos := 0
			labelFunc := func() string {
				label := tt.input[pos]
				pos++
				return label
			}

			// Collect results from iterator
			var result []string
			for _, val := range IterateWithFunc(idx, labelFunc) {
				result = append(result, val)
			}

			if !slices.Equal(result, tt.expected) {
				t.Errorf("got %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestIteratorSequences(t *testing.T) {
	tests := []struct {
		name     string
		n        int
		div      int
		expected []int
	}{
		{
			name:     "single element",
			n:        1,
			div:      1,
			expected: []int{0},
		},
		{
			name:     "seven elements divided into three groups",
			n:        7,
			div:      3,
			expected: []int{0, 0, 0, 1, 1, 1, 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seq, _ := NewIndexer(tt.n, tt.div)
			iter := NewIterator(seq.Iterate())
			defer iter.Close()

			var got []int
			for {
				val, ok := iter.Next()
				if !ok {
					break
				}
				got = append(got, val)
			}

			// Compare lengths
			if len(got) != len(tt.expected) {
				t.Errorf("got %d values, want %d", len(got), len(tt.expected))
				return
			}

			// Compare values
			for i := range got {
				if got[i] != tt.expected[i] {
					t.Errorf("value at index %d = %v, want %v", i, got[i], tt.expected[i])
				}
			}
		})
	}
}

// TestIteratorEarlyBreak verifies that the iterator maintains proper state
// when breaking early and can be recreated correctly
func TestIteratorEarlyBreak(t *testing.T) {
	// Create first iterator
	seq, _ := NewIndexer(3, 1)
	iter := NewIterator(seq.Iterate())
	defer iter.Close()

	// Get first two values
	for i := 0; i < 2; i++ {
		val, ok := iter.Next()
		if !ok {
			t.Fatal("Next() returned ok=false too early")
		}
		if val != i {
			t.Errorf("value at position %d = %v, want %v", i, val, i+1)
		}
	}

	// Close early
	iter.Close()

	// Create new iterator and verify fresh start
	seq, _ = NewIndexer(3, 1)
	iter = NewIterator(seq.Iterate())
	val, ok := iter.Next()
	if !ok || val != 0 {
		t.Errorf("fresh iterator first value = %v, %v, want 1, true", val, ok)
	}
}

// TestIteratorNilSafety verifies that nil and zero-value iterators behave safely
func TestIteratorNilSafety(t *testing.T) {
	t.Run("zero value implementation", func(t *testing.T) {
		impl := &iteratorImpl[int]{} // zero value, not nil
		val, ok := impl.Next()
		if ok {
			t.Errorf("zero value implementation Next() = %v, %v, want 0, false", val, ok)
		}
		// This should not panic
		impl.Close()
	})

	t.Run("nil next function", func(t *testing.T) {
		impl := &iteratorImpl[int]{
			next: nil,
			stop: func() {},
		}
		val, ok := impl.Next()
		if ok {
			t.Errorf("iterator with nil next func returned = %v, %v, want 0, false", val, ok)
		}
	})

	t.Run("nil stop function", func(t *testing.T) {
		impl := &iteratorImpl[int]{
			next: func() (int, bool) { return 0, false },
			stop: nil,
		}
		// This should not panic
		impl.Close()
	})

	t.Run("closed iterator behavior", func(t *testing.T) {
		seq, _ := NewIndexer(3, 1)
		iter := NewIterator(seq.Iterate())
		iter.Close()

		// After closing, Next should return zero value and false
		val, ok := iter.Next()
		if ok {
			t.Errorf("closed iterator Next() = %v, %v, want 0, false", val, ok)
		}

		// Multiple closes should not panic
		iter.Close()
	})
}
