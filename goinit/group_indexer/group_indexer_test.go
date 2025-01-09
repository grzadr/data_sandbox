package groupindexer

import (
	"fmt"
	"reflect"
	"slices"
	"testing"
)

func TestNewIndexer(t *testing.T) {
	tests := []struct {
		name string
		n    int64
		div  int64
		want Indexer
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
			name: "negative n",
			n:    -1,
			div:  3,
			want: Indexer{size: 0, groups: 0, remainder: 0},
		},
		{
			name: "negative div",
			n:    5,
			div:  -3,
			want: Indexer{size: 0, groups: 0, remainder: 0},
		},
		{
			name: "zero n",
			n:    0,
			div:  3,
			want: Indexer{size: 0, groups: 0, remainder: 0},
		},
		{
			name: "zero div",
			n:    5,
			div:  0,
			want: Indexer{size: 0, groups: 0, remainder: 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewIndexer(tt.n, tt.div)

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
		n        int64
		div      int64
		expected []int64
	}{
		{
			name:     "zero",
			n:        0,
			div:      01,
			expected: []int64{},
		},
		{
			name:     "single element",
			n:        1,
			div:      1,
			expected: []int64{0},
		},
		{
			name:     "seven elements divided into three groups",
			n:        7,
			div:      3,
			expected: []int64{0, 0, 0, 1, 1, 1, 2},
		},
		{
			name:     "ten elements in single group",
			n:        10,
			div:      1,
			expected: []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			indexer := NewIndexer(tt.n, tt.div)

			var result []int64
			for val := range indexer.Iterate() {
				result = append(result, val)
			}

			if !slices.Equal(result, tt.expected) {
				t.Errorf("Iterate() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestIndexerIterateStr(t *testing.T) {
	tests := []struct {
		name     string
		n        int64
		div      int64
		expected []string
	}{
		{
			name:     "single element",
			n:        1,
			div:      1,
			expected: []string{"0"},
		},
		{
			name:     "seven elements divided into three groups",
			n:        7,
			div:      3,
			expected: []string{"0", "0", "0", "1", "1", "1", "2"},
		},
		{
			name:     "ten elements in single group",
			n:        10,
			div:      1,
			expected: []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			indexer := NewIndexer(tt.n, tt.div)

			var result []string
			for val := range indexer.IterateStr() {
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
		n        int64
		div      int64
		input    []string
		expected []string
	}{
		{
			name:     "zero",
			n:        0,
			div:      0,
			input:    []string{"A", "B", "C"},
			expected: []string{},
		},
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
			idx := NewIndexer(tt.n, tt.div)

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

func TestIterateWithMap(t *testing.T) {
	tests := []struct {
		name     string
		n        int64
		div      int64
		expected []string
	}{
		{
			name:     "zero",
			n:        0,
			div:      0,
			expected: []string{},
		},
		{
			name:     "single element",
			n:        1,
			div:      1,
			expected: []string{"Item 1"},
		},
		{
			name:     "multiple groups with reminder",
			n:        5,
			div:      2,
			expected: []string{"Item 1", "Item 1", "Item 2", "Item 2", "Item 3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := NewIndexer(tt.n, tt.div)

			// Create an iterator over input labels
			mapFn := func(i int64) string {
				return fmt.Sprintf("Item %d", i+1)
			}

			// Collect results from iterator
			var result []string
			for val := range IterateWithMap(idx, mapFn) {
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
		n        int64
		div      int64
		expected []int64
	}{
		{
			name:     "single element",
			n:        1,
			div:      1,
			expected: []int64{0},
		},
		{
			name:     "seven elements divided into three groups",
			n:        7,
			div:      3,
			expected: []int64{0, 0, 0, 1, 1, 1, 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iter := NewIndexerIterator(tt.n, tt.div)
			defer iter.Close()

			var got []int64
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
	iter := NewIndexerIterator(3, 1)
	defer iter.Close()

	// Get first two values
	for i := int64(0); i < 2; i++ {
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
	iter = NewIndexerIterator(3, 1)
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
		iter := NewIndexerIterator(3, 1)
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
