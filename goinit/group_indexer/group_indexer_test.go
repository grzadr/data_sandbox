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
