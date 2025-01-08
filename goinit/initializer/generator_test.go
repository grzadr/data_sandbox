package initializer

import (
	"reflect"
	"testing"
)

// TestPerson represents a simple struct for testing
type TestPerson struct {
	ID       int
	Name     string
	Age      int
	IsActive bool
}

// FieldGenerators encapsulates generators for each field of a struct
type PersonGenerators struct {
	ID       ValueGenerator[int]
	Name     ValueGenerator[string]
	Age      ValueGenerator[int]
	IsActive ValueGenerator[bool]
}

// StructGenerator is now type-safe and specific to Person
type PersonGenerator struct {
	PersonGenerators
}

func NewPersonGenerator(generators PersonGenerators) *PersonGenerator {
	return &PersonGenerator{
		PersonGenerators: generators,
	}
}

func (g *PersonGenerator) Generate() TestPerson {
	return TestPerson{
		ID:       g.ID.Generate(),
		Name:     g.Name.Generate(),
		Age:      g.Age.Generate(),
		IsActive: g.IsActive.Generate(),
	}
}

func (g *PersonGenerator) GenerateSlice(count int) []TestPerson {
	result := make([]TestPerson, count)
	for i := 0; i < count; i++ {
		result[i] = g.Generate()
	}
	return result
}

func defineTestGenerators() PersonGenerators {
	return PersonGenerators{
		ID: ValueGeneratorFunc[int](func() int {
			return 1
		}),
		Name: ValueGeneratorFunc[string](func() string {
			return "John Doe"
		}),
		Age: ValueGeneratorFunc[int](func() int {
			return 30
		}),
		IsActive: ValueGeneratorFunc[bool](func() bool {
			return true
		}),
	}
}

func TestStructGenerator(t *testing.T) {
	expected := TestPerson{
		ID:       1,
		Name:     "John Doe",
		Age:      30,
		IsActive: true,
	}

	count := 10
	gen := NewPersonGenerator(defineTestGenerators())
	result := gen.GenerateSlice(count)

	if len(result) != count {
		t.Errorf("Expected slice of %d, got %d", count, len(result))
	}

	for _, person := range result {
		if !reflect.DeepEqual(person, expected) {
			t.Errorf("Expected %v, got %v", person, expected)
		}
	}
}
