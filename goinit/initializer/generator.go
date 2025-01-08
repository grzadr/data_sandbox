package initializer

// import (
// 	"fmt"
// 	"iter"
// 	"reflect"
// )

// // ValueGenerator represents a function that generates a value of any type
// type ValueGenerator func() any

// // StructGenerator holds configuration for generating struct instances
// type StructGenerator struct {
// 	generators map[string]ValueGenerator
// }

// // NewStructGenerator creates a new StructGenerator instance with predefined generators
// func NewStructGenerator(generators map[string]ValueGenerator) *StructGenerator {
// 	if generators == nil {
// 		generators = make(map[string]ValueGenerator)
// 	}
// 	return &StructGenerator{
// 		generators: generators,
// 	}
// }

// func iterateTypeNames(t reflect.Type) iter.Seq[string] {
// 	return func(yield func(string) bool) {
// 		structType := reflect.TypeOf(t)
// 		if structType.Kind() == reflect.Ptr {
// 			structType = structType.Elem()
// 		}

// 		if structType.Kind() != reflect.Struct {
// 			return
// 		}

// 		for i := 0; i < t.NumField(); i++ {
// 			if !yield(t.Field(i).Name) {
// 				return
// 			}
// 		}
// 	}

// }

// // Generate creates a slice of struct instances of type T
// func Generate[T any](g *StructGenerator, count int) ([]T, error) {
// 	var zero T
// 	structType := reflect.TypeOf(zero)

// 	// Handle pointer types
// 	if structType.Kind() == reflect.Ptr {
// 		structType = structType.Elem()
// 	}

// 	if structType.Kind() != reflect.Struct {
// 		return nil, fmt.Errorf("type parameter T must be a struct type, got %v", structType.Kind())
// 	}

// 	if err := g.validateGenerators(structType); err != nil {
// 		return nil, err
// 	}

// 	result := make([]T, count)

// 	for i := 0; i < count; i++ {
// 		instance, err := g.generateInstance(structType)
// 		if err != nil {
// 			return nil, fmt.Errorf("failed to generate instance %d: %w", i, err)
// 		}

// 		// Convert the reflect.Value back to type T
// 		result[i] = instance.Interface().(T)
// 	}

// 	return result, nil
// }

// // validateGenerators checks if all struct fields have corresponding generators
// func (g *StructGenerator) validateGenerators(structType reflect.Type) error {
// 	for name := range iterateTypeNames(structType) {
// 		if _, exists := g.generators[name]; !exists {
// 			return fmt.Errorf("no generator registered for %v for field %s", structType, name)
// 		}
// 	}
// 	return nil
// }

// // generateInstance creates a single struct instance using registered generators
// func (g *StructGenerator) generateInstance(structType reflect.Type) (reflect.Value, error) {
// 	instance := reflect.New(structType).Elem()

// 	for i := 0; i < structType.NumField(); i++ {
// 		field := structType.Field(i)

// 		generator := g.generators[field.Name]
// 		value := generator()

// 		// Convert the generated value to the field's type
// 		val := reflect.ValueOf(value)
// 		if !val.Type().AssignableTo(field.Type) {
// 			return reflect.Value{}, fmt.Errorf(
// 				"generator for field %s produced value of type %v, which cannot be assigned to field of type %v",
// 				field.Name,
// 				val.Type(),
// 				field.Type,
// 			)
// 		}

// 		instance.Field(i).Set(val)
// 	}

// 	return instance, nil
// }

// Generator is a generic interface for generating values of type T
type ValueGenerator[T any] interface {
	Generate() T
}

// ValueGenerator represents a simple function that generates values of type T
type ValueGeneratorFunc[T any] func() T

// Generate implements the Generator interface for function types
func (f ValueGeneratorFunc[T]) Generate() T {
	return f()
}
