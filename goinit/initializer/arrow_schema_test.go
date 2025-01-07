package initializer

import (
	"reflect"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
)

// TestData represents a struct with various field types for testing
type TestData struct {
	ID        int32     `arrow:"id"`
	Name      string    `arrow:"name"`
	Active    bool      `arrow:"active"`
	CreatedAt time.Time `arrow:"created_at,date"`
	UpdatedAt time.Time `arrow:"updated_at"`
	Score     float64   `arrow:"score"`
	Count     int64     `arrow:"count"`
}

// InvalidTestData represents a struct with an unsupported type
type InvalidTestData struct {
	ID      int32     `arrow:"id"`
	Complex complex64 `arrow:"complex"` // Unsupported type
}

// EmptyTestData represents a struct with no arrow tags
type EmptyTestData struct {
	ID   int32
	Name string
}

func TestSchemaFromType(t *testing.T) {
	tests := []struct {
		name        string
		inputType   reflect.Type
		wantFields  int
		wantError   bool
		checkFields func(*testing.T, []arrow.Field)
	}{
		{
			name:       "Valid struct with all supported types",
			inputType:  reflect.TypeOf(TestData{}),
			wantFields: 7,
			wantError:  false,
			checkFields: func(t *testing.T, fields []arrow.Field) {
				// Test each field's type conversion
				for _, f := range fields {
					switch f.Name {
					case "id":
						if f.Type != arrow.PrimitiveTypes.Int32 {
							t.Errorf("field 'id' should be Int32, got %v", f.Type)
						}
					case "created_at":
						if f.Type != arrow.FixedWidthTypes.Date32 {
							t.Errorf("field 'created_at' should be Date32, got %v", f.Type)
						}
					case "updated_at":
						if timestampType, ok := f.Type.(*arrow.TimestampType); !ok {
							t.Errorf("field 'updated_at' should be TimestampType, got %v", f.Type)
						} else if timestampType.Unit != arrow.Microsecond {
							t.Errorf("timestamp should have Microsecond unit, got %v", timestampType.Unit)
						}
					}
				}
			},
		},
		{
			name:      "Invalid struct with unsupported type",
			inputType: reflect.TypeOf(InvalidTestData{}),
			wantError: true,
		},
		{
			name:      "Empty struct with no arrow tags",
			inputType: reflect.TypeOf(EmptyTestData{}),
			wantError: true,
		},
		{
			name:      "Nil type",
			inputType: nil,
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema, err := SchemaFromType(tt.inputType)

			// Check error cases
			if tt.wantError {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}

			// Check success cases
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if schema == nil {
				t.Error("schema is nil but no error returned")
				return
			}

			fields := schema.Fields()
			if got := len(fields); got != tt.wantFields {
				t.Errorf("got %d fields, want %d", got, tt.wantFields)
			}

			// Run field-specific checks if provided
			if tt.checkFields != nil {
				tt.checkFields(t, fields)
			}
		})
	}
}

// TestSchemaFromType_Nullable tests that all fields are properly marked as non-nullable
func TestSchemaFromType_Nullable(t *testing.T) {
	schema, err := SchemaFromType(reflect.TypeOf(TestData{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, field := range schema.Fields() {
		if field.Nullable {
			t.Errorf("field %s is nullable, expected non-nullable", field.Name)
		}
	}
}

// TestSchemaFromType_FieldNames tests that field names are correctly taken from arrow tags
func TestSchemaFromType_FieldNames(t *testing.T) {
	schema, err := SchemaFromType(reflect.TypeOf(TestData{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expectedNames := map[string]bool{
		"id":         true,
		"name":       true,
		"active":     true,
		"created_at": true,
		"updated_at": true,
		"score":      true,
		"count":      true,
	}

	for _, field := range schema.Fields() {
		if !expectedNames[field.Name] {
			t.Errorf("unexpected field name: %s", field.Name)
		}
		delete(expectedNames, field.Name)
	}

	if len(expectedNames) > 0 {
		t.Errorf("missing expected fields: %v", expectedNames)
	}
}
