package initializer

import (
	"fmt"
	"github.com/apache/arrow/go/v17/arrow"
	"reflect"
	"strings"
	"time"
)



func SchemaFromType(t reflect.Type) (*arrow.Schema, error) {
	if t == nil {
		return nil, fmt.Errorf("cannot create schema from nil type")
	}

	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("can only create schema from struct type, got %v", t.Kind())
	}

	fields := make([]arrow.Field, 0, t.NumField())

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tagContent := field.Tag.Get("arrow")
		if tagContent == "" {
			continue
		}

		tagParts := strings.Split(tagContent, ",")
		arrowName := tagParts[0]
		isDate := len(tagParts) > 1 && tagParts[1] == "date"

		var dataType arrow.DataType

		if field.Type == reflect.TypeOf(time.Time{}) {
			if isDate {
				dataType = arrow.FixedWidthTypes.Date32
			} else {
				dataType = &arrow.TimestampType{
					Unit:     arrow.Microsecond,
					TimeZone: "UTC",
				}
			}
			fields = append(fields, arrow.Field{
				Name:     arrowName,
				Type:     dataType,
				Nullable: false,
			})
			continue
		}

		switch field.Type.Kind() {
		case reflect.Int32:
			dataType = arrow.PrimitiveTypes.Int32
		case reflect.Int64:
			dataType = arrow.PrimitiveTypes.Int64
		case reflect.String:
			dataType = arrow.BinaryTypes.String
		case reflect.Bool:
			dataType = arrow.FixedWidthTypes.Boolean
		case reflect.Float64:
			dataType = arrow.PrimitiveTypes.Float64
		default:
			return nil, fmt.Errorf("unsupported type for field %s: %v", field.Name, field.Type)
		}

		fields = append(fields, arrow.Field{
			Name:     arrowName,
			Type:     dataType,
			Nullable: false,
		})
	}

	if len(fields) == 0 {
		return nil, fmt.Errorf("no valid fields found in type %v", t)
	}

	return arrow.NewSchema(fields, nil), nil
}
