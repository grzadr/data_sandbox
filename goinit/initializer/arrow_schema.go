package initializer

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
)

func getNumField(t reflect.Type) (int, error) {
	if t == nil {
		return 0, fmt.Errorf("cannot create schema from nil type")
	}

	if t.Kind() != reflect.Struct {
		return 0, fmt.Errorf("can only create schema from struct type, got %v", t.Kind())
	}

	numField := t.NumField()

	if numField < 1 {
		return 0, fmt.Errorf("struct must have at least one field")
	}

	return t.NumField(), nil
}

type arrowFieldSpecs struct {
	Name   string
	Type   reflect.Type
	IsDate bool
}

func handleTypeSpecifier(found bool, typeSpecifier string) (isDate bool, err error) {
	if !found {
		return isDate, err
	}

	switch typeSpecifier {
	case "date":
		isDate = true
	default:
		err = fmt.Errorf("unsupported type specifier: %s", typeSpecifier)
	}

	return isDate, err
}

func newArrowFieldSpecs(field reflect.StructField) (arrowFieldSpecs, bool, error) {
	tagContent := field.Tag.Get("arrow")
	if tagContent == "" {
		return arrowFieldSpecs{}, false, nil
	}

	arrowName, typeSpecifier, found := strings.Cut(tagContent, ",")

	isDate, err := handleTypeSpecifier(found, typeSpecifier)

	if err != nil {
		return arrowFieldSpecs{}, false, err
	}

	return arrowFieldSpecs{
		Name:   arrowName,
		Type:   field.Type,
		IsDate: isDate,
	}, true, nil
}

func convReflectKindToArrowDataType(kind reflect.Kind) (dataType arrow.DataType, err error) {
	switch kind {
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
		err = fmt.Errorf("unsupported kind for field %v", kind)
	}

	return dataType, err
}

func (a arrowFieldSpecs) intoArrowDataType() (dataType arrow.DataType, err error) {
	if a.Type == reflect.TypeOf(time.Time{}) {
		if a.IsDate {
			dataType = arrow.FixedWidthTypes.Date32
		} else {
			dataType = &arrow.TimestampType{
				Unit:     arrow.Microsecond,
				TimeZone: "UTC",
			}
		}
	} else {
		dataType, err = convReflectKindToArrowDataType(a.Type.Kind())

		if err != nil {
			err = fmt.Errorf("Unsupported type %v for field %s", a.Type, a.Name)
		}
	}

	return dataType, err
}

func (a arrowFieldSpecs) intoArrowField() (arrow.Field, error) {
	dataType, err := a.intoArrowDataType()

	if err != nil {
		return arrow.Field{}, err
	}

	return arrow.Field{
		Name:     a.Name,
		Type:     dataType,
		Nullable: false,
	}, nil
}

func SchemaFromType(t reflect.Type) (*arrow.Schema, error) {
	numField, err := getNumField(t)

	if err != nil {
		return nil, err
	}

	fields := make([]arrow.Field, 0, numField)

	for i := 0; i < t.NumField(); i++ {
		arrowSpecs, found, err := newArrowFieldSpecs(t.Field(i))

		if err != nil {
			return nil, err
		}

		if !found {
			continue
		}

		arrowField, err := arrowSpecs.intoArrowField()

		if err != nil {
			return nil, err
		}

		fields = append(fields, arrowField)
	}

	if len(fields) == 0 {
		return nil, fmt.Errorf("no valid fields found in type %v", t)
	}

	return arrow.NewSchema(fields, nil), nil
}
