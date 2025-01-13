package initializer

type DataRow interface {
	partition() string
	getStringValue(i int) string
}
