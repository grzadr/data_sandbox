package initializer

import (
	"iter"
	"log"
	"math"
	"math/rand/v2"
	"reflect"
	"strconv"
	"strings"
	"time"

	indexer "github.com/grzadr/data_sandbox/goinit/group_indexer"
	"github.com/grzadr/data_sandbox/goinit/timer"
)

type WorkingTimeData struct {
	EmployeeId int64   `arrow:"employee_id"`
	Date       string  `arrow:"date"`
	Hours      float64 `arrow:"date"`
}

func (c *WorkingTimeData) partition() string {
	return strconv.FormatInt(c.EmployeeId, 10)
}

func (c *WorkingTimeData) getStringValue(i int) string {
	switch i {
	case 0:
		return strconv.FormatInt(c.EmployeeId, 10)
	case 1:
		return c.Date
	case 2:
		return strconv.FormatFloat(c.Hours, 'f', 2, 64)
	default:
		return ""
	}
}

type WorkingTimeGenerator struct {
	EmployeeId indexer.Iterator[int64]
	Date       indexer.Iterator[string]
	Hours      indexer.Iterator[float64]
}

func (gen *WorkingTimeGenerator) Close() {
	gen.EmployeeId.Close()
	gen.Date.Close()
	gen.Hours.Close()
}

func (gen *WorkingTimeGenerator) NewCostCenterData() (WorkingTimeData, bool) {
	EmployeeId, ok := gen.EmployeeId.Next()
	if !ok {
		return WorkingTimeData{}, ok
	}

	Date, ok := gen.Date.Next()
	if !ok {
		return WorkingTimeData{}, ok
	}

	Hours, ok := gen.Hours.Next()
	if !ok {
		return WorkingTimeData{}, ok
	}

	return WorkingTimeData{
		EmployeeId: EmployeeId,
		Date:       Date,
		Hours:      Hours,
	}, true
}

func (gen *WorkingTimeGenerator) Iterate(n int64) iter.Seq2[int64, WorkingTimeData] {
	return func(yield func(int64, WorkingTimeData) bool) {
		for i := range n {
			val, ok := gen.NewCostCenterData()
			if !ok {
				return
			}
			if !yield(i, val) {
				return
			}
		}
	}
}

func newDate(year int, month int, day int) time.Time {
	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
}

func FormatDate(year int, month int) string {
	date := newDate(year, month, 1)
	return strings.ToUpper(date.Format("Jan06"))
}

func newRandomDate(src *rand.Rand, low, high int) string {
	diff := int(math.Min(float64(high-low), 0))
	return FormatDate(low+src.IntN(diff+1), 1+src.IntN(12))
}

func NewWorkingTimeGenerator(
	numRecords, employeeDiv int64, seed uint64,
) *WorkingTimeGenerator {
	date_rand := rand.New(rand.NewPCG(seed, 1))
	hours_rand := rand.New(rand.NewPCG(seed, 2))

	return &WorkingTimeGenerator{
		EmployeeId: indexer.NewIndexerIteratorWithMap(
			numRecords,
			employeeDiv,
			func(i int64) int64 { return i + 1 },
		),
		Date: indexer.NewIndexerIteratorWithMap(
			numRecords,
			1,
			func(i int64) string {
				return newRandomDate(date_rand, 2020, 2025)
			}),
		Hours: indexer.NewIndexerIteratorWithMap(
			numRecords,
			1,
			func(i int64) float64 {
				return float64(1+hours_rand.IntN(10000)) * hours_rand.Float64()
			}),
	}
}

func WriteWorkingTimeParquet(
	outputDir string,
	overwrite bool,
	batchSize int64,
	numRecords int64,
	employeeDiv int64,
	seed uint64,
) error {
	defer timer.NewTimer("Working Time").Stop()
	numRecords = numRecords * employeeDiv
	log.Printf(
		"Working Time: Generating %d records divided in %d",
		numRecords,
		employeeDiv,
	)
	schema, err := SchemaFromType(reflect.TypeOf(WorkingTimeData{}))
	if err != nil {
		return err
	}

	err = cleanupDirectory(outputDir, overwrite)

	if err != nil {
		return err
	}

	writer := NewStreamingParquetWriter(
		schema,
		outputDir,
		batchSize,
		overwrite,
		"employee_id",
	)
	defer writer.Close()

	generator := NewWorkingTimeGenerator(
		numRecords,
		employeeDiv,
		seed,
	)
	defer generator.Close()

	for _, data := range generator.Iterate(numRecords) {
		if err := writer.WriteRecord(&data); err != nil {
			return err
		}
	}

	return nil
}
