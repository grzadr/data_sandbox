package initializer

import (
	"fmt"
	"github.com/grzadr/data_sandbox/goinit/timer"
	"iter"
	"math/rand/v2"
	"reflect"
	"strconv"

	indexer "github.com/grzadr/data_sandbox/goinit/group_indexer"
)

type EmployeesData struct {
	EmployeeId   int64  `arrow:"employee_id"`
	EmployeeName string `arrow:"employee_name"`
	CostCenter   int64  `arrow:"cost_center"`
	IsEmployed   int64  `arrow:"company_name"`
	isActive     int64  `arrow:"company_number"`
}

func (c *EmployeesData) partition() string {
	return strconv.FormatInt(c.CostCenter, 10)
}

func (c *EmployeesData) getStringValue(i int) string {
	switch i {
	case 0:
		return strconv.FormatInt(c.EmployeeId, 10)
	case 1:
		return c.EmployeeName
	case 2:
		return strconv.FormatInt(c.CostCenter, 10)
	case 3:
		return strconv.FormatInt(c.IsEmployed, 10)
	case 4:
		return strconv.FormatInt(c.isActive, 10)
	default:
		return ""
	}
}

type EmployeesGenerator struct {
	EmployeeId   indexer.Iterator[int64]
	EmployeeName indexer.Iterator[string]
	CostCenter   indexer.Iterator[int64]
	IsEmployed   indexer.Iterator[int64]
	isActive     indexer.Iterator[int64]
}

func (gen *EmployeesGenerator) Close() {
	gen.EmployeeId.Close()
	gen.EmployeeName.Close()
	gen.CostCenter.Close()
	gen.IsEmployed.Close()
	gen.isActive.Close()
}

func (gen *EmployeesGenerator) NewCostCenterData() (EmployeesData, bool) {
	EmployeeId, ok := gen.EmployeeId.Next()
	if !ok {
		return EmployeesData{}, ok
	}

	EmployeeName, ok := gen.EmployeeName.Next()
	if !ok {
		return EmployeesData{}, ok
	}

	CostCenter, ok := gen.CostCenter.Next()
	if !ok {
		return EmployeesData{}, ok
	}

	IsEmployed, ok := gen.IsEmployed.Next()
	if !ok {
		return EmployeesData{}, ok
	}

	isActive, ok := gen.isActive.Next()
	if !ok {
		return EmployeesData{}, ok
	}

	return EmployeesData{
		EmployeeId:   EmployeeId,
		EmployeeName: EmployeeName,
		CostCenter:   CostCenter,
		IsEmployed:   IsEmployed,
		isActive:     isActive,
	}, true
}

func (gen *EmployeesGenerator) Iterate(n int64) iter.Seq2[int64, EmployeesData] {
	return func(yield func(int64, EmployeesData) bool) {
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

func NewEmployeesGenerator(
	numRecords, costCenterDiv int64, seed uint64,
) *EmployeesGenerator {
	employed_rand := rand.New(rand.NewPCG(seed, 1))
	active_rand := rand.New(rand.NewPCG(seed, 2))
	return &EmployeesGenerator{
		EmployeeId: indexer.NewIndexerIteratorWithMap(
			numRecords,
			1,
			func(i int64) int64 { return i + 1 },
		),
		EmployeeName: indexer.NewIndexerIteratorWithMap(
			numRecords,
			1,
			func(i int64) string {
				return fmt.Sprintf("Employee %d", i+1)
			}),
		CostCenter: indexer.NewIndexerIteratorWithMap(
			numRecords,
			costCenterDiv,
			func(i int64) int64 { return i + 1 }),
		IsEmployed: indexer.NewIndexerIteratorWithMap(
			numRecords,
			1,
			func(i int64) int64 { return employed_rand.Int64N(2) },
		),
		isActive: indexer.NewIndexerIteratorWithMap(
			numRecords,
			1,
			func(i int64) int64 { return active_rand.Int64N(2) },
		),
	}
}

func WriteEmployeesParquet(
	outputDir string,
	overwrite bool,
	batchSize int64,
	numRecords int64,
	costCenterDiv int64,
	seed uint64,
) error {
	defer timer.NewTimer("Employees").Stop()
	schema, err := SchemaFromType(reflect.TypeOf(EmployeesData{}))
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
		"cost_center",
	)
	defer writer.Close()

	generator := NewEmployeesGenerator(
		numRecords*costCenterDiv,
		costCenterDiv,
		seed)
	defer generator.Close()

	for _, data := range generator.Iterate(numRecords) {
		if err := writer.WriteRecord(&data); err != nil {
			return err
		}
	}

	return nil
}
