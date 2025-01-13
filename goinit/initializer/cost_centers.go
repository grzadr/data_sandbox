package initializer

import (
	"fmt"
	"iter"
	"reflect"
	"strconv"

	indexer "github.com/grzadr/data_sandbox/goinit/group_indexer"
)

type CostCenterData struct {
	CostCenter      string `arrow:"cost_center"`
	CostCenterName  string `arrow:"cost_center_name"`
	Suborganisation string `arrow:"suborganisation"`
	CompanyName     string `arrow:"company_name"`
	CompanyNumber   int64  `arrow:"company_number"`
}

type CostCenterGenerator struct {
	CostCenter      indexer.Iterator[string]
	CostCenterName  indexer.Iterator[string]
	Suborganisation indexer.Iterator[string]
	CompanyName     indexer.Iterator[string]
	CompanyNumber   indexer.Iterator[int64]
}

func (gen *CostCenterGenerator) Close() {
	gen.CostCenter.Close()
	gen.CostCenterName.Close()
	gen.Suborganisation.Close()
	gen.CompanyName.Close()
	gen.CompanyNumber.Close()
}

func (gen *CostCenterGenerator) NewCostCenterData() (CostCenterData, bool) {
	costCenter, ok := gen.CostCenter.Next()
	if !ok {
		return CostCenterData{}, ok
	}

	costCenterName, ok := gen.CostCenterName.Next()
	if !ok {
		return CostCenterData{}, ok
	}

	suborganisation, ok := gen.Suborganisation.Next()
	if !ok {
		return CostCenterData{}, ok
	}

	companyName, ok := gen.CompanyName.Next()
	if !ok {
		return CostCenterData{}, ok
	}

	companyNumber, ok := gen.CompanyNumber.Next()
	if !ok {
		return CostCenterData{}, ok
	}

	return CostCenterData{
		CostCenter:      costCenter,
		CostCenterName:  costCenterName,
		Suborganisation: suborganisation,
		CompanyName:     companyName,
		CompanyNumber:   companyNumber,
	}, true
}

func (gen *CostCenterGenerator) Iterate(n int64) iter.Seq2[int64, CostCenterData] {
	return func(yield func(int64, CostCenterData) bool) {
		fmt.Printf("n = %d\n", n)
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

func NewCostCenterGenerator(
	numRecords, suborganisationDiv, companyDiv int64,
) *CostCenterGenerator {
	return &CostCenterGenerator{
		CostCenter: indexer.NewIndexerIteratorWithMap(
			numRecords,
			1,
			func(i int64) string { return strconv.FormatInt(i+1, 10) },
		),
		CostCenterName: indexer.NewIndexerIteratorWithMap(
			numRecords,
			1,
			func(i int64) string {
				return fmt.Sprintf("CostCenter %d", i+1)
			}),
		Suborganisation: indexer.NewIndexerIteratorWithMap(
			numRecords,
			suborganisationDiv,
			func(i int64) string {
				return fmt.Sprintf("Suborganisation %d", i+1)
			}),
		CompanyName: indexer.NewIndexerIteratorWithMap(
			numRecords,
			companyDiv,
			func(i int64) string {
				return fmt.Sprintf("CompanyName %d", i+1)
			}),
		CompanyNumber: indexer.NewIndexerIteratorWithMap(
			numRecords,
			companyDiv,
			func(i int64) int64 { return i + 1 },
		),
	}
}

func WriteCostCenterParquet(
	outputDir string,
	overwrite bool,
	batchSize int64,
	numRecords int64,
) error {
	schema, err := SchemaFromType(reflect.TypeOf(CostCenterData{}))
	if err != nil {
		return err
	}

	err = cleanupDirectory(outputDir, overwrite)

	if err != nil {
		return err
	}

	writer := NewStreamingParquetWriter(schema, outputDir, batchSize, overwrite)
	defer writer.Close()

	generator := NewCostCenterGenerator(numRecords, 10000, 100000)
	defer generator.Close()

	for _, data := range generator.Iterate(numRecords) {
		if err := writer.WriteRecord(data); err != nil {
            return err
        }
	}


	return nil
}
