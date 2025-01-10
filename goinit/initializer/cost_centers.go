package initializer

import (
	"fmt"
	"iter"
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
	num_records, suborganisation_div, company_div int64,
) *CostCenterGenerator {
	return &CostCenterGenerator{
		CostCenter: indexer.NewIndexerIteratorWithMap(
			num_records,
			1,
			func(i int64) string { return strconv.FormatInt(i+1, 10) },
		),
		CostCenterName: indexer.NewIndexerIteratorWithMap(
			num_records,
			1,
			func(i int64) string {
				return fmt.Sprintf("CostCenter %d", i+1)
			}),
		Suborganisation: indexer.NewIndexerIteratorWithMap(
			num_records,
			suborganisation_div,
			func(i int64) string {
				return fmt.Sprintf("Suborganisation %d", i+1)
			}),
		CompanyName: indexer.NewIndexerIteratorWithMap(
			num_records,
			company_div,
			func(i int64) string {
				return fmt.Sprintf("CompanyName %d", i+1)
			}),
		CompanyNumber: indexer.NewIndexerIteratorWithMap(
			num_records,
			company_div,
			func(i int64) int64 { return i + 1 },
		),
	}
}

