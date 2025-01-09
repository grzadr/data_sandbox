package initializer

import (
	"fmt"
	"iter"

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
	costCenter, done := gen.CostCenter.Next()
	if done {
		fmt.Println("CostCenter is done")
		return CostCenterData{}, true // true means done
	}

	costCenterName, done := gen.CostCenterName.Next()
	if done {
		fmt.Println("CostCenterName is done")
		return CostCenterData{}, true
	}

	suborganisation, done := gen.Suborganisation.Next()
	if done {
		fmt.Println("Suborganisation is done")
		return CostCenterData{}, true
	}

	companyName, done := gen.CompanyName.Next()
	if done {
		fmt.Println("CompanyName is done")
		return CostCenterData{}, true
	}

	companyNumber, done := gen.CompanyNumber.Next()
	if done {
		fmt.Println("CompanyNumber is done")
		return CostCenterData{}, true
	}

	return CostCenterData{
		CostCenter:      costCenter,
		CostCenterName:  costCenterName,
		Suborganisation: suborganisation,
		CompanyName:     companyName,
		CompanyNumber:   companyNumber,
	}, false // false means not done
}

func (gen *CostCenterGenerator) Iterate(n int64) iter.Seq2[int64, CostCenterData] {
	return func(yield func(int64, CostCenterData) bool) {
		fmt.Printf("n = %d\n", n)
		for i := range n {
			fmt.Printf("Iteration %d: ", i)
			val, done := gen.NewCostCenterData()
			fmt.Printf("Value %v, status %t\n", val, done)
			if !done {
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
		CostCenter: indexer.NewIndexerIteratorStr(num_records, 1),
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
		CompanyNumber: indexer.NewIndexerIterator(
			num_records,
			company_div,
		),
	}
}
