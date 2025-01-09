package initializer

import (
	"reflect"
	"testing"
)

func TestNewCostCenterGenerators(t *testing.T) {
	// Expected data when called with num_records=10, suborganisation_div=2, company_div=5
	expected := []CostCenterData{
		{
			CostCenter:      "1",
			CostCenterName:  "CostCenter 1",
			Suborganisation: "Suborganisation 1",
			CompanyName:     "CompanyName 1",
			CompanyNumber:   0,
		},
		{
			CostCenter:      "2",
			CostCenterName:  "CostCenter 2",
			Suborganisation: "Suborganisation 1",
			CompanyName:     "CompanyName 1",
			CompanyNumber:   0,
		},
		{
			CostCenter:      "3",
			CostCenterName:  "CostCenter 3",
			Suborganisation: "Suborganisation 2",
			CompanyName:     "CompanyName 1",
			CompanyNumber:   0,
		},
		{
			CostCenter:      "4",
			CostCenterName:  "CostCenter 4",
			Suborganisation: "Suborganisation 2",
			CompanyName:     "CompanyName 1",
			CompanyNumber:   0,
		},
		{
			CostCenter:      "5",
			CostCenterName:  "CostCenter 5",
			Suborganisation: "Suborganisation 3",
			CompanyName:     "CompanyName 2",
			CompanyNumber:   1,
		},
		{
			CostCenter:      "6",
			CostCenterName:  "CostCenter 6",
			Suborganisation: "Suborganisation 3",
			CompanyName:     "CompanyName 2",
			CompanyNumber:   1,
		},
		{
			CostCenter:      "7",
			CostCenterName:  "CostCenter 7",
			Suborganisation: "Suborganisation 4",
			CompanyName:     "CompanyName 2",
			CompanyNumber:   1,
		},
		{
			CostCenter:      "8",
			CostCenterName:  "CostCenter 8",
			Suborganisation: "Suborganisation 4",
			CompanyName:     "CompanyName 2",
			CompanyNumber:   1,
		},
		{
			CostCenter:      "9",
			CostCenterName:  "CostCenter 9",
			Suborganisation: "Suborganisation 5",
			CompanyName:     "CompanyName 3",
			CompanyNumber:   2,
		},
		{
			CostCenter:      "10",
			CostCenterName:  "CostCenter 10",
			Suborganisation: "Suborganisation 5",
			CompanyName:     "CompanyName 3",
			CompanyNumber:   2,
		},
	}

	// Create generator
	generator := NewCostCenterGenerator(10, 2, 5)
	defer generator.Close()

	// Collect generated data
	var actual []CostCenterData
	for i, data := range generator.Iterate(10) {
		actual = append(actual, data)
		// Additional check for correct index
		if i >= 10 {
			t.Errorf("Iterator produced more than 10 elements")
			return
		}
	}

	// Compare lengths
	if len(actual) != len(expected) {
		t.Errorf("Generated data length mismatch: got %d, want %d", len(actual), len(expected))
		return
	}

	// Compare each element
	for i := range expected {
		if !reflect.DeepEqual(actual[i], expected[i]) {
			t.Errorf("Mismatch at index %d:\ngot: %+v\nwant: %+v", i, actual[i], expected[i])
		}
	}
}
