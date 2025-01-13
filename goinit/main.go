package main

import (
	"github.com/grzadr/data_sandbox/goinit/initializer"
)

func main() {
	batchSize := int64(2000000)
	baseNumRecords := int64(10000000)
	employeeMulti := int64(1000)
	seed := uint64(42)

	if err := initializer.WriteCostCenterParquet(
		"../data_go/cost_centers",
		true,
		batchSize,
		baseNumRecords,
	); err != nil {
		panic(err)
	}

	if err := initializer.WriteEmployeesParquet(
		"../data_go/cost_centers",
		true,
		batchSize,
		baseNumRecords,
		employeeMulti,
		seed,
	); err != nil {
		panic(err)
	}
}
