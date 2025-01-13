package main

import (
	"path/filepath"
	"github.com/grzadr/data_sandbox/goinit/initializer"
)

func main() {
	batchSize := int64(2000000)
	baseNumRecords := int64(10000000)
	employeeMulti := int64(1000)
	seed := uint64(42)
	mainDir := "../data_go"
	overwrite := true

	if err := initializer.WriteCostCenterParquet(
		filepath.Join(mainDir, "cost_centers"),
		overwrite,
		batchSize,
		baseNumRecords,
	); err != nil {
		panic(err)
	}

	if err := initializer.WriteEmployeesParquet(
		filepath.Join(mainDir, "employees"),
		overwrite,
		batchSize,
		baseNumRecords,
		employeeMulti,
		seed,
	); err != nil {
		panic(err)
	}
}
