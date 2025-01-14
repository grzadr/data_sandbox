package main

import (
	"flag"
	"fmt"
	"log"
	"path/filepath"

	"github.com/grzadr/data_sandbox/goinit/initializer"
	"github.com/grzadr/data_sandbox/goinit/timer"
)

type Config struct {
	BatchSize        int64
	BaseNumRecords   int64
	EmployeeMulti    int64
	WorkingTimeMulti int64
	Seed             uint64
	MainDir          string
	Overwrite        bool
}

func parseConfig() (*Config, error) {
	cfg := &Config{}

	flag.Int64Var(
		&cfg.BatchSize,
		"batch-size",
		2000000,
		"Size of the batch for processing",
	)
	flag.Int64Var(
		&cfg.BaseNumRecords,
		"base-records",
		10000000,
		"Base number of records to generate",
	)
	flag.Int64Var(
		&cfg.EmployeeMulti,
		"employee-multi",
		1000,
		"Employee multiplier",
	)
	flag.Int64Var(
		&cfg.WorkingTimeMulti,
		"workingtime-multi",
		1000,
		"Employee multiplier",
	)
	flag.Uint64Var(
		&cfg.Seed,
		"seed",
		42,
		"Random seed for data generation",
	)
	flag.StringVar(
		&cfg.MainDir,
		"dir",
		"data_go",
		"Main directory for output",
	)
	flag.BoolVar(
		&cfg.Overwrite,
		"overwrite",
		true,
		"Whether to overwrite existing files",
	)

	flag.Parse()

	if cfg.BatchSize <= 0 {
		return nil, fmt.Errorf(
			"batch size must be positive, got %d",
			cfg.BatchSize,
		)
	}
	if cfg.BaseNumRecords <= 0 {
		return nil, fmt.Errorf(
			"base number of records must be positive, got %d",
			cfg.BaseNumRecords,
		)
	}

	return cfg, nil
}

func main() {
	defer timer.NewTimer("Main").Stop()
	cfg, err := parseConfig()
	if err != nil {
		log.Fatalf("failed to parse configuration: %v", err)
	}

	if err := initializer.WriteCostCenterParquet(
		filepath.Join(cfg.MainDir, "cost_centers"),
		cfg.Overwrite,
		cfg.BatchSize,
		cfg.BaseNumRecords,
	); err != nil {
		log.Fatalf("failed to write cost centers: %v", err)
	}

	if err := initializer.WriteEmployeesParquet(
		filepath.Join(cfg.MainDir, "employees"),
		cfg.Overwrite,
		cfg.BatchSize,
		cfg.BaseNumRecords,
		cfg.EmployeeMulti,
		cfg.Seed,
	); err != nil {
		log.Fatalf("failed to write employees: %v", err)
	}

	if err := initializer.WriteWorkingTimeParquet(
		filepath.Join(cfg.MainDir, "working_time"),
		cfg.Overwrite,
		cfg.BatchSize,
		cfg.BaseNumRecords,
		cfg.EmployeeMulti*cfg.WorkingTimeMulti,
		cfg.Seed,
	); err != nil {
		log.Fatalf("failed to write working time: %v", err)
	}

}
