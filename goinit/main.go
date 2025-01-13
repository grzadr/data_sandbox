package main

import (
	"github.com/grzadr/data_sandbox/goinit/initializer"
)

func main() {
	if err := initializer.WriteCostCenterParquet(
		"../data_go/cost_centers",
		true,
		2000000,
		10000000,
	); err != nil {
		panic(err)
	}
}
