package timer

import (
	"log"
	"time"
)

// FuncTimer provides utilities for function execution timing
type FuncTimer struct {
	startTime time.Time
	label     string
}

// NewTimer creates a new function timer
func NewTimer(label string) *FuncTimer {
	log.Printf("%-20s starting", label)

	return &FuncTimer{
		startTime: time.Now(),
		label:     label,
	}
}

// Stop ends timing and prints the execution duration
func (ft *FuncTimer) Stop() {
	duration := time.Since(ft.startTime)
	log.Printf(
		"%-20s completed: %12s\n",
		ft.label,
		duration.Round(time.Microsecond),
	)
}
