package metrics

import (
	"log"
	"sync/atomic"
	"time"
)

// Metrics holds various counters and gauges for monitoring the pipeline
type Metrics struct {
	// Generation metrics
	RecordsGenerated int64
	GenerationErrors int64

	// Consumption metrics
	RecordsConsumed   int64
	ConsumptionErrors int64

	// Sorting metrics
	RecordsSorted int64
	SortingErrors int64

	// Production metrics
	RecordsProduced  int64
	ProductionErrors int64

	// Timing metrics
	GenerationDuration  time.Duration
	ConsumptionDuration time.Duration
	SortingDuration     time.Duration
	ProductionDuration  time.Duration
}

// NewMetrics creates a new metrics collector
func NewMetrics() *Metrics {
	return &Metrics{}
}

// IncrementRecordsGenerated increments the generated records counter
func (m *Metrics) IncrementRecordsGenerated(count int) {
	atomic.AddInt64(&m.RecordsGenerated, int64(count))
}

// IncrementGenerationErrors increments the generation errors counter
func (m *Metrics) IncrementGenerationErrors() {
	atomic.AddInt64(&m.GenerationErrors, 1)
}

// IncrementRecordsConsumed increments the consumed records counter
func (m *Metrics) IncrementRecordsConsumed() {
	atomic.AddInt64(&m.RecordsConsumed, 1)
}

// IncrementConsumptionErrors increments the consumption errors counter
func (m *Metrics) IncrementConsumptionErrors() {
	atomic.AddInt64(&m.ConsumptionErrors, 1)
}

// IncrementRecordsSorted increments the sorted records counter
func (m *Metrics) IncrementRecordsSorted(count int) {
	atomic.AddInt64(&m.RecordsSorted, int64(count))
}

// IncrementSortingErrors increments the sorting errors counter
func (m *Metrics) IncrementSortingErrors() {
	atomic.AddInt64(&m.SortingErrors, 1)
}

// IncrementRecordsProduced increments the produced records counter
func (m *Metrics) IncrementRecordsProduced(count int) {
	atomic.AddInt64(&m.RecordsProduced, int64(count))
}

// IncrementProductionErrors increments the production errors counter
func (m *Metrics) IncrementProductionErrors() {
	atomic.AddInt64(&m.ProductionErrors, 1)
}

// SetGenerationDuration sets the generation duration
func (m *Metrics) SetGenerationDuration(d time.Duration) {
	m.GenerationDuration = d
}

// SetConsumptionDuration sets the consumption duration
func (m *Metrics) SetConsumptionDuration(d time.Duration) {
	m.ConsumptionDuration = d
}

// SetSortingDuration sets the sorting duration
func (m *Metrics) SetSortingDuration(d time.Duration) {
	m.SortingDuration = d
}

// SetProductionDuration sets the production duration
func (m *Metrics) SetProductionDuration(d time.Duration) {
	m.ProductionDuration = d
}

// Print prints the current metrics to the log
func (m *Metrics) Print() {
	log.Println("===================================================")
	log.Println("Pipeline Metrics Summary")
	log.Println("===================================================")
	log.Printf("Records Generated     : %d", atomic.LoadInt64(&m.RecordsGenerated))
	log.Printf("Generation Errors     : %d", atomic.LoadInt64(&m.GenerationErrors))
	log.Printf("Records Consumed      : %d", atomic.LoadInt64(&m.RecordsConsumed))
	log.Printf("Consumption Errors    : %d", atomic.LoadInt64(&m.ConsumptionErrors))
	log.Printf("Records Sorted        : %d", atomic.LoadInt64(&m.RecordsSorted))
	log.Printf("Sorting Errors        : %d", atomic.LoadInt64(&m.SortingErrors))
	log.Printf("Records Produced      : %d", atomic.LoadInt64(&m.RecordsProduced))
	log.Printf("Production Errors     : %d", atomic.LoadInt64(&m.ProductionErrors))
	log.Println("---------------------------------------------------")
	log.Printf("Generation Duration   : %s", m.GenerationDuration)
	log.Printf("Consumption Duration  : %s", m.ConsumptionDuration)
	log.Printf("Sorting Duration      : %s", m.SortingDuration)
	log.Printf("Production Duration   : %s", m.ProductionDuration)
	log.Println("===================================================")
}
