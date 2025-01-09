package initializer

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/compress"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
)

// PartitionWriter manages writing to a single partition
type PartitionWriter struct {
	writer       *pqarrow.FileWriter
	file         *os.File
	builder      *recordBuilder
	recordCount  int64
	maxBatchSize int64
}

// recordBuilder manages column builders for a single record batch
type recordBuilder struct {
	pool            *memory.GoAllocator
	schema          *arrow.Schema
	costCenter      *array.StringBuilder
	costCenterName  *array.StringBuilder
	suborganisation *array.StringBuilder
	companyName     *array.StringBuilder
	companyNumber   *array.Int64Builder
	count           int64
}

func newRecordBuilder(pool *memory.GoAllocator, schema *arrow.Schema) *recordBuilder {
	return &recordBuilder{
		pool:            pool,
		schema:          schema,
		costCenter:      array.NewStringBuilder(pool),
		costCenterName:  array.NewStringBuilder(pool),
		suborganisation: array.NewStringBuilder(pool),
		companyName:     array.NewStringBuilder(pool),
		companyNumber:   array.NewInt64Builder(pool),
		count:           0,
	}
}

func (rb *recordBuilder) append(data CostCenterData) {
	rb.costCenter.Append(data.CostCenter)
	rb.costCenterName.Append(data.CostCenterName)
	rb.suborganisation.Append(data.Suborganisation)
	rb.companyName.Append(data.CompanyName)
	rb.companyNumber.Append(data.CompanyNumber)
	rb.count++
}

func (rb *recordBuilder) build() arrow.Record {
	cols := []arrow.Array{
		rb.costCenter.NewArray(),
		rb.costCenterName.NewArray(),
		rb.suborganisation.NewArray(),
		rb.companyName.NewArray(),
		rb.companyNumber.NewArray(),
	}

	// Create record batch
	record := array.NewRecord(rb.schema, cols, rb.count)

	// Release arrays after creating record
	for _, col := range cols {
		col.Release()
	}

	// Reset builders
	rb.count = 0

	return record
}

func (rb *recordBuilder) release() {
	rb.costCenter.Release()
	rb.costCenterName.Release()
	rb.suborganisation.Release()
	rb.companyName.Release()
	rb.companyNumber.Release()
}

// StreamingParquetWriter manages multiple partition writers
type StreamingParquetWriter struct {
	pool         *memory.GoAllocator
	schema       *arrow.Schema
	writers      map[string]*PartitionWriter
	outputDir    string
	maxBatchSize int64
	writeProps   *parquet.WriterProperties
	arrowProps   *pqarrow.ArrowWriterProperties
}

func NewStreamingParquetWriter(schema *arrow.Schema, outputDir string, maxBatchSize int64) *StreamingParquetWriter {
	arrowProps := pqarrow.NewArrowWriterProperties(
		pqarrow.WithStoreSchema(),
	)

	return &StreamingParquetWriter{
		pool:         memory.NewGoAllocator(),
		schema:       schema,
		writers:      make(map[string]*PartitionWriter),
		outputDir:    outputDir,
		maxBatchSize: maxBatchSize,
		writeProps: parquet.NewWriterProperties(
			parquet.WithCompression(compress.Codecs.Zstd),
			parquet.WithDictionaryDefault(true),
		),
		arrowProps: &arrowProps,
	}
}

func (spw *StreamingParquetWriter) getOrCreateWriter(partition string) (*PartitionWriter, error) {
	if writer, exists := spw.writers[partition]; exists {
		return writer, nil
	}

	partitionDir := filepath.Join(spw.outputDir, fmt.Sprintf("suborganisation=%s", partition))
	if err := os.MkdirAll(partitionDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create partition directory: %w", err)
	}

	outputPath := filepath.Join(partitionDir, "data.parquet")
	file, err := os.Create(outputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create output file: %w", err)
	}

	writer, err := pqarrow.NewFileWriter(
		spw.schema,
		file,
		spw.writeProps,
		*spw.arrowProps,
	)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to create parquet writer: %w", err)
	}

	pw := &PartitionWriter{
		writer:       writer,
		file:         file,
		builder:      newRecordBuilder(spw.pool, spw.schema),
		maxBatchSize: spw.maxBatchSize,
	}

	spw.writers[partition] = pw
	return pw, nil
}

func (spw *StreamingParquetWriter) WriteRecord(data CostCenterData) error {
	writer, err := spw.getOrCreateWriter(data.Suborganisation)
	if err != nil {
		return err
	}

	writer.builder.append(data)
	writer.recordCount++

	if writer.recordCount >= writer.maxBatchSize {
		if err := spw.flushPartition(data.Suborganisation); err != nil {
			return err
		}
	}

	return nil
}

func (spw *StreamingParquetWriter) flushPartition(partition string) error {
	writer, exists := spw.writers[partition]
	if !exists || writer.recordCount == 0 {
		return nil
	}

	record := writer.builder.build()
	defer record.Release()

	return writer.writer.Write(record)
}

func (spw *StreamingParquetWriter) Close() error {
	var errors []error

	for partition, writer := range spw.writers {
		// Flush any remaining records
		if err := spw.flushPartition(partition); err != nil {
			errors = append(errors, fmt.Errorf("failed to flush partition %s: %w", partition, err))
		}

		// Close writer and file
		if err := writer.writer.Close(); err != nil {
			errors = append(errors, fmt.Errorf("failed to close writer for partition %s: %w", partition, err))
		}
		if err := writer.file.Close(); err != nil {
			errors = append(errors, fmt.Errorf("failed to close file for partition %s: %w", partition, err))
		}

		// Release builder resources
		writer.builder.release()
	}

	if len(errors) > 0 {
		return fmt.Errorf("multiple errors occurred during close: %v", errors)
	}

	return nil
}
