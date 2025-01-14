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

func (pw *PartitionWriter) Close() error {
	if err := pw.writer.Close(); err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}
	if err := pw.file.Close(); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}
	return nil
}

// recordBuilder manages column builders for a single record batch
type recordBuilder struct {
	pool     *memory.GoAllocator
	schema   *arrow.Schema
	builders []*array.Builder
	count    int64
}

func createBuilderForField(field arrow.Field, pool memory.Allocator) (array.Builder, error) {
	switch field.Type.ID() {
	case arrow.STRING:
		return array.NewStringBuilder(pool), nil
	case arrow.INT8:
		return array.NewInt8Builder(pool), nil
	case arrow.INT16:
		return array.NewInt16Builder(pool), nil
	case arrow.INT32:
		return array.NewInt32Builder(pool), nil
	case arrow.INT64:
		return array.NewInt64Builder(pool), nil
	case arrow.UINT8:
		return array.NewUint8Builder(pool), nil
	case arrow.UINT16:
		return array.NewUint16Builder(pool), nil
	case arrow.UINT32:
		return array.NewUint32Builder(pool), nil
	case arrow.UINT64:
		return array.NewUint64Builder(pool), nil
	case arrow.FLOAT32:
		return array.NewFloat32Builder(pool), nil
	case arrow.FLOAT64:
		return array.NewFloat64Builder(pool), nil
	case arrow.BOOL:
		return array.NewBooleanBuilder(pool), nil
	case arrow.TIMESTAMP:
		return array.NewTimestampBuilder(
			pool,
			field.Type.(*arrow.TimestampType),
		), nil
	case arrow.DATE32:
		return array.NewDate32Builder(pool), nil
	case arrow.DATE64:
		return array.NewDate64Builder(pool), nil
	// Add other types as needed
	default:
		return nil, fmt.Errorf("unsupported arrow type: %s", field.Type.Name())
	}
}

func newRecordBuilder(
	pool *memory.GoAllocator,
	schema *arrow.Schema,
) (*recordBuilder, error) {

	fields := schema.Fields()
	builders := make([]*array.Builder, len(fields))

	for i, field := range fields {
		builder, err := createBuilderForField(field, pool)
		if err != nil {
			for j := 0; j < i; j++ {
				(*builders[j]).Release()
			}
			return nil, fmt.Errorf("failed to create builder for field %s: %w", field.Name, err)
		}
		builders[i] = &builder
	}

	return &recordBuilder{
		pool:     pool,
		schema:   schema,
		builders: builders,
		count:    0,
	}, nil
}

func (rb *recordBuilder) append(data DataRow) error {
	for i, b := range rb.builders {
		if err := (*b).AppendValueFromString(data.getStringValue(i)); err != nil {
			return err
		}
	}
	rb.count++
	return nil
}

func (rb *recordBuilder) build() arrow.Record {
	cols := make([]arrow.Array, len(rb.builders))

	for i, b := range rb.builders {
		cols[i] = (*b).NewArray()
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
	for _, b := range rb.builders {
		(*b).Release()
	}
}

// StreamingParquetWriter manages multiple partition writers
type StreamingParquetWriter struct {
	pool          *memory.GoAllocator
	schema        *arrow.Schema
	writers       map[string]*PartitionWriter
	outputDir     string
	maxBatchSize  int64
	writeProps    *parquet.WriterProperties
	arrowProps    *pqarrow.ArrowWriterProperties
	partitionName string
	lastPartition string
}

func cleanupDirectory(dir string, overwrite bool) error {
	if _, err := os.Stat(dir); !os.IsNotExist(err) && !overwrite {
		return fmt.Errorf(
			"Directory %s already exists and overwrite is disabled", dir,
		)
	}

	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("cleanup failed: %v", err)
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create fresh directory: %v", err)
	}

	return nil
}

func NewStreamingParquetWriter(
	schema *arrow.Schema,
	outputDir string,
	maxBatchSize int64,
	overwrite bool,
	partitionName string,
) *StreamingParquetWriter {
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
		arrowProps:    &arrowProps,
		partitionName: partitionName,
	}
}

func (spw *StreamingParquetWriter) getOrCreateWriter(
	partition string,
) (*PartitionWriter, error) {
	if writer, exists := spw.writers[partition]; exists {
		return writer, nil
	}

	if writer, exists := spw.writers[spw.lastPartition]; exists {
		spw.lastPartition = partition
		if err := writer.Close(); err != nil {
			return nil, fmt.Errorf(
				"Failed to close previous writer %s: %w",
				spw.lastPartition,
				err,
			)
		}

	} else if spw.lastPartition != "" {
		return nil, fmt.Errorf(
			"Failed to find previous writer %s",
			spw.lastPartition,
		)
	}

	partitionDir := filepath.Join(
		spw.outputDir,
		fmt.Sprintf("%s=%s", spw.partitionName, partition),
	)

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

	builder, err := newRecordBuilder(spw.pool, spw.schema)

	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to create a builder: %w", err)
	}

	pw := &PartitionWriter{
		writer:       writer,
		file:         file,
		builder:      builder,
		maxBatchSize: spw.maxBatchSize,
	}
	spw.writers[partition] = pw
	return pw, nil
}

func (spw *StreamingParquetWriter) WriteRecord(data DataRow) error {
	writer, err := spw.getOrCreateWriter(data.partition())
	if err != nil {
		return err
	}

	if err := writer.builder.append(data); err != nil {
		return err
	}
	writer.recordCount++

	if writer.recordCount >= writer.maxBatchSize {
		if err := spw.flushPartition(data.partition()); err != nil {
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
