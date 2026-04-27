// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package parquetfile

import (
	"database/sql"
	"fmt"
	"io"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/docker/go-units"
)

// DefaultCompressionType is the default Parquet compression codec.
var DefaultCompressionType = compress.Codecs.Zstd

const (
	defaultRowGroupMemoryLimitBytes   = 120 * units.MiB
	definitionLevelMemoryBytes        = int64(2)
	byteArraySliceHeaderBytes         = int64(unsafe.Sizeof(parquet.ByteArray(nil)))
	fixedLenByteArraySliceHeaderBytes = int64(unsafe.Sizeof(parquet.FixedLenByteArray(nil)))

	defaultTimestampPrecision = 6
	millisecondPrecision      = 3
	maxTimestampPrecision     = 6
)

// ColumnInfo describes a SQL result column to be written into Parquet.
type ColumnInfo struct {
	Name      string
	Type      string
	Nullable  bool
	Precision int64
	Scale     int64
}

// ColumnType describes the physical and logical Parquet type for a SQL column.
type ColumnType struct {
	Physical   parquet.Type
	Logical    schema.LogicalType
	TypeLength int
	Precision  int
	Scale      int
}

type column struct {
	ColumnInfo
	ColumnType
	Repetition parquet.Repetition
	Optional   bool
}

type columnBuffer struct {
	defLevels               []int16
	boolValues              []bool
	int32Values             []int32
	int64Values             []int64
	float32Values           []float32
	float64Values           []float64
	byteArrayValues         []parquet.ByteArray
	fixedLenByteArrayValues []parquet.FixedLenByteArray
}

type parsedColumnValue struct {
	value  any
	isNull bool
}

type countingWriter struct {
	writer       io.Writer
	writtenBytes int64
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.writer.Write(p)
	cw.writtenBytes += int64(n)
	return n, err
}

func (cw *countingWriter) Close() error {
	if closer, ok := cw.writer.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// ParquetWriter writes SQL rows into a Parquet file using parquet/file.Writer.
type ParquetWriter struct {
	writer                   *file.Writer
	output                   *countingWriter
	columns                  []column
	buffers                  []columnBuffer
	rowGroupMemoryLimitBytes int64
	bufferedRows             int
	bufferedMemoryBytes      int64
	closed                   bool
}

type writerOptions struct {
	writerProperties         []parquet.WriterProperty
	rowGroupMemoryLimitBytes int64
}

// WriterOption configures parquet writer properties exposed by this package.
type WriterOption func(writerOptions) writerOptions

func defaultWriterOptions() writerOptions {
	return writerOptions{
		writerProperties: []parquet.WriterProperty{
			parquet.WithCompression(DefaultCompressionType),
		},
		rowGroupMemoryLimitBytes: defaultRowGroupMemoryLimitBytes,
	}
}

// WithCompression sets parquet writer compression codec.
func WithCompression(codec compress.Compression) WriterOption {
	return func(options writerOptions) writerOptions {
		options.writerProperties = append(options.writerProperties, parquet.WithCompression(codec))
		return options
	}
}

// WithDataPageSize sets parquet writer data page size in bytes.
func WithDataPageSize(pageSize int64) WriterOption {
	return func(options writerOptions) writerOptions {
		options.writerProperties = append(options.writerProperties, parquet.WithDataPageSize(pageSize))
		return options
	}
}

// WithRowGroupMemoryLimit sets the row-group flush threshold by accounted
// in-memory bytes.
func WithRowGroupMemoryLimit(limitBytes int64) WriterOption {
	return func(options writerOptions) writerOptions {
		if limitBytes > 0 {
			options.rowGroupMemoryLimitBytes = limitBytes
		}
		return options
	}
}

// NewParquetWriter creates a Parquet writer for SQL result rows.
func NewParquetWriter(w io.Writer, columns []*ColumnInfo, options ...WriterOption) (*ParquetWriter, error) {
	if w == nil {
		return nil, fmt.Errorf("parquet output buffer is nil")
	}
	output := &countingWriter{writer: w}

	parquetSchema, parsedColumns, err := newSchemaFromSQL(columns)
	if err != nil {
		return nil, err
	}

	localOptions := newWriterOptions(options)
	props := parquet.NewWriterProperties(localOptions.writerProperties...)

	buffers, err := newColumnBuffers(parsedColumns, 0)
	if err != nil {
		return nil, err
	}

	return &ParquetWriter{
		writer:                   file.NewParquetWriter(output, parquetSchema, file.WithWriterProps(props)),
		output:                   output,
		columns:                  parsedColumns,
		buffers:                  buffers,
		rowGroupMemoryLimitBytes: localOptions.rowGroupMemoryLimitBytes,
	}, nil
}

func newWriterOptions(options []WriterOption) writerOptions {
	localOptions := defaultWriterOptions()
	for _, option := range options {
		if option != nil {
			localOptions = option(localOptions)
		}
	}
	return localOptions
}

func newSchemaFromSQL(columns []*ColumnInfo) (*schema.GroupNode, []column, error) {
	fields := make([]schema.Node, 0, len(columns))
	parsedColumns := make([]column, 0, len(columns))
	for _, columnInfo := range columns {
		if columnInfo == nil {
			return nil, nil, fmt.Errorf("parquet column info is nil")
		}
		if columnInfo.Name == "" {
			return nil, nil, fmt.Errorf("parquet column name is empty")
		}

		repetition := parquet.Repetitions.Required
		optional := false
		// TIMESTAMP and DATETIME can carry invalid MySQL values. Preserve the
		// previous behavior by writing those invalid values as NULL.
		if columnInfo.Nullable || columnInfo.Type == "TIMESTAMP" || columnInfo.Type == "DATETIME" {
			repetition = parquet.Repetitions.Optional
			optional = true
		}

		columnType := ToColumnType(columnInfo)
		parsedColumn := column{
			ColumnInfo: *columnInfo,
			ColumnType: columnType,
			Repetition: repetition,
			Optional:   optional,
		}
		field, err := newPrimitiveNode(parsedColumn)
		if err != nil {
			return nil, nil, fmt.Errorf("build parquet schema for column %s: %w", columnInfo.Name, err)
		}
		fields = append(fields, field)
		parsedColumns = append(parsedColumns, parsedColumn)
	}

	root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, fields, -1)
	if err != nil {
		return nil, nil, err
	}
	return root, parsedColumns, nil
}

func newPrimitiveNode(column column) (schema.Node, error) {
	if column.Logical != nil && !column.Logical.IsNone() {
		return schema.NewPrimitiveNodeLogical(
			column.Name,
			column.Repetition,
			column.Logical,
			column.Physical,
			column.TypeLength,
			-1,
		)
	}
	return schema.NewPrimitiveNode(
		column.Name,
		column.Repetition,
		column.Physical,
		-1,
		int32(column.TypeLength),
	)
}

// Write appends one row from SQL raw column bytes.
func (pw *ParquetWriter) Write(src []sql.RawBytes) error {
	if pw.closed {
		return fmt.Errorf("parquet writer is closed")
	}
	pw.writer.FileMetadata()
	if err := pw.normalizeRow(src); err != nil {
		return err
	}
	if pw.rowGroupMemoryLimitBytes > 0 && pw.bufferedMemoryBytes >= pw.rowGroupMemoryLimitBytes {
		return pw.flushRows()
	}
	return nil
}

// Close flushes buffered rows and closes the Parquet writer.
func (pw *ParquetWriter) Close() error {
	if pw.closed {
		return nil
	}
	if err := pw.flushRows(); err != nil {
		return err
	}
	pw.closed = true
	return pw.writer.Close()
}

// EstimateFileSize returns an estimated final file size by summing bytes
// already flushed to the sink and bytes still buffered in memory.
func (pw *ParquetWriter) EstimateFileSize() uint64 {
	estimatedBytes := pw.totalWrittenBytes() + pw.bufferedMemoryBytes
	if estimatedBytes <= 0 {
		return 0
	}
	return uint64(estimatedBytes)
}

func (pw *ParquetWriter) totalWrittenBytes() int64 {
	if pw.output == nil {
		return 0
	}
	return pw.output.writtenBytes
}

func (pw *ParquetWriter) normalizeRow(rawRow []sql.RawBytes) error {
	if len(rawRow) != len(pw.columns) {
		return fmt.Errorf("parquet row has %d values, expected %d", len(rawRow), len(pw.columns))
	}

	parsedValues := make([]parsedColumnValue, len(rawRow))
	for i, rawValue := range rawRow {
		parsedValue, err := pw.parseColumnValue(i, rawValue)
		if err != nil {
			return fmt.Errorf("convert parquet column %s: %w", pw.columns[i].Name, err)
		}
		parsedValues[i] = parsedValue
	}

	for i, parsedValue := range parsedValues {
		if err := pw.appendParsedColumnValue(i, parsedValue); err != nil {
			return fmt.Errorf("convert parquet column %s: %w", pw.columns[i].Name, err)
		}
	}

	pw.bufferedRows++
	return nil
}

func (pw *ParquetWriter) flushRows() error {
	if pw.bufferedRows == 0 {
		return nil
	}

	rowGroupWriter := pw.writer.AppendRowGroup()
	for i := range pw.columns {
		columnWriter, err := rowGroupWriter.NextColumn()
		if err != nil {
			return err
		}
		if err := writeColumnBatch(columnWriter, pw.columns[i], pw.buffers[i]); err != nil {
			_ = columnWriter.Close()
			return fmt.Errorf("write parquet column %s: %w", pw.columns[i].Name, err)
		}
		if err := columnWriter.Close(); err != nil {
			return fmt.Errorf("close parquet column %s: %w", pw.columns[i].Name, err)
		}
	}
	if err := rowGroupWriter.Close(); err != nil {
		return err
	}
	pw.bufferedRows = 0
	pw.bufferedMemoryBytes = 0
	pw.resetBuffers()
	return nil
}

func newColumnBuffers(columns []column, capacity int) ([]columnBuffer, error) {
	buffers := make([]columnBuffer, len(columns))
	for i := range columns {
		buffer, err := newColumnBuffer(columns[i], capacity)
		if err != nil {
			return nil, fmt.Errorf("init parquet buffer for column %s: %w", columns[i].Name, err)
		}
		buffers[i] = buffer
	}
	return buffers, nil
}

func newColumnBuffer(column column, capacity int) (columnBuffer, error) {
	buffer := columnBuffer{}
	if column.Optional {
		buffer.defLevels = make([]int16, 0, capacity)
	}

	switch column.Physical {
	case parquet.Types.Boolean:
		buffer.boolValues = make([]bool, 0, capacity)
	case parquet.Types.Int32:
		buffer.int32Values = make([]int32, 0, capacity)
	case parquet.Types.Int64:
		buffer.int64Values = make([]int64, 0, capacity)
	case parquet.Types.Float:
		buffer.float32Values = make([]float32, 0, capacity)
	case parquet.Types.Double:
		buffer.float64Values = make([]float64, 0, capacity)
	case parquet.Types.ByteArray:
		buffer.byteArrayValues = make([]parquet.ByteArray, 0, capacity)
	case parquet.Types.FixedLenByteArray:
		if column.TypeLength <= 0 {
			return columnBuffer{}, fmt.Errorf("invalid fixed-size byte width %d", column.TypeLength)
		}
		buffer.fixedLenByteArrayValues = make([]parquet.FixedLenByteArray, 0, capacity)
	default:
		return columnBuffer{}, fmt.Errorf("unsupported parquet physical type %s", column.Physical)
	}
	return buffer, nil
}

func (pw *ParquetWriter) parseColumnValue(colIdx int, rawValue sql.RawBytes) (parsedColumnValue, error) {
	column := pw.columns[colIdx]
	if rawValue == nil {
		if !column.Optional {
			return parsedColumnValue{}, fmt.Errorf("required column receives NULL")
		}
		return parsedColumnValue{isNull: true}, nil
	}

	parsedValue, isNull, err := parseRawColumnValue(rawValue, column)
	if err != nil {
		return parsedColumnValue{}, err
	}
	return parsedColumnValue{value: parsedValue, isNull: isNull}, nil
}

func (pw *ParquetWriter) appendParsedColumnValue(colIdx int, parsedValue parsedColumnValue) error {
	column := pw.columns[colIdx]
	buffer := &pw.buffers[colIdx]

	if column.Optional {
		pw.bufferedMemoryBytes += definitionLevelMemoryBytes
		if parsedValue.isNull {
			buffer.defLevels = append(buffer.defLevels, 0)
			return nil
		}
		buffer.defLevels = append(buffer.defLevels, 1)
	}

	if parsedValue.isNull {
		return nil
	}
	if err := appendColumnValue(buffer, column, parsedValue.value); err != nil {
		return err
	}
	pw.bufferedMemoryBytes += accountColumnValueMemoryBytes(column, parsedValue.value)
	return nil
}

func (pw *ParquetWriter) resetBuffers() {
	for i := range pw.buffers {
		pw.buffers[i].defLevels = pw.buffers[i].defLevels[:0]
		pw.buffers[i].boolValues = pw.buffers[i].boolValues[:0]
		pw.buffers[i].int32Values = pw.buffers[i].int32Values[:0]
		pw.buffers[i].int64Values = pw.buffers[i].int64Values[:0]
		pw.buffers[i].float32Values = pw.buffers[i].float32Values[:0]
		pw.buffers[i].float64Values = pw.buffers[i].float64Values[:0]
		pw.buffers[i].byteArrayValues = pw.buffers[i].byteArrayValues[:0]
		pw.buffers[i].fixedLenByteArrayValues = pw.buffers[i].fixedLenByteArrayValues[:0]
	}
}

func parseRawColumnValue(rawValue sql.RawBytes, column column) (any, bool, error) {
	s := string(rawValue)
	switch column.Physical {
	case parquet.Types.Boolean:
		v, err := strconv.ParseBool(s)
		if err != nil {
			return nil, false, err
		}
		return v, false, nil
	case parquet.Types.Int32:
		if _, ok := column.Logical.(schema.DecimalLogicalType); ok {
			scaled, err := parseDecimalToScaledInteger(s, column.ColumnType.Scale)
			if err != nil {
				return nil, false, err
			}
			if !scaled.IsInt64() {
				return nil, false, fmt.Errorf("decimal value %q does not fit in INT32", s)
			}
			value := scaled.Int64()
			if value < math.MinInt32 || value > math.MaxInt32 {
				return nil, false, fmt.Errorf("decimal value %q does not fit in INT32", s)
			}
			return int32(value), false, nil
		}
		v, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, false, err
		}
		return int32(v), false, nil
	case parquet.Types.Int64:
		if _, ok := column.Logical.(schema.TimestampLogicalType); ok {
			// For mysql text protocol, temporal values are emitted as
			// "YYYY-MM-DD HH:MM:SS[.fraction]"; time.Parse(time.DateTime, s)
			// accepts optional fractional digits.
			// Since there is no timezone info in temporal values, Go parses them
			// as UTC. This is counterintuitive but matches our isAdjustedToUTC=false
			// setting in ToColumnType (local-semantics "as-if UTC" encoding).
			t, err := time.Parse(time.DateTime, s)
			if err != nil {
				if column.Optional {
					return nil, true, nil
				}
				return nil, false, err
			}
			return t.UnixMicro(), false, nil
		}
		if _, ok := column.Logical.(schema.DecimalLogicalType); ok {
			scaled, err := parseDecimalToScaledInteger(s, column.ColumnType.Scale)
			if err != nil {
				return nil, false, err
			}
			if !scaled.IsInt64() {
				return nil, false, fmt.Errorf("decimal value %q does not fit in INT64", s)
			}
			return scaled.Int64(), false, nil
		}
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, false, err
		}
		return v, false, nil
	case parquet.Types.Float:
		// Kept for completeness when handling external/custom parquet schema:
		// current ToColumnType maps SQL FLOAT to parquet.Types.Double.
		v, err := strconv.ParseFloat(s, 32)
		if err != nil {
			return nil, false, err
		}
		return float32(v), false, nil
	case parquet.Types.Double:
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, false, err
		}
		return v, false, nil
	case parquet.Types.ByteArray:
		cloned := make([]byte, len(rawValue))
		copy(cloned, rawValue)
		return parquet.ByteArray(cloned), false, nil
	case parquet.Types.FixedLenByteArray:
		if _, ok := column.Logical.(schema.DecimalLogicalType); ok {
			scaled, err := parseDecimalToScaledInteger(s, column.ColumnType.Scale)
			if err != nil {
				return nil, false, err
			}
			encoded, err := toFixedLenTwoComplement(scaled, column.TypeLength)
			if err != nil {
				return nil, false, err
			}
			return parquet.FixedLenByteArray(encoded), false, nil
		}
		if column.TypeLength <= 0 {
			return nil, false, fmt.Errorf("invalid fixed-size byte width %d", column.TypeLength)
		}
		cloned := make([]byte, len(rawValue))
		copy(cloned, rawValue)
		if len(cloned) != column.TypeLength {
			return nil, false, fmt.Errorf("fixed-len byte array width mismatch: got %d, expected %d", len(cloned), column.TypeLength)
		}
		return parquet.FixedLenByteArray(cloned), false, nil
	default:
		return nil, false, fmt.Errorf("unsupported parquet physical type %s", column.Physical)
	}
}

func appendColumnValue(buffer *columnBuffer, column column, value any) error {
	switch column.Physical {
	case parquet.Types.Boolean:
		buffer.boolValues = append(buffer.boolValues, value.(bool))
	case parquet.Types.Int32:
		buffer.int32Values = append(buffer.int32Values, value.(int32))
	case parquet.Types.Int64:
		buffer.int64Values = append(buffer.int64Values, value.(int64))
	case parquet.Types.Float:
		buffer.float32Values = append(buffer.float32Values, value.(float32))
	case parquet.Types.Double:
		buffer.float64Values = append(buffer.float64Values, value.(float64))
	case parquet.Types.ByteArray:
		buffer.byteArrayValues = append(buffer.byteArrayValues, value.(parquet.ByteArray))
	case parquet.Types.FixedLenByteArray:
		buffer.fixedLenByteArrayValues = append(buffer.fixedLenByteArrayValues, value.(parquet.FixedLenByteArray))
	default:
		return fmt.Errorf("unsupported parquet physical type %s", column.Physical)
	}
	return nil
}

func accountColumnValueMemoryBytes(column column, value any) int64 {
	switch column.Physical {
	case parquet.Types.Boolean:
		return 1
	case parquet.Types.Int32, parquet.Types.Float:
		return 4
	case parquet.Types.Int64, parquet.Types.Double:
		return 8
	case parquet.Types.ByteArray:
		return byteArraySliceHeaderBytes + int64(len(value.(parquet.ByteArray)))
	case parquet.Types.FixedLenByteArray:
		return fixedLenByteArraySliceHeaderBytes + int64(len(value.(parquet.FixedLenByteArray)))
	default:
		return 0
	}
}

func writeColumnBatch(columnWriter file.ColumnChunkWriter, column column, buffer columnBuffer) error {
	var defLevels []int16
	if column.Optional {
		defLevels = buffer.defLevels
	}

	var err error
	switch writer := columnWriter.(type) {
	case *file.BooleanColumnChunkWriter:
		_, err = writer.WriteBatch(buffer.boolValues, defLevels, nil)
	case *file.Int32ColumnChunkWriter:
		_, err = writer.WriteBatch(buffer.int32Values, defLevels, nil)
	case *file.Int64ColumnChunkWriter:
		_, err = writer.WriteBatch(buffer.int64Values, defLevels, nil)
	case *file.Float32ColumnChunkWriter:
		_, err = writer.WriteBatch(buffer.float32Values, defLevels, nil)
	case *file.Float64ColumnChunkWriter:
		_, err = writer.WriteBatch(buffer.float64Values, defLevels, nil)
	case *file.ByteArrayColumnChunkWriter:
		_, err = writer.WriteBatch(buffer.byteArrayValues, defLevels, nil)
	case *file.FixedLenByteArrayColumnChunkWriter:
		_, err = writer.WriteBatch(buffer.fixedLenByteArrayValues, defLevels, nil)
	default:
		return fmt.Errorf("unsupported column chunk writer %T", columnWriter)
	}
	return err
}

// parseDecimalToScaledInteger converts decimal text into an integer scaled by
// 10^scale. This writer layer intentionally only performs conversion/serialization
// and does not enforce original SQL type/domain validity; callers are expected to
// pass already validated values. Extra fractional digits are truncated toward zero.
func parseDecimalToScaledInteger(s string, scale int) (*big.Int, error) {
	if scale < 0 {
		return nil, fmt.Errorf("invalid decimal scale %d", scale)
	}
	rat, ok := new(big.Rat).SetString(s)
	if !ok {
		return nil, fmt.Errorf("invalid decimal value %q", s)
	}
	multiplier := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	rat.Mul(rat, new(big.Rat).SetInt(multiplier))
	return new(big.Int).Quo(rat.Num(), rat.Denom()), nil
}

func toFixedLenTwoComplement(value *big.Int, byteWidth int) ([]byte, error) {
	if byteWidth <= 0 {
		return nil, fmt.Errorf("invalid fixed-size byte width %d", byteWidth)
	}

	bitWidth := uint(8*byteWidth - 1)
	maxValue := new(big.Int).Lsh(big.NewInt(1), bitWidth)
	maxValue.Sub(maxValue, big.NewInt(1))
	minValue := new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), bitWidth))
	if value.Cmp(minValue) < 0 || value.Cmp(maxValue) > 0 {
		return nil, fmt.Errorf("decimal value %s does not fit in %d bytes", value.String(), byteWidth)
	}

	encoded := make([]byte, byteWidth)
	if value.Sign() >= 0 {
		rawBytes := value.Bytes()
		copy(encoded[byteWidth-len(rawBytes):], rawBytes)
		return encoded, nil
	}

	modulus := new(big.Int).Lsh(big.NewInt(1), uint(8*byteWidth))
	twoComplement := new(big.Int).Add(value, modulus)
	rawBytes := twoComplement.Bytes()
	copy(encoded[byteWidth-len(rawBytes):], rawBytes)
	return encoded, nil
}

// ToColumnType converts database type to parquet column type.
func ToColumnType(columnInfo *ColumnInfo) ColumnType {
	columnType := normalizeColumnType(columnInfo.Type)
	switch columnType {
	case "CHAR", "VARCHAR", "DATE", "TIME", "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT", "SET", "JSON", "VECTOR":
		return ColumnType{Physical: parquet.Types.ByteArray, Logical: schema.StringLogicalType{}, TypeLength: -1}
	case "ENUM":
		return ColumnType{Physical: parquet.Types.ByteArray, Logical: schema.StringLogicalType{}, TypeLength: -1}
	case "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB", "BINARY", "VARBINARY", "BIT":
		return ColumnType{Physical: parquet.Types.ByteArray, TypeLength: -1}
	case "TIMESTAMP", "DATETIME":
		return ColumnType{Physical: parquet.Types.Int64, Logical: schema.NewTimestampLogicalType(false, schema.TimeUnitMicros), TypeLength: -1}
	case "YEAR", "TINYINT", "SMALLINT", "MEDIUMINT", "UNSIGNED TINYINT", "UNSIGNED SMALLINT", "INT", "UNSIGNED MEDIUMINT":
		return ColumnType{Physical: parquet.Types.Int32, TypeLength: -1}
	case "BIGINT", "UNSIGNED INT":
		return ColumnType{Physical: parquet.Types.Int64, TypeLength: -1}
	case "DECIMAL", "NUMERIC":
		return decimalColumnType(columnInfo)
	case "UNSIGNED BIGINT":
		return ColumnType{
			Physical:   parquet.Types.FixedLenByteArray,
			Logical:    schema.NewDecimalLogicalType(20, 0),
			TypeLength: 9,
			Precision:  20,
			Scale:      0,
		}
	case "FLOAT":
		// MySQL stores FLOAT as single-precision (4-byte) values, but MySQL evaluates
		// FLOAT expressions in double precision. Exporting as Parquet FLOAT (32-bit)
		// is not safe for compatibility because intermediate/query results may need
		// more precision than 32-bit can preserve.
		return ColumnType{Physical: parquet.Types.Double, TypeLength: -1}
	case "DOUBLE":
		return ColumnType{Physical: parquet.Types.Double, TypeLength: -1}
	}

	// Other database, like MariaDB.
	switch columnType {
	case "NCHAR", "NVARCHAR", "CHARACTER", "VARCHARACTER", "SQL_TSI_YEAR", "NULL", "VAR_STRING", "GEOMETRY", "LONG":
		return ColumnType{Physical: parquet.Types.ByteArray, TypeLength: -1}
	case "INTEGER", "INT1", "INT2", "INT3":
		return ColumnType{Physical: parquet.Types.Int32, TypeLength: -1}
	case "INT8":
		return ColumnType{Physical: parquet.Types.Int64, TypeLength: -1}
	case "BOOL", "BOOLEAN":
		return ColumnType{Physical: parquet.Types.Boolean, TypeLength: -1}
	case "REAL", "DOUBLE PRECISION", "FIXED":
		return ColumnType{Physical: parquet.Types.Double, TypeLength: -1}
	}
	return ColumnType{Physical: parquet.Types.ByteArray, TypeLength: -1}
}

func normalizeColumnType(columnType string) string {
	// ColumnInfo.Type comes from database/sql ColumnType.DatabaseTypeName().
	// With MySQL drivers, temporal types are reported as base names
	// (e.g. TIMESTAMP/DATETIME) without precision suffixes such as "(6)".
	return strings.ToUpper(strings.TrimSpace(columnType))
}

func decimalColumnType(columnInfo *ColumnInfo) ColumnType {
	precision, scale := int(columnInfo.Precision), int(columnInfo.Scale)
	// Keep an interoperability-first mapping here, not just raw Parquet limits:
	// - Parquet DECIMAL itself is not capped at 38 digits when stored as
	//   BYTE_ARRAY.
	// - However, many downstream engines cap DECIMAL at 38 digits, so map
	//   DECIMAL/NUMERIC with precision <= 38 to DECIMAL
	//   (INT32/INT64/FIXED_LEN_BYTE_ARRAY), and represent larger precision as
	//   UTF-8 strings in BYTE_ARRAY to avoid reader incompatibilities.
	if precision <= 0 || precision > 38 {
		return ColumnType{Physical: parquet.Types.ByteArray, Logical: schema.StringLogicalType{}, TypeLength: -1}
	}
	decimalLogicalType := schema.NewDecimalLogicalType(int32(precision), int32(scale))
	if precision <= 9 {
		return ColumnType{
			Physical:   parquet.Types.Int32,
			Logical:    decimalLogicalType,
			TypeLength: -1,
			Precision:  precision,
			Scale:      scale,
		}
	}
	if precision <= 18 {
		return ColumnType{
			Physical:   parquet.Types.Int64,
			Logical:    decimalLogicalType,
			TypeLength: -1,
			Precision:  precision,
			Scale:      scale,
		}
	}

	typeLength := decimalFixedLengthBytesForPrecision(precision)
	return ColumnType{
		Physical:   parquet.Types.FixedLenByteArray,
		Logical:    decimalLogicalType,
		TypeLength: typeLength,
		Precision:  precision,
		Scale:      scale,
	}
}

func decimalFixedLengthBytesForPrecision(precision int) int {
	if precision <= 0 {
		return -1
	}
	// Parquet DECIMAL for FIXED_LEN_BYTE_ARRAY requires:
	// precision <= floor(log10(2^(8*n - 1) - 1)).
	// Solving for n gives:
	// n >= (precision*log2(10) + 1) / 8.
	return int(math.Ceil((float64(precision)*math.Log2(10) + 1) / 8))
}
