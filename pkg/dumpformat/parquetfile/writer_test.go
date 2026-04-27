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
	"bytes"
	"database/sql"
	"math/big"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/require"
)

func TestParquetWriterWritesArrowReadableRows(t *testing.T) {
	var buf bytes.Buffer
	columns := []*ColumnInfo{
		{Name: "id", Type: "INT"},
		{Name: "name", Type: "VARCHAR", Nullable: true},
		{Name: "price", Type: "DECIMAL", Precision: 10, Scale: 2},
		{Name: "created_at", Type: "DATETIME"},
		{Name: "flag", Type: "BOOLEAN", Nullable: true},
	}

	pw, err := NewParquetWriter(
		&buf,
		columns,
		WithCompression(compress.Codecs.Zstd),
		WithDataPageSize(1024),
	)
	require.NoError(t, err)
	require.NoError(t, pw.Write([]sql.RawBytes{
		sql.RawBytes("1"),
		sql.RawBytes("Alice"),
		sql.RawBytes("123.45"),
		sql.RawBytes("2024-01-02 03:04:05"),
		sql.RawBytes("true"),
	}))
	require.NoError(t, pw.Write([]sql.RawBytes{
		sql.RawBytes("2"),
		nil,
		sql.RawBytes("-0.50"),
		sql.RawBytes("0000-00-00 00:00:00"),
		sql.RawBytes("false"),
	}))
	require.NoError(t, pw.Close())
	require.NotEmpty(t, buf.Bytes())

	reader, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	defer reader.Close()

	require.EqualValues(t, 2, reader.NumRows())
	require.Equal(t, 1, reader.NumRowGroups())

	metaSchema := reader.MetaData().Schema
	require.Equal(t, parquet.Types.Int32, metaSchema.Column(0).PhysicalType())
	require.EqualValues(t, 0, metaSchema.Column(0).MaxDefinitionLevel())
	require.Equal(t, parquet.Types.ByteArray, metaSchema.Column(1).PhysicalType())
	require.Equal(t, schema.ConvertedTypes.UTF8, metaSchema.Column(1).ConvertedType())
	require.EqualValues(t, 1, metaSchema.Column(1).MaxDefinitionLevel())
	require.Equal(t, parquet.Types.Int64, metaSchema.Column(2).PhysicalType())
	require.Equal(t, schema.ConvertedTypes.Decimal, metaSchema.Column(2).ConvertedType())
	require.Equal(t, parquet.Types.Int64, metaSchema.Column(3).PhysicalType())
	require.EqualValues(t, 1, metaSchema.Column(3).MaxDefinitionLevel())
	require.Equal(t, schema.ConvertedTypes.None, metaSchema.Column(3).ConvertedType())
	require.Equal(t, parquet.Types.Boolean, metaSchema.Column(4).PhysicalType())

	rowGroup := reader.RowGroup(0)
	readInt32Column(t, rowGroup, 0, 2, []int32{1, 2}, []int16{0, 0}, 2)
	readByteArrayColumn(t, rowGroup, 1, 2, []string{"Alice"}, []int16{1, 0}, 1)
	readInt64Column(t, rowGroup, 2, 2, []int64{12345, -50}, []int16{0, 0}, 2)
	readInt64Column(t, rowGroup, 3, 2, []int64{time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC).UnixMicro()}, []int16{1, 0}, 1)
	readBooleanColumn(t, rowGroup, 4, 2, []bool{true, false}, []int16{1, 1}, 2)

	t.Run("validates writer constructor inputs", func(t *testing.T) {
		_, err := NewParquetWriter(nil, columns)
		require.ErrorContains(t, err, "parquet output buffer is nil")

		_, err = NewParquetWriter(&bytes.Buffer{}, []*ColumnInfo{nil})
		require.ErrorContains(t, err, "parquet column info is nil")

		_, err = NewParquetWriter(&bytes.Buffer{}, []*ColumnInfo{{Name: "", Type: "INT"}})
		require.ErrorContains(t, err, "parquet column name is empty")
	})

	t.Run("Close closes writer and Write fails afterwards", func(t *testing.T) {
		var buf bytes.Buffer
		pw, err := NewParquetWriter(&buf, []*ColumnInfo{
			{Name: "id", Type: "INT"},
		})
		require.NoError(t, err)
		require.NoError(t, pw.Write([]sql.RawBytes{sql.RawBytes("7")}))
		require.NoError(t, pw.Close())
		require.ErrorContains(t, pw.Write([]sql.RawBytes{sql.RawBytes("8")}), "parquet writer is closed")
	})
}

func TestParquetWriterMapsUnsignedBigIntAsFixedDecimal(t *testing.T) {
	var buf bytes.Buffer
	pw, err := NewParquetWriter(&buf, []*ColumnInfo{
		{Name: "u", Type: "UNSIGNED BIGINT"},
	}, WithCompression(compress.Codecs.Uncompressed))
	require.NoError(t, err)
	require.NoError(t, pw.Write([]sql.RawBytes{sql.RawBytes("18446744073709551615")}))
	require.NoError(t, pw.Close())

	reader, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	defer reader.Close()

	col := reader.MetaData().Schema.Column(0)
	require.Equal(t, parquet.Types.FixedLenByteArray, col.PhysicalType())
	require.Equal(t, schema.ConvertedTypes.Decimal, col.ConvertedType())
	require.Equal(t, 9, col.TypeLength())

	chunkReader, err := reader.RowGroup(0).Column(0)
	require.NoError(t, err)
	fixedReader := chunkReader.(*file.FixedLenByteArrayColumnChunkReader)
	values := make([]parquet.FixedLenByteArray, 1)
	total, valuesRead, err := fixedReader.ReadBatch(1, values, nil, nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, total)
	require.Equal(t, 1, valuesRead)
	require.Equal(t, []byte{0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, []byte(values[0]))

	t.Run("maps FLOAT to DOUBLE", func(t *testing.T) {
		columnType := ToColumnType(&ColumnInfo{Type: "FLOAT"})
		require.Equal(t, parquet.Types.Double, columnType.Physical)
	})

	t.Run("maps CHAR to BYTE_ARRAY with string logical type", func(t *testing.T) {
		columnType := ToColumnType(&ColumnInfo{Type: "CHAR"})
		require.Equal(t, parquet.Types.ByteArray, columnType.Physical)
		_, ok := columnType.Logical.(schema.StringLogicalType)
		require.True(t, ok)
	})

	t.Run("maps NUMERIC as DECIMAL", func(t *testing.T) {
		columnType := ToColumnType(&ColumnInfo{Type: "NUMERIC", Precision: 10, Scale: 2})
		require.Equal(t, parquet.Types.Int64, columnType.Physical)
		_, ok := columnType.Logical.(schema.DecimalLogicalType)
		require.True(t, ok)
		require.Equal(t, 10, columnType.Precision)
		require.Equal(t, 2, columnType.Scale)
	})

	t.Run("maps UNSIGNED MEDIUMINT to INT32", func(t *testing.T) {
		columnType := ToColumnType(&ColumnInfo{Type: "UNSIGNED MEDIUMINT"})
		require.Equal(t, parquet.Types.Int32, columnType.Physical)
	})

	t.Run("maps DECIMAL precision 38 to FIXED_LEN_BYTE_ARRAY", func(t *testing.T) {
		columnType := ToColumnType(&ColumnInfo{Type: "DECIMAL", Precision: 38, Scale: 6})
		require.Equal(t, parquet.Types.FixedLenByteArray, columnType.Physical)
		require.Equal(t, 16, columnType.TypeLength)
		_, ok := columnType.Logical.(schema.DecimalLogicalType)
		require.True(t, ok)
		require.Equal(t, 38, columnType.Precision)
		require.Equal(t, 6, columnType.Scale)
	})

	t.Run("maps NUMERIC precision over 38 to string byte array", func(t *testing.T) {
		columnType := ToColumnType(&ColumnInfo{Type: "NUMERIC", Precision: 39, Scale: 2})
		require.Equal(t, parquet.Types.ByteArray, columnType.Physical)
		_, ok := columnType.Logical.(schema.StringLogicalType)
		require.True(t, ok)
	})

	t.Run("maps MariaDB aliases and unknown types", func(t *testing.T) {
		realType := ToColumnType(&ColumnInfo{Type: "REAL"})
		require.Equal(t, parquet.Types.Double, realType.Physical)

		boolType := ToColumnType(&ColumnInfo{Type: "BOOL"})
		require.Equal(t, parquet.Types.Boolean, boolType.Physical)

		ncharType := ToColumnType(&ColumnInfo{Type: "NCHAR"})
		require.Equal(t, parquet.Types.ByteArray, ncharType.Physical)

		fallbackType := ToColumnType(&ColumnInfo{Type: "SOME_NEW_TYPE"})
		require.Equal(t, parquet.Types.ByteArray, fallbackType.Physical)
	})

	t.Run("maps DECIMAL precision boundaries", func(t *testing.T) {
		int32Type := ToColumnType(&ColumnInfo{Type: "DECIMAL", Precision: 9, Scale: 3})
		require.Equal(t, parquet.Types.Int32, int32Type.Physical)
		require.Equal(t, 9, int32Type.Precision)
		require.Equal(t, 3, int32Type.Scale)

		int64Type := ToColumnType(&ColumnInfo{Type: "DECIMAL", Precision: 18, Scale: 4})
		require.Equal(t, parquet.Types.Int64, int64Type.Physical)

		fixedType := ToColumnType(&ColumnInfo{Type: "DECIMAL", Precision: 19, Scale: 5})
		require.Equal(t, parquet.Types.FixedLenByteArray, fixedType.Physical)
		require.Equal(t, decimalFixedLengthBytesForPrecision(19), fixedType.TypeLength)

		stringType := ToColumnType(&ColumnInfo{Type: "DECIMAL", Precision: 0, Scale: 2})
		require.Equal(t, parquet.Types.ByteArray, stringType.Physical)
	})

	t.Run("maps additional SQL type families", func(t *testing.T) {
		enumType := ToColumnType(&ColumnInfo{Type: "ENUM"})
		require.Equal(t, parquet.Types.ByteArray, enumType.Physical)
		_, ok := enumType.Logical.(schema.StringLogicalType)
		require.True(t, ok)

		blobType := ToColumnType(&ColumnInfo{Type: "BLOB"})
		require.Equal(t, parquet.Types.ByteArray, blobType.Physical)

		timestampType := ToColumnType(&ColumnInfo{Type: "TIMESTAMP"})
		require.Equal(t, parquet.Types.Int64, timestampType.Physical)
		timestampLogicalType, ok := timestampType.Logical.(schema.TimestampLogicalType)
		require.True(t, ok)
		require.Equal(t, schema.TimeUnitMicros, timestampLogicalType.TimeUnit())

		timestampMillisType := ToColumnType(&ColumnInfo{Type: "TIMESTAMP", Scale: 3})
		timestampMillisLogicalType, ok := timestampMillisType.Logical.(schema.TimestampLogicalType)
		require.True(t, ok)
		require.Equal(t, schema.TimeUnitMillis, timestampMillisLogicalType.TimeUnit())

		datetimeMicrosType := ToColumnType(&ColumnInfo{Type: "DATETIME", Scale: 6})
		datetimeMicrosLogicalType, ok := datetimeMicrosType.Logical.(schema.TimestampLogicalType)
		require.True(t, ok)
		require.Equal(t, schema.TimeUnitMicros, datetimeMicrosLogicalType.TimeUnit())

		datetimeMillisType := ToColumnType(&ColumnInfo{Type: "DATETIME", Precision: 23, Scale: 3})
		datetimeMillisLogicalType, ok := datetimeMillisType.Logical.(schema.TimestampLogicalType)
		require.True(t, ok)
		require.Equal(t, schema.TimeUnitMillis, datetimeMillisLogicalType.TimeUnit())

		bigintType := ToColumnType(&ColumnInfo{Type: "BIGINT"})
		require.Equal(t, parquet.Types.Int64, bigintType.Physical)

		doubleType := ToColumnType(&ColumnInfo{Type: "DOUBLE"})
		require.Equal(t, parquet.Types.Double, doubleType.Physical)

		integerAlias := ToColumnType(&ColumnInfo{Type: "INTEGER"})
		require.Equal(t, parquet.Types.Int32, integerAlias.Physical)

		int8Alias := ToColumnType(&ColumnInfo{Type: "INT8"})
		require.Equal(t, parquet.Types.Int64, int8Alias.Physical)

		fixedAlias := ToColumnType(&ColumnInfo{Type: "FIXED"})
		require.Equal(t, parquet.Types.Double, fixedAlias.Physical)
	})

	t.Run("decimalFixedLengthBytesForPrecision handles edge cases", func(t *testing.T) {
		require.Equal(t, -1, decimalFixedLengthBytesForPrecision(0))
		require.Equal(t, 1, decimalFixedLengthBytesForPrecision(2))
	})

	t.Run("parseDecimalToScaledInteger handles truncation and errors", func(t *testing.T) {
		scaled, err := parseDecimalToScaledInteger("12.349", 2)
		require.NoError(t, err)
		require.Equal(t, "1234", scaled.String())

		scaled, err = parseDecimalToScaledInteger("-12.349", 2)
		require.NoError(t, err)
		require.Equal(t, "-1234", scaled.String())

		_, err = parseDecimalToScaledInteger("not-decimal", 2)
		require.ErrorContains(t, err, "invalid decimal value")

		_, err = parseDecimalToScaledInteger("1", -1)
		require.ErrorContains(t, err, "invalid decimal scale")
	})

	t.Run("toFixedLenTwoComplement handles boundaries and overflow", func(t *testing.T) {
		encoded, err := toFixedLenTwoComplement(big.NewInt(255), 2)
		require.NoError(t, err)
		require.Equal(t, []byte{0x00, 0xff}, encoded)

		encoded, err = toFixedLenTwoComplement(big.NewInt(-1), 2)
		require.NoError(t, err)
		require.Equal(t, []byte{0xff, 0xff}, encoded)

		_, err = toFixedLenTwoComplement(big.NewInt(128), 1)
		require.ErrorContains(t, err, "does not fit in 1 bytes")

		_, err = toFixedLenTwoComplement(big.NewInt(-129), 1)
		require.ErrorContains(t, err, "does not fit in 1 bytes")

		_, err = toFixedLenTwoComplement(big.NewInt(0), 0)
		require.ErrorContains(t, err, "invalid fixed-size byte width")
	})

	t.Run("parseRawColumnValue covers success and error branches", func(t *testing.T) {
		value, isNull, err := parseRawColumnValue(sql.RawBytes("true"), column{
			ColumnType: ColumnType{Physical: parquet.Types.Boolean},
		})
		require.NoError(t, err)
		require.False(t, isNull)
		require.Equal(t, true, value.(bool))

		_, _, err = parseRawColumnValue(sql.RawBytes("bad-bool"), column{
			ColumnType: ColumnType{Physical: parquet.Types.Boolean},
		})
		require.Error(t, err)

		value, isNull, err = parseRawColumnValue(sql.RawBytes("12.34"), column{
			ColumnType: ColumnType{
				Physical: parquet.Types.Int32,
				Logical:  schema.NewDecimalLogicalType(9, 2),
				Scale:    2,
			},
		})
		require.NoError(t, err)
		require.False(t, isNull)
		require.Equal(t, int32(1234), value.(int32))

		_, _, err = parseRawColumnValue(sql.RawBytes("21474836.48"), column{
			ColumnType: ColumnType{
				Physical: parquet.Types.Int32,
				Logical:  schema.NewDecimalLogicalType(9, 2),
				Scale:    2,
			},
		})
		require.ErrorContains(t, err, "does not fit in INT32")

		value, isNull, err = parseRawColumnValue(sql.RawBytes("2024-01-02 03:04:05"), column{
			Optional: true,
			ColumnType: ColumnType{
				Physical: parquet.Types.Int64,
				Logical:  schema.NewTimestampLogicalType(false, schema.TimeUnitMicros),
			},
		})
		require.NoError(t, err)
		require.False(t, isNull)
		require.Equal(t, time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC).UnixMicro(), value.(int64))

		value, isNull, err = parseRawColumnValue(sql.RawBytes("2024-01-02 03:04:05.123"), column{
			Optional: true,
			ColumnType: ColumnType{
				Physical: parquet.Types.Int64,
				Logical:  schema.NewTimestampLogicalType(false, schema.TimeUnitMillis),
			},
		})
		require.NoError(t, err)
		require.False(t, isNull)
		require.Equal(t, time.Date(2024, 1, 2, 3, 4, 5, 123000000, time.UTC).UnixMilli(), value.(int64))

		value, isNull, err = parseRawColumnValue(sql.RawBytes("2024-01-02 03:04:05.123456"), column{
			Optional: true,
			ColumnType: ColumnType{
				Physical: parquet.Types.Int64,
				Logical:  schema.NewTimestampLogicalType(false, schema.TimeUnitMicros),
			},
		})
		require.NoError(t, err)
		require.False(t, isNull)
		require.Equal(t, time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC).UnixMicro(), value.(int64))

		value, isNull, err = parseRawColumnValue(sql.RawBytes("2024-01-02 03:04:05.1"), column{
			Optional: true,
			ColumnType: ColumnType{
				Physical: parquet.Types.Int64,
				Logical:  schema.NewTimestampLogicalType(false, schema.TimeUnitMicros),
			},
		})
		require.NoError(t, err)
		require.False(t, isNull)
		require.Equal(t, time.Date(2024, 1, 2, 3, 4, 5, 100000000, time.UTC).UnixMicro(), value.(int64))

		value, isNull, err = parseRawColumnValue(sql.RawBytes("2024-01-02T03:04:05"), column{
			Optional: true,
			ColumnType: ColumnType{
				Physical: parquet.Types.Int64,
				Logical:  schema.NewTimestampLogicalType(false, schema.TimeUnitMicros),
			},
		})
		require.NoError(t, err)
		require.False(t, isNull)
		require.Equal(t, time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC).UnixMicro(), value.(int64))

		value, isNull, err = parseRawColumnValue(sql.RawBytes("0000-00-00 00:00:00"), column{
			Optional: true,
			ColumnType: ColumnType{
				Physical: parquet.Types.Int64,
				Logical:  schema.NewTimestampLogicalType(false, schema.TimeUnitMicros),
			},
		})
		require.NoError(t, err)
		require.True(t, isNull)
		require.Nil(t, value)

		_, _, err = parseRawColumnValue(sql.RawBytes("0000-00-00 00:00:00"), column{
			Optional: false,
			ColumnType: ColumnType{
				Physical: parquet.Types.Int64,
				Logical:  schema.NewTimestampLogicalType(false, schema.TimeUnitMicros),
			},
		})
		require.Error(t, err)

		_, _, err = parseRawColumnValue(sql.RawBytes("9223372036854775808"), column{
			ColumnType: ColumnType{
				Physical: parquet.Types.Int64,
				Logical:  schema.NewDecimalLogicalType(19, 0),
			},
		})
		require.ErrorContains(t, err, "does not fit in INT64")

		rawBytes := sql.RawBytes("abcd")
		value, isNull, err = parseRawColumnValue(rawBytes, column{
			ColumnType: ColumnType{Physical: parquet.Types.ByteArray},
		})
		require.NoError(t, err)
		require.False(t, isNull)
		rawBytes[0] = 'z'
		require.Equal(t, "abcd", string(value.(parquet.ByteArray)))

		value, isNull, err = parseRawColumnValue(sql.RawBytes("-1.23"), column{
			ColumnType: ColumnType{
				Physical:   parquet.Types.FixedLenByteArray,
				Logical:    schema.NewDecimalLogicalType(10, 2),
				TypeLength: 4,
				Scale:      2,
			},
		})
		require.NoError(t, err)
		require.False(t, isNull)
		require.Equal(t, []byte{0xff, 0xff, 0xff, 0x85}, []byte(value.(parquet.FixedLenByteArray)))

		rawFixedBytes := sql.RawBytes("wxyz")
		value, isNull, err = parseRawColumnValue(rawFixedBytes, column{
			ColumnType: ColumnType{
				Physical:   parquet.Types.FixedLenByteArray,
				TypeLength: 4,
			},
		})
		require.NoError(t, err)
		require.False(t, isNull)
		rawFixedBytes[0] = 'q'
		require.Equal(t, "wxyz", string(value.(parquet.FixedLenByteArray)))

		_, _, err = parseRawColumnValue(sql.RawBytes("abc"), column{
			ColumnType: ColumnType{
				Physical:   parquet.Types.FixedLenByteArray,
				TypeLength: 4,
			},
		})
		require.ErrorContains(t, err, "width mismatch")

		_, _, err = parseRawColumnValue(sql.RawBytes("abcd"), column{
			ColumnType: ColumnType{
				Physical:   parquet.Types.FixedLenByteArray,
				TypeLength: 0,
			},
		})
		require.ErrorContains(t, err, "invalid fixed-size byte width")

		_, _, err = parseRawColumnValue(sql.RawBytes("v"), column{
			ColumnType: ColumnType{Physical: parquet.Types.Int96},
		})
		require.ErrorContains(t, err, "unsupported parquet physical type")
	})

	t.Run("parseRawColumnValue numeric primitive branches", func(t *testing.T) {
		value, isNull, err := parseRawColumnValue(sql.RawBytes("123"), column{
			ColumnType: ColumnType{Physical: parquet.Types.Int32},
		})
		require.NoError(t, err)
		require.False(t, isNull)
		require.Equal(t, int32(123), value.(int32))

		value, isNull, err = parseRawColumnValue(sql.RawBytes("456"), column{
			ColumnType: ColumnType{Physical: parquet.Types.Int64},
		})
		require.NoError(t, err)
		require.False(t, isNull)
		require.Equal(t, int64(456), value.(int64))

		value, isNull, err = parseRawColumnValue(sql.RawBytes("1.5"), column{
			ColumnType: ColumnType{Physical: parquet.Types.Float},
		})
		require.NoError(t, err)
		require.False(t, isNull)
		require.Equal(t, float32(1.5), value.(float32))

		_, _, err = parseRawColumnValue(sql.RawBytes("bad-float"), column{
			ColumnType: ColumnType{Physical: parquet.Types.Float},
		})
		require.Error(t, err)

		value, isNull, err = parseRawColumnValue(sql.RawBytes("2.5"), column{
			ColumnType: ColumnType{Physical: parquet.Types.Double},
		})
		require.NoError(t, err)
		require.False(t, isNull)
		require.Equal(t, float64(2.5), value.(float64))

		_, _, err = parseRawColumnValue(sql.RawBytes("bad-double"), column{
			ColumnType: ColumnType{Physical: parquet.Types.Double},
		})
		require.Error(t, err)

		_, _, err = parseRawColumnValue(sql.RawBytes("1.28"), column{
			ColumnType: ColumnType{
				Physical:   parquet.Types.FixedLenByteArray,
				Logical:    schema.NewDecimalLogicalType(3, 2),
				TypeLength: 1,
				Scale:      2,
			},
		})
		require.ErrorContains(t, err, "does not fit in 1 bytes")
	})

	t.Run("appendColumnValue appends all supported physical types", func(t *testing.T) {
		buffer := &columnBuffer{}

		require.NoError(t, appendColumnValue(buffer, column{
			ColumnType: ColumnType{Physical: parquet.Types.Boolean},
		}, true))
		require.Equal(t, []bool{true}, buffer.boolValues)

		require.NoError(t, appendColumnValue(buffer, column{
			ColumnType: ColumnType{Physical: parquet.Types.Int32},
		}, int32(7)))
		require.Equal(t, []int32{7}, buffer.int32Values)

		require.NoError(t, appendColumnValue(buffer, column{
			ColumnType: ColumnType{Physical: parquet.Types.Int64},
		}, int64(8)))
		require.Equal(t, []int64{8}, buffer.int64Values)

		require.NoError(t, appendColumnValue(buffer, column{
			ColumnType: ColumnType{Physical: parquet.Types.Float},
		}, float32(1.25)))
		require.Equal(t, []float32{1.25}, buffer.float32Values)

		require.NoError(t, appendColumnValue(buffer, column{
			ColumnType: ColumnType{Physical: parquet.Types.Double},
		}, float64(2.25)))
		require.Equal(t, []float64{2.25}, buffer.float64Values)

		require.NoError(t, appendColumnValue(buffer, column{
			ColumnType: ColumnType{Physical: parquet.Types.ByteArray},
		}, parquet.ByteArray([]byte("a"))))
		require.Equal(t, []parquet.ByteArray{parquet.ByteArray([]byte("a"))}, buffer.byteArrayValues)

		require.NoError(t, appendColumnValue(buffer, column{
			ColumnType: ColumnType{Physical: parquet.Types.FixedLenByteArray},
		}, parquet.FixedLenByteArray([]byte("bc"))))
		require.Equal(t, []parquet.FixedLenByteArray{parquet.FixedLenByteArray([]byte("bc"))}, buffer.fixedLenByteArrayValues)
	})

}

func TestParquetWriterCopiesByteRowsBeforeClose(t *testing.T) {
	var buf bytes.Buffer
	pw, err := NewParquetWriter(&buf, []*ColumnInfo{
		{Name: "name", Type: "VARCHAR"},
	})
	require.NoError(t, err)

	value := sql.RawBytes("before")
	require.NoError(t, pw.Write([]sql.RawBytes{value}))
	copy(value, "after!")
	require.NoError(t, pw.Close())

	reader, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	defer reader.Close()

	readByteArrayColumn(t, reader.RowGroup(0), 0, 1, []string{"before"}, []int16{0}, 1)

	t.Run("Close without buffered rows is safe", func(t *testing.T) {
		var closeBuf bytes.Buffer
		closePW, err := NewParquetWriter(&closeBuf, []*ColumnInfo{
			{Name: "name", Type: "VARCHAR"},
		})
		require.NoError(t, err)
		require.NoError(t, closePW.Close())
		require.NoError(t, closePW.Close())
		require.ErrorContains(t, closePW.Write([]sql.RawBytes{sql.RawBytes("x")}), "parquet writer is closed")
	})
}

func TestParquetWriterCapsMaxRowGroupLength(t *testing.T) {
	localOptions := newWriterOptions([]WriterOption{
		WithCompression(compress.Codecs.Uncompressed),
		WithDataPageSize(2048),
		WithRowGroupMemoryLimit(4),
	})
	props := parquet.NewWriterProperties(localOptions.writerProperties...)
	require.Equal(t, compress.Codecs.Uncompressed, props.Compression())
	require.EqualValues(t, 2048, props.DataPageSize())
	require.EqualValues(t, 4, localOptions.rowGroupMemoryLimitBytes)

	defaultOptions := defaultWriterOptions()
	defaultProps := parquet.NewWriterProperties(defaultOptions.writerProperties...)
	require.Equal(t, DefaultCompressionType, defaultProps.Compression())
	require.EqualValues(t, defaultRowGroupMemoryLimitBytes, defaultOptions.rowGroupMemoryLimitBytes)

	t.Run("flushes row group by accounted memory bytes", func(t *testing.T) {
		var buf bytes.Buffer
		pw, err := NewParquetWriter(&buf, []*ColumnInfo{
			{Name: "name", Type: "VARCHAR"},
		}, WithRowGroupMemoryLimit(4))
		require.NoError(t, err)

		require.NoError(t, pw.Write([]sql.RawBytes{sql.RawBytes("abcd")}))
		require.NoError(t, pw.Write([]sql.RawBytes{sql.RawBytes("efgh")}))
		require.NoError(t, pw.Close())

		reader, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
		require.NoError(t, err)
		defer reader.Close()

		require.EqualValues(t, 2, reader.NumRows())
		require.Equal(t, 2, reader.NumRowGroups())
	})

	t.Run("accounts byte-array slice header memory", func(t *testing.T) {
		col := column{ColumnType: ColumnType{Physical: parquet.Types.ByteArray}}
		require.EqualValues(
			t,
			byteArraySliceHeaderBytes+4,
			accountColumnValueMemoryBytes(col, parquet.ByteArray([]byte("abcd"))),
		)
	})

	t.Run("accounts fixed-len byte-array slice header memory", func(t *testing.T) {
		col := column{ColumnType: ColumnType{Physical: parquet.Types.FixedLenByteArray}}
		require.EqualValues(
			t,
			fixedLenByteArraySliceHeaderBytes+6,
			accountColumnValueMemoryBytes(col, parquet.FixedLenByteArray([]byte("abcdef"))),
		)
	})

	t.Run("accounts primitive and unknown physical types", func(t *testing.T) {
		require.EqualValues(t, 1, accountColumnValueMemoryBytes(column{
			ColumnType: ColumnType{Physical: parquet.Types.Boolean},
		}, true))
		require.EqualValues(t, 4, accountColumnValueMemoryBytes(column{
			ColumnType: ColumnType{Physical: parquet.Types.Int32},
		}, int32(1)))
		require.EqualValues(t, 8, accountColumnValueMemoryBytes(column{
			ColumnType: ColumnType{Physical: parquet.Types.Int64},
		}, int64(1)))
		require.EqualValues(t, 0, accountColumnValueMemoryBytes(column{
			ColumnType: ColumnType{Physical: parquet.Types.Int96},
		}, nil))
	})

	t.Run("estimates written bytes plus buffered bytes", func(t *testing.T) {
		var buf bytes.Buffer
		pw, err := NewParquetWriter(&buf, []*ColumnInfo{
			{Name: "name", Type: "VARCHAR"},
		}, WithRowGroupMemoryLimit(defaultRowGroupMemoryLimitBytes))
		require.NoError(t, err)

		require.Equal(t, uint64(pw.totalWrittenBytes()), pw.EstimateFileSize())

		require.NoError(t, pw.Write([]sql.RawBytes{sql.RawBytes("abcd")}))
		expected := pw.totalWrittenBytes() + pw.bufferedMemoryBytes
		require.Greater(t, pw.bufferedMemoryBytes, int64(0))
		require.Equal(t, uint64(expected), pw.EstimateFileSize())

		require.NoError(t, pw.flushRows())
		require.Equal(t, uint64(pw.totalWrittenBytes()), pw.EstimateFileSize())
		require.NoError(t, pw.Close())
	})
}

func TestParquetWriterRecoversAfterRowConversionError(t *testing.T) {
	var buf bytes.Buffer
	pw, err := NewParquetWriter(&buf, []*ColumnInfo{
		{Name: "id", Type: "INT"},
		{Name: "flag", Type: "BOOLEAN"},
	})
	require.NoError(t, err)

	require.NoError(t, pw.Write([]sql.RawBytes{
		sql.RawBytes("1"),
		sql.RawBytes("true"),
	}))
	err = pw.Write([]sql.RawBytes{
		sql.RawBytes("2"),
		sql.RawBytes("bad-bool"),
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "convert parquet column flag")
	require.NoError(t, pw.Write([]sql.RawBytes{
		sql.RawBytes("3"),
		sql.RawBytes("false"),
	}))
	require.NoError(t, pw.Close())

	reader, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	defer reader.Close()

	require.EqualValues(t, 2, reader.NumRows())
	rowGroup := reader.RowGroup(0)
	readInt32Column(t, rowGroup, 0, 2, []int32{1, 3}, []int16{0, 0}, 2)
	readBooleanColumn(t, rowGroup, 1, 2, []bool{true, false}, []int16{0, 0}, 2)

	t.Run("Write validates row length and required NULL", func(t *testing.T) {
		var localBuf bytes.Buffer
		localPW, err := NewParquetWriter(&localBuf, []*ColumnInfo{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "VARCHAR"},
		})
		require.NoError(t, err)

		err = localPW.Write([]sql.RawBytes{
			sql.RawBytes("1"),
		})
		require.ErrorContains(t, err, "parquet row has 1 values, expected 2")

		err = localPW.Write([]sql.RawBytes{
			nil,
			sql.RawBytes("alice"),
		})
		require.ErrorContains(t, err, "required column receives NULL")

		require.NoError(t, localPW.Close())
	})

	t.Run("newColumnBuffer and appendColumnValue validate unsupported cases", func(t *testing.T) {
		_, err := newColumnBuffer(column{
			ColumnInfo: ColumnInfo{Name: "f"},
			ColumnType: ColumnType{
				Physical:   parquet.Types.FixedLenByteArray,
				TypeLength: 0,
			},
		}, 1)
		require.ErrorContains(t, err, "invalid fixed-size byte width")

		_, err = newColumnBuffer(column{
			ColumnInfo: ColumnInfo{Name: "u"},
			ColumnType: ColumnType{
				Physical: parquet.Types.Int96,
			},
		}, 1)
		require.ErrorContains(t, err, "unsupported parquet physical type")

		_, err = newColumnBuffers([]column{
			{
				ColumnInfo: ColumnInfo{Name: "bad"},
				ColumnType: ColumnType{
					Physical:   parquet.Types.FixedLenByteArray,
					TypeLength: 0,
				},
			},
		}, 1)
		require.ErrorContains(t, err, "init parquet buffer for column bad")

		err = appendColumnValue(&columnBuffer{}, column{
			ColumnType: ColumnType{Physical: parquet.Types.Int96},
		}, nil)
		require.ErrorContains(t, err, "unsupported parquet physical type")
	})

	t.Run("newColumnBuffer initializes supported float and double columns", func(t *testing.T) {
		floatBuffer, err := newColumnBuffer(column{
			ColumnInfo: ColumnInfo{Name: "f"},
			ColumnType: ColumnType{
				Physical: parquet.Types.Float,
			},
		}, 2)
		require.NoError(t, err)
		require.NotNil(t, floatBuffer.float32Values)

		doubleBuffer, err := newColumnBuffer(column{
			ColumnInfo: ColumnInfo{Name: "d"},
			ColumnType: ColumnType{
				Physical: parquet.Types.Double,
			},
		}, 2)
		require.NoError(t, err)
		require.NotNil(t, doubleBuffer.float64Values)
	})

	t.Run("writeColumnBatch handles float32 and float64 writers", func(t *testing.T) {
		newFloatSchema := func(physical parquet.Type, name string) *schema.GroupNode {
			field, err := schema.NewPrimitiveNode(name, parquet.Repetitions.Required, physical, -1, -1)
			require.NoError(t, err)
			root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, []schema.Node{field}, -1)
			require.NoError(t, err)
			return root
		}

		t.Run("float32 writer", func(t *testing.T) {
			var out bytes.Buffer
			writer := file.NewParquetWriter(&out, newFloatSchema(parquet.Types.Float, "f32"))
			rowGroupWriter := writer.AppendRowGroup()
			columnWriter, err := rowGroupWriter.NextColumn()
			require.NoError(t, err)

			err = writeColumnBatch(columnWriter, column{
				ColumnType: ColumnType{Physical: parquet.Types.Float},
			}, columnBuffer{
				float32Values: []float32{1.5},
			})
			require.NoError(t, err)
			require.NoError(t, columnWriter.Close())
			require.NoError(t, rowGroupWriter.Close())
			require.NoError(t, writer.Close())
		})

		t.Run("float64 writer", func(t *testing.T) {
			var out bytes.Buffer
			writer := file.NewParquetWriter(&out, newFloatSchema(parquet.Types.Double, "f64"))
			rowGroupWriter := writer.AppendRowGroup()
			columnWriter, err := rowGroupWriter.NextColumn()
			require.NoError(t, err)

			err = writeColumnBatch(columnWriter, column{
				ColumnType: ColumnType{Physical: parquet.Types.Double},
			}, columnBuffer{
				float64Values: []float64{2.5},
			})
			require.NoError(t, err)
			require.NoError(t, columnWriter.Close())
			require.NoError(t, rowGroupWriter.Close())
			require.NoError(t, writer.Close())
		})
	})

	t.Run("writeColumnBatch returns error for unsupported concrete writer type", func(t *testing.T) {
		var out bytes.Buffer
		pw, err := NewParquetWriter(&out, []*ColumnInfo{
			{Name: "id", Type: "INT"},
		})
		require.NoError(t, err)

		rowGroupWriter := pw.writer.AppendRowGroup()
		columnWriter, err := rowGroupWriter.NextColumn()
		require.NoError(t, err)

		err = writeColumnBatch(wrappedColumnChunkWriter{ColumnChunkWriter: columnWriter}, column{
			ColumnType: ColumnType{Physical: parquet.Types.Int32},
		}, columnBuffer{
			int32Values: []int32{1},
		})
		require.ErrorContains(t, err, "unsupported column chunk writer")

		require.NoError(t, columnWriter.Close())
		require.NoError(t, rowGroupWriter.Close())
		require.NoError(t, pw.writer.Close())
	})
}

type wrappedColumnChunkWriter struct {
	file.ColumnChunkWriter
}

func readInt32Column(t *testing.T, rowGroup *file.RowGroupReader, column, rows int, expected []int32, expectedDef []int16, expectedValues int) {
	t.Helper()
	chunkReader, err := rowGroup.Column(column)
	require.NoError(t, err)
	reader := chunkReader.(*file.Int32ColumnChunkReader)
	values := make([]int32, rows)
	defLevels := make([]int16, rows)
	total, valuesRead, err := reader.ReadBatch(int64(rows), values, defLevels, nil)
	require.NoError(t, err)
	require.EqualValues(t, rows, total)
	require.Equal(t, expectedValues, valuesRead)
	require.Equal(t, expected, values[:valuesRead])
	require.Equal(t, expectedDef, defLevels)
}

func readInt64Column(t *testing.T, rowGroup *file.RowGroupReader, column, rows int, expected []int64, expectedDef []int16, expectedValues int) {
	t.Helper()
	chunkReader, err := rowGroup.Column(column)
	require.NoError(t, err)
	reader := chunkReader.(*file.Int64ColumnChunkReader)
	values := make([]int64, rows)
	defLevels := make([]int16, rows)
	total, valuesRead, err := reader.ReadBatch(int64(rows), values, defLevels, nil)
	require.NoError(t, err)
	require.EqualValues(t, rows, total)
	require.Equal(t, expectedValues, valuesRead)
	require.Equal(t, expected, values[:valuesRead])
	require.Equal(t, expectedDef, defLevels)
}

func readByteArrayColumn(t *testing.T, rowGroup *file.RowGroupReader, column, rows int, expected []string, expectedDef []int16, expectedValues int) {
	t.Helper()
	chunkReader, err := rowGroup.Column(column)
	require.NoError(t, err)
	reader := chunkReader.(*file.ByteArrayColumnChunkReader)
	values := make([]parquet.ByteArray, rows)
	defLevels := make([]int16, rows)
	total, valuesRead, err := reader.ReadBatch(int64(rows), values, defLevels, nil)
	require.NoError(t, err)
	require.EqualValues(t, rows, total)
	require.Equal(t, expectedValues, valuesRead)
	actual := make([]string, 0, valuesRead)
	for _, value := range values[:valuesRead] {
		actual = append(actual, string(value))
	}
	require.Equal(t, expected, actual)
	require.Equal(t, expectedDef, defLevels)
}

func readBooleanColumn(t *testing.T, rowGroup *file.RowGroupReader, column, rows int, expected []bool, expectedDef []int16, expectedValues int) {
	t.Helper()
	chunkReader, err := rowGroup.Column(column)
	require.NoError(t, err)
	reader := chunkReader.(*file.BooleanColumnChunkReader)
	values := make([]bool, rows)
	defLevels := make([]int16, rows)
	total, valuesRead, err := reader.ReadBatch(int64(rows), values, defLevels, nil)
	require.NoError(t, err)
	require.EqualValues(t, rows, total)
	require.Equal(t, expectedValues, valuesRead)
	require.Equal(t, expected, values[:valuesRead])
	require.Equal(t, expectedDef, defLevels)
}
