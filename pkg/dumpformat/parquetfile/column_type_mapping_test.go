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
	"testing"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/require"
)

func TestToColumnTypeMappings(t *testing.T) {
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
		// we always use TimeUnitMicros.
		require.Equal(t, schema.TimeUnitMicros, timestampMillisLogicalType.TimeUnit())

		datetimeMicrosType := ToColumnType(&ColumnInfo{Type: "DATETIME", Scale: 6})
		datetimeMicrosLogicalType, ok := datetimeMicrosType.Logical.(schema.TimestampLogicalType)
		require.True(t, ok)
		require.Equal(t, schema.TimeUnitMicros, datetimeMicrosLogicalType.TimeUnit())

		datetimeMillisType := ToColumnType(&ColumnInfo{Type: "DATETIME", Precision: 23, Scale: 3})
		datetimeMillisLogicalType, ok := datetimeMillisType.Logical.(schema.TimestampLogicalType)
		require.True(t, ok)
		// we always use TimeUnitMicros.
		require.Equal(t, schema.TimeUnitMicros, datetimeMillisLogicalType.TimeUnit())

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
}

func TestDecimalFixedLengthBytesForPrecisionEdgeCases(t *testing.T) {
	require.Equal(t, -1, decimalFixedLengthBytesForPrecision(0))
	require.Equal(t, 1, decimalFixedLengthBytesForPrecision(2))
}
