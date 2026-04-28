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
	"fmt"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
)

func buildParquetSchemaFromColumns(columns []*ColumnInfo) (*schema.GroupNode, []column, error) {
	fields := make([]schema.Node, 0, len(columns))
	parsedColumns := make([]column, 0, len(columns))
	for _, columnInfo := range columns {
		if columnInfo == nil {
			return nil, nil, fmt.Errorf("parquet column info is nil")
		}
		if columnInfo.Name == "" {
			return nil, nil, fmt.Errorf("parquet column name is empty")
		}

		columnType := ToColumnType(columnInfo)
		repetition := parquet.Repetitions.Required
		allowsNullEncoding := columnInfo.Nullable
		// TIMESTAMP and DATETIME can carry invalid MySQL values. Preserve the
		// previous behavior by writing those invalid values as NULL.
		if _, ok := columnType.Logical.(schema.TimestampLogicalType); ok {
			allowsNullEncoding = true
		}
		if allowsNullEncoding {
			repetition = parquet.Repetitions.Optional
		}

		timestampUnit := timestampUnitFromLogicalType(columnType.Logical)
		parsedColumn := column{
			ColumnInfo:         *columnInfo,
			ColumnType:         columnType,
			Repetition:         repetition,
			allowsNullEncoding: allowsNullEncoding,
			timestampUnit:      timestampUnit,
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
