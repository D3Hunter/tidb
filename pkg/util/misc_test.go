// Copyright 2016 PingCAP, Inc.
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

package util

import (
	"bytes"
	"crypto/x509/pkix"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/fastrand"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/stretchr/testify/assert"
)

func TestRunWithRetry(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		cnt := 0
		err := RunWithRetry(3, 1, func() (bool, error) {
			cnt++
			if cnt < 2 {
				return true, errors.New("err")
			}
			return true, nil
		})
		assert.Nil(t, err)
		assert.Equal(t, 2, cnt)
	})

	t.Run("retry exceeds", func(t *testing.T) {
		cnt := 0
		err := RunWithRetry(3, 1, func() (bool, error) {
			cnt++
			if cnt < 4 {
				return true, errors.New("err")
			}
			return true, nil
		})
		assert.NotNil(t, err)
		assert.Equal(t, 3, cnt)
	})

	t.Run("failed result", func(t *testing.T) {
		cnt := 0
		err := RunWithRetry(3, 1, func() (bool, error) {
			cnt++
			if cnt < 2 {
				return false, errors.New("err")
			}
			return true, nil
		})
		assert.NotNil(t, err)
		assert.Equal(t, 1, cnt)
	})
}

func TestX509NameParseMatch(t *testing.T) {
	assert.Equal(t, "", X509NameOnline(pkix.Name{}))

	check := pkix.Name{
		Names: []pkix.AttributeTypeAndValue{
			MockPkixAttribute(Country, "SE"),
			MockPkixAttribute(Province, "Stockholm2"),
			MockPkixAttribute(Locality, "Stockholm"),
			MockPkixAttribute(Organization, "MySQL demo client certificate"),
			MockPkixAttribute(OrganizationalUnit, "testUnit"),
			MockPkixAttribute(CommonName, "client"),
			MockPkixAttribute(Email, "client@example.com"),
		},
	}
	result := "/C=SE/ST=Stockholm2/L=Stockholm/O=MySQL demo client certificate/OU=testUnit/CN=client/emailAddress=client@example.com"
	assert.Equal(t, result, X509NameOnline(check))
}

func TestBasicFuncWithRecovery(t *testing.T) {
	var recovery any
	WithRecovery(func() {
		panic("test")
	}, func(r any) {
		recovery = r
	})
	assert.Equal(t, "test", recovery)
}

func TestBasicFuncSyntaxError(t *testing.T) {
	assert.Nil(t, SyntaxError(nil))
	assert.True(t, terror.ErrorEqual(SyntaxError(errors.New("test")), parser.ErrParse))
	assert.True(t, terror.ErrorEqual(SyntaxError(parser.ErrSyntax.GenWithStackByArgs()), parser.ErrSyntax))
}

func TestBasicFuncSyntaxWarn(t *testing.T) {
	assert.Nil(t, SyntaxWarn(nil))
	assert.True(t, terror.ErrorEqual(SyntaxWarn(errors.New("test")), parser.ErrParse))
}

func TestBasicFuncProcessInfo(t *testing.T) {
	sc := stmtctx.NewStmtCtx()
	sc.MemTracker = memory.NewTracker(-1, -1)
	pi := sessmgr.ProcessInfo{
		ID:      1,
		User:    "test",
		Host:    "www",
		DB:      "db",
		Command: mysql.ComSleep,
		Plan:    nil,
		Time:    time.Now(),
		State:   3,
		Info:    "test",
		StmtCtx: sc,
	}
	row := pi.ToRowForShow(false)
	row2 := pi.ToRowForShow(true)
	assert.Equal(t, row2, row)
	assert.Len(t, row, 8)
	assert.Equal(t, pi.ID, row[0])
	assert.Equal(t, pi.User, row[1])
	assert.Equal(t, pi.Host, row[2])
	assert.Equal(t, pi.DB, row[3])
	assert.Equal(t, "Sleep", row[4])
	assert.Equal(t, uint64(0), row[5])
	assert.Equal(t, "in transaction; autocommit", row[6])
	assert.Equal(t, "test", row[7])

	row3 := pi.ToRow(time.UTC)
	assert.Equal(t, row, row3[:8])
	assert.Equal(t, int64(0), row3[9])
}

func TestBasicFuncRandomBuf(t *testing.T) {
	buf := fastrand.Buf(5)
	assert.Len(t, buf, 5)
	assert.False(t, bytes.Contains(buf, []byte("$")))
	assert.False(t, bytes.Contains(buf, []byte{0}))
}

func TestToPB(t *testing.T) {
	column := &model.ColumnInfo{
		ID:           1,
		Name:         ast.NewCIStr("c"),
		Offset:       0,
		DefaultValue: 0,
		FieldType:    *types.NewFieldType(0),
		Hidden:       true,
	}
	column.SetCollate("utf8mb4_general_ci")

	column2 := &model.ColumnInfo{
		ID:           1,
		Name:         ast.NewCIStr("c"),
		Offset:       0,
		DefaultValue: 0,
		FieldType:    *types.NewFieldType(0),
		Hidden:       true,
	}
	column2.SetCollate("utf8mb4_bin")

	assert.Equal(t, "column_id:1 collation:-45 columnLen:-1 decimal:-1 ", ColumnToProto(column, false, false).String())
	assert.Equal(t, "column_id:1 collation:-45 columnLen:-1 decimal:-1 ", ColumnsToProto([]*model.ColumnInfo{column, column2}, false, false, false)[0].String())
}

func TestComposeURL(t *testing.T) {
	// TODO Setup config for TLS and verify https protocol output
	assert.Equal(t, ComposeURL("server.example.com", ""), "http://server.example.com")
	assert.Equal(t, ComposeURL("httpserver.example.com", ""), "http://httpserver.example.com")
	assert.Equal(t, ComposeURL("http://httpserver.example.com", "/"), "http://httpserver.example.com/")
	assert.Equal(t, ComposeURL("https://httpserver.example.com", "/api/test"), "https://httpserver.example.com/api/test")
	assert.Equal(t, ComposeURL("http://server.example.com", ""), "http://server.example.com")
	assert.Equal(t, ComposeURL("https://server.example.com", ""), "https://server.example.com")
}
