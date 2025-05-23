// Copyright 2023 PingCAP, Inc.
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

package ddl_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
)

func TestMultiValuedIndexOnlineDDL(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (pk int primary key, a json) partition by hash(pk) partitions 32;")
	var sb strings.Builder
	sb.WriteString("insert into t values ")
	for i := range 100 {
		sb.WriteString(fmt.Sprintf("(%d, '[%d, %d, %d]')", i, i+1, i+2, i+3))
		if i != 99 {
			sb.WriteString(",")
		}
	}
	tk.MustExec(sb.String())

	internalTK := testkit.NewTestKit(t, store)
	internalTK.MustExec("use test")

	n := 100
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		internalTK.MustExec(fmt.Sprintf("insert into t values (%d, '[%d, %d, %d]')", n, n, n+1, n+2))
		internalTK.MustExec(fmt.Sprintf("delete from t where pk = %d", n-4))
		internalTK.MustExec(fmt.Sprintf("update t set a = '[%d, %d, %d]' where pk = %d", n-3, n-2, n+1000, n-3))
		n++
	})

	tk.MustExec("alter table t add index idx((cast(a as signed array)))")
	tk.MustExec("admin check table t")
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (pk int primary key, a json);")
	tk.MustExec("insert into t values (1, '[1,2,3]');")
	tk.MustExec("insert into t values (2, '[2,3,4]');")
	tk.MustExec("insert into t values (3, '[3,4,5]');")
	tk.MustExec("insert into t values (4, '[-4,5,6]');")
	tk.MustGetErrCode("alter table t add unique index idx((cast(a as signed array)));", errno.ErrDupEntry)
	tk.MustGetErrMsg("alter table t add index idx((cast(a as unsigned array)));", "[types:1690]constant -4 overflows bigint")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (pk int primary key, a json);")
	tk.MustExec("insert into t values (1, '[1,2,3]');")
	tk.MustExec("insert into t values (2, '[2,3]');")
	tk.MustGetErrCode("alter table t add unique index idx((cast(a as signed array)));", errno.ErrDupEntry)
}
