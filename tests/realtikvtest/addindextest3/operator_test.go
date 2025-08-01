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

package addindextest

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ngaut/pools"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/copr"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/ddl/testutil"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/disttask/operator"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func init() {
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Path = "127.0.0.1:2379"
	})
}

func getRealAddIndexJob(t *testing.T, tk *testkit.TestKit) *model.Job {
	tk.MustExec("use test;")
	tk.MustExec("create table t (a int);")
	var realJob *model.Job
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced", func(job *model.Job) {
		if job.State == model.JobStateDone && job.Type == model.ActionAddIndex {
			realJob = job.Clone()
		}
	})
	tk.MustExec("alter table t add index idx(a);")
	require.NotNil(t, realJob)
	return realJob
}

func TestBackfillOperators(t *testing.T) {
	store, dom := realtikvtest.CreateMockStoreAndDomainAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	realJob := getRealAddIndexJob(t, tk)
	regionCnt := 10
	tbl, idxInfo, startKey, endKey, copCtx := prepare(t, tk, dom, regionCnt)
	sessPool := newSessPoolForTest(t, store)

	// Test TableScanTaskSource operator.
	var opTasks []ddl.TableScanTask
	{
		ctx := context.Background()
		opCtx, cancel := ddl.NewDistTaskOperatorCtx(ctx)
		pTbl := tbl.(table.PhysicalTable)
		src := ddl.NewTableScanTaskSource(opCtx, store, pTbl, startKey, endKey, nil)
		sink := testutil.NewOperatorTestSink[ddl.TableScanTask]()

		operator.Compose[ddl.TableScanTask](src, sink)

		pipeline := operator.NewAsyncPipeline(src, sink)
		err := pipeline.Execute()
		require.NoError(t, err)
		err = pipeline.Close()
		require.NoError(t, err)

		tasks := sink.Collect()
		require.Len(t, tasks, 10)
		require.Equal(t, 0, tasks[0].ID)
		require.Equal(t, startKey, tasks[0].Start)
		require.Equal(t, endKey, tasks[9].End)

		cancel()
		require.NoError(t, opCtx.OperatorErr())

		opTasks = tasks
	}

	// Test TableScanOperator.
	var chunkResults []ddl.IndexRecordChunk
	{
		// Make sure the buffer is large enough since the chunks do not recycled.
		srcChkPool := &sync.Pool{
			New: func() any {
				return chunk.NewChunkWithCapacity(copCtx.GetBase().FieldTypes, 100)
			},
		}

		ctx := context.Background()
		opCtx, cancel := ddl.NewDistTaskOperatorCtx(ctx)
		src := testutil.NewOperatorTestSource(opTasks...)
		scanOp := ddl.NewTableScanOperator(opCtx, sessPool, copCtx, srcChkPool, 3, 0, nil, nil)
		sink := testutil.NewOperatorTestSink[ddl.IndexRecordChunk]()

		operator.Compose[ddl.TableScanTask](src, scanOp)
		operator.Compose[ddl.IndexRecordChunk](scanOp, sink)

		pipeline := operator.NewAsyncPipeline(src, scanOp, sink)
		err := pipeline.Execute()
		require.NoError(t, err)
		err = pipeline.Close()
		require.NoError(t, err)

		results := sink.Collect()
		cnt := 0
		for _, rs := range results {
			require.NoError(t, rs.Err)
			chkRowCnt := rs.Chunk.NumRows()
			cnt += chkRowCnt
			if chkRowCnt > 0 {
				chunkResults = append(chunkResults, rs)
			}
		}
		require.Equal(t, 10, cnt)

		cancel()
		require.NoError(t, opCtx.OperatorErr())
	}

	// Test IndexIngestOperator.
	{
		ctx := context.Background()
		opCtx, cancel := ddl.NewDistTaskOperatorCtx(ctx)
		var keys, values [][]byte
		onWrite := func(key, val []byte) {
			keys = append(keys, key)
			values = append(values, val)
		}

		srcChkPool := &sync.Pool{
			New: func() any {
				return chunk.NewChunkWithCapacity(copCtx.GetBase().FieldTypes, 100)
			},
		}
		pTbl := tbl.(table.PhysicalTable)
		index := tables.NewIndex(pTbl.GetPhysicalID(), tbl.Meta(), idxInfo)
		cfg, bd, err := ingest.CreateLocalBackend(context.Background(), store, realJob, false, 0)
		require.NoError(t, err)
		defer bd.Close()
		bcCtx, err := ingest.NewBackendCtxBuilder(ctx, store, realJob).Build(cfg, bd)
		require.NoError(t, err)
		defer bcCtx.Close()
		mockEngine := ingest.NewMockEngineInfo(nil)
		mockEngine.SetHook(onWrite)

		src := testutil.NewOperatorTestSource(chunkResults...)
		reorgMeta := ddl.NewDDLReorgMeta(tk.Session())
		ingestOp := ddl.NewIndexIngestOperator(
			opCtx, copCtx, sessPool, pTbl, []table.Index{index}, []ingest.Engine{mockEngine},
			srcChkPool, 3, reorgMeta)
		sink := testutil.NewOperatorTestSink[ddl.IndexWriteResult]()

		operator.Compose[ddl.IndexRecordChunk](src, ingestOp)
		operator.Compose[ddl.IndexWriteResult](ingestOp, sink)

		pipeline := operator.NewAsyncPipeline(src, ingestOp, sink)
		err = pipeline.Execute()
		require.NoError(t, err)
		err = pipeline.Close()
		require.NoError(t, err)

		results := sink.Collect()
		cnt := 0
		for _, rs := range results {
			cnt += rs.Added
		}
		require.Len(t, keys, 10)
		require.Len(t, values, 10)
		require.Equal(t, 10, cnt)

		cancel()
		require.NoError(t, opCtx.OperatorErr())
	}
}

func TestBackfillOperatorPipeline(t *testing.T) {
	store, dom := realtikvtest.CreateMockStoreAndDomainAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	realJob := getRealAddIndexJob(t, tk)
	regionCnt := 10
	tbl, idxInfo, startKey, endKey, _ := prepare(t, tk, dom, regionCnt)
	sessPool := newSessPoolForTest(t, store)

	ctx := context.Background()
	opCtx, cancel := ddl.NewDistTaskOperatorCtx(ctx)
	defer cancel()
	cfg, bd, err := ingest.CreateLocalBackend(context.Background(), store, realJob, false, 0)
	require.NoError(t, err)
	defer bd.Close()
	bcCtx, err := ingest.NewBackendCtxBuilder(ctx, store, realJob).Build(cfg, bd)
	require.NoError(t, err)
	defer bcCtx.Close()
	mockEngine := ingest.NewMockEngineInfo(nil)
	mockEngine.SetHook(func(key, val []byte) {})

	pipeline, err := ddl.NewAddIndexIngestPipeline(
		opCtx, store,
		sessPool,
		bcCtx,
		[]ingest.Engine{mockEngine},
		1, // job id
		tbl.(table.PhysicalTable),
		[]*model.IndexInfo{idxInfo},
		startKey,
		endKey,
		ddl.NewDDLReorgMeta(tk.Session()),
		0,
		2,
		&execute.TestCollector{},
	)
	require.NoError(t, err)
	err = pipeline.Execute()
	require.NoError(t, err)
	err = pipeline.Close()
	require.NoError(t, err)

	require.NoError(t, opCtx.OperatorErr())
}

func TestBackfillOperatorPipelineException(t *testing.T) {
	store, dom := realtikvtest.CreateMockStoreAndDomainAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	realJob := getRealAddIndexJob(t, tk)
	regionCnt := 10
	tbl, idxInfo, startKey, endKey, _ := prepare(t, tk, dom, regionCnt)
	sessPool := newSessPoolForTest(t, store)
	cfg, bd, err := ingest.CreateLocalBackend(context.Background(), store, realJob, false, 0)
	require.NoError(t, err)
	defer bd.Close()
	bcCtx, err := ingest.NewBackendCtxBuilder(context.Background(), store, realJob).Build(cfg, bd)
	require.NoError(t, err)
	defer bcCtx.Close()
	mockEngine := ingest.NewMockEngineInfo(nil)
	mockEngine.SetHook(func(_, _ []byte) {})

	testCase := []struct {
		failPointPath  string
		closeErrMsg    string
		operatorErrMsg string
	}{
		{
			failPointPath:  "github.com/pingcap/tidb/pkg/ddl/mockScanRecordError",
			closeErrMsg:    "context canceled",
			operatorErrMsg: "mock scan record error",
		},
		{
			failPointPath:  "github.com/pingcap/tidb/pkg/ddl/scanRecordExec",
			closeErrMsg:    "context canceled",
			operatorErrMsg: "context canceled",
		},
		{
			failPointPath:  "github.com/pingcap/tidb/pkg/ddl/mockWriteLocalError",
			closeErrMsg:    "context canceled",
			operatorErrMsg: "mock write local error",
		},
		{
			failPointPath:  "github.com/pingcap/tidb/pkg/ddl/writeLocalExec",
			closeErrMsg:    "context canceled",
			operatorErrMsg: "",
		},
		{
			failPointPath:  "github.com/pingcap/tidb/pkg/ddl/mockFlushError",
			closeErrMsg:    "mock flush error",
			operatorErrMsg: "mock flush error",
		},
	}

	for _, tc := range testCase {
		t.Run(tc.failPointPath, func(t *testing.T) {
			defer func() {
				require.NoError(t, failpoint.Disable(tc.failPointPath))
			}()
			ctx, cancel := context.WithCancel(context.Background())
			if strings.Contains(tc.failPointPath, "writeLocalExec") {
				var counter atomic.Int32
				require.NoError(t, failpoint.EnableCall(tc.failPointPath, func(done bool) {
					if !done {
						return
					}
					// we need to want all tableScanWorkers finish scanning, else
					// fetchTableScanResult will might return context error, and cause
					// the case fail.
					// 10 is the table scan task count.
					counter.Add(1)
					if counter.Load() == 10 {
						cancel()
					}
				}))
			} else if strings.Contains(tc.failPointPath, "scanRecordExec") {
				require.NoError(t, failpoint.EnableCall(tc.failPointPath, func(*model.DDLReorgMeta) { cancel() }))
			} else {
				require.NoError(t, failpoint.Enable(tc.failPointPath, `return`))
			}
			opCtx, cancel := ddl.NewDistTaskOperatorCtx(ctx)
			defer cancel()
			pipeline, err := ddl.NewAddIndexIngestPipeline(
				opCtx, store,
				sessPool,
				bcCtx,
				[]ingest.Engine{mockEngine},
				1, // job id
				tbl.(table.PhysicalTable),
				[]*model.IndexInfo{idxInfo},
				startKey,
				endKey,
				ddl.NewDDLReorgMeta(tk.Session()),
				0,
				2,
				&execute.TestCollector{},
			)
			require.NoError(t, err)
			err = pipeline.Execute()
			require.NoError(t, err)
			err = pipeline.Close()
			comment := fmt.Sprintf("case: %s", tc.failPointPath)
			require.ErrorContains(t, err, tc.closeErrMsg, comment)
			if tc.operatorErrMsg == "" {
				require.NoError(t, opCtx.OperatorErr())
			} else {
				require.Error(t, opCtx.OperatorErr())
				require.ErrorContains(t, opCtx.OperatorErr(), tc.operatorErrMsg)
			}
		})
	}
}

func prepare(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, regionCnt int) (
	tbl table.Table, idxInfo *model.IndexInfo, start, end kv.Key, copCtx copr.CopContext) {
	tk.MustExec("drop database if exists op;")
	tk.MustExec("create database op;")
	tk.MustExec("use op;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)

	tk.MustExec("create table t(a int primary key, b int, index idx(b));")
	for i := range regionCnt {
		tk.MustExec("insert into t values (?, ?)", i*10000, i)
	}
	maxRowID := regionCnt * 10000
	tk.MustQuery(fmt.Sprintf("split table t between (0) and (%d) regions %d;", maxRowID, regionCnt)).
		Check(testkit.Rows(fmt.Sprintf("%d 1", regionCnt)))
	// Refresh the region cache.
	tk.MustQuery("select count(*) from t;").Check(testkit.Rows(fmt.Sprintf("%d", regionCnt)))

	var err error
	tbl, err = dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("op"), ast.NewCIStr("t"))
	require.NoError(t, err)
	start = tbl.RecordPrefix()
	end = tbl.RecordPrefix().PrefixNext()

	tblInfo := tbl.Meta()
	idxInfo = tblInfo.FindIndexByName("idx")
	sctx := tk.Session()
	copCtx, err = ddl.NewReorgCopContext(dom.Store(), ddl.NewDDLReorgMeta(sctx), tblInfo, []*model.IndexInfo{idxInfo}, "")
	require.NoError(t, err)
	require.IsType(t, copCtx, &copr.CopContextSingleIndex{})
	return tbl, idxInfo, start, end, copCtx
}

type sessPoolForTest struct {
	pool *pools.ResourcePool
}

func newSessPoolForTest(t *testing.T, store kv.Storage) *sessPoolForTest {
	return &sessPoolForTest{
		pool: pools.NewResourcePool(func() (pools.Resource, error) {
			newTk := testkit.NewTestKit(t, store)
			return newTk.Session(), nil
		}, 8, 8, 0),
	}
}

func (p *sessPoolForTest) Get() (sessionctx.Context, error) {
	resource, err := p.pool.Get()
	if err != nil {
		return nil, err
	}
	return resource.(sessionctx.Context), nil
}

func (p *sessPoolForTest) Put(sctx sessionctx.Context) {
	p.pool.Put(sctx.(pools.Resource))
}

func TestTuneWorkerPoolSize(t *testing.T) {
	store, dom := realtikvtest.CreateMockStoreAndDomainAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	realJob := getRealAddIndexJob(t, tk)
	tbl, idxInfo, _, _, copCtx := prepare(t, tk, dom, 10)
	sessPool := newSessPoolForTest(t, store)

	// Test TableScanOperator.
	{
		ctx := context.Background()
		opCtx, cancel := ddl.NewDistTaskOperatorCtx(ctx)
		scanOp := ddl.NewTableScanOperator(opCtx, sessPool, copCtx, nil, 2, 0, nil, nil)

		scanOp.Open()
		require.Equal(t, scanOp.GetWorkerPoolSize(), int32(2))
		scanOp.TuneWorkerPoolSize(8, false)
		require.Equal(t, scanOp.GetWorkerPoolSize(), int32(8))
		scanOp.TuneWorkerPoolSize(1, false)
		require.Equal(t, scanOp.GetWorkerPoolSize(), int32(1))

		cancel()
		require.NoError(t, opCtx.OperatorErr())
	}

	// Test IndexIngestOperator.
	{
		ctx := context.Background()
		opCtx, cancel := ddl.NewDistTaskOperatorCtx(ctx)
		pTbl := tbl.(table.PhysicalTable)
		index := tables.NewIndex(pTbl.GetPhysicalID(), tbl.Meta(), idxInfo)
		cfg, bd, err := ingest.CreateLocalBackend(context.Background(), store, realJob, false, 0)
		require.NoError(t, err)
		defer bd.Close()
		bcCtx, err := ingest.NewBackendCtxBuilder(context.Background(), store, realJob).Build(cfg, bd)
		require.NoError(t, err)
		defer bcCtx.Close()
		mockEngine := ingest.NewMockEngineInfo(nil)
		ingestOp := ddl.NewIndexIngestOperator(opCtx, copCtx, sessPool, pTbl, []table.Index{index},
			[]ingest.Engine{mockEngine}, nil, 2, nil)

		ingestOp.Open()
		require.Equal(t, ingestOp.GetWorkerPoolSize(), int32(2))
		ingestOp.TuneWorkerPoolSize(8, false)
		require.Equal(t, ingestOp.GetWorkerPoolSize(), int32(8))
		ingestOp.TuneWorkerPoolSize(1, false)
		require.Equal(t, ingestOp.GetWorkerPoolSize(), int32(1))

		cancel()
		require.NoError(t, opCtx.OperatorErr())
	}
}
