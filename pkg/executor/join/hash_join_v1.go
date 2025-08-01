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

package join

import (
	"bytes"
	"context"
	"fmt"
	"runtime/trace"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/aggregate"
	"github.com/pingcap/tidb/pkg/executor/internal/applycache"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/unionexec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/bitmap"
	"github.com/pingcap/tidb/pkg/util/channel"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"go.uber.org/zap"
)

// IsChildCloseCalledForTest is used for test
var IsChildCloseCalledForTest = false

var (
	_ exec.Executor = &HashJoinV1Exec{}
	_ exec.Executor = &NestedLoopApplyExec{}
)

// HashJoinCtxV1 is the context used in hash join
type HashJoinCtxV1 struct {
	hashJoinCtxBase
	UseOuterToBuild    bool
	IsOuterJoin        bool
	RowContainer       *hashRowContainer
	outerMatchedStatus []*bitmap.ConcurrentBitmap
	ProbeTypes         []*types.FieldType
	BuildTypes         []*types.FieldType
	OuterFilter        expression.CNFExprs
	stats              *hashJoinRuntimeStats
}

// ProbeSideTupleFetcherV1 reads tuples from ProbeSideExec and send them to ProbeWorkers.
type ProbeSideTupleFetcherV1 struct {
	probeSideTupleFetcherBase
	*HashJoinCtxV1
}

// ProbeWorkerV1 is the probe side worker in hash join
type ProbeWorkerV1 struct {
	probeWorkerBase
	HashJoinCtx      *HashJoinCtxV1
	ProbeKeyColIdx   []int
	ProbeNAKeyColIdx []int
	// We pre-alloc and reuse the Rows and RowPtrs for each probe goroutine, to avoid allocation frequently
	buildSideRows    []chunk.Row
	buildSideRowPtrs []chunk.RowPtr

	// We build individual joiner for each join worker when use chunk-based
	// execution, to avoid the concurrency of joiner.chk and joiner.selected.
	Joiner               Joiner
	rowIters             *chunk.Iterator4Slice
	rowContainerForProbe *hashRowContainer
	// for every naaj probe worker,  pre-allocate the int slice for store the join column index to check.
	needCheckBuildColPos []int
	needCheckProbeColPos []int
	needCheckBuildTypes  []*types.FieldType
	needCheckProbeTypes  []*types.FieldType
}

// BuildWorkerV1 is the build side worker in hash join
type BuildWorkerV1 struct {
	buildWorkerBase
	HashJoinCtx      *HashJoinCtxV1
	BuildNAKeyColIdx []int
}

// HashJoinV1Exec implements the hash join algorithm.
type HashJoinV1Exec struct {
	exec.BaseExecutor
	*HashJoinCtxV1

	ProbeSideTupleFetcher *ProbeSideTupleFetcherV1
	ProbeWorkers          []*ProbeWorkerV1
	BuildWorker           *BuildWorkerV1

	workerWg util.WaitGroupWrapper
	waiterWg util.WaitGroupWrapper

	Prepared bool
}

// Close implements the Executor Close interface.
func (e *HashJoinV1Exec) Close() error {
	if e.closeCh != nil {
		close(e.closeCh)
	}
	e.finished.Store(true)
	if e.Prepared {
		if e.buildFinished != nil {
			channel.Clear(e.buildFinished)
		}
		if e.joinResultCh != nil {
			channel.Clear(e.joinResultCh)
		}
		if e.ProbeSideTupleFetcher.probeChkResourceCh != nil {
			close(e.ProbeSideTupleFetcher.probeChkResourceCh)
			channel.Clear(e.ProbeSideTupleFetcher.probeChkResourceCh)
		}
		for i := range e.ProbeSideTupleFetcher.probeResultChs {
			channel.Clear(e.ProbeSideTupleFetcher.probeResultChs[i])
		}
		for i := range e.ProbeWorkers {
			close(e.ProbeWorkers[i].joinChkResourceCh)
			channel.Clear(e.ProbeWorkers[i].joinChkResourceCh)
		}
		e.ProbeSideTupleFetcher.probeChkResourceCh = nil
		util.WithRecovery(func() {
			err := e.RowContainer.Close()
			if err != nil {
				logutil.BgLogger().Warn("RowContainer encounters error",
					zap.Error(err),
					zap.Stack("stack trace"))
			}
		}, nil)
		e.HashJoinCtxV1.SessCtx.GetSessionVars().MemTracker.UnbindActionFromHardLimit(e.RowContainer.ActionSpill())
		e.waiterWg.Wait()
	}
	e.outerMatchedStatus = e.outerMatchedStatus[:0]
	for _, w := range e.ProbeWorkers {
		w.buildSideRows = nil
		w.buildSideRowPtrs = nil
		w.needCheckBuildColPos = nil
		w.needCheckProbeColPos = nil
		w.needCheckBuildTypes = nil
		w.needCheckProbeTypes = nil
		w.joinChkResourceCh = nil
	}

	if e.stats != nil && e.RowContainer != nil {
		e.stats.hashStat = *e.RowContainer.stat
	}
	if e.stats != nil {
		defer e.Ctx().GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.ID(), e.stats)
	}

	IsChildCloseCalledForTest = true
	return e.BaseExecutor.Close()
}

// Open implements the Executor Open interface.
func (e *HashJoinV1Exec) Open(ctx context.Context) error {
	if err := e.BaseExecutor.Open(ctx); err != nil {
		e.closeCh = nil
		e.Prepared = false
		return err
	}
	return e.OpenSelf()
}

// OpenSelf opens join itself and initializes the hash join context.
func (e *HashJoinV1Exec) OpenSelf() error {
	e.Prepared = false
	if e.HashJoinCtxV1.memTracker != nil {
		e.HashJoinCtxV1.memTracker.Reset()
	} else {
		e.HashJoinCtxV1.memTracker = memory.NewTracker(e.ID(), -1)
	}
	e.HashJoinCtxV1.memTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.MemTracker)

	if e.HashJoinCtxV1.diskTracker != nil {
		e.HashJoinCtxV1.diskTracker.Reset()
	} else {
		e.HashJoinCtxV1.diskTracker = disk.NewTracker(e.ID(), -1)
	}
	e.HashJoinCtxV1.diskTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.DiskTracker)

	e.workerWg = util.WaitGroupWrapper{}
	e.waiterWg = util.WaitGroupWrapper{}
	e.closeCh = make(chan struct{})
	e.finished.Store(false)

	if e.RuntimeStats() != nil {
		e.stats = &hashJoinRuntimeStats{
			concurrent: int(e.Concurrency),
		}
	}
	return nil
}

func (e *HashJoinV1Exec) initializeForProbe() {
	e.ProbeSideTupleFetcher.HashJoinCtxV1 = e.HashJoinCtxV1
	// e.joinResultCh is for transmitting the join result chunks to the main
	// thread.
	e.joinResultCh = make(chan *hashjoinWorkerResult, e.Concurrency+1)
	e.ProbeSideTupleFetcher.initializeForProbeBase(e.Concurrency, e.joinResultCh)

	for i := range e.Concurrency {
		e.ProbeWorkers[i].initializeForProbe(e.ProbeSideTupleFetcher.probeChkResourceCh, e.ProbeSideTupleFetcher.probeResultChs[i], e)
	}
}

func (e *HashJoinV1Exec) fetchAndProbeHashTable(ctx context.Context) {
	e.initializeForProbe()
	e.workerWg.RunWithRecover(func() {
		defer trace.StartRegion(ctx, "HashJoinProbeSideFetcher").End()
		e.ProbeSideTupleFetcher.fetchProbeSideChunks(
			ctx,
			e.MaxChunkSize(),
			func() bool {
				return e.ProbeSideTupleFetcher.RowContainer.Len() == uint64(0)
			},
			func() bool { return false },
			e.ProbeSideTupleFetcher.JoinType == logicalop.InnerJoin || e.ProbeSideTupleFetcher.JoinType == logicalop.SemiJoin,
			false,
			e.ProbeSideTupleFetcher.IsOuterJoin,
			&e.ProbeSideTupleFetcher.hashJoinCtxBase)
	}, e.ProbeSideTupleFetcher.handleProbeSideFetcherPanic)

	for i := range e.Concurrency {
		workerID := i
		e.workerWg.RunWithRecover(func() {
			defer trace.StartRegion(ctx, "HashJoinWorker").End()
			e.ProbeWorkers[workerID].runJoinWorker()
		}, e.ProbeWorkers[workerID].handleProbeWorkerPanic)
	}
	e.waiterWg.RunWithRecover(e.waitJoinWorkersAndCloseResultChan, nil)
}

func (w *ProbeWorkerV1) handleProbeWorkerPanic(r any) {
	if r != nil {
		w.HashJoinCtx.joinResultCh <- &hashjoinWorkerResult{err: util.GetRecoverError(r)}
	}
}

func (e *HashJoinV1Exec) handleJoinWorkerPanic(r any) {
	if r != nil {
		e.joinResultCh <- &hashjoinWorkerResult{err: util.GetRecoverError(r)}
	}
}

// Concurrently handling unmatched rows from the hash table
func (w *ProbeWorkerV1) handleUnmatchedRowsFromHashTable() {
	ok, joinResult := w.getNewJoinResult()
	if !ok {
		return
	}
	numChks := w.rowContainerForProbe.NumChunks()
	for i := int(w.WorkerID); i < numChks; i += int(w.HashJoinCtx.Concurrency) {
		chk, err := w.rowContainerForProbe.GetChunk(i)
		if err != nil {
			// Catching the error and send it
			joinResult.err = err
			w.HashJoinCtx.joinResultCh <- joinResult
			return
		}
		for j := range chk.NumRows() {
			if !w.HashJoinCtx.outerMatchedStatus[i].UnsafeIsSet(j) { // process unmatched Outer rows
				w.Joiner.OnMissMatch(false, chk.GetRow(j), joinResult.chk)
			}
			if joinResult.chk.IsFull() {
				w.HashJoinCtx.joinResultCh <- joinResult
				ok, joinResult = w.getNewJoinResult()
				if !ok {
					return
				}
			}
		}
	}

	if joinResult == nil {
		return
	} else if joinResult.err != nil || (joinResult.chk != nil && joinResult.chk.NumRows() > 0) {
		w.HashJoinCtx.joinResultCh <- joinResult
	}
}

func (e *HashJoinV1Exec) waitJoinWorkersAndCloseResultChan() {
	e.workerWg.Wait()
	if e.UseOuterToBuild {
		// Concurrently handling unmatched rows from the hash table at the tail
		for i := range e.Concurrency {
			var workerID = i
			e.workerWg.RunWithRecover(func() { e.ProbeWorkers[workerID].handleUnmatchedRowsFromHashTable() }, e.handleJoinWorkerPanic)
		}
		e.workerWg.Wait()
	}
	close(e.joinResultCh)
}

func (w *ProbeWorkerV1) runJoinWorker() {
	probeTime := int64(0)
	if w.HashJoinCtx.stats != nil {
		start := time.Now()
		defer func() {
			t := time.Since(start)
			atomic.AddInt64(&w.HashJoinCtx.stats.probe, probeTime)
			atomic.AddInt64(&w.HashJoinCtx.stats.fetchAndProbe, int64(t))
			setMaxValue(&w.HashJoinCtx.stats.maxFetchAndProbe, int64(t))
		}()
	}

	var (
		probeSideResult *chunk.Chunk
		selected        = make([]bool, 0, chunk.InitialCapacity)
	)
	ok, joinResult := w.getNewJoinResult()
	if !ok {
		return
	}

	// Read and filter probeSideResult, and join the probeSideResult with the build side rows.
	emptyProbeSideResult := &probeChkResource{
		dest: w.probeResultCh,
	}
	hCtx := &HashContext{
		AllTypes:    w.HashJoinCtx.ProbeTypes,
		KeyColIdx:   w.ProbeKeyColIdx,
		NaKeyColIdx: w.ProbeNAKeyColIdx,
	}
	for ok := true; ok; {
		if w.HashJoinCtx.finished.Load() {
			break
		}
		select {
		case <-w.HashJoinCtx.closeCh:
			return
		case probeSideResult, ok = <-w.probeResultCh:
		}
		failpoint.Inject("ConsumeRandomPanic", nil)
		if !ok {
			break
		}
		start := time.Now()
		// waitTime is the time cost on w.sendingResult(), it should not be added to probe time, because if
		// parent executor does not call `e.Next()`, `sendingResult()` will hang, and this hang has nothing to do
		// with the probe
		waitTime := int64(0)
		if w.HashJoinCtx.UseOuterToBuild {
			ok, waitTime, joinResult = w.join2ChunkForOuterHashJoin(probeSideResult, hCtx, joinResult)
		} else {
			ok, waitTime, joinResult = w.join2Chunk(probeSideResult, hCtx, joinResult, selected)
		}
		probeTime += int64(time.Since(start)) - waitTime
		if !ok {
			break
		}
		probeSideResult.Reset()
		emptyProbeSideResult.chk = probeSideResult
		w.probeChkResourceCh <- emptyProbeSideResult
	}
	// note joinResult.chk may be nil when getNewJoinResult fails in loops
	if joinResult == nil {
		return
	} else if joinResult.err != nil || (joinResult.chk != nil && joinResult.chk.NumRows() > 0) {
		w.HashJoinCtx.joinResultCh <- joinResult
	} else if joinResult.chk != nil && joinResult.chk.NumRows() == 0 {
		w.joinChkResourceCh <- joinResult.chk
	}
}

func (w *ProbeWorkerV1) joinMatchedProbeSideRow2ChunkForOuterHashJoin(probeKey uint64, probeSideRow chunk.Row, hCtx *HashContext, joinResult *hashjoinWorkerResult) (bool, int64, *hashjoinWorkerResult) {
	var err error
	waitTime := int64(0)
	oneWaitTime := int64(0)
	w.buildSideRows, w.buildSideRowPtrs, err = w.rowContainerForProbe.GetMatchedRowsAndPtrs(probeKey, probeSideRow, hCtx, w.buildSideRows, w.buildSideRowPtrs, true)
	buildSideRows, rowsPtrs := w.buildSideRows, w.buildSideRowPtrs
	if err != nil {
		joinResult.err = err
		return false, waitTime, joinResult
	}
	if len(buildSideRows) == 0 {
		return true, waitTime, joinResult
	}

	iter := w.rowIters
	iter.Reset(buildSideRows)
	var outerMatchStatus []outerRowStatusFlag
	rowIdx, ok := 0, false
	for iter.Begin(); iter.Current() != iter.End(); {
		outerMatchStatus, err = w.Joiner.TryToMatchOuters(iter, probeSideRow, joinResult.chk, outerMatchStatus)
		if err != nil {
			joinResult.err = err
			return false, waitTime, joinResult
		}
		for i := range outerMatchStatus {
			if outerMatchStatus[i] == outerRowMatched {
				w.HashJoinCtx.outerMatchedStatus[rowsPtrs[rowIdx+i].ChkIdx].Set(int(rowsPtrs[rowIdx+i].RowIdx))
			}
		}
		rowIdx += len(outerMatchStatus)
		if joinResult.chk.IsFull() {
			ok, oneWaitTime, joinResult = w.sendingResult(joinResult)
			waitTime += oneWaitTime
			if !ok {
				return false, waitTime, joinResult
			}
		}
	}
	return true, waitTime, joinResult
}

// joinNAALOSJMatchProbeSideRow2Chunk implement the matching logic for NA-AntiLeftOuterSemiJoin
func (w *ProbeWorkerV1) joinNAALOSJMatchProbeSideRow2Chunk(probeKey uint64, probeKeyNullBits *bitmap.ConcurrentBitmap, probeSideRow chunk.Row, hCtx *HashContext, joinResult *hashjoinWorkerResult) (bool, int64, *hashjoinWorkerResult) {
	var (
		err error
		ok  bool
	)
	waitTime := int64(0)
	oneWaitTime := int64(0)
	if probeKeyNullBits == nil {
		// step1: match the same key bucket first.
		// because AntiLeftOuterSemiJoin cares about the scalar value. If we both have a match from null
		// bucket and same key bucket, we should return the result as <rhs-row, 0> from same-key bucket
		// rather than <rhs-row, null> from null bucket.
		w.buildSideRows, err = w.rowContainerForProbe.GetMatchedRows(probeKey, probeSideRow, hCtx, w.buildSideRows)
		buildSideRows := w.buildSideRows
		if err != nil {
			joinResult.err = err
			return false, waitTime, joinResult
		}
		if len(buildSideRows) != 0 {
			iter1 := w.rowIters
			iter1.Reset(buildSideRows)
			for iter1.Begin(); iter1.Current() != iter1.End(); {
				matched, _, err := w.Joiner.TryToMatchInners(probeSideRow, iter1, joinResult.chk, LeftNotNullRightNotNull)
				if err != nil {
					joinResult.err = err
					return false, waitTime, joinResult
				}
				// here matched means: there is a valid same-key bucket row from right side.
				// as said in the comment, once we meet a same key (NOT IN semantic) in CNF, we can determine the result as <rhs, 0>.
				if matched {
					return true, waitTime, joinResult
				}
				if joinResult.chk.IsFull() {
					ok, oneWaitTime, joinResult = w.sendingResult(joinResult)
					waitTime += oneWaitTime
					if !ok {
						return false, waitTime, joinResult
					}
				}
			}
		}
		// step2: match the null bucket secondly.
		w.buildSideRows, err = w.rowContainerForProbe.GetNullBucketRows(hCtx, probeSideRow, probeKeyNullBits, w.buildSideRows, w.needCheckBuildColPos, w.needCheckProbeColPos, w.needCheckBuildTypes, w.needCheckProbeTypes)
		buildSideRows = w.buildSideRows
		if err != nil {
			joinResult.err = err
			return false, waitTime, joinResult
		}
		if len(buildSideRows) == 0 {
			// when reach here, it means we couldn't find a valid same key match from same-key bucket yet
			// and the null bucket is empty. so the result should be <rhs, 1>.
			w.Joiner.OnMissMatch(false, probeSideRow, joinResult.chk)
			return true, waitTime, joinResult
		}
		iter2 := w.rowIters
		iter2.Reset(buildSideRows)
		for iter2.Begin(); iter2.Current() != iter2.End(); {
			matched, _, err := w.Joiner.TryToMatchInners(probeSideRow, iter2, joinResult.chk, LeftNotNullRightHasNull)
			if err != nil {
				joinResult.err = err
				return false, waitTime, joinResult
			}
			// here matched means: there is a valid null bucket row from right side.
			// as said in the comment, once we meet a null in CNF, we can determine the result as <rhs, null>.
			if matched {
				return true, waitTime, joinResult
			}
			if joinResult.chk.IsFull() {
				ok, oneWaitTime, joinResult = w.sendingResult(joinResult)
				waitTime += oneWaitTime
				if !ok {
					return false, waitTime, joinResult
				}
			}
		}
		// step3: if we couldn't return it quickly in null bucket and same key bucket, here means two cases:
		// case1: x NOT IN (empty set): if other key bucket don't have the valid rows yet.
		// case2: x NOT IN (l,m,n...): if other key bucket do have the valid rows.
		// both cases mean the result should be <rhs, 1>
		w.Joiner.OnMissMatch(false, probeSideRow, joinResult.chk)
		return true, waitTime, joinResult
	}
	// when left side has null values, all we want is to find a valid build side rows (past other condition)
	// so we can return it as soon as possible. here means two cases:
	// case1: <?, null> NOT IN (empty set):             ----------------------> result is <rhs, 1>.
	// case2: <?, null> NOT IN (at least a valid inner row) ------------------> result is <rhs, null>.
	// Step1: match null bucket (assumption that null bucket is quite smaller than all hash table bucket rows)
	w.buildSideRows, err = w.rowContainerForProbe.GetNullBucketRows(hCtx, probeSideRow, probeKeyNullBits, w.buildSideRows, w.needCheckBuildColPos, w.needCheckProbeColPos, w.needCheckBuildTypes, w.needCheckProbeTypes)
	buildSideRows := w.buildSideRows
	if err != nil {
		joinResult.err = err
		return false, waitTime, joinResult
	}
	if len(buildSideRows) != 0 {
		iter1 := w.rowIters
		iter1.Reset(buildSideRows)
		for iter1.Begin(); iter1.Current() != iter1.End(); {
			matched, _, err := w.Joiner.TryToMatchInners(probeSideRow, iter1, joinResult.chk, LeftHasNullRightHasNull)
			if err != nil {
				joinResult.err = err
				return false, waitTime, joinResult
			}
			// here matched means: there is a valid null bucket row from right side. (not empty)
			// as said in the comment, once we found at least a valid row, we can determine the result as <rhs, null>.
			if matched {
				return true, waitTime, joinResult
			}
			if joinResult.chk.IsFull() {
				ok, oneWaitTime, joinResult = w.sendingResult(joinResult)
				waitTime += oneWaitTime
				if !ok {
					return false, waitTime, joinResult
				}
			}
		}
	}
	// Step2: match all hash table bucket build rows (use probeKeyNullBits to filter if any).
	w.buildSideRows, err = w.rowContainerForProbe.GetAllMatchedRows(hCtx, probeSideRow, probeKeyNullBits, w.buildSideRows, w.needCheckBuildColPos, w.needCheckProbeColPos, w.needCheckBuildTypes, w.needCheckProbeTypes)
	buildSideRows = w.buildSideRows
	if err != nil {
		joinResult.err = err
		return false, waitTime, joinResult
	}
	if len(buildSideRows) == 0 {
		// when reach here, it means we couldn't return it quickly in null bucket, and same-bucket is empty,
		// which means x NOT IN (empty set) or x NOT IN (l,m,n), the result should be <rhs, 1>
		w.Joiner.OnMissMatch(false, probeSideRow, joinResult.chk)
		return true, waitTime, joinResult
	}
	iter2 := w.rowIters
	iter2.Reset(buildSideRows)
	for iter2.Begin(); iter2.Current() != iter2.End(); {
		matched, _, err := w.Joiner.TryToMatchInners(probeSideRow, iter2, joinResult.chk, LeftHasNullRightNotNull)
		if err != nil {
			joinResult.err = err
			return false, waitTime, joinResult
		}
		// here matched means: there is a valid same key bucket row from right side. (not empty)
		// as said in the comment, once we found at least a valid row, we can determine the result as <rhs, null>.
		if matched {
			return true, waitTime, joinResult
		}
		if joinResult.chk.IsFull() {
			ok, oneWaitTime, joinResult = w.sendingResult(joinResult)
			waitTime += oneWaitTime
			if !ok {
				return false, waitTime, joinResult
			}
		}
	}
	// step3: if we couldn't return it quickly in null bucket and all hash bucket, here means only one cases:
	// case1: <?, null> NOT IN (empty set):
	// empty set comes from no rows from all bucket can pass other condition. the result should be <rhs, 1>
	w.Joiner.OnMissMatch(false, probeSideRow, joinResult.chk)
	return true, waitTime, joinResult
}

// joinNAASJMatchProbeSideRow2Chunk implement the matching logic for NA-AntiSemiJoin
func (w *ProbeWorkerV1) joinNAASJMatchProbeSideRow2Chunk(probeKey uint64, probeKeyNullBits *bitmap.ConcurrentBitmap, probeSideRow chunk.Row, hCtx *HashContext, joinResult *hashjoinWorkerResult) (bool, int64, *hashjoinWorkerResult) {
	var (
		err error
		ok  bool
	)
	waitTime := int64(0)
	oneWaitTime := int64(0)
	if probeKeyNullBits == nil {
		// step1: match null bucket first.
		// need fetch the "valid" rows every time. (nullBits map check is necessary)
		w.buildSideRows, err = w.rowContainerForProbe.GetNullBucketRows(hCtx, probeSideRow, probeKeyNullBits, w.buildSideRows, w.needCheckBuildColPos, w.needCheckProbeColPos, w.needCheckBuildTypes, w.needCheckProbeTypes)
		buildSideRows := w.buildSideRows
		if err != nil {
			joinResult.err = err
			return false, waitTime, joinResult
		}
		if len(buildSideRows) != 0 {
			iter1 := w.rowIters
			iter1.Reset(buildSideRows)
			for iter1.Begin(); iter1.Current() != iter1.End(); {
				matched, _, err := w.Joiner.TryToMatchInners(probeSideRow, iter1, joinResult.chk)
				if err != nil {
					joinResult.err = err
					return false, waitTime, joinResult
				}
				// here matched means: there is a valid null bucket row from right side.
				// as said in the comment, once we meet a rhs null in CNF, we can determine the reject of lhs row.
				if matched {
					return true, waitTime, joinResult
				}
				if joinResult.chk.IsFull() {
					ok, oneWaitTime, joinResult = w.sendingResult(joinResult)
					waitTime += oneWaitTime
					if !ok {
						return false, waitTime, joinResult
					}
				}
			}
		}
		// step2: then same key bucket.
		w.buildSideRows, err = w.rowContainerForProbe.GetMatchedRows(probeKey, probeSideRow, hCtx, w.buildSideRows)
		buildSideRows = w.buildSideRows
		if err != nil {
			joinResult.err = err
			return false, waitTime, joinResult
		}
		if len(buildSideRows) == 0 {
			// when reach here, it means we couldn't return it quickly in null bucket, and same-bucket is empty,
			// which means x NOT IN (empty set), accept the rhs Row.
			w.Joiner.OnMissMatch(false, probeSideRow, joinResult.chk)
			return true, waitTime, joinResult
		}
		iter2 := w.rowIters
		iter2.Reset(buildSideRows)
		for iter2.Begin(); iter2.Current() != iter2.End(); {
			matched, _, err := w.Joiner.TryToMatchInners(probeSideRow, iter2, joinResult.chk)
			if err != nil {
				joinResult.err = err
				return false, waitTime, joinResult
			}
			// here matched means: there is a valid same key bucket row from right side.
			// as said in the comment, once we meet a false in CNF, we can determine the reject of lhs Row.
			if matched {
				return true, waitTime, joinResult
			}
			if joinResult.chk.IsFull() {
				ok, oneWaitTime, joinResult = w.sendingResult(joinResult)
				waitTime += oneWaitTime
				if !ok {
					return false, waitTime, joinResult
				}
			}
		}
		// step3: if we couldn't return it quickly in null bucket and same key bucket, here means two cases:
		// case1: x NOT IN (empty set): if other key bucket don't have the valid rows yet.
		// case2: x NOT IN (l,m,n...): if other key bucket do have the valid rows.
		// both cases should accept the rhs row.
		w.Joiner.OnMissMatch(false, probeSideRow, joinResult.chk)
		return true, waitTime, joinResult
	}
	// when left side has null values, all we want is to find a valid build side rows (passed from other condition)
	// so we can return it as soon as possible. here means two cases:
	// case1: <?, null> NOT IN (empty set):             ----------------------> accept rhs row.
	// case2: <?, null> NOT IN (at least a valid inner row) ------------------> unknown result, refuse rhs row.
	// Step1: match null bucket (assumption that null bucket is quite smaller than all hash table bucket rows)
	w.buildSideRows, err = w.rowContainerForProbe.GetNullBucketRows(hCtx, probeSideRow, probeKeyNullBits, w.buildSideRows, w.needCheckBuildColPos, w.needCheckProbeColPos, w.needCheckBuildTypes, w.needCheckProbeTypes)
	buildSideRows := w.buildSideRows
	if err != nil {
		joinResult.err = err
		return false, waitTime, joinResult
	}
	if len(buildSideRows) != 0 {
		iter1 := w.rowIters
		iter1.Reset(buildSideRows)
		for iter1.Begin(); iter1.Current() != iter1.End(); {
			matched, _, err := w.Joiner.TryToMatchInners(probeSideRow, iter1, joinResult.chk)
			if err != nil {
				joinResult.err = err
				return false, waitTime, joinResult
			}
			// here matched means: there is a valid null bucket row from right side. (not empty)
			// as said in the comment, once we found at least a valid row, we can determine the reject of lhs row.
			if matched {
				return true, waitTime, joinResult
			}
			if joinResult.chk.IsFull() {
				ok, oneWaitTime, joinResult = w.sendingResult(joinResult)
				waitTime += oneWaitTime
				if !ok {
					return false, waitTime, joinResult
				}
			}
		}
	}
	// Step2: match all hash table bucket build rows.
	w.buildSideRows, err = w.rowContainerForProbe.GetAllMatchedRows(hCtx, probeSideRow, probeKeyNullBits, w.buildSideRows, w.needCheckBuildColPos, w.needCheckProbeColPos, w.needCheckBuildTypes, w.needCheckProbeTypes)
	buildSideRows = w.buildSideRows
	if err != nil {
		joinResult.err = err
		return false, waitTime, joinResult
	}
	if len(buildSideRows) == 0 {
		// when reach here, it means we couldn't return it quickly in null bucket, and same-bucket is empty,
		// which means <?,null> NOT IN (empty set) or <?,null> NOT IN (no valid rows) accept the rhs row.
		w.Joiner.OnMissMatch(false, probeSideRow, joinResult.chk)
		return true, waitTime, joinResult
	}
	iter2 := w.rowIters
	iter2.Reset(buildSideRows)
	for iter2.Begin(); iter2.Current() != iter2.End(); {
		matched, _, err := w.Joiner.TryToMatchInners(probeSideRow, iter2, joinResult.chk)
		if err != nil {
			joinResult.err = err
			return false, waitTime, joinResult
		}
		// here matched means: there is a valid key row from right side. (not empty)
		// as said in the comment, once we found at least a valid row, we can determine the reject of lhs row.
		if matched {
			return true, waitTime, joinResult
		}
		if joinResult.chk.IsFull() {
			ok, oneWaitTime, joinResult = w.sendingResult(joinResult)
			waitTime += oneWaitTime
			if !ok {
				return false, waitTime, joinResult
			}
		}
	}
	// step3: if we couldn't return it quickly in null bucket and all hash bucket, here means only one cases:
	// case1: <?, null> NOT IN (empty set):
	// empty set comes from no rows from all bucket can pass other condition. we should accept the rhs row.
	w.Joiner.OnMissMatch(false, probeSideRow, joinResult.chk)
	return true, waitTime, joinResult
}

// joinNAAJMatchProbeSideRow2Chunk implement the matching priority logic for NA-AntiSemiJoin and NA-AntiLeftOuterSemiJoin
// there are some bucket-matching priority difference between them.
//
//		Since NA-AntiSemiJoin don't need to append the scalar value with the left side row, there is a quick matching path.
//		1: lhs row has null:
//	       lhs row has null can't determine its result in advance, we should judge whether the right valid set is empty
//	       or not. For semantic like x NOT IN(y set), If y set is empty, the scalar result is 1; Otherwise, the result
//	       is 0. Since NA-AntiSemiJoin don't care about the scalar value, we just try to find a valid row from right side,
//	       once we found it then just return the left side row instantly. (same as NA-AntiLeftOuterSemiJoin)
//
//		2: lhs row without null:
//	       same-key bucket and null-bucket which should be the first to match? For semantic like x NOT IN(y set), once y
//	       set has a same key x, the scalar value is 0; else if y set has a null key, then the scalar value is null. Both
//	       of them lead the refuse of the lhs row without any difference. Since NA-AntiSemiJoin don't care about the scalar
//	       value, we can just match the null bucket first and refuse the lhs row as quickly as possible, because a null of
//	       yi in the CNF (x NA-EQ yi) can always determine a negative value (refuse lhs row) in advance here.
//
//	       For NA-AntiLeftOuterSemiJoin, we couldn't match null-bucket first, because once y set has a same key x and null
//	       key, we should return the result as left side row appended with a scalar value 0 which is from same key matching failure.
func (w *ProbeWorkerV1) joinNAAJMatchProbeSideRow2Chunk(probeKey uint64, probeKeyNullBits *bitmap.ConcurrentBitmap, probeSideRow chunk.Row, hCtx *HashContext, joinResult *hashjoinWorkerResult) (bool, int64, *hashjoinWorkerResult) {
	naAntiSemiJoin := w.HashJoinCtx.JoinType == logicalop.AntiSemiJoin && w.HashJoinCtx.IsNullAware
	naAntiLeftOuterSemiJoin := w.HashJoinCtx.JoinType == logicalop.AntiLeftOuterSemiJoin && w.HashJoinCtx.IsNullAware
	if naAntiSemiJoin {
		return w.joinNAASJMatchProbeSideRow2Chunk(probeKey, probeKeyNullBits, probeSideRow, hCtx, joinResult)
	}
	if naAntiLeftOuterSemiJoin {
		return w.joinNAALOSJMatchProbeSideRow2Chunk(probeKey, probeKeyNullBits, probeSideRow, hCtx, joinResult)
	}
	// shouldn't be here, not a valid NAAJ.
	return false, 0, joinResult
}

func (w *ProbeWorkerV1) joinMatchedProbeSideRow2Chunk(probeKey uint64, probeSideRow chunk.Row, hCtx *HashContext,
	joinResult *hashjoinWorkerResult) (bool, int64, *hashjoinWorkerResult) {
	var err error
	waitTime := int64(0)
	oneWaitTime := int64(0)
	var buildSideRows []chunk.Row
	if w.Joiner.isSemiJoinWithoutCondition() {
		var rowPtr *chunk.Row
		rowPtr, err = w.rowContainerForProbe.GetOneMatchedRow(probeKey, probeSideRow, hCtx)
		if rowPtr != nil {
			buildSideRows = append(buildSideRows, *rowPtr)
		}
	} else {
		w.buildSideRows, err = w.rowContainerForProbe.GetMatchedRows(probeKey, probeSideRow, hCtx, w.buildSideRows)
		buildSideRows = w.buildSideRows
	}

	if err != nil {
		joinResult.err = err
		return false, waitTime, joinResult
	}
	if len(buildSideRows) == 0 {
		w.Joiner.OnMissMatch(false, probeSideRow, joinResult.chk)
		return true, waitTime, joinResult
	}
	iter := w.rowIters
	iter.Reset(buildSideRows)
	hasMatch, hasNull, ok := false, false, false
	for iter.Begin(); iter.Current() != iter.End(); {
		matched, isNull, err := w.Joiner.TryToMatchInners(probeSideRow, iter, joinResult.chk)
		if err != nil {
			joinResult.err = err
			return false, waitTime, joinResult
		}
		hasMatch = hasMatch || matched
		hasNull = hasNull || isNull

		if joinResult.chk.IsFull() {
			ok, oneWaitTime, joinResult = w.sendingResult(joinResult)
			waitTime += oneWaitTime
			if !ok {
				return false, waitTime, joinResult
			}
		}
	}
	if !hasMatch {
		w.Joiner.OnMissMatch(hasNull, probeSideRow, joinResult.chk)
	}
	return true, waitTime, joinResult
}

func (w *ProbeWorkerV1) getNewJoinResult() (bool, *hashjoinWorkerResult) {
	joinResult := &hashjoinWorkerResult{
		src: w.joinChkResourceCh,
	}
	ok := true
	select {
	case <-w.HashJoinCtx.closeCh:
		ok = false
	case joinResult.chk, ok = <-w.joinChkResourceCh:
	}
	return ok, joinResult
}

func (w *ProbeWorkerV1) join2Chunk(probeSideChk *chunk.Chunk, hCtx *HashContext, joinResult *hashjoinWorkerResult,
	selected []bool) (ok bool, waitTime int64, _ *hashjoinWorkerResult) {
	var err error
	waitTime = 0
	oneWaitTime := int64(0)
	selected, err = expression.VectorizedFilter(w.HashJoinCtx.SessCtx.GetExprCtx().GetEvalCtx(), w.HashJoinCtx.SessCtx.GetSessionVars().EnableVectorizedExpression, w.HashJoinCtx.OuterFilter, chunk.NewIterator4Chunk(probeSideChk), selected)
	if err != nil {
		joinResult.err = err
		return false, waitTime, joinResult
	}

	numRows := probeSideChk.NumRows()
	hCtx.InitHash(numRows)
	// By now, path 1 and 2 won't be conducted at the same time.
	// 1: write the row data of join key to hashVals. (normal EQ key should ignore the null values.) null-EQ for Except statement is an exception.
	for keyIdx, i := range hCtx.KeyColIdx {
		ignoreNull := len(w.HashJoinCtx.IsNullEQ) > keyIdx && w.HashJoinCtx.IsNullEQ[keyIdx]
		err = codec.HashChunkSelected(w.rowContainerForProbe.sc.TypeCtx(), hCtx.HashVals, probeSideChk, hCtx.AllTypes[keyIdx], i, hCtx.Buf, hCtx.HasNull, selected, ignoreNull)
		if err != nil {
			joinResult.err = err
			return false, waitTime, joinResult
		}
	}
	// 2: write the how data of NA join key to hashVals. (NA EQ key should collect all how including null value, store null value in a special position)
	isNAAJ := len(hCtx.NaKeyColIdx) > 0
	for keyIdx, i := range hCtx.NaKeyColIdx {
		// NAAJ won't ignore any null values, but collect them up to probe.
		err = codec.HashChunkSelected(w.rowContainerForProbe.sc.TypeCtx(), hCtx.HashVals, probeSideChk, hCtx.AllTypes[keyIdx], i, hCtx.Buf, hCtx.HasNull, selected, false)
		if err != nil {
			joinResult.err = err
			return false, waitTime, joinResult
		}
		// after fetch one NA column, collect the null value to null bitmap for every how. (use hasNull flag to accelerate)
		// eg: if a NA Join cols is (a, b, c), for every build row here we maintained a 3-bit map to mark which column is null for them.
		for rowIdx := range numRows {
			if hCtx.HasNull[rowIdx] {
				hCtx.naColNullBitMap[rowIdx].UnsafeSet(keyIdx)
				// clean and try fetch Next NA join col.
				hCtx.HasNull[rowIdx] = false
				hCtx.naHasNull[rowIdx] = true
			}
		}
	}

	for i := range selected {
		err := w.HashJoinCtx.SessCtx.GetSessionVars().SQLKiller.HandleSignal()
		failpoint.Inject("killedInJoin2Chunk", func(val failpoint.Value) {
			if val.(bool) {
				err = exeerrors.ErrQueryInterrupted
			}
		})
		if err != nil {
			joinResult.err = err
			return false, waitTime, joinResult
		}
		if isNAAJ {
			if !selected[i] {
				// since this is the case of using inner to build, so for an outer row unselected, we should fill the result when it's outer join.
				w.Joiner.OnMissMatch(false, probeSideChk.GetRow(i), joinResult.chk)
			} else if hCtx.naHasNull[i] {
				// here means the probe join connecting column has null value in it and this is special for matching all the hash buckets
				// for it. (probeKey is not necessary here)
				probeRow := probeSideChk.GetRow(i)
				ok, oneWaitTime, joinResult = w.joinNAAJMatchProbeSideRow2Chunk(0, hCtx.naColNullBitMap[i].Clone(), probeRow, hCtx, joinResult)
				waitTime += oneWaitTime
				if !ok {
					return false, waitTime, joinResult
				}
			} else {
				// here means the probe join connecting column without null values, where we should match same key bucket and null bucket for it at its order.
				// step1: process same key matched probe side rows
				probeKey, probeRow := hCtx.HashVals[i].Sum64(), probeSideChk.GetRow(i)
				ok, oneWaitTime, joinResult = w.joinNAAJMatchProbeSideRow2Chunk(probeKey, nil, probeRow, hCtx, joinResult)
				waitTime += oneWaitTime
				if !ok {
					return false, waitTime, joinResult
				}
			}
		} else {
			// since this is the case of using inner to build, so for an outer row unselected, we should fill the result when it's outer join.
			if !selected[i] || hCtx.HasNull[i] { // process unmatched probe side rows
				w.Joiner.OnMissMatch(false, probeSideChk.GetRow(i), joinResult.chk)
			} else { // process matched probe side rows
				probeKey, probeRow := hCtx.HashVals[i].Sum64(), probeSideChk.GetRow(i)
				ok, oneWaitTime, joinResult = w.joinMatchedProbeSideRow2Chunk(probeKey, probeRow, hCtx, joinResult)
				waitTime += oneWaitTime
				if !ok {
					return false, waitTime, joinResult
				}
			}
		}
		if joinResult.chk.IsFull() {
			ok, oneWaitTime, joinResult = w.sendingResult(joinResult)
			waitTime += oneWaitTime
			if !ok {
				return false, waitTime, joinResult
			}
		}
	}
	return true, waitTime, joinResult
}

func (w *ProbeWorkerV1) sendingResult(joinResult *hashjoinWorkerResult) (ok bool, cost int64, newJoinResult *hashjoinWorkerResult) {
	start := time.Now()
	w.HashJoinCtx.joinResultCh <- joinResult
	ok, newJoinResult = w.getNewJoinResult()
	cost = int64(time.Since(start))
	return ok, cost, newJoinResult
}

// join2ChunkForOuterHashJoin joins chunks when using the outer to build a hash table (refer to outer hash join)
func (w *ProbeWorkerV1) join2ChunkForOuterHashJoin(probeSideChk *chunk.Chunk, hCtx *HashContext, joinResult *hashjoinWorkerResult) (ok bool, waitTime int64, _ *hashjoinWorkerResult) {
	waitTime = 0
	oneWaitTime := int64(0)
	hCtx.InitHash(probeSideChk.NumRows())
	for keyIdx, i := range hCtx.KeyColIdx {
		err := codec.HashChunkColumns(w.rowContainerForProbe.sc.TypeCtx(), hCtx.HashVals, probeSideChk, hCtx.AllTypes[keyIdx], i, hCtx.Buf, hCtx.HasNull)
		if err != nil {
			joinResult.err = err
			return false, waitTime, joinResult
		}
	}
	for i := range probeSideChk.NumRows() {
		err := w.HashJoinCtx.SessCtx.GetSessionVars().SQLKiller.HandleSignal()
		failpoint.Inject("killedInJoin2ChunkForOuterHashJoin", func(val failpoint.Value) {
			if val.(bool) {
				err = exeerrors.ErrQueryInterrupted
			}
		})
		if err != nil {
			joinResult.err = err
			return false, waitTime, joinResult
		}
		probeKey, probeRow := hCtx.HashVals[i].Sum64(), probeSideChk.GetRow(i)
		ok, oneWaitTime, joinResult = w.joinMatchedProbeSideRow2ChunkForOuterHashJoin(probeKey, probeRow, hCtx, joinResult)
		waitTime += oneWaitTime
		if !ok {
			return false, waitTime, joinResult
		}
		if joinResult.chk.IsFull() {
			ok, oneWaitTime, joinResult = w.sendingResult(joinResult)
			waitTime += oneWaitTime
			if !ok {
				return false, waitTime, joinResult
			}
		}
	}
	return true, waitTime, joinResult
}

// Next implements the Executor Next interface.
// hash join constructs the result following these steps:
// step 1. fetch data from build side child and build a hash table;
// step 2. fetch data from probe child in a background goroutine and probe the hash table in multiple join workers.
func (e *HashJoinV1Exec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	if !e.Prepared {
		e.buildFinished = make(chan error, 1)
		hCtx := &HashContext{
			AllTypes:    e.BuildTypes,
			KeyColIdx:   e.BuildWorker.BuildKeyColIdx,
			NaKeyColIdx: e.BuildWorker.BuildNAKeyColIdx,
		}
		e.RowContainer = newHashRowContainer(e.Ctx(), hCtx, exec.RetTypes(e.BuildWorker.BuildSideExec))
		// we shallow copies RowContainer for each probe worker to avoid lock contention
		for i := range e.Concurrency {
			if i == 0 {
				e.ProbeWorkers[i].rowContainerForProbe = e.RowContainer
			} else {
				e.ProbeWorkers[i].rowContainerForProbe = e.RowContainer.ShallowCopy()
			}
		}
		for i := range e.Concurrency {
			e.ProbeWorkers[i].rowIters = chunk.NewIterator4Slice([]chunk.Row{})
		}
		e.workerWg.RunWithRecover(func() {
			defer trace.StartRegion(ctx, "HashJoinHashTableBuilder").End()
			e.fetchAndBuildHashTable(ctx)
		}, e.handleFetchAndBuildHashTablePanic)
		e.fetchAndProbeHashTable(ctx)
		e.Prepared = true
	}
	if e.IsOuterJoin {
		atomic.StoreInt64(&e.ProbeSideTupleFetcher.requiredRows, int64(req.RequiredRows()))
	}
	req.Reset()

	result, ok := <-e.joinResultCh
	if !ok {
		return nil
	}
	if result.err != nil {
		e.finished.Store(true)
		return result.err
	}
	req.SwapColumns(result.chk)
	result.src <- result.chk
	return nil
}

func (e *HashJoinV1Exec) handleFetchAndBuildHashTablePanic(r any) {
	if r != nil {
		e.buildFinished <- util.GetRecoverError(r)
	}
	close(e.buildFinished)
}

func (e *HashJoinV1Exec) fetchAndBuildHashTable(ctx context.Context) {
	if e.stats != nil {
		start := time.Now()
		defer func() {
			e.stats.fetchAndBuildHashTable = time.Since(start)
		}()
	}
	// buildSideResultCh transfers build side chunk from build side fetch to build hash table.
	buildSideResultCh := make(chan *chunk.Chunk, 1)
	doneCh := make(chan struct{})
	fetchBuildSideRowsOk := make(chan error, 1)
	e.workerWg.RunWithRecover(
		func() {
			defer trace.StartRegion(ctx, "HashJoinBuildSideFetcher").End()
			e.BuildWorker.fetchBuildSideRows(ctx, &e.BuildWorker.HashJoinCtx.hashJoinCtxBase, nil, nil, buildSideResultCh, fetchBuildSideRowsOk, doneCh)
		},
		func(r any) {
			if r != nil {
				fetchBuildSideRowsOk <- util.GetRecoverError(r)
			}
			close(fetchBuildSideRowsOk)
		},
	)

	// TODO: Parallel build hash table. Currently not support because `unsafeHashTable` is not thread-safe.
	err := e.BuildWorker.BuildHashTableForList(buildSideResultCh)
	if err != nil {
		e.buildFinished <- errors.Trace(err)
		close(doneCh)
	}
	// Wait fetchBuildSideRows be Finished.
	// 1. if BuildHashTableForList fails
	// 2. if probeSideResult.NumRows() == 0, fetchProbeSideChunks will not wait for the build side.
	channel.Clear(buildSideResultCh)
	// Check whether err is nil to avoid sending redundant error into buildFinished.
	if err == nil {
		if err = <-fetchBuildSideRowsOk; err != nil {
			e.buildFinished <- err
		}
	}
}

// BuildHashTableForList builds hash table from `list`.
func (w *BuildWorkerV1) BuildHashTableForList(buildSideResultCh <-chan *chunk.Chunk) error {
	var err error
	var selected []bool
	rowContainer := w.HashJoinCtx.RowContainer
	rowContainer.GetMemTracker().AttachTo(w.HashJoinCtx.memTracker)
	rowContainer.GetMemTracker().SetLabel(memory.LabelForBuildSideResult)
	rowContainer.GetDiskTracker().AttachTo(w.HashJoinCtx.diskTracker)
	rowContainer.GetDiskTracker().SetLabel(memory.LabelForBuildSideResult)
	if vardef.EnableTmpStorageOnOOM.Load() {
		actionSpill := rowContainer.ActionSpill()
		failpoint.Inject("testRowContainerSpill", func(val failpoint.Value) {
			if val.(bool) {
				actionSpill = rowContainer.rowContainer.ActionSpillForTest()
				defer actionSpill.(*chunk.SpillDiskAction).WaitForTest()
			}
		})
		w.HashJoinCtx.SessCtx.GetSessionVars().MemTracker.FallbackOldAndSetNewAction(actionSpill)
	}
	for chk := range buildSideResultCh {
		if w.HashJoinCtx.finished.Load() {
			return nil
		}
		if !w.HashJoinCtx.UseOuterToBuild {
			err = rowContainer.PutChunk(chk, w.HashJoinCtx.IsNullEQ)
		} else {
			var bitMap = bitmap.NewConcurrentBitmap(chk.NumRows())
			w.HashJoinCtx.outerMatchedStatus = append(w.HashJoinCtx.outerMatchedStatus, bitMap)
			w.HashJoinCtx.memTracker.Consume(bitMap.BytesConsumed())
			if len(w.HashJoinCtx.OuterFilter) == 0 {
				err = w.HashJoinCtx.RowContainer.PutChunk(chk, w.HashJoinCtx.IsNullEQ)
			} else {
				selected, err = expression.VectorizedFilter(w.HashJoinCtx.SessCtx.GetExprCtx().GetEvalCtx(), w.HashJoinCtx.SessCtx.GetSessionVars().EnableVectorizedExpression, w.HashJoinCtx.OuterFilter, chunk.NewIterator4Chunk(chk), selected)
				if err != nil {
					return err
				}
				err = rowContainer.PutChunkSelected(chk, selected, w.HashJoinCtx.IsNullEQ)
			}
		}
		failpoint.Inject("ConsumeRandomPanic", nil)
		if err != nil {
			return err
		}
	}
	return nil
}

// NestedLoopApplyExec is the executor for apply.
type NestedLoopApplyExec struct {
	exec.BaseExecutor

	Sctx        sessionctx.Context
	innerRows   []chunk.Row
	cursor      int
	InnerExec   exec.Executor
	OuterExec   exec.Executor
	InnerFilter expression.CNFExprs
	OuterFilter expression.CNFExprs

	Joiner Joiner

	cache              *applycache.ApplyCache
	CanUseCache        bool
	cacheHitCounter    int
	cacheAccessCounter int

	OuterSchema []*expression.CorrelatedColumn

	OuterChunk       *chunk.Chunk
	outerChunkCursor int
	outerSelected    []bool
	InnerList        *chunk.List
	InnerChunk       *chunk.Chunk
	innerSelected    []bool
	innerIter        chunk.Iterator
	outerRow         *chunk.Row
	hasMatch         bool
	hasNull          bool

	Outer bool

	memTracker *memory.Tracker // track memory usage.
}

// Close implements the Executor interface.
func (e *NestedLoopApplyExec) Close() error {
	e.innerRows = nil
	e.memTracker = nil
	if e.RuntimeStats() != nil {
		runtimeStats := NewJoinRuntimeStats()
		if e.CanUseCache {
			var hitRatio float64
			if e.cacheAccessCounter > 0 {
				hitRatio = float64(e.cacheHitCounter) / float64(e.cacheAccessCounter)
			}
			runtimeStats.SetCacheInfo(true, hitRatio)
		} else {
			runtimeStats.SetCacheInfo(false, 0)
		}
		runtimeStats.SetConcurrencyInfo(execdetails.NewConcurrencyInfo("concurrency", 0))
		defer e.Ctx().GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.ID(), runtimeStats)
	}
	return exec.Close(e.OuterExec)
}

// Open implements the Executor interface.
func (e *NestedLoopApplyExec) Open(ctx context.Context) error {
	err := exec.Open(ctx, e.OuterExec)
	if err != nil {
		return err
	}
	e.cursor = 0
	e.innerRows = e.innerRows[:0]
	e.OuterChunk = exec.TryNewCacheChunk(e.OuterExec)
	e.InnerChunk = exec.TryNewCacheChunk(e.InnerExec)
	e.InnerList = chunk.NewList(exec.RetTypes(e.InnerExec), e.InitCap(), e.MaxChunkSize())

	e.memTracker = memory.NewTracker(e.ID(), -1)
	e.memTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.MemTracker)

	e.InnerList.GetMemTracker().SetLabel(memory.LabelForInnerList)
	e.InnerList.GetMemTracker().AttachTo(e.memTracker)

	if e.CanUseCache {
		e.cache, err = applycache.NewApplyCache(e.Sctx)
		if err != nil {
			return err
		}
		e.cacheHitCounter = 0
		e.cacheAccessCounter = 0
		e.cache.GetMemTracker().AttachTo(e.memTracker)
	}
	return nil
}

// aggExecutorTreeInputEmpty checks whether the executor tree returns empty if without aggregate operators.
// Note that, the prerequisite is that this executor tree has been executed already and it returns one Row.
func aggExecutorTreeInputEmpty(e exec.Executor) bool {
	children := e.AllChildren()
	if len(children) == 0 {
		return false
	}
	if len(children) > 1 {
		_, ok := e.(*unionexec.UnionExec)
		if !ok {
			// It is a Join executor.
			return false
		}
		for _, child := range children {
			if !aggExecutorTreeInputEmpty(child) {
				return false
			}
		}
		return true
	}
	// Single child executors.
	if aggExecutorTreeInputEmpty(children[0]) {
		return true
	}
	if hashAgg, ok := e.(*aggregate.HashAggExec); ok {
		return hashAgg.IsChildReturnEmpty
	}
	if streamAgg, ok := e.(*aggregate.StreamAggExec); ok {
		return streamAgg.IsChildReturnEmpty
	}
	return false
}

func (e *NestedLoopApplyExec) fetchSelectedOuterRow(ctx context.Context, chk *chunk.Chunk) (*chunk.Row, error) {
	outerIter := chunk.NewIterator4Chunk(e.OuterChunk)
	for {
		if e.outerChunkCursor >= e.OuterChunk.NumRows() {
			err := exec.Next(ctx, e.OuterExec, e.OuterChunk)
			if err != nil {
				return nil, err
			}
			if e.OuterChunk.NumRows() == 0 {
				return nil, nil
			}
			e.outerSelected, err = expression.VectorizedFilter(e.Sctx.GetExprCtx().GetEvalCtx(), e.Sctx.GetSessionVars().EnableVectorizedExpression, e.OuterFilter, outerIter, e.outerSelected)
			if err != nil {
				return nil, err
			}
			// For cases like `select count(1), (select count(1) from s where s.a > t.a) as sub from t where t.a = 1`,
			// if outer child has no row satisfying `t.a = 1`, `sub` should be `null` instead of `0` theoretically; however, the
			// outer `count(1)` produces one row <0, null> over the empty input, we should specially mark this outer row
			// as not selected, to trigger the mismatch join procedure.
			if e.outerChunkCursor == 0 && e.OuterChunk.NumRows() == 1 && e.outerSelected[0] && aggExecutorTreeInputEmpty(e.OuterExec) {
				e.outerSelected[0] = false
			}
			e.outerChunkCursor = 0
		}
		outerRow := e.OuterChunk.GetRow(e.outerChunkCursor)
		selected := e.outerSelected[e.outerChunkCursor]
		e.outerChunkCursor++
		if selected {
			return &outerRow, nil
		} else if e.Outer {
			e.Joiner.OnMissMatch(false, outerRow, chk)
			if chk.IsFull() {
				return nil, nil
			}
		}
	}
}

// fetchAllInners reads all data from the inner table and stores them in a List.
func (e *NestedLoopApplyExec) fetchAllInners(ctx context.Context) error {
	err := exec.Open(ctx, e.InnerExec)
	defer func() { terror.Log(exec.Close(e.InnerExec)) }()
	if err != nil {
		return err
	}

	if e.CanUseCache {
		// create a new one since it may be in the cache
		e.InnerList = chunk.NewListWithMemTracker(exec.RetTypes(e.InnerExec), e.InitCap(), e.MaxChunkSize(), e.InnerList.GetMemTracker())
	} else {
		e.InnerList.Reset()
	}
	innerIter := chunk.NewIterator4Chunk(e.InnerChunk)
	for {
		err := exec.Next(ctx, e.InnerExec, e.InnerChunk)
		if err != nil {
			return err
		}
		if e.InnerChunk.NumRows() == 0 {
			return nil
		}

		e.innerSelected, err = expression.VectorizedFilter(e.Sctx.GetExprCtx().GetEvalCtx(), e.Sctx.GetSessionVars().EnableVectorizedExpression, e.InnerFilter, innerIter, e.innerSelected)
		if err != nil {
			return err
		}
		for row := innerIter.Begin(); row != innerIter.End(); row = innerIter.Next() {
			if e.innerSelected[row.Idx()] {
				e.InnerList.AppendRow(row)
			}
		}
	}
}

// Next implements the Executor interface.
func (e *NestedLoopApplyExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	req.Reset()
	for {
		if e.innerIter == nil || e.innerIter.Current() == e.innerIter.End() {
			if e.outerRow != nil && !e.hasMatch {
				e.Joiner.OnMissMatch(e.hasNull, *e.outerRow, req)
			}
			e.outerRow, err = e.fetchSelectedOuterRow(ctx, req)
			if e.outerRow == nil || err != nil {
				return err
			}
			e.hasMatch = false
			e.hasNull = false

			if e.CanUseCache {
				var key []byte
				for _, col := range e.OuterSchema {
					*col.Data = e.outerRow.GetDatum(col.Index, col.RetType)
					key, err = codec.EncodeKey(e.Ctx().GetSessionVars().StmtCtx.TimeZone(), key, *col.Data)
					err = e.Ctx().GetSessionVars().StmtCtx.HandleError(err)
					if err != nil {
						return err
					}
				}
				e.cacheAccessCounter++
				value, err := e.cache.Get(key)
				if err != nil {
					return err
				}
				if value != nil {
					e.InnerList = value
					e.cacheHitCounter++
				} else {
					err = e.fetchAllInners(ctx)
					if err != nil {
						return err
					}
					if _, err := e.cache.Set(key, e.InnerList); err != nil {
						return err
					}
				}
			} else {
				for _, col := range e.OuterSchema {
					*col.Data = e.outerRow.GetDatum(col.Index, col.RetType)
				}
				err = e.fetchAllInners(ctx)
				if err != nil {
					return err
				}
			}
			e.innerIter = chunk.NewIterator4List(e.InnerList)
			e.innerIter.Begin()
		}

		matched, isNull, err := e.Joiner.TryToMatchInners(*e.outerRow, e.innerIter, req)
		e.hasMatch = e.hasMatch || matched
		e.hasNull = e.hasNull || isNull

		if err != nil || req.IsFull() {
			return err
		}
	}
}

// cacheInfo is used to save the concurrency information of the executor operator
type cacheInfo struct {
	hitRatio float64
	useCache bool
}

type joinRuntimeStats struct {
	*execdetails.RuntimeStatsWithConcurrencyInfo

	applyCache  bool
	cache       cacheInfo
	hasHashStat bool
	hashStat    hashStatistic
}

// NewJoinRuntimeStats returns a new joinRuntimeStats
func NewJoinRuntimeStats() *joinRuntimeStats {
	stats := &joinRuntimeStats{
		RuntimeStatsWithConcurrencyInfo: &execdetails.RuntimeStatsWithConcurrencyInfo{},
	}
	return stats
}

// SetCacheInfo sets the cache information. Only used for apply executor.
func (e *joinRuntimeStats) SetCacheInfo(useCache bool, hitRatio float64) {
	e.Lock()
	e.applyCache = true
	e.cache.useCache = useCache
	e.cache.hitRatio = hitRatio
	e.Unlock()
}

func (e *joinRuntimeStats) String() string {
	buf := bytes.NewBuffer(make([]byte, 0, 16))
	buf.WriteString(e.RuntimeStatsWithConcurrencyInfo.String())
	if e.applyCache {
		if e.cache.useCache {
			fmt.Fprintf(buf, ", cache:ON, cacheHitRatio:%.3f%%", e.cache.hitRatio*100)
		} else {
			buf.WriteString(", cache:OFF")
		}
	}
	if e.hasHashStat {
		buf.WriteString(", " + e.hashStat.String())
	}
	return buf.String()
}

// Tp implements the RuntimeStats interface.
func (*joinRuntimeStats) Tp() int {
	return execdetails.TpJoinRuntimeStats
}

func (e *joinRuntimeStats) Clone() execdetails.RuntimeStats {
	newJRS := &joinRuntimeStats{
		RuntimeStatsWithConcurrencyInfo: e.RuntimeStatsWithConcurrencyInfo,
		applyCache:                      e.applyCache,
		cache:                           e.cache,
		hasHashStat:                     e.hasHashStat,
		hashStat:                        e.hashStat,
	}
	return newJRS
}
