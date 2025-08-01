// Copyright 2022 PingCAP, Inc.
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

package ddl

import (
	"bytes"
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	driver "github.com/pingcap/tidb/pkg/store/driver/txn"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	kvutil "github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

func (w *mergeIndexWorker) batchCheckTemporaryUniqueKey(
	txn kv.Transaction,
	idxRecords []*temporaryIndexRecord,
) error {
	if !w.currentIndex.Unique {
		// non-unique key need no check, just overwrite it,
		// because in most case, backfilling indices is not exists.
		return nil
	}

	batchVals, err := txn.BatchGet(context.Background(), w.originIdxKeys)
	if err != nil {
		return errors.Trace(err)
	}

	for i, key := range w.originIdxKeys {
		keyStr := string(key)
		if val, found := batchVals[keyStr]; found {
			// Found a value in the original index key.
			matchDeleted, err := checkTempIndexKey(txn, idxRecords[i], val, w.table)
			if err != nil {
				if kv.ErrKeyExists.Equal(err) {
					return driver.ExtractKeyExistsErrFromIndex(key, val, w.table.Meta(), w.currentIndex.ID)
				}
				return errors.Trace(err)
			}
			if matchDeleted {
				// Delete from batchVals to prevent false-positive duplicate detection.
				delete(batchVals, keyStr)
			}
		} else if idxRecords[i].distinct {
			// The keys in w.batchCheckKeys also maybe duplicate,
			// so we need to backfill the not found key into `batchVals` map.
			batchVals[keyStr] = idxRecords[i].vals
		}
	}
	return nil
}

// checkTempIndexKey determines whether there is a duplicated index key entry according to value of temp index.
// For non-delete temp record, if the index values mismatch, it is duplicated.
// For delete temp record, we decode the handle from the origin index value and temp index value.
//   - if the handles match, we can delete the index key.
//   - otherwise, we further check if the row exists in the table.
func checkTempIndexKey(txn kv.Transaction, tmpRec *temporaryIndexRecord, originIdxVal []byte, tblInfo table.Table) (matchDelete bool, err error) {
	if !tmpRec.delete {
		if tmpRec.distinct && !bytes.Equal(originIdxVal, tmpRec.vals) {
			return false, kv.ErrKeyExists
		}
		// The key has been found in the original index, skip merging it.
		tmpRec.skip = true
		return false, nil
	}
	// Delete operation.
	distinct := tablecodec.IndexKVIsUnique(originIdxVal)
	if !distinct {
		// For non-distinct key, it is consist of a null value and the handle.
		// Same as the non-unique indexes, replay the delete operation on non-distinct keys.
		return false, nil
	}
	// For distinct index key values, prevent deleting an unexpected index KV in original index.
	hdInVal, err := tablecodec.DecodeHandleInIndexValue(originIdxVal)
	if err != nil {
		return false, errors.Trace(err)
	}
	if !tmpRec.handle.Equal(hdInVal) {
		// The inequality means multiple modifications happened in the same key.
		// We use the handle in origin index value to check if the row exists.
		rowKey := tablecodec.EncodeRecordKey(tblInfo.RecordPrefix(), hdInVal)
		_, err := txn.Get(context.Background(), rowKey)
		if err != nil {
			if kv.IsErrNotFound(err) {
				// The row is deleted, so we can merge the delete operation to the origin index.
				tmpRec.skip = false
				return false, nil
			}
			// Unexpected errors.
			return false, errors.Trace(err)
		}
		// Don't delete the index key if the row exists.
		tmpRec.skip = true
		return false, nil
	}
	return true, nil
}

// temporaryIndexRecord is the record information of an index.
type temporaryIndexRecord struct {
	vals     []byte
	skip     bool // skip indicates that the index key is already exists, we should not add it.
	delete   bool
	unique   bool
	distinct bool
	handle   kv.Handle
}

type mergeIndexWorker struct {
	*backfillCtx

	indexes []table.Index

	tmpIdxRecords []*temporaryIndexRecord
	originIdxKeys []kv.Key
	tmpIdxKeys    []kv.Key

	currentIndex *model.IndexInfo
}

func newMergeTempIndexWorker(bfCtx *backfillCtx, t table.PhysicalTable, elements []*meta.Element) *mergeIndexWorker {
	allIndexes := make([]table.Index, 0, len(elements))
	for _, elem := range elements {
		indexInfo := model.FindIndexInfoByID(t.Meta().Indices, elem.ID)
		index := tables.NewIndex(t.GetPhysicalID(), t.Meta(), indexInfo)
		allIndexes = append(allIndexes, index)
	}

	return &mergeIndexWorker{
		backfillCtx: bfCtx,
		indexes:     allIndexes,
	}
}

func (w *mergeIndexWorker) setCurrentIndexForRange(taskRange *reorgBackfillTask) (err error) {
	tmpID, err := tablecodec.DecodeIndexID(taskRange.startKey)
	if err != nil {
		return err
	}
	startIndexID := tmpID & tablecodec.IndexIDMask
	for _, idx := range w.indexes {
		idxInfo := idx.Meta()
		if idxInfo.ID == startIndexID {
			w.currentIndex = idxInfo
			return nil
		}
	}
	return errors.Errorf("index (id=%d) not found", startIndexID)
}

// BackfillData merge temp index data in txn.
func (w *mergeIndexWorker) BackfillData(ctx context.Context, taskRange reorgBackfillTask) (taskCtx backfillTaskContext, errInTxn error) {
	err := w.setCurrentIndexForRange(&taskRange)
	if err != nil {
		return taskCtx, err
	}

	var currentTxnStartTS uint64
	oprStartTime := time.Now()
	ctx = kv.WithInternalSourceAndTaskType(ctx, w.jobContext.ddlJobSourceType(), kvutil.ExplicitTypeDDL)
	bfCtx := w.GetCtx()
	originBatchCnt := bfCtx.batchCnt
	defer func() {
		bfCtx.batchCnt = originBatchCnt
	}()

	attempts := 0
	for {
		attempts++

		err := kv.RunInNewTxn(ctx, w.ddlCtx.store, false, func(_ context.Context, txn kv.Transaction) error {
			currentTxnStartTS = txn.StartTS()
			taskCtx.addedCount = 0
			taskCtx.scanCount = 0
			updateTxnEntrySizeLimitIfNeeded(txn)
			txn.SetOption(kv.Priority, taskRange.priority)
			if tagger := w.GetCtx().getResourceGroupTaggerForTopSQL(taskRange.getJobID()); tagger != nil {
				txn.SetOption(kv.ResourceGroupTagger, tagger)
			}
			txn.SetOption(kv.ResourceGroupName, w.jobContext.resourceGroupName)

			tmpIdxRecords, nextKey, taskDone, err := w.fetchTempIndexVals(txn, &taskCtx.scanCount, taskRange)
			if err != nil {
				return errors.Trace(err)
			}
			taskCtx.nextKey = nextKey
			taskCtx.done = taskDone

			err = w.batchCheckTemporaryUniqueKey(txn, tmpIdxRecords)
			if err != nil {
				return errors.Trace(err)
			}

			for i, idxRecord := range tmpIdxRecords {
				// The index is already exists, we skip it, no needs to backfill it.
				// The following update, delete, insert on these rows, TiDB can handle it correctly.
				// If all batch are skipped, update first index key to make txn commit to release lock.
				if idxRecord.skip {
					continue
				}

				originIdxKey := w.originIdxKeys[i]
				if idxRecord.delete {
					err = txn.GetMemBuffer().Delete(originIdxKey)
				} else {
					err = txn.GetMemBuffer().Set(originIdxKey, idxRecord.vals)
				}
				if err != nil {
					return err
				}

				err = txn.GetMemBuffer().Delete(w.tmpIdxKeys[i])
				if err != nil {
					return err
				}

				failpoint.InjectCall("mockDMLExecutionMergingInTxn")

				taskCtx.addedCount++
			}
			return nil
		})
		if err != nil {
			if kv.IsTxnRetryableError(err) {
				if err := w.ddlCtx.isReorgRunnable(ctx, false); err != nil {
					return taskCtx, errors.Trace(err)
				}
				if bfCtx.batchCnt > 1 {
					bfCtx.batchCnt /= 2
				}
				w.conflictCounter.Add(1)
				backoff := kv.BackOff(uint(attempts))
				logutil.DDLLogger().Warn("temp index merge worker retry",
					zap.Int64("jobID", taskRange.jobID),
					zap.Int("batchCnt", bfCtx.batchCnt),
					zap.Int("attempts", attempts),
					zap.Duration("backoff", time.Duration(backoff)),
					zap.Uint64("startTS", currentTxnStartTS),
					zap.Error(err))
				continue
			}
			return taskCtx, errors.Trace(err)
		}
		break
	}

	metrics.DDLSetTempIndexScanAndMerge(w.table.Meta().ID, uint64(taskCtx.scanCount), uint64(taskCtx.addedCount))
	failpoint.Inject("mockDMLExecutionMerging", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) && MockDMLExecutionMerging != nil {
			MockDMLExecutionMerging()
		}
	})
	logSlowOperations(time.Since(oprStartTime), "AddIndexMergeDataInTxn", 3000)
	return
}

func (w *mergeIndexWorker) AddMetricInfo(cnt float64) {
	w.metricCounter.Add(cnt)
}

func (*mergeIndexWorker) String() string {
	return typeAddIndexMergeTmpWorker.String()
}

func (w *mergeIndexWorker) GetCtx() *backfillCtx {
	return w.backfillCtx
}

func (w *mergeIndexWorker) fetchTempIndexVals(
	txn kv.Transaction,
	scannedCnt *int,
	taskRange reorgBackfillTask,
) ([]*temporaryIndexRecord, kv.Key, bool, error) {
	startTime := time.Now()
	w.tmpIdxRecords = w.tmpIdxRecords[:0]
	w.tmpIdxKeys = w.tmpIdxKeys[:0]
	w.originIdxKeys = w.originIdxKeys[:0]
	// taskDone means that the merged handle is out of taskRange.endHandle.
	taskDone := false
	oprStartTime := startTime
	idxPrefix := w.table.IndexPrefix()
	var lastKey kv.Key
	err := iterateSnapshotKeys(w.jobContext, w.ddlCtx.store, taskRange.priority, idxPrefix, txn.StartTS(),
		taskRange.startKey, taskRange.endKey, func(_ kv.Handle, indexKey kv.Key, rawValue []byte) (more bool, err error) {
			oprEndTime := time.Now()
			logSlowOperations(oprEndTime.Sub(oprStartTime), "iterate temporary index in merge process", 0)
			oprStartTime = oprEndTime

			taskDone = indexKey.Cmp(taskRange.endKey) >= 0

			if taskDone || len(w.tmpIdxRecords) >= w.batchCnt {
				return false, nil
			}

			tempIdxVal, err := tablecodec.DecodeTempIndexValue(rawValue)
			if err != nil {
				return false, err
			}
			tempIdxVal, err = decodeTempIndexHandleFromIndexKV(indexKey, tempIdxVal, len(w.currentIndex.Columns))
			if err != nil {
				return false, err
			}

			*scannedCnt += len(tempIdxVal)
			tempIdxVal = tempIdxVal.FilterOverwritten()

			// Extract the operations on the original index and replay them later.
			for _, elem := range tempIdxVal {
				if elem.KeyVer == tablecodec.TempIndexKeyTypeMerge || elem.KeyVer == tablecodec.TempIndexKeyTypeDelete {
					// For 'm' version kvs, they are double-written.
					// For 'd' version kvs, they are written in the delete-only state and can be dropped safely.
					continue
				}

				originIdxKey := make([]byte, len(indexKey))
				copy(originIdxKey, indexKey)
				tablecodec.TempIndexKey2IndexKey(originIdxKey)

				idxRecord := &temporaryIndexRecord{
					handle: elem.Handle,
					delete: elem.Delete,
					unique: elem.Distinct,
					skip:   false,
				}
				if !elem.Delete {
					idxRecord.vals = elem.Value
					idxRecord.distinct = tablecodec.IndexKVIsUnique(elem.Value)
				}
				w.tmpIdxRecords = append(w.tmpIdxRecords, idxRecord)
				w.originIdxKeys = append(w.originIdxKeys, originIdxKey)
				w.tmpIdxKeys = append(w.tmpIdxKeys, indexKey)
			}

			lastKey = indexKey
			return true, nil
		})

	if len(w.tmpIdxRecords) == 0 {
		taskDone = true
	}
	var nextKey kv.Key
	if taskDone {
		nextKey = taskRange.endKey
	} else {
		nextKey = lastKey
	}

	logutil.DDLLogger().Debug("merge temp index txn fetches handle info", zap.Uint64("txnStartTS", txn.StartTS()),
		zap.Stringer("taskRange", &taskRange), zap.Duration("takeTime", time.Since(startTime)))
	return w.tmpIdxRecords, nextKey.Next(), taskDone, errors.Trace(err)
}

func decodeTempIndexHandleFromIndexKV(indexKey kv.Key, tmpVal tablecodec.TempIndexValue, idxColLen int) (ret tablecodec.TempIndexValue, err error) {
	for _, elem := range tmpVal {
		if elem.Handle == nil {
			// If the handle is not found in the value of the temp index, it means
			// 1) This is not a deletion marker, the handle is in the key or the origin value.
			// 2) This is a deletion marker, but the handle is in the key of temp index.
			elem.Handle, err = tablecodec.DecodeIndexHandle(indexKey, elem.Value, idxColLen)
			if err != nil {
				return nil, err
			}
		}
	}
	return tmpVal, nil
}
