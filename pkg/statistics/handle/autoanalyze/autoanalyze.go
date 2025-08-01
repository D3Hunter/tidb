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

package autoanalyze

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sysproctrack"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/exec"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/refresher"
	"github.com/pingcap/tidb/pkg/statistics/handle/lockstats"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"go.uber.org/zap"
)

// statsAnalyze implements util.StatsAnalyze.
// statsAnalyze is used to handle auto-analyze and manage analyze jobs.
type statsAnalyze struct {
	statsHandle statstypes.StatsHandle
	// sysProcTracker is used to track sys process like analyze
	sysProcTracker sysproctrack.Tracker
	// refresher is used to refresh the analyze job queue and analyze the highest priority tables.
	// It is only used when auto-analyze priority queue is enabled.
	refresher *refresher.Refresher
}

// NewStatsAnalyze creates a new StatsAnalyze.
func NewStatsAnalyze(
	ctx context.Context,
	statsHandle statstypes.StatsHandle,
	sysProcTracker sysproctrack.Tracker,
	ddlNotifier *notifier.DDLNotifier,
) statstypes.StatsAnalyze {
	// Usually, we should only create the refresher when auto-analyze priority queue is enabled.
	// But to allow users to enable auto-analyze priority queue on the fly, we need to create the refresher here.
	r := refresher.NewRefresher(ctx, statsHandle, sysProcTracker, ddlNotifier)
	return &statsAnalyze{
		statsHandle:    statsHandle,
		sysProcTracker: sysProcTracker,
		refresher:      r,
	}
}

// InsertAnalyzeJob inserts the analyze job to the storage.
func (sa *statsAnalyze) InsertAnalyzeJob(job *statistics.AnalyzeJob, instance string, procID uint64) error {
	return statsutil.CallWithSCtx(sa.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		return insertAnalyzeJob(sctx, job, instance, procID)
	})
}

func (sa *statsAnalyze) StartAnalyzeJob(job *statistics.AnalyzeJob) {
	err := statsutil.CallWithSCtx(sa.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		startAnalyzeJob(sctx, job)
		return nil
	})
	if err != nil {
		statslogutil.StatsLogger().Warn("failed to start analyze job", zap.Error(err))
	}
}

func (sa *statsAnalyze) UpdateAnalyzeJobProgress(job *statistics.AnalyzeJob, rowCount int64) {
	err := statsutil.CallWithSCtx(sa.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		updateAnalyzeJobProgress(sctx, job, rowCount)
		return nil
	})
	if err != nil {
		statslogutil.StatsLogger().Warn("failed to update analyze job progress", zap.Error(err))
	}
}

func (sa *statsAnalyze) FinishAnalyzeJob(job *statistics.AnalyzeJob, failReason error, analyzeType statistics.JobType) {
	err := statsutil.CallWithSCtx(sa.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		finishAnalyzeJob(sctx, job, failReason, analyzeType)
		return nil
	})
	if err != nil {
		statslogutil.StatsLogger().Warn("failed to finish analyze job", zap.Error(err))
	}
}

// DeleteAnalyzeJobs deletes the analyze jobs whose update time is earlier than updateTime.
func (sa *statsAnalyze) DeleteAnalyzeJobs(updateTime time.Time) error {
	return statsutil.CallWithSCtx(sa.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		_, _, err := statsutil.ExecRows(sctx, "DELETE FROM mysql.analyze_jobs WHERE update_time < CONVERT_TZ(%?, '+00:00', @@TIME_ZONE)", updateTime.UTC().Format(types.TimeFormat))
		return err
	})
}

// CleanupCorruptedAnalyzeJobsOnCurrentInstance cleans up the potentially corrupted analyze job.
// It only cleans up the jobs that are associated with the current instance.
func (sa *statsAnalyze) CleanupCorruptedAnalyzeJobsOnCurrentInstance(currentRunningProcessIDs map[uint64]struct{}) error {
	return statsutil.CallWithSCtx(sa.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		return CleanupCorruptedAnalyzeJobsOnCurrentInstance(sctx, currentRunningProcessIDs)
	}, statsutil.FlagWrapTxn)
}

// CleanupCorruptedAnalyzeJobsOnDeadInstances removes analyze jobs that may have been corrupted.
// Specifically, it removes jobs associated with instances that no longer exist in the cluster.
func (sa *statsAnalyze) CleanupCorruptedAnalyzeJobsOnDeadInstances() error {
	return statsutil.CallWithSCtx(sa.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		return CleanupCorruptedAnalyzeJobsOnDeadInstances(sctx)
	}, statsutil.FlagWrapTxn)
}

// OnBecomeOwner is used to handle the event when the current TiDB instance becomes the stats owner.
func (sa *statsAnalyze) OnBecomeOwner() {
	sa.refresher.OnBecomeOwner()
}

// OnRetireOwner is used to handle the event when the current TiDB instance retires from being the stats owner.
func (sa *statsAnalyze) OnRetireOwner() {
	sa.refresher.OnRetireOwner()
}

// SelectAnalyzeJobsOnCurrentInstanceSQL is the SQL to select the analyze jobs whose
// state is `pending` or `running` and the update time is more than 10 minutes ago
// and the instance is current instance.
const SelectAnalyzeJobsOnCurrentInstanceSQL = `SELECT id, process_id
		FROM mysql.analyze_jobs
		WHERE instance = %?
		AND state IN ('pending', 'running')
		AND update_time < CONVERT_TZ(%?, '+00:00', @@TIME_ZONE)`

// SelectAnalyzeJobsSQL is the SQL to select the analyze jobs whose
// state is `pending` or `running` and the update time is more than 10 minutes ago.
const SelectAnalyzeJobsSQL = `SELECT id, instance
		FROM mysql.analyze_jobs
		WHERE state IN ('pending', 'running')
		AND update_time < CONVERT_TZ(%?, '+00:00', @@TIME_ZONE)`

// BatchUpdateAnalyzeJobSQL is the SQL to update the analyze jobs to `failed` state.
const BatchUpdateAnalyzeJobSQL = `UPDATE mysql.analyze_jobs
            SET state = 'failed',
            fail_reason = 'The TiDB Server has either shut down or the analyze query was terminated during the analyze job execution',
            process_id = NULL
            WHERE id IN (%?)`

func tenMinutesAgo() string {
	return time.Now().Add(-10 * time.Minute).UTC().Format(types.TimeFormat)
}

// CleanupCorruptedAnalyzeJobsOnCurrentInstance cleans up the potentially corrupted analyze job from current instance.
// Exported for testing.
func CleanupCorruptedAnalyzeJobsOnCurrentInstance(
	sctx sessionctx.Context,
	currentRunningProcessIDs map[uint64]struct{},
) error {
	serverInfo, err := infosync.GetServerInfo()
	if err != nil {
		return errors.Trace(err)
	}
	instance := net.JoinHostPort(serverInfo.IP, strconv.Itoa(int(serverInfo.Port)))
	// Get all the analyze jobs whose state is `pending` or `running` and the update time is more than 10 minutes ago
	// and the instance is current instance.
	rows, _, err := statsutil.ExecRows(
		sctx,
		SelectAnalyzeJobsOnCurrentInstanceSQL,
		instance,
		tenMinutesAgo(),
	)
	if err != nil {
		return errors.Trace(err)
	}

	jobIDs := make([]string, 0, len(rows))
	for _, row := range rows {
		// The process ID is typically non-null for running or pending jobs.
		// However, in rare cases(I don't which case), it may be null. Therefore, it's necessary to check its value.
		if !row.IsNull(1) {
			processID := row.GetUint64(1)
			// If the process id is not in currentRunningProcessIDs, we need to clean up the job.
			// They don't belong to current instance any more.
			if _, ok := currentRunningProcessIDs[processID]; !ok {
				jobID := row.GetUint64(0)
				jobIDs = append(jobIDs, strconv.FormatUint(jobID, 10))
			}
		}
	}

	// Do a batch update to clean up the jobs.
	if len(jobIDs) > 0 {
		_, _, err = statsutil.ExecRows(
			sctx,
			BatchUpdateAnalyzeJobSQL,
			jobIDs,
		)
		if err != nil {
			return errors.Trace(err)
		}
		statslogutil.StatsLogger().Info(
			"clean up the potentially corrupted analyze jobs from current instance",
			zap.Strings("jobIDs", jobIDs),
		)
	}

	return nil
}

// CleanupCorruptedAnalyzeJobsOnDeadInstances cleans up the potentially corrupted analyze job from dead instances.
func CleanupCorruptedAnalyzeJobsOnDeadInstances(
	sctx sessionctx.Context,
) error {
	rows, _, err := statsutil.ExecRows(
		sctx,
		SelectAnalyzeJobsSQL,
		tenMinutesAgo(),
	)
	if err != nil {
		return errors.Trace(err)
	}
	if len(rows) == 0 {
		return nil
	}

	// Get all the instances from etcd.
	serverInfo, err := infosync.GetAllServerInfo(context.Background())
	if err != nil {
		return errors.Trace(err)
	}
	instances := make(map[string]struct{}, len(serverInfo))
	for _, info := range serverInfo {
		instance := net.JoinHostPort(info.IP, strconv.Itoa(int(info.Port)))
		instances[instance] = struct{}{}
	}

	jobIDs := make([]string, 0, len(rows))
	for _, row := range rows {
		// If the instance is not in instances, we need to clean up the job.
		// It means the instance is down or the instance is not in the cluster any more.
		instance := row.GetString(1)
		if _, ok := instances[instance]; !ok {
			jobID := row.GetUint64(0)
			jobIDs = append(jobIDs, strconv.FormatUint(jobID, 10))
		}
	}

	// Do a batch update to clean up the jobs.
	if len(jobIDs) > 0 {
		_, _, err = statsutil.ExecRows(
			sctx,
			BatchUpdateAnalyzeJobSQL,
			jobIDs,
		)
		if err != nil {
			return errors.Trace(err)
		}
		statslogutil.StatsLogger().Info(
			"clean up the potentially corrupted analyze jobs from dead instances",
			zap.Strings("jobIDs", jobIDs),
		)
	}

	return nil
}

// HandleAutoAnalyze analyzes the outdated tables. (The change percent of the table exceeds the threshold)
// It also analyzes newly created tables and newly added indexes.
func (sa *statsAnalyze) HandleAutoAnalyze() (analyzed bool) {
	if err := statsutil.CallWithSCtx(sa.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		analyzed = sa.handleAutoAnalyze(sctx)
		return nil
	}); err != nil {
		statslogutil.StatsErrVerboseSampleLogger().Error("Failed to handle auto analyze", zap.Error(err))
	}
	return
}

// CheckAnalyzeVersion checks whether all the statistics versions of this table's columns and indexes are the same.
func (sa *statsAnalyze) CheckAnalyzeVersion(tblInfo *model.TableInfo, physicalIDs []int64, version *int) bool {
	// We simply choose one physical id to get its stats.
	var tbl *statistics.Table
	for _, pid := range physicalIDs {
		tbl = sa.statsHandle.GetPartitionStats(tblInfo, pid)
		if !tbl.Pseudo {
			break
		}
	}
	if tbl == nil || tbl.Pseudo {
		return true
	}
	return statistics.CheckAnalyzeVerOnTable(tbl, version)
}

// GetPriorityQueueSnapshot returns the stats priority queue snapshot.
func (sa *statsAnalyze) GetPriorityQueueSnapshot() (statstypes.PriorityQueueSnapshot, error) {
	return sa.refresher.GetPriorityQueueSnapshot()
}

func (sa *statsAnalyze) handleAutoAnalyze(sctx sessionctx.Context) bool {
	defer func() {
		if r := recover(); r != nil {
			statslogutil.StatsLogger().Error(
				"HandleAutoAnalyze panicked",
				zap.Any("recover", r),
				zap.Stack("stack"),
			)
		}
	}()
	if vardef.EnableAutoAnalyzePriorityQueue.Load() {
		// During the test, we need to fetch all DML changes before analyzing the highest priority tables.
		if intest.InTest {
			sa.refresher.ProcessDMLChangesForTest()
			sa.refresher.RequeueMustRetryJobsForTest()
		}
		analyzed := sa.refresher.AnalyzeHighestPriorityTables(sctx)
		// During the test, we need to wait for the auto analyze job to be finished.
		if intest.InTest {
			sa.refresher.WaitAutoAnalyzeFinishedForTest()
		}
		return analyzed
	}

	parameters := exec.GetAutoAnalyzeParameters(sctx)
	autoAnalyzeRatio := exec.ParseAutoAnalyzeRatio(parameters[vardef.TiDBAutoAnalyzeRatio])
	start, end, ok := checkAutoAnalyzeWindow(parameters)
	if !ok {
		return false
	}

	pruneMode := variable.PartitionPruneMode(sctx.GetSessionVars().PartitionPruneMode.Load())

	return RandomPickOneTableAndTryAutoAnalyze(
		sctx,
		sa.statsHandle,
		sa.sysProcTracker,
		autoAnalyzeRatio,
		pruneMode,
		start,
		end,
	)
}

// Close closes the auto-analyze worker.
func (sa *statsAnalyze) Close() {
	sa.refresher.Close()
}

// CheckAutoAnalyzeWindow determine the time window for auto-analysis and verify if the current time falls within this range.
func CheckAutoAnalyzeWindow(sctx sessionctx.Context) (startStr, endStr string, ok bool) {
	parameters := exec.GetAutoAnalyzeParameters(sctx)
	start, end, ok := checkAutoAnalyzeWindow(parameters)
	startStr = start.Format("15:04")
	endStr = end.Format("15:04")
	return
}

func checkAutoAnalyzeWindow(parameters map[string]string) (_, _ time.Time, _ bool) {
	start, end, err := exec.ParseAutoAnalysisWindow(
		parameters[vardef.TiDBAutoAnalyzeStartTime],
		parameters[vardef.TiDBAutoAnalyzeEndTime],
	)
	if err != nil {
		statslogutil.StatsLogger().Error(
			"parse auto analyze period failed",
			zap.Error(err),
		)
		return start, end, false
	}
	if !timeutil.WithinDayTimePeriod(start, end, time.Now()) {
		return start, end, false
	}
	return start, end, true
}

// RandomPickOneTableAndTryAutoAnalyze randomly picks one table and tries to analyze it.
// 1. If the table is not analyzed, analyze it.
// 2. If the table is analyzed, analyze it when "tbl.ModifyCount/tbl.Count > autoAnalyzeRatio".
// 3. If the table is analyzed, analyze its indices when the index is not analyzed.
// 4. If the table is locked, skip it.
// Exposed solely for testing.
func RandomPickOneTableAndTryAutoAnalyze(
	sctx sessionctx.Context,
	statsHandle statstypes.StatsHandle,
	sysProcTracker sysproctrack.Tracker,
	autoAnalyzeRatio float64,
	pruneMode variable.PartitionPruneMode,
	start, end time.Time,
) bool {
	is := sctx.GetLatestInfoSchema().(infoschema.InfoSchema)
	dbs := infoschema.AllSchemaNames(is)
	// Shuffle the database and table slice to randomize the order of analyzing tables.
	rd := rand.New(rand.NewSource(time.Now().UnixNano())) // #nosec G404
	rd.Shuffle(len(dbs), func(i, j int) {
		dbs[i], dbs[j] = dbs[j], dbs[i]
	})
	// Query locked tables once to minimize overhead.
	// Outdated lock info is acceptable as we verify table lock status pre-analysis.
	lockedTables, err := lockstats.QueryLockedTables(statsutil.StatsCtx, sctx)
	if err != nil {
		statslogutil.StatsLogger().Error(
			"check table lock failed",
			zap.Error(err),
		)
		return false
	}

	for _, db := range dbs {
		// Ignore the memory and system database.
		if metadef.IsMemOrSysDB(strings.ToLower(db)) {
			continue
		}

		tbls, err := is.SchemaTableInfos(context.Background(), ast.NewCIStr(db))
		terror.Log(err)
		// We shuffle dbs and tbls so that the order of iterating tables is random. If the order is fixed and the auto
		// analyze job of one table fails for some reason, it may always analyze the same table and fail again and again
		// when the HandleAutoAnalyze is triggered. Randomizing the order can avoid the problem.
		// TODO: Design a priority queue to place the table which needs analyze most in the front.
		rd.Shuffle(len(tbls), func(i, j int) {
			tbls[i], tbls[j] = tbls[j], tbls[i]
		})

		// We need to check every partition of every table to see if it needs to be analyzed.
		for _, tblInfo := range tbls {
			// Sometimes the tables are too many. Auto-analyze will take too much time on it.
			// so we need to check the available time.
			if !timeutil.WithinDayTimePeriod(start, end, time.Now()) {
				return false
			}
			// If table locked, skip analyze all partitions of the table.
			// FIXME: This check is not accurate, because other nodes may change the table lock status at any time.
			if _, ok := lockedTables[tblInfo.ID]; ok {
				continue
			}

			if tblInfo.IsView() {
				continue
			}

			pi := tblInfo.GetPartitionInfo()
			// No partitions, analyze the whole table.
			if pi == nil {
				statsTbl := statsHandle.GetTableStatsForAutoAnalyze(tblInfo)
				sql := "analyze table %n.%n"
				analyzed := tryAutoAnalyzeTable(sctx, statsHandle, sysProcTracker, tblInfo, statsTbl, autoAnalyzeRatio, sql, db, tblInfo.Name.O)
				if analyzed {
					// analyze one table at a time to let it get the freshest parameters.
					// others will be analyzed next round which is just 3s later.
					return true
				}
				continue
			}
			// Only analyze the partition that has not been locked.
			partitionDefs := make([]model.PartitionDefinition, 0, len(pi.Definitions))
			for _, def := range pi.Definitions {
				if _, ok := lockedTables[def.ID]; !ok {
					partitionDefs = append(partitionDefs, def)
				}
			}
			partitionStats := getPartitionStats(statsHandle, tblInfo, partitionDefs)
			if pruneMode == variable.Dynamic {
				analyzed := tryAutoAnalyzePartitionTableInDynamicMode(
					sctx,
					statsHandle,
					sysProcTracker,
					tblInfo,
					partitionDefs,
					partitionStats,
					db,
					autoAnalyzeRatio,
				)
				if analyzed {
					return true
				}
				continue
			}
			for _, def := range partitionDefs {
				sql := "analyze table %n.%n partition %n"
				statsTbl := partitionStats[def.ID]
				analyzed := tryAutoAnalyzeTable(sctx, statsHandle, sysProcTracker, tblInfo, statsTbl, autoAnalyzeRatio, sql, db, tblInfo.Name.O, def.Name.O)
				if analyzed {
					return true
				}
			}
		}
	}

	return false
}

func getPartitionStats(
	statsHandle statstypes.StatsHandle,
	tblInfo *model.TableInfo,
	defs []model.PartitionDefinition,
) map[int64]*statistics.Table {
	partitionStats := make(map[int64]*statistics.Table, len(defs))

	for _, def := range defs {
		partitionStats[def.ID] = statsHandle.GetPartitionStatsForAutoAnalyze(tblInfo, def.ID)
	}

	return partitionStats
}

// Determine whether the table and index require analysis.
func tryAutoAnalyzeTable(
	sctx sessionctx.Context,
	statsHandle statstypes.StatsHandle,
	sysProcTracker sysproctrack.Tracker,
	tblInfo *model.TableInfo,
	statsTbl *statistics.Table,
	ratio float64,
	sql string,
	params ...any,
) bool {
	// 1. If the statistics are either not loaded or are classified as pseudo, there is no need for analyze
	//    Pseudo statistics can be created by the optimizer, so we need to double check it.
	// 2. If the table is too small, we don't want to waste time to analyze it.
	//    Leave the opportunity to other bigger tables.
	if statsTbl == nil || statsTbl.Pseudo || statsTbl.RealtimeCount < statistics.AutoAnalyzeMinCnt {
		return false
	}

	// Check if the table needs to analyze.
	if needAnalyze, reason := NeedAnalyzeTable(
		statsTbl,
		ratio,
	); needAnalyze {
		escaped, err := sqlescape.EscapeSQL(sql, params...)
		if err != nil {
			return false
		}
		statslogutil.StatsLogger().Info(
			"auto analyze triggered",
			zap.String("sql", escaped),
			zap.String("reason", reason),
		)

		tableStatsVer := sctx.GetSessionVars().AnalyzeVersion
		statistics.CheckAnalyzeVerOnTable(statsTbl, &tableStatsVer)
		exec.AutoAnalyze(sctx, statsHandle, sysProcTracker, tableStatsVer, sql, params...)

		return true
	}

	// Whether the table needs to analyze or not, we need to check the indices of the table.
	for _, idx := range tblInfo.Indices {
		if idxStats := statsTbl.GetIdx(idx.ID); idxStats == nil && !statsTbl.ColAndIdxExistenceMap.HasAnalyzed(idx.ID, true) && idx.State == model.StatePublic {
			// Columnar index doesn't need stats yet.
			if idx.IsColumnarIndex() {
				continue
			}
			sqlWithIdx := sql + " index %n"
			paramsWithIdx := append(params, idx.Name.O)
			escaped, err := sqlescape.EscapeSQL(sqlWithIdx, paramsWithIdx...)
			if err != nil {
				return false
			}

			statslogutil.StatsLogger().Info(
				"auto analyze for unanalyzed indexes",
				zap.String("sql", escaped),
			)
			tableStatsVer := sctx.GetSessionVars().AnalyzeVersion
			statistics.CheckAnalyzeVerOnTable(statsTbl, &tableStatsVer)
			exec.AutoAnalyze(sctx, statsHandle, sysProcTracker, tableStatsVer, sqlWithIdx, paramsWithIdx...)
			return true
		}
	}
	return false
}

// NeedAnalyzeTable checks if we need to analyze the table:
//  1. If the table has never been analyzed, we need to analyze it.
//  2. If the table had been analyzed before, we need to analyze it when
//     "tbl.ModifyCount/tbl.Count > autoAnalyzeRatio" and the current time is
//     between `start` and `end`.
//
// Exposed for test.
func NeedAnalyzeTable(tbl *statistics.Table, autoAnalyzeRatio float64) (bool, string) {
	analyzed := tbl.IsAnalyzed()
	if !analyzed {
		return true, "table unanalyzed"
	}
	// Auto analyze is disabled.
	if autoAnalyzeRatio == 0 {
		return false, ""
	}
	// No need to analyze it.
	tblCnt := float64(tbl.RealtimeCount)
	if histCnt := tbl.GetAnalyzeRowCount(); histCnt > 0 {
		tblCnt = histCnt
	}
	if float64(tbl.ModifyCount)/tblCnt <= autoAnalyzeRatio {
		return false, ""
	}
	return true, fmt.Sprintf("too many modifications(%v/%v>%v)", tbl.ModifyCount, tblCnt, autoAnalyzeRatio)
}

// It is very similar to tryAutoAnalyzeTable, but it commits the analyze job in batch for partitions.
func tryAutoAnalyzePartitionTableInDynamicMode(
	sctx sessionctx.Context,
	statsHandle statstypes.StatsHandle,
	sysProcTracker sysproctrack.Tracker,
	tblInfo *model.TableInfo,
	partitionDefs []model.PartitionDefinition,
	partitionStats map[int64]*statistics.Table,
	db string,
	ratio float64,
) bool {
	tableStatsVer := sctx.GetSessionVars().AnalyzeVersion
	analyzePartitionBatchSize := int(vardef.AutoAnalyzePartitionBatchSize.Load())
	needAnalyzePartitionNames := make([]any, 0, len(partitionDefs))

	for _, def := range partitionDefs {
		partitionStats := partitionStats[def.ID]
		// 1. If the statistics are either not loaded or are classified as pseudo, there is no need for analyze.
		//	  Pseudo statistics can be created by the optimizer, so we need to double check it.
		// 2. If the table is too small, we don't want to waste time to analyze it.
		//    Leave the opportunity to other bigger tables.
		if partitionStats == nil || partitionStats.Pseudo || partitionStats.RealtimeCount < statistics.AutoAnalyzeMinCnt {
			continue
		}
		if needAnalyze, reason := NeedAnalyzeTable(
			partitionStats,
			ratio,
		); needAnalyze {
			needAnalyzePartitionNames = append(needAnalyzePartitionNames, def.Name.O)
			statslogutil.StatsLogger().Info(
				"need to auto analyze",
				zap.String("database", db),
				zap.String("table", tblInfo.Name.String()),
				zap.String("partition", def.Name.O),
				zap.String("reason", reason),
			)
			statistics.CheckAnalyzeVerOnTable(partitionStats, &tableStatsVer)
		}
	}

	getSQL := func(prefix, suffix string, numPartitions int) string {
		var sqlBuilder strings.Builder
		sqlBuilder.WriteString(prefix)
		for i := range numPartitions {
			if i != 0 {
				sqlBuilder.WriteString(",")
			}
			sqlBuilder.WriteString(" %n")
		}
		sqlBuilder.WriteString(suffix)
		return sqlBuilder.String()
	}

	if len(needAnalyzePartitionNames) > 0 {
		statslogutil.StatsLogger().Info("start to auto analyze",
			zap.String("database", db),
			zap.String("table", tblInfo.Name.String()),
			zap.Any("partitions", needAnalyzePartitionNames),
			zap.Int("analyze partition batch size", analyzePartitionBatchSize),
		)

		statsTbl := statsHandle.GetTableStats(tblInfo)
		statistics.CheckAnalyzeVerOnTable(statsTbl, &tableStatsVer)
		for i := 0; i < len(needAnalyzePartitionNames); i += analyzePartitionBatchSize {
			start := i
			end := min(start+analyzePartitionBatchSize, len(needAnalyzePartitionNames))

			// Do batch analyze for partitions.
			sql := getSQL("analyze table %n.%n partition", "", end-start)
			params := append([]any{db, tblInfo.Name.O}, needAnalyzePartitionNames[start:end]...)

			statslogutil.StatsLogger().Info(
				"auto analyze triggered",
				zap.String("database", db),
				zap.String("table", tblInfo.Name.String()),
				zap.Any("partitions", needAnalyzePartitionNames[start:end]),
			)
			exec.AutoAnalyze(sctx, statsHandle, sysProcTracker, tableStatsVer, sql, params...)
		}

		return true
	}
	// Check if any index of the table needs to analyze.
	for _, idx := range tblInfo.Indices {
		if idx.State != model.StatePublic || statsutil.IsSpecialGlobalIndex(idx, tblInfo) {
			continue
		}
		// Columnar index doesn't need stats yet.
		if idx.IsColumnarIndex() {
			continue
		}
		// Collect all the partition names that need to analyze.
		for _, def := range partitionDefs {
			partitionStats := partitionStats[def.ID]
			// 1. If the statistics are either not loaded or are classified as pseudo, there is no need for analyze.
			//    Pseudo statistics can be created by the optimizer, so we need to double check it.
			if partitionStats == nil || partitionStats.Pseudo {
				continue
			}
			// 2. If the index is not analyzed, we need to analyze it.
			if !partitionStats.ColAndIdxExistenceMap.HasAnalyzed(idx.ID, true) {
				needAnalyzePartitionNames = append(needAnalyzePartitionNames, def.Name.O)
				statistics.CheckAnalyzeVerOnTable(partitionStats, &tableStatsVer)
			}
		}
		if len(needAnalyzePartitionNames) > 0 {
			statsTbl := statsHandle.GetTableStats(tblInfo)
			statistics.CheckAnalyzeVerOnTable(statsTbl, &tableStatsVer)

			for i := 0; i < len(needAnalyzePartitionNames); i += analyzePartitionBatchSize {
				start := i
				end := min(start+analyzePartitionBatchSize, len(needAnalyzePartitionNames))

				sql := getSQL("analyze table %n.%n partition", " index %n", end-start)
				params := append([]any{db, tblInfo.Name.O}, needAnalyzePartitionNames[start:end]...)
				params = append(params, idx.Name.O)
				statslogutil.StatsLogger().Info("auto analyze for unanalyzed",
					zap.String("database", db),
					zap.String("table", tblInfo.Name.String()),
					zap.String("index", idx.Name.String()),
					zap.Any("partitions", needAnalyzePartitionNames[start:end]),
				)
				exec.AutoAnalyze(sctx, statsHandle, sysProcTracker, tableStatsVer, sql, params...)
			}

			return true
		}
	}

	return false
}

// insertAnalyzeJob inserts analyze job into mysql.analyze_jobs and gets job ID for further updating job.
func insertAnalyzeJob(sctx sessionctx.Context, job *statistics.AnalyzeJob, instance string, procID uint64) (err error) {
	jobInfo := job.JobInfo
	const textMaxLength = 65535
	if len(jobInfo) > textMaxLength {
		jobInfo = jobInfo[:textMaxLength]
	}
	const insertJob = "INSERT INTO mysql.analyze_jobs (table_schema, table_name, partition_name, job_info, state, instance, process_id) VALUES (%?, %?, %?, %?, %?, %?, %?)"
	_, _, err = statsutil.ExecRows(sctx, insertJob, job.DBName, job.TableName, job.PartitionName, jobInfo, statistics.AnalyzePending, instance, procID)
	if err != nil {
		return err
	}
	const getJobID = "SELECT LAST_INSERT_ID()"
	rows, _, err := statsutil.ExecRows(sctx, getJobID)
	if err != nil {
		return err
	}
	job.ID = new(uint64)
	*job.ID = rows[0].GetUint64(0)
	failpoint.Inject("DebugAnalyzeJobOperations", func(val failpoint.Value) {
		if val.(bool) {
			logutil.BgLogger().Info("InsertAnalyzeJob",
				zap.String("table_schema", job.DBName),
				zap.String("table_name", job.TableName),
				zap.String("partition_name", job.PartitionName),
				zap.String("job_info", jobInfo),
				zap.Uint64("job_id", *job.ID),
			)
		}
	})
	return nil
}

// startAnalyzeJob marks the state of the analyze job as running and sets the start time.
func startAnalyzeJob(sctx sessionctx.Context, job *statistics.AnalyzeJob) {
	if job == nil || job.ID == nil {
		return
	}
	job.StartTime = time.Now()
	job.Progress.SetLastDumpTime(job.StartTime)
	const sql = "UPDATE mysql.analyze_jobs SET start_time = CONVERT_TZ(%?, '+00:00', @@TIME_ZONE), state = %? WHERE id = %?"
	_, _, err := statsutil.ExecRows(sctx, sql, job.StartTime.UTC().Format(types.TimeFormat), statistics.AnalyzeRunning, *job.ID)
	if err != nil {
		statslogutil.StatsLogger().Warn("failed to update analyze job", zap.String("update", fmt.Sprintf("%s->%s", statistics.AnalyzePending, statistics.AnalyzeRunning)), zap.Error(err))
	}
	failpoint.Inject("DebugAnalyzeJobOperations", func(val failpoint.Value) {
		if val.(bool) {
			logutil.BgLogger().Info("StartAnalyzeJob",
				zap.Time("start_time", job.StartTime),
				zap.Uint64("job id", *job.ID),
			)
		}
	})
}

// updateAnalyzeJobProgress updates count of the processed rows when increment reaches a threshold.
func updateAnalyzeJobProgress(sctx sessionctx.Context, job *statistics.AnalyzeJob, rowCount int64) {
	if job == nil || job.ID == nil {
		return
	}
	delta := job.Progress.Update(rowCount)
	if delta == 0 {
		return
	}
	const sql = "UPDATE mysql.analyze_jobs SET processed_rows = processed_rows + %? WHERE id = %?"
	_, _, err := statsutil.ExecRows(sctx, sql, delta, *job.ID)
	if err != nil {
		statslogutil.StatsLogger().Warn("failed to update analyze job", zap.String("update", fmt.Sprintf("process %v rows", delta)), zap.Error(err))
	}
	failpoint.Inject("DebugAnalyzeJobOperations", func(val failpoint.Value) {
		if val.(bool) {
			logutil.BgLogger().Info("UpdateAnalyzeJobProgress",
				zap.Int64("increase processed_rows", delta),
				zap.Uint64("job id", *job.ID),
			)
		}
	})
}

// finishAnalyzeJob finishes an analyze or merge job
func finishAnalyzeJob(sctx sessionctx.Context, job *statistics.AnalyzeJob, analyzeErr error, analyzeType statistics.JobType) {
	if job == nil || job.ID == nil {
		return
	}

	job.EndTime = time.Now()
	var sql string
	var args []any

	// process_id is used to see which process is running the analyze job and kill the analyze job. After the analyze job
	// is finished(or failed), process_id is useless and we set it to NULL to avoid `kill tidb process_id` wrongly.
	if analyzeErr != nil {
		failReason := analyzeErr.Error()
		const textMaxLength = 65535
		if len(failReason) > textMaxLength {
			failReason = failReason[:textMaxLength]
		}

		if analyzeType == statistics.TableAnalysisJob {
			sql = "UPDATE mysql.analyze_jobs SET processed_rows = processed_rows + %?, end_time = CONVERT_TZ(%?, '+00:00', @@TIME_ZONE), state = %?, fail_reason = %?, process_id = NULL WHERE id = %?"
			args = []any{job.Progress.GetDeltaCount(), job.EndTime.UTC().Format(types.TimeFormat), statistics.AnalyzeFailed, failReason, *job.ID}
		} else {
			sql = "UPDATE mysql.analyze_jobs SET end_time = CONVERT_TZ(%?, '+00:00', @@TIME_ZONE), state = %?, fail_reason = %?, process_id = NULL WHERE id = %?"
			args = []any{job.EndTime.UTC().Format(types.TimeFormat), statistics.AnalyzeFailed, failReason, *job.ID}
		}
	} else {
		if analyzeType == statistics.TableAnalysisJob {
			sql = "UPDATE mysql.analyze_jobs SET processed_rows = processed_rows + %?, end_time = CONVERT_TZ(%?, '+00:00', @@TIME_ZONE), state = %?, process_id = NULL WHERE id = %?"
			args = []any{job.Progress.GetDeltaCount(), job.EndTime.UTC().Format(types.TimeFormat), statistics.AnalyzeFinished, *job.ID}
		} else {
			sql = "UPDATE mysql.analyze_jobs SET end_time = CONVERT_TZ(%?, '+00:00', @@TIME_ZONE), state = %?, process_id = NULL WHERE id = %?"
			args = []any{job.EndTime.UTC().Format(types.TimeFormat), statistics.AnalyzeFinished, *job.ID}
		}
	}

	_, _, err := statsutil.ExecRows(sctx, sql, args...)
	if err != nil {
		state := statistics.AnalyzeFinished
		if analyzeErr != nil {
			state = statistics.AnalyzeFailed
		}
		logutil.BgLogger().Warn("failed to update analyze job", zap.String("update", fmt.Sprintf("%s->%s", statistics.AnalyzeRunning, state)), zap.Error(err))
	}

	failpoint.Inject("DebugAnalyzeJobOperations", func(val failpoint.Value) {
		if val.(bool) {
			logger := logutil.BgLogger().With(
				zap.Time("end_time", job.EndTime),
				zap.Uint64("job id", *job.ID),
			)
			if analyzeType == statistics.TableAnalysisJob {
				logger = logger.With(zap.Int64("increase processed_rows", job.Progress.GetDeltaCount()))
			}
			if analyzeErr != nil {
				logger = logger.With(zap.Error(analyzeErr))
			}
			logger.Info("FinishAnalyzeJob")
		}
	})
}
