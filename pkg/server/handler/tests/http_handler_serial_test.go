// Copyright 2021 PingCAP, Inc.
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

package tests

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/server/handler"
	"github.com/pingcap/tidb/pkg/server/handler/tikvhandler"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/statistics/handle/ddl/testutil"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/deadlockhistory"
	"github.com/pingcap/tidb/pkg/util/versioninfo"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/client/clients/gc"
	"go.uber.org/zap"
)

func dummyRecord() *deadlockhistory.DeadlockRecord {
	return &deadlockhistory.DeadlockRecord{}
}

func TestPostSettings(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()
	ts.startServer(t)
	ts.prepareData(t)
	defer ts.stopServer(t)
	se, err := session.CreateSession(ts.store)
	require.NoError(t, err)

	form := make(url.Values)
	form.Set("log_level", "error")
	form.Set("tidb_general_log", "1")
	form.Set("tidb_enable_async_commit", "1")
	form.Set("tidb_enable_1pc", "1")
	resp, err := ts.FormStatus("/settings", form)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, zap.ErrorLevel, log.GetLevel())
	require.Equal(t, "error", config.GetGlobalConfig().Log.Level)
	require.True(t, vardef.ProcessGeneralLog.Load())
	val, err := se.GetSessionVars().GetGlobalSystemVar(context.Background(), vardef.TiDBEnableAsyncCommit)
	require.NoError(t, err)
	require.Equal(t, vardef.On, val)
	val, err = se.GetSessionVars().GetGlobalSystemVar(context.Background(), vardef.TiDBEnable1PC)
	require.NoError(t, err)
	require.Equal(t, vardef.On, val)

	form = make(url.Values)
	form.Set("log_level", "fatal")
	form.Set("tidb_general_log", "0")
	form.Set("tidb_enable_async_commit", "0")
	form.Set("tidb_enable_1pc", "0")
	resp, err = ts.FormStatus("/settings", form)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
	require.False(t, vardef.ProcessGeneralLog.Load())
	require.Equal(t, zap.FatalLevel, log.GetLevel())
	require.Equal(t, "fatal", config.GetGlobalConfig().Log.Level)
	val, err = se.GetSessionVars().GetGlobalSystemVar(context.Background(), vardef.TiDBEnableAsyncCommit)
	require.NoError(t, err)
	require.Equal(t, vardef.Off, val)
	val, err = se.GetSessionVars().GetGlobalSystemVar(context.Background(), vardef.TiDBEnable1PC)
	require.NoError(t, err)
	require.Equal(t, vardef.Off, val)
	form.Set("log_level", os.Getenv("log_level"))

	// test ddl_slow_threshold
	form = make(url.Values)
	form.Set("ddl_slow_threshold", "200")
	resp, err = ts.FormStatus("/settings", form)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, uint32(200), atomic.LoadUint32(&vardef.DDLSlowOprThreshold))

	// test check_mb4_value_in_utf8
	db, err := sql.Open("mysql", ts.GetDSN())
	require.NoError(t, err)
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()
	dbt := testkit.NewDBTestKit(t, db)

	dbt.MustExec("create database tidb_test;")
	dbt.MustExec("use tidb_test;")
	dbt.MustExec("drop table if exists t2;")
	dbt.MustExec("create table t2(a varchar(100) charset utf8);")
	form.Set("check_mb4_value_in_utf8", "1")
	resp, err = ts.FormStatus("/settings", form)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, true, config.GetGlobalConfig().Instance.CheckMb4ValueInUTF8.Load())
	txn1, err := dbt.GetDB().Begin()
	require.NoError(t, err)
	_, err = txn1.Exec("insert t2 values (unhex('F0A48BAE'));")
	require.Error(t, err)
	err = txn1.Commit()
	require.NoError(t, err)

	// Disable CheckMb4ValueInUTF8.
	form = make(url.Values)
	form.Set("check_mb4_value_in_utf8", "0")
	resp, err = ts.FormStatus("/settings", form)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, false, config.GetGlobalConfig().Instance.CheckMb4ValueInUTF8.Load())
	dbt.MustExec("insert t2 values (unhex('f09f8c80'));")

	// test deadlock_history_capacity
	deadlockhistory.GlobalDeadlockHistory.Resize(10)
	for range 10 {
		deadlockhistory.GlobalDeadlockHistory.Push(dummyRecord())
	}
	form = make(url.Values)
	form.Set("deadlock_history_capacity", "5")
	resp, err = ts.FormStatus("/settings", form)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, 5, len(deadlockhistory.GlobalDeadlockHistory.GetAll()))
	require.Equal(t, uint64(6), deadlockhistory.GlobalDeadlockHistory.GetAll()[0].ID)
	require.Equal(t, uint64(10), deadlockhistory.GlobalDeadlockHistory.GetAll()[4].ID)
	deadlockhistory.GlobalDeadlockHistory.Push(dummyRecord())
	require.Equal(t, 5, len(deadlockhistory.GlobalDeadlockHistory.GetAll()))
	require.Equal(t, uint64(7), deadlockhistory.GlobalDeadlockHistory.GetAll()[0].ID)
	require.Equal(t, uint64(11), deadlockhistory.GlobalDeadlockHistory.GetAll()[4].ID)
	form = make(url.Values)
	form.Set("deadlock_history_capacity", "6")
	resp, err = ts.FormStatus("/settings", form)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	deadlockhistory.GlobalDeadlockHistory.Push(dummyRecord())
	require.Equal(t, 6, len(deadlockhistory.GlobalDeadlockHistory.GetAll()))
	require.Equal(t, uint64(7), deadlockhistory.GlobalDeadlockHistory.GetAll()[0].ID)
	require.Equal(t, uint64(12), deadlockhistory.GlobalDeadlockHistory.GetAll()[5].ID)

	// test deadlock_history_collect_retryable
	form = make(url.Values)
	form.Set("deadlock_history_collect_retryable", "true")
	resp, err = ts.FormStatus("/settings", form)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.True(t, config.GetGlobalConfig().PessimisticTxn.DeadlockHistoryCollectRetryable)
	form = make(url.Values)
	form.Set("deadlock_history_collect_retryable", "false")
	resp, err = ts.FormStatus("/settings", form)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.False(t, config.GetGlobalConfig().PessimisticTxn.DeadlockHistoryCollectRetryable)
	form = make(url.Values)
	form.Set("deadlock_history_collect_retryable", "123")
	resp, err = ts.FormStatus("/settings", form)
	require.NoError(t, err)
	require.Equal(t, 400, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	// restore original value.
	config.GetGlobalConfig().Instance.CheckMb4ValueInUTF8.Store(true)
}

func TestAllServerInfo(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()
	ts.startServer(t)
	defer ts.stopServer(t)
	resp, err := ts.FetchStatus("/info/all")
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	decoder := json.NewDecoder(resp.Body)

	clusterInfo := tikvhandler.ClusterServerInfo{}
	err = decoder.Decode(&clusterInfo)
	require.NoError(t, err)

	require.True(t, clusterInfo.IsAllServerVersionConsistent)
	require.Equal(t, 1, clusterInfo.ServersNum)

	store := ts.server.NewTikvHandlerTool().Store.(kv.Storage)
	do, err := session.GetDomain(store)
	require.NoError(t, err)
	ddl := do.DDL()
	require.Equal(t, ddl.GetID(), clusterInfo.OwnerID)
	serverInfo, ok := clusterInfo.AllServersInfo[ddl.GetID()]
	require.Equal(t, true, ok)

	cfg := config.GetGlobalConfig()
	require.Equal(t, cfg.AdvertiseAddress, serverInfo.IP)
	require.Equal(t, cfg.Status.StatusPort, serverInfo.StatusPort)
	require.Equal(t, cfg.Lease, serverInfo.Lease)
	require.Equal(t, mysql.ServerVersion, serverInfo.Version)
	require.Equal(t, versioninfo.TiDBGitHash, serverInfo.GitHash)
	require.Equal(t, ddl.GetID(), serverInfo.ID)
}

func TestRegionsFromMeta(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()
	ts.startServer(t)
	defer ts.stopServer(t)
	resp, err := ts.FetchStatus("/regions/meta")
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Verify the resp body.
	decoder := json.NewDecoder(resp.Body)
	metas := make([]handler.RegionMeta, 0)
	err = decoder.Decode(&metas)
	require.NoError(t, err)
	for _, m := range metas {
		require.True(t, m.ID != 0)
	}

	// test no panic
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/server/errGetRegionByIDEmpty", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/server/errGetRegionByIDEmpty"))
	}()
	resp1, err := ts.FetchStatus("/regions/meta")
	require.NoError(t, err)
	defer func() { require.NoError(t, resp1.Body.Close()) }()
}

func TestTiFlashReplica(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()
	ts.startServer(t)
	ts.prepareData(t)
	defer ts.stopServer(t)

	db, err := sql.Open("mysql", ts.GetDSN())
	require.NoError(t, err)
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()
	dbt := testkit.NewDBTestKit(t, db)

	defer func(originGC bool) {
		if originGC {
			ddlutil.EmulatorGCEnable()
		} else {
			ddlutil.EmulatorGCDisable()
		}
	}(ddlutil.IsEmulatorGCEnable())

	// Disable emulator GC.
	// Otherwise emulator GC will delete table record as soon as possible after execute drop table DDL.
	ddlutil.EmulatorGCDisable()
	gcTimeFormat := "20060102-15:04:05 -0700 MST"
	timeBeforeDrop := time.Now().Add(0 - 48*60*60*time.Second).Format(gcTimeFormat)
	safePointSQL := `INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('tikv_gc_safe_point', '%[1]s', ''),('tikv_gc_enable','true','')
			       ON DUPLICATE KEY
			       UPDATE variable_value = '%[1]s'`
	// Set GC safe point and enable GC.
	dbt.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))

	resp, err := ts.FetchStatus("/tiflash/replica-deprecated")
	require.NoError(t, err)
	decoder := json.NewDecoder(resp.Body)
	var data []tikvhandler.TableFlashReplicaInfo
	err = decoder.Decode(&data)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, 0, len(data))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount"))
	}()

	dbt.MustExec("use tidb")
	dbt.MustExec("alter table test set tiflash replica 2 location labels 'a','b';")

	resp, err = ts.FetchStatus("/tiflash/replica-deprecated")
	require.NoError(t, err)
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&data)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, 1, len(data))
	require.Equal(t, uint64(2), data[0].ReplicaCount)
	require.Equal(t, "a,b", strings.Join(data[0].LocationLabels, ","))
	require.Equal(t, false, data[0].Available)

	resp, err = ts.PostStatus("/tiflash/replica-deprecated", "application/json", bytes.NewBuffer([]byte(`{"id":184,"region_count":3,"flash_region_count":3}`)))
	require.NoError(t, err)
	require.NotNil(t, resp)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, "[schema:1146]Table which ID = 184 does not exist.", string(body))

	tbl, err := ts.domain.InfoSchema().TableByName(context.Background(), ast.NewCIStr("tidb"), ast.NewCIStr("test"))
	require.NoError(t, err)
	req := fmt.Sprintf(`{"id":%d,"region_count":3,"flash_region_count":3}`, tbl.Meta().ID)
	resp, err = ts.PostStatus("/tiflash/replica-deprecated", "application/json", bytes.NewBuffer([]byte(req)))
	require.NoError(t, err)
	require.NotNil(t, resp)
	body, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, "", string(body))

	resp, err = ts.FetchStatus("/tiflash/replica-deprecated")
	require.NoError(t, err)
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&data)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, 1, len(data))
	require.Equal(t, uint64(2), data[0].ReplicaCount)
	require.Equal(t, "a,b", strings.Join(data[0].LocationLabels, ","))
	require.Equal(t, true, data[0].Available)

	// Should not take effect.
	dbt.MustExec("alter table test set tiflash replica 2 location labels 'a','b';")
	checkFunc := func() {
		resp, err := ts.FetchStatus("/tiflash/replica-deprecated")
		require.NoError(t, err)
		decoder = json.NewDecoder(resp.Body)
		err = decoder.Decode(&data)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, 1, len(data))
		require.Equal(t, uint64(2), data[0].ReplicaCount)
		require.Equal(t, "a,b", strings.Join(data[0].LocationLabels, ","))
		require.Equal(t, true, data[0].Available)
	}

	// Test for get dropped table tiflash replica info.
	dbt.MustExec("drop table test")
	checkFunc()

	// Test unique table id replica info.
	dbt.MustExec("flashback table test")
	checkFunc()
	dbt.MustExec("drop table test")
	checkFunc()
	dbt.MustExec("flashback table test")
	checkFunc()

	// Test for partition table.
	dbt.MustExec("alter table pt set tiflash replica 2 location labels 'a','b';")
	dbt.MustExec("alter table test set tiflash replica 0;")
	resp, err = ts.FetchStatus("/tiflash/replica-deprecated")
	require.NoError(t, err)
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&data)
	require.NoError(t, err)
	err = resp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, 3, len(data))
	require.Equal(t, uint64(2), data[0].ReplicaCount)
	require.Equal(t, "a,b", strings.Join(data[0].LocationLabels, ","))
	require.Equal(t, false, data[0].Available)

	pid0 := data[0].ID
	pid1 := data[1].ID
	pid2 := data[2].ID

	// Mock for partition 1 replica was available.
	req = fmt.Sprintf(`{"id":%d,"region_count":3,"flash_region_count":3}`, pid1)
	resp, err = ts.PostStatus("/tiflash/replica-deprecated", "application/json", bytes.NewBuffer([]byte(req)))
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	resp, err = ts.FetchStatus("/tiflash/replica-deprecated")
	require.NoError(t, err)
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&data)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, 3, len(data))
	require.Equal(t, false, data[0].Available)
	require.Equal(t, true, data[1].Available)
	require.Equal(t, false, data[2].Available)

	// Mock for partition 0,2 replica was available.
	req = fmt.Sprintf(`{"id":%d,"region_count":3,"flash_region_count":3}`, pid0)
	resp, err = ts.PostStatus("/tiflash/replica-deprecated", "application/json", bytes.NewBuffer([]byte(req)))
	require.NoError(t, err)
	err = resp.Body.Close()
	require.NoError(t, err)
	req = fmt.Sprintf(`{"id":%d,"region_count":3,"flash_region_count":3}`, pid2)
	resp, err = ts.PostStatus("/tiflash/replica-deprecated", "application/json", bytes.NewBuffer([]byte(req)))
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	checkFunc = func() {
		resp, err := ts.FetchStatus("/tiflash/replica-deprecated")
		require.NoError(t, err)
		decoder = json.NewDecoder(resp.Body)
		err = decoder.Decode(&data)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, 3, len(data))
		require.Equal(t, true, data[0].Available)
		require.Equal(t, true, data[1].Available)
		require.Equal(t, true, data[2].Available)
	}

	// Test for get truncated table tiflash replica info.
	dbt.MustExec("truncate table pt")
	dbt.MustExec("alter table pt set tiflash replica 0;")
	checkFunc()
}

func TestDebugRoutes(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()
	ts.startServer(t)
	defer ts.stopServer(t)

	debugRoutes := []string{
		"/debug/pprof/",
		"/debug/pprof/heap?debug=1",
		"/debug/pprof/goroutine?debug=1",
		"/debug/pprof/goroutine?debug=2",
		"/debug/pprof/allocs?debug=1",
		"/debug/pprof/block?debug=1",
		"/debug/pprof/threadcreate?debug=1",
		"/debug/pprof/cmdline",
		"/debug/pprof/profile?seconds=5",
		"/debug/pprof/mutex?debug=1",
		"/debug/pprof/symbol",
		"/debug/pprof/trace",
		"/debug/gogc",
		// "/debug/zip", // this creates unexpected goroutines which will make goleak complain, so we skip it for now
		"/debug/ballast-object-sz",
	}
	for _, route := range debugRoutes {
		resp, err := ts.FetchStatus(route)
		require.NoError(t, err, fmt.Sprintf("GET route %s failed", route))
		require.Equal(t, http.StatusOK, resp.StatusCode, fmt.Sprintf("GET route %s failed", route))
		require.NoError(t, resp.Body.Close())
	}
}

func TestFailpointHandler(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()

	// start server without enabling failpoint integration
	ts.startServer(t)
	defer ts.stopServer(t)
	resp, err := ts.FetchStatus("/fail/")
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
	ts.stopServer(t)

	// enable failpoint integration and start server
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/server/enableTestAPI", "return"))
	defer func() { require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/server/enableTestAPI")) }()
	ts.startServer(t)
	resp, err = ts.FetchStatus("/fail/")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.True(t, strings.Contains(string(b), "github.com/pingcap/tidb/pkg/server/enableTestAPI=return"))
	require.NoError(t, resp.Body.Close())
}

func TestTestHandler(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()

	// start server without enabling failpoint integration
	ts.startServer(t)
	defer ts.stopServer(t)
	resp, err := ts.FetchStatus("/test")
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
	ts.stopServer(t)

	// enable failpoint integration and start server
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/server/enableTestAPI", "return"))
	defer func() { require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/server/enableTestAPI")) }()
	ts.startServer(t)

	resp, err = ts.FetchStatus("/test/gc/gc")
	require.NoError(t, err)
	err = resp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)

	resp, err = ts.FetchStatus("/test/gc/resolvelock")
	require.NoError(t, err)
	err = resp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)

	resp, err = ts.FetchStatus("/test/gc/resolvelock?safepoint=a")
	require.NoError(t, err)
	err = resp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)

	resp, err = ts.FetchStatus("/test/gc/resolvelock?physical=1")
	require.NoError(t, err)
	err = resp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)

	resp, err = ts.FetchStatus("/test/gc/resolvelock?physical=true")
	require.NoError(t, err)
	err = resp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)

	resp, err = ts.FetchStatus("/test/gc/resolvelock?safepoint=10000&physical=true")
	require.NoError(t, err)
	err = resp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestServerInfo(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()
	ts.startServer(t)
	defer ts.stopServer(t)
	resp, err := ts.FetchStatus("/info")
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	decoder := json.NewDecoder(resp.Body)

	info := tikvhandler.ServerInfo{}
	err = decoder.Decode(&info)
	require.NoError(t, err)

	cfg := config.GetGlobalConfig()
	require.True(t, info.IsOwner)
	require.Equal(t, cfg.AdvertiseAddress, info.IP)
	require.Equal(t, cfg.Status.StatusPort, info.StatusPort)
	require.Equal(t, cfg.Lease, info.Lease)
	require.Equal(t, mysql.ServerVersion, info.Version)
	require.Equal(t, versioninfo.TiDBGitHash, info.GitHash)

	store := ts.server.NewTikvHandlerTool().Store.(kv.Storage)
	do, err := session.GetDomain(store)
	require.NoError(t, err)
	d := do.DDL()
	require.Equal(t, d.GetID(), info.ID)
}

func TestGetSchemaStorage(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()
	ts.startServer(t)
	ts.prepareData(t)
	defer ts.stopServer(t)

	do := ts.domain
	h := do.StatsHandle()
	do.SetStatsUpdating(true)

	tk := testkit.NewTestKit(t, ts.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int, d int, e char(5), index idx(e))")
	testutil.HandleNextDDLEventWithTxn(h)
	tk.MustExec(`insert into t(c, d, e) values(1, 2, "c"), (2, 3, "d"), (3, 4, "e")`)
	h.FlushStats()

	resp, err := ts.FetchStatus("/schema_storage/test")
	require.NoError(t, err)
	decoder := json.NewDecoder(resp.Body)
	var tables []*tikvhandler.SchemaTableStorage
	err = decoder.Decode(&tables)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Len(t, tables, 1)
	expects := []string{`t`}
	names := make([]string, len(tables))
	for i, v := range tables {
		names[i] = v.TableName
	}

	sort.Strings(names)
	require.Equal(t, expects, names)
	require.Equal(t, []int64{3, 16, 48, 0, 0, 0}, []int64{
		tables[0].TableRows,
		tables[0].AvgRowLength,
		tables[0].DataLength,
		tables[0].MaxDataLength,
		tables[0].IndexLength,
		tables[0].DataFree,
	})
}

func TestTTL(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()
	ts.startServer(t)
	defer ts.stopServer(t)

	db, err := sql.Open("mysql", ts.GetDSN())
	require.NoError(t, err)
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()
	dbt := testkit.NewDBTestKit(t, db)
	dbt.MustExec("create database test_ttl")
	dbt.MustExec("use test_ttl")
	dbt.MustExec("create table t1(t timestamp) TTL=`t` + interval 1 day")

	getJobCnt := func(status string) int {
		selectSQL := "select count(1) from mysql.tidb_ttl_job_history"
		if status != "" {
			selectSQL += " where status = '" + status + "'"
		}

		rs, err := db.Query(selectSQL)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, rs.Close())
		}()

		cnt := -1
		rowNum := 0
		for rs.Next() {
			rowNum++
			require.Equal(t, 1, rowNum)
			require.NoError(t, rs.Scan(&cnt))
		}
		require.NoError(t, rs.Err())
		return cnt
	}

	waitAllJobsFinish := func() {
		start := time.Now()
		for time.Since(start) < time.Minute {
			cnt := getJobCnt("running")
			if cnt == 0 {
				return
			}
		}
		require.Fail(t, "timeout for waiting job finished")
	}

	doTrigger := func(db, tb string) (map[string]any, error) {
		resp, err := ts.PostStatus(fmt.Sprintf("/test/ttl/trigger/%s/%s", db, tb), "application/json", nil)
		if err != nil {
			return nil, err
		}

		defer func() {
			require.NoError(t, resp.Body.Close())
		}()

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		if resp.StatusCode != 200 {
			return nil, errors.Errorf("http status: %s, %s", resp.Status, body)
		}

		var obj map[string]any
		require.NoError(t, json.Unmarshal(body, &obj))
		return obj, nil
	}

	expectedJobCnt := 1
	obj, err := doTrigger("test_ttl", "t1")
	require.NoError(t, err)
	if err != nil {
		// if error returns, may be a job is running, we should skip it and have a next try when it stopped
		require.Equal(t, expectedJobCnt, getJobCnt(""))
		waitAllJobsFinish()
		obj, err = doTrigger("test_ttl", "t1")
		require.NoError(t, err)
		expectedJobCnt++
	}

	_, ok := obj["table_result"]
	require.True(t, ok)
	require.Equal(t, expectedJobCnt, getJobCnt(""))

	// error case, table not exist
	obj, err = doTrigger("test_ttl", "t2")
	require.Nil(t, obj)
	require.EqualError(t, err, "http status: 400 Bad Request, table test_ttl.t2 not exists")
}

func TestGC(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()
	ts.startServer(t)
	defer ts.stopServer(t)

	var data url.Values
	resp, err := ts.FormStatus("/txn-gc-states", data)
	require.NoError(t, err)
	require.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)

	resp, err = ts.FetchStatus("/txn-gc-states")
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Verify the resp body.
	decoder := json.NewDecoder(resp.Body)
	var state gc.GCState
	err = decoder.Decode(&state)
	require.NoError(t, err)

	var empty gc.GCState
	require.NotEqual(t, empty, state)
}
