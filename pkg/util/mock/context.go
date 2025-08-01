// Copyright 2015 PingCAP, Inc.
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

// Package mock is just for test only.
package mock

import (
	"context"
	"fmt"
	"iter"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/domain/sqlsvrapi"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/expression/sessionexpr"
	"github.com/pingcap/tidb/pkg/extension"
	infoschema "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/infoschema/validatorapi"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/session/cursor"
	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionstates"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics/handle/usage/indexusage"
	"github.com/pingcap/tidb/pkg/table/tblctx"
	"github.com/pingcap/tidb/pkg/table/tblsession"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	rangerctx "github.com/pingcap/tidb/pkg/util/ranger/context"
	"github.com/pingcap/tidb/pkg/util/sli"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/topsql/stmtstats"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
)

var (
	_ sessionctx.Context  = (*Context)(nil)
	_ planctx.PlanContext = (*Context)(nil)
	_ sqlexec.SQLExecutor = (*Context)(nil)
)

// Context represents mocked sessionctx.Context.
type Context struct {
	planctx.EmptyPlanContextExtended
	*sessionexpr.ExprContext
	txn           wrapTxn // mock global variable
	dom           any
	schValidator  validatorapi.Validator
	Store         kv.Storage // mock global variable
	ctx           context.Context
	sm            sessmgr.Manager
	is            infoschema.MetaOnlyInfoSchema
	values        map[fmt.Stringer]any
	sessionVars   *variable.SessionVars
	tblctx        *tblsession.MutateContext
	cancel        context.CancelFunc
	pcache        sessionctx.SessionPlanCache
	level         kvrpcpb.DiskFullOpt
	inSandBoxMode bool
	isDDLOwner    bool
}

type wrapTxn struct {
	kv.Transaction
	tsFuture oracle.Future
}

func (txn *wrapTxn) validOrPending() bool {
	return txn.tsFuture != nil || (txn.Transaction != nil && txn.Transaction.Valid())
}

func (txn *wrapTxn) pending() bool {
	return txn.Transaction == nil && txn.tsFuture != nil
}

// Wait creates a new kvTransaction
func (txn *wrapTxn) Wait(_ context.Context, sctx sessionctx.Context) (kv.Transaction, error) {
	if !txn.validOrPending() {
		return txn, errors.AddStack(kv.ErrInvalidTxn)
	}
	if txn.pending() {
		ts, err := txn.tsFuture.Wait()
		if err != nil {
			return nil, err
		}
		kvTxn, err := sctx.GetStore().Begin(tikv.WithStartTS(ts))
		if err != nil {
			return nil, errors.Trace(err)
		}
		txn.Transaction = kvTxn
	}
	return txn, nil
}

func (txn *wrapTxn) Valid() bool {
	return txn.Transaction != nil && txn.Transaction.Valid()
}

func (txn *wrapTxn) CacheTableInfo(id int64, info *model.TableInfo) {
	if txn.Transaction == nil {
		return
	}
	txn.Transaction.CacheTableInfo(id, info)
}

func (txn *wrapTxn) GetTableInfo(id int64) *model.TableInfo {
	if txn.Transaction == nil {
		return nil
	}
	return txn.Transaction.GetTableInfo(id)
}

// Execute implements sqlexec.SQLExecutor Execute interface.
func (*Context) Execute(_ context.Context, _ string) ([]sqlexec.RecordSet, error) {
	return nil, errors.Errorf("Not Supported")
}

// ExecuteStmt implements sqlexec.SQLExecutor ExecuteStmt interface.
func (*Context) ExecuteStmt(_ context.Context, _ ast.StmtNode) (sqlexec.RecordSet, error) {
	return nil, errors.Errorf("Not Supported")
}

// ParseWithParams implements sqlexec.RestrictedSQLExecutor ParseWithParams interface.
func (*Context) ParseWithParams(_ context.Context, _ string, _ ...any) (ast.StmtNode, error) {
	return nil, errors.Errorf("Not Supported")
}

// ExecRestrictedStmt implements sqlexec.RestrictedSQLExecutor ExecRestrictedStmt interface.
func (*Context) ExecRestrictedStmt(_ context.Context, _ ast.StmtNode, _ ...sqlexec.OptionFuncAlias) ([]chunk.Row, []*resolve.ResultField, error) {
	return nil, nil, errors.Errorf("Not Supported")
}

// ExecRestrictedSQL implements sqlexec.RestrictedSQLExecutor ExecRestrictedSQL interface.
func (*Context) ExecRestrictedSQL(_ context.Context, _ []sqlexec.OptionFuncAlias, _ string, _ ...any) ([]chunk.Row, []*resolve.ResultField, error) {
	return nil, nil, errors.Errorf("Not Supported")
}

// GetSQLExecutor returns the SQLExecutor.
func (c *Context) GetSQLExecutor() sqlexec.SQLExecutor {
	return c
}

// GetRestrictedSQLExecutor returns the RestrictedSQLExecutor.
func (c *Context) GetRestrictedSQLExecutor() sqlexec.RestrictedSQLExecutor {
	return c
}

// ExecuteInternal implements sqlexec.SQLExecutor ExecuteInternal interface.
func (*Context) ExecuteInternal(_ context.Context, _ string, _ ...any) (sqlexec.RecordSet, error) {
	return nil, errors.Errorf("Not Supported")
}

// ShowProcess implements sessionctx.Context ShowProcess interface.
func (*Context) ShowProcess() *sessmgr.ProcessInfo {
	return &sessmgr.ProcessInfo{}
}

// SetIsDDLOwner sets return value of IsDDLOwner.
func (c *Context) SetIsDDLOwner(isOwner bool) {
	c.isDDLOwner = isOwner
}

// IsDDLOwner checks whether this session is DDL owner.
func (c *Context) IsDDLOwner() bool {
	return c.isDDLOwner
}

// SetValue implements sessionctx.Context SetValue interface.
func (c *Context) SetValue(key fmt.Stringer, value any) {
	c.values[key] = value
}

// Value implements sessionctx.Context Value interface.
func (c *Context) Value(key fmt.Stringer) any {
	value := c.values[key]
	return value
}

// ClearValue implements sessionctx.Context ClearValue interface.
func (c *Context) ClearValue(key fmt.Stringer) {
	delete(c.values, key)
}

// HasDirtyContent implements sessionctx.Context ClearValue interface.
func (*Context) HasDirtyContent(_ int64) bool {
	return false
}

// GetSessionVars implements the sessionctx.Context GetSessionVars interface.
func (c *Context) GetSessionVars() *variable.SessionVars {
	return c.sessionVars
}

// GetPlanCtx returns the PlanContext.
func (c *Context) GetPlanCtx() planctx.PlanContext {
	return c
}

// GetNullRejectCheckExprCtx gets the expression context with null rejected check.
func (c *Context) GetNullRejectCheckExprCtx() exprctx.ExprContext {
	return exprctx.WithNullRejectCheck(c)
}

// GetExprCtx returns the expression context of the session.
func (c *Context) GetExprCtx() exprctx.ExprContext {
	return c
}

// GetTableCtx returns the table.MutateContext
func (c *Context) GetTableCtx() tblctx.MutateContext {
	return c.tblctx
}

// GetDistSQLCtx returns the distsql context of the session
func (c *Context) GetDistSQLCtx() *distsqlctx.DistSQLContext {
	vars := c.GetSessionVars()
	sc := vars.StmtCtx

	return &distsqlctx.DistSQLContext{
		WarnHandler:                          sc.WarnHandler,
		InRestrictedSQL:                      sc.InRestrictedSQL,
		Client:                               c.GetClient(),
		EnabledRateLimitAction:               vars.EnabledRateLimitAction,
		EnableChunkRPC:                       vars.EnableChunkRPC,
		OriginalSQL:                          sc.OriginalSQL,
		KVVars:                               vars.KVVars,
		KvExecCounter:                        sc.KvExecCounter,
		SessionMemTracker:                    vars.MemTracker,
		Location:                             sc.TimeZone(),
		RuntimeStatsColl:                     sc.RuntimeStatsColl,
		SQLKiller:                            &vars.SQLKiller,
		CPUUsage:                             &vars.SQLCPUUsages,
		ErrCtx:                               sc.ErrCtx(),
		TiFlashReplicaRead:                   vars.TiFlashReplicaRead,
		TiFlashMaxThreads:                    vars.TiFlashMaxThreads,
		TiFlashMaxBytesBeforeExternalJoin:    vars.TiFlashMaxBytesBeforeExternalJoin,
		TiFlashMaxBytesBeforeExternalGroupBy: vars.TiFlashMaxBytesBeforeExternalGroupBy,
		TiFlashMaxBytesBeforeExternalSort:    vars.TiFlashMaxBytesBeforeExternalSort,
		TiFlashMaxQueryMemoryPerNode:         vars.TiFlashMaxQueryMemoryPerNode,
		TiFlashQuerySpillRatio:               vars.TiFlashQuerySpillRatio,
		TiFlashHashJoinVersion:               vars.TiFlashHashJoinVersion,
		ResourceGroupName:                    sc.ResourceGroupName,
		ExecDetails:                          &sc.SyncExecDetails,
	}
}

// GetRangerCtx returns the context used in `ranger` related functions
func (c *Context) GetRangerCtx() *rangerctx.RangerContext {
	return &rangerctx.RangerContext{
		ExprCtx: c.GetExprCtx(),
		TypeCtx: c.GetSessionVars().StmtCtx.TypeCtx(),
		ErrCtx:  c.GetSessionVars().StmtCtx.ErrCtx(),

		RegardNULLAsPoint:        c.GetSessionVars().RegardNULLAsPoint,
		OptPrefixIndexSingleScan: c.GetSessionVars().OptPrefixIndexSingleScan,
		OptimizerFixControl:      c.GetSessionVars().OptimizerFixControl,

		PlanCacheTracker:     &c.GetSessionVars().StmtCtx.PlanCacheTracker,
		RangeFallbackHandler: &c.GetSessionVars().StmtCtx.RangeFallbackHandler,
	}
}

// GetBuildPBCtx returns the `ToPB` context of the session
func (c *Context) GetBuildPBCtx() *planctx.BuildPBContext {
	return &planctx.BuildPBContext{
		ExprCtx: c.GetExprCtx(),
		Client:  c.GetClient(),

		TiFlashFastScan:                    c.GetSessionVars().TiFlashFastScan,
		TiFlashFineGrainedShuffleBatchSize: c.GetSessionVars().TiFlashFineGrainedShuffleBatchSize,

		// the following fields are used to build `expression.PushDownContext`.
		// TODO: it'd be better to embed `expression.PushDownContext` in `BuildPBContext`. But `expression` already
		// depends on this package, so we need to move `expression.PushDownContext` to a standalone package first.
		GroupConcatMaxLen: c.GetSessionVars().GroupConcatMaxLen,
		InExplainStmt:     c.GetSessionVars().StmtCtx.InExplainStmt,
		WarnHandler:       c.GetSessionVars().StmtCtx.WarnHandler,
		ExtraWarnghandler: c.GetSessionVars().StmtCtx.ExtraWarnHandler,
	}
}

// Txn implements sessionctx.Context Txn interface.
func (c *Context) Txn(active bool) (kv.Transaction, error) {
	if active {
		if !c.txn.validOrPending() {
			err := c.newTxn(context.Background())
			if err != nil {
				return nil, err
			}
		}
	}
	return &c.txn, nil
}

// GetClient implements sessionctx.Context GetClient interface.
func (c *Context) GetClient() kv.Client {
	if c.Store == nil {
		return nil
	}
	return c.Store.GetClient()
}

// GetMPPClient implements sessionctx.Context GetMPPClient interface.
func (c *Context) GetMPPClient() kv.MPPClient {
	if c.Store == nil {
		return nil
	}
	return c.Store.GetMPPClient()
}

// GetInfoSchema implements sessionctx.Context GetInfoSchema interface.
func (c *Context) GetInfoSchema() infoschema.MetaOnlyInfoSchema {
	vars := c.GetSessionVars()
	if snap, ok := vars.SnapshotInfoschema.(infoschema.MetaOnlyInfoSchema); ok {
		return snap
	}
	if vars.TxnCtx != nil && vars.InTxn() {
		if is, ok := vars.TxnCtx.InfoSchema.(infoschema.MetaOnlyInfoSchema); ok {
			return is
		}
	}
	if c.is == nil {
		c.is = MockInfoschema(nil)
	}
	return c.is
}

// MockInfoschema only serves for test.
var MockInfoschema func(tbList []*model.TableInfo) infoschema.MetaOnlyInfoSchema

// GetLatestInfoSchema returns the latest information schema in domain
func (c *Context) GetLatestInfoSchema() infoschema.MetaOnlyInfoSchema {
	if c.is == nil {
		c.is = MockInfoschema(nil)
	}
	return c.is
}

// GetLatestISWithoutSessExt implements sessionctx.Context GetLatestISWithoutSessExt interface.
func (c *Context) GetLatestISWithoutSessExt() infoschema.MetaOnlyInfoSchema {
	return c.GetLatestInfoSchema()
}

// GetSQLServer implements sessionctx.Context GetSQLServer interface.
func (c *Context) GetSQLServer() sqlsvrapi.Server {
	return c.dom.(sqlsvrapi.Server)
}

// IsCrossKS implements sessionctx.Context IsCrossKS interface.
func (*Context) IsCrossKS() bool {
	return false
}

// GetSchemaValidator implements sessionctx.Context GetSchemaValidator interface.
func (c *Context) GetSchemaValidator() validatorapi.Validator {
	return c.schValidator
}

// GetBuiltinFunctionUsage implements sessionctx.Context GetBuiltinFunctionUsage interface.
func (*Context) GetBuiltinFunctionUsage() map[string]uint32 {
	return make(map[string]uint32)
}

// BuiltinFunctionUsageInc implements sessionctx.Context.
func (*Context) BuiltinFunctionUsageInc(_ string) {}

// GetGlobalSysVar implements GlobalVarAccessor GetGlobalSysVar interface.
func (*Context) GetGlobalSysVar(_ sessionctx.Context, name string) (string, error) {
	v := variable.GetSysVar(name)
	if v == nil {
		return "", variable.ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	return v.Value, nil
}

// SetGlobalSysVar implements GlobalVarAccessor SetGlobalSysVar interface.
func (*Context) SetGlobalSysVar(_ sessionctx.Context, name string, value string) error {
	v := variable.GetSysVar(name)
	if v == nil {
		return variable.ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	v.Value = value
	return nil
}

// GetSessionPlanCache implements the sessionctx.Context interface.
func (c *Context) GetSessionPlanCache() sessionctx.SessionPlanCache {
	return c.pcache
}

// newTxn Creates new transaction on the session context.
func (c *Context) newTxn(ctx context.Context) error {
	if c.Store == nil {
		logutil.Logger(ctx).Warn("mock.Context: No store is specified when trying to create new transaction. A fake transaction will be created. Note that this is unrecommended usage.")
		c.fakeTxn()
		return nil
	}
	if c.txn.Valid() {
		err := c.txn.Commit(c.ctx)
		if err != nil {
			return errors.Trace(err)
		}
	}

	txn, err := c.Store.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	c.txn.Transaction = txn
	return nil
}

// fakeTxn is used to let some tests pass in the context without an available kv.Storage. Once usages to access
// transactions without a kv.Storage are removed, this type should also be removed.
// New code should never use this.
type fakeTxn struct {
	// The inner should always be nil.
	kv.Transaction
	startTS uint64
}

func (t *fakeTxn) StartTS() uint64 {
	return t.startTS
}

func (*fakeTxn) SetDiskFullOpt(_ kvrpcpb.DiskFullOpt) {}

func (*fakeTxn) SetOption(_ int, _ any) {}

func (*fakeTxn) Get(ctx context.Context, _ kv.Key) ([]byte, error) {
	// Check your implementation if you meet this error. It's dangerous if some calculation relies on the data but the
	// read result is faked.
	logutil.Logger(ctx).Warn("mock.Context: No store is specified but trying to access data from a transaction.")
	return nil, nil
}

func (*fakeTxn) Valid() bool { return true }

func (c *Context) fakeTxn() {
	c.txn.Transaction = &fakeTxn{
		startTS: 1,
	}
}

// RefreshTxnCtx implements the sessionctx.Context interface.
func (c *Context) RefreshTxnCtx(ctx context.Context) error {
	return errors.Trace(c.newTxn(ctx))
}

// RollbackTxn indicates an expected call of RollbackTxn.
func (c *Context) RollbackTxn(_ context.Context) {
	defer c.sessionVars.SetInTxn(false)
	if c.txn.Valid() {
		terror.Log(c.txn.Rollback())
	}
}

// CommitTxn indicates an expected call of CommitTxn.
func (c *Context) CommitTxn(ctx context.Context) error {
	defer c.sessionVars.SetInTxn(false)
	c.txn.SetDiskFullOpt(c.level)
	if c.txn.Valid() {
		return c.txn.Commit(ctx)
	}
	return nil
}

// GetStore gets the store of session.
func (c *Context) GetStore() kv.Storage {
	return c.Store
}

// GetSessionManager implements the sessionctx.Context interface.
func (c *Context) GetSessionManager() sessmgr.Manager {
	return c.sm
}

// SetSessionManager set the session manager.
func (c *Context) SetSessionManager(sm sessmgr.Manager) {
	c.sm = sm
}

// Cancel implements the Session interface.
func (c *Context) Cancel() {
	c.cancel()
}

// GoCtx returns standard sessionctx.Context that bind with current transaction.
func (c *Context) GoCtx() context.Context {
	return c.ctx
}

// UpdateColStatsUsage updates the column stats usage.
func (*Context) UpdateColStatsUsage(_ iter.Seq[model.TableItemID]) {}

// StoreIndexUsage strores the index usage information.
func (*Context) StoreIndexUsage(_ int64, _ int64, _ int64) {}

// GetTxnWriteThroughputSLI implements the sessionctx.Context interface.
func (*Context) GetTxnWriteThroughputSLI() *sli.TxnWriteThroughputSLI {
	return &sli.TxnWriteThroughputSLI{}
}

// StmtCommit implements the sessionctx.Context interface.
func (*Context) StmtCommit(context.Context) {}

// StmtRollback implements the sessionctx.Context interface.
func (*Context) StmtRollback(context.Context, bool) {}

// AddTableLock implements the sessionctx.Context interface.
func (*Context) AddTableLock(_ []model.TableLockTpInfo) {
}

// ReleaseTableLocks implements the sessionctx.Context interface.
func (*Context) ReleaseTableLocks(_ []model.TableLockTpInfo) {
}

// ReleaseTableLockByTableIDs implements the sessionctx.Context interface.
func (*Context) ReleaseTableLockByTableIDs(_ []int64) {
}

// CheckTableLocked implements the sessionctx.Context interface.
func (*Context) CheckTableLocked(_ int64) (bool, ast.TableLockType) {
	return false, ast.TableLockNone
}

// GetAllTableLocks implements the sessionctx.Context interface.
func (*Context) GetAllTableLocks() []model.TableLockTpInfo {
	return nil
}

// ReleaseAllTableLocks implements the sessionctx.Context interface.
func (*Context) ReleaseAllTableLocks() {
}

// HasLockedTables implements the sessionctx.Context interface.
func (*Context) HasLockedTables() bool {
	return false
}

// PrepareTSFuture implements the sessionctx.Context interface.
func (c *Context) PrepareTSFuture(_ context.Context, future oracle.Future, _ string) error {
	c.txn.Transaction = nil
	c.txn.tsFuture = future
	return nil
}

// GetPreparedTxnFuture returns the TxnFuture if it is prepared.
// It returns nil otherwise.
func (c *Context) GetPreparedTxnFuture() sessionctx.TxnFuture {
	if !c.txn.validOrPending() {
		return nil
	}
	return &c.txn
}

// GetStmtStats implements the sessionctx.Context interface.
func (*Context) GetStmtStats() *stmtstats.StatementStats {
	return nil
}

// GetAdvisoryLock acquires an advisory lock
func (*Context) GetAdvisoryLock(_ string, _ int64) error {
	return nil
}

// IsUsedAdvisoryLock check if a lock name is in use
func (*Context) IsUsedAdvisoryLock(_ string) uint64 {
	return 0
}

// ReleaseAdvisoryLock releases an advisory lock
func (*Context) ReleaseAdvisoryLock(_ string) bool {
	return true
}

// ReleaseAllAdvisoryLocks releases all advisory locks
func (*Context) ReleaseAllAdvisoryLocks() int {
	return 0
}

// EncodeStates implements the sessionapi.Session interface
func (*Context) EncodeStates(context.Context, *sessionstates.SessionStates) error {
	return errors.Errorf("Not Supported")
}

// DecodeStates implements the sessionapi.Session interface
func (*Context) DecodeStates(context.Context, *sessionstates.SessionStates) error {
	return errors.Errorf("Not Supported")
}

// GetExtensions returns the `*extension.SessionExtensions` object
func (*Context) GetExtensions() *extension.SessionExtensions {
	return nil
}

// EnableSandBoxMode enable the sandbox mode.
func (c *Context) EnableSandBoxMode() {
	c.inSandBoxMode = true
}

// DisableSandBoxMode enable the sandbox mode.
func (c *Context) DisableSandBoxMode() {
	c.inSandBoxMode = false
}

// InSandBoxMode indicates that this Session is in sandbox mode
func (c *Context) InSandBoxMode() bool {
	return c.inSandBoxMode
}

// SetInfoSchema is to set info shema for the test.
func (c *Context) SetInfoSchema(is infoschema.MetaOnlyInfoSchema) {
	c.is = is
}

// ResetSessionAndStmtTimeZone resets the timezone for session and statement.
func (c *Context) ResetSessionAndStmtTimeZone(tz *time.Location) {
	c.GetSessionVars().TimeZone = tz
	c.GetSessionVars().StmtCtx.SetTimeZone(tz)
}

// ReportUsageStats implements the sessionctx.Context interface.
func (*Context) ReportUsageStats() {}

// Close implements the sessionctx.Context interface.
func (*Context) Close() {}

// NewStmtIndexUsageCollector implements the sessionctx.Context interface
func (*Context) NewStmtIndexUsageCollector() *indexusage.StmtIndexUsageCollector {
	return nil
}

// GetCursorTracker implements the sessionctx.Context interface
func (*Context) GetCursorTracker() cursor.Tracker {
	return nil
}

// GetCommitWaitGroup implements the sessionctx.Context interface
func (*Context) GetCommitWaitGroup() *sync.WaitGroup {
	return nil
}

// BindDomainAndSchValidator bind domain into ctx.
func (c *Context) BindDomainAndSchValidator(dom any, validator validatorapi.Validator) {
	c.dom = dom
	c.schValidator = validator
}

// GetDomain get domain from ctx.
func (c *Context) GetDomain() any {
	return c.dom
}

// NewContextDeprecated creates a new mocked sessionctx.Context.
// Deprecated: This method is only used for some legacy code.
// DO NOT use mock.Context in new production code, and use the real Context instead.
func NewContextDeprecated() *Context {
	return newContext()
}

// newContext creates a new mocked sessionctx.Context.
func newContext() *Context {
	ctx, cancel := context.WithCancel(context.Background())
	sctx := &Context{
		values: make(map[fmt.Stringer]any),
		ctx:    ctx,
		cancel: cancel,
	}
	vars := variable.NewSessionVars(sctx)
	sctx.sessionVars = vars
	sctx.ExprContext = sessionexpr.NewExprContext(sctx)
	sctx.tblctx = tblsession.NewMutateContext(sctx)
	vars.InitChunkSize = 2
	vars.MaxChunkSize = 32
	vars.TimeZone = time.UTC
	vars.StmtCtx.SetTimeZone(time.UTC)
	vars.MemTracker.SetBytesLimit(-1)
	vars.DiskTracker.SetBytesLimit(-1)
	vars.StmtCtx.MemTracker, vars.StmtCtx.DiskTracker = memory.NewTracker(-1, -1), disk.NewTracker(-1, -1)
	vars.StmtCtx.MemTracker.AttachTo(vars.MemTracker)
	vars.StmtCtx.DiskTracker.AttachTo(vars.DiskTracker)
	vars.GlobalVarsAccessor = variable.NewMockGlobalAccessor()
	vars.EnablePaging = vardef.DefTiDBEnablePaging
	vars.MinPagingSize = vardef.DefMinPagingSize
	vars.EnableChunkRPC = true
	vars.DivPrecisionIncrement = vardef.DefDivPrecisionIncrement
	if err := sctx.GetSessionVars().SetSystemVar(vardef.MaxAllowedPacket, "67108864"); err != nil {
		panic(err)
	}
	if err := sctx.GetSessionVars().SetSystemVar(vardef.CharacterSetConnection, "utf8mb4"); err != nil {
		panic(err)
	}
	return sctx
}

// HookKeyForTest is as alias, used by context.WithValue.
// golint forbits using string type as key in context.WithValue.
type HookKeyForTest string
