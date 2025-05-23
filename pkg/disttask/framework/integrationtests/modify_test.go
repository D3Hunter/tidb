// Copyright 2024 PingCAP, Inc.
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

package integrationtests

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	mockexecute "github.com/pingcap/tidb/pkg/disttask/framework/mock/execute"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

type collectedRuntimeInfo struct {
	currTaskMeta        atomic.Pointer[[]byte]
	currTaskConcurrency atomic.Int64
	subtaskInfos        []subtaskRuntimeInfo
}

type subtaskRuntimeInfo struct {
	Step        proto.Step
	Concurrency int
}

func TestModifyTaskConcurrency(t *testing.T) {
	c := testutil.NewTestDXFContext(t, 1, 16, true)
	stepInfos := []testutil.StepInfo{
		{Step: proto.StepOne, SubtaskCnt: 2},
		{Step: proto.StepTwo, SubtaskCnt: 3},
	}
	schedulerExt := testutil.GetMockSchedulerExt(c.MockCtrl, testutil.SchedulerInfo{
		AllErrorRetryable: true,
		StepInfos:         stepInfos,
	})
	subtaskCh := make(chan struct{})
	runtimeInfo := collectedRuntimeInfo{}
	var (
		testModifyWhenSubtaskRun atomic.Bool
		modifyWaitCh             = make(chan struct{})
	)

	runSubtaskFn := func(ctx context.Context, subtask *proto.Subtask) error {
		if testModifyWhenSubtaskRun.Load() {
			<-modifyWaitCh
			<-modifyWaitCh
		}
		select {
		case <-subtaskCh:
			runtimeInfo.subtaskInfos = append(runtimeInfo.subtaskInfos, subtaskRuntimeInfo{
				Step:        subtask.Step,
				Concurrency: subtask.Concurrency,
			})
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	}

	executorExt := testutil.GetCommonTaskExecutorExt(c.MockCtrl, func(task *proto.Task) (execute.StepExecutor, error) {
		runtimeInfo.currTaskMeta.Store(&task.Meta)
		runtimeInfo.currTaskConcurrency.Store(int64(task.Concurrency))
		executor := mockexecute.NewMockStepExecutor(c.MockCtrl)
		executor.EXPECT().Init(gomock.Any()).Return(nil).AnyTimes()
		executor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).DoAndReturn(runSubtaskFn).AnyTimes()
		executor.EXPECT().GetStep().Return(task.Step).AnyTimes()
		executor.EXPECT().SetResource(gomock.Any()).AnyTimes()
		executor.EXPECT().Cleanup(gomock.Any()).Return(nil).AnyTimes()
		executor.EXPECT().RealtimeSummary().Return(nil).AnyTimes()
		executor.EXPECT().TaskMetaModified(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, newMeta []byte) error {
			runtimeInfo.currTaskMeta.Store(&newMeta)
			return nil
		}).AnyTimes()
		executor.EXPECT().ResourceModified(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, newResource *proto.StepResource) error {
			runtimeInfo.currTaskConcurrency.Store(newResource.CPU.Capacity())
			return nil
		}).AnyTimes()
		return executor, nil
	})
	testutil.RegisterExampleTask(t, schedulerExt, executorExt, testutil.GetCommonCleanUpRoutine(c.MockCtrl))

	resetRuntimeInfoFn := func() {
		runtimeInfo = collectedRuntimeInfo{}
	}

	t.Run("modify pending task concurrency", func(t *testing.T) {
		defer resetRuntimeInfoFn()
		var once sync.Once
		modifySyncCh := make(chan struct{})
		var theTask *proto.Task
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/disttask/framework/scheduler/beforeGetSchedulableTasks", func() {
			once.Do(func() {
				task, err := handle.SubmitTask(c.Ctx, "k1", proto.TaskTypeExample, 3, "", 0, []byte("init"))
				require.NoError(t, err)
				require.Equal(t, 3, task.Concurrency)
				require.NoError(t, c.TaskMgr.ModifyTaskByID(c.Ctx, task.ID, &proto.ModifyParam{
					PrevState: proto.TaskStatePending,
					Modifications: []proto.Modification{
						{Type: proto.ModifyConcurrency, To: 7},
					},
				}))
				theTask = task
				gotTask, err := c.TaskMgr.GetTaskBaseByID(c.Ctx, theTask.ID)
				require.NoError(t, err)
				require.Equal(t, proto.TaskStateModifying, gotTask.State)
				require.Equal(t, 3, gotTask.Concurrency)
				<-modifySyncCh
			})
		})
		modifySyncCh <- struct{}{}
		// finish subtasks
		for range 5 {
			subtaskCh <- struct{}{}
		}
		task2Base := testutil.WaitTaskDone(c.Ctx, t, theTask.Key)
		require.Equal(t, proto.TaskStateSucceed, task2Base.State)
		require.EqualValues(t, []subtaskRuntimeInfo{
			{Step: proto.StepOne, Concurrency: 7},
			{Step: proto.StepOne, Concurrency: 7},
			{Step: proto.StepTwo, Concurrency: 7},
			{Step: proto.StepTwo, Concurrency: 7},
			{Step: proto.StepTwo, Concurrency: 7},
		}, runtimeInfo.subtaskInfos)
	})

	t.Run("modify running task concurrency at step two", func(t *testing.T) {
		defer resetRuntimeInfoFn()
		var once sync.Once
		modifySyncCh := make(chan struct{})
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/disttask/framework/scheduler/beforeRefreshTask", func(task *proto.Task) {
			if task.State != proto.TaskStateRunning && task.Step != proto.StepTwo {
				return
			}
			once.Do(func() {
				require.NoError(t, c.TaskMgr.ModifyTaskByID(c.Ctx, task.ID, &proto.ModifyParam{
					PrevState: proto.TaskStateRunning,
					Modifications: []proto.Modification{
						{Type: proto.ModifyConcurrency, To: 7},
					},
				}))
				<-modifySyncCh
			})
		})
		task, err := handle.SubmitTask(c.Ctx, "k2", proto.TaskTypeExample, 3, "", 0, nil)
		require.NoError(t, err)
		require.Equal(t, 3, task.Concurrency)
		// finish StepOne
		subtaskCh <- struct{}{}
		subtaskCh <- struct{}{}
		// wait task move to 'modifying' state
		modifySyncCh <- struct{}{}
		// wait task move back to 'running' state
		require.Eventually(t, func() bool {
			gotTask, err2 := c.TaskMgr.GetTaskByID(c.Ctx, task.ID)
			require.NoError(t, err2)
			return gotTask.State == proto.TaskStateRunning
		}, 10*time.Second, 100*time.Millisecond)
		// finish StepTwo
		subtaskCh <- struct{}{}
		subtaskCh <- struct{}{}
		subtaskCh <- struct{}{}
		task2Base := testutil.WaitTaskDone(c.Ctx, t, task.Key)
		require.Equal(t, proto.TaskStateSucceed, task2Base.State)
		require.EqualValues(t, []subtaskRuntimeInfo{
			{Step: proto.StepOne, Concurrency: 3},
			{Step: proto.StepOne, Concurrency: 3},
			{Step: proto.StepTwo, Concurrency: 7},
			{Step: proto.StepTwo, Concurrency: 7},
			{Step: proto.StepTwo, Concurrency: 7},
		}, runtimeInfo.subtaskInfos)
	})

	t.Run("modify running task concurrency at second subtask of step two", func(t *testing.T) {
		defer resetRuntimeInfoFn()
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/beforeGetTaskByIDInRun",
			func(taskID int64) {
				if len(runtimeInfo.subtaskInfos) == 3 {
					require.NoError(t, c.TaskMgr.ModifyTaskByID(c.Ctx, taskID, &proto.ModifyParam{
						PrevState: proto.TaskStateRunning,
						Modifications: []proto.Modification{
							{Type: proto.ModifyConcurrency, To: 7},
						},
					}))
					// wait task move back to 'running' state
					require.Eventually(t, func() bool {
						gotTask, err2 := c.TaskMgr.GetTaskByID(c.Ctx, taskID)
						require.NoError(t, err2)
						return gotTask.State == proto.TaskStateRunning
					}, 10*time.Second, 100*time.Millisecond)
				}
			},
		)
		task, err := handle.SubmitTask(c.Ctx, "k2-2", proto.TaskTypeExample, 3, "", 0, nil)
		require.NoError(t, err)
		require.Equal(t, 3, task.Concurrency)
		for range 5 {
			subtaskCh <- struct{}{}
		}
		task2Base := testutil.WaitTaskDone(c.Ctx, t, task.Key)
		require.Equal(t, proto.TaskStateSucceed, task2Base.State)
		require.EqualValues(t, []subtaskRuntimeInfo{
			{Step: proto.StepOne, Concurrency: 3},
			{Step: proto.StepOne, Concurrency: 3},
			{Step: proto.StepTwo, Concurrency: 3},
			{Step: proto.StepTwo, Concurrency: 7},
			{Step: proto.StepTwo, Concurrency: 7},
		}, runtimeInfo.subtaskInfos)
	})

	t.Run("modify paused task concurrency", func(t *testing.T) {
		defer resetRuntimeInfoFn()
		var once sync.Once
		syncCh := make(chan struct{})
		var theTask *proto.Task
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/disttask/framework/scheduler/beforeGetSchedulableTasks", func() {
			once.Do(func() {
				task, err := handle.SubmitTask(c.Ctx, "k3", proto.TaskTypeExample, 3, "", 0, nil)
				require.NoError(t, err)
				require.Equal(t, 3, task.Concurrency)
				found, err := c.TaskMgr.PauseTask(c.Ctx, task.Key)
				require.NoError(t, err)
				require.True(t, found)
				theTask = task
				<-syncCh
			})
		})
		syncCh <- struct{}{}
		taskBase := testutil.WaitTaskDoneOrPaused(c.Ctx, t, theTask.Key)
		require.Equal(t, proto.TaskStatePaused, taskBase.State)
		require.NoError(t, c.TaskMgr.ModifyTaskByID(c.Ctx, theTask.ID, &proto.ModifyParam{
			PrevState: proto.TaskStatePaused,
			Modifications: []proto.Modification{
				{Type: proto.ModifyConcurrency, To: 7},
			},
		}))
		taskBase = testutil.WaitTaskDoneOrPaused(c.Ctx, t, theTask.Key)
		require.Equal(t, proto.TaskStatePaused, taskBase.State)
		found, err := c.TaskMgr.ResumeTask(c.Ctx, theTask.Key)
		require.NoError(t, err)
		require.True(t, found)
		// finish subtasks
		for range 5 {
			subtaskCh <- struct{}{}
		}
		task2Base := testutil.WaitTaskDone(c.Ctx, t, theTask.Key)
		require.Equal(t, proto.TaskStateSucceed, task2Base.State)
		require.EqualValues(t, []subtaskRuntimeInfo{
			{Step: proto.StepOne, Concurrency: 7},
			{Step: proto.StepOne, Concurrency: 7},
			{Step: proto.StepTwo, Concurrency: 7},
			{Step: proto.StepTwo, Concurrency: 7},
			{Step: proto.StepTwo, Concurrency: 7},
		}, runtimeInfo.subtaskInfos)
	})

	t.Run("modify pending task concurrency, but other owner already done it", func(t *testing.T) {
		defer resetRuntimeInfoFn()
		var once sync.Once
		modifySyncCh := make(chan struct{})
		var theTask *proto.Task
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/disttask/framework/scheduler/beforeGetSchedulableTasks", func() {
			once.Do(func() {
				task, err := handle.SubmitTask(c.Ctx, "k4", proto.TaskTypeExample, 3, "", 0, nil)
				require.NoError(t, err)
				require.Equal(t, 3, task.Concurrency)
				require.NoError(t, c.TaskMgr.ModifyTaskByID(c.Ctx, task.ID, &proto.ModifyParam{
					PrevState: proto.TaskStatePending,
					Modifications: []proto.Modification{
						{Type: proto.ModifyConcurrency, To: 7},
					},
				}))
				theTask = task
				gotTask, err := c.TaskMgr.GetTaskBaseByID(c.Ctx, theTask.ID)
				require.NoError(t, err)
				require.Equal(t, proto.TaskStateModifying, gotTask.State)
				require.Equal(t, 3, gotTask.Concurrency)
			})
		})
		var onceForRefresh sync.Once
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/disttask/framework/scheduler/afterRefreshTask",
			func(task *proto.Task) {
				onceForRefresh.Do(func() {
					require.Equal(t, proto.TaskStateModifying, task.State)
					taskClone := *task
					taskClone.Concurrency = 7
					require.NoError(t, c.TaskMgr.ModifiedTask(c.Ctx, &taskClone))
					gotTask, err := c.TaskMgr.GetTaskBaseByID(c.Ctx, task.ID)
					require.NoError(t, err)
					require.Equal(t, proto.TaskStatePending, gotTask.State)
					<-modifySyncCh
				})
			},
		)
		modifySyncCh <- struct{}{}
		// finish subtasks
		for range 5 {
			subtaskCh <- struct{}{}
		}
		task2Base := testutil.WaitTaskDone(c.Ctx, t, theTask.Key)
		require.Equal(t, proto.TaskStateSucceed, task2Base.State)
		require.EqualValues(t, []subtaskRuntimeInfo{
			{Step: proto.StepOne, Concurrency: 7},
			{Step: proto.StepOne, Concurrency: 7},
			{Step: proto.StepTwo, Concurrency: 7},
			{Step: proto.StepTwo, Concurrency: 7},
			{Step: proto.StepTwo, Concurrency: 7},
		}, runtimeInfo.subtaskInfos)
	})

	t.Run("modify pending task meta, only check the scheduler part", func(t *testing.T) {
		defer resetRuntimeInfoFn()
		var once sync.Once
		modifySyncCh := make(chan struct{})
		var theTask *proto.Task
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/disttask/framework/scheduler/beforeGetSchedulableTasks", func() {
			once.Do(func() {
				task, err := handle.SubmitTask(c.Ctx, "k5", proto.TaskTypeExample, 3, "", 0, []byte("init"))
				require.NoError(t, err)
				require.Equal(t, 3, task.Concurrency)
				require.EqualValues(t, []byte("init"), task.Meta)
				require.NoError(t, c.TaskMgr.ModifyTaskByID(c.Ctx, task.ID, &proto.ModifyParam{
					PrevState: proto.TaskStatePending,
					Modifications: []proto.Modification{
						{Type: proto.ModifyMaxWriteSpeed, To: 123},
					},
				}))
				theTask = task
				gotTask, err := c.TaskMgr.GetTaskBaseByID(c.Ctx, theTask.ID)
				require.NoError(t, err)
				require.Equal(t, proto.TaskStateModifying, gotTask.State)
				require.Equal(t, 3, gotTask.Concurrency)
				<-modifySyncCh
			})
		})
		modifySyncCh <- struct{}{}
		// finish subtasks
		for range 5 {
			subtaskCh <- struct{}{}
		}
		task2Base := testutil.WaitTaskDone(c.Ctx, t, theTask.Key)
		require.Equal(t, proto.TaskStateSucceed, task2Base.State)
		require.EqualValues(t, []byte("modify_max_write_speed=123"), *runtimeInfo.currTaskMeta.Load())
	})

	t.Run("modify meta and increase concurrency when subtask is running, and apply success", func(t *testing.T) {
		defer resetRuntimeInfoFn()
		defer testModifyWhenSubtaskRun.Store(false)
		testModifyWhenSubtaskRun.Store(true)
		task, err := handle.SubmitTask(c.Ctx, "k6", proto.TaskTypeExample, 3, "", 0, []byte("init"))
		require.NoError(t, err)
		require.Equal(t, 3, task.Concurrency)
		require.EqualValues(t, []byte("init"), task.Meta)
		modifyWaitCh <- struct{}{}
		require.NoError(t, c.TaskMgr.ModifyTaskByID(c.Ctx, task.ID, &proto.ModifyParam{
			PrevState: proto.TaskStateRunning,
			Modifications: []proto.Modification{
				{Type: proto.ModifyConcurrency, To: 7},
				{Type: proto.ModifyMaxWriteSpeed, To: 123},
			},
		}))
		require.Eventually(t, func() bool {
			return "modify_max_write_speed=123" == string(*runtimeInfo.currTaskMeta.Load()) &&
				runtimeInfo.currTaskConcurrency.Load() == 7
		}, 10*time.Second, 100*time.Millisecond)
		testModifyWhenSubtaskRun.Store(false)
		modifyWaitCh <- struct{}{}
		// finish subtasks
		for range 5 {
			subtaskCh <- struct{}{}
		}
		task2Base := testutil.WaitTaskDone(c.Ctx, t, task.Key)
		require.Equal(t, proto.TaskStateSucceed, task2Base.State)
		require.Equal(t, "modify_max_write_speed=123", string(*runtimeInfo.currTaskMeta.Load()))
		require.Equal(t, int64(7), runtimeInfo.currTaskConcurrency.Load())
	})

	t.Run("modify meta and decrease concurrency when subtask is running, and apply success", func(t *testing.T) {
		defer resetRuntimeInfoFn()
		defer testModifyWhenSubtaskRun.Store(false)
		testModifyWhenSubtaskRun.Store(true)
		task, err := handle.SubmitTask(c.Ctx, "k7", proto.TaskTypeExample, 9, "", 0, []byte("init"))
		require.NoError(t, err)
		require.Equal(t, 9, task.Concurrency)
		require.EqualValues(t, []byte("init"), task.Meta)
		modifyWaitCh <- struct{}{}
		require.NoError(t, c.TaskMgr.ModifyTaskByID(c.Ctx, task.ID, &proto.ModifyParam{
			PrevState: proto.TaskStateRunning,
			Modifications: []proto.Modification{
				{Type: proto.ModifyConcurrency, To: 5},
				{Type: proto.ModifyMaxWriteSpeed, To: 456},
			},
		}))
		require.Eventually(t, func() bool {
			return "modify_max_write_speed=456" == string(*runtimeInfo.currTaskMeta.Load()) &&
				runtimeInfo.currTaskConcurrency.Load() == 5
		}, 10*time.Second, 100*time.Millisecond)
		testModifyWhenSubtaskRun.Store(false)
		modifyWaitCh <- struct{}{}
		// finish subtasks
		for range 5 {
			subtaskCh <- struct{}{}
		}
		task2Base := testutil.WaitTaskDone(c.Ctx, t, task.Key)
		require.Equal(t, proto.TaskStateSucceed, task2Base.State)
		require.Equal(t, "modify_max_write_speed=456", string(*runtimeInfo.currTaskMeta.Load()))
		require.Equal(t, int64(5), runtimeInfo.currTaskConcurrency.Load())
	})
}
