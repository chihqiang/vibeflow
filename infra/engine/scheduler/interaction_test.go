package scheduler

import (
	"os"
	"testing"

	"chihqiang/vibeflow/domain/model"
	"chihqiang/vibeflow/infra/config"
	"chihqiang/vibeflow/infra/logger"
)

// ============================================================================
// 事件触发相关测试
// ============================================================================

func TestEventTriggerType_Constants(t *testing.T) {
	if model.EventTriggerWebhook != "webhook" {
		t.Errorf("EventTriggerWebhook = %q, want %q", model.EventTriggerWebhook, "webhook")
	}
	if model.EventTriggerWorkflowCompleted != "workflow_completed" {
		t.Errorf("EventTriggerWorkflowCompleted = %q, want %q", model.EventTriggerWorkflowCompleted, "workflow_completed")
	}
	if model.EventTriggerTaskFailed != "task_failed" {
		t.Errorf("EventTriggerTaskFailed = %q, want %q", model.EventTriggerTaskFailed, "task_failed")
	}
}

func TestTriggerEvent_Constant(t *testing.T) {
	if model.TriggerEvent != "event" {
		t.Errorf("TriggerEvent = %q, want %q", model.TriggerEvent, "event")
	}
}

func TestValidateWorkflowTasks_EventTrigger_Webhook(t *testing.T) {
	wf := &model.Workflow{
		UUID: "test-wf",
		Name: "test-wf",
		Trigger: model.TriggerEvent,
		EventTrigger: &model.EventTrigger{
			EventType: model.EventTriggerWebhook,
		},
		TaskGroups: [][]model.TaskNode{
			{{Name: "task1"}},
		},
	}
	if err := validateWorkflowTasks(wf); err != nil {
		t.Errorf("valid webhook trigger should pass: %v", err)
	}
}

func TestValidateWorkflowTasks_EventTrigger_WorkflowCompleted(t *testing.T) {
	wf := &model.Workflow{
		UUID: "test-wf",
		Name: "test-wf",
		Trigger: model.TriggerEvent,
		EventTrigger: &model.EventTrigger{
			EventType: model.EventTriggerWorkflowCompleted,
			Filter:    "source-workflow-uuid",
		},
		TaskGroups: [][]model.TaskNode{
			{{Name: "task1"}},
		},
	}
	if err := validateWorkflowTasks(wf); err != nil {
		t.Errorf("valid workflow_completed trigger should pass: %v", err)
	}
}

func TestValidateWorkflowTasks_EventTrigger_TaskFailed(t *testing.T) {
	wf := &model.Workflow{
		UUID: "test-wf",
		Name: "test-wf",
		Trigger: model.TriggerEvent,
		EventTrigger: &model.EventTrigger{
			EventType: model.EventTriggerTaskFailed,
			Filter:    "wf-uuid:task-name",
		},
		TaskGroups: [][]model.TaskNode{
			{{Name: "task1"}},
		},
	}
	if err := validateWorkflowTasks(wf); err != nil {
		t.Errorf("valid task_failed trigger should pass: %v", err)
	}
}

func TestValidateWorkflowTasks_EventTrigger_MissingDefinition(t *testing.T) {
	wf := &model.Workflow{
		UUID:     "test-wf",
		Name:     "test-wf",
		Trigger:  model.TriggerEvent,
		TaskGroups: [][]model.TaskNode{
			{{Name: "task1"}},
		},
	}
	if err := validateWorkflowTasks(wf); err == nil {
		t.Error("event trigger without definition should fail")
	}
}

func TestValidateWorkflowTasks_EventTrigger_WorkflowCompleted_MissingFilter(t *testing.T) {
	wf := &model.Workflow{
		UUID: "test-wf",
		Name: "test-wf",
		Trigger: model.TriggerEvent,
		EventTrigger: &model.EventTrigger{
			EventType: model.EventTriggerWorkflowCompleted,
		},
		TaskGroups: [][]model.TaskNode{
			{{Name: "task1"}},
		},
	}
	if err := validateWorkflowTasks(wf); err == nil {
		t.Error("workflow_completed trigger without filter should fail")
	}
}

func TestValidateWorkflowTasks_EventTrigger_TaskFailed_MissingFilter(t *testing.T) {
	wf := &model.Workflow{
		UUID: "test-wf",
		Name: "test-wf",
		Trigger: model.TriggerEvent,
		EventTrigger: &model.EventTrigger{
			EventType: model.EventTriggerTaskFailed,
		},
		TaskGroups: [][]model.TaskNode{
			{{Name: "task1"}},
		},
	}
	if err := validateWorkflowTasks(wf); err == nil {
		t.Error("task_failed trigger without filter should fail")
	}
}

func TestValidateWorkflowTasks_EventTrigger_UnknownType(t *testing.T) {
	wf := &model.Workflow{
		UUID: "test-wf",
		Name: "test-wf",
		Trigger: model.TriggerEvent,
		EventTrigger: &model.EventTrigger{
			EventType: "unknown_type",
		},
		TaskGroups: [][]model.TaskNode{
			{{Name: "task1"}},
		},
	}
	if err := validateWorkflowTasks(wf); err == nil {
		t.Error("unknown event trigger type should fail")
	}
}

// ============================================================================
// 输入映射相关测试
// ============================================================================

func TestApplyInputMapping_Nil(t *testing.T) {
	node := model.TaskNode{Name: "task1"}
	upstream := []map[string]any{{"key1": "val1"}}
	result := model.ApplyInputMapping(node, upstream)
	if result != nil {
		t.Error("nil InputMapping should return nil")
	}
}

func TestApplyInputMapping_Empty(t *testing.T) {
	node := model.TaskNode{Name: "task1", InputMapping: map[string]string{}}
	upstream := []map[string]any{{"key1": "val1"}}
	result := model.ApplyInputMapping(node, upstream)
	if result != nil {
		t.Error("empty InputMapping should return nil")
	}
}

func TestApplyInputMapping_SingleMapping(t *testing.T) {
	node := model.TaskNode{
		Name: "task1",
		InputMapping: map[string]string{
			"repo_url": "git_url",
		},
	}
	upstream := []map[string]any{
		{"git_url": "https://github.com/test/repo"},
	}
	result := model.ApplyInputMapping(node, upstream)
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result["repo_url"] != "https://github.com/test/repo" {
		t.Errorf("expected repo_url to be mapped, got %v", result["repo_url"])
	}
	if _, exists := result["git_url"]; exists {
		t.Error("source key should not be in result")
	}
}

func TestApplyInputMapping_MultipleMappings(t *testing.T) {
	node := model.TaskNode{
		Name: "task1",
		InputMapping: map[string]string{
			"repo": "git_url",
			"env":  "deploy_env",
		},
	}
	upstream := []map[string]any{
		{"git_url": "https://github.com/test/repo"},
		{"deploy_env": "production"},
	}
	result := model.ApplyInputMapping(node, upstream)
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result["repo"] != "https://github.com/test/repo" {
		t.Errorf("expected repo to be mapped, got %v", result["repo"])
	}
	if result["env"] != "production" {
		t.Errorf("expected env to be mapped, got %v", result["env"])
	}
}

func TestApplyInputMapping_SourceNotFound(t *testing.T) {
	node := model.TaskNode{
		Name: "task1",
		InputMapping: map[string]string{
			"repo": "nonexistent_key",
		},
	}
	upstream := []map[string]any{
		{"git_url": "https://github.com/test/repo"},
	}
	result := model.ApplyInputMapping(node, upstream)
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if len(result) != 0 {
		t.Errorf("expected empty result for missing source, got %v", result)
	}
}

func TestApplyInputMapping_LatestUpstreamWins(t *testing.T) {
	node := model.TaskNode{
		Name: "task1",
		InputMapping: map[string]string{
			"result": "value",
		},
	}
	upstream := []map[string]any{
		{"value": "first"},
		{"value": "second"},
		{"value": "third"},
	}
	result := model.ApplyInputMapping(node, upstream)
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	// Latest write wins
	if result["result"] != "third" {
		t.Errorf("expected result to be 'third' (latest), got %v", result["result"])
	}
}

func TestValidateWorkflowTasks_InputMapping_Valid(t *testing.T) {
	wf := &model.Workflow{
		UUID: "test-wf",
		Name: "test-wf",
		TaskGroups: [][]model.TaskNode{
			{
				{
					Name: "task1",
					InputMapping: map[string]string{
						"repo": "git_url",
					},
				},
			},
		},
	}
	if err := validateWorkflowTasks(wf); err != nil {
		t.Errorf("valid input mapping should pass: %v", err)
	}
}

func TestValidateWorkflowTasks_InputMapping_EmptyKey(t *testing.T) {
	wf := &model.Workflow{
		UUID: "test-wf",
		Name: "test-wf",
		TaskGroups: [][]model.TaskNode{
			{
				{
					Name: "task1",
					InputMapping: map[string]string{
						"": "git_url",
					},
				},
			},
		},
	}
	if err := validateWorkflowTasks(wf); err == nil {
		t.Error("input mapping with empty local key should fail")
	}
}

func TestValidateWorkflowTasks_InputMapping_EmptySource(t *testing.T) {
	wf := &model.Workflow{
		UUID: "test-wf",
		Name: "test-wf",
		TaskGroups: [][]model.TaskNode{
			{
				{
					Name: "task1",
					InputMapping: map[string]string{
						"repo": "",
					},
				},
			},
		},
	}
	if err := validateWorkflowTasks(wf); err == nil {
		t.Error("input mapping with empty source key should fail")
	}
}

// ============================================================================
// 增强条件分支相关测试
// ============================================================================

func TestValidateWorkflowTasks_DefaultBranch_Valid(t *testing.T) {
	wf := &model.Workflow{
		UUID: "test-wf",
		Name: "test-wf",
		TaskGroups: [][]model.TaskNode{
			{
				{
					Name:          "check",
					Branches:      model.BranchDef{"approved": {{{Name: "deploy"}}}, "rejected": {{{Name: "notify"}}}},
					DefaultBranch: "rejected",
				},
			},
		},
	}
	if err := validateWorkflowTasks(wf); err != nil {
		t.Errorf("valid default branch should pass: %v", err)
	}
}

func TestValidateWorkflowTasks_DefaultBranch_NotExists(t *testing.T) {
	wf := &model.Workflow{
		UUID: "test-wf",
		Name: "test-wf",
		TaskGroups: [][]model.TaskNode{
			{
				{
					Name:          "check",
					Branches:      model.BranchDef{"approved": {{{Name: "deploy"}}}},
					DefaultBranch: "nonexistent",
				},
			},
		},
	}
	if err := validateWorkflowTasks(wf); err == nil {
		t.Error("default branch that doesn't exist in BranchDef should fail")
	}
}

func TestValidateWorkflowTasks_ParallelBranch(t *testing.T) {
	wf := &model.Workflow{
		UUID: "test-wf",
		Name: "test-wf",
		TaskGroups: [][]model.TaskNode{
			{
				{
					Name:           "check",
					Branches:       model.BranchDef{"branch_a": {{{Name: "task_a"}}}, "branch_b": {{{Name: "task_b"}}}},
					ParallelBranch: true,
				},
			},
		},
	}
	if err := validateWorkflowTasks(wf); err != nil {
		t.Errorf("valid parallel branch should pass: %v", err)
	}
}

// ============================================================================
// Workflow 便捷方法测试
// ============================================================================

func TestWorkflow_SetEventTrigger(t *testing.T) {
	wf := model.NewWorkflow("test-wf").
		AddTaskGroup("task1").
		SetEventTrigger(model.EventTriggerWebhook, "")

	if wf.Trigger != model.TriggerEvent {
		t.Errorf("expected trigger=event, got %s", wf.Trigger)
	}
	if wf.EventTrigger == nil {
		t.Fatal("expected EventTrigger to be set")
	}
	if wf.EventTrigger.EventType != model.EventTriggerWebhook {
		t.Errorf("expected webhook, got %s", wf.EventTrigger.EventType)
	}
}

func TestWorkflow_SetEventTrigger_WithFilter(t *testing.T) {
	wf := model.NewWorkflow("compensation").
		AddTaskGroup("cleanup").
		SetEventTrigger(model.EventTriggerTaskFailed, "deploy-wf:deploy_task")

	if wf.EventTrigger.Filter != "deploy-wf:deploy_task" {
		t.Errorf("expected filter to be set, got %s", wf.EventTrigger.Filter)
	}
}

func TestWorkflow_AddTaskGroupWithInputMapping(t *testing.T) {
	wf := model.NewWorkflow("test").
		AddTaskGroupWithInputMapping(
			[]string{"task1", "task2"},
			[]map[string]string{
				{"repo": "git_url"},
				{"env": "deploy_env"},
			},
		)

	if len(wf.TaskGroups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(wf.TaskGroups))
	}
	if wf.TaskGroups[0][0].InputMapping["repo"] != "git_url" {
		t.Errorf("expected input mapping for task1, got %v", wf.TaskGroups[0][0].InputMapping)
	}
	if wf.TaskGroups[0][1].InputMapping["env"] != "deploy_env" {
		t.Errorf("expected input mapping for task2, got %v", wf.TaskGroups[0][1].InputMapping)
	}
}

func TestWorkflow_AddBranchNodeGroup(t *testing.T) {
	wf := model.NewWorkflow("test").
		AddTaskGroup("check").
		AddBranchNodeGroup("check2",
			model.BranchDef{
				"approved": {{{Name: "deploy"}}},
				"rejected": {{{Name: "notify"}}},
			},
			"rejected",
			false,
		)

	if len(wf.TaskGroups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(wf.TaskGroups))
	}
	node := wf.TaskGroups[1][0]
	if node.DefaultBranch != "rejected" {
		t.Errorf("expected default_branch=rejected, got %s", node.DefaultBranch)
	}
	if node.ParallelBranch {
		t.Error("expected parallel_branch=false")
	}
}

func TestWorkflow_DeepCopy_WithNewFields(t *testing.T) {
	original := model.NewWorkflow("test").
		AddTaskGroup("task1").
		AddTaskGroupWithInputMapping(
			[]string{"task2"},
			[]map[string]string{{"repo": "git_url"}},
		).
		SetEventTrigger(model.EventTriggerWebhook, "")

	copy, err := original.DeepCopy()
	if err != nil {
		t.Fatalf("DeepCopy failed: %v", err)
	}

	// Verify event trigger preserved
	if copy.Trigger != model.TriggerEvent {
		t.Error("trigger type not preserved in deep copy")
	}
	if copy.EventTrigger == nil || copy.EventTrigger.EventType != model.EventTriggerWebhook {
		t.Error("event trigger not preserved in deep copy")
	}

	// Verify input mapping preserved
	if copy.TaskGroups[1][0].InputMapping == nil {
		t.Error("input mapping not preserved in deep copy")
	}
	if copy.TaskGroups[1][0].InputMapping["repo"] != "git_url" {
		t.Error("input mapping value not preserved in deep copy")
	}

	// Verify independence
	copy.TaskGroups[1][0].InputMapping["repo"] = "modified"
	if original.TaskGroups[1][0].InputMapping["repo"] != "git_url" {
		t.Error("deep copy should be independent from original")
	}
}

// ============================================================================
// EventTriggerManager 测试
// ============================================================================

// registerTestListener 直接写入 eventTriggerManager（避免依赖 logger）
func registerTestListener(mgr *eventTriggerManager, wf *model.Workflow) {
	et := wf.EventTrigger
	listener := EventListener{
		WorkflowUUID:  wf.UUID,
		WorkflowName:  wf.Name,
		EventType:     et.EventType,
		Filter:        et.Filter,
		WebhookSecret: et.WebhookSecret,
	}

	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	eventKey := string(et.EventType)
	mgr.listeners[eventKey] = append(mgr.listeners[eventKey], listener)

	if et.EventType == model.EventTriggerWebhook {
		mgr.webhookWfs[wf.UUID] = listener
	}
}

func TestEventTriggerManager_RegisterAndFireWebhook(t *testing.T) {
	mgr := newEventTriggerManager()

	wf := &model.Workflow{
		UUID: "webhook-wf",
		Name: "webhook-workflow",
		Trigger: model.TriggerEvent,
		EventTrigger: &model.EventTrigger{
			EventType: model.EventTriggerWebhook,
		},
		TaskGroups: [][]model.TaskNode{{{Name: "task1"}}},
	}
	// 直接写入 map 而非调用 RegisterEventTrigger（避免依赖 logger）
	mgr.mu.Lock()
	mgr.webhookWfs[wf.UUID] = EventListener{
		WorkflowUUID: wf.UUID,
		WorkflowName: wf.Name,
		EventType:    model.EventTriggerWebhook,
	}
	mgr.mu.Unlock()

	listener, err := mgr.FireWebhook("webhook-wf")
	if err != nil {
		t.Fatalf("FireWebhook failed: %v", err)
	}
	if listener.WorkflowName != "webhook-workflow" {
		t.Errorf("expected workflow name 'webhook-workflow', got %s", listener.WorkflowName)
	}
}

func TestEventTriggerManager_FireWebhook_NotFound(t *testing.T) {
	mgr := newEventTriggerManager()
	_, err := mgr.FireWebhook("nonexistent")
	if err == nil {
		t.Error("FireWebhook should fail for nonexistent workflow")
	}
}

func TestEventTriggerManager_RegisterAndFireWorkflowCompleted(t *testing.T) {
	mgr := newEventTriggerManager()

	targetWf := &model.Workflow{
		UUID: "target-wf",
		Name: "target-workflow",
		Trigger: model.TriggerEvent,
		EventTrigger: &model.EventTrigger{
			EventType: model.EventTriggerWorkflowCompleted,
			Filter:    "source-wf",
		},
		TaskGroups: [][]model.TaskNode{{{Name: "task1"}}},
	}
	registerTestListener(mgr, targetWf)

	var triggeredUUID string
	mgr.FireWorkflowCompleted("source-wf", func(targetUUID string) {
		triggeredUUID = targetUUID
	})

	if triggeredUUID != "target-wf" {
		t.Errorf("expected target-wf to be triggered, got %s", triggeredUUID)
	}
}

func TestEventTriggerManager_WorkflowCompleted_NoMatch(t *testing.T) {
	mgr := newEventTriggerManager()

	targetWf := &model.Workflow{
		UUID: "target-wf",
		Name: "target-workflow",
		Trigger: model.TriggerEvent,
		EventTrigger: &model.EventTrigger{
			EventType: model.EventTriggerWorkflowCompleted,
			Filter:    "source-wf",
		},
		TaskGroups: [][]model.TaskNode{{{Name: "task1"}}},
	}
	registerTestListener(mgr, targetWf)

	triggered := false
	mgr.FireWorkflowCompleted("other-source-wf", func(targetUUID string) {
		triggered = true
	})

	if triggered {
		t.Error("should not trigger for non-matching source")
	}
}

func TestEventTriggerManager_RegisterAndFireTaskFailed(t *testing.T) {
	mgr := newEventTriggerManager()

	targetWf := &model.Workflow{
		UUID: "target-wf",
		Name: "target-workflow",
		Trigger: model.TriggerEvent,
		EventTrigger: &model.EventTrigger{
			EventType: model.EventTriggerTaskFailed,
			Filter:    "deploy-wf:deploy_task",
		},
		TaskGroups: [][]model.TaskNode{{{Name: "task1"}}},
	}
	registerTestListener(mgr, targetWf)

	var triggeredUUID string
	mgr.FireTaskFailed("deploy-wf", "deploy_task", func(targetUUID string) {
		triggeredUUID = targetUUID
	})

	if triggeredUUID != "target-wf" {
		t.Errorf("expected target-wf to be triggered, got %s", triggeredUUID)
	}
}

func TestEventTriggerManager_TaskFailed_WorkflowLevelFilter(t *testing.T) {
	mgr := newEventTriggerManager()

	targetWf := &model.Workflow{
		UUID: "target-wf",
		Name: "target-workflow",
		Trigger: model.TriggerEvent,
		EventTrigger: &model.EventTrigger{
			EventType: model.EventTriggerTaskFailed,
			Filter:    "deploy-wf",
		},
		TaskGroups: [][]model.TaskNode{{{Name: "task1"}}},
	}
	registerTestListener(mgr, targetWf)

	var triggeredUUID string
	mgr.FireTaskFailed("deploy-wf", "any_task", func(targetUUID string) {
		triggeredUUID = targetUUID
	})

	if triggeredUUID != "target-wf" {
		t.Errorf("expected workflow-level filter to match, got %s", triggeredUUID)
	}
}

func TestEventTriggerManager_Unregister(t *testing.T) {
	mgr := newEventTriggerManager()

	wf := &model.Workflow{
		UUID: "test-wf",
		Name: "test-workflow",
		Trigger: model.TriggerEvent,
		EventTrigger: &model.EventTrigger{
			EventType: model.EventTriggerWebhook,
		},
		TaskGroups: [][]model.TaskNode{{{Name: "task1"}}},
	}
	registerTestListener(mgr, wf)
	mgr.UnregisterEventTrigger("test-wf")

	_, err := mgr.FireWebhook("test-wf")
	if err == nil {
		t.Error("FireWebhook should fail after unregister")
	}
}

func TestEventTriggerManager_ListWebhookWorkflows(t *testing.T) {
	mgr := newEventTriggerManager()

	wf1 := &model.Workflow{
		UUID: "webhook-1",
		Name: "webhook-1",
		Trigger: model.TriggerEvent,
		EventTrigger: &model.EventTrigger{
			EventType: model.EventTriggerWebhook,
		},
		TaskGroups: [][]model.TaskNode{{{Name: "task1"}}},
	}
	wf2 := &model.Workflow{
		UUID: "webhook-2",
		Name: "webhook-2",
		Trigger: model.TriggerEvent,
		EventTrigger: &model.EventTrigger{
			EventType: model.EventTriggerWebhook,
		},
		TaskGroups: [][]model.TaskNode{{{Name: "task1"}}},
	}
	registerTestListener(mgr, wf1)
	registerTestListener(mgr, wf2)

	list := mgr.ListWebhookWorkflows()
	if len(list) != 2 {
		t.Errorf("expected 2 webhook workflows, got %d", len(list))
	}
}

func TestEventTriggerManager_MultipleTargets(t *testing.T) {
	mgr := newEventTriggerManager()

	target1 := &model.Workflow{
		UUID: "target-1",
		Name: "target-1",
		Trigger: model.TriggerEvent,
		EventTrigger: &model.EventTrigger{
			EventType: model.EventTriggerWorkflowCompleted,
			Filter:    "source-wf",
		},
		TaskGroups: [][]model.TaskNode{{{Name: "task1"}}},
	}
	target2 := &model.Workflow{
		UUID: "target-2",
		Name: "target-2",
		Trigger: model.TriggerEvent,
		EventTrigger: &model.EventTrigger{
			EventType: model.EventTriggerWorkflowCompleted,
			Filter:    "source-wf",
		},
		TaskGroups: [][]model.TaskNode{{{Name: "task1"}}},
	}
	registerTestListener(mgr, target1)
	registerTestListener(mgr, target2)

	var triggered []string
	mgr.FireWorkflowCompleted("source-wf", func(targetUUID string) {
		triggered = append(triggered, targetUUID)
	})

	if len(triggered) != 2 {
		t.Errorf("expected 2 targets to be triggered, got %d", len(triggered))
	}
}

func TestMain(m *testing.M) {
	// 初始化 logger（测试中依赖 logger.Info 的代码路径）
	logger.Init(config.LoggerConfig{Level: "warn"})
	os.Exit(m.Run())
}
