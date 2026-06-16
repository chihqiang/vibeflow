package scheduler

import (
	"testing"

	"chihqiang/vibeflow/domain/model"
)

// ============================================================================
// 验证测试 — 确保高级编排类型的模型定义和验证逻辑正确
// ============================================================================

func TestGetTaskNodeType_Default(t *testing.T) {
	node := model.TaskNode{Name: "task1"}
	if got := model.GetTaskNodeType(node); got != model.TaskNodeTypeTask {
		t.Errorf("expected task, got %s", got)
	}
}

func TestGetTaskNodeType_SubWorkflow(t *testing.T) {
	node := model.TaskNode{Type: model.TaskNodeTypeSubWorkflow, SubWorkflow: "sub-uuid"}
	if got := model.GetTaskNodeType(node); got != model.TaskNodeTypeSubWorkflow {
		t.Errorf("expected sub_workflow, got %s", got)
	}
}

func TestGetTaskNodeType_FanOut(t *testing.T) {
	node := model.TaskNode{Type: model.TaskNodeTypeFanOut, FanOut: &model.FanOutDef{IteratorKey: "items"}}
	if got := model.GetTaskNodeType(node); got != model.TaskNodeTypeFanOut {
		t.Errorf("expected fan_out, got %s", got)
	}
}

// ============================================================================
// validateWorkflowTasks 测试
// ============================================================================

func TestValidateWorkflowTasks_SubWorkflow(t *testing.T) {
	wf := &model.Workflow{
		UUID: "test-wf",
		Name: "test-wf",
		TaskGroups: [][]model.TaskNode{
			{
				{Name: "task1"},
			},
			{
				{
					Type:        model.TaskNodeTypeSubWorkflow,
					Name:        "sub_task",
					SubWorkflow: "sub-uuid",
				},
			},
		},
	}
	if err := validateWorkflowTasks(wf); err != nil {
		t.Errorf("valid sub-workflow should pass: %v", err)
	}
}

func TestValidateWorkflowTasks_SubWorkflow_MissingUUID(t *testing.T) {
	wf := &model.Workflow{
		UUID: "test-wf",
		Name: "test-wf",
		TaskGroups: [][]model.TaskNode{
			{
				{
					Type: model.TaskNodeTypeSubWorkflow,
					Name: "sub_task",
				},
			},
		},
	}
	if err := validateWorkflowTasks(wf); err == nil {
		t.Error("sub-workflow without UUID should fail")
	}
}

func TestValidateWorkflowTasks_FanOut(t *testing.T) {
	wf := &model.Workflow{
		UUID: "test-wf",
		Name: "test-wf",
		TaskGroups: [][]model.TaskNode{
			{
				{Name: "task1"},
			},
			{
				{
					Type: model.TaskNodeTypeFanOut,
					Name: "fan_out_task",
					FanOut: &model.FanOutDef{
						IteratorKey: "items",
						Task:        model.TaskNode{Name: "process_item"},
					},
				},
			},
		},
	}
	if err := validateWorkflowTasks(wf); err != nil {
		t.Errorf("valid fan-out should pass: %v", err)
	}
}

func TestValidateWorkflowTasks_FanOut_MissingIteratorKey(t *testing.T) {
	wf := &model.Workflow{
		UUID: "test-wf",
		Name: "test-wf",
		TaskGroups: [][]model.TaskNode{
			{
				{
					Type: model.TaskNodeTypeFanOut,
					Name: "fan_out_task",
					FanOut: &model.FanOutDef{
						Task: model.TaskNode{Name: "process_item"},
					},
				},
			},
		},
	}
	if err := validateWorkflowTasks(wf); err == nil {
		t.Error("fan-out without iterator_key should fail")
	}
}

func TestValidateWorkflowTasks_Loop(t *testing.T) {
	wf := &model.Workflow{
		UUID: "test-wf",
		Name: "test-wf",
		TaskGroups: [][]model.TaskNode{
			{
				{
					Name: "poll_task",
					Loop: &model.LoopDef{
						MaxIterations: 5,
						ConditionType: model.LoopConditionAlways,
					},
				},
			},
		},
	}
	if err := validateWorkflowTasks(wf); err != nil {
		t.Errorf("valid loop should pass: %v", err)
	}
}

func TestValidateWorkflowTasks_Loop_ZeroMaxIterations(t *testing.T) {
	wf := &model.Workflow{
		UUID: "test-wf",
		Name: "test-wf",
		TaskGroups: [][]model.TaskNode{
			{
				{
					Name: "poll_task",
					Loop: &model.LoopDef{
						MaxIterations: 0,
						ConditionType: model.LoopConditionAlways,
					},
				},
			},
		},
	}
	if err := validateWorkflowTasks(wf); err == nil {
		t.Error("loop with max_iterations=0 should fail")
	}
}

func TestValidateWorkflowTasks_Loop_KeyCondition_MissingKey(t *testing.T) {
	wf := &model.Workflow{
		UUID: "test-wf",
		Name: "test-wf",
		TaskGroups: [][]model.TaskNode{
			{
				{
					Name: "poll_task",
					Loop: &model.LoopDef{
						MaxIterations: 10,
						ConditionType: model.LoopConditionKey,
					},
				},
			},
		},
	}
	if err := validateWorkflowTasks(wf); err == nil {
		t.Error("loop with condition_type=key but no condition_key should fail")
	}
}

func TestValidateWorkflowTasks_Loop_KeyCondition_Valid(t *testing.T) {
	wf := &model.Workflow{
		UUID: "test-wf",
		Name: "test-wf",
		TaskGroups: [][]model.TaskNode{
			{
				{
					Name: "poll_task",
					Loop: &model.LoopDef{
						MaxIterations: 10,
						ConditionType: model.LoopConditionKey,
						ConditionKey:  "should_continue",
					},
				},
			},
		},
	}
	if err := validateWorkflowTasks(wf); err != nil {
		t.Errorf("valid loop with key condition should pass: %v", err)
	}
}

func TestValidateWorkflowTasks_MixedNodes(t *testing.T) {
	wf := &model.Workflow{
		UUID: "test-wf",
		Name: "test-wf",
		TaskGroups: [][]model.TaskNode{
			{
				{Name: "task1"},
				{Name: "task2"},
			},
			{
				{
					Type: model.TaskNodeTypeSubWorkflow,
					Name: "sub_task",
					SubWorkflow: "sub-uuid",
				},
				{Name: "task3"},
			},
			{
				{
					Type: model.TaskNodeTypeFanOut,
					Name: "fan_out_task",
					FanOut: &model.FanOutDef{
						IteratorKey: "items",
						Task:        model.TaskNode{Name: "process_item"},
					},
				},
			},
		},
	}
	if err := validateWorkflowTasks(wf); err != nil {
		t.Errorf("valid mixed nodes should pass: %v", err)
	}
}

func TestValidateWorkflowTasks_SubWorkflowWithLoop(t *testing.T) {
	wf := &model.Workflow{
		UUID: "test-wf",
		Name: "test-wf",
		TaskGroups: [][]model.TaskNode{
			{
				{
					Type:        model.TaskNodeTypeSubWorkflow,
					Name:        "retry_sub",
					SubWorkflow: "sub-uuid",
					Loop: &model.LoopDef{
						MaxIterations: 3,
						ConditionType: model.LoopConditionAlways,
					},
				},
			},
		},
	}
	if err := validateWorkflowTasks(wf); err != nil {
		t.Errorf("valid sub-workflow with loop should pass: %v", err)
	}
}

// ============================================================================
// detectAdvancedNodes 测试
// ============================================================================

func TestDetectAdvancedNodes_Loop(t *testing.T) {
	group := []model.TaskNode{
		{Name: "poll_task", Loop: &model.LoopDef{MaxIterations: 5, ConditionType: model.LoopConditionAlways}},
	}
	s := &SerialExecutor{}
	has, typ := s.detectAdvancedNodes(group)
	if !has || typ != "loop" {
		t.Errorf("expected loop, got has=%v typ=%s", has, typ)
	}
}

func TestDetectAdvancedNodes_FanOut(t *testing.T) {
	group := []model.TaskNode{
		{Type: model.TaskNodeTypeFanOut, Name: "fan", FanOut: &model.FanOutDef{IteratorKey: "items", Task: model.TaskNode{Name: "proc"}}},
	}
	s := &SerialExecutor{}
	has, typ := s.detectAdvancedNodes(group)
	if !has || typ != "fan_out" {
		t.Errorf("expected fan_out, got has=%v typ=%s", has, typ)
	}
}

func TestDetectAdvancedNodes_SubWorkflow(t *testing.T) {
	group := []model.TaskNode{
		{Type: model.TaskNodeTypeSubWorkflow, Name: "sub", SubWorkflow: "sub-uuid"},
	}
	s := &SerialExecutor{}
	has, typ := s.detectAdvancedNodes(group)
	if !has || typ != "sub_workflow" {
		t.Errorf("expected sub_workflow, got has=%v typ=%s", has, typ)
	}
}

func TestDetectAdvancedNodes_Normal(t *testing.T) {
	group := []model.TaskNode{
		{Name: "task1"},
		{Name: "task2"},
	}
	s := &SerialExecutor{}
	has, _ := s.detectAdvancedNodes(group)
	if has {
		t.Error("normal group should not have advanced nodes")
	}
}

// ============================================================================
// shouldContinueLoop 测试
// ============================================================================

func TestShouldContinueLoop_Always(t *testing.T) {
	loop := &model.LoopDef{MaxIterations: 5, ConditionType: model.LoopConditionAlways}
	if !shouldContinueLoop(nil, loop, 3) {
		t.Error("should continue at iteration 3")
	}
	if shouldContinueLoop(nil, loop, 5) {
		t.Error("should stop at max iterations")
	}
}

func TestShouldContinueLoop_Key_True(t *testing.T) {
	loop := &model.LoopDef{MaxIterations: 10, ConditionType: model.LoopConditionKey, ConditionKey: "continue"}
	output := map[string]any{"continue": true}
	if !shouldContinueLoop(output, loop, 1) {
		t.Error("should continue when condition key is true")
	}
}

func TestShouldContinueLoop_Key_False(t *testing.T) {
	loop := &model.LoopDef{MaxIterations: 10, ConditionType: model.LoopConditionKey, ConditionKey: "continue"}
	output := map[string]any{"continue": false}
	if shouldContinueLoop(output, loop, 1) {
		t.Error("should stop when condition key is false")
	}
}

func TestShouldContinueLoop_LoopContinueKey(t *testing.T) {
	loop := &model.LoopDef{MaxIterations: 10, ConditionType: model.LoopConditionKey, ConditionKey: "continue"}
	output := map[string]any{model.LoopContinueKey: false}
	if shouldContinueLoop(output, loop, 1) {
		t.Error("should stop when LoopContinueKey is false")
	}
}

// ============================================================================
// extractIteratorList 测试
// ============================================================================

func TestExtractIteratorList_FromUpstream(t *testing.T) {
	upstream := []map[string]any{
		{"key1": "value1"},
		{"items": []any{"a", "b", "c"}},
	}
	result := extractIteratorList(upstream, "items")
	if len(result) != 3 {
		t.Errorf("expected 3 items, got %d", len(result))
	}
}

func TestExtractIteratorList_FromInterfaceSlice(t *testing.T) {
	upstream := []map[string]any{
		{"items": []interface{}{"x", "y"}},
	}
	result := extractIteratorList(upstream, "items")
	if len(result) != 2 {
		t.Errorf("expected 2 items, got %d", len(result))
	}
}

func TestExtractIteratorList_NotFound(t *testing.T) {
	upstream := []map[string]any{
		{"key1": "value1"},
	}
	result := extractIteratorList(upstream, "items")
	if result != nil {
		t.Error("expected nil for non-existent key")
	}
}

func TestExtractIteratorList_Empty(t *testing.T) {
	result := extractIteratorList(nil, "items")
	if result != nil {
		t.Error("expected nil for empty upstream")
	}
}

// ============================================================================
// buildItemOutput 测试
// ============================================================================

func TestBuildItemOutput(t *testing.T) {
	upstream := []map[string]any{
		{"global_key": "global_value"},
	}
	output := buildItemOutput(2, "item_value", upstream)

	if output["_index"] != 2 {
		t.Errorf("expected _index=2, got %v", output["_index"])
	}
	if output["_item"] != "item_value" {
		t.Errorf("expected _item=item_value, got %v", output["_item"])
	}
	if output["global_key"] != "global_value" {
		t.Errorf("expected global_key to be propagated, got %v", output["global_key"])
	}
}

func TestBuildItemOutput_MapItem(t *testing.T) {
	upstream := []map[string]any{
		{"global_key": "global_value"},
	}
	item := map[string]any{"name": "test", "count": 42}
	output := buildItemOutput(0, item, upstream)

	if output["name"] != "test" {
		t.Errorf("expected name=test, got %v", output["name"])
	}
	if output["count"] != 42 {
		t.Errorf("expected count=42, got %v", output["count"])
	}
}

// ============================================================================
// Workflow 辅助方法测试
// ============================================================================

func TestWorkflow_AddSubWorkflowGroup(t *testing.T) {
	wf := model.NewWorkflow("parent").
		AddTaskGroup("task1").
		AddSubWorkflowGroup("sub_task", "sub-uuid")

	if len(wf.TaskGroups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(wf.TaskGroups))
	}

	subNode := wf.TaskGroups[1][0]
	if model.GetTaskNodeType(subNode) != model.TaskNodeTypeSubWorkflow {
		t.Error("expected sub_workflow type")
	}
	if subNode.SubWorkflow != "sub-uuid" {
		t.Errorf("expected sub_workflow=sub-uuid, got %s", subNode.SubWorkflow)
	}
}

func TestWorkflow_AddFanOutGroup(t *testing.T) {
	wf := model.NewWorkflow("test").
		AddFanOutGroup("fan", "items", "process_item")

	if len(wf.TaskGroups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(wf.TaskGroups))
	}

	fanNode := wf.TaskGroups[0][0]
	if model.GetTaskNodeType(fanNode) != model.TaskNodeTypeFanOut {
		t.Error("expected fan_out type")
	}
	if fanNode.FanOut.IteratorKey != "items" {
		t.Errorf("expected iterator_key=items, got %s", fanNode.FanOut.IteratorKey)
	}
}

func TestWorkflow_AddLoopGroup(t *testing.T) {
	wf := model.NewWorkflow("test").
		AddLoopGroup("poll", 5, model.LoopConditionAlways, "")

	if len(wf.TaskGroups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(wf.TaskGroups))
	}

	loopNode := wf.TaskGroups[0][0]
	if loopNode.Loop == nil {
		t.Fatal("expected loop definition")
	}
	if loopNode.Loop.MaxIterations != 5 {
		t.Errorf("expected max_iterations=5, got %d", loopNode.Loop.MaxIterations)
	}
}

func TestWorkflow_AllTaskNames_WithAdvancedNodes(t *testing.T) {
	wf := model.NewWorkflow("test").
		AddTaskGroup("task1").
		AddSubWorkflowGroup("sub_task", "sub-uuid").
		AddFanOutGroup("fan_task", "items", "process_item")

	names := wf.AllTaskNames()
	found := make(map[string]bool)
	for _, n := range names {
		found[n] = true
	}

	if !found["task1"] {
		t.Error("task1 should be in all names")
	}
	if !found["sub_task"] {
		t.Error("sub_task should be in all names")
	}
	if !found["fan_task"] {
		t.Error("fan_task should be in all names")
	}
}

func TestWorkflow_DeepCopy_WithAdvancedNodes(t *testing.T) {
	original := model.NewWorkflow("test").
		AddTaskGroup("task1").
		AddSubWorkflowGroup("sub_task", "sub-uuid").
		AddFanOutGroup("fan_task", "items", "process_item").
		AddLoopGroup("poll", 3, model.LoopConditionKey, "continue")

	copy, err := original.DeepCopy()
	if err != nil {
		t.Fatalf("DeepCopy failed: %v", err)
	}

	if len(copy.TaskGroups) != len(original.TaskGroups) {
		t.Errorf("expected %d groups, got %d", len(original.TaskGroups), len(copy.TaskGroups))
	}

	// Verify sub-workflow node preserved
	subNode := copy.TaskGroups[1][0]
	if model.GetTaskNodeType(subNode) != model.TaskNodeTypeSubWorkflow {
		t.Error("sub-workflow type not preserved in deep copy")
	}

	// Verify fan-out node preserved
	fanNode := copy.TaskGroups[2][0]
	if model.GetTaskNodeType(fanNode) != model.TaskNodeTypeFanOut {
		t.Error("fan-out type not preserved in deep copy")
	}
	if fanNode.FanOut == nil || fanNode.FanOut.IteratorKey != "items" {
		t.Error("fan-out definition not preserved in deep copy")
	}

	// Verify loop node preserved
	loopNode := copy.TaskGroups[3][0]
	if loopNode.Loop == nil || loopNode.Loop.MaxIterations != 3 {
		t.Error("loop definition not preserved in deep copy")
	}
}

// ============================================================================
// collectCompletedOutputs 测试
// ============================================================================

func TestCollectCompletedOutputs(t *testing.T) {
	completed := map[string]map[string]any{
		"task1": {"result": "ok"},
		"task2": {"count": 42},
	}
	outputs := collectCompletedOutputs(completed)
	if len(outputs) != 2 {
		t.Errorf("expected 2 outputs, got %d", len(outputs))
	}
}
