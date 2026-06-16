package model

import (
	"sync"
	"testing"
)


// ============================================================================
// ApplyInputMapping 测试
// ============================================================================

func TestApplyInputMapping_BasicMapping(t *testing.T) {
	node := TaskNode{
		Name: "deploy",
		InputMapping: map[string]string{
			"repo": "git_url",
			"env":  "deploy_env",
		},
	}
	upstream := []map[string]any{
		{"git_url": "github.com/repo", "deploy_env": "prod", "extra": "ignored"},
	}

	result := ApplyInputMapping(node, upstream)
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result["repo"] != "github.com/repo" {
		t.Errorf("expected repo 'github.com/repo', got %v", result["repo"])
	}
	if result["env"] != "prod" {
		t.Errorf("expected env 'prod', got %v", result["env"])
	}
	if _, ok := result["extra"]; ok {
		t.Error("expected 'extra' to be filtered out")
	}
}

func TestApplyInputMapping_NoMapping(t *testing.T) {
	node := TaskNode{Name: "deploy"}
	upstream := []map[string]any{{"key": "value"}}

	result := ApplyInputMapping(node, upstream)
	if result != nil {
		t.Error("expected nil result when no mapping defined")
	}
}

func TestApplyInputMapping_EmptyMapping(t *testing.T) {
	node := TaskNode{Name: "deploy", InputMapping: map[string]string{}}
	upstream := []map[string]any{{"key": "value"}}

	result := ApplyInputMapping(node, upstream)
	if result != nil {
		t.Error("expected nil result for empty mapping")
	}
}

func TestApplyInputMapping_MissingSourceKey(t *testing.T) {
	node := TaskNode{
		Name: "deploy",
		InputMapping: map[string]string{
			"repo": "nonexistent_key",
		},
	}
	upstream := []map[string]any{{"git_url": "github.com/repo"}}

	result := ApplyInputMapping(node, upstream)
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if _, ok := result["repo"]; ok {
		t.Error("expected 'repo' to be absent when source key doesn't exist")
	}
}

func TestApplyInputMapping_MultipleUpstreamOutputs(t *testing.T) {
	node := TaskNode{
		Name: "task",
		InputMapping: map[string]string{
			"from_first":  "key1",
			"from_second": "key2",
		},
	}
	upstream := []map[string]any{
		{"key1": "value1"},
		{"key2": "value2"},
	}

	result := ApplyInputMapping(node, upstream)
	if result["from_first"] != "value1" {
		t.Errorf("expected 'from_first'='value1', got %v", result["from_first"])
	}
	if result["from_second"] != "value2" {
		t.Errorf("expected 'from_second'='value2', got %v", result["from_second"])
	}
}

// ============================================================================
// Context 并发安全测试
// ============================================================================

func TestContext_ConcurrentReadWrite(t *testing.T) {
	ctx := NewContext()
	var wg sync.WaitGroup

	// 并发写入
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx.Set("key", i)
		}(i)
	}

	// 并发读取
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx.Get("key")
			ctx.GetAll()
		}()
	}

	wg.Wait()
}

func TestContext_SkipGroups(t *testing.T) {
	ctx := NewContext()
	if ctx.GetSkipGroups() != 0 {
		t.Error("expected initial SkipGroups to be 0")
	}

	ctx.SkipGroups(3)
	if ctx.GetSkipGroups() != 3 {
		t.Errorf("expected SkipGroups=3, got %d", ctx.GetSkipGroups())
	}
}

func TestContext_BranchControl(t *testing.T) {
	ctx := NewContext()
	if ctx.GetBranch() != "" {
		t.Error("expected empty branch")
	}

	ctx.SetBranch("approved")
	if ctx.GetBranch() != "approved" {
		t.Errorf("expected 'approved', got %q", ctx.GetBranch())
	}
}

func TestContext_Approval(t *testing.T) {
	ctx := NewContext()
	if ctx.GetApproval() != "" {
		t.Error("expected empty approval")
	}

	msg := "请确认部署"
	ctx.SetApproval(msg)
	if ctx.GetApproval() != msg {
		t.Errorf("expected %q, got %q", msg, ctx.GetApproval())
	}
}

// ============================================================================
// ErrorPolicy 测试
// ============================================================================

func TestErrorPolicy_GetTaskErrorPolicy(t *testing.T) {
	ep := &ErrorPolicy{
		OnTaskFailure: ErrorPolicySkip,
		TaskPolicies: map[string]ErrorPolicyType{
			"critical_task": ErrorPolicyFailFast,
		},
	}

	if ep.GetTaskErrorPolicy("normal_task") != ErrorPolicySkip {
		t.Error("expected global policy for normal task")
	}
	if ep.GetTaskErrorPolicy("critical_task") != ErrorPolicyFailFast {
		t.Error("expected task-specific policy for critical task")
	}
}

func TestErrorPolicy_IsSkippable(t *testing.T) {
	ep := &ErrorPolicy{
		OnTaskFailure: ErrorPolicySkip,
		SkippableTasks: []string{"non_critical"},
	}

	if !ep.IsSkippable("non_critical") {
		t.Error("expected non_critical to be skippable")
	}
	if ep.IsSkippable("critical") {
		t.Error("expected critical to not be skippable")
	}

	// 空列表表示所有任务可跳过
	epEmpty := &ErrorPolicy{OnTaskFailure: ErrorPolicySkip}
	if !epEmpty.IsSkippable("any_task") {
		t.Error("expected all tasks skippable when list is empty")
	}
}

func TestErrorPolicy_Validate(t *testing.T) {
	tests := []struct {
		name    string
		policy  *ErrorPolicy
		wantErr bool
	}{
		{"nil policy", nil, false},
		{"valid retry", &ErrorPolicy{OnTaskFailure: ErrorPolicyRetry}, false},
		{"valid rollback", &ErrorPolicy{OnTaskFailure: ErrorPolicyRollback}, false},
		{"valid skip", &ErrorPolicy{OnTaskFailure: ErrorPolicySkip}, false},
		{"valid fail_fast", &ErrorPolicy{OnTaskFailure: ErrorPolicyFailFast}, false},
		{"invalid policy", &ErrorPolicy{OnTaskFailure: "invalid"}, true},
		{"invalid timeout", &ErrorPolicy{OnTimeout: "invalid"}, true},
		{"valid timeout rollback", &ErrorPolicy{OnTimeout: "rollback"}, false},
		{"valid timeout fail", &ErrorPolicy{OnTimeout: "fail"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.policy.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestErrorPolicy_NilSafe(t *testing.T) {
	var ep *ErrorPolicy

	if ep.GetTaskErrorPolicy("any") != ErrorPolicyRetry {
		t.Error("nil policy should default to retry")
	}
	if ep.IsSkippable("any") {
		t.Error("nil policy should not be skippable")
	}
	if ep.GetTimeoutPolicy() != "rollback" {
		t.Error("nil policy should default timeout to rollback")
	}
}

// ============================================================================
// Workflow DeepCopy 测试
// ============================================================================

func TestWorkflow_DeepCopy(t *testing.T) {
	wf := NewWorkflow("test-workflow")
	wf.UUID = "unique-uuid"
	wf.TaskGroups = [][]TaskNode{
		{{Name: "task1", Params: map[string]any{"key": "value"}}},
		{{Name: "task2"}, {Name: "task3"}},
	}

	copy, err := wf.DeepCopy()
	if err != nil {
		t.Fatalf("DeepCopy failed: %v", err)
	}

	// 验证基本字段
	if copy.Name != wf.Name {
		t.Errorf("name mismatch: got %q, want %q", copy.Name, wf.Name)
	}
	if copy.UUID != wf.UUID {
		t.Errorf("UUID mismatch: got %q, want %q", copy.UUID, wf.UUID)
	}

	// 验证嵌套结构独立性
	if len(copy.TaskGroups) != len(wf.TaskGroups) {
		t.Fatal("task groups count mismatch")
	}

	// 修改副本不应影响原对象
	copy.Name = "modified"
	copy.UUID = "modified-uuid"
	copy.TaskGroups[0][0].Params["key"] = "modified"

	if wf.Name == "modified" {
		t.Error("modifying copy should not affect original name")
	}
	if wf.UUID == "modified-uuid" {
		t.Error("modifying copy should not affect original UUID")
	}
	if wf.TaskGroups[0][0].Params["key"] == "modified" {
		t.Error("modifying copy should not affect original params")
	}
}

func TestWorkflow_DeepCopy_WithAdvancedFeatures(t *testing.T) {
	wf := NewWorkflow("test-workflow")
	wf.UUID = "uuid"
	wf.TaskGroups = [][]TaskNode{
		{
			{
				Type:        TaskNodeTypeSubWorkflow,
				Name:        "deploy",
				SubWorkflow: "sub-uuid",
				SubWorkflowParams: map[string]any{
					"env": "prod",
				},
			},
		},
		{
			{
				Type: TaskNodeTypeFanOut,
				Name: "batch",
				FanOut: &FanOutDef{
					IteratorKey: "items",
					Task:        TaskNode{Name: "process"},
					MaxParallel: 10,
				},
			},
		},
		{
			{
				Name: "poll",
				Loop: &LoopDef{
					MaxIterations: 10,
					ConditionType: LoopConditionKey,
					ConditionKey:  "continue",
				},
			},
		},
	}

	copy, err := wf.DeepCopy()
	if err != nil {
		t.Fatalf("DeepCopy failed: %v", err)
	}

	// 验证子工作流
	if copy.TaskGroups[0][0].SubWorkflow != "sub-uuid" {
		t.Error("sub workflow UUID not preserved in deep copy")
	}
	copy.TaskGroups[0][0].SubWorkflowParams["env"] = "staging"
	if wf.TaskGroups[0][0].SubWorkflowParams["env"] == "staging" {
		t.Error("modifying copy's sub workflow params affected original")
	}

	// 验证 Fan-Out
	if copy.TaskGroups[1][0].FanOut.MaxParallel != 10 {
		t.Error("fan-out max_parallel not preserved")
	}

	// 验证 Loop
	if copy.TaskGroups[2][0].Loop.MaxIterations != 10 {
		t.Error("loop max_iterations not preserved")
	}
}

// ============================================================================
// AllTaskNames 测试
// ============================================================================

func TestAllTaskNames_Ordered(t *testing.T) {
	wf := NewWorkflow("test")
	wf.TaskGroups = [][]TaskNode{
		{{Name: "task1"}},
		{{Name: "task2"}, {Name: "task3"}},
		{{Name: "check", Branches: BranchDef{
			"yes": {{{Name: "branch_task1"}}},
			"no":  {{{Name: "branch_task2"}}},
		}}},
	}

	names := wf.AllTaskNames()
	expected := []string{"task1", "task2", "task3", "check", "branch_task1", "branch_task2"}
	if len(names) != len(expected) {
		t.Fatalf("expected %d names, got %d: %v", len(expected), len(names), names)
	}
	for i, n := range expected {
		if names[i] != n {
			t.Errorf("names[%d] = %q, want %q", i, names[i], n)
		}
	}
}
