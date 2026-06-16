package mysql

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"chihqiang/vibeflow/infra/store"

	"gorm.io/gorm"
)

// VibeflowWorkflow 工作流定义，每次执行创建一条新记录
type VibeflowWorkflow struct {
	ID             uint      `gorm:"primaryKey;autoIncrement"`
	UUID           string    `gorm:"size:36;not null;uniqueIndex;comment:工作流 UUID"`
	Name           string    `gorm:"size:255;not null;index;comment:工作流名称"`
	Status         string    `gorm:"size:20;not null;default:PENDING;comment:PENDING/RUNNING/COMPLETED/FAILED"`
	Trigger        string    `gorm:"size:20;default:manual;comment:触发方式: manual/cron"`
	CronExpr       string    `gorm:"size:100;comment:Cron 表达式"`
	TimeoutSec     int64     `gorm:"default:0;comment:工作流超时秒数"`
	TaskTimeoutSec int64     `gorm:"default:0;comment:任务默认超时秒数"`
	MaxRetries     int       `gorm:"default:0;comment:任务默认最大重试次数"`
	BaseBackoff    int64     `gorm:"default:0;comment:任务默认基础退避秒数"`
	CreatedAt      time.Time `gorm:"autoCreateTime"`
	UpdatedAt      time.Time `gorm:"autoUpdateTime"`
}

func (VibeflowWorkflow) TableName() string { return "vibeflow_workflows" }

// VibeflowWorkflowTask 工作流中的任务定义，一个工作流有多条
type VibeflowWorkflowTask struct {
	ID         uint      `gorm:"primaryKey;autoIncrement"`
	WorkflowID uint      `gorm:"column:workflow_id;not null;uniqueIndex:idx_wf_task;comment:所属工作流 ID"`
	Name       string    `gorm:"size:255;not null;uniqueIndex:idx_wf_task;comment:任务名称"`
	Params     string    `gorm:"type:text;comment:参数定义 JSON"`
	SortOrder  int       `gorm:"not null;default:0;comment:执行顺序"`
	CreatedAt  time.Time `gorm:"autoCreateTime"`
}

func (VibeflowWorkflowTask) TableName() string { return "vibeflow_workflow_tasks" }

// VibeflowExecution 工作流执行记录，每次提交/重试新增一行
type VibeflowExecution struct {
	ID         uint           `gorm:"primaryKey;autoIncrement"`
	WorkflowID uint           `gorm:"column:workflow_id;not null;index;comment:关联的 workflow ID"`
	Data       string         `gorm:"type:longtext;not null;comment:WorkflowState JSON"`
	Status     string         `gorm:"size:20;not null;comment:RUNNING/COMPLETED/FAILED"`
	Error      string         `gorm:"type:text;comment:错误信息"`
	StartedAt  time.Time      `gorm:"not null;index;comment:提交时间"`
	CreatedAt  time.Time      `gorm:"autoCreateTime"`
	UpdatedAt  time.Time      `gorm:"autoUpdateTime"`
	DeletedAt  gorm.DeletedAt `gorm:"index"`
}

func (VibeflowExecution) TableName() string { return "vibeflow_executions" }

// VibeflowExecutionTask 任务执行详情，每次任务完成/失败追加一行
type VibeflowExecutionTask struct {
	ID             uint      `gorm:"primaryKey;autoIncrement"`
	ExecutionID    uint      `gorm:"not null;index;comment:所属执行记录 ID"`
	WorkflowID     uint      `gorm:"column:workflow_id;not null;index;comment:所属工作流 ID"`
	WorkflowTaskID uint      `gorm:"not null;index;comment:关联的任务定义 ID"`
	TaskName       string    `gorm:"size:255;not null;comment:任务名称"`
	Status         string    `gorm:"size:20;not null;comment:COMPLETED/FAILED"`
	Params         string    `gorm:"type:text;comment:输入参数 JSON"`
	Output         string    `gorm:"type:text;comment:输出结果 JSON"`
	Error          string    `gorm:"type:text;comment:错误信息"`
	RetryCount     int       `gorm:"default:0;comment:当前重试次数"`
	MaxRetries     int       `gorm:"default:0;comment:最大重试次数"`
	CreatedAt      time.Time `gorm:"autoCreateTime"`
}

func (VibeflowExecutionTask) TableName() string { return "vibeflow_execution_tasks" }

// VibeflowTaskDelta 运行时增量快照，每次任务变更追加一条记录
// 替代运行时全量序列化 WorkflowState，快照大小从 O(总任务数) 降为 O(1)
type VibeflowTaskDelta struct {
	ID          uint      `gorm:"primaryKey;autoIncrement"`
	ExecutionID uint      `gorm:"not null;index;comment:所属执行记录 ID"`
	Data        string    `gorm:"type:text;not null;comment:TaskDelta JSON"`
	CreatedAt   time.Time `gorm:"autoCreateTime"`
}

func (VibeflowTaskDelta) TableName() string { return "vibeflow_task_deltas" }

// HistoryStore MySQL 实现的 store.HistoryStore
type HistoryStore struct {
	db *gorm.DB

	// taskIDCache 缓存 workflow_id + task_name → task_id 的映射
	// key 格式: "workflowID:taskName"
	taskIDCache   map[string]uint
	taskIDCacheMu sync.RWMutex
}

// NewHistoryStore 创建历史记录存储，自动迁库
func NewHistoryStore(db *gorm.DB) (*HistoryStore, error) {
	if err := db.AutoMigrate(&VibeflowWorkflow{}, &VibeflowWorkflowTask{}, &VibeflowExecution{}, &VibeflowExecutionTask{}, &VibeflowTaskDelta{}); err != nil {
		return nil, err
	}
	s := &HistoryStore{db: db}
	if err := s.buildTaskIDCache(); err != nil {
		return nil, fmt.Errorf("构建任务 ID 缓存失败: %w", err)
	}
	return s, nil
}

// buildTaskIDCache 启动时一次性加载所有 workflow_id + task_name → task_id 映射
// 避免每次 SaveTask 都查一次数据库
func (s *HistoryStore) buildTaskIDCache() error {
	var tasks []VibeflowWorkflowTask
	if err := s.db.Select("id, workflow_id, name").Find(&tasks).Error; err != nil {
		return err
	}

	cache := make(map[string]uint, len(tasks))
	for _, t := range tasks {
		key := taskIDCacheKey(t.WorkflowID, t.Name)
		cache[key] = t.ID
	}

	s.taskIDCacheMu.Lock()
	s.taskIDCache = cache
	s.taskIDCacheMu.Unlock()

	return nil
}

// taskIDCacheKey 生成缓存键
func taskIDCacheKey(workflowID uint, taskName string) string {
	return fmt.Sprintf("%d:%s", workflowID, taskName)
}

// putTaskIDCache 线程安全地写入缓存
func (s *HistoryStore) putTaskIDCache(workflowID uint, taskName string, taskID uint) {
	s.taskIDCacheMu.Lock()
	s.taskIDCache[taskIDCacheKey(workflowID, taskName)] = taskID
	s.taskIDCacheMu.Unlock()
}

func (s *HistoryStore) SaveWorkflowDef(ctx context.Context, uuid, name string, tasks []store.TaskDef,
	timeoutSec, taskTimeoutSec int64, maxRetries int, baseBackoff int64, trigger, cronExpr string) (uint, error) {

	tx := s.db.WithContext(ctx).Begin()

	rec := &VibeflowWorkflow{
		UUID:           uuid,
		Name:           name,
		Status:         store.WorkflowStatusPending,
		Trigger:        trigger,
		CronExpr:       cronExpr,
		TimeoutSec:     timeoutSec,
		TaskTimeoutSec: taskTimeoutSec,
		MaxRetries:     maxRetries,
		BaseBackoff:    baseBackoff,
	}
	if err := tx.Create(rec).Error; err != nil {
		tx.Rollback()
		return 0, err
	}

	for i, t := range tasks {
		paramsJSON, err := json.Marshal(t.Params)
		if err != nil {
			tx.Rollback()
			return 0, fmt.Errorf("序列化任务参数失败 task=%s: %w", t.Name, err)
		}
		taskRec := &VibeflowWorkflowTask{
			WorkflowID: rec.ID,
			Name:       t.Name,
			Params:     string(paramsJSON),
			SortOrder:  i,
		}
		if err := tx.Create(taskRec).Error; err != nil {
			tx.Rollback()
			return 0, err
		}
		// 更新缓存
		s.putTaskIDCache(taskRec.WorkflowID, taskRec.Name, taskRec.ID)
	}

	if err := tx.Commit().Error; err != nil {
		return 0, err
	}
	return rec.ID, nil
}

// UpsertWorkflowDef 按 UUID upsert 工作流定义
// UUID 已存在时更新定义及其关联任务（先删旧任务再插入新任务），返回已有记录 ID
func (s *HistoryStore) UpsertWorkflowDef(ctx context.Context, uuid, name string, tasks []store.TaskDef,
	timeoutSec, taskTimeoutSec int64, maxRetries int, baseBackoff int64, trigger, cronExpr string) (uint, error) {

	// 检查是否已存在同 UUID 定义
	var existing VibeflowWorkflow
	err := s.db.WithContext(ctx).Where("uuid = ?", uuid).First(&existing).Error
	if err == nil {
		// 已存在：更新定义字段并替换关联任务
		tx := s.db.WithContext(ctx).Begin()

		if err := tx.Model(&existing).Updates(map[string]any{
			"name":             name,
			"timeout_sec":      timeoutSec,
			"task_timeout_sec": taskTimeoutSec,
			"max_retries":      maxRetries,
			"base_backoff":     baseBackoff,
			"trigger":          trigger,
			"cron_expr":        cronExpr,
		}).Error; err != nil {
			tx.Rollback()
			return 0, err
		}

		// 删除旧任务，插入新任务
		if err := tx.Where("workflow_id = ?", existing.ID).Delete(&VibeflowWorkflowTask{}).Error; err != nil {
			tx.Rollback()
			return 0, err
		}
		for i, t := range tasks {
			paramsJSON, err := json.Marshal(t.Params)
			if err != nil {
				tx.Rollback()
				return 0, fmt.Errorf("序列化任务参数失败 task=%s: %w", t.Name, err)
			}
			taskRec := &VibeflowWorkflowTask{
				WorkflowID: existing.ID,
				Name:       t.Name,
				Params:     string(paramsJSON),
				SortOrder:  i,
			}
			if err := tx.Create(taskRec).Error; err != nil {
				tx.Rollback()
				return 0, err
			}
			// 更新缓存（新任务可能名称相同但 ID 不同）
			s.putTaskIDCache(taskRec.WorkflowID, taskRec.Name, taskRec.ID)
		}

		if err := tx.Commit().Error; err != nil {
			return 0, err
		}
		return existing.ID, nil
	}
	if err != gorm.ErrRecordNotFound {
		return 0, err
	}

	return s.SaveWorkflowDef(ctx, uuid, name, tasks, timeoutSec, taskTimeoutSec, maxRetries, baseBackoff, trigger, cronExpr)
}

// LoadWorkflowDefs 获取所有 PENDING 状态的工作流定义
// 使用两次查询替代 N+1：先查所有工作流，再批量查所有关联任务，内存中分组
func (s *HistoryStore) LoadWorkflowDefs(ctx context.Context) ([]store.WorkflowDefRecord, error) {
	var wfs []VibeflowWorkflow
	if err := s.db.WithContext(ctx).Where("status = ?", store.WorkflowStatusPending).Find(&wfs).Error; err != nil {
		return nil, err
	}
	if len(wfs) == 0 {
		return nil, nil
	}

	// 收集所有工作流 ID
	ids := make([]uint, len(wfs))
	for i, wf := range wfs {
		ids[i] = wf.ID
	}

	// 一次查询加载所有关联任务
	var allTasks []VibeflowWorkflowTask
	if err := s.db.WithContext(ctx).
		Where("workflow_id IN ?", ids).
		Order("workflow_id, sort_order asc").
		Find(&allTasks).Error; err != nil {
		return nil, err
	}

	// 按 workflow_id 分组
	taskMap := make(map[uint][]VibeflowWorkflowTask, len(wfs))
	for _, t := range allTasks {
		taskMap[t.WorkflowID] = append(taskMap[t.WorkflowID], t)
	}

	result := make([]store.WorkflowDefRecord, 0, len(wfs))
	for _, wf := range wfs {
		tasks := taskMap[wf.ID]

		taskDefs := make([]store.TaskDef, len(tasks))
		for i, t := range tasks {
			var params map[string]interface{}
			if t.Params != "" {
				if err := json.Unmarshal([]byte(t.Params), &params); err != nil {
					return nil, fmt.Errorf("解析任务参数失败 workflow=%s task=%s: %w", wf.Name, t.Name, err)
				}
			}
			taskDefs[i] = store.TaskDef{Name: t.Name, Params: params}
		}

		result = append(result, store.WorkflowDefRecord{
			ID:             wf.ID,
			Name:           wf.Name,
			Tasks:          taskDefs,
			TimeoutSec:     wf.TimeoutSec,
			TaskTimeoutSec: wf.TaskTimeoutSec,
			MaxRetries:     wf.MaxRetries,
			BaseBackoff:    wf.BaseBackoff,
			Trigger:        wf.Trigger,
			CronExpr:       wf.CronExpr,
		})
	}
	return result, nil
}

func (s *HistoryStore) CreateExecution(ctx context.Context, workflowID uint, data []byte, status string, startedAt time.Time, errMsg string) (uint, error) {
	rec := &VibeflowExecution{
		WorkflowID: workflowID,
		Data:       string(data),
		Status:     status,
		Error:      errMsg,
		StartedAt:  startedAt,
	}
	if err := s.db.WithContext(ctx).Create(rec).Error; err != nil {
		return 0, err
	}
	return rec.ID, nil
}

func (s *HistoryStore) UpdateWorkflowStatus(ctx context.Context, workflowID uint, status string) error {
	return s.db.WithContext(ctx).Model(&VibeflowWorkflow{}).
		Where("id = ?", workflowID).
		Update("status", status).Error
}

func (s *HistoryStore) UpdateExecution(ctx context.Context, executionID uint, data []byte, status string, errMsg string) error {
	return s.db.WithContext(ctx).Model(&VibeflowExecution{}).
		Where("id = ?", executionID).
		Updates(map[string]any{
			"data":   string(data),
			"status": status,
			"error":  errMsg,
		}).Error
}

func (s *HistoryStore) SaveTask(ctx context.Context, executionID, workflowID uint, taskName string, status string,
	params map[string]any, output map[string]any, errMsg string, retryCount int, maxRetries int) error {

	var paramsStr, outputStr string
	if params != nil {
		if b, err := json.Marshal(params); err == nil {
			paramsStr = string(b)
		}
	}
	if output != nil {
		if b, err := json.Marshal(output); err == nil {
			outputStr = string(b)
		}
	}

	// 从缓存查找任务定义 ID，未命中时回退到数据库查询并更新缓存
	key := taskIDCacheKey(workflowID, taskName)
	s.taskIDCacheMu.RLock()
	workflowTaskID, ok := s.taskIDCache[key]
	s.taskIDCacheMu.RUnlock()

	if !ok {
		var queriedID uint
		err := s.db.WithContext(ctx).Model(&VibeflowWorkflowTask{}).
			Select("id").Where("workflow_id = ? AND name = ?", workflowID, taskName).
			Scan(&queriedID).Error
		if err != nil {
			return fmt.Errorf("查询任务定义 ID 失败: workflow_id=%d, task=%s: %w", workflowID, taskName, err)
		}
		if queriedID == 0 {
			return fmt.Errorf("任务定义不存在: workflow_id=%d, task=%s", workflowID, taskName)
		}
		// 回填缓存
		s.taskIDCacheMu.Lock()
		s.taskIDCache[key] = queriedID
		s.taskIDCacheMu.Unlock()
		workflowTaskID = queriedID
	}

	return s.db.WithContext(ctx).Create(&VibeflowExecutionTask{
		ExecutionID:    executionID,
		WorkflowID:     workflowID,
		WorkflowTaskID: workflowTaskID,
		TaskName:       taskName,
		Status:         status,
		Params:         paramsStr,
		Output:         outputStr,
		Error:          errMsg,
		RetryCount:     retryCount,
		MaxRetries:     maxRetries,
	}).Error
}

// executionWithName 用于 JOIN 查询执行记录和工作流名称、UUID
type executionWithName struct {
	VibeflowExecution
	WorkflowName string `gorm:"column:name"`
	WorkflowUUID string `gorm:"column:uuid"`
}

func (s *HistoryStore) LoadExecutions(ctx context.Context, offset, limit int) ([]store.ExecutionRecord, int64, error) {
	var total int64
	if err := s.db.WithContext(ctx).
		Table("vibeflow_executions").
		Count(&total).Error; err != nil {
		return nil, 0, err
	}
	var records []executionWithName
	if err := s.db.WithContext(ctx).
		Table("vibeflow_executions").
		Select("vibeflow_executions.*, vibeflow_workflows.name, vibeflow_workflows.uuid").
		Joins("left join vibeflow_workflows on vibeflow_executions.workflow_id = vibeflow_workflows.id").
		Order("vibeflow_executions.id desc").
		Offset(offset).
		Limit(limit).
		Find(&records).Error; err != nil {
		return nil, 0, err
	}
	result := make([]store.ExecutionRecord, 0, len(records))
	for _, r := range records {
		result = append(result, store.ExecutionRecord{
			ExecutionID: r.ID,
			UUID:        r.WorkflowUUID,
			Name:        r.WorkflowName,
			Data:        []byte(r.Data),
		})
	}
	return result, total, nil
}

// GetExecutionByUUID 按工作流 UUID 获取最新一条执行记录
// 使用 WHERE uuid = ? ORDER BY id DESC LIMIT 1 精确查询，避免全表扫描
func (s *HistoryStore) GetExecutionByUUID(ctx context.Context, uuid string) (*store.ExecutionRecord, error) {
	var rec executionWithName
	err := s.db.WithContext(ctx).
		Table("vibeflow_executions").
		Select("vibeflow_executions.*, vibeflow_workflows.name, vibeflow_workflows.uuid").
		Joins("left join vibeflow_workflows on vibeflow_executions.workflow_id = vibeflow_workflows.id").
		Where("vibeflow_workflows.uuid = ?", uuid).
		Order("vibeflow_executions.id desc").
		Limit(1).
		First(&rec).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &store.ExecutionRecord{
		ExecutionID: rec.ID,
		UUID:        rec.WorkflowUUID,
		Name:        rec.WorkflowName,
		Data:        []byte(rec.Data),
	}, nil
}

// SaveTaskDeltas 批量保存任务增量变更记录
func (s *HistoryStore) SaveTaskDeltas(ctx context.Context, executionID uint, deltas []byte) error {
	return s.db.WithContext(ctx).Create(&VibeflowTaskDelta{
		ExecutionID: executionID,
		Data:        string(deltas),
	}).Error
}

// LoadTaskDeltas 加载指定执行记录的所有增量变更
// 将多条增量记录的 JSON 数组合并为一个扁平的 JSON 数组
func (s *HistoryStore) LoadTaskDeltas(ctx context.Context, executionID uint) ([]byte, error) {
	var records []VibeflowTaskDelta
	if err := s.db.WithContext(ctx).
		Where("execution_id = ?", executionID).
		Order("id asc").
		Find(&records).Error; err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, nil
	}
	// 将多条增量记录的 JSON 数组合并为一个扁平数组
	var allDeltas []json.RawMessage
	for _, r := range records {
		var batch []json.RawMessage
		if err := json.Unmarshal([]byte(r.Data), &batch); err != nil {
			// 单条数据无法解析为数组时，尝试作为单个对象处理
			allDeltas = append(allDeltas, json.RawMessage(r.Data))
			continue
		}
		allDeltas = append(allDeltas, batch...)
	}
	result, err := json.Marshal(allDeltas)
	if err != nil {
		return nil, fmt.Errorf("聚合增量记录失败: %w", err)
	}
	return result, nil
}

func (s *HistoryStore) Close() error {
	sqlDB, err := s.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}

// Ping 检查 MySQL 连接是否正常，测量并返回延迟
func (s *HistoryStore) Ping(ctx context.Context) (time.Duration, error) {
	start := time.Now()
	sqlDB, err := s.db.DB()
	if err != nil {
		return 0, err
	}
	if err := sqlDB.PingContext(ctx); err != nil {
		return time.Since(start), err
	}
	return time.Since(start), nil
}

// Stats 返回 MySQL 连接池统计信息
func (s *HistoryStore) Stats() store.DBStats {
	sqlDB, err := s.db.DB()
	if err != nil {
		return store.DBStats{}
	}
	stats := sqlDB.Stats()
	return store.DBStats{
		MaxOpenConnections: stats.MaxOpenConnections,
		OpenConnections:    stats.OpenConnections,
		InUse:              stats.InUse,
		Idle:               stats.Idle,
	}
}

// LoadRunningExecutions 获取所有状态为 RUNNING 的执行记录
// 用于 Master 重启后恢复中断的工作流
func (s *HistoryStore) LoadRunningExecutions(ctx context.Context) ([]store.ExecutionRecord, error) {
	var records []executionWithName
	if err := s.db.WithContext(ctx).
		Table("vibeflow_executions").
		Select("vibeflow_executions.*, vibeflow_workflows.name, vibeflow_workflows.uuid").
		Joins("left join vibeflow_workflows on vibeflow_executions.workflow_id = vibeflow_workflows.id").
		Where("vibeflow_executions.status = ?", store.WorkflowStatusRunning).
		Order("vibeflow_executions.id asc").
		Find(&records).Error; err != nil {
		return nil, err
	}
	result := make([]store.ExecutionRecord, 0, len(records))
	for _, r := range records {
		result = append(result, store.ExecutionRecord{
			ExecutionID: r.ID,
			UUID:        r.WorkflowUUID,
			Name:        r.WorkflowName,
			Data:        []byte(r.Data),
		})
	}
	return result, nil
}

// BatchLoadTaskDeltas 批量加载多个执行记录的所有增量变更
// 使用一次 SQL 查询（WHERE execution_id IN (...）替代 N 次查询
// 结果按 executionID 分组返回，无增量记录的不在 map 中
func (s *HistoryStore) BatchLoadTaskDeltas(ctx context.Context, executionIDs []uint) (map[uint][]byte, error) {
	if len(executionIDs) == 0 {
		return nil, nil
	}

	var records []VibeflowTaskDelta
	if err := s.db.WithContext(ctx).
		Where("execution_id IN ?", executionIDs).
		Order("execution_id asc, id asc").
		Find(&records).Error; err != nil {
		return nil, err
	}

	if len(records) == 0 {
		return nil, nil
	}

	// 按 executionID 分组聚合增量数据
	result := make(map[uint][]byte, len(executionIDs))
	for _, r := range records {
		// 将多条增量记录的 JSON 数组合并为一个扁平数组
		var batch []json.RawMessage
		if err := json.Unmarshal([]byte(r.Data), &batch); err != nil {
			// 单条数据无法解析为数组时，作为单个对象处理
			existing := result[r.ExecutionID]
			if existing != nil {
				// 追加到已有数组
				var existingArr []json.RawMessage
				if err := json.Unmarshal(existing, &existingArr); err != nil {
					continue
				}
				existingArr = append(existingArr, json.RawMessage(r.Data))
				if merged, err := json.Marshal(existingArr); err == nil {
					result[r.ExecutionID] = merged
				}
			} else {
				singleArr := []json.RawMessage{json.RawMessage(r.Data)}
				if encoded, err := json.Marshal(singleArr); err == nil {
					result[r.ExecutionID] = encoded
				}
			}
			continue
		}

		existing := result[r.ExecutionID]
		if existing != nil {
			var existingArr []json.RawMessage
			if err := json.Unmarshal(existing, &existingArr); err != nil {
				continue
			}
			existingArr = append(existingArr, batch...)
			if merged, err := json.Marshal(existingArr); err == nil {
				result[r.ExecutionID] = merged
			}
		} else {
			if encoded, err := json.Marshal(batch); err == nil {
				result[r.ExecutionID] = encoded
			}
		}
	}

	return result, nil
}
