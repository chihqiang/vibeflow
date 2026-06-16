package builtin

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"chihqiang/vibeflow/infra/logger"
	"chihqiang/vibeflow/domain/model"
)

// writeFile 将上游任务输出写入文件
// 读取上游 "content"，写入到 file_path 指定的路径
type writeFile struct{}

func NewWriteFile() model.Task { return &writeFile{} }

func (t *writeFile) Name() string { return "write_file" }

func (t *writeFile) Params() []model.Param {
	return []model.Param{
		{Key: "file_path", Label: "文件路径", Type: model.ParamTypeText, Required: true, Default: "/tmp/output.html", Placeholder: "/tmp/output.html", Description: "写入的目标文件路径"},
	}
}

func (t *writeFile) Execute(ctx context.Context, paramCtx *model.Context, taskCtx *model.Context) error {
	filePath, _ := paramCtx.GetString("file_path")
	if filePath == "" {
		return model.WrapNoRetry(fmt.Errorf("参数 file_path 为空"))
	}

	// 读取上游 fetch_url 写入的 content
	content, ok := taskCtx.GetString("content")
	if !ok {
		return model.WrapNoRetry(fmt.Errorf("未找到上游输出 content，请确保前置任务为 fetch_url"))
	}

	logger.Info("写入文件", "path", filePath, "size", len(content))

	// 确保目录存在
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("创建目录失败: %w", err)
	}

	if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
		return fmt.Errorf("写入文件失败: %w", err)
	}

	logger.Info("写入完成")
	return nil
}
