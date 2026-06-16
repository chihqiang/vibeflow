package builtin

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"chihqiang/vibeflow/infra/logger"
	"chihqiang/vibeflow/domain/model"
)

// fetchURL 从指定 URL 获取内容
// 输出: content(string)
type fetchURL struct{}

func NewFetchURL() model.Task { return &fetchURL{} }

func (t *fetchURL) Name() string { return "fetch_url" }

func (t *fetchURL) Params() []model.Param {
	return []model.Param{
		{Key: "url", Label: "请求地址", Type: model.ParamTypeText, Required: true, Default: "https://www.example.com", Placeholder: "https://www.example.com", Description: "要获取内容的 URL"},
	}
}

func (t *fetchURL) Execute(ctx context.Context, paramCtx *model.Context, taskCtx *model.Context) error {
	url, _ := paramCtx.GetString("url")
	if url == "" {
		return model.WrapNoRetry(fmt.Errorf("参数 url 为空，请提供有效的 URL"))
	}

	logger.Info("获取 URL 内容", "url", url)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("创建请求失败: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("请求失败: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("读取响应失败: %w", err)
	}

	// 输出内容，下游任务通过 "content" 读取
	taskCtx.Set("content", string(body))

	logger.Info("获取完成", "size", len(body))
	return nil
}
